"""Gateway command-line tools."""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error, request


DEFAULT_RUN_ROOT = Path(r"D:\FlowKitRuntimeDiag")
SUPPORTED_MIME_TYPES = {"image/png", "image/jpeg", "image/webp"}
SUPPORTED_DURATIONS = {10}
SUPPORTED_ASPECT_RATIOS = {"9:16", "16:9"}


class RunVideoOnceError(Exception):
    def __init__(self, stage: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.stage = stage
        self.details = details or {}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m gateway.cli")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run-video-once")
    run.add_argument("--image", required=True)
    run.add_argument("--prompt", required=True)
    run.add_argument("--duration", required=True, type=int)
    run.add_argument("--aspect-ratio", required=True)
    run.add_argument("--preferred-account")
    run.add_argument("--output-dir")
    run.add_argument("--timeout-seconds", type=int, default=1200)
    run.add_argument("--idempotency-key")
    args = parser.parse_args(argv)

    if args.command == "run-video-once":
        result = run_video_once(args)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1
    return 2


def run_video_once(args) -> dict[str, Any]:
    gateway_process: subprocess.Popen | None = None
    run_dir: Path | None = None
    try:
        image_path = _validate_inputs(args)
        run_dir = _create_run_dir(Path(args.output_dir) if args.output_dir else DEFAULT_RUN_ROOT)
        db_path = run_dir / "gateway.db"
        port = _find_free_local_port()
        log_path = run_dir / "gateway.log"
        gateway_process = _start_gateway(port, db_path, log_path)
        try:
            _wait_for_gateway(port)
            accounts = _wait_for_eligible_account(port, args.preferred_account)
            selected = _select_account(accounts, args.preferred_account)
            project = _create_project(selected["api_url"], run_dir)
            project_id = project.get("id")
            if not isinstance(project_id, str) or not project_id.strip():
                raise RunVideoOnceError("project_create_failed", "Worker project response did not include id")
            if project_id.startswith("gateway-"):
                raise RunVideoOnceError("project_create_failed", "Worker returned a non-Flow project id")
            task_payload = {
                "idempotency_key": args.idempotency_key or f"FLOW-VIDEO-ONCE-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4()}",
                "project_id": project_id,
                "image_path": str(image_path),
                "prompt": args.prompt,
                "duration": args.duration,
                "aspect_ratio": args.aspect_ratio,
                "preferred_account_id": selected["account_id"],
            }
            task = _post_json(port, "/api/pool/tasks", task_payload, timeout=10)
            task_id = task.get("task_id")
            if not task_id:
                raise RunVideoOnceError("gateway_task_create_failed", "Gateway task response did not include task_id")
            final_task = _wait_for_task(port, task_id, args.timeout_seconds)
            _validate_completed_task(final_task)
            video_path = Path(final_task["video_path"])
            video_size = video_path.stat().st_size
            result = {
                "result": "completed",
                "ok": True,
                "run_dir": str(run_dir),
                "gateway_task_id": task_id,
                "project_id": project_id,
                "assigned_account_id": final_task.get("assigned_account_id"),
                "worker_job_id": final_task.get("worker_job_id"),
                "attempt_count": final_task.get("attempt_count"),
                "status": final_task.get("status"),
                "error_code": final_task.get("error_code"),
                "error_message": final_task.get("error_message"),
                "video_path": str(video_path),
                "video_size_bytes": video_size,
                "mp4_ftyp_valid": True,
                "gateway_stopped": None,
                "flow_runtime_kept_running": True,
            }
            return result
        finally:
            stopped = _stop_gateway(gateway_process)
            gateway_process = None
            if "result" in locals():
                result["gateway_stopped"] = stopped
    except RunVideoOnceError as exc:
        result = {
            "result": exc.stage,
            "ok": False,
            "stage": exc.stage,
            "error_message": str(exc)[:500],
            "run_dir": str(run_dir) if run_dir else None,
            "gateway_stopped": False,
            "flow_runtime_kept_running": True,
        }
        result.update(exc.details)
        return result
    finally:
        if gateway_process is not None:
            _stop_gateway(gateway_process)

    # The success path returns inside the inner try so gateway_stopped can be updated after stop.
    # This statement is unreachable, but keeps type checkers and defensive callers happy.
    return {"result": "generation_failed", "ok": False, "flow_runtime_kept_running": True}


def _validate_inputs(args) -> Path:
    image_path = Path(args.image)
    if not image_path.is_absolute() or not image_path.exists() or not image_path.is_file():
        raise RunVideoOnceError("input_validation_failed", "Image must be an existing absolute local file")
    if image_path.stat().st_size <= 0:
        raise RunVideoOnceError("input_validation_failed", "Image file is empty")
    mime_type = mimetypes.guess_type(str(image_path))[0]
    if mime_type not in SUPPORTED_MIME_TYPES:
        raise RunVideoOnceError("input_validation_failed", "Unsupported image MIME type")
    if not args.prompt or not args.prompt.strip():
        raise RunVideoOnceError("input_validation_failed", "Prompt must be non-empty")
    if args.duration not in SUPPORTED_DURATIONS:
        raise RunVideoOnceError("input_validation_failed", "Unsupported duration")
    if args.aspect_ratio not in SUPPORTED_ASPECT_RATIOS:
        raise RunVideoOnceError("input_validation_failed", "Unsupported aspect_ratio")
    if args.timeout_seconds <= 0:
        raise RunVideoOnceError("input_validation_failed", "timeout-seconds must be positive")
    return image_path


def _create_run_dir(base_dir: Path) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    for index in range(100):
        suffix = "" if index == 0 else f"-{index}"
        run_dir = base_dir / f"flow-video-once-{stamp}{suffix}"
        try:
            run_dir.mkdir()
            return run_dir
        except FileExistsError:
            continue
    raise RunVideoOnceError("input_validation_failed", "Unable to create unique run_dir")


def _find_free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _start_gateway(port: int, db_path: Path, log_path: Path) -> subprocess.Popen:
    env = os.environ.copy()
    env.update(
        {
            "GATEWAY_API_HOST": "127.0.0.1",
            "GATEWAY_API_PORT": str(port),
            "POOL_MAX_CONCURRENCY": "1",
            "POOL_DRY_RUN": "false",
            "CANARY_ONLY": "true",
            "CANARY_LIMIT": "1",
            "REAL_SUBMIT_MAX_ATTEMPTS": "1",
            "GATEWAY_WORKER_SUBMIT_TIMEOUT_SECONDS": "300",
            "FLOWKIT_GATEWAY_WORKER_SOURCE": "runtime_registry",
            "GATEWAY_DB_PATH": str(db_path),
        }
    )
    stdout = log_path.open("ab")
    return subprocess.Popen(
        [sys.executable, "-m", "gateway.main"],
        cwd=str(Path(__file__).resolve().parents[1]),
        env=env,
        stdout=stdout,
        stderr=subprocess.STDOUT,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def _wait_for_gateway(port: int, timeout_seconds: int = 30) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            health = _get_json(port, "/health", timeout=5)
            if health.get("status") == "ok":
                return
        except Exception as exc:
            last_error = str(exc)[:200]
        time.sleep(0.5)
    raise RunVideoOnceError("gateway_start_failed", f"Gateway /health did not become ready: {last_error}")


def _select_account(accounts: list[dict[str, Any]], preferred_account: str | None) -> dict[str, Any]:
    ready = [account for account in accounts if account.get("status") == "ready" and account.get("api_url")]
    if preferred_account:
        selected = next((account for account in ready if account.get("account_id") == preferred_account), None)
        if not selected:
            raise RunVideoOnceError("no_eligible_worker", "Preferred account is not eligible")
        return selected
    if not ready:
        raise RunVideoOnceError("no_eligible_worker", "No eligible runtime worker")
    return sorted(ready, key=lambda account: account["account_id"])[0]


def _wait_for_eligible_account(port: int, preferred_account: str | None, timeout_seconds: int = 60) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout_seconds
    accounts: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        accounts = _get_json(port, "/api/pool/accounts", timeout=10)
        try:
            _select_account(accounts, preferred_account)
            return accounts
        except RunVideoOnceError:
            time.sleep(1)
    _select_account(accounts, preferred_account)
    return accounts


def _create_project(worker_api_url: str, run_dir: Path) -> dict[str, Any]:
    payload = {
        "name": f"Flow Video Once {datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "language": "en",
        "material": "realistic",
        "allow_music": False,
        "allow_voice": False,
    }
    try:
        return _post_url_json(f"{worker_api_url}/api/projects", payload, timeout=120)
    except Exception as exc:
        raise RunVideoOnceError("project_create_failed", str(exc)[:500], {"run_dir": str(run_dir)}) from exc


def _wait_for_task(port: int, task_id: str, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_task: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last_task = _get_json(port, f"/api/pool/tasks/{task_id}", timeout=10)
        status = last_task.get("status")
        if status == "completed":
            return last_task
        if status == "manual_review":
            raise RunVideoOnceError(_failure_stage(last_task), "Gateway task entered manual_review", _task_details(last_task))
        time.sleep(10)
    raise RunVideoOnceError("generation_timeout", "Gateway task did not complete before timeout", _task_details(last_task))


def _validate_completed_task(task: dict[str, Any]) -> None:
    if task.get("attempt_count") != 1:
        raise RunVideoOnceError("worker_submit_failed", "Expected exactly one Worker submit attempt", _task_details(task))
    if not task.get("worker_job_id"):
        raise RunVideoOnceError("worker_submit_failed", "Completed task is missing worker_job_id", _task_details(task))
    if task.get("error_code") is not None or task.get("error_message") is not None:
        raise RunVideoOnceError("generation_failed", "Completed task retained an error", _task_details(task))
    video_path = Path(task.get("video_path") or "")
    if not video_path.exists() or video_path.stat().st_size <= 0:
        raise RunVideoOnceError("download_failed", "Completed task video_path is missing or empty", _task_details(task))
    with video_path.open("rb") as fh:
        header = fh.read(8)
    if len(header) < 8 or header[4:8] != b"ftyp":
        raise RunVideoOnceError("download_failed", "Completed task video is not a valid MP4 ftyp file", _task_details(task))


def _failure_stage(task: dict[str, Any]) -> str:
    code = str(task.get("error_code") or "").lower()
    if "download" in code or "video_path" in code or "url" in code:
        return "download_failed"
    if "submit" in code or "http" in code or "timeout" in code or "worker" in code:
        return "worker_submit_failed"
    return "generation_failed"


def _task_details(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "gateway_task_id": task.get("task_id"),
        "assigned_account_id": task.get("assigned_account_id"),
        "worker_job_id": task.get("worker_job_id"),
        "attempt_count": task.get("attempt_count"),
        "status": task.get("status"),
        "error_code": task.get("error_code"),
        "error_message": (task.get("error_message") or "")[:500] or None,
        "video_path": task.get("video_path"),
    }


def _get_json(port: int, path: str, timeout: int) -> Any:
    return _request_json("GET", f"http://127.0.0.1:{port}{path}", None, timeout)


def _post_json(port: int, path: str, payload: dict[str, Any], timeout: int) -> Any:
    return _post_url_json(f"http://127.0.0.1:{port}{path}", payload, timeout)


def _post_url_json(url: str, payload: dict[str, Any], timeout: int) -> Any:
    return _request_json("POST", url, payload, timeout)


def _request_json(method: str, url: str, payload: dict[str, Any] | None, timeout: int) -> Any:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=timeout) as response:
            body = response.read()
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        raise RunVideoOnceError("gateway_task_create_failed", f"HTTP {exc.code}: {body}") from exc
    return json.loads(body.decode("utf-8"))


def _stop_gateway(process: subprocess.Popen) -> bool:
    if process.poll() is not None:
        return True
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)
    return process.poll() is not None


if __name__ == "__main__":
    raise SystemExit(main())
