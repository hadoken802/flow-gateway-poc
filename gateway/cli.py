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
    batch = subparsers.add_parser("run-storyboard-batch")
    batch.add_argument("--manifest", required=True)
    batch.add_argument("--concurrency", type=int, default=3)
    batch.add_argument("--output-dir")
    batch.add_argument("--timeout-seconds", type=int, default=1200)
    batch.add_argument("--gateway-port", type=int)
    batch.add_argument("--gateway-db")
    batch.add_argument("--legacy-run-db", action="store_true")
    batch.add_argument("--test-mode", action="store_true")
    batch.add_argument("--account-ids")
    batch.add_argument("--preflight-only", action="store_true")
    reconcile = subparsers.add_parser("reconcile-existing-run")
    reconcile.add_argument("--run-dir", required=True)
    reconcile.add_argument("--output-run-dir")
    smoke = subparsers.add_parser("gateway-startup-smoke")
    smoke.add_argument("--account-ids", required=True)
    smoke.add_argument("--output-dir", required=True)
    smoke.add_argument("--timeout-seconds", type=int, default=60)
    retry_downloads = subparsers.add_parser("retry-downloads")
    retry_downloads.add_argument("--run-dir", required=True)
    retry_downloads.add_argument("--account-id", action="append", default=[])
    retry_downloads.add_argument("--execute", action="store_true")
    resume = subparsers.add_parser("resume-manual-submit")
    resume.add_argument("--task-id", required=True)
    resume.add_argument("--gateway-db", required=True)
    resume.add_argument("--preflight-only", action="store_true")
    resume.add_argument("--execute", action="store_true")
    resume.add_argument("--confirm-task-id")
    resume.add_argument("--user-confirmed-verification-cleared", action="store_true")
    reconcile_unknown = subparsers.add_parser("reconcile-submission-unknown")
    reconcile_unknown.add_argument("--task-id", required=True)
    reconcile_unknown.add_argument("--gateway-db", required=True)
    reconcile_unknown.add_argument("--execute", action="store_true")
    reconcile_unknown.add_argument("--confirm-task-id")
    resolve_unknown = subparsers.add_parser("resolve-submission-unknown")
    resolve_unknown.add_argument("--task-id", required=True)
    resolve_unknown.add_argument("--resolution", required=True, choices=["confirmed-rejected", "confirmed-not-started"])
    resolve_unknown.add_argument("--gateway-db", required=True)
    resolve_unknown.add_argument("--execute", action="store_true")
    resolve_unknown.add_argument("--confirm-task-id")
    args = parser.parse_args(argv)

    if args.command == "run-video-once":
        result = run_video_once(args)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1
    if args.command == "run-storyboard-batch":
        from .storyboard_batch import run_storyboard_batch
        result = run_storyboard_batch(args)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1
    if args.command == "reconcile-existing-run":
        from .reconcile import reconcile_existing_run
        result = reconcile_existing_run(args)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1
    if args.command == "gateway-startup-smoke":
        result = gateway_startup_smoke(args)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1
    if args.command == "retry-downloads":
        from .download_recovery import retry_downloads
        result = retry_downloads(args)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1
    if args.command == "resume-manual-submit":
        from .manual_submit_resume import resume_manual_submit
        if not args.execute:
            args.preflight_only = True
        result = resume_manual_submit(args)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1
    if args.command == "reconcile-submission-unknown":
        from .submission_reconcile import reconcile_submission_unknown
        result = reconcile_submission_unknown(args)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1
    if args.command == "resolve-submission-unknown":
        from .submission_reconcile import resolve_submission_unknown
        result = resolve_submission_unknown(args)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("ok") else 1
    return 2


def gateway_startup_smoke(args) -> dict[str, Any]:
    from .storyboard_batch import _cleanup_lock, _flow_task_count, _start_gateway_for_batch, parse_account_ids, run_preflight

    account_ids = parse_account_ids(args.account_ids)
    run_dir = _create_run_dir(Path(args.output_dir))
    db_path = run_dir / "gateway.db"
    log_path = run_dir / "gateway.log"
    port = _find_free_local_port()
    credits_before = run_preflight([], account_ids, run_dir)
    process = None
    health_payload: dict[str, Any] | None = None
    health_http_status = None
    lock_payload: dict[str, Any] = {}
    shutdown_completed = False
    try:
        process = _start_gateway_for_batch(port, db_path, log_path, concurrency=max(1, min(3, len(account_ids) or 1)), test_mode=False, account_ids=account_ids)
        _wait_for_gateway(port, timeout_seconds=int(args.timeout_seconds))
        health_payload = _get_json(port, "/health", timeout=10)
        health_http_status = 200
        lock_path = db_path.with_suffix(db_path.suffix + ".lock")
        if lock_path.exists():
            lock_payload = json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception as exc:
        health_payload = {"error": str(exc)[:500]}
    finally:
        if process is not None:
            shutdown_completed = _stop_gateway(process)
            _cleanup_lock(db_path)
    credits_after = run_preflight([], account_ids, run_dir)
    current_task_ids = {
        account["account_id"]: account.get("current_task_id")
        for account in credits_after.get("accounts", [])
        if account.get("account_id") in account_ids
    }
    result = {
        "ok": health_http_status == 200 and _flow_task_count(db_path) == 0 and shutdown_completed,
        "run_dir": str(run_dir),
        "python_executable": sys.executable,
        "launcher_pid": os.getpid(),
        "server_pid": lock_payload.get("server_pid"),
        "gateway_port": port,
        "database_path": str(db_path),
        "health_http_status": health_http_status,
        "health_payload": health_payload,
        "startup_completed": health_http_status == 200,
        "shutdown_completed": shutdown_completed,
        "flow_task_count": _flow_task_count(db_path),
        "project_create_call_count": 0,
        "worker_submit_call_count": 0,
        "current_task_ids": current_task_ids,
        "credits_before": _credits_by_account(credits_before),
        "credits_after": _credits_by_account(credits_after),
        "gateway_exit_code": process.poll() if process is not None else None,
        "lock_cleaned": not db_path.with_suffix(db_path.suffix + ".lock").exists(),
    }
    (run_dir / "gateway-startup-smoke-result.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def _credits_by_account(preflight: dict[str, Any]) -> dict[str, int | None]:
    return {
        account["account_id"]: account.get("credits")
        for account in preflight.get("accounts", [])
        if account.get("account_id")
    }


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
