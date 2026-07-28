"""Storyboard batch runner for Gateway-managed Flow video tasks."""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import cli
from .config import DEFAULT_GATEWAY_DB_PATH
from .worker_client import WorkerClient
from .worker_provider import RuntimeRegistryWorkerProvider


SUPPORTED_MIME_TYPES = {"image/png", "image/jpeg", "image/webp"}
SUPPORTED_ASPECT_RATIOS = {"9:16", "16:9"}


class StoryboardBatchError(Exception):
    def __init__(self, stage: str, message: str):
        super().__init__(message)
        self.stage = stage


@dataclass(frozen=True)
class StoryboardShot:
    shot_id: str
    image: str
    prompt: str
    duration: int
    aspect_ratio: str
    idempotency_key: str | None = None


def run_storyboard_batch(args: argparse.Namespace) -> dict[str, Any]:
    gateway_process = None
    run_dir = None
    port = int(args.gateway_port) if getattr(args, "gateway_port", None) else cli._find_free_local_port()
    try:
        shots = load_manifest(Path(args.manifest))
        account_ids = parse_account_ids(getattr(args, "account_ids", None))
        concurrency = _validate_concurrency(int(getattr(args, "concurrency", 3)))
        timeout_seconds = int(getattr(args, "timeout_seconds", 1200))
        if timeout_seconds <= 0:
            raise StoryboardBatchError("input_validation_failed", "timeout-seconds must be positive")
        run_dir = cli._create_run_dir(Path(args.output_dir) if getattr(args, "output_dir", None) else cli.DEFAULT_RUN_ROOT)
        preflight = run_preflight(shots, account_ids, run_dir)
        if getattr(args, "preflight_only", False):
            return preflight
        if account_ids and not preflight.get("ok"):
            return preflight
        db_path = _gateway_db_path(args, run_dir)
        log_path = run_dir / "gateway.log"
        gateway_process = _start_gateway_for_batch(port, db_path, log_path, concurrency, bool(getattr(args, "test_mode", False)), account_ids)
        try:
            cli._wait_for_gateway(port, timeout_seconds=int(getattr(args, "gateway_startup_timeout_seconds", 60)))
        except Exception as exc:
            result = _gateway_start_failed_result(run_dir, port, gateway_process, db_path, exc)
            result.update(_account_summary(preflight))
            result["gateway_stopped"] = _stop_gateway(gateway_process)
            result["gateway_exit_code"] = gateway_process.poll()
            gateway_process = None
            _cleanup_lock(db_path)
            _write_result(run_dir, result)
            return result
        tasks = _post_tasks(port, shots)
        final_tasks = _wait_for_tasks(port, tasks, timeout_seconds)
        result = _result(run_dir, port, gateway_process.pid if gateway_process else None, shots, final_tasks)
        result.update(_account_summary(preflight))
        result["gateway_stopped"] = _stop_gateway(gateway_process)
        gateway_process = None
        _write_result(run_dir, result)
        return result
    except StoryboardBatchError as exc:
        result = {
            "ok": False,
            "stage": exc.stage,
            "error_message": _safe(str(exc)),
            "run_dir": str(run_dir) if run_dir else None,
            "gateway_port": port,
            "gateway_stopped": False,
        }
        if run_dir:
            _write_result(run_dir, result)
        return result
    except Exception as exc:
        result = {
            "ok": False,
            "result": "gateway_start_failed",
            "stage": "gateway_startup",
            "error_code": "gateway_start_failed",
            "error_message": _safe(str(exc)),
            "run_dir": str(run_dir) if run_dir else None,
            "gateway_port": port,
            "gateway_stopped": False,
        }
        if run_dir:
            _write_result(run_dir, result)
        return result
    finally:
        if gateway_process is not None:
            _stop_gateway(gateway_process)


def load_manifest(path: Path) -> list[StoryboardShot]:
    if not path.is_absolute() or not path.exists() or not path.is_file():
        raise StoryboardBatchError("input_validation_failed", "manifest must be an existing absolute JSON file")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list) or not data:
        raise StoryboardBatchError("input_validation_failed", "manifest must contain at least one shot")
    seen: set[str] = set()
    shots = []
    for index, item in enumerate(data, start=1):
        shot_id = str(item.get("shot_id") or "").strip()
        if not shot_id:
            raise StoryboardBatchError("input_validation_failed", "shot_id must be non-empty")
        if shot_id in seen:
            raise StoryboardBatchError("input_validation_failed", f"duplicate shot_id: {shot_id}")
        seen.add(shot_id)
        image = Path(str(item.get("image") or ""))
        if not image.is_absolute() or not image.exists() or not image.is_file():
            raise StoryboardBatchError("input_validation_failed", f"image not found: {shot_id}")
        if image.stat().st_size <= 0:
            raise StoryboardBatchError("input_validation_failed", f"image is empty: {shot_id}")
        if mimetypes.guess_type(str(image))[0] not in SUPPORTED_MIME_TYPES:
            raise StoryboardBatchError("input_validation_failed", f"unsupported image type: {shot_id}")
        prompt = str(item.get("prompt") or "").strip()
        if not prompt:
            raise StoryboardBatchError("input_validation_failed", f"prompt must be non-empty: {shot_id}")
        duration = int(item.get("duration", 10))
        if duration != 10:
            raise StoryboardBatchError("input_validation_failed", "duration currently must be 10")
        aspect_ratio = str(item.get("aspect_ratio") or "")
        if aspect_ratio not in SUPPORTED_ASPECT_RATIOS:
            raise StoryboardBatchError("input_validation_failed", "aspect_ratio must be 9:16 or 16:9")
        shots.append(StoryboardShot(shot_id, str(image), prompt, duration, aspect_ratio))
    return shots


def _validate_concurrency(value: int) -> int:
    if value < 1 or value > 3:
        raise StoryboardBatchError("input_validation_failed", "concurrency must be 1, 2, or 3")
    return value


def parse_account_ids(value: str | None) -> list[str]:
    seen = set()
    result = []
    for item in (value or "").split(","):
        account_id = item.strip()
        if account_id and account_id not in seen:
            seen.add(account_id)
            result.append(account_id)
    return result


def run_preflight(shots: list[StoryboardShot], account_ids: list[str], run_dir: Path) -> dict[str, Any]:
    provider = RuntimeRegistryWorkerProvider()
    snapshot = provider.load_workers()
    workers = {worker.account_id: worker for worker in snapshot.workers}
    candidates = {candidate["account_id"]: candidate for candidate in snapshot.candidates}
    client = WorkerClient()
    requested = list(account_ids)
    target_ids = requested or sorted(workers)
    accounts = []
    eligible = []
    excluded: dict[str, list[str]] = {}
    required_credits = 15
    missing = [account_id for account_id in requested if account_id not in candidates]
    for account_id in target_ids:
        candidate = candidates.get(account_id, {"account_id": account_id, "exclusion_reasons": ["account_not_found"]})
        worker = workers.get(account_id)
        reasons = list(candidate.get("exclusion_reasons") or [])
        credits = None
        credits_http_status = None
        if not worker:
            reasons.append("not_ready_worker")
        else:
            try:
                info = _run_async(client.inspect(worker))
                credits_http_status = 200
                credits = info.get("credits")
                if info.get("status") == "offline":
                    reasons.append("worker_offline")
                if not info.get("extension_connected"):
                    reasons.append("extension_not_connected")
                if not info.get("flow_key_present"):
                    reasons.append("flow_key_missing")
            except Exception as exc:
                credits_http_status = None
                reasons.append(type(exc).__name__)
        enough = isinstance(credits, int) and credits >= required_credits
        if not enough:
            reasons.append("insufficient_credits")
        if candidate.get("current_task_id"):
            reasons.append("account_busy")
        deduped = []
        for reason in reasons:
            if reason not in deduped:
                deduped.append(reason)
        eligible_for_batch = not deduped and bool(worker)
        if eligible_for_batch:
            eligible.append(account_id)
        else:
            excluded[account_id] = deduped
        accounts.append({
            "account_id": account_id,
            "worker_api_endpoint": candidate.get("worker_api_endpoint") or (worker.api_url if worker else None),
            "runtime_instance_id": candidate.get("runtime_instance_id") or (worker.runtime_instance_id if worker else None),
            "registration_status": candidate.get("registration_status"),
            "runtime_status": candidate.get("runtime_status"),
            "runtime_healthy": bool(candidate.get("runtime_healthy")),
            "extension_ready": bool(candidate.get("extension_ready")),
            "account_match": bool(candidate.get("account_match")),
            "ownership_verified": bool(candidate.get("ownership_verified")),
            "worker_health_reachable": bool(candidate.get("worker_health_reachable")),
            "credits_http_status": credits_http_status,
            "credits": credits,
            "required_credits": required_credits,
            "enough_credits": enough,
            "current_task_id": candidate.get("current_task_id"),
            "eligible_for_batch": eligible_for_batch,
            "exclusion_reasons": deduped,
        })
    if requested:
        for account_id in sorted(set(candidates) - set(requested)):
            excluded[account_id] = ["excluded_by_allowlist"]
            accounts.append({
                "account_id": account_id,
                "excluded_by_allowlist": True,
                "eligible_for_batch": False,
                "exclusion_reasons": ["excluded_by_allowlist"],
            })
    ok = not missing and (not requested or set(eligible) == set(requested))
    result = {
        "ok": ok,
        "result": "preflight_passed" if ok else "preflight_failed",
        "run_dir": str(run_dir),
        "shot_count": len(shots),
        "requested_account_ids": requested,
        "eligible_account_ids": eligible,
        "excluded_account_ids": list(excluded),
        "excluded_reasons": excluded,
        "accounts": accounts,
        "gateway_started": False,
        "project_create_call_count": 0,
        "worker_submit_call_count": 0,
    }
    (run_dir / "preflight-result.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def _run_async(coro):
    import asyncio

    return asyncio.run(coro)


def _account_summary(preflight: dict[str, Any]) -> dict[str, Any]:
    return {
        "requested_account_ids": preflight.get("requested_account_ids", []),
        "eligible_account_ids": preflight.get("eligible_account_ids", []),
        "excluded_account_ids": preflight.get("excluded_account_ids", []),
        "excluded_reasons": preflight.get("excluded_reasons", {}),
    }


def _gateway_db_path(args: argparse.Namespace, run_dir: Path) -> Path:
    explicit = getattr(args, "gateway_db", None)
    if explicit:
        return Path(explicit)
    if getattr(args, "legacy_run_db", False):
        warning = "WARNING: --legacy-run-db does not provide cross-batch global account locks."
        print(warning, file=sys.stderr)
        return run_dir / "gateway.db"
    return DEFAULT_GATEWAY_DB_PATH


def _start_gateway_for_batch(port: int, db_path: Path, log_path: Path, concurrency: int, test_mode: bool, account_ids: list[str] | None = None):
    import os
    import subprocess
    import sys

    env = os.environ.copy()
    env.update({
        "GATEWAY_API_HOST": "127.0.0.1",
        "GATEWAY_API_PORT": str(port),
        "POOL_MAX_CONCURRENCY": str(concurrency),
        "POOL_DRY_RUN": "true" if test_mode else "false",
        "CANARY_ONLY": "false",
        "REAL_SUBMIT_MAX_ATTEMPTS": "1",
        "GATEWAY_WORKER_SUBMIT_TIMEOUT_SECONDS": "300",
        "FLOWKIT_GATEWAY_WORKER_SOURCE": "runtime_registry",
        "GATEWAY_DB_PATH": str(db_path),
        "GATEWAY_LAUNCHER_PID": str(os.getpid()),
        "GATEWAY_ALLOWED_ACCOUNT_IDS": ",".join(account_ids or []),
        "GATEWAY_STARTUP_TIMEOUT_SECONDS": "60",
    })
    stdout = log_path.open("ab")
    _append_launcher_event(log_path, {
        "event": "gateway_launcher_started",
        "launcher_pid": os.getpid(),
        "python_executable": sys.executable,
        "command": [sys.executable, "-m", "gateway.main"],
        "cwd": str(Path(__file__).resolve().parents[1]),
        "gateway_port": port,
        "database_path": str(db_path),
        "allowed_account_ids": account_ids or [],
    })
    return subprocess.Popen(
        [sys.executable, "-m", "gateway.main"],
        cwd=str(Path(__file__).resolve().parents[1]),
        env=env,
        stdout=stdout,
        stderr=subprocess.STDOUT,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def _append_launcher_event(log_path: Path, payload: dict[str, Any]) -> None:
    with log_path.open("ab") as fh:
        fh.write(("gateway_audit " + json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"))


def _gateway_start_failed_result(run_dir: Path, port: int, process, db_path: Path, exc: Exception) -> dict[str, Any]:
    return {
        "ok": False,
        "result": "gateway_start_failed",
        "stage": "gateway_startup",
        "error_code": "gateway_start_failed",
        "error_message": _safe(str(exc)),
        "run_dir": str(run_dir),
        "gateway_port": port,
        "gateway_pid": process.pid if process else None,
        "database_path": str(db_path),
        "flow_task_count": _flow_task_count(db_path),
        "project_create_call_count": 0,
        "worker_submit_call_count": 0,
    }


def _flow_task_count(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    import sqlite3

    with sqlite3.connect(db_path) as db:
        try:
            return int(db.execute("SELECT count(*) FROM flow_tasks").fetchone()[0])
        except sqlite3.Error:
            return 0


def _cleanup_lock(db_path: Path) -> None:
    lock_path = db_path.with_suffix(db_path.suffix + ".lock")
    try:
        if lock_path.exists():
            lock_path.unlink()
    except OSError:
        pass


def _post_tasks(port: int, shots: list[StoryboardShot]) -> list[dict[str, Any]]:
    payloads = [{
        "image_path": shot.image,
        "prompt": shot.prompt,
        "duration": shot.duration,
        "aspect_ratio": shot.aspect_ratio,
    } for shot in shots]
    response = cli._post_json(port, "/api/pool/tasks/batch", {"tasks": payloads}, timeout=30)
    return response["tasks"]


def _wait_for_tasks(port: int, tasks: list[dict[str, Any]], timeout_seconds: int) -> list[dict[str, Any]]:
    deadlines = {task["task_id"]: time.monotonic() + timeout_seconds for task in tasks}
    final: dict[str, dict[str, Any]] = {}
    while len(final) < len(tasks):
        for task in cli._get_json(port, "/api/pool/tasks", timeout=10):
            task_id = task["task_id"]
            if task_id in final:
                continue
            if task["status"] in {"completed", "manual_review", "manual_submit_required", "failed"}:
                final[task_id] = task
            elif time.monotonic() > deadlines.get(task_id, 0):
                final[task_id] = {**task, "status": "manual_review", "error_code": "task_timeout", "error_message": "Task timeout"}
        time.sleep(10)
    return [final[task["task_id"]] for task in tasks]


def _result(run_dir: Path, port: int, pid: int | None, shots: list[StoryboardShot], tasks: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    by_index = list(zip(shots, tasks))
    for shot, task in by_index:
        video_value = task.get("video_path")
        video_path = Path(video_value) if video_value else None
        rows.append({
            "shot_id": shot.shot_id,
            "task_id": task.get("task_id"),
            "project_id": task.get("project_id"),
            "assigned_account_id": task.get("assigned_account_id"),
            "assigned_runtime_instance_id": task.get("assigned_runtime_instance_id"),
            "worker_job_id": task.get("worker_job_id"),
            "attempt_count": task.get("attempt_count"),
            "status": task.get("status"),
            "error_code": task.get("error_code"),
            "error_message": _safe(task.get("error_message")),
            "video_path": str(video_path) if video_path else None,
            "video_size_bytes": video_path.stat().st_size if video_path and video_path.exists() else 0,
            "mp4_ftyp_valid": _mp4_ftyp_valid(video_path) if video_path else False,
            "manual_submit_required_at": task.get("manual_submit_required_at"),
            "manual_result_media_id": task.get("manual_result_media_id"),
            "manual_result_operation_id": task.get("manual_result_operation_id"),
            "manual_result_source": task.get("manual_result_source"),
        })
    ok = all(row["status"] == "completed" and row["mp4_ftyp_valid"] for row in rows)
    summary = _failure_summary(rows)
    return {
        "ok": ok,
        **({} if ok else summary),
        "run_dir": str(run_dir),
        "gateway_port": port,
        "gateway_pid": pid,
        "task_count": len(rows),
        "project_count": sum(1 for row in rows if row["project_id"]),
        "tasks": rows,
        "flow_runtime_kept_running": True,
    }


def _failure_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    failed = [row for row in rows if row.get("status") != "completed" or not row.get("mp4_ftyp_valid")]
    if any(row.get("status") == "manual_submit_required" for row in failed):
        return {
            "stage": "worker_submit",
            "error_code": "UPSTREAM_UNUSUAL_ACTIVITY",
            "error_message": "One or more tasks require manual submission",
        }
    first = failed[0] if failed else {}
    return {
        "stage": _stage_for_error(first),
        "error_code": first.get("error_code") or "task_failed",
        "error_message": first.get("error_message") or "One or more tasks failed",
    }


def _stage_for_error(row: dict[str, Any]) -> str:
    code = str(row.get("error_code") or "")
    if "project" in code:
        return "project_create"
    if "download" in code or "video" in code:
        return "task_download"
    if code:
        return "worker_submit"
    return "task"


def _mp4_ftyp_valid(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size <= 0:
            return False
        return path.read_bytes()[:8][4:8] == b"ftyp"
    except OSError:
        return False


def _safe(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)[:500]
    lowered = text.lower()
    for blocked in ("cookie", "token", "authorization", "secret", "nonce"):
        if blocked in lowered:
            return "[redacted]"
    return text


def _write_result(run_dir: Path, result: dict[str, Any]) -> None:
    (run_dir / "batch-result.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")


def _stop_gateway(process) -> bool:
    return cli._stop_gateway(process)
