"""Storyboard batch runner for Gateway-managed Flow video tasks."""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import cli


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
    idempotency_key: str


def run_storyboard_batch(args: argparse.Namespace) -> dict[str, Any]:
    gateway_process = None
    run_dir = None
    port = int(args.gateway_port) if getattr(args, "gateway_port", None) else cli._find_free_local_port()
    try:
        shots = load_manifest(Path(args.manifest))
        concurrency = _validate_concurrency(int(getattr(args, "concurrency", 3)))
        timeout_seconds = int(getattr(args, "timeout_seconds", 1200))
        if timeout_seconds <= 0:
            raise StoryboardBatchError("input_validation_failed", "timeout-seconds must be positive")
        run_dir = cli._create_run_dir(Path(args.output_dir) if getattr(args, "output_dir", None) else cli.DEFAULT_RUN_ROOT)
        db_path = run_dir / "gateway.db"
        log_path = run_dir / "gateway.log"
        gateway_process = _start_gateway_for_batch(port, db_path, log_path, concurrency, bool(getattr(args, "test_mode", False)))
        cli._wait_for_gateway(port)
        tasks = _post_tasks(port, shots)
        final_tasks = _wait_for_tasks(port, tasks, timeout_seconds)
        result = _result(run_dir, port, gateway_process.pid if gateway_process else None, shots, final_tasks)
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
        shots.append(StoryboardShot(shot_id, str(image), prompt, duration, aspect_ratio, item.get("idempotency_key") or f"storyboard-{shot_id}-{index}"))
    return shots


def _validate_concurrency(value: int) -> int:
    if value < 1 or value > 3:
        raise StoryboardBatchError("input_validation_failed", "concurrency must be 1, 2, or 3")
    return value


def _start_gateway_for_batch(port: int, db_path: Path, log_path: Path, concurrency: int, test_mode: bool):
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
    })
    stdout = log_path.open("ab")
    return subprocess.Popen(
        [sys.executable, "-m", "gateway.main"],
        cwd=str(Path(__file__).resolve().parents[1]),
        env=env,
        stdout=stdout,
        stderr=subprocess.STDOUT,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def _post_tasks(port: int, shots: list[StoryboardShot]) -> list[dict[str, Any]]:
    payloads = [{
        "idempotency_key": shot.idempotency_key,
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
            if task["status"] in {"completed", "manual_review", "failed"}:
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
        })
    return {
        "ok": all(row["status"] == "completed" and row["mp4_ftyp_valid"] for row in rows),
        "run_dir": str(run_dir),
        "gateway_port": port,
        "gateway_pid": pid,
        "task_count": len(rows),
        "project_count": sum(1 for row in rows if row["project_id"]),
        "tasks": rows,
        "flow_runtime_kept_running": True,
    }


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
