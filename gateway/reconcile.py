"""Reconcile an existing Gateway run from already-submitted Worker jobs."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import request

from .storyboard_batch import _mp4_ftyp_valid
from .worker_provider import RuntimeRegistryWorkerProvider


def reconcile_existing_run(args: argparse.Namespace) -> dict[str, Any]:
    source_run_dir = Path(args.run_dir)
    if not source_run_dir.exists():
        return {"ok": False, "error_code": "run_dir_not_found"}
    output_run_dir = Path(args.output_run_dir) if getattr(args, "output_run_dir", None) else source_run_dir.parent / f"reconcile-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    output_run_dir.mkdir(parents=True, exist_ok=False)
    source_db = source_run_dir / "gateway.db"
    target_db = output_run_dir / "gateway.db"
    original_hashes_before = _hashes(source_run_dir)
    shutil.copy2(source_db, target_db)

    workers = {worker.account_id: worker for worker in RuntimeRegistryWorkerProvider().load_workers().workers}
    conn = sqlite3.connect(target_db)
    conn.row_factory = sqlite3.Row
    rows = [dict(row) for row in conn.execute("SELECT * FROM flow_tasks ORDER BY created_at, task_id")]
    result_rows = []
    project_create_calls = 0
    worker_submit_calls = 0
    for task in rows:
        worker = workers.get(task.get("assigned_account_id"))
        before = task.get("status")
        worker_status = None
        video_path = task.get("video_path")
        if task.get("worker_job_id") and worker:
            job = _get_json(f"{worker.api_url}/api/test/omni-video/{task['worker_job_id']}")
            worker_status = job.get("status")
            if worker_status in {"waiting_download", "completed_remote"}:
                job = _post_json(f"{worker.api_url}/api/test/omni-video/{task['worker_job_id']}/retry-download")
                worker_status = job.get("status")
            if worker_status == "completed":
                video_path = job.get("video_path")
                conn.execute(
                    """
                    UPDATE flow_tasks
                    SET status='completed', video_path=?, remaining_credits=?, error_code=NULL, error_message=NULL,
                        completed_at=COALESCE(completed_at, strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                        updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                    WHERE task_id=?
                    """,
                    (video_path, job.get("remaining_credits"), task["task_id"]),
                )
                conn.execute(
                    """
                    UPDATE flow_accounts
                    SET current_task_id=NULL,
                        credits=COALESCE(?, credits),
                        status=CASE WHEN COALESCE(?, credits) >= 15 THEN 'ready' ELSE 'low_credits' END
                    WHERE account_id=? AND (current_task_id IS NULL OR current_task_id=?)
                    """,
                    (job.get("remaining_credits"), job.get("remaining_credits"), task.get("assigned_account_id"), task["task_id"]),
                )
            elif worker_status in {"failed"}:
                conn.execute(
                    "UPDATE flow_tasks SET status='manual_review', error_code=?, error_message=? WHERE task_id=?",
                    (job.get("error_code") or "worker_failed", _safe(job.get("error_message")), task["task_id"]),
                )
        conn.commit()
        after = dict(conn.execute("SELECT * FROM flow_tasks WHERE task_id=?", (task["task_id"],)).fetchone())
        path = Path(after.get("video_path") or "") if after.get("video_path") else None
        result_rows.append({
            "shot_id": _shot_id(task.get("idempotency_key")),
            "task_id": task.get("task_id"),
            "assigned_account_id": task.get("assigned_account_id"),
            "assigned_runtime_instance_id": task.get("assigned_runtime_instance_id"),
            "project_id": task.get("project_id"),
            "worker_job_id": task.get("worker_job_id"),
            "worker_status": worker_status,
            "gateway_status_before": before,
            "gateway_status_after": after.get("status"),
            "video_path": str(path) if path else None,
            "video_size_bytes": path.stat().st_size if path and path.exists() else 0,
            "mp4_ftyp_valid": _mp4_ftyp_valid(path) if path else False,
            "project_create_called": False,
            "worker_submit_called": False,
        })
    conn.close()
    result = {
        "ok": all(row["gateway_status_after"] == "completed" and row["mp4_ftyp_valid"] for row in result_rows),
        "source_run_dir": str(source_run_dir),
        "run_dir": str(output_run_dir),
        "project_create_call_count": project_create_calls,
        "worker_submit_call_count": worker_submit_calls,
        "tasks": result_rows,
        "source_hashes_before": original_hashes_before,
        "source_hashes_after": _hashes(source_run_dir),
    }
    (output_run_dir / "batch-result-reconciled.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def _get_json(url: str) -> dict[str, Any]:
    with request.urlopen(url, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _post_json(url: str) -> dict[str, Any]:
    req = request.Request(url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
    with request.urlopen(req, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def _hashes(run_dir: Path) -> dict[str, str | None]:
    result = {}
    for name in ("gateway.db", "batch-result.json"):
        path = run_dir / name
        result[name] = _sha256(path) if path.exists() else None
    return result


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _shot_id(idempotency_key: str | None) -> str | None:
    if not idempotency_key:
        return None
    parts = idempotency_key.split("-")
    return parts[1] if len(parts) >= 3 and parts[0] == "storyboard" else None


def _safe(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)[:500]
    lowered = text.lower()
    if any(word in lowered for word in ("cookie", "token", "authorization", "secret", "nonce")):
        return "[redacted]"
    return text
