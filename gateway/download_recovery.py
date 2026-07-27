"""Retry download for existing Gateway tasks without resubmitting generation."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from .scheduler import _valid_local_mp4
from .worker_client import WorkerClient


TERMINAL_DOWNLOAD_STATUSES = {"completed", "completed_remote", "waiting_download", "download_failed", "manual_review"}


def retry_downloads(args, worker_client: WorkerClient | None = None) -> dict[str, Any]:
    run_dir = Path(args.run_dir).resolve()
    db_path = run_dir / "gateway.db"
    account_ids = _stable_account_ids(getattr(args, "account_id", None) or [])
    execute = bool(getattr(args, "execute", False))
    if not db_path.exists():
        return _result(False, run_dir, execute, [], "input_validation_failed", "gateway.db not found")
    if not account_ids:
        return _result(False, run_dir, execute, [], "input_validation_failed", "at least one --account-id is required")

    client = worker_client or WorkerClient()
    rows = _load_rows(db_path, account_ids)
    plans = []
    for row in rows:
        plans.append(_plan_row(row))
    if execute and any(not row["allowed"] for row in plans):
        return _result(False, run_dir, execute, plans, "preflight_failed", "One or more tasks are not eligible for retry-download")
    if execute:
        for plan in plans:
            if plan["skip_reason"] == "valid_mp4_exists":
                continue
            worker = SimpleNamespace(api_url=plan["worker_api_endpoint"], account_id=plan["assigned_account_id"])
            response = asyncio.run(client.retry_omni_video_download(worker, plan["worker_job_id"]))
            plan["download_attempted"] = True
            plan["worker_status_after"] = response.get("status")
            plan["video_path_after"] = response.get("video_path")
            check = _valid_local_mp4(response.get("video_path"))
            plan["mp4_ftyp_valid"] = bool(check["ok"])
            plan["video_size_bytes"] = _video_size(response.get("video_path"))
            if check["ok"]:
                _mark_completed(db_path, plan, response)
                plan["gateway_status_after"] = "completed"
            else:
                _mark_download_failed(db_path, plan["task_id"], check["error_code"], check["error_message"])
                plan["gateway_status_after"] = "download_failed"
                plan["error_code"] = check["error_code"]
                plan["error_message"] = check["error_message"]
    result = _result(
        all(item.get("allowed") and (not execute or item.get("skip_reason") == "valid_mp4_exists" or item.get("mp4_ftyp_valid")) for item in plans),
        run_dir,
        execute,
        plans,
    )
    (run_dir / ("retry-downloads-result.json" if execute else "retry-downloads-dry-run.json")).write_text(
        json.dumps(result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return result


def _load_rows(db_path: Path, account_ids: list[str]) -> list[dict]:
    placeholders = ",".join("?" for _ in account_ids)
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        return [
            dict(row) for row in db.execute(
                f"""
                SELECT t.*, a.api_url AS worker_api_endpoint
                FROM flow_tasks t
                LEFT JOIN flow_accounts a ON a.account_id=t.assigned_account_id
                WHERE t.assigned_account_id IN ({placeholders})
                ORDER BY t.created_at, t.task_id
                """,
                tuple(account_ids),
            ).fetchall()
        ]


def _plan_row(row: dict) -> dict:
    check = _valid_local_mp4(row.get("video_path"))
    allowed = True
    reason = None
    if check["ok"]:
        reason = "valid_mp4_exists"
    elif not row.get("worker_job_id"):
        allowed, reason = False, "missing_worker_job_id"
    elif not row.get("project_id"):
        allowed, reason = False, "missing_project_id"
    elif not row.get("worker_api_endpoint"):
        allowed, reason = False, "missing_worker_api_endpoint"
    elif row.get("status") not in TERMINAL_DOWNLOAD_STATUSES:
        allowed, reason = False, f"unsupported_status:{row.get('status')}"
    return {
        "task_id": row.get("task_id"),
        "assigned_account_id": row.get("assigned_account_id"),
        "assigned_runtime_instance_id": row.get("assigned_runtime_instance_id"),
        "project_id": row.get("project_id"),
        "worker_job_id": row.get("worker_job_id"),
        "worker_api_endpoint": row.get("worker_api_endpoint"),
        "gateway_status_before": row.get("status"),
        "video_path_before": row.get("video_path"),
        "allowed": allowed,
        "skip_reason": reason,
        "download_attempted": False,
        "mp4_ftyp_valid": bool(check["ok"]),
        "video_size_bytes": _video_size(row.get("video_path")),
    }


def _mark_completed(db_path: Path, plan: dict, response: dict) -> None:
    with sqlite3.connect(db_path) as db:
        db.execute(
            """
            UPDATE flow_tasks
            SET status='completed', video_path=?, remaining_credits=?, error_code=NULL, error_message=NULL,
                completed_at=COALESCE(completed_at, strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=? AND assigned_account_id=? AND project_id=? AND worker_job_id=?
            """,
            (
                response.get("video_path"),
                response.get("remaining_credits"),
                plan["task_id"],
                plan["assigned_account_id"],
                plan["project_id"],
                plan["worker_job_id"],
            ),
        )
        db.commit()


def _mark_download_failed(db_path: Path, task_id: str, error_code: str, error_message: str) -> None:
    with sqlite3.connect(db_path) as db:
        db.execute(
            """
            UPDATE flow_tasks
            SET status='download_failed', error_code=?, error_message=?,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=?
            """,
            (error_code, error_message, task_id),
        )
        db.commit()


def _stable_account_ids(values) -> list[str]:
    seen = set()
    result = []
    for value in values:
        for item in str(value).split(","):
            account_id = item.strip()
            if account_id and account_id not in seen:
                seen.add(account_id)
                result.append(account_id)
    return result


def _video_size(video_path) -> int:
    if not video_path:
        return 0
    try:
        path = Path(video_path)
        return path.stat().st_size if path.exists() else 0
    except OSError:
        return 0


def _result(ok: bool, run_dir: Path, execute: bool, tasks: list[dict], error_code: str | None = None, error_message: str | None = None) -> dict:
    return {
        "ok": ok,
        "run_dir": str(run_dir),
        "execute": execute,
        "task_count": len(tasks),
        "download_attempt_count": sum(1 for item in tasks if item.get("download_attempted")),
        "project_create_call_count": 0,
        "gateway_task_create_count": 0,
        "worker_submit_call_count": 0,
        "credits_consumed": 0,
        "error_code": error_code,
        "error_message": error_message,
        "tasks": tasks,
    }
