"""Dry-run-first one-shot polling for reconciled remote Flow results."""
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse, parse_qsl

from .worker_client import WorkerClient


GATEWAY_STATUS = "reconciled_remote_accepted_unknown"
AGENT_STATUS = "remote_reconciled_bound"


def poll_reconciled_remote_result(args: argparse.Namespace, worker_client: Any | None = None) -> dict[str, Any]:
    return asyncio.run(_poll_reconciled_remote_result(args, worker_client=worker_client))


async def _poll_reconciled_remote_result(args: argparse.Namespace, worker_client: Any | None = None) -> dict[str, Any]:
    execute = bool(getattr(args, "execute", False))
    mode = "execute" if execute else "dry_run"
    gateway_db = Path(args.gateway_db)
    agent_db = Path(args.agent_db)
    gw = _connect_ro(gateway_db)
    agent = _connect_ro(agent_db)
    try:
        state = _read_state(gw, agent, args.task_id)
        validation = _validate_state(args, state)
        blockers = list(validation["blocking_reasons"])
        if execute:
            blockers.extend(_execute_blockers(args, state))
        result = {
            "ok": not blockers or not execute,
            "mode": mode,
            "task_id": args.task_id,
            "account_id": (state.get("gateway_account") or {}).get("account_id"),
            "job_id": (state.get("agent_job") or {}).get("job_id"),
            "current_gateway_state": state.get("gateway_task"),
            "current_agent_state": state.get("agent_job"),
            "project_id": (state.get("gateway_task") or {}).get("project_id"),
            "workflow_id": (state.get("gateway_task") or {}).get("workflow_id"),
            "output_media_id": (state.get("gateway_task") or {}).get("output_media_id"),
            "upstream_batch_id": (state.get("gateway_task") or {}).get("upstream_batch_id"),
            "poll_identifier": (state.get("gateway_task") or {}).get("output_media_id"),
            "candidate_validation": validation,
            "fencing_validation": validation,
            "worker_capability": _worker_capability(args, execute),
            "planned_remote_call": _planned_remote_call(args, state),
            "planned_database_changes": [],
            "submit_called": False,
            "poll_called": False,
            "download_called": False,
            "writes_performed": False,
            "remote_query_call_count": 0,
            "allowed_to_execute": not blockers,
            "blocking_reasons": blockers,
        }
        if not execute or blockers:
            return result
    finally:
        gw.close()
        agent.close()

    query_result = await _execute_one_remote_query(args, state, worker_client)
    result.update(
        {
            "ok": bool(query_result.get("query_ok")) or query_result.get("remote_state") in {"remote_completed", "remote_processing", "remote_failed", "remote_not_found", "remote_unknown", "remote_query_error"},
            "remote_query": query_result,
            "remote_query_call_count": query_result.get("remote_query_call_count", 1),
            "poll_called": True,
            "submit_called": False,
            "download_called": False,
            "writes_performed": False,
        }
    )
    result["remote_state"] = query_result.get("remote_state")
    if getattr(args, "result_file", None):
        _write_result_file(Path(args.result_file), result)
    return result


def _connect_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _read_state(gw: sqlite3.Connection, agent: sqlite3.Connection, task_id: str) -> dict[str, Any]:
    task = _fetchone(gw, "SELECT * FROM flow_tasks WHERE task_id=?", (task_id,))
    account = None
    job = None
    if task:
        account_id = task.get("account_id") or task.get("assigned_account_id")
        account = _fetchone(gw, "SELECT * FROM flow_accounts WHERE account_id=?", (account_id,))
        if task.get("worker_job_id"):
            job = _fetchone(agent, "SELECT * FROM omni_test_jobs WHERE job_id=?", (task["worker_job_id"],))
    return {"gateway_task": _public_task(task), "gateway_account": _public_account(account), "agent_job": _public_job(job)}


def _fetchone(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else None


def _public_task(task: dict[str, Any] | None) -> dict[str, Any] | None:
    if not task:
        return None
    keys = ["task_id", "status", "project_id", "account_id", "assigned_account_id", "worker_job_id", "generation_attempts", "lease_version", "output_media_id", "workflow_id", "upstream_batch_id", "operation_name"]
    return {key: task.get(key) for key in keys if key in task}


def _public_account(account: dict[str, Any] | None) -> dict[str, Any] | None:
    if not account:
        return None
    keys = ["account_id", "status", "current_task_id", "lock_version"]
    return {key: account.get(key) for key in keys if key in account}


def _public_job(job: dict[str, Any] | None) -> dict[str, Any] | None:
    if not job:
        return None
    keys = ["job_id", "status", "project_id", "input_media_id", "output_media_id", "workflow_id", "upstream_batch_id", "operation_name", "idempotency_key"]
    return {key: job.get(key) for key in keys if key in job}


def _validate_state(args: argparse.Namespace, state: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    task = state.get("gateway_task") or {}
    account = state.get("gateway_account") or {}
    job = state.get("agent_job") or {}
    if not task:
        reasons.append("gateway_task_missing")
    elif task.get("status") != GATEWAY_STATUS:
        reasons.append("gateway_status_not_reconciled")
    if not account:
        reasons.append("gateway_account_missing")
    elif account.get("status") != "busy":
        reasons.append("account_not_busy")
    if account and account.get("current_task_id") != args.task_id:
        reasons.append("account_current_task_mismatch")
    if not job:
        reasons.append("agent_job_missing")
    elif job.get("status") != AGENT_STATUS:
        reasons.append("agent_job_status_not_reconciled_bound")
    if task and job:
        for field in ("project_id", "output_media_id", "workflow_id", "upstream_batch_id"):
            if task.get(field) != job.get(field):
                reasons.append(f"{field}_mismatch")
    if not (task.get("output_media_id") if task else None):
        reasons.append("missing_output_media_id")
    return {"ok": not reasons, "blocking_reasons": reasons}


def _execute_blockers(args: argparse.Namespace, state: dict[str, Any]) -> list[str]:
    task = state.get("gateway_task") or {}
    account = state.get("gateway_account") or {}
    job = state.get("agent_job") or {}
    expected = {
        "confirm_task_id": task.get("task_id"),
        "confirm_project_id": task.get("project_id"),
        "confirm_account_id": account.get("account_id"),
        "confirm_job_id": job.get("job_id"),
        "confirm_output_media_id": task.get("output_media_id"),
        "confirm_generation_attempt": task.get("generation_attempts"),
        "confirm_lock_version": account.get("lock_version"),
        "confirm_lease_version": task.get("lease_version"),
    }
    blockers = [f"{name}_mismatch_or_missing" for name, value in expected.items() if getattr(args, name, None) != value]
    if not getattr(args, "allow_real_remote_query", False):
        blockers.append("allow_real_remote_query_required")
    if not getattr(args, "worker_base_url", None):
        blockers.append("worker_base_url_required")
    return blockers


def _worker_capability(args: argparse.Namespace, execute: bool) -> dict[str, Any]:
    if not getattr(args, "worker_base_url", None):
        return {"checked": False, "reason": "worker_base_url_not_provided"}
    if not execute:
        return {"checked": False, "reason": "dry_run_does_not_call_worker"}
    return {"checked": False, "reason": "execute_uses_query_route_directly"}


def _planned_remote_call(args: argparse.Namespace, state: dict[str, Any]) -> dict[str, Any]:
    task = state.get("gateway_task") or {}
    return {
        "method": "POST",
        "url": f"{str(getattr(args, 'worker_base_url', '')).rstrip('/')}/api/test/omni-video/{task.get('worker_job_id')}/query-remote-status-once" if getattr(args, "worker_base_url", None) else None,
        "project_id": task.get("project_id"),
        "output_media_id": task.get("output_media_id"),
        "max_remote_query_call_count": 1,
    }


async def _execute_one_remote_query(args: argparse.Namespace, state: dict[str, Any], worker_client: Any | None) -> dict[str, Any]:
    task = state["gateway_task"]
    worker = SimpleNamespace(api_url=str(args.worker_base_url).rstrip("/"))
    client = worker_client or WorkerClient()
    result = await client.query_omni_video_remote_status_once(worker, task["worker_job_id"], task["project_id"], task["output_media_id"])
    result["remote_query_call_count"] = min(int(result.get("remote_query_call_count") or 1), 1)
    result["submit_called"] = False
    result["download_called"] = False
    result["database_writes_performed"] = False
    return result


def _write_result_file(path: Path, result: dict[str, Any]) -> None:
    safe = _sanitize_result(result)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(safe, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _sanitize_result(value: Any) -> Any:
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            lower = key.lower()
            if any(token in lower for token in ("authorization", "cookie", "token", "recaptcha")):
                result[key] = "redacted"
            elif isinstance(item, str) and item.startswith("http"):
                result[key] = _safe_url(item)
            else:
                result[key] = _sanitize_result(item)
        return result
    if isinstance(value, list):
        return [_sanitize_result(item) for item in value]
    return value


def _safe_url(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    return {"scheme": parsed.scheme, "host": parsed.netloc, "path": parsed.path, "query_parameter_names": sorted({key for key, _ in parse_qsl(parsed.query, keep_blank_values=True)})}
