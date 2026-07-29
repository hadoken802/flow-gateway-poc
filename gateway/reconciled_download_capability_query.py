"""Dry-run-first one-shot media capability query for reconciled results."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sqlite3
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from .reconciled_remote_download_preflight import (
    ACCOUNT_ID,
    AGENT_REQUIRED_STATUS,
    BATCH_ID,
    EXPECTED_POLL_SHA,
    GATEWAY_REQUIRED_STATUS,
    JOB_ID,
    OUTPUT_MEDIA_ID,
    PROJECT_ID,
    TASK_ID,
    WORKFLOW_ID,
    _json_or_empty,
    _sha256_file,
)
from .worker_client import WorkerClient


ROUTE = "/api/test/omni-video/{job_id}/query-download-capability-once"


def query_reconciled_download_capability_once(args: argparse.Namespace, worker_client: Any | None = None) -> dict[str, Any]:
    return asyncio.run(_query(args, worker_client))


async def _query(args: argparse.Namespace, worker_client: Any | None) -> dict[str, Any]:
    execute = bool(getattr(args, "execute", False))
    mode = "execute" if execute else "dry_run"
    poll_path = Path(args.poll_result_file)
    poll_sha = _sha256_file(poll_path) if poll_path.exists() else None
    try:
        poll_result = json.loads(poll_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return _base(args, mode, poll_sha, ["poll_result_file_missing"])
    except json.JSONDecodeError as exc:
        result = _base(args, mode, poll_sha, ["poll_result_json_invalid"])
        result["error_message"] = str(exc)
        return result

    gw = _connect_ro(Path(args.gateway_db))
    agent = _connect_ro(Path(args.agent_db))
    try:
        state = _read_state(gw, agent, args.task_id)
    finally:
        gw.close()
        agent.close()

    poll_validation = _validate_poll_result(poll_result, poll_sha)
    fencing_validation = _validate_fencing(args, state, poll_sha)
    worker = _inspect_worker(args.worker_base_url) if execute else _dry_run_worker()
    blockers = poll_validation["blocking_reasons"] + fencing_validation["blocking_reasons"]
    if execute:
        blockers += worker["blocking_reasons"]
    if execute:
        blockers.extend(_execute_blockers(args, poll_sha))
    result = {
        "ok": not blockers or not execute,
        "mode": mode,
        "task_id": args.task_id,
        "account_id": ACCOUNT_ID,
        "job_id": JOB_ID,
        "project_id": PROJECT_ID,
        "workflow_id": WORKFLOW_ID,
        "output_media_id": OUTPUT_MEDIA_ID,
        "upstream_batch_id": BATCH_ID,
        "poll_result_sha256": poll_sha,
        "current_gateway_state": state.get("task"),
        "current_agent_state": state.get("agent_job"),
        "fencing_validation": fencing_validation,
        "poll_result_validation": poll_validation,
        "worker_health": worker.get("health"),
        "extension_connected": worker.get("extension_connected"),
        "media_query_route": ROUTE,
        "query_strategy": "agent_safe_summary_route",
        "planned_remote_call": "get_media(output_media_id)",
        "planned_remote_call_count": 1,
        "planned_database_changes": [],
        "planned_file_changes": [],
        "get_media_called": False,
        "poll_called": False,
        "submit_called": False,
        "download_called": False,
        "network_calls_performed": 0,
        "writes_performed": False,
        "allowed_to_execute": not blockers,
        "worker_restart_required": "download_capability_route_missing" in worker.get("blocking_reasons", []),
        "blocking_reasons": blockers,
    }
    if not execute or blockers:
        return result

    query_result = await _execute_once(args, worker_client)
    result.update({
        "ok": True,
        "media_capability": _sanitize(query_result),
        "get_media_called": True,
        "get_media_call_count": min(int(query_result.get("get_media_call_count") or 1), 1),
        "network_calls_performed": 1,
        "poll_called": False,
        "submit_called": False,
        "download_called": False,
        "writes_performed": False,
    })
    if getattr(args, "result_file", None):
        _write_result(Path(args.result_file), result)
    return result


def _connect_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _read_state(gw: sqlite3.Connection, agent: sqlite3.Connection, task_id: str) -> dict[str, Any]:
    task = _fetchone(gw, "SELECT * FROM flow_tasks WHERE task_id=?", (task_id,))
    account = _fetchone(gw, "SELECT * FROM flow_accounts WHERE account_id=?", (ACCOUNT_ID,))
    job = _fetchone(agent, "SELECT * FROM omni_test_jobs WHERE job_id=?", (JOB_ID,))
    request_count = agent.execute("SELECT COUNT(*) FROM request").fetchone()[0]
    return {"task": _public(task), "account": _public(account), "agent_job": _public(job), "request_count": request_count, "agent_poll_evidence": _json_or_empty((job or {}).get("raw_response_shape")).get("reconciled_poll_result") if job else {}}


def _fetchone(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else None


def _public(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    keys = ["task_id", "account_id", "assigned_account_id", "status", "current_task_id", "lock_version", "generation_attempts", "lease_version", "worker_job_id", "project_id", "job_id", "output_media_id", "workflow_id", "upstream_batch_id", "video_path", "completed_at"]
    return {key: row.get(key) for key in keys if key in row}


def _validate_poll_result(poll_result: dict[str, Any], poll_sha: str | None) -> dict[str, Any]:
    q = poll_result.get("remote_query") if isinstance(poll_result.get("remote_query"), dict) else poll_result
    checks = {
        "poll_result_sha_mismatch": poll_sha == EXPECTED_POLL_SHA,
        "poll_result_not_completed": (poll_result.get("remote_state") or q.get("remote_state")) == "remote_completed",
        "poll_result_completed_not_true": q.get("completed") is True,
        "poll_result_call_count_not_one": int(poll_result.get("remote_query_call_count") or q.get("remote_query_call_count") or 0) == 1,
        "poll_result_submit_called": poll_result.get("submit_called") is False,
        "poll_result_download_called": poll_result.get("download_called") is False,
        "poll_result_database_writes_performed": q.get("database_writes_performed") is False,
    }
    reasons = [code for code, ok in checks.items() if not ok]
    return {"ok": not reasons, "blocking_reasons": reasons}


def _validate_fencing(args: argparse.Namespace, state: dict[str, Any], poll_sha: str | None) -> dict[str, Any]:
    task = state.get("task") or {}
    account = state.get("account") or {}
    job = state.get("agent_job") or {}
    evidence = state.get("agent_poll_evidence") or {}
    reasons: list[str] = []
    if task.get("status") != GATEWAY_REQUIRED_STATUS:
        reasons.append("gateway_state_conflict")
    if job.get("status") != AGENT_REQUIRED_STATUS:
        reasons.append("agent_state_conflict")
    if account.get("status") != "busy":
        reasons.append("account_not_busy")
    if account.get("current_task_id") != args.task_id:
        reasons.append("current_task_id_conflict")
    if task.get("generation_attempts") != 2:
        reasons.append("generation_attempt_conflict")
    if task.get("lease_version") != 2:
        reasons.append("lease_version_conflict")
    if account.get("lock_version") != 2:
        reasons.append("lock_version_conflict")
    if task.get("worker_job_id") != JOB_ID:
        reasons.append("worker_job_id_conflict")
    for field, expected in {"project_id": PROJECT_ID, "output_media_id": OUTPUT_MEDIA_ID, "workflow_id": WORKFLOW_ID, "upstream_batch_id": BATCH_ID}.items():
        if task.get(field) != expected or job.get(field) != expected:
            reasons.append(f"{field}_conflict")
    if job.get("video_path"):
        reasons.append("agent_video_path_exists")
    if job.get("completed_at"):
        reasons.append("agent_completed_at_exists")
    if evidence.get("poll_result_sha256") != poll_sha:
        reasons.append("agent_poll_evidence_sha_mismatch")
    if state.get("request_count") != 0:
        reasons.append("agent_request_table_not_empty")
    return {"ok": not reasons, "blocking_reasons": reasons}


def _inspect_worker(base_url: str) -> dict[str, Any]:
    health = _get_json(f"{base_url.rstrip('/')}/health")
    openapi = _get_json(f"{base_url.rstrip('/')}/openapi.json")
    paths = set(((openapi.get("payload") or {}).get("paths") or {}).keys()) if isinstance(openapi.get("payload"), dict) else set()
    route = ROUTE in paths
    reasons: list[str] = []
    if not health.get("ok"):
        reasons.append("worker_health_unavailable")
    if health.get("payload", {}).get("status") != "ok":
        reasons.append("worker_not_ok")
    if health.get("payload", {}).get("extension_connected") is not True:
        reasons.append("extension_not_connected")
    if not route:
        reasons.append("download_capability_route_missing")
    return {"health": health.get("payload"), "extension_connected": health.get("payload", {}).get("extension_connected") is True, "route_available": route, "blocking_reasons": reasons}


def _dry_run_worker() -> dict[str, Any]:
    return {"health": {"checked": False, "reason": "dry_run_does_not_call_worker"}, "extension_connected": None, "route_available": None, "blocking_reasons": []}


def _execute_blockers(args: argparse.Namespace, poll_sha: str | None) -> list[str]:
    expected = {"confirm_task_id": TASK_ID, "confirm_project_id": PROJECT_ID, "confirm_account_id": ACCOUNT_ID, "confirm_job_id": JOB_ID, "confirm_output_media_id": OUTPUT_MEDIA_ID, "confirm_generation_attempt": 2, "confirm_lock_version": 2, "confirm_lease_version": 2, "confirm_poll_result_sha256": poll_sha}
    blockers = [f"{name}_mismatch_or_missing" for name, value in expected.items() if getattr(args, name, None) != value]
    if not getattr(args, "allow_real_media_query", False):
        blockers.append("allow_real_media_query_required")
    if not getattr(args, "result_file", None):
        blockers.append("result_file_required")
    return blockers


async def _execute_once(args: argparse.Namespace, worker_client: Any | None) -> dict[str, Any]:
    worker = SimpleNamespace(api_url=str(args.worker_base_url).rstrip("/"))
    client = worker_client or WorkerClient()
    return await client.query_omni_video_download_capability_once(worker, JOB_ID, PROJECT_ID, OUTPUT_MEDIA_ID)


def _get_json(url: str) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return {"ok": True, "payload": json.loads(resp.read().decode("utf-8"))}
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": type(exc).__name__, "message": str(exc)[:300]}


def _base(args: argparse.Namespace, mode: str, poll_sha: str | None, blockers: list[str]) -> dict[str, Any]:
    return {"ok": False, "mode": mode, "task_id": args.task_id, "poll_result_sha256": poll_sha, "get_media_called": False, "poll_called": False, "submit_called": False, "download_called": False, "network_calls_performed": 0, "writes_performed": False, "blocking_reasons": blockers}


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        clean = {}
        for key, item in value.items():
            lower = key.lower()
            if any(token in lower for token in ("authorization", "cookie", "token", "recaptcha", "encodedvideo")):
                if lower == "encoded_video_present" or lower == "encoded_video_length" or lower == "encoded_video_sha256":
                    clean[key] = item
                else:
                    clean[key] = "redacted"
            elif isinstance(item, str) and item.startswith("http"):
                clean[key] = "redacted_url"
            else:
                clean[key] = _sanitize(item)
        return clean
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    return value


def _write_result(path: Path, result: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_sanitize(result), indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
