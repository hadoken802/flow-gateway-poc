"""Local-only preflight for downloading a reconciled remote result."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


GATEWAY_REQUIRED_STATUS = "reconciled_remote_completed_download_unverified"
AGENT_REQUIRED_STATUS = "remote_reconciled_completed_download_unverified"
EXPECTED_POLL_SHA = "3278e5cd9d465797b63d7526b7a6a76fc866def660a01780df1fe4be16e4fe94"
TASK_ID = "3d98ece6-4ace-4dc6-817c-979d23881bc3"
ACCOUNT_ID = "FLOW-002"
PROJECT_ID = "c23337ee-3be2-4e13-a6b0-d74c87675394"
JOB_ID = "c443c4e8-fff7-59a7-a8b9-9a8f2c8d09eb"
OUTPUT_MEDIA_ID = "ed7cef70-ae56-4f7a-9c7f-2da5aa66d44a"
WORKFLOW_ID = "475f2092-1f91-46e1-ba2b-968e3dfa3aec"
BATCH_ID = "f2b691af-823b-4002-9d4d-d67a22f6a281"


def prepare_reconciled_remote_download(args: argparse.Namespace) -> dict[str, Any]:
    poll_path = Path(args.poll_result_file)
    poll_sha = _sha256_file(poll_path) if poll_path.exists() else None
    try:
        poll_result = json.loads(poll_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return _base(args, poll_sha, ["poll_result_file_missing"])
    except json.JSONDecodeError as exc:
        result = _base(args, poll_sha, ["poll_result_json_invalid"])
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
    worker = _inspect_worker(args.worker_base_url)
    output_plan = _output_plan(Path(args.output_dir), args.task_id, OUTPUT_MEDIA_ID)
    blockers = (
        poll_validation["blocking_reasons"]
        + fencing_validation["blocking_reasons"]
        + worker["blocking_reasons"]
        + output_plan["blocking_reasons"]
    )
    classification = "download_blocked" if blockers else "direct_download_is_only_remote_probe"
    return {
        "ok": not blockers,
        "mode": "dry_run",
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
        "get_media_behavior": {
            "definition": "agent/services/flow_client.py:get_media",
            "remote_request": "GET /v1/media/{media_id}",
            "returns": "media metadata with fresh signed URL or encoded video fields when present",
            "direct_disk_write": False,
            "direct_database_write": False,
            "download_file": False,
            "download_wrappers": [
                "agent/api/omni_test.py:_download_completed",
                "agent/api/omni_test.py:download_manual_flow_result",
            ],
        },
        "metadata_query_available": bool(worker.get("metadata_query_route_available")),
        "download_route_available": bool(worker.get("download_route_available")),
        "download_capability_classification": classification,
        "planned_remote_method": "FlowClient.get_media(output_media_id) via /api/flow/media/{media_id}; not called by prepare",
        "planned_remote_call_count": 0,
        "planned_output_path": output_plan["planned_output_path"],
        "planned_temp_path": output_plan["planned_temp_path"],
        "output_directory_check": output_plan["output_directory_check"],
        "existing_file_check": output_plan["existing_file_check"],
        "disk_space_check": output_plan["disk_space_check"],
        "planned_database_changes": [],
        "planned_lock_changes": {"account_status": "preserved_busy", "current_task_id": "preserved"},
        "submit_called": False,
        "poll_called": False,
        "get_media_called": False,
        "download_called": False,
        "network_calls_performed": 0,
        "writes_performed": False,
        "download_preflight_allowed": not blockers,
        "blocking_reasons": blockers,
    }


def _base(args: argparse.Namespace, poll_sha: str | None, blockers: list[str]) -> dict[str, Any]:
    return {
        "ok": False,
        "mode": "dry_run",
        "task_id": args.task_id,
        "poll_result_sha256": poll_sha,
        "submit_called": False,
        "poll_called": False,
        "get_media_called": False,
        "download_called": False,
        "network_calls_performed": 0,
        "writes_performed": False,
        "download_preflight_allowed": False,
        "blocking_reasons": blockers,
    }


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
    return {
        "task": _public(task),
        "account": _public(account),
        "agent_job": _public(job),
        "request_count": request_count,
        "agent_poll_evidence": _json_or_empty((job or {}).get("raw_response_shape")).get("reconciled_poll_result") if job else {},
    }


def _fetchone(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else None


def _public(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    keys = [
        "task_id", "account_id", "assigned_account_id", "status", "current_task_id", "lock_version",
        "generation_attempts", "lease_version", "worker_job_id", "project_id", "job_id",
        "output_media_id", "workflow_id", "upstream_batch_id", "video_path", "completed_at",
    ]
    return {key: row.get(key) for key in keys if key in row}


def _validate_poll_result(poll_result: dict[str, Any], poll_sha: str | None) -> dict[str, Any]:
    q = poll_result.get("remote_query") if isinstance(poll_result.get("remote_query"), dict) else poll_result
    reasons: list[str] = []
    checks = {
        "poll_result_sha_mismatch": poll_sha == EXPECTED_POLL_SHA,
        "poll_result_task_id_mismatch": poll_result.get("task_id") == TASK_ID,
        "poll_result_account_id_mismatch": poll_result.get("account_id") == ACCOUNT_ID,
        "poll_result_job_id_mismatch": poll_result.get("job_id") == JOB_ID,
        "poll_result_project_id_mismatch": (q.get("project_id") or poll_result.get("project_id")) == PROJECT_ID,
        "poll_result_output_media_id_mismatch": (q.get("output_media_id") or poll_result.get("output_media_id")) == OUTPUT_MEDIA_ID,
        "poll_result_call_count_not_one": int(poll_result.get("remote_query_call_count") or q.get("remote_query_call_count") or 0) == 1,
        "poll_result_not_completed": (poll_result.get("remote_state") or q.get("remote_state")) == "remote_completed",
        "poll_result_completed_not_true": q.get("completed") is True,
        "poll_result_submit_called": poll_result.get("submit_called") is False,
        "poll_result_download_called": poll_result.get("download_called") is False,
        "poll_result_writes_performed": poll_result.get("writes_performed") is False,
        "poll_result_database_writes_performed": q.get("database_writes_performed") is False,
    }
    for code, ok in checks.items():
        if not ok:
            reasons.append(code)
    return {"ok": not reasons, "blocking_reasons": reasons, "raw_status": q.get("raw_status"), "remote_state": poll_result.get("remote_state") or q.get("remote_state")}


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
    if evidence.get("remote_state") != "remote_completed":
        reasons.append("agent_poll_evidence_not_completed")
    if state.get("request_count") != 0:
        reasons.append("agent_request_table_not_empty")
    return {"ok": not reasons, "blocking_reasons": reasons, "request_count": state.get("request_count")}


def _inspect_worker(base_url: str) -> dict[str, Any]:
    health = _get_json(f"{base_url.rstrip('/')}/health")
    openapi = _get_json(f"{base_url.rstrip('/')}/openapi.json")
    openapi_payload = openapi.get("payload") if isinstance(openapi.get("payload"), dict) else {}
    paths = set((openapi_payload.get("paths") or {}).keys())
    reasons: list[str] = []
    if not health.get("ok"):
        reasons.append("worker_health_unavailable")
    if health.get("payload", {}).get("status") != "ok":
        reasons.append("worker_not_ok")
    if health.get("payload", {}).get("extension_connected") is not True:
        reasons.append("extension_not_connected")
    download_route = f"/api/test/omni-video/{'{job_id}'}/retry-download" in paths
    metadata_route = f"/api/flow/media/{'{media_id}'}" in paths
    if not download_route:
        reasons.append("download_route_missing")
    return {
        "health": health.get("payload"),
        "extension_connected": health.get("payload", {}).get("extension_connected") is True,
        "download_route_available": download_route,
        "metadata_query_route_available": metadata_route,
        "openapi_paths_checked": sorted(path for path in paths if "download" in path or "media" in path),
        "blocking_reasons": reasons,
    }


def _get_json(url: str) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return {"ok": True, "payload": json.loads(resp.read().decode("utf-8"))}
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": type(exc).__name__, "message": str(exc)[:300]}


def _output_plan(output_dir: Path, task_id: str, media_id: str) -> dict[str, Any]:
    filename = f"FLOW-002_{task_id[:8]}_{media_id[:8]}.mp4"
    final = output_dir / filename
    temp = output_dir / f".{filename}.part"
    parent_exists = output_dir.exists() and output_dir.is_dir()
    parent_writable = parent_exists and _writable_dir(output_dir)
    existing = _existing_file(final)
    free = shutil.disk_usage(output_dir if parent_exists else output_dir.parent if output_dir.parent.exists() else Path.cwd()).free
    reasons: list[str] = []
    if not parent_exists:
        reasons.append("output_directory_missing")
    if parent_exists and not parent_writable:
        reasons.append("output_directory_not_writable")
    if existing.get("exists"):
        reasons.append("planned_output_file_already_exists")
    if temp.exists():
        reasons.append("planned_temp_file_already_exists")
    if free < 100 * 1024 * 1024:
        reasons.append("insufficient_disk_space")
    return {
        "planned_output_path": str(final),
        "planned_temp_path": str(temp),
        "output_directory_check": {"path": str(output_dir), "exists": parent_exists, "writable": parent_writable},
        "existing_file_check": existing,
        "disk_space_check": {"free_disk_bytes": free, "minimum_required_free_bytes": 100 * 1024 * 1024},
        "blocking_reasons": reasons,
    }


def _writable_dir(path: Path) -> bool:
    return path.exists() and path.is_dir()


def _existing_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "path": str(path), "size": None, "sha256": None}
    return {"exists": True, "path": str(path), "size": path.stat().st_size, "sha256": _sha256_file(path)}


def _json_or_empty(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
