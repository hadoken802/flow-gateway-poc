"""Apply a saved reconciled poll result without network or download calls."""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


GATEWAY_FROM_STATUS = "reconciled_remote_accepted_unknown"
GATEWAY_TO_STATUS = "reconciled_remote_completed_download_unverified"
AGENT_FROM_STATUS = "remote_reconciled_bound"
AGENT_TO_STATUS = "remote_reconciled_completed_download_unverified"
COMMAND_VERSION = "apply-reconciled-poll-result/v1"
REAL_GATEWAY_DB = Path(r"D:\Codex\projects\flow_gateway_poc\flowkit\data\gateway.db")
REAL_FLOW002_AGENT_DB = Path(r"D:\Codex\projects\flow_gateway_poc\data\FLOW-002.db")


def apply_reconciled_poll_result(args: argparse.Namespace) -> dict[str, Any]:
    execute = bool(getattr(args, "execute", False))
    mode = "execute" if execute else "dry_run"
    poll_path = Path(args.poll_result_file)
    poll_sha = _sha256_file(poll_path) if poll_path.exists() else None
    try:
        poll_result = json.loads(poll_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return _base(args, mode, poll_sha, ok=False, blocking=["poll_result_file_missing"])
    except json.JSONDecodeError as exc:
        return _base(args, mode, poll_sha, ok=False, blocking=["poll_result_json_invalid"], error=str(exc))

    readonly = not execute
    gw = _connect(Path(args.gateway_db), readonly=readonly)
    agent = _connect(Path(args.agent_db), readonly=readonly)
    try:
        state = _read_state(gw, agent, args.task_id)
        validation = _validate_poll_result(args, poll_result, poll_sha, state)
        fencing = _validate_fencing(args, state)
        blockers = validation["blocking_reasons"] + fencing["blocking_reasons"]
        if execute:
            blockers.extend(_execute_blockers(args, poll_sha))
        result = _base(args, mode, poll_sha)
        result.update(
            {
                "account_id": (state.get("account") or {}).get("account_id"),
                "job_id": (state.get("agent_job") or {}).get("job_id"),
                "poll_result_validation": validation,
                "fencing_validation": fencing,
                "current_gateway_state": state.get("task"),
                "current_agent_state": state.get("agent_job"),
                "remote_state": _remote_query(poll_result).get("remote_state") or poll_result.get("remote_state"),
                "raw_status": _remote_query(poll_result).get("raw_status"),
                "download_ready": bool(_remote_query(poll_result).get("download_ready")),
                "encoded_video_present": bool(_remote_query(poll_result).get("encoded_video_present")),
                "download_url_present": bool(_remote_query(poll_result).get("download_url_present")),
                "planned_gateway_changes": {"status": GATEWAY_TO_STATUS},
                "planned_agent_changes": {"status": AGENT_TO_STATUS, "raw_response_shape": "merge_reconciled_poll_result"},
                "planned_status_transition": {
                    "gateway": {"from": GATEWAY_FROM_STATUS, "to": GATEWAY_TO_STATUS},
                    "agent": {"from": AGENT_FROM_STATUS, "to": AGENT_TO_STATUS},
                },
                "planned_lock_changes": {"account_status": "preserved_busy", "current_task_id": "preserved"},
                "planned_version_changes": {"lease_version": "unchanged", "lock_version": "unchanged", "generation_attempts": "unchanged"},
                "planned_network_calls": [],
                "submit_called": False,
                "poll_called": False,
                "download_called": False,
                "network_calls_performed": 0,
                "writes_performed": False,
                "allowed_to_execute": not blockers,
                "blocking_reasons": blockers,
            }
        )
        if not execute:
            result["ok"] = True
            return result
        if blockers:
            result["ok"] = False
            return result
    finally:
        if not execute:
            gw.close()
            agent.close()

    try:
        exec_result = _execute_apply(gw, agent, args, poll_result, poll_sha)
        result.update(exec_result)
        result["ok"] = exec_result["result"] in {"applied", "already_applied"}
        result["writes_performed"] = exec_result["result"] == "applied"
        return result
    except ApplyError as exc:
        result["ok"] = False
        result["error_code"] = exc.code
        result["error_message"] = str(exc)[:500]
        result["writes_performed"] = False
        return result
    finally:
        gw.close()
        agent.close()


def _connect(path: Path, *, readonly: bool) -> sqlite3.Connection:
    if readonly:
        conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
        conn.execute("PRAGMA query_only=ON")
    else:
        conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _base(args: argparse.Namespace, mode: str, poll_sha: str | None, *, ok: bool | None = None, blocking: list[str] | None = None, error: str | None = None) -> dict[str, Any]:
    result = {
        "ok": bool(ok) if ok is not None else False,
        "mode": mode,
        "task_id": args.task_id,
        "poll_result_sha256": poll_sha,
        "submit_called": False,
        "poll_called": False,
        "download_called": False,
        "network_calls_performed": 0,
        "writes_performed": False,
        "blocking_reasons": blocking or [],
    }
    if error:
        result["error_message"] = error
    return result


def _read_state(gw: sqlite3.Connection, agent: sqlite3.Connection, task_id: str) -> dict[str, Any]:
    task = _fetchone(gw, "SELECT * FROM flow_tasks WHERE task_id=?", (task_id,))
    account = None
    job = None
    if task:
        account = _fetchone(gw, "SELECT * FROM flow_accounts WHERE account_id=?", (task.get("account_id") or task.get("assigned_account_id"),))
        if task.get("worker_job_id"):
            job = _fetchone(agent, "SELECT * FROM omni_test_jobs WHERE job_id=?", (task["worker_job_id"],))
    return {"task": _public_task(task), "account": _public_account(account), "agent_job": _public_job(job)}


def _fetchone(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else None


def _public_task(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    keys = ["task_id", "status", "project_id", "account_id", "assigned_account_id", "worker_job_id", "generation_attempts", "lease_version", "output_media_id", "workflow_id", "upstream_batch_id", "operation_name"]
    return {k: row.get(k) for k in keys if k in row}


def _public_account(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    keys = ["account_id", "status", "current_task_id", "lock_version"]
    return {k: row.get(k) for k in keys if k in row}


def _public_job(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    keys = ["job_id", "status", "project_id", "input_media_id", "output_media_id", "workflow_id", "upstream_batch_id", "operation_name", "video_path", "completed_at", "raw_response_shape"]
    return {k: row.get(k) for k in keys if k in row}


def _remote_query(poll_result: dict[str, Any]) -> dict[str, Any]:
    nested = poll_result.get("remote_query")
    return nested if isinstance(nested, dict) else poll_result


def _validate_poll_result(args: argparse.Namespace, poll_result: dict[str, Any], poll_sha: str | None, state: dict[str, Any]) -> dict[str, Any]:
    q = _remote_query(poll_result)
    task = state.get("task") or {}
    job = state.get("agent_job") or {}
    account = state.get("account") or {}
    reasons: list[str] = []
    if poll_result.get("task_id") != args.task_id:
        reasons.append("poll_result_task_id_mismatch")
    if poll_result.get("account_id") != account.get("account_id"):
        reasons.append("poll_result_account_id_mismatch")
    if poll_result.get("job_id") != job.get("job_id"):
        reasons.append("poll_result_job_id_mismatch")
    if (q.get("project_id") or poll_result.get("project_id")) != task.get("project_id"):
        reasons.append("poll_result_project_id_mismatch")
    if (q.get("output_media_id") or poll_result.get("output_media_id")) != task.get("output_media_id"):
        reasons.append("poll_result_output_media_id_mismatch")
    if int(poll_result.get("remote_query_call_count") or q.get("remote_query_call_count") or 0) != 1:
        reasons.append("poll_result_remote_query_call_count_not_one")
    if (poll_result.get("remote_state") or q.get("remote_state")) != "remote_completed":
        reasons.append("poll_result_remote_state_not_completed")
    if q.get("raw_status") not in {"MEDIA_GENERATION_STATUS_SUCCESSFUL", "MEDIA_GENERATION_STATUS_COMPLETED"}:
        reasons.append("poll_result_raw_status_not_success")
    if q.get("completed") is not True:
        reasons.append("poll_result_completed_not_true")
    for key in ("submit_called", "download_called", "writes_performed"):
        if poll_result.get(key) is not False:
            reasons.append(f"poll_result_{key}_not_false")
    if q.get("database_writes_performed") is not False:
        reasons.append("poll_result_database_writes_performed_not_false")
    if q.get("query_ok") is not True:
        reasons.append("poll_result_query_not_ok")
    if q.get("error_code") or q.get("error_message"):
        reasons.append("poll_result_query_error_present")
    if not q.get("status_evidence"):
        reasons.append("poll_result_status_evidence_missing")
    if _contains_sensitive_key(poll_result):
        reasons.append("poll_result_contains_sensitive_key")
    return {"ok": not reasons, "poll_result_sha256": poll_sha, "blocking_reasons": reasons}


def _validate_fencing(args: argparse.Namespace, state: dict[str, Any]) -> dict[str, Any]:
    task = state.get("task") or {}
    account = state.get("account") or {}
    job = state.get("agent_job") or {}
    reasons: list[str] = []
    if not task:
        reasons.append("gateway_task_missing")
    elif task.get("status") not in {GATEWAY_FROM_STATUS, GATEWAY_TO_STATUS}:
        reasons.append("gateway_state_conflict")
    if not account:
        reasons.append("gateway_account_missing")
    else:
        if account.get("status") != "busy":
            reasons.append("account_not_busy")
        if account.get("current_task_id") != args.task_id:
            reasons.append("current_task_id_conflict")
    if not job:
        reasons.append("agent_job_missing")
    elif job.get("status") not in {AGENT_FROM_STATUS, AGENT_TO_STATUS}:
        reasons.append("agent_state_conflict")
    if job and (job.get("video_path") or job.get("completed_at")):
        reasons.append("agent_already_has_local_completion")
    if task and job:
        for field in ("project_id", "output_media_id", "workflow_id", "upstream_batch_id"):
            if task.get(field) != job.get(field):
                reasons.append(f"{field}_conflict")
    return {"ok": not reasons, "blocking_reasons": reasons}


def _execute_blockers(args: argparse.Namespace, poll_sha: str | None) -> list[str]:
    checks = {
        "confirm_task_id": args.task_id,
        "confirm_project_id": "c23337ee-3be2-4e13-a6b0-d74c87675394",
        "confirm_account_id": "FLOW-002",
        "confirm_job_id": "c443c4e8-fff7-59a7-a8b9-9a8f2c8d09eb",
        "confirm_output_media_id": "ed7cef70-ae56-4f7a-9c7f-2da5aa66d44a",
        "confirm_generation_attempt": 2,
        "confirm_lock_version": 2,
        "confirm_lease_version": 2,
        "confirm_poll_result_sha256": poll_sha,
    }
    blockers = [f"{name}_mismatch_or_missing" for name, expected in checks.items() if getattr(args, name, None) != expected]
    if (_is_real_path(Path(args.gateway_db), REAL_GATEWAY_DB) or _is_real_path(Path(args.agent_db), REAL_FLOW002_AGENT_DB)) and not getattr(args, "allow_real_database", False):
        blockers.append("real_database_execute_blocked")
    return blockers


def _execute_apply(gw: sqlite3.Connection, agent: sqlite3.Connection, args: argparse.Namespace, poll_result: dict[str, Any], poll_sha: str) -> dict[str, Any]:
    now = _now()
    agent_phase = _apply_agent(agent, args, poll_result, poll_sha, now)
    gateway_phase = _apply_gateway(gw, args, poll_result, poll_sha, now)
    return {"result": "already_applied" if agent_phase == "already_applied" and gateway_phase == "already_applied" else "applied", "agent_phase": agent_phase, "gateway_phase": gateway_phase}


def _apply_agent(agent: sqlite3.Connection, args: argparse.Namespace, poll_result: dict[str, Any], poll_sha: str, now: str) -> str:
    q = _remote_query(poll_result)
    agent.execute("BEGIN IMMEDIATE")
    try:
        job = _fetchone(agent, "SELECT * FROM omni_test_jobs WHERE job_id=?", (poll_result["job_id"],))
        if not job:
            raise ApplyError("agent_job_missing", "Agent job missing")
        existing = _existing_poll_shape(job)
        if job.get("status") == AGENT_TO_STATUS:
            if existing.get("poll_result_sha256") == poll_sha:
                agent.commit()
                return "already_applied"
            raise ApplyError("poll_result_conflict", "Different poll result already applied")
        if job.get("status") != AGENT_FROM_STATUS:
            raise ApplyError("agent_state_conflict", "Agent state changed")
        if job.get("video_path") or job.get("completed_at"):
            raise ApplyError("agent_already_has_local_completion", "Agent already has local completion")
        merged = _merge_raw_shape(job.get("raw_response_shape"), poll_result, poll_sha, q, now)
        cur = agent.execute(
            """
            UPDATE omni_test_jobs
            SET status=?, raw_response_shape=?, updated_at=?
            WHERE job_id=? AND status=? AND video_path IS NULL AND completed_at IS NULL
            """,
            (AGENT_TO_STATUS, json.dumps(merged, ensure_ascii=False, sort_keys=True), now, poll_result["job_id"], AGENT_FROM_STATUS),
        )
        if cur.rowcount != 1:
            raise ApplyError("agent_cas_failed", "Agent row was not updated")
        agent.commit()
        return "updated"
    except Exception:
        agent.rollback()
        raise


def _apply_gateway(gw: sqlite3.Connection, args: argparse.Namespace, poll_result: dict[str, Any], poll_sha: str, now: str) -> str:
    gw.execute("BEGIN IMMEDIATE")
    try:
        columns = _table_columns(gw, "flow_tasks")
        has_query_state = "remote_result_query_state" in columns
        task = _fetchone(gw, "SELECT * FROM flow_tasks WHERE task_id=?", (args.task_id,))
        account = _fetchone(gw, "SELECT * FROM flow_accounts WHERE account_id=?", (poll_result["account_id"],))
        if not task or not account:
            raise ApplyError("gateway_rows_missing", "Gateway rows missing")
        if task.get("status") == GATEWAY_TO_STATUS:
            if not has_query_state:
                gw.commit()
                return "already_applied"
            existing = _json_or_empty(task.get("remote_result_query_state"))
            if existing.get("reconciled_poll_result", {}).get("poll_result_sha256") in {None, poll_sha}:
                gw.commit()
                return "already_applied"
            raise ApplyError("poll_result_conflict", "Different poll result already applied")
        if task.get("status") != GATEWAY_FROM_STATUS:
            raise ApplyError("gateway_state_conflict", "Gateway state changed")
        if account.get("status") != "busy" or account.get("current_task_id") != args.task_id:
            raise ApplyError("fencing_state_changed", "Account lock changed")
        if task.get("lease_version") != 2 or account.get("lock_version") != 2 or task.get("generation_attempts") != 2:
            raise ApplyError("fencing_state_changed", "Version changed")
        values: list[Any]
        if has_query_state:
            remote_state = _json_or_empty(task.get("remote_result_query_state"))
            remote_state["reconciled_poll_result"] = _poll_evidence(poll_result, poll_sha, _remote_query(poll_result), now)
            set_clause = "status=?, remote_result_query_state=?, updated_at=?"
            values = [GATEWAY_TO_STATUS, json.dumps(remote_state, ensure_ascii=False, sort_keys=True), now]
        else:
            set_clause = "status=?, updated_at=?"
            values = [GATEWAY_TO_STATUS, now]
        values.extend([args.task_id, GATEWAY_FROM_STATUS, 2, 2])
        cur = gw.execute(
            f"""
            UPDATE flow_tasks
            SET {set_clause}
            WHERE task_id=? AND status=? AND generation_attempts=? AND lease_version=?
            """,
            tuple(values),
        )
        if cur.rowcount != 1:
            raise ApplyError("gateway_cas_failed", "Gateway row was not updated")
        gw.commit()
        return "updated"
    except Exception:
        gw.rollback()
        raise


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _merge_raw_shape(raw: str | None, poll_result: dict[str, Any], poll_sha: str, q: dict[str, Any], now: str) -> dict[str, Any]:
    shape = _json_or_empty(raw)
    shape["reconciled_poll_result"] = _poll_evidence(poll_result, poll_sha, q, now)
    return shape


def _poll_evidence(poll_result: dict[str, Any], poll_sha: str, q: dict[str, Any], applied_at: str) -> dict[str, Any]:
    return {
        "poll_result_sha256": poll_sha,
        "remote_state": poll_result.get("remote_state") or q.get("remote_state"),
        "raw_status": q.get("raw_status"),
        "remote_query_call_count": poll_result.get("remote_query_call_count") or q.get("remote_query_call_count"),
        "completed": q.get("completed"),
        "download_ready": q.get("download_ready"),
        "encoded_video_present": q.get("encoded_video_present"),
        "download_url_present": q.get("download_url_present"),
        "query_started_at": q.get("query_started_at"),
        "query_finished_at": q.get("query_finished_at"),
        "applied_at": applied_at,
        "command_version": COMMAND_VERSION,
    }


def _existing_poll_shape(job: dict[str, Any]) -> dict[str, Any]:
    return _json_or_empty(job.get("raw_response_shape")).get("reconciled_poll_result") or {}


def _json_or_empty(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {"previous_raw_response_shape": parsed}
    except Exception:
        return {"previous_raw_response_shape_sha256": hashlib.sha256(value.encode("utf-8")).hexdigest()}


def _contains_sensitive_key(value: Any) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if any(token in key.lower() for token in ("authorization", "cookie", "token", "recaptcha")):
                return True
            if _contains_sensitive_key(item):
                return True
    elif isinstance(value, list):
        return any(_contains_sensitive_key(item) for item in value)
    return False


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_real_path(path: Path, real_path: Path) -> bool:
    try:
        return path.resolve().samefile(real_path)
    except FileNotFoundError:
        return path.resolve() == real_path


class ApplyError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
