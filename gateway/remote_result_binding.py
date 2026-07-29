"""Bind a reconciled remote Flow result without submitting or polling."""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BIND_STATUS = "reconciled_remote_accepted_unknown"
AGENT_BOUND_STATUS = "remote_reconciled_bound"
COMMAND_VERSION = "bind-reconciled-remote-result/v1"
REAL_GATEWAY_DB = Path(r"D:\Codex\projects\flow_gateway_poc\flowkit\data\gateway.db")
REAL_FLOW002_AGENT_DB = Path(r"D:\Codex\projects\flow_gateway_poc\data\FLOW-002.db")

GATEWAY_TASK_REQUIRED_COLUMNS = {
    "task_id",
    "status",
    "project_id",
    "image_path",
    "prompt",
    "duration",
    "aspect_ratio",
    "assigned_account_id",
    "account_id",
    "worker_job_id",
    "generation_attempts",
    "lease_version",
    "output_media_id",
    "workflow_id",
    "operation_name",
    "upstream_batch_id",
    "updated_at",
}
GATEWAY_ACCOUNT_REQUIRED_COLUMNS = {"account_id", "status", "current_task_id", "lock_version"}
AGENT_JOB_REQUIRED_COLUMNS = {
    "job_id",
    "idempotency_key",
    "project_id",
    "prompt",
    "image_path",
    "input_media_id",
    "output_media_id",
    "workflow_id",
    "operation_name",
    "upstream_batch_id",
    "status",
    "raw_response_shape",
    "created_at",
    "updated_at",
}
GATEWAY_TASK_OPTIONAL_AUDIT_COLUMNS = {"resume_attempt_id", "request_batch_id", "remote_submission_state", "remote_result_query_state"}
AGENT_JOB_OPTIONAL_AUDIT_COLUMNS = {
    "gateway_task_id",
    "generation_attempt",
    "source_worker_job_id",
    "resume_attempt_id",
    "request_batch_id",
    "remote_submission_state",
}


def bind_reconciled_remote_result(args: argparse.Namespace) -> dict[str, Any]:
    execute = bool(getattr(args, "execute", False))
    candidate_path = Path(args.candidate_file)
    candidate_bytes = candidate_path.read_bytes()
    candidate_sha = hashlib.sha256(candidate_bytes).hexdigest()
    try:
        candidate = json.loads(candidate_bytes)
    except json.JSONDecodeError as exc:
        return _base(args, "dry_run" if not execute else "execute", candidate_sha, ok=False, blocking=["candidate_json_invalid"], error=str(exc))

    mode = "execute" if execute else "dry_run"
    gateway_db = Path(args.gateway_db)
    agent_db = Path(args.agent_db)
    if execute and not getattr(args, "allow_real_database", False):
        real_blockers = []
        if _is_real_path(gateway_db, REAL_GATEWAY_DB) or _is_real_path(agent_db, REAL_FLOW002_AGENT_DB):
            real_blockers.append("real_database_execute_blocked")
        if real_blockers:
            result = _base(args, mode, candidate_sha, ok=False, blocking=real_blockers)
            result["allowed_to_execute"] = False
            return result
    readonly = not execute
    gw = _connect(gateway_db, readonly=readonly)
    agent = _connect(agent_db, readonly=readonly)
    try:
        state = _read_state(gw, agent, args.task_id, candidate)
        result = _base(args, mode, candidate_sha)
        result.update(state)
        validation = _validate_candidate(args, candidate, candidate_sha, state)
        schema = _schema_compatibility(gw, agent)
        plans = _planned_changes(candidate, state, candidate_sha)
        blocking = validation["blocking_reasons"] + schema["blocking_reasons"]
        if execute:
            blocking += _execute_confirm_blockers(args, candidate, candidate_sha, gateway_db, agent_db)
        result.update(
            {
                "candidate_validation": validation,
                "schema_compatibility": schema,
                "planned_gateway_changes": plans["gateway"],
                "planned_agent_changes": plans["agent"],
                "planned_status_transition": plans["status_transition"],
                "planned_audit_record": plans["audit"],
                "planned_version_changes": {"lease_version": "unchanged", "lock_version": "unchanged"},
                "poll_readiness": bool(candidate.get("safe_to_poll") and candidate.get("poll_identifier") == candidate.get("output_media_id")),
                "download_readiness": bool(candidate.get("safe_to_download") and candidate.get("download_identifier")),
                "submit_called": False,
                "poll_called": False,
                "download_called": False,
                "writes_performed": False,
                "allowed_to_execute": not blocking,
                "blocking_reasons": blocking,
            }
        )
        if not execute:
            result["ok"] = True
            return result
        if blocking:
            result["ok"] = False
            return result
    finally:
        if not execute:
            gw.close()
            agent.close()

    try:
        exec_result = _execute_bind(gw, agent, args.task_id, candidate, candidate_sha, state)
        result.update(exec_result)
        result["ok"] = exec_result.get("result") in {"bound", "already_bound"}
        result["writes_performed"] = exec_result.get("result") == "bound"
        return result
    except BindingError as exc:
        result["ok"] = False
        result["error_code"] = exc.code
        result["error_message"] = str(exc)[:500]
        result["writes_performed"] = False
        result["retriable"] = exc.code in {"gateway_cas_failed", "fencing_state_changed"}
        return result
    except Exception as exc:
        result["ok"] = False
        result["error_code"] = type(exc).__name__
        result["error_message"] = str(exc)[:500]
        result["writes_performed"] = False
        result["retriable"] = True
        return result
    finally:
        gw.close()
        agent.close()


def _connect(path: Path, *, readonly: bool) -> sqlite3.Connection:
    if readonly:
        uri = f"file:{path.resolve().as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.execute("PRAGMA query_only=ON")
    else:
        conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _base(args: argparse.Namespace, mode: str, candidate_sha: str, *, ok: bool | None = None, blocking: list[str] | None = None, error: str | None = None) -> dict[str, Any]:
    result = {
        "ok": bool(ok) if ok is not None else False,
        "mode": mode,
        "task_id": args.task_id,
        "candidate_sha256": candidate_sha,
        "submit_called": False,
        "poll_called": False,
        "download_called": False,
        "writes_performed": False,
        "blocking_reasons": blocking or [],
    }
    if error:
        result["error_message"] = error
    return result


def _read_state(gw: sqlite3.Connection, agent: sqlite3.Connection, task_id: str, candidate: dict[str, Any]) -> dict[str, Any]:
    task = _fetchone(gw, "SELECT * FROM flow_tasks WHERE task_id=?", (task_id,))
    account = None
    if task:
        account_id = task.get("account_id") or task.get("assigned_account_id") or candidate.get("account_id")
        account = _fetchone(gw, "SELECT * FROM flow_accounts WHERE account_id=?", (account_id,))
    source_job_id = task.get("worker_job_id") if task else candidate.get("source_worker_job_id")
    source_job = _fetchone(agent, "SELECT * FROM omni_test_jobs WHERE job_id=?", (source_job_id,)) if source_job_id else None
    bound_job_id = deterministic_job_id(task_id, int(candidate.get("generation_attempt") or 0))
    bound_job = _fetchone(agent, "SELECT * FROM omni_test_jobs WHERE job_id=?", (bound_job_id,))
    conflicts = _find_remote_conflicts(gw, agent, task_id, candidate, bound_job_id)
    return {
        "account_id": candidate.get("account_id"),
        "current_gateway_state": {"task": _public_row(task), "account": _public_row(account)},
        "current_agent_state": {
            "source_job": _public_row(source_job),
            "bound_job": _public_row(bound_job),
            "deterministic_job_id": bound_job_id,
            "remote_id_conflicts": conflicts,
        },
    }


def _fetchone(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else None


def _public_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    allowed = {
        "task_id", "account_id", "assigned_account_id", "project_id", "status", "current_task_id",
        "generation_attempts", "generation_attempt", "lease_version", "lock_version", "worker_job_id",
        "job_id", "idempotency_key", "gateway_task_id", "source_worker_job_id", "input_media_id",
        "output_media_id", "workflow_id", "operation_name", "upstream_batch_id", "remote_submission_state",
        "resume_attempt_id", "request_batch_id", "created_at", "updated_at",
    }
    return {k: row.get(k) for k in allowed if k in row}


def _validate_candidate(args: argparse.Namespace, candidate: dict[str, Any], candidate_sha: str, state: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    required_equal = {
        "task_id": args.task_id,
        "match_class": "exact_match",
        "safe_to_bind": True,
        "safe_to_poll": True,
    }
    for key, expected in required_equal.items():
        if candidate.get(key) != expected:
            reasons.append(f"candidate_{key}_mismatch")
    for key in ("account_id", "project_id", "workflow_id", "output_media_id", "upstream_batch_id", "poll_identifier", "input_media_id"):
        if not candidate.get(key):
            reasons.append(f"candidate_missing_{key}")
    if candidate.get("conflicts"):
        reasons.append("candidate_has_conflicts")
    if candidate.get("poll_identifier") and candidate.get("poll_identifier") != candidate.get("output_media_id"):
        reasons.append("poll_identifier_mismatch")
    if not candidate.get("database_safety_evidence", {}).get("all_unchanged"):
        reasons.append("candidate_database_safety_not_all_unchanged")
    mapping = candidate.get("flow001_mapping_evidence") or {}
    for key in ("canonical_workflow_id", "canonical_output_media_id", "canonical_upstream_batch_id"):
        if not (isinstance(mapping.get(key), dict) and mapping[key].get("verified")):
            reasons.append(f"candidate_mapping_unverified_{key}")
    task = (state.get("current_gateway_state") or {}).get("task") or {}
    account = (state.get("current_gateway_state") or {}).get("account") or {}
    source_job = (state.get("current_agent_state") or {}).get("source_job") or {}
    if not task:
        reasons.append("gateway_task_missing")
    else:
        if task.get("account_id") != candidate.get("account_id") and task.get("assigned_account_id") != candidate.get("account_id"):
            reasons.append("gateway_account_mismatch")
        if task.get("project_id") != candidate.get("project_id"):
            reasons.append("gateway_project_mismatch")
        if task.get("generation_attempts") != candidate.get("generation_attempt"):
            reasons.append("gateway_generation_attempt_mismatch")
        if task.get("status") not in {"submission_unknown", BIND_STATUS}:
            reasons.append("gateway_status_not_bindable")
    if account:
        if account.get("status") != "busy":
            reasons.append("account_not_busy")
        if account.get("current_task_id") != args.task_id:
            reasons.append("account_current_task_mismatch")
    else:
        reasons.append("gateway_account_missing")
    if not source_job:
        reasons.append("source_agent_job_missing")
    elif source_job.get("input_media_id") != candidate.get("input_media_id"):
        reasons.append("source_input_media_mismatch")
    for conflict in state.get("current_agent_state", {}).get("remote_id_conflicts", []):
        reasons.append(conflict["reason"])
    return {"ok": not reasons, "candidate_sha256": candidate_sha, "blocking_reasons": reasons}


def _schema_compatibility(gw: sqlite3.Connection, agent: sqlite3.Connection) -> dict[str, Any]:
    task_cols = _columns(gw, "flow_tasks")
    account_cols = _columns(gw, "flow_accounts")
    job_cols = _columns(agent, "omni_test_jobs")
    missing = {
        "gateway.flow_tasks": sorted(GATEWAY_TASK_REQUIRED_COLUMNS - task_cols),
        "gateway.flow_accounts": sorted(GATEWAY_ACCOUNT_REQUIRED_COLUMNS - account_cols),
        "agent.omni_test_jobs": sorted(AGENT_JOB_REQUIRED_COLUMNS - job_cols),
    }
    optional_missing = {
        "gateway.flow_tasks": sorted(GATEWAY_TASK_OPTIONAL_AUDIT_COLUMNS - task_cols),
        "agent.omni_test_jobs": sorted(AGENT_JOB_OPTIONAL_AUDIT_COLUMNS - job_cols),
    }
    blockers = [f"{table}_schema_missing_columns" for table, cols in missing.items() if cols]
    if missing["agent.omni_test_jobs"]:
        blockers.append("agent_schema_upgrade_required")
    return {
        "ok": not blockers,
        "missing_columns": missing,
        "optional_missing_columns": optional_missing,
        "agent_bound_status": AGENT_BOUND_STATUS,
        "auto_migration_performed": False,
        "blocking_reasons": blockers,
    }


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _planned_changes(candidate: dict[str, Any], state: dict[str, Any], candidate_sha: str) -> dict[str, Any]:
    job_id = state["current_agent_state"]["deterministic_job_id"]
    return {
        "gateway": {
            "status": BIND_STATUS,
            "worker_job_id": job_id,
            "workflow_id": candidate.get("workflow_id"),
            "output_media_id": candidate.get("output_media_id"),
            "upstream_batch_id": candidate.get("upstream_batch_id"),
            "operation_name": candidate.get("operation_name"),
            "remote_submission_state": candidate.get("remote_state"),
            "account_lock": "preserved",
        },
        "agent": {
            "job_id": job_id,
            "status": AGENT_BOUND_STATUS,
            "workflow_id": candidate.get("workflow_id"),
            "output_media_id": candidate.get("output_media_id"),
            "upstream_batch_id": candidate.get("upstream_batch_id"),
            "remote_submission_state": candidate.get("remote_state"),
        },
        "status_transition": {"from": "submission_unknown", "to": BIND_STATUS},
        "audit": _audit_payload(candidate, candidate_sha),
    }


def deterministic_job_id(task_id: str, generation_attempt: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"reconciled:{task_id}:attempt:{generation_attempt}"))


def _execute_confirm_blockers(args: argparse.Namespace, candidate: dict[str, Any], candidate_sha: str, gateway_db: Path, agent_db: Path) -> list[str]:
    blockers: list[str] = []
    checks = {
        "confirm_task_id": candidate.get("task_id"),
        "confirm_project_id": candidate.get("project_id"),
        "confirm_account_id": candidate.get("account_id"),
        "confirm_generation_attempt": candidate.get("generation_attempt"),
        "confirm_output_media_id": candidate.get("output_media_id"),
        "confirm_candidate_sha256": candidate_sha,
    }
    for arg_name, expected in checks.items():
        if getattr(args, arg_name, None) != expected:
            blockers.append(f"{arg_name}_mismatch_or_missing")
    if _is_real_path(gateway_db, REAL_GATEWAY_DB) or _is_real_path(agent_db, REAL_FLOW002_AGENT_DB):
        if not getattr(args, "allow_real_database", False):
            blockers.append("real_database_execute_blocked")
    return blockers


def _is_real_path(path: Path, real_path: Path) -> bool:
    try:
        return path.resolve().samefile(real_path)
    except FileNotFoundError:
        return path.resolve() == real_path


def _find_remote_conflicts(gw: sqlite3.Connection, agent: sqlite3.Connection, task_id: str, candidate: dict[str, Any], bound_job_id: str) -> list[dict[str, str]]:
    conflicts = []
    output_id = candidate.get("output_media_id")
    workflow_id = candidate.get("workflow_id")
    if output_id:
        for row in gw.execute("SELECT task_id FROM flow_tasks WHERE output_media_id=? AND task_id<>?", (output_id, task_id)):
            conflicts.append({"reason": "output_media_id_bound_to_other_task", "task_id": row[0]})
        for row in agent.execute("SELECT job_id FROM omni_test_jobs WHERE output_media_id=? AND job_id<>?", (output_id, bound_job_id)):
            conflicts.append({"reason": "output_media_id_bound_to_other_agent_job", "job_id": row[0]})
    if workflow_id:
        for row in gw.execute("SELECT task_id FROM flow_tasks WHERE workflow_id=? AND task_id<>?", (workflow_id, task_id)):
            conflicts.append({"reason": "workflow_id_bound_to_other_task", "task_id": row[0]})
        for row in agent.execute("SELECT job_id FROM omni_test_jobs WHERE workflow_id=? AND job_id<>?", (workflow_id, bound_job_id)):
            conflicts.append({"reason": "workflow_id_bound_to_other_agent_job", "job_id": row[0]})
    return conflicts


def _execute_bind(gw: sqlite3.Connection, agent: sqlite3.Connection, task_id: str, candidate: dict[str, Any], candidate_sha: str, state: dict[str, Any]) -> dict[str, Any]:
    job_id = state["current_agent_state"]["deterministic_job_id"]
    idempotency_key = f"reconciled:{task_id}:attempt:{candidate['generation_attempt']}"
    now = _now()
    source_job_id = (state["current_gateway_state"]["task"] or {}).get("worker_job_id")
    source_job = _fetchone(agent, "SELECT * FROM omni_test_jobs WHERE job_id=?", (source_job_id,)) if source_job_id else None
    if not source_job:
        raise BindingError("source_agent_job_missing", "Source Agent job is missing")
    agent_result = _prepare_agent_job(agent, job_id, idempotency_key, candidate, source_job, candidate_sha, now)
    gateway_result = _commit_gateway_binding(gw, job_id, idempotency_key, task_id, candidate, candidate_sha, now)
    return {
        "result": "already_bound" if gateway_result == "already_bound" and agent_result in {"created", "reused"} else "bound",
        "agent_phase": agent_result,
        "gateway_phase": gateway_result,
        "deterministic_job_id": job_id,
        "idempotency_key": idempotency_key,
        "resume_attempt_id": idempotency_key,
    }


def _prepare_agent_job(agent: sqlite3.Connection, job_id: str, idempotency_key: str, candidate: dict[str, Any], source_job: dict[str, Any], candidate_sha: str, now: str) -> str:
    agent.execute("BEGIN IMMEDIATE")
    try:
        existing = _fetchone(agent, "SELECT * FROM omni_test_jobs WHERE job_id=?", (job_id,))
        if existing:
            _assert_agent_job_consistent(existing, candidate, idempotency_key)
            agent.commit()
            return "reused"
        cols = _columns(agent, "omni_test_jobs")
        raw_shape = _agent_raw_shape(candidate, candidate_sha)
        raw_shape["source_worker_job_id"] = source_job.get("job_id")
        values = {
            "job_id": job_id,
            "idempotency_key": idempotency_key,
            "project_id": candidate["project_id"],
            "prompt": source_job.get("prompt"),
            "image_path": source_job.get("image_path"),
            "input_media_id": candidate["input_media_id"],
            "output_media_id": candidate["output_media_id"],
            "workflow_id": candidate["workflow_id"],
            "operation_name": candidate.get("operation_name"),
            "upstream_batch_id": candidate["upstream_batch_id"],
            "status": AGENT_BOUND_STATUS,
            "gateway_task_id": candidate["task_id"],
            "generation_attempt": candidate["generation_attempt"],
            "source_worker_job_id": source_job.get("job_id"),
            "resume_attempt_id": idempotency_key,
            "request_batch_id": candidate["upstream_batch_id"],
            "remote_submission_state": candidate.get("remote_state"),
            "raw_response_shape": json.dumps(raw_shape, ensure_ascii=False, sort_keys=True),
            "created_at": now,
            "updated_at": now,
        }
        insert_cols = [col for col in values if col in cols]
        placeholders = ",".join("?" for _ in insert_cols)
        agent.execute(
            f"INSERT INTO omni_test_jobs({','.join(insert_cols)}) VALUES({placeholders})",
            tuple(values[col] for col in insert_cols),
        )
        agent.commit()
        return "created"
    except Exception:
        agent.rollback()
        raise


def _assert_agent_job_consistent(job: dict[str, Any], candidate: dict[str, Any], idempotency_key: str) -> None:
    expected = {
        "idempotency_key": idempotency_key,
        "project_id": candidate["project_id"],
        "input_media_id": candidate["input_media_id"],
        "output_media_id": candidate["output_media_id"],
        "workflow_id": candidate["workflow_id"],
        "upstream_batch_id": candidate["upstream_batch_id"],
        "status": AGENT_BOUND_STATUS,
    }
    if "gateway_task_id" in job:
        expected["gateway_task_id"] = candidate["task_id"]
    if "generation_attempt" in job:
        expected["generation_attempt"] = candidate["generation_attempt"]
    for key, value in expected.items():
        if job.get(key) != value:
            raise BindingError("agent_bound_job_conflict", f"{key} differs")


def _commit_gateway_binding(gw: sqlite3.Connection, job_id: str, resume_attempt_id: str, task_id: str, candidate: dict[str, Any], candidate_sha: str, now: str) -> str:
    gw.execute("BEGIN IMMEDIATE")
    try:
        task = _fetchone(gw, "SELECT * FROM flow_tasks WHERE task_id=?", (task_id,))
        account = _fetchone(gw, "SELECT * FROM flow_accounts WHERE account_id=?", (candidate["account_id"],))
        if not task or not account:
            raise BindingError("gateway_rows_missing", "Task or account missing")
        if _gateway_already_bound(task, candidate, job_id):
            gw.commit()
            return "already_bound"
        _assert_gateway_fence(task, account, candidate)
        audit = _audit_payload(candidate, candidate_sha)
        audit["bound_at"] = now
        audit["agent_job_id"] = job_id
        cols = _columns(gw, "flow_tasks")
        updates = {
            "status": BIND_STATUS,
            "worker_job_id": job_id,
            "output_media_id": candidate["output_media_id"],
            "workflow_id": candidate["workflow_id"],
            "operation_name": candidate.get("operation_name"),
            "upstream_batch_id": candidate["upstream_batch_id"],
            "resume_attempt_id": resume_attempt_id,
            "request_batch_id": candidate["upstream_batch_id"],
            "remote_submission_state": candidate.get("remote_state"),
            "remote_result_query_state": json.dumps(audit, ensure_ascii=False, sort_keys=True),
            "updated_at": now,
        }
        update_cols = [col for col in updates if col in cols]
        set_sql = ", ".join(f"{col}=?" for col in update_cols)
        cur = gw.execute(
            f"""
            UPDATE flow_tasks
            SET {set_sql}
            WHERE task_id=? AND status='submission_unknown' AND generation_attempts=?
              AND lease_version=? AND account_id=? AND assigned_account_id=?
            """,
            tuple(updates[col] for col in update_cols)
            + (
                task_id,
                candidate["generation_attempt"],
                task.get("lease_version"),
                candidate["account_id"],
                candidate["account_id"],
            ),
        )
        if cur.rowcount != 1:
            raise BindingError("gateway_cas_failed", "Task fencing row was not updated")
        gw.commit()
        return "updated"
    except Exception:
        gw.rollback()
        raise


def _gateway_already_bound(task: dict[str, Any], candidate: dict[str, Any], job_id: str) -> bool:
    return (
        task.get("status") == BIND_STATUS
        and task.get("worker_job_id") == job_id
        and task.get("output_media_id") == candidate.get("output_media_id")
        and task.get("workflow_id") == candidate.get("workflow_id")
        and task.get("upstream_batch_id") == candidate.get("upstream_batch_id")
    )


def _assert_gateway_fence(task: dict[str, Any], account: dict[str, Any], candidate: dict[str, Any]) -> None:
    checks = {
        "status": task.get("status") == "submission_unknown",
        "generation_attempts": task.get("generation_attempts") == candidate.get("generation_attempt"),
        "lease_version": task.get("lease_version") == 2,
        "account_status": account.get("status") == "busy",
        "account_current_task_id": account.get("current_task_id") == candidate.get("task_id"),
        "account_lock_version": account.get("lock_version") == 2,
    }
    failed = [key for key, ok in checks.items() if not ok]
    if failed:
        raise BindingError("fencing_state_changed", ",".join(failed))


def _audit_payload(candidate: dict[str, Any], candidate_sha: str) -> dict[str, Any]:
    return {
        "event_type": "remote_result_bound",
        "command_version": COMMAND_VERSION,
        "task_id": candidate.get("task_id"),
        "account_id": candidate.get("account_id"),
        "generation_attempt": candidate.get("generation_attempt"),
        "candidate_sha256": candidate_sha,
        "source_capture_dir": candidate.get("source_capture_dir"),
        "source_procedure": candidate.get("source_procedure") or "flow.projectInitialData",
        "source_request_id": candidate.get("source_request_id"),
        "project_id": candidate.get("project_id"),
        "workflow_id": candidate.get("workflow_id"),
        "output_media_id": candidate.get("output_media_id"),
        "upstream_batch_id": candidate.get("upstream_batch_id"),
        "match_class": candidate.get("match_class"),
        "match_confidence": candidate.get("match_confidence"),
        "remote_state": candidate.get("remote_state"),
    }


def _agent_raw_shape(candidate: dict[str, Any], candidate_sha: str) -> dict[str, Any]:
    return {
        "source": "reconciled_remote_project_result",
        "task_id": candidate.get("task_id"),
        "account_id": candidate.get("account_id"),
        "generation_attempt": candidate.get("generation_attempt"),
        "source_worker_job_id": candidate.get("source_worker_job_id"),
        "resume_attempt_id": f"reconciled:{candidate.get('task_id')}:attempt:{candidate.get('generation_attempt')}",
        "request_batch_id": candidate.get("upstream_batch_id"),
        "candidate_sha256": candidate_sha,
        "remote_state": candidate.get("remote_state"),
        "match_class": candidate.get("match_class"),
        "match_confidence": candidate.get("match_confidence"),
        "source_procedure": candidate.get("source_procedure") or "flow.projectInitialData",
        "field_evidence_keys": sorted((candidate.get("field_evidence") or {}).keys()),
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class BindingError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
