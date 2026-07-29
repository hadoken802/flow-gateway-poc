"""Read-only reconciliation helpers for submission_unknown tasks."""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any

from . import crud, db as gateway_db
from .worker_client import WorkerClient
from .worker_provider import RuntimeRegistryWorkerProvider


def reconcile_submission_unknown(args: argparse.Namespace, worker_client: WorkerClient | None = None, worker_provider=None) -> dict[str, Any]:
    return asyncio.run(_reconcile_submission_unknown(args, worker_client=worker_client, worker_provider=worker_provider))


def resolve_submission_unknown(args: argparse.Namespace) -> dict[str, Any]:
    return asyncio.run(_resolve_submission_unknown(args))


async def _reconcile_submission_unknown(args: argparse.Namespace, worker_client: WorkerClient | None = None, worker_provider=None) -> dict[str, Any]:
    task_id = str(args.task_id).strip()
    db_path = Path(args.gateway_db).resolve()
    if bool(getattr(args, "execute", False)):
        return {
            "ok": False,
            "task_id": task_id,
            "gateway_db": str(db_path),
            "execute": True,
            "reason_code": "reconcile_execute_not_implemented",
            "submit_called": False,
        }
    client = worker_client or WorkerClient()
    provider = worker_provider or RuntimeRegistryWorkerProvider()
    db = await gateway_db.connect_readonly(db_path)
    try:
        task = await crud.get_task(db, task_id)
        if not task:
            return {"ok": False, "task_id": task_id, "reason_code": "task_not_found"}
        account = await crud.get_account(db, task.get("account_id") or task.get("assigned_account_id"))
        local_candidates: dict[str, Any] = {"source": "agent_local_request_table", "candidate_count": None}
        worker_job: dict[str, Any] = {}
        worker = _worker_for_account(provider, task.get("account_id"))
        if worker and task.get("worker_job_id"):
            try:
                worker_job = await client.get_omni_video(worker, task.get("worker_job_id"))
            except Exception as exc:
                worker_job = {"error": str(exc)[:300]}
            try:
                local_candidates.update(await client.list_manual_flow_results(worker, task.get("project_id"), after=task.get("submission_started_at"), exclude_media_ids={task.get("output_media_id")}))
            except Exception as exc:
                local_candidates.update({"error": str(exc)[:300], "candidate_count": None})
        local_count = local_candidates.get("candidate_count")
        remote_query = {
            "available": False,
            "state": "remote_query_unavailable",
            "reason": "No reliable Google Flow project-history query is implemented; local candidates are not remote evidence.",
        }
        classification = _classify(task, worker_job, remote_query)
        return {
            "ok": task.get("status") == "submission_unknown",
            "task_id": task_id,
            "gateway_db": str(db_path),
            "task_status": task.get("status"),
            "classification": classification,
            "reconciliation_status": "blocked_remote_query_unavailable",
            "remote_submission_state": task.get("remote_submission_state") or worker_job.get("remote_submission_state"),
            "remote_query_available": remote_query["available"],
            "remote_query_reason": remote_query["state"],
            "local_candidate_count": local_count,
            "account": _account_summary(account or {}),
            "task": _task_summary(task),
            "worker_job": _job_summary(worker_job),
            "agent_job_evidence": _job_summary(worker_job),
            "extension_evidence": {
                "extension_request_id": task.get("extension_request_id") or worker_job.get("extension_request_id"),
                "available": False,
                "reason": "No persisted extension request log row is implemented; live service worker requestLog may be ephemeral.",
            },
            "credits_evidence": {
                "gateway_account_credits": account.get("credits") if account else None,
                "note": "Credits are auxiliary evidence only and do not prove remote submission state.",
            },
            "project_id": task.get("project_id"),
            "request_batch_id": task.get("request_batch_id") or task.get("upstream_batch_id") or worker_job.get("request_batch_id"),
            "extension_request_id": task.get("extension_request_id") or worker_job.get("extension_request_id"),
            "operation_name": task.get("operation_name") or worker_job.get("operation_name"),
            "workflow_id": task.get("workflow_id") or worker_job.get("workflow_id"),
            "upstream_batch_id": task.get("upstream_batch_id") or worker_job.get("upstream_batch_id"),
            "output_media_id": task.get("output_media_id") or worker_job.get("output_media_id"),
            "processing": False,
            "completed": False,
            "failed": False,
            "local_manual_candidates": local_candidates,
            "actual_remote_project_results": remote_query,
            "resolution_allowed": False,
            "allowed_resolutions": [],
            "recommended_action": "keep_submission_unknown",
            "submit_called": False,
        }
    finally:
        await db.close()


async def _resolve_submission_unknown(args: argparse.Namespace) -> dict[str, Any]:
    task_id = str(args.task_id).strip()
    db_path = Path(args.gateway_db).resolve()
    execute = bool(getattr(args, "execute", False))
    resolution = str(args.resolution).strip()
    confirm = getattr(args, "confirm_task_id", None)
    result = {
        "ok": False,
        "task_id": task_id,
        "gateway_db": str(db_path),
        "execute": execute,
        "resolution": resolution,
        "submit_called": False,
    }
    if resolution not in {"confirmed-rejected", "confirmed-not-started"}:
        result["reason_code"] = "unsupported_resolution"
        return result
    if execute and confirm != task_id:
        result["reason_code"] = "confirm_task_id_mismatch"
        return result
    db = await gateway_db.connect(db_path)
    try:
        task = await crud.get_task(db, task_id)
        if not task:
            result["reason_code"] = "task_not_found"
            return result
        if task.get("status") != "submission_unknown":
            result["reason_code"] = "status_not_submission_unknown"
            return result
        if resolution == "confirmed-not-started" and task.get("remote_submission_state") not in {None, "not_started"}:
            result["reason_code"] = "resolution_conflicts_with_remote_submission_state"
            return result
        result["preflight"] = {"resolution_allowed": True, "task": _task_summary(task)}
        if not execute:
            result["ok"] = True
            return result
        lease_owner = task.get("lease_owner")
        lease_version = int(task.get("lease_version") or 0)
        account_id = task.get("account_id")
        account = await crud.get_account(db, account_id)
        if not lease_owner or not account or account.get("current_task_id") != task_id:
            result["reason_code"] = "missing_fencing_or_account_lock"
            return result
        updated = await crud.guarded_update_task(
            db,
            task_id,
            lease_owner,
            lease_version,
            "manual_submit_required",
            error_code="UPSTREAM_UNUSUAL_ACTIVITY",
            error_message=f"Resolved submission_unknown as {resolution}",
            last_error_code=task.get("error_code"),
            last_error_message=task.get("error_message"),
        )
        if not updated:
            result["reason_code"] = "fencing_lost"
            return result
        result.update({"ok": True, "status": "manual_submit_required"})
        return result
    finally:
        await db.close()


def _worker_for_account(provider, account_id):
    if not account_id:
        return None
    snapshot = provider.load_workers()
    return next((worker for worker in snapshot.workers if worker.account_id == account_id), None)


def _task_summary(task: dict) -> dict:
    keys = [
        "task_id", "status", "account_id", "project_id", "worker_job_id",
        "output_media_id", "workflow_id", "operation_name", "upstream_batch_id",
        "resume_attempt_id", "request_batch_id", "extension_request_id",
        "remote_http_status", "remote_submission_state", "generation_attempts",
        "lease_owner", "lease_version", "error_code", "last_error_code",
    ]
    return {key: task.get(key) for key in keys}


def _account_summary(account: dict) -> dict:
    keys = ["account_id", "status", "current_task_id", "lock_owner", "lock_version", "lock_expires_at"]
    return {key: account.get(key) for key in keys}


def _job_summary(job: dict) -> dict:
    keys = [
        "job_id", "status", "project_id", "input_media_id", "output_media_id",
        "workflow_id", "operation_name", "upstream_batch_id", "resume_attempt_id",
        "request_batch_id", "extension_request_id", "remote_http_status",
        "remote_submission_state", "error_code",
    ]
    return {key: job.get(key) for key in keys}


def _classify(task: dict, worker_job: dict, remote_query: dict) -> str:
    state = task.get("remote_submission_state") or worker_job.get("remote_submission_state")
    if state == "accepted_persist_failed":
        return "accepted_response_identifiers_lost"
    if task.get("status") == "submission_unknown" and not remote_query.get("available"):
        return "submission_unknown_remote_query_unavailable"
    return "not_submission_unknown" if task.get("status") != "submission_unknown" else "submission_unknown"
