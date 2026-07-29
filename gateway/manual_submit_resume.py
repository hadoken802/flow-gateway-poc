"""Resume a manual-submit task without creating a new Gateway task."""
from __future__ import annotations

import argparse
import asyncio
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from . import crud, db as gateway_db
from .config import GatewaySettings
from .scheduler import _map_worker_status, _requires_manual_submit, _valid_local_mp4
from .worker_client import WorkerClient
from .worker_provider import RuntimeRegistryWorkerProvider


UNKNOWN_REMOTE_CODE = "REMOTE_RESULT_DETECTED_BEFORE_RESUME"
MANUAL_CODE = "UPSTREAM_UNUSUAL_ACTIVITY"


def resume_manual_submit(args: argparse.Namespace, worker_client: WorkerClient | None = None, worker_provider=None) -> dict[str, Any]:
    return asyncio.run(_resume_manual_submit(args, worker_client=worker_client, worker_provider=worker_provider))


async def _resume_manual_submit(args: argparse.Namespace, worker_client: WorkerClient | None = None, worker_provider=None) -> dict[str, Any]:
    task_id = str(args.task_id).strip()
    execute = bool(getattr(args, "execute", False))
    confirm_task_id = getattr(args, "confirm_task_id", None)
    db_path = Path(args.gateway_db).resolve()
    client = worker_client or WorkerClient()
    provider = worker_provider or RuntimeRegistryWorkerProvider()

    preflight = await preflight_manual_submit_resume(db_path, task_id, client, provider)
    result: dict[str, Any] = {
        "ok": False,
        "task_id": task_id,
        "gateway_db": str(db_path),
        "execute": execute,
        "preflight": preflight,
    }
    if not execute:
        result["ok"] = bool(preflight.get("resume_allowed"))
        return result
    if confirm_task_id != task_id:
        result.update({"error_code": "confirm_task_id_mismatch", "error_message": "--confirm-task-id must match --task-id"})
        return result
    if not getattr(args, "user_confirmed_verification_cleared", False):
        result.update({"error_code": "verification_not_confirmed", "error_message": "--user-confirmed-verification-cleared is required with --execute"})
        return result
    if not preflight.get("resume_allowed"):
        result.update({"error_code": preflight.get("reason_code"), "error_message": preflight.get("reason")})
        return result
    if preflight.get("page_check", {}).get("recaptcha_blocking_text_present"):
        result.update({"error_code": "recaptcha_page_still_blocked", "error_message": "Flow page still reports reCAPTCHA unavailable"})
        return result
    if preflight.get("page_check", {}).get("unknown_or_unavailable"):
        result["page_check_warning"] = "No safe DOM checker is available; execution relies on user confirmation plus Worker health."

    return await _execute_resume(db_path, task_id, client, provider, result)


async def preflight_manual_submit_resume(db_path: Path, task_id: str, client: WorkerClient, provider) -> dict[str, Any]:
    reasons: list[str] = []
    db = await gateway_db.connect(db_path)
    try:
        task = await crud.get_task(db, task_id)
        if not task:
            return _preflight(False, "task_not_found")
        account = await crud.get_account(db, task.get("account_id") or task.get("assigned_account_id"))
        if not account:
            return _preflight(False, "account_not_found", task=task)

        _check(bool(task.get("status") == "manual_submit_required"), "status_not_manual_submit_required", reasons)
        _check(bool((task.get("error_code") or task.get("last_error_code")) == MANUAL_CODE), "error_code_not_upstream_unusual_activity", reasons)
        _check(bool(task.get("account_id")), "missing_account_id", reasons)
        _check(bool(account.get("current_task_id") == task_id), "account_current_task_mismatch", reasons)
        _check(bool(account.get("status") in {"busy", "locked"}), "account_not_locked", reasons)
        _check(bool(task.get("account_id") == account.get("account_id")), "task_account_mismatch", reasons)
        _check(bool(task.get("project_id")), "missing_project_id", reasons)
        _check(bool(task.get("prompt") and task.get("duration") and task.get("aspect_ratio")), "missing_prompt_or_parameters", reasons)
        _check(bool(task.get("image_path")), "missing_reference_image", reasons)
        _check(not bool(task.get("output_media_id")), "has_output_media_id", reasons)
        _check(not bool(task.get("workflow_id")), "has_workflow_id", reasons)
        _check(not bool(task.get("operation_name")), "has_operation_name", reasons)
        _check(not bool(task.get("upstream_batch_id")), "has_upstream_batch_id", reasons)
        _check(not bool(task.get("remote_project_url")), "has_remote_project_url", reasons)
        _check(not _valid_local_mp4(task.get("video_path")).get("ok"), "valid_mp4_exists", reasons)
        _check(task.get("generation_attempts") is not None, "generation_attempts_unreadable", reasons)

        worker = _worker_for_account(provider, account.get("account_id"))
        if not worker:
            reasons.append("worker_not_found")
            worker_info = {}
            job = {}
            candidates = {}
        else:
            try:
                worker_info = await client.inspect(worker)
            except Exception as exc:
                worker_info = {"error": str(exc)[:300]}
                reasons.append("worker_health_unreachable")
            try:
                job = await client.get_omni_video(worker, task.get("worker_job_id"))
            except Exception as exc:
                job = {"error": str(exc)[:300]}
                reasons.append("worker_job_unreadable")
            try:
                candidates = await client.list_manual_flow_results(worker, task.get("project_id"), after=task.get("manual_submit_required_at"), exclude_media_ids={job.get("input_media_id"), job.get("output_media_id")})
            except Exception as exc:
                candidates = {"error": str(exc)[:300], "candidate_count": None}
                reasons.append("manual_flow_results_unreadable")
            _check(bool(worker_info.get("status") != "offline" and not worker_info.get("error")), "runtime_not_healthy", reasons)
            _check(bool(worker_info.get("extension_connected")), "extension_not_ready", reasons)
            _check(bool(worker_info.get("flow_key_present")), "flow_key_missing", reasons)
            _check(bool(job.get("input_media_id")), "missing_input_media_id", reasons)
            _check(not bool(job.get("output_media_id") or job.get("workflow_id") or job.get("operation_name") or job.get("upstream_batch_id")), "worker_job_has_remote_result", reasons)
            _check(int(candidates.get("candidate_count") or 0) == 0, "manual_flow_results_found", reasons)

        active_conflicts = await _active_conflicts(db, task_id, account.get("account_id"))
        _check(not active_conflicts, "account_has_another_active_task", reasons)

        page_check = {"unknown_or_unavailable": True, "reason": "no_safe_gateway_dom_checker"}
        return {
            "resume_allowed": not reasons,
            "reason_code": reasons[0] if reasons else None,
            "reasons": reasons,
            "task": _task_summary(task),
            "account": _account_summary(account),
            "worker_info": worker_info,
            "worker_job": _job_summary(job),
            "manual_flow_results": {"candidate_count": candidates.get("candidate_count"), "error": candidates.get("error")},
            "active_conflicts": active_conflicts,
            "page_check": page_check,
            "legacy_db": False,
        }
    finally:
        await db.close()


async def _execute_resume(db_path: Path, task_id: str, client: WorkerClient, provider, base_result: dict[str, Any]) -> dict[str, Any]:
    settings = GatewaySettings(db_path=db_path, dry_run=False)
    db = await gateway_db.connect(db_path)
    heartbeat_task = None
    try:
        task = await crud.get_task(db, task_id)
        account_id = task.get("account_id") if task else None
        worker = _worker_for_account(provider, account_id)
        if not task or not worker:
            base_result.update({"error_code": "task_or_worker_missing"})
            return base_result
        acquired = await crud.acquire_manual_submit_resume(db, task_id, settings.lease_duration_seconds, worker_instance_id=str(uuid.uuid4()), boot_id=str(uuid.uuid4()))
        if not acquired:
            base_result.update({"error_code": "resume_lease_not_acquired"})
            return base_result
        account = await crud.get_account(db, account_id)
        token = {"lease_owner": acquired["lease_owner"], "lease_version": int(acquired["lease_version"])}
        lock_version = int(account.get("lock_version") or 0)
        heartbeat_task = asyncio.create_task(_heartbeat_loop(db, task_id, account_id, token, lock_version, settings.heartbeat_interval_seconds, settings.lease_duration_seconds))

        before_submit = await _remote_evidence(client, worker, acquired)
        if before_submit.get("remote_detected"):
            await crud.guarded_update_task(db, task_id, token["lease_owner"], token["lease_version"], "submission_unknown", error_code=UNKNOWN_REMOTE_CODE, error_message="Remote result detected before resume submit", last_error_code=UNKNOWN_REMOTE_CODE, last_error_message=str(before_submit)[:500])
            base_result.update({"ok": False, "error_code": UNKNOWN_REMOTE_CODE, "remote_evidence": before_submit})
            return base_result

        payload = {
            "idempotency_key": acquired["idempotency_key"],
            "project_id": acquired["project_id"],
            "input_media_id": before_submit["worker_job"]["input_media_id"],
            "image_path": acquired["image_path"],
            "prompt": acquired["prompt"],
            "duration": acquired["duration"],
            "aspect_ratio": acquired["aspect_ratio"],
        }
        try:
            submit_result = await client.resume_omni_video(worker, payload)
        except Exception as exc:
            await crud.guarded_update_task(db, task_id, token["lease_owner"], token["lease_version"], "submission_unknown", error_code=type(exc).__name__[:120], error_message=str(exc)[:500], last_error_code=type(exc).__name__[:120], last_error_message=str(exc)[:500])
            base_result.update({"ok": False, "error_code": "submission_unknown", "error_message": str(exc)[:500]})
            return base_result
        if _requires_manual_submit(submit_result):
            await crud.guarded_update_task(db, task_id, token["lease_owner"], token["lease_version"], "manual_submit_required", worker_job_id=submit_result.get("job_id") or submit_result.get("worker_job_id"), error_code=MANUAL_CODE, error_message="Google requires manual submission for this Flow project", manual_submit_required_at=crud.utc_now(), remaining_credits=submit_result.get("remaining_credits"))
            base_result.update({"ok": False, "error_code": MANUAL_CODE, "submit_call_count": 1})
            return base_result
        worker_job_id = submit_result.get("job_id") or submit_result.get("worker_job_id")
        if not worker_job_id:
            await crud.guarded_update_task(db, task_id, token["lease_owner"], token["lease_version"], "submission_unknown", error_code="missing_worker_job_id", error_message=str(submit_result)[:500], last_error_code="missing_worker_job_id", last_error_message=str(submit_result)[:500])
            base_result.update({"ok": False, "error_code": "missing_worker_job_id", "submit_call_count": 1})
            return base_result
        await crud.guarded_update_task(db, task_id, token["lease_owner"], token["lease_version"], "submitted", worker_job_id=worker_job_id, output_media_id=submit_result.get("output_media_id"), workflow_id=submit_result.get("workflow_id"), operation_name=submit_result.get("operation_name"), upstream_batch_id=submit_result.get("upstream_batch_id"), submission_confirmed_at=crud.utc_now(), remaining_credits=submit_result.get("remaining_credits"))
        final = await _poll_download_complete(db, task_id, account_id, worker, client, token, lock_version)
        base_result.update(final)
        base_result["submit_call_count"] = 1
        return base_result
    finally:
        if heartbeat_task:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
        await db.close()


async def _poll_download_complete(db, task_id, account_id, worker, client, token, lock_version):
    for _ in range(120):
        task = await crud.get_task(db, task_id)
        if not task:
            return {"ok": False, "error_code": "task_missing_after_submit"}
        result = await client.get_omni_video(worker, task.get("worker_job_id"))
        if _requires_manual_submit(result):
            await crud.guarded_update_task(db, task_id, token["lease_owner"], token["lease_version"], "manual_submit_required", error_code=MANUAL_CODE, error_message="Google requires manual submission for this Flow project", manual_submit_required_at=crud.utc_now())
            return {"ok": False, "error_code": MANUAL_CODE}
        if result.get("status") in {"waiting_download", "completed_remote"}:
            downloading = await crud.guarded_transition(db, task_id, token["lease_owner"], token["lease_version"], task.get("status"), "downloading", download_attempts=("increment", 1))
            if not downloading:
                return {"ok": False, "error_code": "fencing_lost"}
            result = await client.retry_omni_video_download(worker, task.get("worker_job_id"))
        mapped = _map_worker_status(result.get("status"))
        if mapped == "completed":
            check = _valid_local_mp4(result.get("video_path"))
            if not check["ok"]:
                await crud.guarded_release_account(db, task_id, account_id, token["lease_owner"], token["lease_version"], lock_version, "download_failed", error_code=check["error_code"], error_message=check["error_message"])
                return {"ok": False, "error_code": check["error_code"]}
            completed = await crud.guarded_complete_real_task(db, task_id, account_id, token["lease_owner"], token["lease_version"], lock_version, result.get("video_path"), result.get("remaining_credits"))
            if not completed:
                return {"ok": False, "error_code": "fencing_lost"}
            return {"ok": True, "status": "completed", "video_path": result.get("video_path")}
        await crud.guarded_update_task(db, task_id, token["lease_owner"], token["lease_version"], mapped)
        await asyncio.sleep(1)
    await crud.guarded_update_task(db, task_id, token["lease_owner"], token["lease_version"], "submission_unknown", error_code="resume_poll_timeout", error_message="Resume polling timed out")
    return {"ok": False, "error_code": "resume_poll_timeout"}


async def _remote_evidence(client, worker, task):
    job = await client.get_omni_video(worker, task.get("worker_job_id"))
    candidates = await client.list_manual_flow_results(worker, task.get("project_id"), after=task.get("manual_submit_required_at"), exclude_media_ids={job.get("input_media_id"), job.get("output_media_id")})
    local_mp4 = _valid_local_mp4(task.get("video_path")).get("ok")
    remote_detected = bool(
        job.get("output_media_id") or job.get("workflow_id") or job.get("operation_name") or job.get("upstream_batch_id")
        or int(candidates.get("candidate_count") or 0) > 0
        or local_mp4
    )
    return {"remote_detected": remote_detected, "worker_job": job, "manual_flow_results": candidates, "local_mp4": local_mp4}


async def _heartbeat_loop(db, task_id, account_id, token, lock_version, interval, lease_seconds):
    try:
        while True:
            await asyncio.sleep(interval)
            ok = await crud.heartbeat(db, task_id, account_id, token["lease_owner"], token["lease_version"], lock_version, lease_seconds)
            if not ok:
                return
    except asyncio.CancelledError:
        raise


def _worker_for_account(provider, account_id):
    if not account_id:
        return None
    snapshot = provider.load_workers()
    for worker in snapshot.workers:
        if worker.account_id == account_id:
            return worker
    return None


async def _active_conflicts(db, task_id, account_id):
    cursor = await db.execute(
        """
        SELECT task_id, status FROM flow_tasks
        WHERE account_id=? AND task_id<>? AND status IN (
          'leased','project_create_pending','project_create_in_progress','project_creation_unknown',
          'project_created','submit_pending','submit_in_progress','submission_unknown','submitted',
          'processing','download_pending','downloading','manual_submit_required'
        )
        """,
        (account_id, task_id),
    )
    return [dict(row) for row in await cursor.fetchall()]


def _check(ok: bool, reason: str, reasons: list[str]) -> None:
    if not ok and reason not in reasons:
        reasons.append(reason)


def _preflight(allowed: bool, reason: str | None = None, **extra) -> dict[str, Any]:
    return {"resume_allowed": allowed, "reason_code": reason, "reasons": [reason] if reason else [], **extra}


def _task_summary(task: dict) -> dict:
    keys = ["task_id", "idempotency_key", "status", "account_id", "project_id", "worker_job_id", "output_media_id", "workflow_id", "operation_name", "upstream_batch_id", "generation_attempts", "download_attempts", "lease_version", "lease_expires_at", "video_path", "error_code", "last_error_code"]
    return {key: task.get(key) for key in keys}


def _account_summary(account: dict) -> dict:
    keys = ["account_id", "status", "current_task_id", "lock_owner", "lock_version", "lock_expires_at", "last_heartbeat_at"]
    return {key: account.get(key) for key in keys}


def _job_summary(job: dict) -> dict:
    keys = ["job_id", "project_id", "input_media_id", "output_media_id", "workflow_id", "operation_name", "upstream_batch_id", "status", "error_code", "submitted_at", "video_path"]
    return {key: job.get(key) for key in keys}
