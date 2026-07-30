"""Scheduler V2 primitives: state, leases, quota, selection, and retry policy."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import uuid


TASK_STATES = {
    "queued",
    "reserving",
    "leased",
    "submitting",
    "submission_unknown",
    "submitted",
    "generating",
    "generated",
    "downloading",
    "completed",
    "retry_wait",
    "need_manual",
    "failed",
    "cancelled",
}

ACTIVE_GENERATION_STATES = {"reserving", "leased", "submitting", "submission_unknown", "submitted", "generating"}
ACTIVE_DOWNLOAD_STATES = {"generated", "downloading"}
TERMINAL_STATES = {"completed", "need_manual", "failed", "cancelled"}

STATUS_TO_STATE = {
    "queued": "queued",
    "leased": "leased",
    "assigning": "leased",
    "project_create_pending": "leased",
    "project_create_in_progress": "leased",
    "project_created": "leased",
    "submit_pending": "submitting",
    "submit_in_progress": "submitting",
    "submission_unknown": "submission_unknown",
    "submitted": "submitted",
    "processing": "generating",
    "download_pending": "generated",
    "downloading": "downloading",
    "completed": "completed",
    "waiting_recovery": "retry_wait",
    "manual_review": "need_manual",
    "manual_submit_required": "need_manual",
    "download_failed": "failed",
    "failed_before_remote_submit": "failed",
    "failed": "failed",
    "cancelled": "cancelled",
}

ERROR_CATEGORIES = {
    "transient_network",
    "gateway_timeout",
    "agent_timeout",
    "browser_disconnected",
    "extension_disconnected",
    "flow_rate_limited",
    "account_unusual_activity",
    "recaptcha_required",
    "authentication_expired",
    "account_not_bound",
    "ownership_not_verified",
    "quota_insufficient",
    "project_creation_failed",
    "media_upload_failed",
    "submission_failed_confirmed",
    "submission_result_unknown",
    "generation_failed",
    "generation_timeout",
    "download_failed",
    "file_validation_failed",
    "local_storage_failed",
    "configuration_error",
    "permanent_task_error",
    "need_manual",
}


@dataclass(frozen=True)
class RetryPolicy:
    retryable: bool
    retry_stage: str
    max_attempts: int
    backoff_seconds: int
    switch_account: bool
    account_health_penalty: int
    cooldown_seconds: int
    manual_required: bool


RETRY_POLICIES = {
    "transient_network": RetryPolicy(True, "generation", 2, 60, False, 2, 0, False),
    "gateway_timeout": RetryPolicy(True, "reconcile", 1, 0, False, 1, 0, False),
    "agent_timeout": RetryPolicy(True, "reconcile", 1, 0, False, 2, 0, False),
    "browser_disconnected": RetryPolicy(True, "reconcile", 1, 120, False, 10, 300, False),
    "extension_disconnected": RetryPolicy(True, "reconcile", 1, 120, False, 10, 300, False),
    "flow_rate_limited": RetryPolicy(True, "generation", 2, 600, True, 10, 600, False),
    "account_unusual_activity": RetryPolicy(False, "none", 0, 0, False, 40, 3600, True),
    "recaptcha_required": RetryPolicy(False, "none", 0, 0, False, 40, 3600, True),
    "authentication_expired": RetryPolicy(False, "none", 0, 0, False, 50, 0, True),
    "account_not_bound": RetryPolicy(False, "none", 0, 0, False, 30, 0, True),
    "ownership_not_verified": RetryPolicy(False, "none", 0, 0, False, 20, 0, True),
    "quota_insufficient": RetryPolicy(False, "none", 0, 0, True, 0, 0, False),
    "project_creation_failed": RetryPolicy(True, "generation", 2, 120, True, 5, 0, False),
    "media_upload_failed": RetryPolicy(True, "generation", 2, 120, True, 5, 0, False),
    "submission_failed_confirmed": RetryPolicy(True, "generation", 1, 300, True, 10, 0, False),
    "submission_result_unknown": RetryPolicy(False, "reconcile", 0, 0, False, 0, 0, True),
    "generation_failed": RetryPolicy(True, "generation", 1, 300, True, 10, 0, False),
    "generation_timeout": RetryPolicy(True, "reconcile", 1, 0, False, 5, 0, False),
    "download_failed": RetryPolicy(True, "download", 2, 60, False, 0, 0, False),
    "file_validation_failed": RetryPolicy(True, "download", 2, 60, False, 0, 0, False),
    "local_storage_failed": RetryPolicy(True, "download", 1, 0, False, 0, 0, False),
    "configuration_error": RetryPolicy(False, "none", 0, 0, False, 0, 0, True),
    "permanent_task_error": RetryPolicy(False, "none", 0, 0, False, 0, 0, True),
    "need_manual": RetryPolicy(False, "none", 0, 0, False, 0, 0, True),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_after(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def state_for_status(status: str | None) -> str:
    return STATUS_TO_STATE.get(status or "queued", status or "queued")


def classify_error(error_code: str | None, error_message: str | None = None) -> str:
    text = f"{error_code or ''} {error_message or ''}".lower()
    if "unusual_activity" in text or "public_error_unusual_activity" in text:
        return "account_unusual_activity"
    if "recaptcha" in text:
        return "recaptcha_required"
    if "permission_denied" in text or "403" in text:
        return "authentication_expired"
    if "quota" in text or "credit" in text:
        return "quota_insufficient"
    if "download" in text:
        return "download_failed"
    if "timeout" in text:
        return "gateway_timeout"
    if "offline" in text or "connection" in text:
        return "transient_network"
    if error_code:
        return "permanent_task_error"
    return "need_manual"


def validate_transition(old_state: str | None, new_state: str) -> bool:
    old = state_for_status(old_state)
    if new_state not in TASK_STATES:
        return False
    if old == new_state:
        return True
    allowed = {
        "queued": {"reserving", "leased", "cancelled"},
        "reserving": {"leased", "queued", "retry_wait", "failed"},
        "leased": {"submitting", "retry_wait", "need_manual", "failed"},
        "submitting": {"submission_unknown", "submitted", "need_manual", "failed"},
        "submission_unknown": {"submitted", "generating", "generated", "downloading", "need_manual"},
        "submitted": {"generating", "submission_unknown", "need_manual", "failed"},
        "generating": {"generated", "downloading", "completed", "retry_wait", "need_manual", "failed"},
        "generated": {"downloading", "completed", "retry_wait", "failed"},
        "downloading": {"completed", "retry_wait", "failed"},
        "retry_wait": {"queued", "leased", "submission_unknown", "need_manual", "failed"},
        "need_manual": set(),
        "failed": set(),
        "cancelled": set(),
        "completed": set(),
    }
    return new_state in allowed.get(old, set())


async def record_state_event(
    db,
    *,
    task_id: str,
    old_state: str | None,
    new_state: str,
    reason: str | None = None,
    error_category: str | None = None,
    error_code: str | None = None,
    account_id: str | None = None,
    worker_id: str | None = None,
    lease_id: str | None = None,
    attempt_type: str | None = None,
    attempt_number: int | None = None,
):
    await db.execute(
        """
        INSERT INTO task_state_events(event_id, task_id, old_state, new_state, reason, error_category,
          error_code, account_id, worker_id, lease_id, attempt_type, attempt_number)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            task_id,
            state_for_status(old_state),
            new_state,
            reason,
            error_category,
            error_code,
            account_id,
            worker_id,
            lease_id,
            attempt_type,
            attempt_number,
        ),
    )


async def transition_task(
    db,
    task_id: str,
    new_state: str,
    *,
    expected_state: str | None = None,
    reason: str | None = None,
    error_category: str | None = None,
    error_code: str | None = None,
    account_id: str | None = None,
    worker_id: str | None = None,
    lease_id: str | None = None,
    status: str | None = None,
    fields: dict | None = None,
):
    fields = dict(fields or {})
    cursor = await db.execute("SELECT * FROM flow_tasks WHERE task_id=?", (task_id,))
    row = await cursor.fetchone()
    if not row:
        return None
    current = dict(row)
    old_state = current.get("state") or state_for_status(current.get("status"))
    if expected_state and old_state != expected_state:
        return None
    if not validate_transition(old_state, new_state):
        return None
    sets = [
        "state=?",
        "status=COALESCE(?, status)",
        "state_version=state_version+1",
        "updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')",
    ]
    values = [new_state, status]
    for key, value in fields.items():
        sets.append(f"{key}=?")
        values.append(value)
    values.append(task_id)
    if expected_state:
        values.append(expected_state)
        predicate = "AND COALESCE(state, status)=?"
    else:
        predicate = ""
    cursor = await db.execute(f"UPDATE flow_tasks SET {', '.join(sets)} WHERE task_id=? {predicate}", tuple(values))
    if cursor.rowcount != 1:
        return None
    await record_state_event(
        db,
        task_id=task_id,
        old_state=old_state,
        new_state=new_state,
        reason=reason,
        error_category=error_category,
        error_code=error_code,
        account_id=account_id or current.get("assigned_account_id"),
        worker_id=worker_id or current.get("assigned_worker_id"),
        lease_id=lease_id or current.get("active_lease_id"),
    )
    cursor = await db.execute("SELECT * FROM flow_tasks WHERE task_id=?", (task_id,))
    return dict(await cursor.fetchone())


def available_credits(account: dict) -> int:
    credits = int(account.get("credits") or 0)
    reserved = int(account.get("reserved_credits") or 0)
    return max(credits - reserved, 0)


def account_is_schedulable(account: dict, now: str | None = None) -> tuple[bool, str | None]:
    now = now or utc_now()
    if int(account.get("manual_paused") or 0):
        return False, "manual_paused"
    cooldown_until = account.get("cooldown_until")
    if cooldown_until and cooldown_until > now:
        return False, "cooldown"
    if account.get("status") != "ready":
        return False, "not_ready"
    if account.get("current_task_id"):
        return False, "current_task_id_active"
    if account.get("lock_owner"):
        return False, "lock_active"
    return True, None


def score_account(account: dict, *, task_count_today: int = 0, total_task_count: int = 0, required_credits: int = 0) -> float:
    health = float(account.get("health_score") if account.get("health_score") is not None else 100)
    weight = float(account.get("account_weight") or 1.0)
    recent_penalty = 5.0 if account.get("last_used_at") else 0.0
    usage_penalty = task_count_today * 20.0 + total_task_count * 2.0
    failure_penalty = int(account.get("consecutive_failures") or 0) * 15.0
    credit_bonus = min(available_credits(account), 1000) / 100.0
    if available_credits(account) < required_credits:
        return -1_000_000.0
    return (health * weight) + credit_bonus - usage_penalty - failure_penalty - recent_penalty


async def usage_counts(db, account_id: str) -> tuple[int, int]:
    today = utc_now()[:10]
    cursor = await db.execute(
        "SELECT COUNT(*) FROM flow_tasks WHERE assigned_account_id=? AND substr(COALESCE(assigned_at, created_at),1,10)=?",
        (account_id, today),
    )
    today_count = int((await cursor.fetchone())[0])
    cursor = await db.execute("SELECT COUNT(*) FROM flow_tasks WHERE assigned_account_id=?", (account_id,))
    total_count = int((await cursor.fetchone())[0])
    return today_count, total_count


async def acquire_account_lease(
    db,
    *,
    task_id: str,
    account_id: str,
    worker_id: str | None,
    quota_cost: int,
    lease_seconds: float,
):
    lease_id = str(uuid.uuid4())
    expires_at = utc_after(lease_seconds)
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        cursor = await db.execute("SELECT * FROM flow_accounts WHERE account_id=?", (account_id,))
        account_row = await cursor.fetchone()
        cursor = await db.execute("SELECT * FROM flow_tasks WHERE task_id=?", (task_id,))
        task_row = await cursor.fetchone()
        if not account_row or not task_row:
            await db.execute("ROLLBACK")
            return None
        account = dict(account_row)
        task = dict(task_row)
        ok, _reason = account_is_schedulable(account)
        task_state = task.get("state") or state_for_status(task.get("status"))
        if not ok or task_state != "queued" or task.get("status") != "queued":
            await db.execute("ROLLBACK")
            return None
        if task.get("preferred_account_id") and task.get("preferred_account_id") != account_id:
            await db.execute("ROLLBACK")
            return None
        if available_credits(account) < quota_cost:
            await db.execute("ROLLBACK")
            return None
        await db.execute(
            """
            INSERT INTO account_leases(lease_id, account_id, task_id, worker_id, expires_at, status)
            VALUES(?, ?, ?, ?, ?, 'active')
            """,
            (lease_id, account_id, task_id, worker_id, expires_at),
        )
        await db.execute(
            """
            INSERT OR IGNORE INTO quota_ledger(ledger_id, account_id, task_id, lease_id, entry_type, amount, status)
            VALUES(?, ?, ?, ?, 'reserve', ?, 'active')
            """,
            (str(uuid.uuid4()), account_id, task_id, lease_id, quota_cost),
        )
        cursor = await db.execute(
            """
            UPDATE flow_accounts
            SET status='busy', current_task_id=?, lock_owner=?, lock_version=lock_version+1,
                lock_expires_at=?, last_heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                last_assigned_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), last_used_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                reserved_credits=reserved_credits+?,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=? AND current_task_id IS NULL AND lock_owner IS NULL
            """,
            (task_id, lease_id, expires_at, quota_cost, account_id),
        )
        if cursor.rowcount != 1:
            await db.execute("ROLLBACK")
            return None
        await db.execute(
            """
            UPDATE flow_tasks
            SET status='leased', state='leased', state_version=state_version+1,
                assigned_account_id=?, account_id=?, assigned_worker_id=?, assigned_runtime_instance_id=?, active_lease_id=?,
                lease_owner=?, lease_version=lease_version+1, lease_expires_at=?,
                heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), assigned_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                estimated_quota_cost=?, reserved_quota_cost=?,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=? AND status='queued' AND COALESCE(state, status)='queued'
            """,
            (account_id, account_id, worker_id, worker_id, lease_id, lease_id, expires_at, quota_cost, quota_cost, task_id),
        )
        cursor = await db.execute("SELECT changes()")
        if int((await cursor.fetchone())[0]) != 1:
            await db.execute("ROLLBACK")
            return None
        await record_state_event(
            db,
            task_id=task_id,
            old_state=task_state,
            new_state="leased",
            reason="account_lease_acquired",
            account_id=account_id,
            worker_id=worker_id,
            lease_id=lease_id,
        )
        await db.commit()
        cursor = await db.execute("SELECT * FROM flow_tasks WHERE task_id=?", (task_id,))
        return dict(await cursor.fetchone())
    except Exception:
        await db.execute("ROLLBACK")
        raise


async def heartbeat_lease(db, *, lease_id: str, lease_seconds: float):
    expires_at = utc_after(lease_seconds)
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        cursor = await db.execute(
            "UPDATE account_leases SET heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), expires_at=? WHERE lease_id=? AND status='active'",
            (expires_at, lease_id),
        )
        if cursor.rowcount != 1:
            await db.execute("ROLLBACK")
            return False
        await db.execute(
            """
            UPDATE flow_tasks SET heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), lease_expires_at=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE active_lease_id=?
            """,
            (expires_at, lease_id),
        )
        await db.execute(
            """
            UPDATE flow_accounts SET last_heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), lock_expires_at=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE lock_owner=?
            """,
            (expires_at, lease_id),
        )
        await db.commit()
        return True
    except Exception:
        await db.execute("ROLLBACK")
        raise


async def release_account_lease(
    db,
    *,
    lease_id: str,
    final_status: str,
    release_quota: bool = False,
    consume_quota: bool = False,
    remaining_credits: int | None = None,
):
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        cursor = await db.execute("SELECT * FROM account_leases WHERE lease_id=? AND status='active'", (lease_id,))
        row = await cursor.fetchone()
        if not row:
            await db.execute("ROLLBACK")
            return False
        lease = dict(row)
        cursor = await db.execute("SELECT COALESCE(SUM(amount),0) FROM quota_ledger WHERE lease_id=? AND entry_type='reserve' AND status='active'", (lease_id,))
        reserved = int((await cursor.fetchone())[0] or 0)
        await db.execute("UPDATE account_leases SET status=? WHERE lease_id=?", (final_status, lease_id))
        if release_quota and reserved:
            await db.execute(
                "INSERT OR IGNORE INTO quota_ledger(ledger_id, account_id, task_id, lease_id, entry_type, amount, status) VALUES(?, ?, ?, ?, 'release', ?, 'posted')",
                (str(uuid.uuid4()), lease["account_id"], lease["task_id"], lease_id, reserved),
            )
            await db.execute("UPDATE flow_accounts SET reserved_credits=MAX(reserved_credits-?,0) WHERE account_id=?", (reserved, lease["account_id"]))
        if consume_quota and reserved:
            await db.execute(
                "INSERT OR IGNORE INTO quota_ledger(ledger_id, account_id, task_id, lease_id, entry_type, amount, status) VALUES(?, ?, ?, ?, 'consume', ?, 'posted')",
                (str(uuid.uuid4()), lease["account_id"], lease["task_id"], lease_id, reserved),
            )
            await db.execute(
                "UPDATE flow_accounts SET reserved_credits=MAX(reserved_credits-?,0), consumed_credits=consumed_credits+?, credits=COALESCE(?, credits) WHERE account_id=?",
                (reserved, reserved, remaining_credits, lease["account_id"]),
            )
        await db.commit()
        return True
    except Exception:
        await db.execute("ROLLBACK")
        raise
