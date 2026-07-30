"""Gateway database helpers."""
import uuid
from datetime import datetime, timedelta, timezone

from . import scheduler_kernel


LEASE_SECONDS = 15 * 60


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def lease_deadline(seconds: float = LEASE_SECONDS) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


async def upsert_account(db, worker, status="offline", credits=None, last_error=None, quota_source=None, quota_confidence=None):
    await db.execute(
        """
        INSERT INTO flow_accounts(account_id, api_url, enabled, status, credits, credits_total, last_error, quota_source, quota_confidence)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(account_id) DO UPDATE SET
          api_url=excluded.api_url,
          enabled=excluded.enabled,
          status=excluded.status,
          credits=excluded.credits,
          credits_total=COALESCE(excluded.credits_total, flow_accounts.credits_total),
          last_error=excluded.last_error,
          quota_updated_at=CASE
            WHEN excluded.quota_confidence='live' THEN strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            ELSE flow_accounts.quota_updated_at
          END,
          quota_source=COALESCE(excluded.quota_source, flow_accounts.quota_source),
          quota_confidence=COALESCE(excluded.quota_confidence, flow_accounts.quota_confidence),
          last_health_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
          updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        """,
        (
            worker.account_id,
            worker.api_url,
            int(worker.enabled),
            status,
            credits,
            credits,
            last_error,
            quota_source,
            quota_confidence,
        ),
    )
    await db.commit()


async def list_accounts(db):
    cursor = await db.execute("SELECT * FROM flow_accounts ORDER BY account_id")
    return [dict(row) for row in await cursor.fetchall()]


async def get_account(db, account_id):
    cursor = await db.execute("SELECT * FROM flow_accounts WHERE account_id=?", (account_id,))
    row = await cursor.fetchone()
    return dict(row) if row else None


async def account_history_counts(db, account_id):
    task_cursor = await db.execute(
        "SELECT COUNT(*) FROM flow_tasks WHERE account_id=? OR assigned_account_id=? OR preferred_account_id=?",
        (account_id, account_id, account_id),
    )
    task_count = int((await task_cursor.fetchone())[0])
    lease_cursor = await db.execute("SELECT COUNT(*) FROM account_leases WHERE account_id=?", (account_id,))
    lease_count = int((await lease_cursor.fetchone())[0])
    quota_cursor = await db.execute("SELECT COUNT(*) FROM quota_ledger WHERE account_id=?", (account_id,))
    quota_count = int((await quota_cursor.fetchone())[0])
    attempt_cursor = await db.execute("SELECT COUNT(*) FROM task_attempts WHERE account_id=?", (account_id,))
    attempt_count = int((await attempt_cursor.fetchone())[0])
    event_cursor = await db.execute("SELECT COUNT(*) FROM task_state_events WHERE account_id=?", (account_id,))
    event_count = int((await event_cursor.fetchone())[0])
    return {
        "flow_tasks": task_count,
        "account_leases": lease_count,
        "quota_ledger": quota_count,
        "task_attempts": attempt_count,
        "task_state_events": event_count,
    }


async def delete_orphan_account(db, account_id, *, known_registry_account_ids):
    account = await get_account(db, account_id)
    if not account:
        return {"ok": False, "account_id": account_id, "result": "not_found"}
    if account_id in set(known_registry_account_ids):
        return {"ok": False, "account_id": account_id, "result": "in_registry"}
    counts = await account_history_counts(db, account_id)
    if any(counts.values()):
        return {"ok": False, "account_id": account_id, "result": "has_history", "history_counts": counts}
    await db.execute("DELETE FROM flow_accounts WHERE account_id=?", (account_id,))
    await db.commit()
    return {"ok": True, "account_id": account_id, "result": "deleted", "history_counts": counts}


async def update_account_controls(db, account_id, **fields):
    allowed = {
        "manual_paused",
        "manual_pause_reason",
        "cooldown_until",
        "cooldown_reason",
        "account_weight",
        "credits",
        "credits_total",
        "quota_source",
        "quota_confidence",
        "health_score",
    }
    updates = {key: value for key, value in fields.items() if key in allowed}
    if not updates:
        return await get_account(db, account_id)
    sets = ["updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"]
    values = []
    for key, value in updates.items():
        sets.append(f"{key}=?")
        values.append(value)
    if "credits" in updates:
        sets.append("quota_updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')")
        if "credits_total" not in updates:
            sets.append("credits_total=?")
            values.append(updates["credits"])
    values.append(account_id)
    await db.execute(f"UPDATE flow_accounts SET {', '.join(sets)} WHERE account_id=?", tuple(values))
    await db.commit()
    return await get_account(db, account_id)


async def create_task(db, payload):
    task_id = str(uuid.uuid4())
    idempotency_key = payload.get("idempotency_key") or f"storyboard:{task_id}:attempt:1"
    estimated_quota_cost = int(payload.get("estimated_quota_cost") or 15)
    priority = int(payload.get("priority") or 0)
    not_before = payload.get("not_before")
    duration = int(payload.get("duration") or payload.get("seconds") or 10)
    aspect_ratio = payload.get("aspect_ratio") or "9:16"
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        cursor = await db.execute(
            "SELECT * FROM flow_tasks WHERE idempotency_key=?",
            (idempotency_key,),
        )
        existing = await cursor.fetchone()
        if existing:
            await db.commit()
            item = dict(existing)
            item["reused"] = True
            return item
        await db.execute(
            """
            INSERT INTO flow_tasks(task_id, idempotency_key, project_id, image_path, prompt, duration, aspect_ratio,
              preferred_account_id, priority, not_before, queue_status, state, estimated_quota_cost,
              external_task_id, batch_id, output_directory, output_filename, metadata_json, generation_parameters_json)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', 'queued', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id, idempotency_key, payload.get("project_id"), payload["image_path"],
                payload["prompt"], duration, aspect_ratio, payload.get("preferred_account_id"),
                priority, not_before, estimated_quota_cost,
                payload.get("external_task_id"), payload.get("batch_id"),
                payload.get("output_directory"), payload.get("output_filename"),
                payload.get("metadata_json"), payload.get("generation_parameters_json"),
            ),
        )
        await scheduler_kernel.record_state_event(
            db,
            task_id=task_id,
            old_state=None,
            new_state="queued",
            reason="task_created",
        )
        await db.commit()
    except Exception:
        await db.execute("ROLLBACK")
        raise
    item = await get_task(db, task_id)
    item["reused"] = False
    return item


async def list_tasks(db):
    cursor = await db.execute("SELECT * FROM flow_tasks ORDER BY created_at, task_id")
    return [dict(row) for row in await cursor.fetchall()]


async def list_tasks_filtered(db, *, batch_id=None, status=None, account_id=None, error_category=None):
    where = []
    values = []
    if batch_id:
        where.append("batch_id=?")
        values.append(batch_id)
    if status:
        where.append("status=?")
        values.append(status)
    if account_id:
        where.append("(assigned_account_id=? OR account_id=?)")
        values.extend([account_id, account_id])
    if error_category:
        where.append("last_error_category=?")
        values.append(error_category)
    sql = "SELECT * FROM flow_tasks"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at, task_id"
    cursor = await db.execute(sql, tuple(values))
    return [dict(row) for row in await cursor.fetchall()]


async def get_task(db, task_id):
    cursor = await db.execute("SELECT * FROM flow_tasks WHERE task_id=?", (task_id,))
    row = await cursor.fetchone()
    return dict(row) if row else None


async def list_batches(db):
    cursor = await db.execute(
        """
        SELECT batch_id,
               COUNT(*) AS task_count,
               SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed_count,
               SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_count,
               MIN(created_at) AS created_at,
               MAX(updated_at) AS updated_at
        FROM flow_tasks
        WHERE batch_id IS NOT NULL
        GROUP BY batch_id
        ORDER BY created_at DESC, batch_id
        """
    )
    return [dict(row) for row in await cursor.fetchall()]


async def set_task_manual_pause(db, task_id, paused: bool):
    cursor = await db.execute(
        """
        UPDATE flow_tasks
        SET manual_paused=?, pause_requested=?,
            updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        WHERE task_id=?
        """,
        (1 if paused else 0, 1 if paused else 0, task_id),
    )
    await db.commit()
    return await get_task(db, task_id) if cursor.rowcount == 1 else None


async def cancel_queued_task(db, task_id):
    cursor = await db.execute(
        """
        UPDATE flow_tasks
        SET status='cancelled', state='cancelled', state_version=state_version+1,
            updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        WHERE task_id=? AND status='queued'
        """,
        (task_id,),
    )
    await db.commit()
    if cursor.rowcount != 1:
        return None
    await scheduler_kernel.record_state_event(
        db,
        task_id=task_id,
        old_state="queued",
        new_state="cancelled",
        reason="task_cancelled_by_api",
    )
    await db.commit()
    return await get_task(db, task_id)


async def update_task_priority(db, task_id, priority: int):
    cursor = await db.execute(
        """
        UPDATE flow_tasks
        SET priority=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        WHERE task_id=? AND status='queued'
        """,
        (priority, task_id),
    )
    await db.commit()
    return await get_task(db, task_id) if cursor.rowcount == 1 else None


async def requeue_task(db, task_id):
    cursor = await db.execute(
        """
        UPDATE flow_tasks
        SET status='queued', state='queued', state_version=state_version+1,
            manual_paused=0, pause_requested=0,
            updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        WHERE task_id=? AND status IN ('failed','download_failed','retry_wait','need_manual','manual_review')
        """,
        (task_id,),
    )
    await db.commit()
    if cursor.rowcount != 1:
        return None
    await scheduler_kernel.record_state_event(
        db,
        task_id=task_id,
        old_state=None,
        new_state="queued",
        reason="task_requeued_by_api",
    )
    await db.commit()
    return await get_task(db, task_id)


async def get_waiting_recovery_task_for_account(db, account_id):
    cursor = await db.execute(
        """
        SELECT * FROM flow_tasks
        WHERE status='waiting_recovery' AND assigned_account_id=?
        ORDER BY assigned_at, created_at LIMIT 1
        """,
        (account_id,),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def count_tasks(db, statuses=None):
    if statuses:
        placeholders = ",".join("?" for _ in statuses)
        cursor = await db.execute(f"SELECT COUNT(*) FROM flow_tasks WHERE status IN ({placeholders})", tuple(statuses))
    else:
        cursor = await db.execute("SELECT COUNT(*) FROM flow_tasks")
    return int((await cursor.fetchone())[0])


async def assign_task(db, task_id, account_id, runtime_instance_id, credit_cost, lease_seconds=LEASE_SECONDS):
    return await scheduler_kernel.acquire_account_lease(
        db,
        task_id=task_id,
        account_id=account_id,
        worker_id=runtime_instance_id,
        quota_cost=credit_cost,
        lease_seconds=lease_seconds,
    )


async def assign_next_task(db, account_id, runtime_instance_id, credit_cost, lease_seconds=LEASE_SECONDS):
    cursor = await db.execute(
        """
        SELECT task_id FROM flow_tasks
        WHERE status='queued' AND COALESCE(state, status)='queued'
          AND (not_before IS NULL OR not_before <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
          AND (preferred_account_id IS NULL OR preferred_account_id=?)
          AND COALESCE(manual_paused, 0)=0
        ORDER BY priority DESC, created_at, task_id LIMIT 1
        """,
        (account_id,),
    )
    task = await cursor.fetchone()
    if not task:
        return None
    return await assign_task(db, task["task_id"], account_id, runtime_instance_id, credit_cost, lease_seconds)


async def mark_project_created(db, task_id, project_id):
    await db.execute(
        """
        UPDATE flow_tasks
        SET project_id=?, project_created_by_gateway=1,
            project_created_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
            updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        WHERE task_id=? AND project_id IS NULL
        """,
        (project_id, task_id),
    )
    await db.commit()
    return await get_task(db, task_id)


async def guarded_update_task(db, task_id, lease_owner, lease_version, status=None, **fields):
    sets = ["updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"]
    values = []
    if status is not None:
        sets.append("status=?")
        values.append(status)
        sets.append("state=?")
        values.append(scheduler_kernel.state_for_status(status))
        sets.append("state_version=state_version+1")
    for key, value in fields.items():
        sets.append(f"{key}=?")
        values.append(value)
    values.extend([task_id, lease_owner, lease_version])
    cursor = await db.execute(
        f"""
        UPDATE flow_tasks
        SET {', '.join(sets)}
        WHERE task_id=? AND lease_owner=? AND lease_version=?
        """,
        tuple(values),
    )
    await db.commit()
    if cursor.rowcount != 1:
        return None
    if status is not None:
        await scheduler_kernel.record_state_event(
            db,
            task_id=task_id,
            old_state=None,
            new_state=scheduler_kernel.state_for_status(status),
            reason="guarded_update_task",
            lease_id=lease_owner,
        )
        await db.commit()
    return await get_task(db, task_id)


async def guarded_transition(db, task_id, lease_owner, lease_version, from_status, to_status, **fields):
    sets = ["status=?", "state=?", "state_version=state_version+1", "updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"]
    values = [to_status, scheduler_kernel.state_for_status(to_status)]
    for key, value in fields.items():
        if isinstance(value, tuple) and len(value) == 2 and value[0] == "increment":
            sets.append(f"{key}={key}+?")
            values.append(value[1])
        else:
            sets.append(f"{key}=?")
            values.append(value)
    values.extend([task_id, lease_owner, lease_version, from_status])
    cursor = await db.execute(
        f"""
        UPDATE flow_tasks
        SET {', '.join(sets)}
        WHERE task_id=? AND lease_owner=? AND lease_version=? AND status=?
        """,
        tuple(values),
    )
    await db.commit()
    if cursor.rowcount != 1:
        return None
    await scheduler_kernel.record_state_event(
        db,
        task_id=task_id,
        old_state=scheduler_kernel.state_for_status(from_status),
        new_state=scheduler_kernel.state_for_status(to_status),
        reason="guarded_transition",
        lease_id=lease_owner,
    )
    await db.commit()
    return await get_task(db, task_id)


async def heartbeat(db, task_id, account_id, lease_owner, lease_version, lock_version, lease_seconds=LEASE_SECONDS):
    deadline = lease_deadline(lease_seconds)
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        cursor = await db.execute(
            """
            UPDATE flow_tasks
            SET heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), lease_expires_at=?,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=? AND lease_owner=? AND lease_version=?
            """,
            (deadline, task_id, lease_owner, lease_version),
        )
        if cursor.rowcount != 1:
            await db.execute("ROLLBACK")
            return False
        cursor = await db.execute(
            """
            UPDATE flow_accounts
            SET last_heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), lock_expires_at=?,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=? AND current_task_id=? AND lock_owner=? AND lock_version=?
            """,
            (deadline, account_id, task_id, lease_owner, lock_version),
        )
        if cursor.rowcount != 1:
            await db.execute("ROLLBACK")
            return False
        if lease_owner:
            await db.execute(
                """
                UPDATE account_leases
                SET heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), expires_at=?
                WHERE lease_id=? AND status='active'
                """,
                (deadline, lease_owner),
            )
        await db.commit()
        return True
    except Exception:
        await db.execute("ROLLBACK")
        raise


async def acquire_manual_submit_resume(db, task_id, lease_seconds=LEASE_SECONDS, worker_instance_id=None, boot_id=None):
    owner = str(uuid.uuid4())
    deadline = lease_deadline(lease_seconds)
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        cursor = await db.execute(
            """
            SELECT t.*, a.lock_version AS account_lock_version, a.current_task_id, a.status AS account_status
            FROM flow_tasks t
            JOIN flow_accounts a ON a.account_id=t.account_id
            WHERE t.task_id=?
            """,
            (task_id,),
        )
        row = await cursor.fetchone()
        if not row:
            await db.commit()
            return None
        item = dict(row)
        if item.get("status") != "manual_submit_required":
            await db.commit()
            return None
        if item.get("error_code") != "UPSTREAM_UNUSUAL_ACTIVITY":
            await db.commit()
            return None
        if item.get("current_task_id") != task_id or item.get("account_status") not in {"busy", "locked"}:
            await db.commit()
            return None
        account_id = item.get("account_id")
        cursor = await db.execute(
            """
            UPDATE flow_tasks
            SET status='submit_in_progress',
                lease_owner=?, lease_version=lease_version+1, lease_expires_at=?,
                heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                worker_instance_id=COALESCE(?, worker_instance_id),
                boot_id=COALESCE(?, boot_id),
                generation_attempts=generation_attempts+1,
                attempt_count=attempt_count+1,
                submission_started_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                last_error_code=error_code,
                last_error_message=error_message,
                error_code=NULL,
                error_message=NULL,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=? AND status='manual_submit_required' AND error_code='UPSTREAM_UNUSUAL_ACTIVITY'
              AND account_id=? AND output_media_id IS NULL AND workflow_id IS NULL
              AND operation_name IS NULL AND upstream_batch_id IS NULL
            """,
            (owner, deadline, worker_instance_id, boot_id, task_id, account_id),
        )
        if cursor.rowcount != 1:
            await db.execute("ROLLBACK")
            return None
        cursor = await db.execute(
            """
            UPDATE flow_accounts
            SET lock_owner=?, lock_version=lock_version+1, lock_expires_at=?,
                last_heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=? AND current_task_id=?
            """,
            (owner, deadline, account_id, task_id),
        )
        if cursor.rowcount != 1:
            await db.execute("ROLLBACK")
            return None
        await db.commit()
        return await get_task(db, task_id)
    except Exception:
        await db.execute("ROLLBACK")
        raise


async def ensure_active_lease(db, task_id, account_id, lease_seconds=LEASE_SECONDS):
    owner = str(uuid.uuid4())
    deadline = lease_deadline(lease_seconds)
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        cursor = await db.execute(
            """
            UPDATE flow_tasks
            SET lease_owner=?, lease_version=lease_version+1, lease_expires_at=?,
                heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=? AND assigned_account_id=?
            """,
            (owner, deadline, task_id, account_id),
        )
        if cursor.rowcount != 1:
            await db.execute("ROLLBACK")
            return None
        await db.execute(
            """
            INSERT OR IGNORE INTO account_leases(lease_id, account_id, task_id, worker_id, expires_at, status)
            VALUES(?, ?, ?, NULL, ?, 'active')
            """,
            (owner, account_id, task_id, deadline),
        )
        cursor = await db.execute(
            """
            UPDATE flow_accounts
            SET current_task_id=?, status='busy', lock_owner=?,
                lock_version=lock_version+1, lock_expires_at=?,
                last_heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=?
            """
            ,
            (task_id, owner, deadline, account_id),
        )
        if cursor.rowcount != 1:
            await db.execute("ROLLBACK")
            return None
        await db.commit()
        return await get_task(db, task_id)
    except Exception:
        await db.execute("ROLLBACK")
        raise


async def update_task_status(db, task_id, status, **fields):
    sets = ["status=?", "state=?", "state_version=state_version+1", "updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"]
    values = [status, scheduler_kernel.state_for_status(status)]
    for key, value in fields.items():
        sets.append(f"{key}=?")
        values.append(value)
    values.append(task_id)
    await db.execute(f"UPDATE flow_tasks SET {', '.join(sets)} WHERE task_id=?", tuple(values))
    await db.commit()
    await scheduler_kernel.record_state_event(
        db,
        task_id=task_id,
        old_state=None,
        new_state=scheduler_kernel.state_for_status(status),
        reason="update_task_status",
        error_category=fields.get("last_error_category") or fields.get("error_category"),
        error_code=fields.get("error_code") or fields.get("last_error_code"),
    )
    await db.commit()
    return await get_task(db, task_id)


async def mark_submitted(db, task_id, worker_job_id, **fields):
    fields["worker_job_id"] = worker_job_id
    sets = ["status='submitted'", "worker_job_id=?", "submitted_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')", "updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"]
    values = [worker_job_id]
    for key, value in fields.items():
        sets.append(f"{key}=?")
        values.append(value)
    values.append(task_id)
    await db.execute(f"UPDATE flow_tasks SET {', '.join(sets)} WHERE task_id=?", tuple(values))
    await db.commit()
    return await get_task(db, task_id)


async def guarded_release_account(db, task_id, account_id, expected_lease_owner, expected_lease_version, expected_lock_version, status, **fields):
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        sets = ["status=?", "state=?", "state_version=state_version+1", "updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"]
        values = [status, scheduler_kernel.state_for_status(status)]
        for key, value in fields.items():
            sets.append(f"{key}=?")
            values.append(value)
        values.extend([task_id, expected_lease_owner, expected_lease_version])
        cursor = await db.execute(
            f"""
            UPDATE flow_tasks
            SET {', '.join(sets)}
            WHERE task_id=? AND lease_owner=? AND lease_version=?
            """,
            tuple(values),
        )
        if cursor.rowcount != 1:
            await db.execute("ROLLBACK")
            return None
        quota_release = status in {"failed", "failed_before_remote_submit", "cancelled", "manual_review", "manual_submit_required", "download_failed", "retry_wait"}
        quota_consume = status == "completed"
        reserved = 0
        if expected_lease_owner:
            await db.execute(
                "UPDATE account_leases SET status=? WHERE lease_id=? AND status='active'",
                ("completed" if quota_consume else "released", expected_lease_owner),
            )
            cursor = await db.execute(
                "SELECT COALESCE(SUM(amount),0) FROM quota_ledger WHERE lease_id=? AND entry_type='reserve' AND status='active'",
                (expected_lease_owner,),
            )
            reserved = int((await cursor.fetchone())[0] or 0)
            if reserved and quota_release:
                await db.execute(
                    "INSERT OR IGNORE INTO quota_ledger(ledger_id, account_id, task_id, lease_id, entry_type, amount, status) VALUES(?, ?, ?, ?, 'release', ?, 'posted')",
                    (str(uuid.uuid4()), account_id, task_id, expected_lease_owner, reserved),
                )
                await db.execute(
                    "UPDATE quota_ledger SET status='released' WHERE lease_id=? AND entry_type='reserve' AND status='active'",
                    (expected_lease_owner,),
                )
            if reserved and quota_consume:
                await db.execute(
                    "INSERT OR IGNORE INTO quota_ledger(ledger_id, account_id, task_id, lease_id, entry_type, amount, status) VALUES(?, ?, ?, ?, 'consume', ?, 'posted')",
                    (str(uuid.uuid4()), account_id, task_id, expected_lease_owner, reserved),
                )
                await db.execute(
                    "UPDATE quota_ledger SET status='consumed' WHERE lease_id=? AND entry_type='reserve' AND status='active'",
                    (expected_lease_owner,),
                )
            if quota_release or quota_consume:
                await db.execute(
                    "UPDATE flow_tasks SET actual_quota_cost=?, active_lease_id=NULL WHERE task_id=?",
                    (reserved if quota_consume else 0, task_id),
                )
        cursor = await db.execute(
            """
            UPDATE flow_accounts
            SET current_task_id=NULL, lock_owner=NULL, lock_expires_at=NULL,
                status=CASE WHEN COALESCE(credits, 0) >= 15 THEN 'ready' ELSE 'low_credits' END,
                reserved_credits=MAX(reserved_credits-?,0),
                consumed_credits=consumed_credits+?,
                success_count=success_count+?,
                failure_count=failure_count+?,
                consecutive_failures=CASE WHEN ? THEN 0 ELSE consecutive_failures+1 END,
                health_score=MIN(100, MAX(0, health_score+?)),
                last_success_at=CASE WHEN ? THEN strftime('%Y-%m-%dT%H:%M:%SZ', 'now') ELSE last_success_at END,
                last_failure_at=CASE WHEN ? THEN last_failure_at ELSE strftime('%Y-%m-%dT%H:%M:%SZ', 'now') END,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=? AND current_task_id=? AND lock_owner=? AND lock_version=?
            """,
            (
                reserved if (expected_lease_owner and (quota_release or quota_consume)) else 0,
                reserved if quota_consume else 0,
                1 if quota_consume else 0,
                0 if quota_consume else 1,
                1 if quota_consume else 0,
                2 if quota_consume else -10,
                1 if quota_consume else 0,
                1 if quota_consume else 0,
                account_id,
                task_id,
                expected_lease_owner,
                expected_lock_version,
            ),
        )
        if cursor.rowcount != 1:
            await db.execute("ROLLBACK")
            return None
        await db.commit()
        await scheduler_kernel.record_state_event(
            db,
            task_id=task_id,
            old_state=None,
            new_state=scheduler_kernel.state_for_status(status),
            reason=f"release:{status}",
            error_category=fields.get("last_error_category") or fields.get("error_category"),
            error_code=fields.get("error_code"),
            account_id=account_id,
            lease_id=expected_lease_owner,
        )
        await db.commit()
        return await get_task(db, task_id)
    except Exception:
        await db.execute("ROLLBACK")
        raise


async def guarded_complete_real_task(db, task_id, account_id, lease_owner, lease_version, lock_version, video_path, remaining_credits=None):
    return await guarded_release_account(
        db,
        task_id,
        account_id,
        lease_owner,
        lease_version,
        lock_version,
        "completed",
        video_path=video_path,
        remaining_credits=remaining_credits,
        error_code=None,
        error_message=None,
        last_error_code=None,
        last_error_message=None,
        completed_at=utc_now(),
        lease_owner=None,
        lease_expires_at=None,
    )


async def guarded_release_manual_submit_required(db, task_id, account_id, lease_owner, lease_version, lock_version, **fields):
    fields.setdefault("error_code", "UPSTREAM_UNUSUAL_ACTIVITY")
    fields.setdefault("error_message", "Google requires manual submission for this Flow project")
    fields.setdefault("manual_submit_required_at", utc_now())
    fields.setdefault("lease_owner", None)
    fields.setdefault("lease_expires_at", None)
    return await guarded_release_account(
        db,
        task_id,
        account_id,
        lease_owner,
        lease_version,
        lock_version,
        "manual_submit_required",
        **fields,
    )


async def complete_task(db, task_id, account_id, credit_cost, video_path):
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        await db.execute(
            """
            UPDATE flow_tasks
            SET status='completed', state='completed', state_version=state_version+1,
                video_path=?, completed_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=?
            """,
            (video_path, task_id),
        )
        await db.execute(
            """
            UPDATE flow_accounts
            SET credits=MAX(COALESCE(credits, 0)-?, 0), current_task_id=NULL,
                lock_owner=NULL, lock_expires_at=NULL,
                reserved_credits=MAX(reserved_credits-?,0),
                consumed_credits=consumed_credits+?,
                status=CASE WHEN MAX(COALESCE(credits, 0)-?, 0) >= ? THEN 'ready' ELSE 'low_credits' END,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=? AND current_task_id=?
            """,
            (credit_cost, credit_cost, credit_cost, credit_cost, credit_cost, account_id, task_id),
        )
        await db.execute("UPDATE account_leases SET status='completed' WHERE task_id=? AND account_id=? AND status='active'", (task_id, account_id))
        await db.commit()
    except Exception:
        await db.execute("ROLLBACK")
        raise


async def complete_real_task(db, task_id, account_id, video_path, remaining_credits=None):
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        await db.execute(
            """
            UPDATE flow_tasks
            SET status='completed', state='completed', state_version=state_version+1,
                video_path=?, remaining_credits=?, error_code=NULL, error_message=NULL,
                completed_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=?
            """,
            (video_path, remaining_credits, task_id),
        )
        await db.execute(
            """
            UPDATE flow_accounts
            SET credits=COALESCE(?, credits), current_task_id=NULL,
                status=CASE WHEN COALESCE(?, credits) >= 15 THEN 'ready' ELSE 'low_credits' END,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=? AND current_task_id=?
            """,
            (remaining_credits, remaining_credits, account_id, task_id),
        )
        await db.commit()
    except Exception:
        await db.execute("ROLLBACK")
        raise


async def complete_manual_result_task(db, task_id, account_id, video_path, media_id=None, operation_id=None, source=None):
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        await db.execute(
            """
            UPDATE flow_tasks
            SET status='completed', video_path=?, manual_result_media_id=?,
                manual_result_operation_id=?, manual_result_source=?,
                error_code=NULL, error_message=NULL,
                completed_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=?
            """,
            (video_path, media_id, operation_id, source, task_id),
        )
        await db.execute(
            """
            UPDATE flow_accounts
            SET current_task_id=NULL,
                status=CASE WHEN COALESCE(credits, 0) >= 15 THEN 'ready' ELSE 'low_credits' END,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=? AND (current_task_id=? OR current_task_id IS NULL)
            """,
            (account_id, task_id),
        )
        await db.commit()
    except Exception:
        await db.execute("ROLLBACK")
        raise


async def release_task_for_manual_review(db, task_id, account_id, error_code=None, error_message=None, remaining_credits=None, status="manual_review"):
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        manual_submit_required_at = "strftime('%Y-%m-%dT%H:%M:%SZ', 'now')" if status == "manual_submit_required" else "manual_submit_required_at"
        lease_clear = ", lease_owner=NULL, lease_expires_at=NULL" if status == "manual_submit_required" else ""
        account_lock_clear = ", lock_owner=NULL, lock_expires_at=NULL" if status == "manual_submit_required" else ""
        await db.execute(
            """
            UPDATE flow_tasks
            SET status=?, error_code=?, error_message=?, remaining_credits=?,
                manual_submit_required_at={manual_submit_required_at},
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                {lease_clear}
            WHERE task_id=?
            """.format(manual_submit_required_at=manual_submit_required_at, lease_clear=lease_clear),
            (status, error_code, error_message, remaining_credits, task_id),
        )
        await db.execute(
            """
            UPDATE flow_accounts
            SET credits=COALESCE(?, credits), current_task_id=NULL,
                status=CASE WHEN COALESCE(?, credits) >= 15 THEN 'ready' ELSE 'low_credits' END,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                {account_lock_clear}
            WHERE account_id=? AND current_task_id=?
            """.format(account_lock_clear=account_lock_clear),
            (remaining_credits, remaining_credits, account_id, task_id),
        )
        await db.commit()
    except Exception:
        await db.execute("ROLLBACK")
        raise
