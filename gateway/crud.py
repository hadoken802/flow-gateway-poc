"""Gateway database helpers."""
import uuid
from datetime import datetime, timedelta, timezone


LEASE_SECONDS = 15 * 60


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def lease_deadline(seconds: float = LEASE_SECONDS) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


async def upsert_account(db, worker, status="offline", credits=None, last_error=None):
    await db.execute(
        """
        INSERT INTO flow_accounts(account_id, api_url, enabled, status, credits, last_error)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(account_id) DO UPDATE SET
          api_url=excluded.api_url,
          enabled=excluded.enabled,
          status=excluded.status,
          credits=excluded.credits,
          last_error=excluded.last_error,
          last_health_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
          updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        """,
        (worker.account_id, worker.api_url, int(worker.enabled), status, credits, last_error),
    )
    await db.commit()


async def list_accounts(db):
    cursor = await db.execute("SELECT * FROM flow_accounts ORDER BY account_id")
    return [dict(row) for row in await cursor.fetchall()]


async def get_account(db, account_id):
    cursor = await db.execute("SELECT * FROM flow_accounts WHERE account_id=?", (account_id,))
    row = await cursor.fetchone()
    return dict(row) if row else None


async def create_task(db, payload):
    task_id = str(uuid.uuid4())
    idempotency_key = payload.get("idempotency_key") or f"storyboard:{task_id}:attempt:1"
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
            INSERT INTO flow_tasks(task_id, idempotency_key, project_id, image_path, prompt, duration, aspect_ratio, preferred_account_id)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id, idempotency_key, payload.get("project_id"), payload["image_path"],
                payload["prompt"], payload["duration"], payload["aspect_ratio"], payload.get("preferred_account_id"),
            ),
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


async def get_task(db, task_id):
    cursor = await db.execute("SELECT * FROM flow_tasks WHERE task_id=?", (task_id,))
    row = await cursor.fetchone()
    return dict(row) if row else None


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
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        cursor = await db.execute(
            """
            SELECT * FROM flow_accounts
            WHERE account_id=? AND enabled=1 AND status='ready'
              AND credits>=? AND current_task_id IS NULL
            """,
            (account_id, credit_cost),
        )
        account = await cursor.fetchone()
        if not account:
            await db.commit()
            return None
        cursor = await db.execute(
            """
            SELECT * FROM flow_tasks
            WHERE task_id=? AND status='queued' AND (preferred_account_id IS NULL OR preferred_account_id=?)
            """,
            (task_id, account_id),
        )
        task = await cursor.fetchone()
        if not task:
            await db.commit()
            return None
        task_id = task["task_id"]
        owner = str(uuid.uuid4())
        deadline = lease_deadline(lease_seconds)
        cursor = await db.execute(
            """
            UPDATE flow_tasks
            SET status='leased', assigned_account_id=?, account_id=?, assigned_runtime_instance_id=?,
                lease_owner=?, lease_version=lease_version+1, lease_expires_at=?,
                heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                assigned_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=? AND status='queued'
            """,
            (account_id, account_id, runtime_instance_id, owner, deadline, task_id),
        )
        if cursor.rowcount != 1:
            await db.execute("ROLLBACK")
            return None
        cursor = await db.execute(
            """
            UPDATE flow_accounts
            SET status='busy', current_task_id=?, lock_owner=?,
                lock_version=lock_version+1, lock_expires_at=?, last_heartbeat_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                last_assigned_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=? AND current_task_id IS NULL
            """,
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


async def assign_next_task(db, account_id, runtime_instance_id, credit_cost, lease_seconds=LEASE_SECONDS):
    cursor = await db.execute(
        """
        SELECT task_id FROM flow_tasks
        WHERE status='queued' AND (preferred_account_id IS NULL OR preferred_account_id=?)
        ORDER BY created_at, task_id LIMIT 1
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
    return await get_task(db, task_id)


async def guarded_transition(db, task_id, lease_owner, lease_version, from_status, to_status, **fields):
    sets = ["status=?", "updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"]
    values = [to_status]
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
        await db.commit()
        return True
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
    sets = ["status=?", "updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"]
    values = [status]
    for key, value in fields.items():
        sets.append(f"{key}=?")
        values.append(value)
    values.append(task_id)
    await db.execute(f"UPDATE flow_tasks SET {', '.join(sets)} WHERE task_id=?", tuple(values))
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
        sets = ["status=?", "updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"]
        values = [status]
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
        cursor = await db.execute(
            """
            UPDATE flow_accounts
            SET current_task_id=NULL, lock_owner=NULL, lock_expires_at=NULL,
                status=CASE WHEN COALESCE(credits, 0) >= 15 THEN 'ready' ELSE 'low_credits' END,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=? AND current_task_id=? AND lock_owner=? AND lock_version=?
            """,
            (account_id, task_id, expected_lease_owner, expected_lock_version),
        )
        if cursor.rowcount != 1:
            await db.execute("ROLLBACK")
            return None
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


async def complete_task(db, task_id, account_id, credit_cost, video_path):
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        await db.execute(
            """
            UPDATE flow_tasks
            SET status='completed', video_path=?, completed_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=?
            """,
            (video_path, task_id),
        )
        await db.execute(
            """
            UPDATE flow_accounts
            SET credits=MAX(COALESCE(credits, 0)-?, 0), current_task_id=NULL,
                status=CASE WHEN MAX(COALESCE(credits, 0)-?, 0) >= ? THEN 'ready' ELSE 'low_credits' END,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=? AND current_task_id=?
            """,
            (credit_cost, credit_cost, credit_cost, account_id, task_id),
        )
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
            SET status='completed', video_path=?, remaining_credits=?, error_code=NULL, error_message=NULL,
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
        await db.execute(
            """
            UPDATE flow_tasks
            SET status=?, error_code=?, error_message=?, remaining_credits=?,
                manual_submit_required_at={manual_submit_required_at},
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=?
            """.format(manual_submit_required_at=manual_submit_required_at),
            (status, error_code, error_message, remaining_credits, task_id),
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
