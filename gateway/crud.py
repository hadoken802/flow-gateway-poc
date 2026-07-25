"""Gateway database helpers."""
import uuid


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
    cursor = await db.execute(
        "SELECT * FROM flow_tasks WHERE idempotency_key=?",
        (payload["idempotency_key"],),
    )
    existing = await cursor.fetchone()
    if existing:
        item = dict(existing)
        item["reused"] = True
        return item
    task_id = str(uuid.uuid4())
    await db.execute(
        """
        INSERT INTO flow_tasks(task_id, idempotency_key, project_id, image_path, prompt, duration, aspect_ratio, preferred_account_id)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id, payload["idempotency_key"], payload.get("project_id"), payload["image_path"],
            payload["prompt"], payload["duration"], payload["aspect_ratio"], payload.get("preferred_account_id"),
        ),
    )
    await db.commit()
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


async def assign_next_task(db, account_id, credit_cost):
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
            WHERE status='queued' AND (preferred_account_id IS NULL OR preferred_account_id=?)
            ORDER BY created_at, task_id LIMIT 1
            """,
            (account_id,),
        )
        task = await cursor.fetchone()
        if not task:
            await db.commit()
            return None
        task_id = task["task_id"]
        await db.execute(
            """
            UPDATE flow_tasks
            SET status='assigning', assigned_account_id=?, assigned_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=? AND status='queued'
            """,
            (account_id, task_id),
        )
        await db.execute(
            """
            UPDATE flow_accounts
            SET status='busy', current_task_id=?, last_assigned_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=? AND current_task_id IS NULL
            """,
            (task_id, account_id),
        )
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
            WHERE account_id=?
            """,
            (credit_cost, credit_cost, credit_cost, account_id),
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
            WHERE account_id=?
            """,
            (remaining_credits, remaining_credits, account_id),
        )
        await db.commit()
    except Exception:
        await db.execute("ROLLBACK")
        raise


async def release_task_for_manual_review(db, task_id, account_id, error_code=None, error_message=None, remaining_credits=None):
    await db.commit()
    await db.execute("BEGIN IMMEDIATE")
    try:
        await db.execute(
            """
            UPDATE flow_tasks
            SET status='manual_review', error_code=?, error_message=?, remaining_credits=?,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE task_id=?
            """,
            (error_code, error_message, remaining_credits, task_id),
        )
        await db.execute(
            """
            UPDATE flow_accounts
            SET credits=COALESCE(?, credits), current_task_id=NULL,
                status=CASE WHEN COALESCE(?, credits) >= 15 THEN 'ready' ELSE 'low_credits' END,
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE account_id=?
            """,
            (remaining_credits, remaining_credits, account_id),
        )
        await db.commit()
    except Exception:
        await db.execute("ROLLBACK")
        raise
