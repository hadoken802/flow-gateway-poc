"""Gateway SQLite connection and schema."""
import aiosqlite
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS flow_accounts (
    account_id TEXT PRIMARY KEY,
    api_url TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'offline',
    credits INTEGER,
    current_task_id TEXT,
    lock_owner TEXT,
    lock_version INTEGER NOT NULL DEFAULT 0,
    lock_expires_at TEXT,
    last_heartbeat_at TEXT,
    lease_until TEXT,
    last_health_at TEXT,
    last_error TEXT,
    last_assigned_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS flow_tasks (
    task_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    project_id TEXT,
    image_path TEXT NOT NULL,
    prompt TEXT NOT NULL,
    duration INTEGER NOT NULL,
    aspect_ratio TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    assigned_account_id TEXT,
    account_id TEXT,
    lease_owner TEXT,
    lease_version INTEGER NOT NULL DEFAULT 0,
    lease_expires_at TEXT,
    heartbeat_at TEXT,
    scheduler_instance_id TEXT,
    worker_instance_id TEXT,
    boot_id TEXT,
    assigned_runtime_instance_id TEXT,
    worker_job_id TEXT,
    output_media_id TEXT,
    workflow_id TEXT,
    operation_name TEXT,
    upstream_batch_id TEXT,
    remote_project_url TEXT,
    submission_started_at TEXT,
    submission_confirmed_at TEXT,
    remaining_credits INTEGER,
    preferred_account_id TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    generation_attempts INTEGER NOT NULL DEFAULT 0,
    download_attempts INTEGER NOT NULL DEFAULT 0,
    project_created_by_gateway INTEGER NOT NULL DEFAULT 0,
    project_created_at TEXT,
    error_code TEXT,
    error_message TEXT,
    last_error_code TEXT,
    last_error_message TEXT,
    next_retry_at TEXT,
    run_dir TEXT,
    video_path TEXT,
    manual_submit_required_at TEXT,
    manual_result_media_id TEXT,
    manual_result_operation_id TEXT,
    manual_result_source TEXT,
    resume_attempt_id TEXT,
    request_batch_id TEXT,
    extension_request_id TEXT,
    remote_http_status INTEGER,
    remote_submission_state TEXT,
    remote_result_query_state TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    assigned_at TEXT,
    submitted_at TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    completed_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_task_per_account
ON flow_tasks(assigned_account_id)
WHERE status IN ('leased','project_create_pending','project_create_in_progress','project_creation_unknown','project_created','submit_pending','submit_in_progress','submission_unknown','submitted','processing','download_pending','downloading','assigning');

CREATE TABLE IF NOT EXISTS gateway_schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
"""


async def connect(db_path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(str(db_path), isolation_level=None)
    db.row_factory = aiosqlite.Row
    await db.execute("BEGIN IMMEDIATE")
    try:
        await db.executescript(SCHEMA)
        await _migrate(db)
        await db.execute("INSERT OR IGNORE INTO gateway_schema_version(version) VALUES(1)")
        await db.commit()
    except Exception:
        await db.execute("ROLLBACK")
        raise
    return db


async def connect_readonly(db_path):
    path = Path(db_path).resolve()
    uri_path = str(path).replace("\\", "/")
    db = await aiosqlite.connect(f"file:{uri_path}?mode=ro", uri=True, isolation_level=None)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA query_only=ON")
    return db


async def _migrate(db):
    cursor = await db.execute("PRAGMA table_info(flow_tasks)")
    columns = {row[1] for row in await cursor.fetchall()}
    additions = {
        "project_id": "TEXT",
        "assigned_runtime_instance_id": "TEXT",
        "remaining_credits": "INTEGER",
        "preferred_account_id": "TEXT",
        "project_created_by_gateway": "INTEGER NOT NULL DEFAULT 0",
        "project_created_at": "TEXT",
        "manual_submit_required_at": "TEXT",
        "manual_result_media_id": "TEXT",
        "manual_result_operation_id": "TEXT",
        "manual_result_source": "TEXT",
        "account_id": "TEXT",
        "lease_owner": "TEXT",
        "lease_version": "INTEGER NOT NULL DEFAULT 0",
        "lease_expires_at": "TEXT",
        "heartbeat_at": "TEXT",
        "scheduler_instance_id": "TEXT",
        "worker_instance_id": "TEXT",
        "boot_id": "TEXT",
        "output_media_id": "TEXT",
        "workflow_id": "TEXT",
        "operation_name": "TEXT",
        "upstream_batch_id": "TEXT",
        "remote_project_url": "TEXT",
        "submission_started_at": "TEXT",
        "submission_confirmed_at": "TEXT",
        "generation_attempts": "INTEGER NOT NULL DEFAULT 0",
        "download_attempts": "INTEGER NOT NULL DEFAULT 0",
        "last_error_code": "TEXT",
        "last_error_message": "TEXT",
        "next_retry_at": "TEXT",
        "run_dir": "TEXT",
        "resume_attempt_id": "TEXT",
        "request_batch_id": "TEXT",
        "extension_request_id": "TEXT",
        "remote_http_status": "INTEGER",
        "remote_submission_state": "TEXT",
        "remote_result_query_state": "TEXT",
    }
    for name, ddl in additions.items():
        if name not in columns:
            await db.execute(f"ALTER TABLE flow_tasks ADD COLUMN {name} {ddl}")
    cursor = await db.execute("PRAGMA table_info(flow_accounts)")
    account_columns = {row[1] for row in await cursor.fetchall()}
    account_additions = {
        "lock_owner": "TEXT",
        "lock_version": "INTEGER NOT NULL DEFAULT 0",
        "lock_expires_at": "TEXT",
        "last_heartbeat_at": "TEXT",
    }
    for name, ddl in account_additions.items():
        if name not in account_columns:
            await db.execute(f"ALTER TABLE flow_accounts ADD COLUMN {name} {ddl}")
