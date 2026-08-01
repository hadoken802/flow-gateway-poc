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

CREATE TABLE IF NOT EXISTS client_files (
    file_id TEXT PRIMARY KEY,
    original_filename TEXT NOT NULL,
    stored_filename TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS task_input_media (
    input_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    file_id TEXT NOT NULL,
    uploaded_media_id TEXT,
    position INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE(task_id, position)
);

CREATE TABLE IF NOT EXISTS task_state_events (
    event_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    old_state TEXT,
    new_state TEXT NOT NULL,
    reason TEXT,
    error_category TEXT,
    error_code TEXT,
    account_id TEXT,
    worker_id TEXT,
    lease_id TEXT,
    attempt_type TEXT,
    attempt_number INTEGER,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS account_leases (
    lease_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    worker_id TEXT,
    acquired_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    heartbeat_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    expires_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_account_lease
ON account_leases(account_id)
WHERE status='active';

CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_task_lease
ON account_leases(task_id)
WHERE status='active';

CREATE TABLE IF NOT EXISTS quota_ledger (
    ledger_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    lease_id TEXT,
    entry_type TEXT NOT NULL,
    amount INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE(task_id, lease_id, entry_type, status)
);

CREATE TABLE IF NOT EXISTS task_attempts (
    attempt_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    attempt_type TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    account_id TEXT,
    worker_id TEXT,
    lease_id TEXT,
    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    finished_at TEXT,
    result TEXT,
    error_category TEXT,
    error_code TEXT,
    UNIQUE(task_id, attempt_type, attempt_number)
);
"""


async def connect(db_path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(str(db_path), isolation_level=None)
    db._flowkit_gateway_db_path = db_path
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
        "priority": "INTEGER NOT NULL DEFAULT 0",
        "queue_status": "TEXT NOT NULL DEFAULT 'queued'",
        "state": "TEXT",
        "state_version": "INTEGER NOT NULL DEFAULT 0",
        "assigned_worker_id": "TEXT",
        "active_lease_id": "TEXT",
        "not_before": "TEXT",
        "started_at": "TEXT",
        "generation_attempt_count": "INTEGER NOT NULL DEFAULT 0",
        "generation_max_attempts": "INTEGER NOT NULL DEFAULT 1",
        "download_attempt_count": "INTEGER NOT NULL DEFAULT 0",
        "download_max_attempts": "INTEGER NOT NULL DEFAULT 2",
        "estimated_quota_cost": "INTEGER NOT NULL DEFAULT 15",
        "reserved_quota_cost": "INTEGER NOT NULL DEFAULT 0",
        "actual_quota_cost": "INTEGER NOT NULL DEFAULT 0",
        "last_error_category": "TEXT",
        "recovery_required": "INTEGER NOT NULL DEFAULT 0",
        "manual_paused": "INTEGER NOT NULL DEFAULT 0",
        "external_task_id": "TEXT",
        "batch_id": "TEXT",
        "output_directory": "TEXT",
        "output_filename": "TEXT",
        "metadata_json": "TEXT",
        "generation_parameters_json": "TEXT",
        "pause_requested": "INTEGER NOT NULL DEFAULT 0",
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
        "credits_total": "INTEGER",
        "reserved_credits": "INTEGER NOT NULL DEFAULT 0",
        "consumed_credits": "INTEGER NOT NULL DEFAULT 0",
        "quota_updated_at": "TEXT",
        "quota_source": "TEXT",
        "quota_confidence": "TEXT",
        "health_score": "INTEGER NOT NULL DEFAULT 100",
        "consecutive_failures": "INTEGER NOT NULL DEFAULT 0",
        "success_count": "INTEGER NOT NULL DEFAULT 0",
        "failure_count": "INTEGER NOT NULL DEFAULT 0",
        "last_success_at": "TEXT",
        "last_failure_at": "TEXT",
        "cooldown_until": "TEXT",
        "cooldown_reason": "TEXT",
        "manual_paused": "INTEGER NOT NULL DEFAULT 0",
        "manual_pause_reason": "TEXT",
        "account_weight": "REAL NOT NULL DEFAULT 1.0",
        "last_used_at": "TEXT",
    }
    for name, ddl in account_additions.items():
        if name not in account_columns:
            await db.execute(f"ALTER TABLE flow_accounts ADD COLUMN {name} {ddl}")
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS task_state_events (
            event_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            old_state TEXT,
            new_state TEXT NOT NULL,
            reason TEXT,
            error_category TEXT,
            error_code TEXT,
            account_id TEXT,
            worker_id TEXT,
            lease_id TEXT,
            attempt_type TEXT,
            attempt_number INTEGER,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );
        CREATE TABLE IF NOT EXISTS account_leases (
            lease_id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            worker_id TEXT,
            acquired_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            heartbeat_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            expires_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active'
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_account_lease
        ON account_leases(account_id)
        WHERE status='active';
        CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_task_lease
        ON account_leases(task_id)
        WHERE status='active';
        CREATE TABLE IF NOT EXISTS quota_ledger (
            ledger_id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            lease_id TEXT,
            entry_type TEXT NOT NULL,
            amount INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(task_id, lease_id, entry_type, status)
        );
        CREATE TABLE IF NOT EXISTS task_attempts (
            attempt_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            attempt_type TEXT NOT NULL,
            attempt_number INTEGER NOT NULL,
            account_id TEXT,
            worker_id TEXT,
            lease_id TEXT,
            started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            finished_at TEXT,
            result TEXT,
            error_category TEXT,
            error_code TEXT,
            UNIQUE(task_id, attempt_type, attempt_number)
        );
        CREATE TABLE IF NOT EXISTS client_files (
            file_id TEXT PRIMARY KEY,
            original_filename TEXT NOT NULL,
            stored_filename TEXT NOT NULL,
            mime_type TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );
        CREATE TABLE IF NOT EXISTS task_input_media (
            input_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            file_id TEXT NOT NULL,
            uploaded_media_id TEXT,
            position INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(task_id, position)
        );
        """
    )
    await db.execute("UPDATE flow_tasks SET state=status WHERE state IS NULL")
    await db.execute("UPDATE flow_accounts SET credits_total=credits WHERE credits_total IS NULL AND credits IS NOT NULL")
