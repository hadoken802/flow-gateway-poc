"""Gateway SQLite connection and schema."""
import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS flow_accounts (
    account_id TEXT PRIMARY KEY,
    api_url TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'offline',
    credits INTEGER,
    current_task_id TEXT,
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
    worker_job_id TEXT,
    remaining_credits INTEGER,
    preferred_account_id TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    error_message TEXT,
    video_path TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    assigned_at TEXT,
    submitted_at TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    completed_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_task_per_account
ON flow_tasks(assigned_account_id)
WHERE status IN ('assigning', 'submitted', 'processing');
"""


async def connect(db_path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(str(db_path), isolation_level=None)
    db.row_factory = aiosqlite.Row
    await db.executescript(SCHEMA)
    await _migrate(db)
    await db.commit()
    return db


async def _migrate(db):
    cursor = await db.execute("PRAGMA table_info(flow_tasks)")
    columns = {row[1] for row in await cursor.fetchall()}
    additions = {
        "project_id": "TEXT",
        "remaining_credits": "INTEGER",
        "preferred_account_id": "TEXT",
    }
    for name, ddl in additions.items():
        if name not in columns:
            await db.execute(f"ALTER TABLE flow_tasks ADD COLUMN {name} {ddl}")
