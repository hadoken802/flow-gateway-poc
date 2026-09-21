"""Persisted daily queue gate; the first real task is the probe, not extra spend."""
from datetime import datetime, timezone

from .models import ACTIVE_TASK_STATUSES


class DailyProbe:
    def __init__(self, db, clock=None):
        self.db = db
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    async def initialize(self):
        await self.db.execute("""CREATE TABLE IF NOT EXISTS daily_queue_probe (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            day TEXT NOT NULL, task_id TEXT NOT NULL, started_at REAL NOT NULL,
            blocked_reason TEXT
        )""")
        await self.db.commit()

    async def _row(self):
        async with self.db.execute(
            "SELECT day,task_id,started_at,blocked_reason FROM daily_queue_probe WHERE singleton=1"
        ) as cursor:
            row = await cursor.fetchone()
        return tuple(row) if row else None

    async def _task(self, task_id):
        async with self.db.execute(
            "SELECT status,error_message FROM flow_tasks WHERE task_id=?", (task_id,)
        ) as cursor:
            row = await cursor.fetchone()
        return tuple(row) if row else None

    async def status(self):
        now = self.clock()
        today = now.astimezone().date().isoformat()
        row = await self._row()
        if not row:
            return {"state": "awaiting", "day": today, "task_id": None, "reason": None}
        day, task_id, started_at, blocked_reason = row
        result = {"day": day, "task_id": task_id, "reason": blocked_reason}
        if blocked_reason:
            return {**result, "state": "blocked"}
        since = datetime.fromtimestamp(started_at, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        async with self.db.execute("""SELECT task_id,error_message,status FROM flow_tasks
            WHERE updated_at>=? AND task_id<>? AND status IN
            ('download_failed','failed_before_remote_submit','submission_unknown','manual_review','manual_submit_required','need_manual','failed')
            ORDER BY updated_at DESC LIMIT 1""", (since, task_id)) as cursor:
            failed = await cursor.fetchone()
        if failed:
            failed_id, message, failed_state = tuple(failed)
            reason = str(message or failed_state)[:500]
            await self.db.execute("UPDATE daily_queue_probe SET task_id=?,blocked_reason=? WHERE singleton=1", (failed_id, reason))
            await self.db.commit()
            return {**result, "state": "blocked", "task_id": failed_id, "reason": reason}
        task = await self._task(task_id)
        if task and task[0] == "completed":
            if day == today:
                return {**result, "state": "passed"}
            return {"state": "awaiting", "day": today, "task_id": None, "reason": None}
        safe_wait_states = (ACTIVE_TASK_STATUSES | {"queued", "waiting_recovery"}) - {
            "submission_unknown", "project_creation_unknown"
        }
        if not task or task[0] not in safe_wait_states:
            reason = str((task[1] or task[0]) if task else "DAILY_PROBE_TASK_MISSING")[:500]
        elif now.timestamp() - started_at > 30 * 60:
            reason = "DAILY_PROBE_TIMEOUT"
        else:
            return {**result, "state": "running"}
        await self.db.execute(
            "UPDATE daily_queue_probe SET blocked_reason=? WHERE singleton=1", (reason,)
        )
        await self.db.commit()
        return {**result, "state": "blocked", "reason": reason}

    async def claim(self, task_id):
        if (await self.status())["state"] != "awaiting":
            raise RuntimeError("PROBE_ALREADY_CLAIMED")
        now = self.clock()
        await self.db.execute(
            "INSERT OR REPLACE INTO daily_queue_probe VALUES(1,?,?,?,NULL)",
            (now.astimezone().date().isoformat(), task_id, now.timestamp()),
        )
        await self.db.commit()

    async def reset(self):
        row = await self._row()
        task = await self._task(row[1]) if row else None
        if task and task[0] in ACTIVE_TASK_STATUSES | {"queued", "waiting_recovery"}:
            raise RuntimeError("PROBE_STILL_ACTIVE")
        await self.db.execute("DELETE FROM daily_queue_probe WHERE singleton=1")
        await self.db.commit()
