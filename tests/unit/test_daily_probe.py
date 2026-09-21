from datetime import datetime, timedelta, timezone

import aiosqlite
import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_state", ["download_failed", "need_manual"])
async def test_later_download_failure_revokes_daily_pass(tmp_path, failed_state):
    from gateway.daily_probe import DailyProbe
    now = datetime(2026, 9, 21, 9, tzinfo=timezone.utc)
    async with aiosqlite.connect(tmp_path / "later.db") as db:
        await db.execute("CREATE TABLE flow_tasks(task_id TEXT PRIMARY KEY,status TEXT,error_message TEXT,updated_at TEXT)")
        await db.execute("INSERT INTO flow_tasks VALUES('probe','completed',NULL,'2026-09-21T09:00:00Z')")
        probe = DailyProbe(db, clock=lambda: now)
        await probe.initialize()
        await probe.claim('probe')
        assert (await probe.status())["state"] == "passed"
        await db.execute("INSERT INTO flow_tasks VALUES('later',?,'PAGE_VIDEO_DOWNLOAD_RESOLUTION_NOT_FOUND','2026-09-21T10:00:00Z')", (failed_state,))
        result = await probe.status()
        assert result["state"] == "blocked"
        assert result["task_id"] == "later"
        assert 'RESOLUTION_NOT_FOUND' in result["reason"]


@pytest.mark.asyncio
async def test_probe_waits_for_whole_task_and_survives_restart(tmp_path):
    from gateway.daily_probe import DailyProbe

    now = datetime(2026, 9, 21, 9, tzinfo=timezone.utc)
    async with aiosqlite.connect(tmp_path / "probe.db") as db:
        await db.execute("CREATE TABLE flow_tasks(task_id TEXT PRIMARY KEY, status TEXT, error_message TEXT, updated_at TEXT DEFAULT '2026-09-21T09:00:00Z')")
        await db.execute("INSERT INTO flow_tasks(task_id,status,error_message) VALUES('one','processing',NULL)")
        probe = DailyProbe(db, clock=lambda: now)
        await probe.initialize()
        assert (await probe.status())["state"] == "awaiting"
        await probe.claim("one")
        assert (await probe.status())["state"] == "running"
        with pytest.raises(RuntimeError, match="PROBE_ALREADY_CLAIMED"):
            await probe.claim("two")
        restarted = DailyProbe(db, clock=lambda: now)
        await restarted.initialize()
        assert (await restarted.status())["task_id"] == "one"
        await db.execute("UPDATE flow_tasks SET status='downloading'")
        assert (await restarted.status())["state"] == "running"
        await db.execute("UPDATE flow_tasks SET status='completed'")
        assert (await restarted.status())["state"] == "passed"
        tomorrow = DailyProbe(db, clock=lambda: now + timedelta(days=1))
        assert (await tomorrow.status())["state"] == "awaiting"


@pytest.mark.asyncio
async def test_failure_stays_blocked_across_midnight_until_explicit_reset(tmp_path):
    from gateway.daily_probe import DailyProbe

    now = datetime(2026, 9, 21, 9, tzinfo=timezone.utc)
    async with aiosqlite.connect(tmp_path / "probe.db") as db:
        await db.execute("CREATE TABLE flow_tasks(task_id TEXT PRIMARY KEY, status TEXT, error_message TEXT, updated_at TEXT DEFAULT '2026-09-21T09:00:00Z')")
        await db.execute("INSERT INTO flow_tasks(task_id,status,error_message) VALUES('one','leased',NULL)")
        probe = DailyProbe(db, clock=lambda: now)
        await probe.initialize()
        await probe.claim("one")
        await db.execute("UPDATE flow_tasks SET status='failed', error_message='PAGE_STAGE_FAILED:upload:0'")
        result = await probe.status()
        assert result["state"] == "blocked"
        assert result["reason"] == "PAGE_STAGE_FAILED:upload:0"
        tomorrow = DailyProbe(db, clock=lambda: now + timedelta(days=1))
        assert (await tomorrow.status())["state"] == "blocked"
        await tomorrow.reset()
        assert (await tomorrow.status())["state"] == "awaiting"


@pytest.mark.asyncio
async def test_probe_has_deadline_and_does_not_release_uncertain_submission(tmp_path):
    from gateway.daily_probe import DailyProbe

    now = datetime(2026, 9, 21, 9, tzinfo=timezone.utc)
    async with aiosqlite.connect(tmp_path / "probe.db") as db:
        await db.execute("CREATE TABLE flow_tasks(task_id TEXT PRIMARY KEY, status TEXT, error_message TEXT, updated_at TEXT DEFAULT '2026-09-21T09:00:00Z')")
        await db.execute("INSERT INTO flow_tasks(task_id,status,error_message) VALUES('one','processing',NULL)")
        probe = DailyProbe(db, clock=lambda: now)
        await probe.initialize()
        await probe.claim("one")
        with pytest.raises(RuntimeError, match="PROBE_STILL_ACTIVE"):
            await probe.reset()
        overdue = DailyProbe(db, clock=lambda: now + timedelta(minutes=31))
        assert (await overdue.status())["state"] == "blocked"
        assert (await overdue.status())["reason"] == "DAILY_PROBE_TIMEOUT"
        with pytest.raises(RuntimeError, match="PROBE_STILL_ACTIVE"):
            await overdue.reset()


@pytest.mark.asyncio
async def test_unknown_or_missing_probe_result_blocks_new_work(tmp_path):
    from gateway.daily_probe import DailyProbe

    async with aiosqlite.connect(tmp_path / "probe.db") as db:
        await db.execute("CREATE TABLE flow_tasks(task_id TEXT PRIMARY KEY, status TEXT, error_message TEXT, updated_at TEXT DEFAULT '2026-09-21T09:00:00Z')")
        await db.execute("INSERT INTO flow_tasks(task_id,status,error_message) VALUES('one','submission_unknown',NULL)")
        probe = DailyProbe(db)
        await probe.initialize()
        await probe.claim("one")
        assert (await probe.status())["state"] == "blocked"


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["completed", "failed"])
async def test_scheduler_runs_only_probe_until_completed_and_blocks_failure(tmp_path, outcome):
    import asyncio
    from gateway.config import GatewaySettings
    from gateway import crud
    from tests.unit.test_gateway_dry_run import FakeRealWorkerClient, make_scheduler

    settings = GatewaySettings(db_path=tmp_path / "gateway.db", dry_run=False,
                               max_concurrency=2, daily_probe_enabled=True)
    worker = FakeRealWorkerClient({"FLOW-001": {"credits": 100}, "FLOW-002": {"credits": 100}})
    scheduler = make_scheduler(settings, worker)
    async def pending(*_):
        await asyncio.Event().wait()
    scheduler._run_real_task = pending
    await scheduler.start()
    scheduler._runner_task.cancel()
    await asyncio.gather(scheduler._runner_task, return_exceptions=True)
    try:
        first = await crud.create_task(scheduler.db, {
            "image_path": "image.png", "prompt": "test", "duration": 10,
            "aspect_ratio": "9:16", "idempotency_key": "first",
        })
        second = await crud.create_task(scheduler.db, {
            "image_path": "image.png", "prompt": "test", "duration": 10,
            "aspect_ratio": "9:16", "idempotency_key": "second",
        })
        await scheduler.schedule_once()
        assert len(scheduler.assignment_history) == 1
        probe_task_id = scheduler.assignment_history[0][0]
        waiting_task_id = next(t["task_id"] for t in (first, second) if t["task_id"] != probe_task_id)
        await scheduler.schedule_once()
        assert len(scheduler.assignment_history) == 1
        await scheduler.db.execute("UPDATE flow_tasks SET status=?,error_message=? WHERE task_id=?",
                                   (outcome, "PAGE_STAGE_FAILED:upload:0" if outcome == "failed" else None, probe_task_id))
        await scheduler.db.commit()
        await scheduler.schedule_once()
        if outcome == "failed":
            assert len(scheduler.assignment_history) == 1
            assert (await scheduler.pool_status())["daily_probe"]["state"] == "blocked"
            assert (await crud.get_task(scheduler.db, waiting_task_id))["status"] == "queued"
            await scheduler.daily_probe.reset()
            await scheduler.schedule_once()
        else:
            assert (await scheduler.pool_status())["daily_probe"]["state"] == "passed"
        assert len(scheduler.assignment_history) == 2
    finally:
        await scheduler.stop()
