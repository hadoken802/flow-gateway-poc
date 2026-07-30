import asyncio
import sqlite3

import pytest

from gateway import crud, scheduler_kernel
from gateway.config import GatewaySettings
from gateway.db import connect
from gateway.scheduler import GatewayScheduler
from gateway.worker_provider import WorkerConfig, WorkerSnapshot


def payload(key="k", preferred=None, priority=0, not_before=None):
    return {
        "idempotency_key": key,
        "image_path": "D:/input.png",
        "prompt": "move camera",
        "duration": 10,
        "aspect_ratio": "9:16",
        "preferred_account_id": preferred,
        "priority": priority,
        "not_before": not_before,
    }


async def add_account(db, account_id="FLOW-001", credits=100, **fields):
    worker = WorkerConfig(account_id, f"http://127.0.0.1/{account_id}", True, f"rt-{account_id}")
    await crud.upsert_account(db, worker, status="ready", credits=credits)
    if fields:
        await crud.update_account_controls(db, account_id, **fields)


@pytest.mark.asyncio
async def test_v2_schema_migrates_kernel_tables_and_columns(tmp_path):
    db = await connect(tmp_path / "gateway.db")
    try:
        task_cols = {row[1] for row in await (await db.execute("PRAGMA table_info(flow_tasks)")).fetchall()}
        account_cols = {row[1] for row in await (await db.execute("PRAGMA table_info(flow_accounts)")).fetchall()}
        tables = {row[0] for row in await (await db.execute("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()}
        assert {"state", "state_version", "priority", "active_lease_id", "reserved_quota_cost"} <= task_cols
        assert {"reserved_credits", "health_score", "cooldown_until", "manual_paused", "account_weight"} <= account_cols
        assert {"task_state_events", "account_leases", "quota_ledger", "task_attempts"} <= tables
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_concurrent_account_lease_allows_only_one_task(tmp_path):
    path = tmp_path / "gateway.db"
    db1 = await connect(path)
    db2 = await connect(path)
    try:
        await add_account(db1, "FLOW-001", 100)
        t1 = await crud.create_task(db1, payload("one"))
        t2 = await crud.create_task(db1, payload("two"))
        result = await asyncio.gather(
            crud.assign_task(db1, t1["task_id"], "FLOW-001", "rt-1", 15),
            crud.assign_task(db2, t2["task_id"], "FLOW-001", "rt-1", 15),
        )
        assert sum(1 for item in result if item) == 1
        active = await (await db1.execute("SELECT COUNT(*) FROM account_leases WHERE status='active'")).fetchone()
        assert active[0] == 1
    finally:
        await db1.close()
        await db2.close()


@pytest.mark.asyncio
async def test_quota_reservation_prevents_oversell_and_releases_once(tmp_path):
    db = await connect(tmp_path / "gateway.db")
    try:
        await add_account(db, "FLOW-001", 20)
        t1 = await crud.create_task(db, payload("one"))
        t2 = await crud.create_task(db, payload("two"))
        leased = await crud.assign_task(db, t1["task_id"], "FLOW-001", "rt-1", 15)
        assert leased
        account = await crud.get_account(db, "FLOW-001")
        assert account["reserved_credits"] == 15
        # Same account is active, so second lease cannot oversell.
        assert await crud.assign_task(db, t2["task_id"], "FLOW-001", "rt-1", 15) is None
        released = await crud.guarded_release_account(
            db,
            t1["task_id"],
            "FLOW-001",
            leased["lease_owner"],
            leased["lease_version"],
            account["lock_version"],
            "failed",
            error_code="x",
        )
        assert released["status"] == "failed"
        account = await crud.get_account(db, "FLOW-001")
        assert account["reserved_credits"] == 0
        releases = await (await db.execute("SELECT COUNT(*) FROM quota_ledger WHERE entry_type='release'")).fetchone()
        assert releases[0] == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_state_events_and_versions_are_written(tmp_path):
    db = await connect(tmp_path / "gateway.db")
    try:
        await add_account(db, "FLOW-001", 100)
        task = await crud.create_task(db, payload("events"))
        leased = await crud.assign_task(db, task["task_id"], "FLOW-001", "rt-1", 15)
        updated = await crud.guarded_transition(db, task["task_id"], leased["lease_owner"], leased["lease_version"], "leased", "submit_in_progress")
        assert updated["state"] == "submitting"
        assert updated["state_version"] >= 2
        rows = await (await db.execute("SELECT new_state FROM task_state_events WHERE task_id=? ORDER BY created_at", (task["task_id"],))).fetchall()
        assert [row[0] for row in rows] == ["queued", "leased", "submitting"]
    finally:
        await db.close()


def test_retry_policy_separates_generation_and_download():
    assert scheduler_kernel.RETRY_POLICIES["download_failed"].retry_stage == "download"
    assert scheduler_kernel.RETRY_POLICIES["generation_failed"].retry_stage == "generation"
    assert scheduler_kernel.RETRY_POLICIES["submission_result_unknown"].retry_stage == "reconcile"
    assert scheduler_kernel.classify_error("UPSTREAM_UNUSUAL_ACTIVITY", "recaptcha evaluation failed") == "account_unusual_activity"


@pytest.mark.asyncio
async def test_weighted_selection_skips_paused_cooldown_and_low_credit_accounts(tmp_path):
    db = await connect(tmp_path / "gateway.db")
    try:
        await add_account(db, "FLOW-001", 100, manual_paused=1)
        await add_account(db, "FLOW-002", 100, cooldown_until="2999-01-01T00:00:00Z")
        await add_account(db, "FLOW-003", 100)
        scheduler = GatewayScheduler(
            GatewaySettings(db_path=tmp_path / "gateway.db", dry_run=True),
            worker_provider=type("P", (), {"load_workers": lambda _self: WorkerSnapshot([
                WorkerConfig("FLOW-001", "http://w1", True, "rt1"),
                WorkerConfig("FLOW-002", "http://w2", True, "rt2"),
                WorkerConfig("FLOW-003", "http://w3", True, "rt3"),
            ], [], "test", "test", "now")})(),
        )
        accounts = await crud.list_accounts(db)
        selected = scheduler.select_worker({"task_id": "t"}, account_states=accounts)
        assert selected["selected_account_id"] == "FLOW-003"
        reasons = {item["account_id"]: item["reason"] for item in selected["candidates"]}
        assert reasons["FLOW-001"] == "manual_paused"
        assert reasons["FLOW-002"] == "cooldown"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_priority_and_not_before_choose_next_schedulable_task(tmp_path):
    scheduler = GatewayScheduler(GatewaySettings(db_path=tmp_path / "gateway.db", dry_run=True))
    tasks = [
        {"task_id": "low", "status": "queued", "state": "queued", "priority": 0, "created_at": "2026-01-01T00:00:00Z"},
        {"task_id": "future", "status": "queued", "state": "queued", "priority": 100, "not_before": "2999-01-01T00:00:00Z", "created_at": "2026-01-01T00:00:00Z"},
        {"task_id": "high", "status": "queued", "state": "queued", "priority": 10, "created_at": "2026-01-01T00:00:01Z"},
    ]
    assert scheduler._next_schedulable_task(tasks)["task_id"] == "high"


@pytest.mark.asyncio
async def test_submission_unknown_recovery_does_not_submit(tmp_path):
    db = await connect(tmp_path / "gateway.db")
    try:
        await add_account(db, "FLOW-001", 100)
        task = await crud.create_task(db, payload("unknown"))
        leased = await crud.assign_task(db, task["task_id"], "FLOW-001", "rt-1", 15)
        await crud.guarded_update_task(db, leased["task_id"], leased["lease_owner"], leased["lease_version"], "submission_unknown", project_id="project-1")
        class NoSubmitClient:
            async def inspect(self, worker):
                return {"status": "ready", "credits": 100, "extension_connected": True, "flow_key_present": True}

            async def submit_omni_video(self, *_args, **_kwargs):
                raise AssertionError("submission_unknown must not submit")

        provider = type("P", (), {"load_workers": lambda _self: WorkerSnapshot(
            [WorkerConfig("FLOW-001", "http://w1", True, "rt-1")],
            [],
            "test",
            "test",
            "now",
        )})()
        scheduler = GatewayScheduler(GatewaySettings(db_path=tmp_path / "gateway.db", dry_run=False), worker_client=NoSubmitClient(), worker_provider=provider)
        scheduler.db = db
        await scheduler.recover_tasks()
        recovered = await crud.get_task(db, task["task_id"])
        assert recovered["status"] == "submission_unknown"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_many_tasks_persist_without_duplicates_and_idempotency_reuses(tmp_path):
    db = await connect(tmp_path / "gateway.db")
    try:
        tasks = [await crud.create_task(db, payload(f"bulk-{index}", priority=index % 3)) for index in range(100)]
        assert len({task["task_id"] for task in tasks}) == 100
        reused = await crud.create_task(db, payload("bulk-42"))
        assert reused["reused"] is True
        count = await (await db.execute("SELECT COUNT(*) FROM flow_tasks")).fetchone()
        assert count[0] == 100
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_completed_idempotency_request_returns_original_task(tmp_path):
    db = await connect(tmp_path / "gateway.db")
    try:
        task = await crud.create_task(db, payload("same-key"))
        await crud.update_task_status(db, task["task_id"], "completed", video_path="D:/out.mp4")
        reused = await crud.create_task(db, payload("same-key"))
        assert reused["reused"] is True
        assert reused["task_id"] == task["task_id"]
        assert reused["status"] == "completed"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_download_failed_attempt_policy_does_not_increment_generation(tmp_path):
    db = await connect(tmp_path / "gateway.db")
    try:
        await add_account(db, "FLOW-001", 100)
        task = await crud.create_task(db, payload("download-separate"))
        leased = await crud.assign_task(db, task["task_id"], "FLOW-001", "rt-1", 15)
        await crud.guarded_update_task(
            db,
            task["task_id"],
            leased["lease_owner"],
            leased["lease_version"],
            "download_pending",
            worker_job_id="job-1",
            output_media_id="media-1",
        )
        await crud.guarded_transition(
            db,
            task["task_id"],
            leased["lease_owner"],
            leased["lease_version"],
            "download_pending",
            "downloading",
            download_attempts=("increment", 1),
            download_attempt_count=("increment", 1),
        )
        current = await crud.get_task(db, task["task_id"])
        assert current["generation_attempts"] == 0
        assert current["download_attempts"] == 1
        assert current["download_attempt_count"] == 1
    finally:
        await db.close()
