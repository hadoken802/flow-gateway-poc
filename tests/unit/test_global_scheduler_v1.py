from argparse import Namespace
import asyncio
import sqlite3
from pathlib import Path

import pytest

from gateway import crud
from gateway.config import DEFAULT_GATEWAY_DB_PATH, GatewaySettings
from gateway.db import connect
from gateway.scheduler import GatewayScheduler
from gateway.storyboard_batch import _gateway_db_path
from gateway.worker_provider import WorkerConfig, WorkerSnapshot


class Provider:
    def __init__(self, account_ids):
        self.account_ids = account_ids

    def load_workers(self):
        workers = [WorkerConfig(account_id, f"http://127.0.0.1/{account_id}", True, f"rt-{account_id}") for account_id in self.account_ids]
        candidates = [
            {
                "account_id": worker.account_id,
                "eligible": True,
                "gateway_status": "ready",
                "exclusion_reasons": [],
                "worker_api_endpoint": worker.api_url,
                "runtime_instance_id": worker.runtime_instance_id,
            }
            for worker in workers
        ]
        return WorkerSnapshot(workers, candidates, "static", "Provider", "now")


class InspectOnlyClient:
    async def inspect(self, worker):
        return {"status": "ready", "credits": 100, "extension_connected": True, "flow_key_present": True}


class FakeCompleteClient(InspectOnlyClient):
    def __init__(self, video_path: Path):
        self.video_path = video_path
        self.submits = 0
        self.polls = 0
        self.retry_downloads = 0

    async def submit_omni_video(self, worker, payload):
        self.submits += 1
        return {"job_id": "job-1", "remaining_credits": 90}

    async def get_omni_video(self, worker, worker_job_id):
        self.polls += 1
        return {"job_id": worker_job_id, "status": "completed", "video_path": str(self.video_path), "remaining_credits": 90}

    async def retry_omni_video_download(self, worker, worker_job_id):
        self.retry_downloads += 1
        return {"job_id": worker_job_id, "status": "completed", "video_path": str(self.video_path), "remaining_credits": 90}


async def make_db(tmp_path, accounts=("FLOW-001",)):
    db = await connect(tmp_path / "gateway.db")
    for account_id in accounts:
        await crud.upsert_account(db, WorkerConfig(account_id, f"http://127.0.0.1/{account_id}", True, f"rt-{account_id}"), status="ready", credits=100)
    return db


def payload(key):
    return {
        "idempotency_key": key,
        "image_path": __file__,
        "prompt": "make a short clip",
        "duration": 10,
        "aspect_ratio": "9:16",
    }


@pytest.mark.asyncio
async def test_two_schedulers_same_db_only_one_claims_same_account(tmp_path):
    db1 = await make_db(tmp_path)
    db2 = await connect(tmp_path / "gateway.db")
    task = await crud.create_task(db1, payload("same-account"))

    first = await crud.assign_task(db1, task["task_id"], "FLOW-001", "rt-1", 15)
    second = await crud.assign_task(db2, task["task_id"], "FLOW-001", "rt-1", 15)

    assert first is not None
    assert second is None
    assert (await crud.get_task(db1, task["task_id"]))["status"] == "leased"
    assert (await crud.get_account(db1, "FLOW-001"))["current_task_id"] == task["task_id"]
    await db1.close()
    await db2.close()


@pytest.mark.asyncio
async def test_one_account_ten_queued_tasks_only_one_active(tmp_path):
    db = await make_db(tmp_path)
    for index in range(10):
        await crud.create_task(db, payload(f"k-{index}"))

    claimed = []
    for task in await crud.list_tasks(db):
        result = await crud.assign_task(db, task["task_id"], "FLOW-001", "rt-1", 15)
        if result:
            claimed.append(result)

    assert len(claimed) == 1
    active = [task for task in await crud.list_tasks(db) if task["status"] != "queued"]
    assert len(active) == 1
    await db.close()


@pytest.mark.asyncio
async def test_three_accounts_thirty_tasks_max_three_active_one_per_account(tmp_path):
    db = await make_db(tmp_path, ("FLOW-001", "FLOW-002", "FLOW-003"))
    for index in range(30):
        await crud.create_task(db, payload(f"batch-{index}"))

    for account_id in ("FLOW-001", "FLOW-002", "FLOW-003"):
        task = next(task for task in await crud.list_tasks(db) if task["status"] == "queued")
        await crud.assign_task(db, task["task_id"], account_id, f"rt-{account_id}", 15)

    active = [task for task in await crud.list_tasks(db) if task["status"] == "leased"]
    assert len(active) == 3
    assert sorted(task["assigned_account_id"] for task in active) == ["FLOW-001", "FLOW-002", "FLOW-003"]
    await db.close()


@pytest.mark.asyncio
async def test_old_worker_lease_version_cannot_heartbeat_or_complete(tmp_path):
    db = await make_db(tmp_path)
    task = await crud.create_task(db, payload("fence"))
    leased = await crud.assign_task(db, task["task_id"], "FLOW-001", "rt-1", 15)
    account = await crud.get_account(db, "FLOW-001")

    ok = await crud.heartbeat(db, task["task_id"], "FLOW-001", leased["lease_owner"], leased["lease_version"] + 1, account["lock_version"])
    completed = await crud.guarded_update_task(db, task["task_id"], leased["lease_owner"], leased["lease_version"] + 1, "completed")

    assert ok is False
    assert completed is None
    assert (await crud.get_task(db, task["task_id"]))["status"] == "leased"
    await db.close()


@pytest.mark.asyncio
async def test_completed_and_account_release_are_fenced_same_transaction(tmp_path):
    db = await make_db(tmp_path)
    task = await crud.create_task(db, payload("complete-fenced"))
    leased = await crud.assign_task(db, task["task_id"], "FLOW-001", "rt-1", 15)
    account = await crud.get_account(db, "FLOW-001")

    completed = await crud.guarded_complete_real_task(db, task["task_id"], "FLOW-001", leased["lease_owner"], leased["lease_version"], account["lock_version"], "D:/out.mp4", 90)

    assert completed["status"] == "completed"
    assert completed["lease_owner"] is None
    assert completed["active_lease_id"] is None
    assert completed["actual_quota_cost"] == 15
    released = await crud.get_account(db, "FLOW-001")
    assert released["current_task_id"] is None
    assert released["lock_owner"] is None
    active_reserves = await (await db.execute(
        "SELECT COUNT(*) FROM quota_ledger WHERE task_id=? AND entry_type='reserve' AND status='active'",
        (task["task_id"],),
    )).fetchone()
    assert active_reserves[0] == 0
    await db.close()


@pytest.mark.asyncio
async def test_old_worker_cannot_release_account_or_write_download_failed(tmp_path):
    db = await make_db(tmp_path)
    task = await crud.create_task(db, payload("old-release"))
    leased = await crud.assign_task(db, task["task_id"], "FLOW-001", "rt-1", 15)
    account = await crud.get_account(db, "FLOW-001")

    result = await crud.guarded_release_account(db, task["task_id"], "FLOW-001", leased["lease_owner"], leased["lease_version"] + 1, account["lock_version"], "download_failed", error_code="x")

    assert result is None
    assert (await crud.get_task(db, task["task_id"]))["status"] == "leased"
    assert (await crud.get_account(db, "FLOW-001"))["current_task_id"] == task["task_id"]
    await db.close()


@pytest.mark.asyncio
async def test_duplicate_idempotency_key_returns_same_task(tmp_path):
    db = await make_db(tmp_path)

    first = await crud.create_task(db, payload("dup-key"))
    second = await crud.create_task(db, payload("dup-key"))

    assert second["reused"] is True
    assert second["task_id"] == first["task_id"]
    assert len(await crud.list_tasks(db)) == 1
    await db.close()


@pytest.mark.asyncio
async def test_recover_submit_in_progress_goes_submission_unknown(tmp_path):
    settings = GatewaySettings(db_path=tmp_path / "gateway.db", dry_run=False, worker_refresh_interval_seconds=999)
    scheduler = GatewayScheduler(settings, worker_client=InspectOnlyClient(), worker_provider=Provider(["FLOW-001"]))
    scheduler.db = await make_db(tmp_path)
    task = await crud.create_task(scheduler.db, payload("submit-crash"))
    leased = await crud.assign_task(scheduler.db, task["task_id"], "FLOW-001", "rt-FLOW-001", 15)
    await crud.update_task_status(scheduler.db, leased["task_id"], "submit_in_progress")

    await scheduler.recover_tasks()

    recovered = await crud.get_task(scheduler.db, task["task_id"])
    assert recovered["status"] == "submission_unknown"
    await scheduler.db.close()


@pytest.mark.asyncio
async def test_recover_project_create_in_progress_not_requeued(tmp_path):
    settings = GatewaySettings(db_path=tmp_path / "gateway.db", dry_run=False, worker_refresh_interval_seconds=999)
    scheduler = GatewayScheduler(settings, worker_client=InspectOnlyClient(), worker_provider=Provider(["FLOW-001"]))
    scheduler.db = await make_db(tmp_path)
    task = await crud.create_task(scheduler.db, payload("project-crash"))
    leased = await crud.assign_task(scheduler.db, task["task_id"], "FLOW-001", "rt-FLOW-001", 15)
    await crud.update_task_status(scheduler.db, leased["task_id"], "project_create_in_progress")

    await scheduler.recover_tasks()

    recovered = await crud.get_task(scheduler.db, task["task_id"])
    assert recovered["status"] == "project_creation_unknown"
    await scheduler.db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["manual_review", "manual_submit_required", "submission_unknown", "project_creation_unknown"])
async def test_manual_and_unknown_statuses_do_not_auto_recover(tmp_path, status):
    settings = GatewaySettings(db_path=tmp_path / "gateway.db", dry_run=False, worker_refresh_interval_seconds=999)
    scheduler = GatewayScheduler(settings, worker_client=InspectOnlyClient(), worker_provider=Provider(["FLOW-001"]))
    scheduler.db = await make_db(tmp_path)
    task = await crud.create_task(scheduler.db, payload(f"hold-{status}"))
    leased = await crud.assign_task(scheduler.db, task["task_id"], "FLOW-001", "rt-FLOW-001", 15)
    await crud.update_task_status(scheduler.db, leased["task_id"], status, worker_job_id="job-existing")

    await scheduler.recover_tasks()

    recovered = await crud.get_task(scheduler.db, task["task_id"])
    assert recovered["status"] == status
    await scheduler.db.close()


@pytest.mark.asyncio
async def test_generation_attempts_increment_atomically_with_submit_in_progress(tmp_path):
    db = await make_db(tmp_path)
    task = await crud.create_task(db, payload("attempt-gen"))
    leased = await crud.assign_task(db, task["task_id"], "FLOW-001", "rt-1", 15)
    await crud.guarded_update_task(db, task["task_id"], leased["lease_owner"], leased["lease_version"], "submit_pending")

    updated = await crud.guarded_transition(db, task["task_id"], leased["lease_owner"], leased["lease_version"], "submit_pending", "submit_in_progress", generation_attempts=("increment", 1), submission_started_at=crud.utc_now())
    stale = await crud.guarded_transition(db, task["task_id"], leased["lease_owner"], leased["lease_version"], "submit_pending", "submit_in_progress", generation_attempts=("increment", 1))

    assert updated["generation_attempts"] == 1
    assert stale is None
    assert (await crud.get_task(db, task["task_id"]))["generation_attempts"] == 1
    await db.close()


@pytest.mark.asyncio
async def test_download_attempts_increment_atomically_with_downloading(tmp_path):
    db = await make_db(tmp_path)
    task = await crud.create_task(db, payload("attempt-download"))
    leased = await crud.assign_task(db, task["task_id"], "FLOW-001", "rt-1", 15)
    await crud.guarded_update_task(db, task["task_id"], leased["lease_owner"], leased["lease_version"], "download_pending")

    updated = await crud.guarded_transition(db, task["task_id"], leased["lease_owner"], leased["lease_version"], "download_pending", "downloading", download_attempts=("increment", 1))
    stale = await crud.guarded_transition(db, task["task_id"], leased["lease_owner"], leased["lease_version"], "download_pending", "downloading", download_attempts=("increment", 1))

    assert updated["download_attempts"] == 1
    assert stale is None
    assert (await crud.get_task(db, task["task_id"]))["download_attempts"] == 1
    await db.close()


@pytest.mark.asyncio
async def test_retry_download_does_not_increment_generation_attempts(tmp_path):
    db = await make_db(tmp_path)
    task = await crud.create_task(db, payload("retry-no-gen"))
    leased = await crud.assign_task(db, task["task_id"], "FLOW-001", "rt-1", 15)
    await crud.guarded_update_task(db, task["task_id"], leased["lease_owner"], leased["lease_version"], "download_pending", worker_job_id="job-1")
    await crud.guarded_transition(db, task["task_id"], leased["lease_owner"], leased["lease_version"], "download_pending", "downloading", download_attempts=("increment", 1))

    current = await crud.get_task(db, task["task_id"])
    assert current["generation_attempts"] == 0
    assert current["download_attempts"] == 1
    await db.close()


@pytest.mark.asyncio
async def test_concurrent_idempotency_key_two_connections_create_one_task(tmp_path):
    db1 = await make_db(tmp_path)
    db2 = await connect(tmp_path / "gateway.db")

    first, second = await asyncio.gather(
        crud.create_task(db1, payload("same-key-concurrent")),
        crud.create_task(db2, payload("same-key-concurrent")),
    )

    assert first["task_id"] == second["task_id"]
    assert len(await crud.list_tasks(db1)) == 1
    await db1.close()
    await db2.close()


@pytest.mark.asyncio
async def test_old_schema_migration_preserves_data_and_is_idempotent(tmp_path):
    db_path = tmp_path / "old.db"
    with sqlite3.connect(db_path) as db:
        db.executescript(
            """
            CREATE TABLE flow_accounts (
                account_id TEXT PRIMARY KEY, api_url TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'offline', credits INTEGER, current_task_id TEXT,
                lease_until TEXT, last_health_at TEXT, last_error TEXT, last_assigned_at TEXT,
                created_at TEXT NOT NULL DEFAULT 'old', updated_at TEXT NOT NULL DEFAULT 'old'
            );
            CREATE TABLE flow_tasks (
                task_id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE, project_id TEXT,
                image_path TEXT NOT NULL, prompt TEXT NOT NULL, duration INTEGER NOT NULL, aspect_ratio TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued', assigned_account_id TEXT, assigned_runtime_instance_id TEXT,
                worker_job_id TEXT, remaining_credits INTEGER, preferred_account_id TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0, project_created_by_gateway INTEGER NOT NULL DEFAULT 0,
                project_created_at TEXT, error_code TEXT, error_message TEXT, video_path TEXT,
                manual_submit_required_at TEXT, manual_result_media_id TEXT, manual_result_operation_id TEXT,
                manual_result_source TEXT, created_at TEXT NOT NULL DEFAULT 'old', assigned_at TEXT,
                submitted_at TEXT, updated_at TEXT NOT NULL DEFAULT 'old', completed_at TEXT
            );
            INSERT INTO flow_accounts(account_id, api_url, status, credits, current_task_id) VALUES('FLOW-001','http://w','busy',50,'task-active');
            INSERT INTO flow_tasks(task_id,idempotency_key,image_path,prompt,duration,aspect_ratio,status) VALUES('task-queued','old-q','D:/x.png','p',10,'9:16','queued');
            INSERT INTO flow_tasks(task_id,idempotency_key,image_path,prompt,duration,aspect_ratio,status,assigned_account_id,worker_job_id) VALUES('task-active','old-a','D:/x.png','p',10,'9:16','processing','FLOW-001','job-1');
            INSERT INTO flow_tasks(task_id,idempotency_key,image_path,prompt,duration,aspect_ratio,status,video_path) VALUES('task-done','old-c','D:/x.png','p',10,'9:16','completed','D:/out.mp4');
            """
        )

    db = await connect(db_path)
    rows_before = await crud.list_tasks(db)
    columns = [row[1] for row in await (await db.execute("PRAGMA table_info(flow_tasks)")).fetchall()]
    indexes = [row[1] for row in await (await db.execute("PRAGMA index_list(flow_tasks)")).fetchall()]
    version = (await (await db.execute("SELECT version FROM gateway_schema_version")).fetchone())[0]
    await db.close()
    db = await connect(db_path)
    rows_after = await crud.list_tasks(db)

    assert [row["task_id"] for row in rows_before] == [row["task_id"] for row in rows_after]
    assert "lease_version" in columns
    assert "idx_one_active_task_per_account" in indexes
    assert version == 1
    assert (await crud.get_task(db, "task-active"))["status"] == "processing"
    assert (await crud.get_account(db, "FLOW-001"))["lock_version"] == 0
    await db.close()


@pytest.mark.asyncio
async def test_continuous_heartbeat_extends_task_and_account_lease(tmp_path):
    settings = GatewaySettings(db_path=tmp_path / "gateway.db", dry_run=False, heartbeat_interval_seconds=0.05, lease_duration_seconds=1)
    scheduler = GatewayScheduler(settings, worker_client=InspectOnlyClient(), worker_provider=Provider(["FLOW-001"]))
    scheduler.db = await make_db(tmp_path)
    task = await crud.create_task(scheduler.db, payload("heartbeat-extends"))
    leased = await crud.assign_task(scheduler.db, task["task_id"], "FLOW-001", "rt-FLOW-001", 15, lease_seconds=0.2)
    before_task = (await crud.get_task(scheduler.db, task["task_id"]))["lease_expires_at"]
    before_account = (await crud.get_account(scheduler.db, "FLOW-001"))["lock_expires_at"]

    hb = scheduler._start_task_heartbeat(task["task_id"], "FLOW-001", {"lease_owner": leased["lease_owner"], "lease_version": leased["lease_version"]})
    await asyncio.sleep(1.1)
    hb.cancel()
    await asyncio.gather(hb, return_exceptions=True)

    after_task = (await crud.get_task(scheduler.db, task["task_id"]))["lease_expires_at"]
    after_account = (await crud.get_account(scheduler.db, "FLOW-001"))["lock_expires_at"]
    assert after_task > before_task
    assert after_account > before_account
    await scheduler.db.close()


@pytest.mark.asyncio
async def test_heartbeat_fencing_lost_stops_loop(tmp_path):
    settings = GatewaySettings(db_path=tmp_path / "gateway.db", dry_run=False, heartbeat_interval_seconds=0.01, lease_duration_seconds=1)
    scheduler = GatewayScheduler(settings, worker_client=InspectOnlyClient(), worker_provider=Provider(["FLOW-001"]))
    scheduler.db = await make_db(tmp_path)
    task = await crud.create_task(scheduler.db, payload("heartbeat-lost"))
    leased = await crud.assign_task(scheduler.db, task["task_id"], "FLOW-001", "rt-FLOW-001", 15)

    hb = scheduler._start_task_heartbeat(task["task_id"], "FLOW-001", {"lease_owner": leased["lease_owner"], "lease_version": leased["lease_version"] + 1})
    await asyncio.sleep(0.05)

    assert task["task_id"] in scheduler._fencing_lost
    assert hb.done()
    await scheduler.db.close()


@pytest.mark.asyncio
async def test_heartbeat_task_removed_after_cancel(tmp_path):
    settings = GatewaySettings(db_path=tmp_path / "gateway.db", dry_run=False, heartbeat_interval_seconds=0.05, lease_duration_seconds=1)
    scheduler = GatewayScheduler(settings, worker_client=InspectOnlyClient(), worker_provider=Provider(["FLOW-001"]))
    scheduler.db = await make_db(tmp_path)
    task = await crud.create_task(scheduler.db, payload("heartbeat-stop"))
    leased = await crud.assign_task(scheduler.db, task["task_id"], "FLOW-001", "rt-FLOW-001", 15)

    hb = scheduler._start_task_heartbeat(task["task_id"], "FLOW-001", {"lease_owner": leased["lease_owner"], "lease_version": leased["lease_version"]})
    hb.cancel()
    await asyncio.gather(hb, return_exceptions=True)

    assert task["task_id"] not in scheduler._heartbeat_tasks
    await scheduler.db.close()


def test_storyboard_batch_defaults_to_global_gateway_db(tmp_path):
    args = Namespace(gateway_db=None, legacy_run_db=False)
    assert _gateway_db_path(args, tmp_path) == DEFAULT_GATEWAY_DB_PATH


def test_legacy_run_db_uses_run_dir_and_warns(tmp_path, capsys):
    args = Namespace(gateway_db=None, legacy_run_db=True)
    assert _gateway_db_path(args, tmp_path) == tmp_path / "gateway.db"
    assert "does not provide cross-batch global account locks" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_migration_can_run_twice_without_duplicate_columns(tmp_path):
    db = await make_db(tmp_path)
    await db.close()
    db = await connect(tmp_path / "gateway.db")
    columns = [row[1] for row in await (await db.execute("PRAGMA table_info(flow_tasks)")).fetchall()]
    assert columns.count("lease_version") == 1
    await db.close()
