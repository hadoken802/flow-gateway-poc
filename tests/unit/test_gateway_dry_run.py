import asyncio
from pathlib import Path

import pytest


class FakeWorkerClient:
    generate_calls = 0

    def __init__(self, states):
        self.states = states

    async def inspect(self, worker):
        state = self.states[worker.account_id]
        if state.get("offline"):
            return {"status": "offline", "credits": None, "extension_connected": False, "flow_key_present": False}
        return {
            "status": "ok",
            "credits": state["credits"],
            "extension_connected": state.get("extension_connected", True),
            "flow_key_present": state.get("flow_key_present", True),
        }

    async def submit_omni_video(self, *_args, **_kwargs):
        FakeWorkerClient.generate_calls += 1
        raise AssertionError("dry run must not call real generation")


class FakeRealWorkerClient(FakeWorkerClient):
    def __init__(self, states, output_dir=None):
        super().__init__(states)
        self.submits = []
        self.jobs = {}
        self.fail_first_submit = False
        self.output_dir = output_dir
        self.retry_downloads = []
        self.return_missing_job_id = False

    async def submit_omni_video(self, worker, payload):
        self.submits.append((worker.account_id, dict(payload)))
        if self.fail_first_submit:
            self.fail_first_submit = False
            raise TimeoutError("lost response")
        if self.return_missing_job_id:
            return {"status": "accepted_without_job", "remaining_credits": self.states[worker.account_id]["credits"]}
        job_id = f"job-{payload['idempotency_key']}"
        video_path = f"D:/out/{job_id}.mp4"
        if self.output_dir:
            video_file = self.output_dir / f"{job_id}.mp4"
            video_file.parent.mkdir(parents=True, exist_ok=True)
            video_file.write_bytes(b"\x00\x00\x00\x18ftypmp42")
            video_path = str(video_file)
        self.jobs[job_id] = {
            "job_id": job_id,
            "status": "completed",
            "video_path": video_path,
            "remaining_credits": self.states[worker.account_id]["credits"] - 15,
        }
        return self.jobs[job_id]

    async def get_omni_video(self, worker, worker_job_id):
        return self.jobs[worker_job_id]

    async def retry_omni_video_download(self, worker, worker_job_id):
        self.retry_downloads.append((worker.account_id, worker_job_id))
        self.jobs[worker_job_id]["status"] = "completed"
        return self.jobs[worker_job_id]


def local_db(name):
    path = Path(".tmp") / "tests" / name / "gateway.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    return path


def make_scheduler(settings, worker_client):
    from gateway.scheduler import GatewayScheduler
    from gateway.worker_provider import StaticJsonWorkerProvider

    return GatewayScheduler(settings, worker_client=worker_client, worker_provider=StaticJsonWorkerProvider(settings.workers_path))


@pytest.mark.asyncio
async def test_gateway_config_defaults_to_8200(monkeypatch):
    from gateway.config import GatewaySettings

    monkeypatch.delenv("GATEWAY_API_PORT", raising=False)
    settings = GatewaySettings.from_env()
    assert settings.api_host == "127.0.0.1"
    assert settings.api_port == 8200
    assert settings.dry_run is True


@pytest.mark.asyncio
async def test_three_workers_are_classified_by_credits(monkeypatch):
    from gateway.config import GatewaySettings

    settings = GatewaySettings(db_path=local_db("classify"))
    states = {
        "FLOW-001": {"credits": 5},
        "FLOW-002": {"credits": 50},
        "FLOW-003": {"credits": 50},
    }
    scheduler = make_scheduler(settings, FakeWorkerClient(states))
    await scheduler.start()
    accounts = {a["account_id"]: a for a in await scheduler.list_accounts()}
    assert accounts["FLOW-001"]["status"] == "low_credits"
    assert accounts["FLOW-002"]["status"] == "ready"
    assert accounts["FLOW-003"]["status"] == "ready"
    await scheduler.stop()


@pytest.mark.asyncio
async def test_five_task_dry_run_respects_concurrency_and_completes(monkeypatch):
    from gateway.config import GatewaySettings

    settings = GatewaySettings(db_path=local_db("five_tasks"), dry_run_step_seconds=(0.01, 0.01, 0.01))
    states = {
        "FLOW-001": {"credits": 5},
        "FLOW-002": {"credits": 50},
        "FLOW-003": {"credits": 50},
    }
    scheduler = make_scheduler(settings, FakeWorkerClient(states))
    await scheduler.start()
    tasks = [
        {"idempotency_key": f"task-{i}", "image_path": f"D:/img-{i}.png", "prompt": f"prompt {i}", "duration": 10, "aspect_ratio": "9:16"}
        for i in range(5)
    ]
    created = await scheduler.create_tasks(tasks)
    assert len(created) == 5
    max_active = 0
    duplicate_account_active = False
    for _ in range(200):
        status = await scheduler.pool_status()
        max_active = max(max_active, status["active_count"])
        active_tasks = [t for t in await scheduler.list_tasks() if t["status"] in {"assigning", "submitted", "processing"}]
        active_accounts = [t["assigned_account_id"] for t in active_tasks]
        duplicate_account_active = duplicate_account_active or len(active_accounts) != len(set(active_accounts))
        if status["completed_count"] == 5:
            break
        await asyncio.sleep(0.02)
    assert (await scheduler.pool_status())["completed_count"] == 5
    assert max_active <= 2
    assert duplicate_account_active is False
    accounts = {a["account_id"]: a for a in await scheduler.list_accounts()}
    assert accounts["FLOW-001"]["credits"] == 5
    assert FakeWorkerClient.generate_calls == 0
    await scheduler.stop()


@pytest.mark.asyncio
async def test_duplicate_idempotency_key_reuses_original_task():
    from gateway.config import GatewaySettings

    settings = GatewaySettings(db_path=local_db("idempotency"))
    scheduler = make_scheduler(settings, FakeWorkerClient({
        "FLOW-001": {"credits": 5},
        "FLOW-002": {"credits": 50},
        "FLOW-003": {"credits": 50},
    }))
    await scheduler.start()
    first = await scheduler.create_task({"idempotency_key": "same", "image_path": "D:/a.png", "prompt": "one", "duration": 10, "aspect_ratio": "9:16"})
    second = await scheduler.create_task({"idempotency_key": "same", "image_path": "D:/a.png", "prompt": "one", "duration": 10, "aspect_ratio": "9:16"})
    assert first["task_id"] == second["task_id"]
    assert second["reused"] is True
    assert len(await scheduler.list_tasks()) == 1
    await scheduler.stop()


@pytest.mark.asyncio
async def test_restart_recovers_queued_and_active_tasks():
    from gateway.config import GatewaySettings

    db_path = local_db("restart")
    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    settings = GatewaySettings(db_path=db_path, dry_run_step_seconds=(0.05, 0.05, 0.05))
    scheduler = make_scheduler(settings, FakeWorkerClient(states))
    await scheduler.start()
    await scheduler.create_tasks([
        {"idempotency_key": f"restart-{i}", "image_path": f"D:/r-{i}.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}
        for i in range(3)
    ])
    await asyncio.sleep(0.07)
    await scheduler.stop()

    scheduler2 = make_scheduler(settings, FakeWorkerClient(states))
    await scheduler2.start()
    for _ in range(200):
        if (await scheduler2.pool_status())["completed_count"] == 3:
            break
        await asyncio.sleep(0.02)
    assert (await scheduler2.pool_status())["completed_count"] == 3
    await scheduler2.stop()


@pytest.mark.asyncio
async def test_worker_offline_and_recovery_keeps_same_task_id():
    from gateway.config import GatewaySettings
    states = {
        "FLOW-001": {"credits": 5},
        "FLOW-002": {"credits": 50, "offline": True},
        "FLOW-003": {"credits": 50},
    }
    settings = GatewaySettings(db_path=local_db("offline"), dry_run_step_seconds=(0.01, 0.01, 0.01))
    scheduler = make_scheduler(settings, FakeWorkerClient(states))
    await scheduler.start()
    await scheduler.refresh_workers()
    accounts = {a["account_id"]: a for a in await scheduler.list_accounts()}
    assert accounts["FLOW-002"]["status"] == "offline"
    task = await scheduler.create_task({"idempotency_key": "offline-task", "image_path": "D:/x.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16"})
    states["FLOW-003"]["offline"] = True
    await scheduler.refresh_workers()
    current = await scheduler.get_task(task["task_id"])
    assert current["status"] in {"waiting_recovery", "queued", "assigning", "submitted", "processing", "completed"}
    states["FLOW-002"]["offline"] = False
    states["FLOW-003"]["offline"] = False
    await scheduler.refresh_workers()
    for _ in range(200):
        current = await scheduler.get_task(task["task_id"])
        if current["status"] == "completed":
            break
        await asyncio.sleep(0.02)
    assert (await scheduler.get_task(task["task_id"]))["task_id"] == task["task_id"]
    await scheduler.stop()


@pytest.mark.asyncio
async def test_real_mode_posts_to_worker_and_passes_idempotency_key():
    from gateway.config import GatewaySettings

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    client = FakeRealWorkerClient(states, output_dir=Path(".tmp") / "tests" / "real_submit_outputs")
    settings = GatewaySettings(db_path=local_db("real_submit"), dry_run=False)
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    await scheduler.create_task({
        "idempotency_key": "real-1",
        "project_id": "project-a",
        "image_path": "D:/img.png",
        "prompt": "prompt",
        "duration": 10,
        "aspect_ratio": "9:16",
        "preferred_account_id": "FLOW-002",
    })
    for _ in range(100):
        task = (await scheduler.list_tasks())[0]
        if task["status"] == "completed":
            break
        await asyncio.sleep(0.02)
    assert client.submits == [("FLOW-002", {
        "idempotency_key": "real-1",
        "project_id": "project-a",
        "image_path": "D:/img.png",
        "prompt": "prompt",
        "duration": 10,
        "aspect_ratio": "9:16",
    })]
    task = (await scheduler.list_tasks())[0]
    assert task["worker_job_id"] == "job-real-1"
    assert task["assigned_account_id"] == "FLOW-002"
    assert task["video_path"].endswith("job-real-1.mp4")
    await scheduler.stop()


@pytest.mark.asyncio
async def test_dry_run_allows_missing_project_id():
    from gateway.config import GatewaySettings

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    settings = GatewaySettings(db_path=local_db("dry_missing_project"), dry_run=True, dry_run_step_seconds=(0.01, 0.01, 0.01))
    scheduler = make_scheduler(settings, FakeWorkerClient(states))
    await scheduler.start()
    task = await scheduler.create_task({"idempotency_key": "dry-no-project", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16"})
    assert task["project_id"] is None
    await scheduler.stop()


@pytest.mark.asyncio
async def test_real_mode_rejects_missing_project_id_before_database_write():
    from gateway.config import GatewaySettings

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    settings = GatewaySettings(db_path=local_db("real_missing_project"), dry_run=False)
    scheduler = make_scheduler(settings, FakeWorkerClient(states))
    await scheduler.start()
    with pytest.raises(ValueError, match="project_id_required_for_real_task"):
        await scheduler.create_task({"idempotency_key": "real-no-project", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16"})
    assert await scheduler.list_tasks() == []
    await scheduler.stop()


@pytest.mark.asyncio
async def test_real_mode_missing_worker_job_id_releases_account_for_manual_review():
    from gateway.config import GatewaySettings

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    client = FakeRealWorkerClient(states)
    client.return_missing_job_id = True
    settings = GatewaySettings(db_path=local_db("missing_worker_job"), dry_run=False)
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    await scheduler.create_task({"idempotency_key": "missing-job", "project_id": "project-a", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-002"})
    for _ in range(100):
        task = (await scheduler.list_tasks())[0]
        if task["status"] == "manual_review":
            break
        await asyncio.sleep(0.02)
    task = (await scheduler.list_tasks())[0]
    assert task["status"] == "manual_review"
    assert task["attempt_count"] == 1
    assert task["worker_job_id"] is None
    accounts = {a["account_id"]: a for a in await scheduler.list_accounts()}
    assert accounts["FLOW-002"]["current_task_id"] is None
    await scheduler.stop()


@pytest.mark.asyncio
async def test_real_mode_does_not_repost_after_worker_job_id_is_saved():
    from gateway.config import GatewaySettings

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    client = FakeRealWorkerClient(states, output_dir=Path(".tmp") / "tests" / "real_restart_outputs")
    settings = GatewaySettings(db_path=local_db("real_restart"), dry_run=False)
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    await scheduler.create_task({"idempotency_key": "real-2", "project_id": "project-a", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-002"})
    for _ in range(50):
        if client.submits:
            break
        await asyncio.sleep(0.02)
    await scheduler.stop()

    client2 = FakeRealWorkerClient(states, output_dir=Path(".tmp") / "tests" / "real_restart_outputs")
    client2.jobs = dict(client.jobs)
    scheduler2 = make_scheduler(settings, client2)
    await scheduler2.start()
    for _ in range(100):
        task = (await scheduler2.list_tasks())[0]
        if task["status"] == "completed":
            break
        await asyncio.sleep(0.02)
    assert client2.submits == []
    assert (await scheduler2.list_tasks())[0]["worker_job_id"] == "job-real-2"
    await scheduler2.stop()


@pytest.mark.asyncio
async def test_real_mode_retries_download_for_existing_worker_job_without_new_task_or_submit():
    from gateway.config import GatewaySettings
    from gateway import crud
    from gateway.db import connect

    output_dir = Path(".tmp") / "tests" / "gateway_retry_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    video_file = output_dir / "job-existing.mp4"
    video_file.write_bytes(b"\x00\x00\x00\x18ftypmp42")
    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 35}, "FLOW-003": {"credits": 35}}
    settings = GatewaySettings(db_path=local_db("gateway_retry_existing"), dry_run=False)
    db = await connect(settings.db_path)
    created = await crud.create_task(db, {
        "idempotency_key": "existing-task",
        "project_id": "project-a",
        "image_path": "D:/img.png",
        "prompt": "p",
        "duration": 10,
        "aspect_ratio": "9:16",
        "preferred_account_id": "FLOW-002",
    })
    await db.execute(
        """
        UPDATE flow_tasks
        SET status='processing', assigned_account_id='FLOW-002', worker_job_id='job-existing', attempt_count=0
        WHERE task_id=?
        """,
        (created["task_id"],),
    )
    await db.commit()
    await db.close()

    client2 = FakeRealWorkerClient(states)
    client2.jobs["job-existing"] = {
        "job_id": "job-existing",
        "status": "waiting_download",
        "video_path": str(video_file),
        "remaining_credits": 35,
    }
    scheduler2 = make_scheduler(settings, client2)
    await scheduler2.start()
    for _ in range(100):
        task = (await scheduler2.list_tasks())[0]
        if task["status"] == "completed":
            break
        await asyncio.sleep(0.02)

    tasks = await scheduler2.list_tasks()
    task = tasks[0]
    assert len(tasks) == 1
    assert client2.submits == []
    assert client2.retry_downloads == [("FLOW-002", "job-existing")]
    assert task["task_id"] == created["task_id"]
    assert task["assigned_account_id"] == "FLOW-002"
    assert task["worker_job_id"] == "job-existing"
    assert task["attempt_count"] == 0
    assert task["status"] == "completed"
    assert task["video_path"] == str(video_file)
    await scheduler2.stop()


@pytest.mark.asyncio
async def test_real_mode_recovers_manual_review_task_with_existing_worker_job():
    from gateway.config import GatewaySettings
    from gateway import crud
    from gateway.db import connect

    output_dir = Path(".tmp") / "tests" / "gateway_manual_review_retry_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    video_file = output_dir / "job-manual.mp4"
    video_file.write_bytes(b"\x00\x00\x00\x18ftypmp42")
    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 35}, "FLOW-003": {"credits": 35}}
    settings = GatewaySettings(db_path=local_db("gateway_manual_review_retry"), dry_run=False)
    db = await connect(settings.db_path)
    created = await crud.create_task(db, {
        "idempotency_key": "manual-existing-task",
        "project_id": "project-a",
        "image_path": "D:/img.png",
        "prompt": "p",
        "duration": 10,
        "aspect_ratio": "9:16",
        "preferred_account_id": "FLOW-003",
    })
    await db.execute(
        """
        UPDATE flow_tasks
        SET status='manual_review', assigned_account_id='FLOW-003', worker_job_id='job-manual',
            attempt_count=0, error_code='missing_video_path'
        WHERE task_id=?
        """,
        (created["task_id"],),
    )
    await db.commit()
    await db.close()

    client = FakeRealWorkerClient(states)
    client.jobs["job-manual"] = {
        "job_id": "job-manual",
        "status": "waiting_download",
        "video_path": str(video_file),
        "remaining_credits": 35,
    }
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    for _ in range(100):
        task = (await scheduler.list_tasks())[0]
        if task["status"] == "completed":
            break
        await asyncio.sleep(0.02)

    task = (await scheduler.list_tasks())[0]
    assert client.submits == []
    assert client.retry_downloads == [("FLOW-003", "job-manual")]
    assert task["task_id"] == created["task_id"]
    assert task["assigned_account_id"] == "FLOW-003"
    assert task["worker_job_id"] == "job-manual"
    assert task["attempt_count"] == 0
    assert task["status"] == "completed"
    assert task["video_path"] == str(video_file)
    await scheduler.stop()


@pytest.mark.asyncio
async def test_real_mode_retries_same_idempotency_key_after_lost_http_response():
    from gateway.config import GatewaySettings

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    client = FakeRealWorkerClient(states, output_dir=Path(".tmp") / "tests" / "lost_response_outputs")
    client.fail_first_submit = True
    settings = GatewaySettings(db_path=local_db("lost_response"), dry_run=False)
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    await scheduler.create_task({"idempotency_key": "lost-1", "project_id": "project-a", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-002"})
    for _ in range(200):
        task = (await scheduler.list_tasks())[0]
        if task["status"] == "completed":
            break
        await asyncio.sleep(0.03)
    task = (await scheduler.list_tasks())[0]
    assert [payload["idempotency_key"] for _, payload in client.submits] == ["lost-1"]
    assert task["attempt_count"] == 1
    assert task["status"] == "manual_review"
    assert task["worker_job_id"] is None
    accounts = {a["account_id"]: a for a in await scheduler.list_accounts()}
    assert accounts["FLOW-002"]["current_task_id"] is None
    await scheduler.stop()


@pytest.mark.asyncio
async def test_manual_review_without_worker_job_id_is_not_requeued_on_restart():
    from gateway.config import GatewaySettings
    from gateway import crud
    from gateway.db import connect

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    settings = GatewaySettings(db_path=local_db("manual_review_no_job"), dry_run=False)
    db = await connect(settings.db_path)
    created = await crud.create_task(db, {
        "idempotency_key": "manual-no-job",
        "project_id": "project-a",
        "image_path": "D:/img.png",
        "prompt": "p",
        "duration": 10,
        "aspect_ratio": "9:16",
        "preferred_account_id": "FLOW-002",
    })
    await db.execute(
        "UPDATE flow_tasks SET status='manual_review', assigned_account_id='FLOW-002', attempt_count=1 WHERE task_id=?",
        (created["task_id"],),
    )
    await db.commit()
    await db.close()

    client = FakeRealWorkerClient(states)
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    task = (await scheduler.list_tasks())[0]
    assert task["status"] == "manual_review"
    assert client.submits == []
    await scheduler.stop()


@pytest.mark.asyncio
async def test_canary_two_tasks_bind_flow_002_and_flow_003_without_double_account():
    from gateway.config import GatewaySettings

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    client = FakeRealWorkerClient(states, output_dir=Path(".tmp") / "tests" / "canary_outputs")
    settings = GatewaySettings(db_path=local_db("canary"), dry_run=False, canary_only=True, canary_limit=2)
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    await scheduler.create_tasks([
        {"idempotency_key": "canary-1", "project_id": "project-a", "image_path": "D:/1.png", "prompt": "p1", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-002"},
        {"idempotency_key": "canary-2", "project_id": "project-b", "image_path": "D:/2.png", "prompt": "p2", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-003"},
    ])
    with pytest.raises(ValueError):
        await scheduler.create_task({"idempotency_key": "canary-3", "project_id": "project-c", "image_path": "D:/3.png", "prompt": "p3", "duration": 10, "aspect_ratio": "9:16"})
    for _ in range(100):
        if (await scheduler.pool_status())["completed_count"] == 2:
            break
        await asyncio.sleep(0.02)
    tasks = {t["idempotency_key"]: t for t in await scheduler.list_tasks()}
    assert tasks["canary-1"]["assigned_account_id"] == "FLOW-002"
    assert tasks["canary-2"]["assigned_account_id"] == "FLOW-003"
    assert len(client.submits) == 2
    assert {account for account, _ in client.submits} == {"FLOW-002", "FLOW-003"}
    await scheduler.stop()


def test_gateway_submit_timeout_defaults_and_env_override(monkeypatch):
    from gateway.config import GatewaySettings
    from gateway.worker_client import WorkerClient

    monkeypatch.delenv("GATEWAY_WORKER_SUBMIT_TIMEOUT_SECONDS", raising=False)
    settings = GatewaySettings.from_env()
    assert settings.worker_submit_timeout_seconds == 300.0
    assert WorkerClient().submit_timeout_seconds == 300.0

    monkeypatch.setenv("GATEWAY_WORKER_SUBMIT_TIMEOUT_SECONDS", "45")
    assert GatewaySettings.from_env().worker_submit_timeout_seconds == 45.0

    monkeypatch.setenv("GATEWAY_WORKER_SUBMIT_TIMEOUT_SECONDS", "0")
    with pytest.raises(ValueError, match="GATEWAY_WORKER_SUBMIT_TIMEOUT_SECONDS"):
        GatewaySettings.from_env()


def test_canary_script_uses_single_run_isolated_database():
    text = Path("start_gateway_real_canary.bat").read_text(encoding="utf-8")
    assert "set POOL_MAX_CONCURRENCY=1" in text
    assert "set CANARY_LIMIT=1" in text
    assert "set REAL_SUBMIT_MAX_ATTEMPTS=1" in text
    assert "set GATEWAY_WORKER_SUBMIT_TIMEOUT_SECONDS=300" in text
    assert "set FLOWKIT_GATEWAY_WORKER_SOURCE=runtime_registry" in text
    assert "flow024-real-canary-" in text
    assert "set GATEWAY_DB_PATH=%CANARY_RUN_DIR%\\gateway.db" in text
    assert "D:\\Codex\\projects\\flow_gateway_poc\\data\\gateway.db" not in text
