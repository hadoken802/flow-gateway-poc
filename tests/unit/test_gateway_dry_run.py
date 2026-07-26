import asyncio
import json
import tempfile
import uuid
from pathlib import Path

import pytest

RUN_ROOT = Path(tempfile.gettempdir()) / "flowkit-gateway-tests" / uuid.uuid4().hex


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
        self.projects = []
        self.fail_project_create = False
        self.gets = []

    async def create_project(self, worker, payload):
        self.projects.append((worker.account_id, dict(payload)))
        if self.fail_project_create:
            raise TimeoutError("project create failed")
        return {"id": f"project-{worker.account_id}-{len(self.projects)}"}

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
        self.gets.append((worker.account_id, worker_job_id))
        return self.jobs[worker_job_id]

    async def retry_omni_video_download(self, worker, worker_job_id):
        self.retry_downloads.append((worker.account_id, worker_job_id))
        self.jobs[worker_job_id]["status"] = "completed"
        return self.jobs[worker_job_id]


def local_db(name):
    path = RUN_ROOT / name / "gateway.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    return path


def make_scheduler(settings, worker_client):
    from gateway.scheduler import GatewayScheduler
    from gateway.worker_provider import StaticJsonWorkerProvider

    return GatewayScheduler(settings, worker_client=worker_client, worker_provider=StaticJsonWorkerProvider(settings.workers_path))


def write_workers(path, account_ids):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([
        {"account_id": account_id, "api_url": f"http://127.0.0.1:{8100 + idx}", "enabled": True, "runtime_instance_id": f"runtime-{account_id}"}
        for idx, account_id in enumerate(account_ids)
    ]), encoding="utf-8")


@pytest.mark.asyncio
async def test_gateway_config_defaults_to_8200(monkeypatch):
    from gateway.config import GatewaySettings

    monkeypatch.delenv("GATEWAY_API_PORT", raising=False)
    settings = GatewaySettings.from_env()
    assert settings.api_host == "127.0.0.1"
    assert settings.api_port == 8200
    assert settings.dry_run is True


@pytest.mark.asyncio
async def test_scheduler_start_records_startup_stages(caplog):
    from gateway.config import GatewaySettings

    caplog.set_level("INFO", logger="gateway.scheduler")
    settings = GatewaySettings(db_path=local_db("startup_logs"), dry_run_step_seconds=(0.01, 0.01, 0.01))
    scheduler = make_scheduler(settings, FakeWorkerClient({
        "FLOW-001": {"credits": 5},
        "FLOW-002": {"credits": 50},
        "FLOW-003": {"credits": 50},
    }))
    await scheduler.start()
    await scheduler.stop()
    text = caplog.text
    assert "database_connect_started" in text
    assert "database_connect_completed" in text
    assert "accounts_upsert_started" in text
    assert "accounts_upsert_completed" in text
    assert "refresh_workers_started" in text
    assert "refresh_workers_completed" in text
    assert "recover_tasks_started" in text
    assert "recover_tasks_completed" in text
    assert "scheduler_loop_started" in text


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
async def test_three_ready_accounts_assign_three_tasks_and_leave_fourth_queued():
    from gateway.config import GatewaySettings

    workers = RUN_ROOT / "three_ready_workers.json"
    write_workers(workers, ["FLOW-024", "FLOW-025", "FLOW-026"])
    states = {account_id: {"credits": 50} for account_id in ["FLOW-024", "FLOW-025", "FLOW-026"]}
    settings = GatewaySettings(db_path=local_db("three_ready_four_tasks"), workers_path=workers, max_concurrency=3, dry_run_step_seconds=(0.2, 0.2, 0.2))
    scheduler = make_scheduler(settings, FakeWorkerClient(states))
    await scheduler.start()
    await scheduler.create_tasks([
        {"idempotency_key": f"multi-{i}", "image_path": f"D:/img-{i}.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}
        for i in range(4)
    ])
    for _ in range(50):
        tasks = await scheduler.list_tasks()
        active = [task for task in tasks if task["status"] in {"assigning", "submitted", "processing"}]
        queued = [task for task in tasks if task["status"] == "queued"]
        if len(active) == 3 and len(queued) == 1:
            break
        await asyncio.sleep(0.02)
    tasks = await scheduler.list_tasks()
    active = [task for task in tasks if task["status"] in {"assigning", "submitted", "processing"}]
    queued = [task for task in tasks if task["status"] == "queued"]
    assert {task["assigned_account_id"] for task in active} == {"FLOW-024", "FLOW-025", "FLOW-026"}
    assert len(queued) == 1
    assert all(task["assigned_runtime_instance_id"] for task in active)
    await scheduler.stop()


@pytest.mark.asyncio
async def test_scheduler_account_allowlist_excludes_ready_account_outside_list():
    from gateway.config import GatewaySettings

    workers = RUN_ROOT / "allowlisted_workers.json"
    write_workers(workers, ["FLOW-024", "FLOW-025", "FLOW-026", "FLOW-027"])
    states = {account_id: {"credits": 50} for account_id in ["FLOW-024", "FLOW-025", "FLOW-026", "FLOW-027"]}
    settings = GatewaySettings(
        db_path=local_db("allowlist_three_tasks"),
        workers_path=workers,
        max_concurrency=3,
        allowed_account_ids=("FLOW-025", "FLOW-026", "FLOW-027"),
        dry_run_step_seconds=(0.2, 0.2, 0.2),
    )
    scheduler = make_scheduler(settings, FakeWorkerClient(states))
    await scheduler.start()
    await scheduler.create_tasks([
        {"idempotency_key": f"allow-{i}", "image_path": f"D:/img-{i}.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}
        for i in range(3)
    ])
    for _ in range(50):
        tasks = await scheduler.list_tasks()
        active = [task for task in tasks if task["status"] in {"assigning", "submitted", "processing"}]
        if len(active) == 3:
            break
        await asyncio.sleep(0.02)
    tasks = await scheduler.list_tasks()
    active = [task for task in tasks if task["status"] in {"assigning", "submitted", "processing"}]
    assert {task["assigned_account_id"] for task in active} == {"FLOW-025", "FLOW-026", "FLOW-027"}
    assert "FLOW-024" not in {task["assigned_account_id"] for task in active}
    accounts = {account["account_id"]: account for account in await scheduler.list_accounts()}
    assert "FLOW-024" not in accounts
    await scheduler.stop()


@pytest.mark.asyncio
async def test_stale_runtime_instance_enters_manual_review_without_submit():
    from gateway.config import GatewaySettings
    from gateway.worker_provider import WorkerConfig, WorkerSnapshot

    first = WorkerSnapshot([WorkerConfig("FLOW-024", "http://127.0.0.1:8124", True, "runtime-old")], [], "runtime_registry", "Fake", "now")
    second = WorkerSnapshot([WorkerConfig("FLOW-024", "http://127.0.0.1:8124", True, "runtime-new")], [], "runtime_registry", "Fake", "now")

    class ChangingProvider:
        def __init__(self):
            self.calls = 0
        def load_workers(self):
            self.calls += 1
            return first

    states = {"FLOW-024": {"credits": 50}}
    class SlowProjectClient(FakeRealWorkerClient):
        async def create_project(self, worker, payload):
            result = await super().create_project(worker, payload)
            await asyncio.sleep(0.1)
            return result

    client = SlowProjectClient(states, output_dir=RUN_ROOT / "stale_outputs")
    settings = GatewaySettings(db_path=local_db("stale_runtime"), dry_run=False)
    from gateway.scheduler import GatewayScheduler
    scheduler = GatewayScheduler(settings, worker_client=client, worker_provider=ChangingProvider())
    await scheduler.start()
    await scheduler.create_task({"idempotency_key": "stale", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16"})
    for _ in range(50):
        if client.projects:
            break
        await asyncio.sleep(0.01)
    scheduler._apply_worker_snapshot(second)
    for _ in range(100):
        task = (await scheduler.list_tasks())[0]
        if task["status"] == "manual_review":
            break
        await asyncio.sleep(0.02)
    task = (await scheduler.list_tasks())[0]
    assert task["error_code"] == "stale_runtime_instance"
    assert len(client.projects) == 1
    assert client.submits == []
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
    client = FakeRealWorkerClient(states, output_dir=RUN_ROOT / "real_submit_outputs")
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
    assert task["error_code"] is None
    assert task["error_message"] is None
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
async def test_real_mode_creates_project_with_selected_worker_when_project_id_missing():
    from gateway.config import GatewaySettings

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    settings = GatewaySettings(db_path=local_db("real_missing_project"), dry_run=False)
    client = FakeRealWorkerClient(states, output_dir=RUN_ROOT / "created_project_outputs")
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    await scheduler.create_task({"idempotency_key": "real-no-project", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-002"})
    for _ in range(100):
        task = (await scheduler.list_tasks())[0]
        if task["status"] == "completed":
            break
        await asyncio.sleep(0.02)
    task = (await scheduler.list_tasks())[0]
    assert client.projects[0][0] == "FLOW-002"
    assert client.submits[0][0] == "FLOW-002"
    assert task["project_id"] == "project-FLOW-002-1"
    assert task["project_created_by_gateway"] == 1
    assert task["assigned_account_id"] == "FLOW-002"
    assert "assigned_runtime_instance_id" in task
    await scheduler.stop()


@pytest.mark.asyncio
async def test_real_mode_project_create_failure_releases_account_without_submit():
    from gateway.config import GatewaySettings

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    client = FakeRealWorkerClient(states)
    client.fail_project_create = True
    settings = GatewaySettings(db_path=local_db("project_create_failure"), dry_run=False)
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    await scheduler.create_task({"idempotency_key": "project-fail", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-002"})
    for _ in range(100):
        task = (await scheduler.list_tasks())[0]
        if task["status"] == "manual_review":
            break
        await asyncio.sleep(0.02)
    task = (await scheduler.list_tasks())[0]
    accounts = {a["account_id"]: a for a in await scheduler.list_accounts()}
    assert task["error_code"] == "project_create_failed"
    assert task["worker_job_id"] is None
    assert task["attempt_count"] == 0
    assert len(client.projects) == 1
    assert client.submits == []
    assert accounts["FLOW-002"]["current_task_id"] is None
    await scheduler.stop()


@pytest.mark.asyncio
async def test_bound_real_task_continues_when_account_status_becomes_low_credits():
    from gateway import crud
    from gateway.config import GatewaySettings

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    client = FakeRealWorkerClient(states, output_dir=RUN_ROOT / "low_credit_bound_outputs")
    settings = GatewaySettings(db_path=local_db("low_credit_bound"), dry_run=False)
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    await scheduler.create_task({
        "idempotency_key": "low-credit-bound",
        "image_path": "D:/img.png",
        "prompt": "p",
        "duration": 10,
        "aspect_ratio": "9:16",
        "preferred_account_id": "FLOW-002",
    })
    for _ in range(100):
        task = (await scheduler.list_tasks())[0]
        if task["project_id"]:
            async with scheduler.assignment_lock:
                await crud.update_task_status(scheduler.db, task["task_id"], task["status"])
                await scheduler.db.execute("UPDATE flow_accounts SET status='low_credits' WHERE account_id='FLOW-002'")
                await scheduler.db.commit()
            break
        await asyncio.sleep(0.02)
    for _ in range(100):
        task = (await scheduler.list_tasks())[0]
        if task["status"] == "completed":
            break
        await asyncio.sleep(0.02)
    task = (await scheduler.list_tasks())[0]
    assert task["status"] == "completed"
    assert task["error_code"] is None
    assert len(client.projects) == 1
    assert len(client.submits) == 1
    await scheduler.stop()


@pytest.mark.asyncio
async def test_three_bound_accounts_release_only_matching_current_task_and_refresh_preserves_bindings():
    from gateway.config import GatewaySettings

    workers = RUN_ROOT / "three_bound_workers.json"
    write_workers(workers, ["FLOW-024", "FLOW-025", "FLOW-026"])
    states = {account_id: {"credits": 50} for account_id in ["FLOW-024", "FLOW-025", "FLOW-026"]}
    class ControlledClient(FakeRealWorkerClient):
        def __init__(self, states, output_dir):
            super().__init__(states, output_dir)
            self.complete_jobs = False
            self.complete_accounts = set()

        async def submit_omni_video(self, worker, payload):
            result = await super().submit_omni_video(worker, payload)
            self.jobs[result["job_id"]]["status"] = "processing"
            return result

        async def get_omni_video(self, worker, worker_job_id):
            if self.complete_jobs or worker.account_id in self.complete_accounts:
                self.jobs[worker_job_id]["status"] = "completed"
                self.jobs[worker_job_id]["remaining_credits"] = self.states[worker.account_id]["credits"]
            return self.jobs[worker_job_id]

    client = ControlledClient(states, output_dir=RUN_ROOT / "three_bound_outputs")
    settings = GatewaySettings(db_path=local_db("three_bound_release"), workers_path=workers, dry_run=False, max_concurrency=3)
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    await scheduler.create_tasks([
        {"idempotency_key": f"bound-{shot}", "image_path": f"D:/img-{shot}.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}
        for shot in ["024", "025", "026"]
    ])
    for _ in range(100):
        accounts = {a["account_id"]: a for a in await scheduler.list_accounts()}
        if all(accounts[account_id]["current_task_id"] for account_id in ["FLOW-024", "FLOW-025", "FLOW-026"]):
            break
        await asyncio.sleep(0.02)
    accounts_before = {a["account_id"]: a for a in await scheduler.list_accounts()}
    task_by_account = {task["assigned_account_id"]: task["task_id"] for task in await scheduler.list_tasks()}
    assert set(task_by_account) == {"FLOW-024", "FLOW-025", "FLOW-026"}

    states["FLOW-024"]["credits"] = 5
    await scheduler.refresh_workers()
    accounts_after_refresh = {a["account_id"]: a for a in await scheduler.list_accounts()}
    assert accounts_after_refresh["FLOW-024"]["status"] == "low_credits"
    assert accounts_after_refresh["FLOW-024"]["current_task_id"] == task_by_account["FLOW-024"]
    assert accounts_after_refresh["FLOW-025"]["current_task_id"] == task_by_account["FLOW-025"]
    assert accounts_after_refresh["FLOW-026"]["current_task_id"] == task_by_account["FLOW-026"]

    client.complete_accounts.add("FLOW-025")
    for _ in range(140):
        tasks_by_account = {task["assigned_account_id"]: task for task in await scheduler.list_tasks()}
        if tasks_by_account["FLOW-025"]["status"] == "completed":
            break
        await asyncio.sleep(0.05)
    accounts_after_release = {a["account_id"]: a for a in await scheduler.list_accounts()}
    tasks_by_account = {task["assigned_account_id"]: task for task in await scheduler.list_tasks()}
    assert tasks_by_account["FLOW-025"]["status"] == "completed"
    assert accounts_after_release["FLOW-024"]["current_task_id"] == task_by_account["FLOW-024"]
    assert accounts_after_release["FLOW-026"]["current_task_id"] == task_by_account["FLOW-026"]

    client.complete_jobs = True
    try:
        for _ in range(140):
            tasks = await scheduler.list_tasks()
            if all(task["status"] == "completed" for task in tasks):
                break
            await asyncio.sleep(0.05)
        tasks = await scheduler.list_tasks()
        assert all(task["status"] == "completed" for task in tasks), tasks
        assert len(client.projects) == 3
        assert len(client.submits) == 3
        accounts_final = {a["account_id"]: a for a in await scheduler.list_accounts()}
        assert accounts_final["FLOW-024"]["status"] == "low_credits"
        assert accounts_final["FLOW-024"]["current_task_id"] is None
    finally:
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
    client = FakeRealWorkerClient(states, output_dir=RUN_ROOT / "real_restart_outputs")
    settings = GatewaySettings(db_path=local_db("real_restart"), dry_run=False)
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    await scheduler.create_task({"idempotency_key": "real-2", "project_id": "project-a", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-002"})
    for _ in range(50):
        if client.submits:
            break
        await asyncio.sleep(0.02)
    await scheduler.stop()

    client2 = FakeRealWorkerClient(states, output_dir=RUN_ROOT / "real_restart_outputs")
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

    output_dir = RUN_ROOT / "gateway_retry_outputs"
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

    output_dir = RUN_ROOT / "gateway_manual_review_retry_outputs"
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
    client = FakeRealWorkerClient(states, output_dir=RUN_ROOT / "lost_response_outputs")
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
async def test_recover_restores_empty_binding_for_active_task_with_worker_job():
    from gateway.config import GatewaySettings
    from gateway import crud
    from gateway.db import connect

    output_dir = RUN_ROOT / "recover_restore_binding_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    video_file = output_dir / "job-restore.mp4"
    video_file.write_bytes(b"\x00\x00\x00\x18ftypmp42")
    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 35}, "FLOW-003": {"credits": 35}}
    settings = GatewaySettings(db_path=local_db("recover_restore_binding"), dry_run=False)
    db = await connect(settings.db_path)
    created = await crud.create_task(db, {"idempotency_key": "restore-binding", "project_id": "project-a", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-002"})
    await db.execute("UPDATE flow_tasks SET status='processing', assigned_account_id='FLOW-002', worker_job_id='job-restore' WHERE task_id=?", (created["task_id"],))
    await db.commit()
    await db.close()
    client = FakeRealWorkerClient(states)
    client.jobs["job-restore"] = {"job_id": "job-restore", "status": "processing", "video_path": str(video_file), "remaining_credits": 35}
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    accounts = {a["account_id"]: a for a in await scheduler.list_accounts()}
    assert accounts["FLOW-002"]["current_task_id"] == created["task_id"]
    await scheduler.stop()


@pytest.mark.asyncio
async def test_recover_clears_stale_terminal_binding_conditionally():
    from gateway.config import GatewaySettings
    from gateway import crud
    from gateway.db import connect

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 35}, "FLOW-003": {"credits": 35}}
    settings = GatewaySettings(db_path=local_db("recover_stale_binding"), dry_run=False)
    db = await connect(settings.db_path)
    created = await crud.create_task(db, {"idempotency_key": "stale-binding", "project_id": "project-a", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-002"})
    await db.execute("UPDATE flow_tasks SET status='completed', assigned_account_id='FLOW-002' WHERE task_id=?", (created["task_id"],))
    await db.execute("UPDATE flow_accounts SET current_task_id=? WHERE account_id='FLOW-002'", (created["task_id"],))
    await db.commit()
    await db.close()
    scheduler = make_scheduler(settings, FakeRealWorkerClient(states))
    await scheduler.start()
    accounts = {a["account_id"]: a for a in await scheduler.list_accounts()}
    assert accounts["FLOW-002"]["current_task_id"] is None
    await scheduler.stop()


@pytest.mark.asyncio
async def test_recover_multiple_active_tasks_for_one_account_go_manual_review():
    from gateway.config import GatewaySettings
    from gateway import crud
    from gateway.db import connect

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 35}, "FLOW-003": {"credits": 35}}
    settings = GatewaySettings(db_path=local_db("recover_multiple_active"), dry_run=False)
    db = await connect(settings.db_path)
    for key in ("multi-active-1", "multi-active-2"):
        created = await crud.create_task(db, {"idempotency_key": key, "project_id": f"project-{key}", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-002"})
        await db.execute("UPDATE flow_tasks SET status='waiting_recovery', assigned_account_id='FLOW-002', worker_job_id=? WHERE task_id=?", (f"job-{key}", created["task_id"]))
    await db.commit()
    await db.close()
    scheduler = make_scheduler(settings, FakeRealWorkerClient(states))
    await scheduler.start()
    tasks = await scheduler.list_tasks()
    assert {task["status"] for task in tasks} == {"manual_review"}
    assert {task["error_code"] for task in tasks} == {"multiple_active_tasks_for_account"}
    await scheduler.stop()


@pytest.mark.asyncio
async def test_recover_existing_worker_job_queries_without_submit_or_project_create():
    from gateway.config import GatewaySettings
    from gateway import crud
    from gateway.db import connect

    output_dir = RUN_ROOT / "recover_existing_job_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    video_file = output_dir / "job-existing-recover.mp4"
    video_file.write_bytes(b"\x00\x00\x00\x18ftypmp42")
    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 35}, "FLOW-003": {"credits": 35}}
    settings = GatewaySettings(db_path=local_db("recover_existing_job"), dry_run=False)
    db = await connect(settings.db_path)
    created = await crud.create_task(db, {"idempotency_key": "recover-existing", "project_id": "project-a", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-002"})
    await db.execute("UPDATE flow_tasks SET status='processing', assigned_account_id='FLOW-002', worker_job_id='job-existing-recover' WHERE task_id=?", (created["task_id"],))
    await db.commit()
    await db.close()
    client = FakeRealWorkerClient(states)
    client.jobs["job-existing-recover"] = {"job_id": "job-existing-recover", "status": "completed", "video_path": str(video_file), "remaining_credits": 35}
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    for _ in range(100):
        task = (await scheduler.list_tasks())[0]
        if task["status"] == "completed":
            break
        await asyncio.sleep(0.02)
    task = (await scheduler.list_tasks())[0]
    assert task["status"] == "completed"
    assert client.projects == []
    assert client.submits == []
    assert client.gets == [("FLOW-002", "job-existing-recover")]
    await scheduler.stop()


@pytest.mark.asyncio
async def test_recover_project_without_worker_job_goes_manual_review_without_submit():
    from gateway.config import GatewaySettings
    from gateway import crud
    from gateway.db import connect

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 35}, "FLOW-003": {"credits": 35}}
    settings = GatewaySettings(db_path=local_db("recover_submit_unknown"), dry_run=False)
    db = await connect(settings.db_path)
    created = await crud.create_task(db, {"idempotency_key": "submit-unknown", "project_id": "project-a", "image_path": "D:/img.png", "prompt": "p", "duration": 10, "aspect_ratio": "9:16", "preferred_account_id": "FLOW-002"})
    await db.execute("UPDATE flow_tasks SET status='assigning', assigned_account_id='FLOW-002' WHERE task_id=?", (created["task_id"],))
    await db.commit()
    await db.close()
    client = FakeRealWorkerClient(states)
    scheduler = make_scheduler(settings, client)
    await scheduler.start()
    task = (await scheduler.list_tasks())[0]
    assert task["status"] == "manual_review"
    assert task["error_code"] == "submit_state_unknown"
    assert client.projects == []
    assert client.submits == []
    await scheduler.stop()


@pytest.mark.asyncio
async def test_canary_two_tasks_bind_flow_002_and_flow_003_without_double_account():
    from gateway.config import GatewaySettings

    states = {"FLOW-001": {"credits": 5}, "FLOW-002": {"credits": 50}, "FLOW-003": {"credits": 50}}
    client = FakeRealWorkerClient(states, output_dir=RUN_ROOT / "canary_outputs")
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
