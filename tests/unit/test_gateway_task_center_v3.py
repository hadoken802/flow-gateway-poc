import pytest


class FakeScheduler:
    def __init__(self, db):
        self.db = db
        self.schedule_calls = 0

    async def create_task(self, payload):
        from gateway import crud

        return await crud.create_task(self.db, payload)

    async def schedule_once(self):
        self.schedule_calls += 1


@pytest.fixture
async def gateway_db(tmp_path):
    from gateway.db import connect

    db = await connect(tmp_path / "gateway.db")
    yield db
    await db.close()


def _item(image, **overrides):
    data = {
        "external_task_id": "shot-001",
        "prompt": "A slow camera move",
        "input_media_path": str(image),
        "duration": 10,
        "aspect_ratio": "9:16",
        "priority": 5,
        "estimated_quota_cost": 15,
        "output_directory": "outputs",
        "output_filename": "video.mp4",
        "metadata": {"scene": 1},
        "generation_parameters": {"camera": "slow"},
    }
    data.update(overrides)
    return data


@pytest.mark.asyncio
async def test_json_import_creates_batch_and_persists_task_metadata(gateway_db, tmp_path):
    from gateway import task_center

    image = tmp_path / "input.png"
    image.write_bytes(b"png")
    scheduler = FakeScheduler(gateway_db)

    result = await task_center.import_tasks(scheduler, {"batch_id": "batch-a", "tasks": [_item(image)]})

    assert result["created"] == 1
    assert result["duplicates"] == 0
    assert result["failed"] == 0
    detail = await task_center.batch_detail(scheduler, "batch-a")
    task = detail["tasks"][0]
    assert task["external_task_id"] == "shot-001"
    assert task["batch_id"] == "batch-a"
    assert task["output_filename"] == "video.mp4"
    assert task["status"] == "queued"


@pytest.mark.asyncio
async def test_csv_import_duplicate_and_row_error_do_not_block_other_rows(gateway_db, tmp_path):
    from gateway import task_center

    image = tmp_path / "input.png"
    image.write_bytes(b"png")
    missing = tmp_path / "missing.png"
    csv_content = (
        "external_task_id,prompt,input_media_path,output_filename\n"
        f"shot-001,A slow move,{image},video.mp4\n"
        f"shot-bad,A slow move,{missing},video.mp4\n"
    )
    scheduler = FakeScheduler(gateway_db)

    first = await task_center.import_tasks(scheduler, {"batch_id": "batch-csv", "format": "csv", "content": csv_content})
    second = await task_center.import_tasks(scheduler, {"batch_id": "batch-csv", "format": "csv", "content": csv_content})

    assert first["created"] == 1
    assert first["failed"] == 1
    assert second["created"] == 0
    assert second["duplicates"] == 1
    assert second["failed"] == 1


def test_output_filename_is_sanitized_and_rejects_path_traversal():
    from gateway import task_center

    plan = task_center.safe_output_plan({"batch_id": "b:1", "external_task_id": "shot/1", "output_filename": "bad:name.mp4"})

    assert "bad_name.mp4" in plan["output_path"]
    assert ".." not in plan["output_path"]


@pytest.mark.asyncio
async def test_task_pause_resume_cancel_priority_and_requeue_use_existing_state(gateway_db, tmp_path):
    from gateway import crud, task_center

    image = tmp_path / "input.png"
    image.write_bytes(b"png")
    scheduler = FakeScheduler(gateway_db)
    created = await scheduler.create_task(task_center.normalize_import_item(_item(image, idempotency_key="ops"), "ops-batch"))

    paused = await task_center.pause_task(scheduler, created["task_id"])
    assert paused["manual_paused"] == 1
    resumed = await task_center.resume_task(scheduler, created["task_id"])
    assert resumed["manual_paused"] == 0
    changed = await task_center.set_priority(scheduler, created["task_id"], 99)
    assert changed["priority"] == 99
    cancelled = await task_center.cancel_task(scheduler, created["task_id"])
    assert cancelled["status"] == "cancelled"
    assert await crud.requeue_task(gateway_db, created["task_id"]) is None


def test_v1_routes_are_registered():
    from gateway.main import app

    paths = {route.path for route in app.routes}
    assert "/api/v1/tasks/import" in paths
    assert "/api/v1/tasks/{task_id}/pause" in paths
    assert "/api/v1/tasks/{task_id}/retry-download" in paths
    assert "/api/v1/accounts" in paths
    assert "/api/v1/nodes" in paths
    assert "/api/v1/nodes/{account_id}/refresh-session" in paths
    assert "/" in paths


def test_account_node_add_form_maps_fields_and_reports_results():
    from gateway.main import TASK_CENTER_HTML

    assert 'id="nodeAccountId"' in TASK_CENTER_HTML
    assert 'id="nodeDisplayName"' in TASK_CENTER_HTML
    assert 'id="nodeWorkerHost"' in TASK_CENTER_HTML
    assert 'value="127.0.0.1"' in TASK_CENTER_HTML
    assert 'id="nodeWorkerPort"' in TASK_CENTER_HTML
    assert 'id="nodeCdpHost"' in TASK_CENTER_HTML
    assert 'id="nodeCdpPort"' in TASK_CENTER_HTML
    assert 'id="nodeEnabled" type="checkbox"' in TASK_CENTER_HTML
    assert "worker_host:(nodeWorkerHost.value||'127.0.0.1').trim()" in TASK_CENTER_HTML
    assert "cdp_host:(nodeCdpHost.value||'127.0.0.1').trim()" in TASK_CENTER_HTML
    assert "enabled:nodeEnabled.checked" in TASK_CENTER_HTML
    assert "POST /api/v1/nodes succeeded" in TASK_CENTER_HTML
    assert "POST /api/v1/nodes returned duplicate" in TASK_CENTER_HTML
    assert "POST /api/v1/nodes failed" in TASK_CENTER_HTML
    assert "await loadNodes();" in TASK_CENTER_HTML
