from pathlib import Path

import pytest
from fastapi.testclient import TestClient


PNG = b"\x89PNG\r\n\x1a\n" + b"x" * 32
MP4 = b"\x00\x00\x00\x18ftypmp42" + b"x" * 2048


class FakeScheduler:
    def __init__(self, db):
        self.db = db
        self.schedule_calls = 0

    async def pool_status(self):
        from gateway import crud

        return {
            "eligible_count": 0,
            "effective_max_concurrency": 0,
            "queued_count": await crud.count_tasks(self.db, ["queued"]),
            "active_count": 0,
        }

    async def create_task(self, payload):
        from gateway import crud

        task = await crud.create_task(self.db, payload)
        self.schedule_calls += 1
        return task

    async def get_task(self, task_id):
        from gateway import crud

        return await crud.get_task(self.db, task_id)

    async def schedule_once(self):
        self.schedule_calls += 1


@pytest.fixture
async def embedded_app(monkeypatch, tmp_path):
    from gateway.config import GatewaySettings
    from gateway.db import connect
    from gateway import main

    db_path = tmp_path / "dir with spaces" / "data" / "gateway.db"
    db = await connect(db_path)
    settings = GatewaySettings(
        db_path=db_path,
        workers_path=tmp_path / "runtime" / "embedded_workers.json",
        worker_source="static_json",
        client_api_key="client-key",
        admin_api_key="admin-key",
        output_root=tmp_path / "outputs",
    )
    monkeypatch.setattr(main, "settings", settings)
    monkeypatch.setattr(main, "scheduler", FakeScheduler(db))
    yield main.app, db, settings
    await db.close()


def test_client_routes_are_registered():
    from gateway.main import app

    paths = {route.path for route in app.routes}
    assert "/api/v1/client/system/ready" in paths
    assert "/api/v1/client/files" in paths
    assert "/api/v1/client/files/batch" in paths
    assert "/api/v1/client/tasks" in paths
    assert "/api/v1/client/tasks/{task_id}" in paths
    assert "/api/v1/client/tasks/{task_id}/cancel" in paths
    assert "/api/v1/client/tasks/{task_id}/download" in paths


@pytest.mark.asyncio
async def test_initial_static_json_accounts_are_zero(tmp_path):
    from gateway.config import GatewaySettings
    from gateway.worker_provider import build_worker_provider

    workers_path = tmp_path / "runtime path with spaces" / "embedded_workers.json"
    workers_path.parent.mkdir(parents=True)
    workers_path.write_text("[]", encoding="utf-8")
    settings = GatewaySettings(workers_path=workers_path, worker_source="static_json")
    snapshot = build_worker_provider(settings).load_workers()
    assert snapshot.candidate_count == 0
    assert snapshot.eligible_count == 0


def test_client_key_required_and_cannot_manage_nodes(embedded_app):
    app, _db, _settings = embedded_app
    client = TestClient(app)

    assert client.get("/api/v1/client/system/ready").status_code == 401
    assert client.get("/api/v1/client/system/ready", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/api/v1/client/system/ready", headers={"X-API-Key": "client-key"}).status_code == 200
    assert client.get("/api/v1/nodes", headers={"X-API-Key": "client-key"}).status_code == 401


@pytest.mark.asyncio
async def test_multi_image_create_query_download_flow(embedded_app):
    app, db, settings = embedded_app
    client = TestClient(app)
    headers = {"X-API-Key": "client-key"}

    upload = client.post(
        "/api/v1/client/files/batch",
        headers=headers,
        files=[
            ("files", ("one.png", PNG, "image/png")),
            ("files", ("two.png", PNG, "image/png")),
        ],
    )
    assert upload.status_code == 200
    files = upload.json()["files"]
    task_response = client.post(
        "/api/v1/client/tasks",
        headers=headers,
        json={
            "idempotency_key": "embedded-flow",
            "input_file_ids": [files[0]["file_id"], files[1]["file_id"]],
            "prompt": "Use both images in order",
            "duration": 10,
            "aspect_ratio": "9:16",
            "estimated_quota_cost": 15,
            "priority": 10,
            "output_filename": "result.mp4",
        },
    )
    assert task_response.status_code == 200
    task_id = task_response.json()["task_id"]
    duplicate = client.post(
        "/api/v1/client/tasks",
        headers=headers,
        json={"idempotency_key": "embedded-flow", "input_file_ids": [files[0]["file_id"], files[1]["file_id"]], "prompt": "Use both images in order"},
    ).json()
    assert duplicate["task_id"] == task_id
    assert duplicate["duplicate"] is True

    detail = client.get(f"/api/v1/client/tasks/{task_id}", headers=headers)
    assert detail.status_code == 200
    assert [item["file_id"] for item in detail.json()["input_media"]] == [files[0]["file_id"], files[1]["file_id"]]

    assert client.get(f"/api/v1/client/tasks/{task_id}/download", headers=headers).status_code == 409
    video = settings.output_root / "result.mp4"
    video.parent.mkdir(parents=True, exist_ok=True)
    video.write_bytes(MP4)
    await db.execute("UPDATE flow_tasks SET status='completed', video_path=?, output_filename='result.mp4' WHERE task_id=?", (str(video), task_id))
    await db.commit()
    download = client.get(f"/api/v1/client/tasks/{task_id}/download", headers=headers)
    assert download.status_code == 200
    assert download.content.startswith(MP4[:12])


@pytest.mark.asyncio
async def test_download_rejects_output_outside_allowed_root(embedded_app):
    app, db, _settings = embedded_app
    client = TestClient(app)
    headers = {"X-API-Key": "client-key"}
    outside = Path(db._flowkit_gateway_db_path).parent.parent / "outside.mp4"
    outside.write_bytes(MP4)
    task = await _create_legacy_task(db)
    await db.execute("UPDATE flow_tasks SET status='completed', video_path=? WHERE task_id=?", (str(outside), task["task_id"]))
    await db.commit()
    assert client.get(f"/api/v1/client/tasks/{task['task_id']}/download", headers=headers).status_code == 409


async def _create_legacy_task(db):
    from gateway import crud

    return await crud.create_task(db, {"idempotency_key": "legacy", "image_path": "D:/input.png", "prompt": "p"})
