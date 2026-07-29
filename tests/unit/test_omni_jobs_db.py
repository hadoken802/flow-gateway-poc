import pytest
import tempfile
import uuid
from pathlib import Path

from agent.db import crud
from agent.db.schema import close_db, init_db

RUN_ROOT = Path(tempfile.gettempdir()) / "flowkit-omni-tests" / uuid.uuid4().hex


@pytest.mark.asyncio
async def test_omni_test_job_persists_independently(monkeypatch):
    import agent.config as config
    import agent.db.schema as schema

    temp_dir = RUN_ROOT / "omni_jobs_db"
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / "flow_agent.db"
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(db_path) + suffix)
        if path.exists():
            path.unlink()
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(schema, "DB_PATH", db_path)
    await close_db()
    await init_db()

    job = await crud.create_omni_test_job(
        "job-1",
        "project-123",
        "A prompt",
        "D:\\image.png",
    )
    assert job["status"] == "queued"

    updated = await crud.update_omni_test_job(
        "job-1",
        input_media_id="input-1",
        output_media_id="output-1",
        workflow_id="workflow-1",
        operation_name="operations/1",
        status="active",
    )

    assert updated["output_media_id"] == "output-1"
    active = await crud.list_omni_test_jobs(["active"])
    assert [row["job_id"] for row in active] == ["job-1"]
    await close_db()


@pytest.mark.asyncio
async def test_omni_test_job_idempotency_key_reuses_original_job(monkeypatch):
    import agent.config as config
    import agent.db.schema as schema

    temp_dir = RUN_ROOT / "omni_jobs_idempotency"
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / "flow_agent.db"
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(db_path) + suffix)
        if path.exists():
            path.unlink()
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(schema, "DB_PATH", db_path)
    await close_db()


@pytest.mark.asyncio
async def test_required_omni_job_update_raises_for_missing_job(monkeypatch):
    import agent.config as config
    import agent.db.schema as schema

    temp_dir = RUN_ROOT / "omni_jobs_required_update"
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / "flow_agent.db"
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(db_path) + suffix)
        if path.exists():
            path.unlink()
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(schema, "DB_PATH", db_path)
    await close_db()
    await init_db()

    with pytest.raises(crud.RowNotUpdatedError):
        await crud.update_omni_test_job_required("missing-job", status="submitted")
    await close_db()
    await init_db()

    first = await crud.create_omni_test_job("job-1", "project-1", "prompt", "D:\\a.png", "same-key")
    second = await crud.create_omni_test_job("job-2", "project-1", "prompt", "D:\\a.png", "same-key")

    assert first["job_id"] == "job-1"
    assert second["job_id"] == "job-1"
    assert second["idempotency_key"] == "same-key"
    assert first["created"] is True
    assert second["reused"] is True
    assert second["conflict_reason"] == "idempotency_key_exists"
    await close_db()


@pytest.mark.asyncio
async def test_omni_submit_upstream_403_returns_failed_local_job(monkeypatch, tmp_path):
    from agent.api import omni_test

    image = tmp_path / "input.png"
    image.write_bytes(b"\x89PNG\r\n\x1a\n0")
    jobs = {}

    class FakeClient:
        connected = True
        _flow_key = "present"

        async def get_credits(self):
            return {"credits": 35}

    class FakeOmniClient:
        def __init__(self, client):
            self.client = client

        async def submit_reference_video(self, **kwargs):
            return {
                "status": 403,
                "error": {
                    "code": 403,
                    "message": "reCAPTCHA evaluation failed",
                    "status": "PERMISSION_DENIED",
                    "details": [{"reason": "PUBLIC_ERROR_UNUSUAL_ACTIVITY"}],
                },
            }

    async def create_job(job_id, project_id, prompt, image_path, idempotency_key=None):
        jobs[job_id] = {
            "job_id": job_id,
            "idempotency_key": idempotency_key,
            "project_id": project_id,
            "prompt": prompt,
            "image_path": image_path,
            "status": "queued",
        }
        return jobs[job_id]

    async def update_job(job_id, **fields):
        jobs[job_id].update(fields)
        return jobs[job_id]

    async def no_existing_job(key):
        return None

    async def upload_image(client, image_path, project_id):
        return {"_mediaId": "input-1"}

    monkeypatch.setattr(omni_test, "get_flow_client", lambda: FakeClient())
    monkeypatch.setattr(omni_test, "OmniClient", FakeOmniClient)
    monkeypatch.setattr(omni_test, "_upload_image", upload_image)
    monkeypatch.setattr(omni_test.crud, "get_omni_test_job_by_idempotency_key", no_existing_job)
    monkeypatch.setattr(omni_test.crud, "create_omni_test_job", create_job)
    monkeypatch.setattr(omni_test.crud, "update_omni_test_job", update_job)
    monkeypatch.setattr(omni_test.crud, "update_omni_test_job_required", update_job)

    result = await omni_test.submit_omni_video(omni_test.OmniVideoRequest(
        idempotency_key="storyboard:task-1:attempt:1",
        project_id="project-1",
        image_path=str(image),
        prompt="p",
        duration=10,
        aspect_ratio="9:16",
    ))

    assert result["job_id"]
    assert result["status"] == "failed"
    assert result["error_code"] == "403"
    assert "PUBLIC_ERROR_UNUSUAL_ACTIVITY" in result["error_message"]
    assert result["reused"] is False


@pytest.mark.asyncio
async def test_resume_submit_reuses_existing_attempt_without_resubmitting(monkeypatch):
    from agent.api import omni_test

    class FakeClient:
        connected = True
        _flow_key = "present"

    class FailingOmniClient:
        def __init__(self, client):
            self.client = client

        async def submit_reference_video(self, **kwargs):
            raise AssertionError("submit_reference_video must not be called for reused attempt")

    existing = {
        "job_id": "resume-job-2",
        "idempotency_key": "storyboard:task-2:attempt:2",
        "project_id": "project-2",
        "input_media_id": "input-2",
        "status": "submit_in_progress",
        "prompt": "p",
        "image_path": "D:/image.png",
        "remote_submission_state": "not_started",
        "resume_attempt_id": "resume:task-2:attempt:2",
        "created": False,
        "reused": True,
        "conflict_reason": "idempotency_key_exists",
    }

    async def create_job(*args, **kwargs):
        return existing

    monkeypatch.setattr(omni_test, "get_flow_client", lambda: FakeClient())
    monkeypatch.setattr(omni_test, "OmniClient", FailingOmniClient)
    monkeypatch.setattr(omni_test.crud, "create_omni_test_job", create_job)

    result = await omni_test.resume_omni_video(omni_test.OmniVideoResumeRequest(
        idempotency_key="storyboard:task-2:attempt:2",
        project_id="project-2",
        input_media_id="input-2",
        image_path="D:/image.png",
        prompt="p",
        duration=10,
        aspect_ratio="9:16",
        gateway_task_id="task-2",
        generation_attempt=2,
        source_worker_job_id="old-job",
        resume_attempt_id="resume:task-2:attempt:2",
    ))

    assert result["reused"] is True
    assert result["job_id"] == "resume-job-2"
    assert result["remote_submission_state"] == "not_started"


@pytest.mark.asyncio
async def test_resume_submit_persists_ids_before_calling_google(monkeypatch):
    from agent.api import omni_test

    created = {}
    calls = []

    class FakeClient:
        connected = True
        _flow_key = "present"

    class FakeOmniClient:
        def __init__(self, client):
            self.client = client

        async def submit_reference_video(self, **kwargs):
            calls.append(kwargs)
            assert created["status"] == "submit_in_progress"
            assert created["request_batch_id"] == "batch-2"
            assert created["extension_request_id"] == "request-2"
            return {"status": 200, "data": {"media": [{"name": "media-2", "operation": {"name": "op-2"}}], "workflows": [{"name": "workflow-2", "metadata": {"batchId": "batch-2"}}]}}

    async def create_job(job_id, project_id, prompt, image_path, idempotency_key=None, **fields):
        created.update({"job_id": job_id, "idempotency_key": idempotency_key, "project_id": project_id, "prompt": prompt, "image_path": image_path, **fields, "created": True, "reused": False})
        return created

    async def update_required(job_id, **fields):
        assert job_id == created["job_id"]
        created.update(fields)
        return created

    monkeypatch.setattr(omni_test, "get_flow_client", lambda: FakeClient())
    monkeypatch.setattr(omni_test, "OmniClient", FakeOmniClient)
    monkeypatch.setattr(omni_test.crud, "create_omni_test_job", create_job)
    monkeypatch.setattr(omni_test.crud, "update_omni_test_job_required", update_required)
    monkeypatch.setattr(omni_test, "_ensure_polling", lambda job_id: calls.append({"poll": job_id}))

    result = await omni_test.resume_omni_video(omni_test.OmniVideoResumeRequest(
        idempotency_key="storyboard:task-2:attempt:2",
        project_id="project-2",
        input_media_id="input-2",
        image_path="D:/image.png",
        prompt="p",
        duration=10,
        aspect_ratio="9:16",
        gateway_task_id="task-2",
        generation_attempt=2,
        source_worker_job_id="old-job",
        resume_attempt_id="resume:task-2:attempt:2",
        request_batch_id="batch-2",
        extension_request_id="request-2",
    ))

    assert result["job_id"] == created["job_id"]
    assert result["remote_submission_state"] == "accepted"
    assert result["output_media_id"] == "media-2"
    assert calls[0]["batch_id"] == "batch-2"
    assert calls[0]["extension_request_id"] == "request-2"


@pytest.mark.asyncio
async def test_resume_submit_returns_accepted_persist_failed_without_attribute_error(monkeypatch):
    from agent.api import omni_test
    from agent.db import crud as agent_crud

    class FakeClient:
        connected = True
        _flow_key = "present"

    class FakeOmniClient:
        def __init__(self, client):
            self.client = client

        async def submit_reference_video(self, **kwargs):
            return {"status": 200, "data": {"media": [{"name": "media-2"}], "workflows": [{"name": "workflow-2"}]}}

    async def create_job(job_id, project_id, prompt, image_path, idempotency_key=None, **fields):
        return {"job_id": job_id, "project_id": project_id, "prompt": prompt, "image_path": image_path, **fields, "created": True, "reused": False}

    async def update_required(job_id, **fields):
        raise agent_crud.RowNotUpdatedError("missing row")

    monkeypatch.setattr(omni_test, "get_flow_client", lambda: FakeClient())
    monkeypatch.setattr(omni_test, "OmniClient", FakeOmniClient)
    monkeypatch.setattr(omni_test.crud, "create_omni_test_job", create_job)
    monkeypatch.setattr(omni_test.crud, "update_omni_test_job_required", update_required)
    monkeypatch.setattr(omni_test, "_ensure_polling", lambda job_id: (_ for _ in ()).throw(AssertionError("polling must not start")))

    response = await omni_test.resume_omni_video(omni_test.OmniVideoResumeRequest(
        idempotency_key="storyboard:task-2:attempt:2",
        project_id="project-2",
        input_media_id="input-2",
        image_path="D:/image.png",
        prompt="p",
        duration=10,
        aspect_ratio="9:16",
        resume_attempt_id="resume:task-2:attempt:2",
        request_batch_id="batch-2",
        extension_request_id="request-2",
    ))
    assert response.status_code == 500
    body = response.body.decode("utf-8")
    assert "accepted_persist_failed" in body
    assert "media-2" in body


@pytest.mark.asyncio
async def test_manual_flow_results_lists_only_new_video_candidates(monkeypatch):
    from agent.api import omni_test

    calls = []

    async def list_requests(project_id=None, status=None):
        calls.append((project_id, status))
        return [
            {"id": "old", "project_id": project_id, "status": "COMPLETED", "type": "GENERATE_VIDEO", "media_id": "old-media", "request_id": "old-op", "created_at": "2026-07-27T07:00:00Z", "updated_at": "2026-07-27T07:00:00Z"},
            {"id": "image", "project_id": project_id, "status": "COMPLETED", "type": "GENERATE_IMAGE", "media_id": "image-media", "request_id": "image-op", "created_at": "2026-07-27T09:00:00Z", "updated_at": "2026-07-27T09:00:00Z"},
            {"id": "new", "project_id": project_id, "status": "COMPLETED", "type": "GENERATE_VIDEO", "media_id": "new-media", "request_id": "new-op", "created_at": "2026-07-27T09:00:00Z", "updated_at": "2026-07-27T09:00:00Z"},
        ]

    monkeypatch.setattr(omni_test.crud, "list_requests", list_requests)

    result = await omni_test.list_manual_flow_results(
        "project-1",
        after="2026-07-27T08:00:00Z",
        exclude_media_ids="old-media",
    )

    assert calls == [("project-1", "COMPLETED")]
    assert result["candidate_count"] == 1
    assert result["candidates"][0]["media_id"] == "new-media"
    assert result["candidates"][0]["operation_id"] == "new-op"


@pytest.mark.asyncio
async def test_manual_flow_result_download_requires_valid_mp4(monkeypatch, tmp_path):
    from agent.api import omni_test
    import agent.config as config

    monkeypatch.setattr(config, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(omni_test, "OUTPUT_DIR", tmp_path)

    class FakeClient:
        connected = True
        _flow_key = "present"

        async def get_media(self, media_id):
            return {"data": {"video": {"encodedVideo": "data:video/mp4;base64,AAAAEGZ0eXBtcDQy"}}}

    monkeypatch.setattr(omni_test, "get_flow_client", lambda: FakeClient())

    result = await omni_test.download_manual_flow_result("manual-media")

    assert result["status"] == "completed"
    assert result["media_id"] == "manual-media"
    assert Path(result["video_path"]).exists()
