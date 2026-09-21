import base64
import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent.api import omni_test


def _mp4_bytes():
    return b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 32


@pytest.mark.asyncio
async def test_automatic_and_requested_download_share_one_attempt(monkeypatch, tmp_path):
    job = {"job_id": "concurrent-download", "project_id": "project", "output_media_id": "media",
           "status": "waiting_download", "video_path": None}
    calls = []

    async def get_job(job_id):
        return dict(job)

    async def update_job(job_id, **fields):
        job.update(fields)
        return dict(job)

    async def get_media(media_id):
        calls.append(media_id)
        await asyncio.sleep(0.02)
        return {"data": {"video": {"encodedVideo": base64.b64encode(_mp4_bytes()).decode()}}}

    monkeypatch.setattr(omni_test, "get_flow_client", lambda: SimpleNamespace(get_media=get_media))
    monkeypatch.setattr(omni_test, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(omni_test, "crud", SimpleNamespace(get_omni_test_job=get_job,
                        update_omni_test_job=update_job, _now=lambda: "now"))
    monkeypatch.setattr(omni_test.OmniClient, "check_status", AsyncMock(return_value={"data": {}}))
    monkeypatch.setattr(omni_test, "_download_locks", {})
    await asyncio.gather(omni_test._retry_download_existing_job(dict(job), wait_schedule=[0]),
                         omni_test.retry_omni_video_download(job["job_id"]))
    assert calls == ["media"]
    assert job["status"] == "completed"


@pytest.mark.asyncio
@pytest.mark.parametrize("matching", [True, False])
async def test_page_recovery_never_submits_and_checks_project(monkeypatch, matching):
    media_id = "22222222-2222-4222-8222-222222222222"
    job = {"job_id": "lost-response", "project_id": "project", "status": "failed",
           "output_media_id": None, "error_code": "submit_error"}
    updated = []
    polled = []
    send = AsyncMock(return_value={"data": {"projectId": "project" if matching else "other", "mediaId": media_id}})
    client = SimpleNamespace(_send=send, _page_video_jobs=set())

    async def update(job_id, **fields):
        updated.append(fields)
        return {**job, **fields}

    monkeypatch.setattr(omni_test, "crud", SimpleNamespace(get_omni_test_job=AsyncMock(return_value=job), update_omni_test_job=update))
    monkeypatch.setattr(omni_test, "get_flow_client", lambda: client)
    monkeypatch.setattr(omni_test, "_ensure_polling", polled.append)
    if matching:
        result = await omni_test.recover_page_result(job["job_id"])
        assert result["submit_called"] is False
        assert result["output_media_id"] == media_id
        assert polled == [job["job_id"]]
    else:
        with pytest.raises(omni_test.HTTPException):
            await omni_test.recover_page_result(job["job_id"])
        assert not updated
        assert not polled
    assert send.call_args.args[0] == "page_reconcile_video"
    assert send.call_count == 1


def test_decode_plain_encoded_video_mp4():
    candidate = type("Candidate", (), {
        "encoded_video": base64.b64encode(_mp4_bytes()).decode(),
        "path": "video.encodedVideo",
    })()

    assert omni_test._decode_encoded_video_candidate(candidate, "media-123456").startswith(b"\x00\x00\x00\x18ftyp")


def test_decode_data_url_encoded_video_mp4():
    encoded = "data:video/mp4;base64," + base64.b64encode(_mp4_bytes()).decode()
    candidate = type("Candidate", (), {"encoded_video": encoded, "path": "video.encodedVideo"})()

    assert omni_test._decode_encoded_video_candidate(candidate, "media-123456")[4:8] == b"ftyp"


def test_invalid_base64_is_rejected():
    candidate = type("Candidate", (), {"encoded_video": "not base64!!!", "path": "video.encodedVideo"})()

    with pytest.raises(ValueError, match="invalid Base64"):
        omni_test._decode_encoded_video_candidate(candidate, "media-123456")


def test_decoded_non_mp4_is_rejected():
    candidate = type("Candidate", (), {
        "encoded_video": base64.b64encode(b"{\"error\":true}").decode(),
        "path": "video.encodedVideo",
    })()

    with pytest.raises(ValueError, match="response is JSON"):
        omni_test._decode_encoded_video_candidate(candidate, "media-123456")


def test_write_mp4_uses_part_then_atomic_rename(tmp_path):
    dest = tmp_path / "job-1.mp4"

    omni_test._write_valid_mp4_bytes_atomic(_mp4_bytes(), dest)

    assert dest.exists()
    assert not (tmp_path / "job-1.mp4.part").exists()
    assert dest.read_bytes()[4:8] == b"ftyp"


def test_invalid_write_cleans_part_file(tmp_path):
    dest = tmp_path / "job-1.mp4"

    with pytest.raises(ValueError):
        omni_test._write_valid_mp4_bytes_atomic(b"<html>bad</html>", dest)

    assert not (tmp_path / "job-1.mp4.part").exists()


def test_existing_valid_mp4_is_reused(tmp_path):
    dest = tmp_path / "job-1.mp4"
    dest.write_bytes(_mp4_bytes())

    assert omni_test._valid_existing_mp4(dest) is True


def test_encoded_video_content_is_not_logged(caplog):
    raw = base64.b64encode(_mp4_bytes()).decode()
    candidate = type("Candidate", (), {"encoded_video": raw, "path": "video.encodedVideo"})()

    with caplog.at_level(logging.INFO):
        omni_test._decode_encoded_video_candidate(candidate, "media-123456")

    assert raw not in caplog.text
    assert "video.encodedVideo" in caplog.text


@pytest.mark.asyncio
async def test_download_completed_saves_encoded_video_and_updates_original_job(monkeypatch, tmp_path):
    payload = {"name": "media-1", "video": {"encodedVideo": base64.b64encode(_mp4_bytes()).decode()}}
    updates = []

    class FakeClient:
        async def get_media(self, media_id):
            assert media_id == "media-1"
            return {"data": payload}

    async def fake_update(job_id, **fields):
        updates.append((job_id, fields))
        return {"job_id": job_id, **fields}

    monkeypatch.setattr(omni_test, "get_flow_client", lambda: FakeClient())
    monkeypatch.setattr(omni_test, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(omni_test, "crud", SimpleNamespace(update_omni_test_job=fake_update, _now=lambda: "now"))

    await omni_test._download_completed({"job_id": "job-1", "output_media_id": "media-1"})

    dest = tmp_path / "job-1.mp4"
    assert dest.exists()
    assert dest.read_bytes()[4:8] == b"ftyp"
    assert updates[-1][0] == "job-1"
    assert updates[-1][1]["status"] == "completed"
    assert updates[-1][1]["video_path"] == str(dest)
    assert updates[-1][1]["error_code"] is None
    assert updates[-1][1]["error_message"] is None


@pytest.mark.asyncio
async def test_download_completed_prefers_encoded_video_over_video_url(monkeypatch, tmp_path):
    payload = {
        "name": "media-1",
        "video": {
            "encodedVideo": base64.b64encode(_mp4_bytes()).decode(),
            "servingUri": "https://v.example/video.mp4",
        },
    }
    downloaded_urls = []

    class FakeClient:
        async def get_media(self, media_id):
            return {"data": payload}

    async def fake_update(job_id, **fields):
        return {"job_id": job_id, **fields}

    async def fake_download(url, dest):
        downloaded_urls.append(url)

    monkeypatch.setattr(omni_test, "get_flow_client", lambda: FakeClient())
    monkeypatch.setattr(omni_test, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(omni_test, "_download_mp4", fake_download)
    monkeypatch.setattr(omni_test, "crud", SimpleNamespace(update_omni_test_job=fake_update, _now=lambda: "now"))

    await omni_test._download_completed({"job_id": "job-1", "output_media_id": "media-1"})

    assert downloaded_urls == []
    assert (tmp_path / "job-1.mp4").exists()


@pytest.mark.asyncio
async def test_retry_download_reuses_existing_output_dir_mp4_without_get_media(monkeypatch, tmp_path):
    dest = tmp_path / "job-1.mp4"
    dest.write_bytes(_mp4_bytes())
    updates = []
    get_media_calls = []

    class FakeClient:
        async def get_media(self, media_id):
            get_media_calls.append(media_id)
            return {"data": {}}

    async def fake_get(job_id):
        return {
            "job_id": job_id,
            "project_id": "project-1",
            "output_media_id": "media-1",
            "status": "waiting_download",
            "video_path": None,
        }

    async def fake_update(job_id, **fields):
        updates.append(fields)
        return {**(await fake_get(job_id)), **fields}

    monkeypatch.setattr(omni_test, "get_flow_client", lambda: FakeClient())
    monkeypatch.setattr(omni_test, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(omni_test, "crud", SimpleNamespace(
        get_omni_test_job=fake_get,
        update_omni_test_job=fake_update,
        _now=lambda: "now",
    ))
    monkeypatch.setattr(omni_test.OmniClient, "check_status", AsyncMock(return_value={"data": {}}))

    await omni_test._retry_download_existing_job(await fake_get("job-1"), wait_schedule=[0])

    assert get_media_calls == []
    assert updates[-1]["status"] == "completed"
    assert updates[-1]["video_path"] == str(dest)
    assert updates[-1]["error_code"] is None
    assert updates[-1]["error_message"] is None


@pytest.mark.asyncio
async def test_retry_download_existing_valid_mp4_clears_old_error(monkeypatch, tmp_path):
    dest = tmp_path / "job-1.mp4"
    dest.write_bytes(_mp4_bytes())
    updates = []

    async def fake_get(job_id):
        return {
            "job_id": job_id,
            "status": "completed",
            "video_path": str(dest),
            "error_code": "missing_video_url",
            "error_message": "old",
        }

    async def fake_update(job_id, **fields):
        updates.append((job_id, fields))
        return {**(await fake_get(job_id)), **fields}

    monkeypatch.setattr(omni_test, "crud", SimpleNamespace(
        get_omni_test_job=fake_get,
        update_omni_test_job=fake_update,
        _now=lambda: "now",
    ))

    response = await omni_test.retry_omni_video_download("job-1")

    assert response["reused"] is True
    assert updates[-1][1]["error_code"] is None
    assert updates[-1][1]["error_message"] is None


@pytest.mark.asyncio
async def test_retry_download_endpoint_runs_one_bounded_attempt(monkeypatch):
    job_id = "job-bounded-retry"
    job = {
        "job_id": job_id,
        "project_id": "project-1",
        "output_media_id": "media-1",
        "status": "waiting_download",
        "video_path": None,
    }
    schedules = []

    async def fake_get(_job_id):
        return dict(job)

    async def fake_retry(_job, wait_schedule=None):
        schedules.append(wait_schedule)

    monkeypatch.setattr(omni_test, "crud", SimpleNamespace(get_omni_test_job=fake_get))
    monkeypatch.setattr(omni_test, "_retry_download_existing_job", fake_retry)

    response = await omni_test.retry_omni_video_download(job_id)

    assert response["status"] == "downloading"
    await omni_test._download_tasks[job_id]
    assert schedules == [[0]]


def test_page_job_route_is_restored_from_persisted_input_after_worker_restart():
    from agent.services.flow_client import FlowClient

    client = FlowClient()
    job = {"input_media_id": "page-upload:input", "output_media_id": "saved-output"}
    omni_test._restore_page_job_route(client, job)
    assert "saved-output" in client._page_video_jobs
    assert "saved-output" in client._page_video_outputs
    legacy = FlowClient()
    omni_test._restore_page_job_route(legacy, {"input_media_id": "api-upload", "output_media_id": "other"})
    assert not legacy._page_video_jobs


@pytest.mark.asyncio
async def test_persistent_download_error_stops_after_two_attempts(monkeypatch, tmp_path):
    job = {"job_id": "bounded-failure", "project_id": "project", "output_media_id": "output",
           "input_media_id": "page-upload:input", "status": "waiting_download"}
    client = SimpleNamespace(_page_video_jobs=set(), _page_video_outputs=set(),
                             recover_page_video_download=AsyncMock())
    async def get(_): return dict(job)
    async def update(_, **fields): job.update(fields); return dict(job)
    calls = []
    async def download(_):
        calls.append(1)
        job.update(error_code="get_media_error", error_message="PAGE_VIDEO_DOWNLOAD_RESOLUTION_NOT_FOUND")
    monkeypatch.setattr(omni_test, "get_flow_client", lambda: client)
    monkeypatch.setattr(omni_test, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(omni_test, "crud", SimpleNamespace(get_omni_test_job=get, update_omni_test_job=update))
    monkeypatch.setattr(omni_test, "_download_completed", download)
    monkeypatch.setattr(omni_test.OmniClient, "check_status", AsyncMock(return_value={"data": {}}))
    monkeypatch.setattr(omni_test.asyncio, "sleep", AsyncMock())
    await omni_test._retry_download_existing_job(job)
    assert len(calls) == 2
    client.recover_page_video_download.assert_awaited_once_with("output")
    assert job["status"] == "failed"
    assert job["error_message"] == "PAGE_VIDEO_DOWNLOAD_RESOLUTION_NOT_FOUND"


@pytest.mark.asyncio
async def test_retry_endpoint_reports_active_download_without_waiting_for_lock(monkeypatch):
    job = {'job_id': 'already-downloading', 'status': 'waiting_download', 'video_path': None}
    lock = omni_test.asyncio.Lock()
    await lock.acquire()
    monkeypatch.setitem(omni_test._download_locks, job['job_id'], lock)
    monkeypatch.setattr(omni_test, 'crud', SimpleNamespace(get_omni_test_job=AsyncMock(return_value=job)))
    retry = AsyncMock(side_effect=AssertionError('must not start or join a second download'))
    monkeypatch.setattr(omni_test, '_retry_download_existing_job', retry)
    try:
        snapshot = omni_test._public_job(job)
        assert snapshot['status'] == 'downloading'
        result = await omni_test.retry_omni_video_download(job['job_id'])
        assert result['status'] == 'downloading'
        assert result['download_in_progress'] is True
        retry.assert_not_awaited()
    finally:
        lock.release()


@pytest.mark.asyncio
async def test_slow_browser_download_outlives_short_http_timeout(monkeypatch, tmp_path):
    job = {'job_id':'slow-browser', 'project_id':'project', 'output_media_id':'media', 'status':'waiting_download'}
    async def get(_): return dict(job)
    async def update(_, **fields): job.update(fields);return dict(job)
    calls = []
    async def slow_download(_):
        calls.append(1)
        dest = tmp_path/'slow.mp4';dest.write_bytes(_mp4_bytes())
        job.update(status='completed',video_path=str(dest))
    async def virtual_wait_for(operation, timeout):
        if operation.cr_code.co_name == 'slow_download' and timeout < 180:
            operation.close()
            raise asyncio.TimeoutError()
        return await operation
    monkeypatch.setattr(omni_test, 'get_flow_client', lambda: SimpleNamespace())
    monkeypatch.setattr(omni_test, 'OUTPUT_DIR', tmp_path)
    monkeypatch.setattr(omni_test, 'crud', SimpleNamespace(get_omni_test_job=get,update_omni_test_job=update))
    monkeypatch.setattr(omni_test.OmniClient, 'check_status', AsyncMock(return_value={'data':{}}))
    monkeypatch.setattr(omni_test, '_download_completed', slow_download)
    monkeypatch.setattr(omni_test.asyncio, 'wait_for', virtual_wait_for)
    await omni_test._retry_download_existing_job(job, wait_schedule=[0])
    assert job['status']=='completed'
    assert calls == [1]


@pytest.mark.asyncio
async def test_retry_endpoint_starts_once_and_returns_while_download_runs(monkeypatch):
    job={'job_id':'background-download','status':'failed','video_path':None}
    entered=asyncio.Event();release=asyncio.Event();calls=[]
    async def slow_retry(_, wait_schedule=None):
        calls.append(1);entered.set();await release.wait()
    monkeypatch.setattr(omni_test,'crud',SimpleNamespace(get_omni_test_job=AsyncMock(return_value=job)))
    monkeypatch.setattr(omni_test,'_retry_download_existing_job',slow_retry)
    try:
        response=await asyncio.wait_for(omni_test.retry_omni_video_download(job['job_id']),0.1)
        assert response['status']=='downloading'
        await entered.wait()
        again=await omni_test.retry_omni_video_download(job['job_id'])
        assert again['download_in_progress'] is True
        assert calls==[1]
    finally:
        release.set();await asyncio.sleep(0)
