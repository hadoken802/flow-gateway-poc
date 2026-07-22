import base64
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent.api import omni_test


def _mp4_bytes():
    return b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 32


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
