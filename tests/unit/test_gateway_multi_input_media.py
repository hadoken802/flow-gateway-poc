import asyncio
from pathlib import Path

import pytest


class FakeUpload:
    def __init__(self, filename, content_type, data):
        self.filename = filename
        self.content_type = content_type
        self._data = data

    async def read(self):
        return self._data


PNG = b"\x89PNG\r\n\x1a\n" + b"x" * 32
JPG = b"\xff\xd8\xff" + b"x" * 32


@pytest.mark.asyncio
async def test_single_and_multi_image_upload_preserve_order(tmp_path):
    from gateway import client_files
    from gateway.db import connect

    db = await connect(tmp_path / "gateway.db")
    one = await client_files.save_upload(db, tmp_path / "gateway.db", FakeUpload("a.png", "image/png", PNG))
    two = await client_files.save_upload(db, tmp_path / "gateway.db", FakeUpload("b.jpg", "image/jpeg", JPG))
    assert one["file_id"].startswith("file_")
    assert [one["original_filename"], two["original_filename"]] == ["a.png", "b.jpg"]
    assert "stored_filename" not in one
    await db.close()


@pytest.mark.asyncio
async def test_multi_upload_returns_item_error_for_invalid_file(tmp_path):
    from gateway import client_files
    from gateway.db import connect

    db = await connect(tmp_path / "gateway.db")
    ok = await client_files.save_upload(db, tmp_path / "gateway.db", FakeUpload("a.png", "image/png", PNG))
    bad = await client_files.save_upload(db, tmp_path / "gateway.db", FakeUpload("bad.txt", "text/plain", b"not image"))
    assert ok["ok"] is True
    assert bad == {"ok": False, "original_filename": "bad.txt", "error": "unsupported_image_type"}
    await db.close()


@pytest.mark.asyncio
async def test_create_task_with_input_file_ids_records_order(tmp_path):
    from gateway import client_files, crud
    from gateway.db import connect

    db_path = tmp_path / "gateway.db"
    db = await connect(db_path)
    first = await client_files.save_upload(db, db_path, FakeUpload("first.png", "image/png", PNG))
    second = await client_files.save_upload(db, db_path, FakeUpload("second.jpg", "image/jpeg", JPG))
    task = await crud.create_task(db, {
        "idempotency_key": "multi-order",
        "input_file_ids": [first["file_id"], second["file_id"]],
        "prompt": "Use both images in this order",
        "duration": 10,
        "aspect_ratio": "9:16",
    })
    media = await crud.list_task_input_media(db, task["task_id"])
    assert [item["file_id"] for item in media] == [first["file_id"], second["file_id"]]
    assert [item["position"] for item in media] == [0, 1]
    await db.close()


@pytest.mark.asyncio
async def test_duplicate_idempotency_key_reuses_multi_image_task(tmp_path):
    from gateway import client_files, crud
    from gateway.db import connect

    db_path = tmp_path / "gateway.db"
    db = await connect(db_path)
    first = await client_files.save_upload(db, db_path, FakeUpload("first.png", "image/png", PNG))
    second = await client_files.save_upload(db, db_path, FakeUpload("second.jpg", "image/jpeg", JPG))
    payload = {"idempotency_key": "same", "input_file_ids": [first["file_id"], second["file_id"]], "prompt": "p"}
    created = await crud.create_task(db, payload)
    reused = await crud.create_task(db, payload)
    assert reused["task_id"] == created["task_id"]
    assert reused["reused"] is True
    await db.close()


@pytest.mark.asyncio
async def test_worker_uploads_all_images_as_ordered_reference_media(monkeypatch, tmp_path):
    from agent.api import omni_test

    img1 = tmp_path / "1.png"
    img2 = tmp_path / "2.jpg"
    img1.write_bytes(PNG)
    img2.write_bytes(JPG)
    uploads = []
    submitted = {}

    class FakeClient:
        connected = True
        _flow_key = "present"

        async def get_credits(self):
            return {"credits": 100}

        async def upload_image(self, image_base64, mime_type, project_id, file_name):
            uploads.append(file_name)
            return {"_mediaId": f"media-{file_name}"}

    class FakeOmni:
        def __init__(self, client):
            pass

        async def submit_reference_video(self, **kwargs):
            submitted.update(kwargs)
            return {"media": [{"name": "out", "mediaStatus": {"mediaGenerationStatus": "SCHEDULED"}}], "workflows": [{"name": "wf", "metadata": {"batchId": "batch"}}]}

    async def no_existing(_key):
        return None

    async def create_job(job_id, project_id, prompt, image_path, idempotency_key=None, **fields):
        return {"job_id": job_id, "project_id": project_id, "prompt": prompt, "image_path": image_path, "idempotency_key": idempotency_key, "reused": False}

    async def update_job(job_id, **fields):
        return {"job_id": job_id, **fields}

    monkeypatch.setattr(omni_test, "get_flow_client", lambda: FakeClient())
    monkeypatch.setattr(omni_test, "OmniClient", FakeOmni)
    monkeypatch.setattr(omni_test.crud, "get_omni_test_job_by_idempotency_key", no_existing)
    monkeypatch.setattr(omni_test.crud, "create_omni_test_job", create_job)
    monkeypatch.setattr(omni_test.crud, "update_omni_test_job_required", update_job)
    monkeypatch.setattr(omni_test.crud, "update_omni_test_job", update_job)
    monkeypatch.setattr(omni_test.crud, "get_omni_test_job", lambda job_id: update_job(job_id, status="scheduled"))
    monkeypatch.setattr(omni_test, "_ensure_polling", lambda _job_id: None)

    response = await omni_test.submit_omni_video(omni_test.OmniVideoRequest(
        idempotency_key="multi",
        project_id="project",
        image_paths=[str(img1), str(img2)],
        prompt="p",
        duration=10,
        aspect_ratio="9:16",
    ))
    assert uploads == ["1.png", "2.jpg"]
    assert submitted["reference_media_ids"] == ["media-1.png", "media-2.jpg"]
    assert response["input_media_ids"] == ["media-1.png", "media-2.jpg"]
