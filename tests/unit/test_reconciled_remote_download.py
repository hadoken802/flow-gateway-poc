import argparse
import base64
import hashlib
import json
import sqlite3

import pytest

import gateway.reconciled_remote_download as download
import gateway.reconciled_remote_download_preflight as preflight
from agent.services.reconciled_encoded_video_fetch import (
    decode_encoded_video,
    encoded_video_fingerprint,
    fetch_reconciled_encoded_video_once,
    fetch_reconciled_media_video_once,
    validate_mp4_bytes,
)
from gateway.worker_client import WorkerRemoteMediaFetchError, _safe_worker_error


TASK_ID = "3d98ece6-4ace-4dc6-817c-979d23881bc3"
ACCOUNT_ID = "FLOW-002"
PROJECT_ID = "c23337ee-3be2-4e13-a6b0-d74c87675394"
JOB_ID = "c443c4e8-fff7-59a7-a8b9-9a8f2c8d09eb"
OUTPUT_MEDIA_ID = "ed7cef70-ae56-4f7a-9c7f-2da5aa66d44a"
WORKFLOW_ID = "475f2092-1f91-46e1-ba2b-968e3dfa3aec"
BATCH_ID = "f2b691af-823b-4002-9d4d-d67a22f6a281"


def _mp4_bytes():
    return b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom" + b"\x00" * 32


def _poll_result():
    return {
        "task_id": TASK_ID,
        "account_id": ACCOUNT_ID,
        "job_id": JOB_ID,
        "project_id": PROJECT_ID,
        "output_media_id": OUTPUT_MEDIA_ID,
        "remote_state": "remote_completed",
        "remote_query_call_count": 1,
        "submit_called": False,
        "download_called": False,
        "writes_performed": False,
        "remote_query": {
            "query_ok": True,
            "project_id": PROJECT_ID,
            "output_media_id": OUTPUT_MEDIA_ID,
            "remote_state": "remote_completed",
            "raw_status": "MEDIA_GENERATION_STATUS_SUCCESSFUL",
            "completed": True,
            "database_writes_performed": False,
        },
    }


def _capability_result(encoded):
    fp = encoded_video_fingerprint(encoded)
    return {
        "ok": True,
        "get_media_call_count": 1,
        "submit_called": False,
        "poll_called": False,
        "download_called": False,
        "writes_performed": False,
        "media_capability": {
            "download_capability": "encoded_video_available",
            "encoded_video_present": True,
            "encoded_video_length": fp["encoded_video_length"],
            "encoded_video_sha256": fp["encoded_video_sha256"],
            "get_media_call_count": 1,
            "submit_called": False,
            "poll_called": False,
            "download_called": False,
            "database_writes_performed": False,
        },
    }


def _seed(gateway_db, agent_db, poll_sha):
    gw = sqlite3.connect(gateway_db)
    gw.executescript(
        """
        CREATE TABLE flow_accounts(account_id TEXT PRIMARY KEY, status TEXT, current_task_id TEXT, lock_version INTEGER);
        CREATE TABLE flow_tasks(task_id TEXT PRIMARY KEY, status TEXT, project_id TEXT, account_id TEXT, assigned_account_id TEXT, worker_job_id TEXT, generation_attempts INTEGER, lease_version INTEGER, output_media_id TEXT, workflow_id TEXT, upstream_batch_id TEXT, video_path TEXT, completed_at TEXT);
        """
    )
    gw.execute("INSERT INTO flow_accounts VALUES(?,?,?,?)", (ACCOUNT_ID, "busy", TASK_ID, 2))
    gw.execute("INSERT INTO flow_tasks VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (TASK_ID, download.GATEWAY_REQUIRED_STATUS, PROJECT_ID, ACCOUNT_ID, ACCOUNT_ID, JOB_ID, 2, 2, OUTPUT_MEDIA_ID, WORKFLOW_ID, BATCH_ID, None, None))
    gw.commit(); gw.close()
    ag = sqlite3.connect(agent_db)
    ag.executescript(
        """
        CREATE TABLE omni_test_jobs(job_id TEXT PRIMARY KEY, project_id TEXT, output_media_id TEXT, workflow_id TEXT, upstream_batch_id TEXT, status TEXT, video_path TEXT, completed_at TEXT, raw_response_shape TEXT);
        CREATE TABLE request(id TEXT);
        """
    )
    raw = {"reconciled_poll_result": {"poll_result_sha256": poll_sha, "remote_state": "remote_completed"}}
    ag.execute("INSERT INTO omni_test_jobs VALUES(?,?,?,?,?,?,?,?,?)", (JOB_ID, PROJECT_ID, OUTPUT_MEDIA_ID, WORKFLOW_ID, BATCH_ID, download.AGENT_REQUIRED_STATUS, None, None, json.dumps(raw)))
    ag.commit(); ag.close()


def _setup(tmp_path, monkeypatch):
    encoded = base64.b64encode(_mp4_bytes()).decode()
    poll = tmp_path / "poll.json"
    cap = tmp_path / "cap.json"
    poll.write_text(json.dumps(_poll_result(), sort_keys=True), encoding="utf-8")
    cap.write_text(json.dumps(_capability_result(encoded), sort_keys=True), encoding="utf-8")
    poll_sha = hashlib.sha256(poll.read_bytes()).hexdigest()
    cap_sha = hashlib.sha256(cap.read_bytes()).hexdigest()
    monkeypatch.setattr(download, "EXPECTED_POLL_SHA", poll_sha)
    monkeypatch.setattr(preflight, "EXPECTED_POLL_SHA", poll_sha)
    monkeypatch.setattr(download, "EXPECTED_CAPABILITY_SHA", cap_sha)
    fp = encoded_video_fingerprint(encoded)
    monkeypatch.setattr(download, "EXPECTED_ENCODED_VIDEO_LENGTH", fp["encoded_video_length"])
    monkeypatch.setattr(download, "EXPECTED_ENCODED_VIDEO_SHA256", fp["encoded_video_sha256"])
    gw = tmp_path / "gateway.db"; ag = tmp_path / "agent.db"
    _seed(gw, ag, poll_sha)
    return gw, ag, poll, cap, poll_sha, cap_sha, fp


def _args(gw, ag, poll, cap, out, *, execute=False, poll_sha=None, cap_sha=None, fp=None, manifest=None):
    return argparse.Namespace(
        task_id=TASK_ID, poll_result_file=str(poll), capability_result_file=str(cap),
        gateway_db=str(gw), agent_db=str(ag), worker_base_url="http://fake",
        output_path=str(out), execute=execute, confirm_task_id=TASK_ID if execute else None,
        confirm_project_id=PROJECT_ID if execute else None, confirm_account_id=ACCOUNT_ID if execute else None,
        confirm_job_id=JOB_ID if execute else None, confirm_output_media_id=OUTPUT_MEDIA_ID if execute else None,
        confirm_generation_attempt=2 if execute else None, confirm_lock_version=2 if execute else None,
        confirm_lease_version=2 if execute else None, confirm_poll_result_sha256=poll_sha if execute else None,
        confirm_capability_result_sha256=cap_sha if execute else None,
        confirm_encoded_video_length=fp["encoded_video_length"] if execute and fp else None,
        confirm_encoded_video_sha256=fp["encoded_video_sha256"] if execute and fp else None,
        confirm_output_path=str(out) if execute else None, allow_real_download=execute,
        result_manifest=str(manifest) if manifest else None,
    )


def test_fingerprint_is_complete_utf8_encoded_string():
    encoded = "data:video/mp4;base64," + base64.b64encode(_mp4_bytes()).decode()
    fp = encoded_video_fingerprint(encoded)
    assert fp["encoded_video_length"] == len(encoded)
    assert fp["encoded_video_sha256"] == hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def test_decode_data_uri_and_plain_base64():
    encoded = base64.b64encode(_mp4_bytes()).decode()
    assert decode_encoded_video(encoded)[0].startswith(b"\x00\x00\x00\x18ftyp")
    assert decode_encoded_video("data:video/mp4;base64," + encoded)[2] is True


def test_invalid_base64_and_invalid_mp4_rejected():
    with pytest.raises(ValueError):
        decode_encoded_video("not base64!!!")
    with pytest.raises(ValueError):
        validate_mp4_bytes(b"<html>no")


def test_dry_run_does_not_call_worker_or_write(monkeypatch, tmp_path):
    gw, ag, poll, cap, _poll_sha, _cap_sha, _fp = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(download, "_inspect_worker", lambda _url: {"health": {"status": "ok", "extension_connected": True}, "extension_connected": True, "route_available": True, "blocking_reasons": []})
    before = (hashlib.sha256(gw.read_bytes()).hexdigest(), hashlib.sha256(ag.read_bytes()).hexdigest())
    result = download.download_reconciled_remote_result(_args(gw, ag, poll, cap, tmp_path / "out.mp4"))
    assert result["mode"] == "dry_run"
    assert result["get_media_called"] is False
    assert result["network_calls_performed"] == 0
    assert result["file_writes_performed"] is False
    assert result["database_writes_performed"] is False
    assert before == (hashlib.sha256(gw.read_bytes()).hexdigest(), hashlib.sha256(ag.read_bytes()).hexdigest())


class FakeWorker:
    def __init__(self, data, headers=None):
        self.calls = 0
        self.data = data
        self.headers = headers or {"x-get-media-call-count": "1"}

    async def fetch_reconciled_encoded_video_once(self, *_args):
        self.calls += 1
        return {"content": self.data, "headers": self.headers}


class FailingWorker:
    def __init__(self, response):
        self.calls = 0
        self.response = response

    async def fetch_reconciled_encoded_video_once(self, *_args):
        self.calls += 1
        raise WorkerRemoteMediaFetchError(409, "Worker encoded video fetch failed: HTTP 409", self.response)


class FakeResponse:
    def __init__(self, status_code, content, json_data=None, json_error=False):
        self.status_code = status_code
        self.content = content
        self._json_data = json_data
        self._json_error = json_error

    def json(self):
        if self._json_error:
            raise ValueError("not json")
        return self._json_data


def test_execute_writes_mp4_manifest_once(monkeypatch, tmp_path):
    gw, ag, poll, cap, poll_sha, cap_sha, fp = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(download, "_inspect_worker", lambda _url: {"health": {"status": "ok", "extension_connected": True}, "extension_connected": True, "route_available": True, "blocking_reasons": []})
    out = tmp_path / "out.mp4"
    manifest = tmp_path / "manifest.json"
    fake = FakeWorker(_mp4_bytes())
    result = download.download_reconciled_remote_result(_args(gw, ag, poll, cap, out, execute=True, poll_sha=poll_sha, cap_sha=cap_sha, fp=fp, manifest=manifest), fake)
    assert result["ok"] is True
    assert fake.calls == 1
    assert out.exists()
    assert manifest.exists()
    assert json.loads(manifest.read_text())["database_writes_performed"] is False


def test_execute_records_url_transport_headers(monkeypatch, tmp_path):
    gw, ag, poll, cap, poll_sha, cap_sha, fp = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(download, "_inspect_worker", lambda _url: {"health": {"status": "ok", "extension_connected": True}, "extension_connected": True, "route_available": True, "blocking_reasons": []})
    out = tmp_path / "out-url.mp4"
    manifest = tmp_path / "manifest-url.json"
    fake = FakeWorker(_mp4_bytes(), {"x-get-media-call-count": "1", "x-url-download-call-count": "1", "x-transport-detected": "video_url"})
    result = download.download_reconciled_remote_result(_args(gw, ag, poll, cap, out, execute=True, poll_sha=poll_sha, cap_sha=cap_sha, fp=fp, manifest=manifest), fake)
    assert result["ok"] is True
    assert result["url_download_call_count"] == 1
    written = json.loads(manifest.read_text())
    assert written["transport_detected"] == "video_url"
    assert written["url_download_call_count"] == 1


def test_execute_surfaces_worker_409_without_worker_submit_error(monkeypatch, tmp_path):
    gw, ag, poll, cap, poll_sha, cap_sha, fp = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(download, "_inspect_worker", lambda _url: {"health": {"status": "ok", "extension_connected": True}, "extension_connected": True, "route_available": True, "blocking_reasons": []})
    out = tmp_path / "out.mp4"
    manifest = tmp_path / "failure.json"
    response = {
        "ok": False,
        "error_code": "encoded_video_fingerprint_changed",
        "error_class": "remote_media_validation_error",
        "stage": "post_get_media_fingerprint_validation",
        "error_message_sanitized": "Encoded video fingerprint changed",
        "get_media_call_count": 1,
        "retry_safe": False,
        "expected_encoded_video_length": 3408328,
        "actual_encoded_video_length": 3409000,
        "expected_encoded_video_sha256": "expected",
        "actual_encoded_video_sha256": "actual",
    }
    fake = FailingWorker(response)
    result = download.download_reconciled_remote_result(_args(gw, ag, poll, cap, out, execute=True, poll_sha=poll_sha, cap_sha=cap_sha, fp=fp, manifest=manifest), fake)
    assert result["ok"] is False
    assert result["result"] == "worker_error"
    assert result["worker_http_status"] == 409
    assert result["worker_error_code"] == "encoded_video_fingerprint_changed"
    assert result["worker_error_stage"] == "post_get_media_fingerprint_validation"
    assert result["get_media_call_count"] == 1
    assert result["network_calls_performed"] == 1
    assert result["file_writes_performed"] is False
    assert result["database_writes_performed"] is False
    assert fake.calls == 1
    assert not out.exists()
    written = json.loads(manifest.read_text(encoding="utf-8"))
    assert written["worker_error_code"] == "encoded_video_fingerprint_changed"


def test_safe_worker_error_sanitizes_standard_json():
    payload = {
        "error_code": "encoded_video_fingerprint_changed",
        "encodedVideo": "A" * 2000,
        "base64": "BBBB",
        "download_url": "https://example.test/video.mp4?X-Goog-Signature=secret&Expires=1",
        "Authorization": "Bearer secret",
        "expected_encoded_video_length": 10,
        "actual_encoded_video_sha256": "abc",
    }
    body = json.dumps(payload).encode("utf-8")
    safe = _safe_worker_error(FakeResponse(409, body, payload))
    assert safe["error_code"] == "encoded_video_fingerprint_changed"
    assert safe["encodedVideo"] == "redacted"
    assert safe["base64"] == "redacted"
    assert safe["Authorization"] == "redacted"
    assert safe["download_url"]["query_parameter_names"] == ["Expires", "X-Goog-Signature"]
    assert "secret" not in json.dumps(safe)
    assert safe["expected_encoded_video_length"] == 10
    assert safe["actual_encoded_video_sha256"] == "abc"
    assert safe["worker_response_body_length"] == len(body)
    assert safe["worker_response_body_sha256"] == hashlib.sha256(body).hexdigest()


def test_safe_worker_error_unwraps_fastapi_detail_and_text():
    detail = {"detail": {"error_code": "encoded_video_missing", "get_media_call_count": 1}}
    safe = _safe_worker_error(FakeResponse(409, json.dumps(detail).encode("utf-8"), detail))
    assert safe["error_code"] == "encoded_video_missing"
    assert safe["get_media_call_count"] == 1
    text_safe = _safe_worker_error(FakeResponse(409, b"<html>Conflict</html>", json_error=True))
    assert text_safe["error_code"] == "worker_non_json_error"
    assert text_safe["worker_response_body_sha256"] == hashlib.sha256(b"<html>Conflict</html>").hexdigest()


def test_safe_worker_error_truncates_large_body():
    body = b"x" * (70 * 1024)
    safe = _safe_worker_error(FakeResponse(500, body, json_error=True))
    assert safe["worker_response_truncated"] is True
    assert safe["worker_response_body_length"] == len(body)
    assert safe["worker_response_body_sha256"] == hashlib.sha256(body).hexdigest()


def test_existing_file_blocks_before_remote(monkeypatch, tmp_path):
    gw, ag, poll, cap, poll_sha, cap_sha, fp = _setup(tmp_path, monkeypatch)
    out = tmp_path / "out.mp4"
    out.write_bytes(_mp4_bytes())
    result = download.download_reconciled_remote_result(_args(gw, ag, poll, cap, out, execute=True, poll_sha=poll_sha, cap_sha=cap_sha, fp=fp, manifest=tmp_path / "m.json"), FakeWorker(_mp4_bytes()))
    assert "final_file_already_exists" in result["blocking_reasons"]
    assert result["get_media_called"] is False


@pytest.mark.asyncio
async def test_agent_fetch_rejects_fingerprint_change():
    encoded = base64.b64encode(_mp4_bytes()).decode()
    class Client:
        async def get_media(self, _media_id):
            return {"data": {"video": {"encodedVideo": encoded}}}
    result = await fetch_reconciled_encoded_video_once(client=Client(), media_id=OUTPUT_MEDIA_ID, expected_encoded_video_length=len(encoded), expected_encoded_video_sha256="bad")
    assert result.ok is False
    assert result.manifest["error_code"] == "encoded_video_fingerprint_changed"


@pytest.mark.asyncio
async def test_atomic_fetch_keeps_encoded_video_success_path():
    encoded = base64.b64encode(_mp4_bytes()).decode()

    class Client:
        calls = 0

        async def get_media(self, _media_id):
            self.calls += 1
            return {"data": {"video": {"encodedVideo": encoded}}}

    client = Client()
    fp = encoded_video_fingerprint(encoded)
    result = await fetch_reconciled_media_video_once(
        client=client,
        media_id=OUTPUT_MEDIA_ID,
        expected_encoded_video_length=fp["encoded_video_length"],
        expected_encoded_video_sha256=fp["encoded_video_sha256"],
    )
    assert result.ok is True
    assert result.video_bytes == _mp4_bytes()
    assert result.manifest["transport_detected"] == "encoded_video"
    assert result.manifest["get_media_call_count"] == 1
    assert result.manifest["url_download_call_count"] == 0
    assert client.calls == 1


@pytest.mark.asyncio
async def test_atomic_fetch_uses_https_url_when_encoded_missing():
    class Client:
        calls = 0

        async def get_media(self, _media_id):
            self.calls += 1
            return {"data": {"video": {"generatedVideo": {"downloadUrl": "https://cdn.example/video.mp4?sig=secret&Expires=1"}}}}

    url_calls = []

    async def fetch_url(url):
        url_calls.append(url)
        return 200, "video/mp4", _mp4_bytes()

    client = Client()
    result = await fetch_reconciled_media_video_once(client=client, media_id=OUTPUT_MEDIA_ID, url_fetcher=fetch_url)
    assert result.ok is True
    assert result.video_bytes == _mp4_bytes()
    assert result.manifest["transport_detected"] == "video_url"
    assert result.manifest["encoded_video_present"] is False
    assert result.manifest["video_url_present"] is True
    assert result.manifest["video_url_host"] == "cdn.example"
    assert result.manifest["video_url_query_parameter_names"] == ["Expires", "sig"]
    assert "secret" not in json.dumps(result.manifest)
    assert result.manifest["get_media_call_count"] == 1
    assert result.manifest["url_download_call_count"] == 1
    assert client.calls == 1
    assert len(url_calls) == 1


@pytest.mark.asyncio
async def test_atomic_fetch_reports_missing_transport_shape():
    class Client:
        async def get_media(self, _media_id):
            return {"data": {"video": {"generatedVideo": {"model": "abra_r2v_10s"}}}}

    result = await fetch_reconciled_media_video_once(client=Client(), media_id=OUTPUT_MEDIA_ID)
    assert result.ok is False
    assert result.manifest["error_code"] == "media_transport_missing"
    assert result.manifest["stage"] == "post_get_media_transport_detection"
    assert result.manifest["encoded_video_present"] is False
    assert result.manifest["video_url_present"] is False
    assert result.manifest["get_media_call_count"] == 1
    assert result.manifest["url_download_call_count"] == 0
    assert "response_shape" in result.manifest


@pytest.mark.asyncio
async def test_atomic_fetch_url_http_error_called_once():
    class Client:
        calls = 0

        async def get_media(self, _media_id):
            self.calls += 1
            return {"data": {"video": {"generatedVideo": {"downloadUrl": "https://cdn.example/video.mp4?sig=secret"}}}}

    url_calls = 0

    async def fetch_url(_url):
        nonlocal url_calls
        url_calls += 1
        return 403, "text/html", b"<html>denied</html>"

    client = Client()
    result = await fetch_reconciled_media_video_once(client=client, media_id=OUTPUT_MEDIA_ID, url_fetcher=fetch_url)
    assert result.ok is False
    assert result.manifest["error_code"] == "video_url_http_error"
    assert result.manifest["http_status"] == 403
    assert result.manifest["get_media_call_count"] == 1
    assert result.manifest["url_download_call_count"] == 1
    assert client.calls == 1
    assert url_calls == 1


@pytest.mark.asyncio
async def test_atomic_fetch_rejects_non_https_url_before_url_get():
    class Client:
        async def get_media(self, _media_id):
            return {"data": {"video": {"generatedVideo": {"downloadUrl": "http://cdn.example/video.mp4?sig=secret"}}}}

    async def fetch_url(_url):
        raise AssertionError("URL fetch must not be called")

    result = await fetch_reconciled_media_video_once(client=Client(), media_id=OUTPUT_MEDIA_ID, url_fetcher=fetch_url)
    assert result.ok is False
    assert result.manifest["error_code"] == "video_url_not_https"
    assert result.manifest["url_download_call_count"] == 0
