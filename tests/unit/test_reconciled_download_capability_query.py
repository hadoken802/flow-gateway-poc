import argparse
import hashlib
import json
import sqlite3

import pytest

import gateway.reconciled_download_capability_query as query
from agent.services.remote_media_capability import summarize_media_capability


TASK_ID = "3d98ece6-4ace-4dc6-817c-979d23881bc3"
ACCOUNT_ID = "FLOW-002"
PROJECT_ID = "c23337ee-3be2-4e13-a6b0-d74c87675394"
JOB_ID = "c443c4e8-fff7-59a7-a8b9-9a8f2c8d09eb"
OUTPUT_MEDIA_ID = "ed7cef70-ae56-4f7a-9c7f-2da5aa66d44a"
WORKFLOW_ID = "475f2092-1f91-46e1-ba2b-968e3dfa3aec"
BATCH_ID = "f2b691af-823b-4002-9d4d-d67a22f6a281"


def _poll_result():
    return {
        "task_id": TASK_ID,
        "account_id": ACCOUNT_ID,
        "job_id": JOB_ID,
        "remote_state": "remote_completed",
        "remote_query_call_count": 1,
        "submit_called": False,
        "download_called": False,
        "writes_performed": False,
        "remote_query": {
            "project_id": PROJECT_ID,
            "output_media_id": OUTPUT_MEDIA_ID,
            "remote_state": "remote_completed",
            "completed": True,
            "database_writes_performed": False,
        },
    }


def _seed(gateway_db, agent_db, poll_sha):
    gw = sqlite3.connect(gateway_db)
    gw.executescript(
        """
        CREATE TABLE flow_accounts(account_id TEXT PRIMARY KEY, status TEXT, current_task_id TEXT, lock_version INTEGER);
        CREATE TABLE flow_tasks(task_id TEXT PRIMARY KEY, status TEXT, project_id TEXT, account_id TEXT, assigned_account_id TEXT, worker_job_id TEXT, generation_attempts INTEGER, lease_version INTEGER, output_media_id TEXT, workflow_id TEXT, upstream_batch_id TEXT);
        """
    )
    gw.execute("INSERT INTO flow_accounts VALUES(?,?,?,?)", (ACCOUNT_ID, "busy", TASK_ID, 2))
    gw.execute("INSERT INTO flow_tasks VALUES(?,?,?,?,?,?,?,?,?,?,?)", (TASK_ID, query.GATEWAY_REQUIRED_STATUS, PROJECT_ID, ACCOUNT_ID, ACCOUNT_ID, JOB_ID, 2, 2, OUTPUT_MEDIA_ID, WORKFLOW_ID, BATCH_ID))
    gw.commit(); gw.close()
    ag = sqlite3.connect(agent_db)
    ag.executescript(
        """
        CREATE TABLE omni_test_jobs(job_id TEXT PRIMARY KEY, project_id TEXT, output_media_id TEXT, workflow_id TEXT, upstream_batch_id TEXT, status TEXT, video_path TEXT, completed_at TEXT, raw_response_shape TEXT);
        CREATE TABLE request(id TEXT);
        """
    )
    raw = {"reconciled_poll_result": {"poll_result_sha256": poll_sha, "remote_state": "remote_completed"}}
    ag.execute("INSERT INTO omni_test_jobs VALUES(?,?,?,?,?,?,?,?,?)", (JOB_ID, PROJECT_ID, OUTPUT_MEDIA_ID, WORKFLOW_ID, BATCH_ID, query.AGENT_REQUIRED_STATUS, None, None, json.dumps(raw)))
    ag.commit(); ag.close()


def _setup(tmp_path, monkeypatch):
    poll = tmp_path / "poll-result.json"
    poll.write_text(json.dumps(_poll_result(), sort_keys=True), encoding="utf-8")
    sha = hashlib.sha256(poll.read_bytes()).hexdigest()
    monkeypatch.setattr(query, "EXPECTED_POLL_SHA", sha)
    gw = tmp_path / "gateway.db"; ag = tmp_path / "agent.db"
    _seed(gw, ag, sha)
    return gw, ag, poll, sha


def _args(gw, ag, poll, *, execute=False, sha=None, result_file=None):
    return argparse.Namespace(task_id=TASK_ID, poll_result_file=str(poll), gateway_db=str(gw), agent_db=str(ag), worker_base_url="http://fake", execute=execute, confirm_task_id=TASK_ID if execute else None, confirm_project_id=PROJECT_ID if execute else None, confirm_account_id=ACCOUNT_ID if execute else None, confirm_job_id=JOB_ID if execute else None, confirm_output_media_id=OUTPUT_MEDIA_ID if execute else None, confirm_generation_attempt=2 if execute else None, confirm_lock_version=2 if execute else None, confirm_lease_version=2 if execute else None, confirm_poll_result_sha256=sha if execute else None, allow_real_media_query=execute, result_file=str(result_file) if result_file else None)


def test_summarize_encoded_video_without_body():
    data = {"media": {"video": {"generatedVideo": {"encodedVideo": "AAAAZm9vYmFy", "mimeType": "video/mp4"}}}}
    result = summarize_media_capability(data, media_id=OUTPUT_MEDIA_ID)
    assert result["download_capability"] == "encoded_video_available"
    assert result["encoded_video_present"] is True
    assert result["encoded_video_length"] == len("AAAAZm9vYmFy")
    assert "AAAAZm9vYmFy" not in json.dumps(result)


def test_summarize_signed_url_without_query_values():
    data = {"media": {"video": {"generatedVideo": {"downloadUrl": "https://cdn.example/video.mp4?sig=secret&Expires=123"}}}}
    result = summarize_media_capability(data, media_id=OUTPUT_MEDIA_ID)
    assert result["download_capability"] == "signed_video_url_available"
    assert result["video_url_host"] == "cdn.example"
    assert result["video_url_query_parameter_names"] == ["Expires", "sig"]
    assert "secret" not in json.dumps(result)


def test_dry_run_does_not_call_worker(monkeypatch, tmp_path):
    gw, ag, poll, _sha = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(query, "_inspect_worker", lambda _url: {"health": {"status": "ok", "extension_connected": True}, "extension_connected": True, "route_available": True, "blocking_reasons": []})
    before = (hashlib.sha256(gw.read_bytes()).hexdigest(), hashlib.sha256(ag.read_bytes()).hexdigest())
    result = query.query_reconciled_download_capability_once(_args(gw, ag, poll))
    assert result["ok"] is True
    assert result["mode"] == "dry_run"
    assert result["get_media_called"] is False
    assert result["network_calls_performed"] == 0
    assert before == (hashlib.sha256(gw.read_bytes()).hexdigest(), hashlib.sha256(ag.read_bytes()).hexdigest())


def test_execute_requires_allow_and_result_file(monkeypatch, tmp_path):
    gw, ag, poll, sha = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(query, "_inspect_worker", lambda _url: {"health": {"status": "ok", "extension_connected": True}, "extension_connected": True, "route_available": True, "blocking_reasons": []})
    args = _args(gw, ag, poll, execute=True, sha=sha)
    args.allow_real_media_query = False
    result = query.query_reconciled_download_capability_once(args)
    assert result["ok"] is False
    assert "allow_real_media_query_required" in result["blocking_reasons"]
    assert "result_file_required" in result["blocking_reasons"]


class FakeWorker:
    def __init__(self, payload):
        self.calls = 0
        self.payload = payload

    async def query_omni_video_download_capability_once(self, *_args):
        self.calls += 1
        return dict(self.payload, get_media_call_count=1, submit_called=False, poll_called=False, download_called=False, database_writes_performed=False)


def test_execute_calls_fake_worker_once_and_sanitizes_result(monkeypatch, tmp_path):
    gw, ag, poll, sha = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(query, "_inspect_worker", lambda _url: {"health": {"status": "ok", "extension_connected": True}, "extension_connected": True, "route_available": True, "blocking_reasons": []})
    result_file = tmp_path / "capability.json"
    fake = FakeWorker({"query_ok": True, "download_capability": "signed_video_url_available", "url": "https://cdn.example/v.mp4?sig=secret"})
    result = query.query_reconciled_download_capability_once(_args(gw, ag, poll, execute=True, sha=sha, result_file=result_file), worker_client=fake)
    assert result["ok"] is True
    assert fake.calls == 1
    saved = result_file.read_text(encoding="utf-8")
    assert "secret" not in saved
    assert result["submit_called"] is False
    assert result["poll_called"] is False
    assert result["download_called"] is False


@pytest.mark.parametrize("status,cap", [(404, "media_not_found"), (403, "media_access_denied"), (500, "media_query_error")])
def test_error_capability_classes(status, cap):
    from agent.services.remote_media_capability import summarize_media_capability
    # Route-level HTTP errors are mapped by query_download_capability_once; shape-only unknown remains safe.
    assert summarize_media_capability({"status": "PROCESSING"}, media_id=OUTPUT_MEDIA_ID)["download_capability"] == "media_not_ready"
