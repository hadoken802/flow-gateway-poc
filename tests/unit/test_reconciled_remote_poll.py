import argparse
import asyncio
import hashlib
import json
import sqlite3

import pytest

from agent.services.remote_status_query import normalize_remote_status_response, query_bound_remote_status_once
from gateway.reconciled_remote_poll import _poll_reconciled_remote_result, poll_reconciled_remote_result


TASK_ID = "3d98ece6-4ace-4dc6-817c-979d23881bc3"
ACCOUNT_ID = "FLOW-002"
PROJECT_ID = "c23337ee-3be2-4e13-a6b0-d74c87675394"
JOB_ID = "c443c4e8-fff7-59a7-a8b9-9a8f2c8d09eb"
OUTPUT_MEDIA_ID = "ed7cef70-ae56-4f7a-9c7f-2da5aa66d44a"
WORKFLOW_ID = "475f2092-1f91-46e1-ba2b-968e3dfa3aec"
BATCH_ID = "f2b691af-823b-4002-9d4d-d67a22f6a281"


def _seed(gateway_db, agent_db, *, task_status="reconciled_remote_accepted_unknown", account_status="busy", job_status="remote_reconciled_bound", output_media_id=OUTPUT_MEDIA_ID, workflow_id=WORKFLOW_ID, batch_id=BATCH_ID):
    gw = sqlite3.connect(gateway_db)
    gw.executescript(
        """
        CREATE TABLE flow_accounts(account_id TEXT PRIMARY KEY, status TEXT, current_task_id TEXT, lock_version INTEGER);
        CREATE TABLE flow_tasks(
          task_id TEXT PRIMARY KEY, project_id TEXT, account_id TEXT, assigned_account_id TEXT,
          worker_job_id TEXT, status TEXT, generation_attempts INTEGER, lease_version INTEGER,
          output_media_id TEXT, workflow_id TEXT, upstream_batch_id TEXT, operation_name TEXT
        );
        """
    )
    gw.execute("INSERT INTO flow_accounts VALUES(?,?,?,?)", (ACCOUNT_ID, account_status, TASK_ID, 2))
    gw.execute("INSERT INTO flow_tasks VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (TASK_ID, PROJECT_ID, ACCOUNT_ID, ACCOUNT_ID, JOB_ID, task_status, 2, 2, output_media_id, workflow_id, batch_id, None))
    gw.commit()
    gw.close()
    ag = sqlite3.connect(agent_db)
    ag.executescript(
        """
        CREATE TABLE omni_test_jobs(
          job_id TEXT PRIMARY KEY, idempotency_key TEXT, project_id TEXT, input_media_id TEXT,
          output_media_id TEXT, workflow_id TEXT, upstream_batch_id TEXT, operation_name TEXT,
          status TEXT, raw_response_shape TEXT
        );
        CREATE TABLE request(id TEXT);
        """
    )
    ag.execute("INSERT INTO omni_test_jobs VALUES(?,?,?,?,?,?,?,?,?,?)", (JOB_ID, "reconciled", PROJECT_ID, "input-1", output_media_id, workflow_id, batch_id, None, job_status, "{}"))
    ag.commit()
    ag.close()


def _args(gateway_db, agent_db, *, execute=False, worker_base_url=None, result_file=None, **overrides):
    values = {
        "task_id": TASK_ID,
        "gateway_db": str(gateway_db),
        "agent_db": str(agent_db),
        "worker_base_url": worker_base_url,
        "execute": execute,
        "confirm_task_id": TASK_ID if execute else None,
        "confirm_project_id": PROJECT_ID if execute else None,
        "confirm_account_id": ACCOUNT_ID if execute else None,
        "confirm_job_id": JOB_ID if execute else None,
        "confirm_output_media_id": OUTPUT_MEDIA_ID if execute else None,
        "confirm_generation_attempt": 2 if execute else None,
        "confirm_lock_version": 2 if execute else None,
        "confirm_lease_version": 2 if execute else None,
        "allow_real_remote_query": execute,
        "result_file": str(result_file) if result_file else None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _setup(tmp_path, **seed_kwargs):
    gateway_db = tmp_path / "gateway.db"
    agent_db = tmp_path / "agent.db"
    _seed(gateway_db, agent_db, **seed_kwargs)
    return gateway_db, agent_db


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_dry_run_is_read_only_and_does_not_call_worker(tmp_path):
    gateway_db, agent_db = _setup(tmp_path)
    before = (_hash(gateway_db), _hash(agent_db))
    result = poll_reconciled_remote_result(_args(gateway_db, agent_db))
    after = (_hash(gateway_db), _hash(agent_db))
    assert result["ok"] is True
    assert result["mode"] == "dry_run"
    assert result["remote_query_call_count"] == 0
    assert result["writes_performed"] is False
    assert result["planned_database_changes"] == []
    assert result["submit_called"] is False
    assert result["poll_called"] is False
    assert result["download_called"] is False
    assert result["allowed_to_execute"] is True
    assert before == after


@pytest.mark.parametrize(
    "seed_kwargs, reason",
    [
        ({"task_status": "submission_unknown"}, "gateway_status_not_reconciled"),
        ({"account_status": "ready"}, "account_not_busy"),
        ({"job_status": "completed"}, "agent_job_status_not_reconciled_bound"),
    ],
)
def test_dry_run_rejects_invalid_state(tmp_path, seed_kwargs, reason):
    gateway_db, agent_db = _setup(tmp_path, **seed_kwargs)
    result = poll_reconciled_remote_result(_args(gateway_db, agent_db))
    assert result["allowed_to_execute"] is False
    assert reason in result["blocking_reasons"]


@pytest.mark.parametrize(
    "field, value, reason",
    [
        ("output_media_id", "other", "output_media_id_mismatch"),
        ("workflow_id", "other", "workflow_id_mismatch"),
        ("upstream_batch_id", "other", "upstream_batch_id_mismatch"),
    ],
)
def test_dry_run_rejects_gateway_agent_id_mismatch(tmp_path, field, value, reason):
    gateway_db, agent_db = _setup(tmp_path)
    conn = sqlite3.connect(agent_db)
    conn.execute(f"UPDATE omni_test_jobs SET {field}=? WHERE job_id=?", (value, JOB_ID))
    conn.commit()
    conn.close()
    result = poll_reconciled_remote_result(_args(gateway_db, agent_db))
    assert result["allowed_to_execute"] is False
    assert reason in result["blocking_reasons"]


def test_execute_requires_all_confirms_and_remote_query_approval(tmp_path):
    gateway_db, agent_db = _setup(tmp_path)
    result = poll_reconciled_remote_result(_args(gateway_db, agent_db, execute=True, worker_base_url="http://worker", confirm_output_media_id=None, allow_real_remote_query=False))
    assert result["ok"] is False
    assert "confirm_output_media_id_mismatch_or_missing" in result["blocking_reasons"]
    assert "allow_real_remote_query_required" in result["blocking_reasons"]


class FakeWorkerClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []
        self.submit_called = False
        self.download_called = False

    async def query_omni_video_remote_status_once(self, worker, job_id, project_id, output_media_id):
        self.calls.append((worker.api_url, job_id, project_id, output_media_id))
        return dict(self.payload, remote_query_call_count=1, submit_called=False, download_called=False, database_writes_performed=False)


@pytest.mark.asyncio
async def test_execute_calls_worker_exactly_once_and_does_not_write_db(tmp_path):
    gateway_db, agent_db = _setup(tmp_path)
    before = (_hash(gateway_db), _hash(agent_db))
    fake = FakeWorkerClient({"query_ok": True, "remote_state": "remote_processing"})
    result = await _poll_reconciled_remote_result(_args(gateway_db, agent_db, execute=True, worker_base_url="http://worker"), worker_client=fake)
    assert result["ok"] is True
    assert result["remote_query_call_count"] == 1
    assert fake.calls == [("http://worker", JOB_ID, PROJECT_ID, OUTPUT_MEDIA_ID)]
    assert result["submit_called"] is False
    assert result["download_called"] is False
    assert result["writes_performed"] is False
    assert before == (_hash(gateway_db), _hash(agent_db))


def test_completed_response_is_standardized_without_download_call():
    result = normalize_remote_status_response(PROJECT_ID, OUTPUT_MEDIA_ID, {"media": [{"name": OUTPUT_MEDIA_ID, "mediaStatus": {"status": "MEDIA_GENERATION_STATUS_COMPLETED"}, "video": {"encodedVideo": "AAA"}}]})
    assert result["remote_state"] == "remote_completed"
    assert result["download_ready"] is True
    assert result["download_called"] is False


def test_processing_failed_not_found_and_unknown_standardization():
    assert normalize_remote_status_response(PROJECT_ID, OUTPUT_MEDIA_ID, {"media": [{"name": OUTPUT_MEDIA_ID, "mediaStatus": {"status": "MEDIA_GENERATION_STATUS_PROCESSING"}}]})["remote_state"] == "remote_processing"
    assert normalize_remote_status_response(PROJECT_ID, OUTPUT_MEDIA_ID, {"media": [{"name": OUTPUT_MEDIA_ID, "mediaStatus": {"status": "MEDIA_GENERATION_STATUS_FAILED"}}]})["remote_state"] == "remote_failed"
    assert normalize_remote_status_response(PROJECT_ID, OUTPUT_MEDIA_ID, {"error": {"code": 404, "message": "not found"}})["remote_state"] == "remote_not_found"
    assert normalize_remote_status_response(PROJECT_ID, OUTPUT_MEDIA_ID, {"media": [{"name": OUTPUT_MEDIA_ID}]})["remote_state"] == "remote_unknown"


@pytest.mark.asyncio
async def test_query_error_permission_and_recaptcha_are_classified():
    class ErrorOmni:
        def __init__(self, message):
            self.message = message
            self.calls = 0

        async def check_status(self, project_id, output_media_id):
            self.calls += 1
            return {"error": self.message}

    permission = ErrorOmni("HTTP 403 forbidden")
    result = await query_bound_remote_status_once(project_id=PROJECT_ID, output_media_id=OUTPUT_MEDIA_ID, omni=permission)
    assert result["remote_query_call_count"] == 1
    assert result["error_code"] == "permission_denied"
    recaptcha = ErrorOmni("recaptcha required")
    result = await query_bound_remote_status_once(project_id=PROJECT_ID, output_media_id=OUTPUT_MEDIA_ID, omni=recaptcha)
    assert result["error_code"] == "recaptcha_required"


def test_result_file_sanitizes_urls_and_secret_fields(tmp_path):
    gateway_db, agent_db = _setup(tmp_path)
    result_file = tmp_path / "poll-result.json"
    fake = FakeWorkerClient({
        "query_ok": True,
        "remote_state": "remote_completed",
        "download_url": "https://example.test/video.mp4?sig=secret&Expires=1",
        "Authorization": "secret",
    })
    result = asyncio.run(_poll_reconciled_remote_result(_args(gateway_db, agent_db, execute=True, worker_base_url="http://worker", result_file=result_file), worker_client=fake))
    assert result["ok"] is True
    saved = json.loads(result_file.read_text(encoding="utf-8"))
    assert saved["remote_query"]["Authorization"] == "redacted"
    assert saved["remote_query"]["download_url"]["query_parameter_names"] == ["Expires", "sig"]
    assert "secret" not in result_file.read_text(encoding="utf-8")


def test_worker_route_missing_blocks_when_no_worker_url(tmp_path):
    gateway_db, agent_db = _setup(tmp_path)
    result = poll_reconciled_remote_result(_args(gateway_db, agent_db, execute=True, worker_base_url=None))
    assert result["ok"] is False
    assert "worker_base_url_required" in result["blocking_reasons"]
