import argparse
import hashlib
import json
import sqlite3

import gateway.reconciled_remote_download_preflight as preflight
from gateway.reconciled_remote_download_preflight import (
    AGENT_REQUIRED_STATUS,
    GATEWAY_REQUIRED_STATUS,
    prepare_reconciled_remote_download,
)


TASK_ID = "3d98ece6-4ace-4dc6-817c-979d23881bc3"
ACCOUNT_ID = "FLOW-002"
PROJECT_ID = "c23337ee-3be2-4e13-a6b0-d74c87675394"
JOB_ID = "c443c4e8-fff7-59a7-a8b9-9a8f2c8d09eb"
OUTPUT_MEDIA_ID = "ed7cef70-ae56-4f7a-9c7f-2da5aa66d44a"
WORKFLOW_ID = "475f2092-1f91-46e1-ba2b-968e3dfa3aec"
BATCH_ID = "f2b691af-823b-4002-9d4d-d67a22f6a281"
POLL_SHA = "3278e5cd9d465797b63d7526b7a6a76fc866def660a01780df1fe4be16e4fe94"


def _seed(gateway_db, agent_db, *, task_status=GATEWAY_REQUIRED_STATUS, agent_status=AGENT_REQUIRED_STATUS, account_status="busy", video_path=None, completed_at=None):
    gw = sqlite3.connect(gateway_db)
    gw.executescript(
        """
        CREATE TABLE flow_accounts(account_id TEXT PRIMARY KEY, status TEXT, current_task_id TEXT, lock_version INTEGER);
        CREATE TABLE flow_tasks(
          task_id TEXT PRIMARY KEY, status TEXT, project_id TEXT, account_id TEXT,
          assigned_account_id TEXT, worker_job_id TEXT, generation_attempts INTEGER,
          lease_version INTEGER, output_media_id TEXT, workflow_id TEXT,
          upstream_batch_id TEXT, updated_at TEXT
        );
        """
    )
    gw.execute("INSERT INTO flow_accounts VALUES(?,?,?,?)", (ACCOUNT_ID, account_status, TASK_ID, 2))
    gw.execute("INSERT INTO flow_tasks VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (TASK_ID, task_status, PROJECT_ID, ACCOUNT_ID, ACCOUNT_ID, JOB_ID, 2, 2, OUTPUT_MEDIA_ID, WORKFLOW_ID, BATCH_ID, "t0"))
    gw.commit()
    gw.close()
    ag = sqlite3.connect(agent_db)
    ag.executescript(
        """
        CREATE TABLE omni_test_jobs(
          job_id TEXT PRIMARY KEY, idempotency_key TEXT, project_id TEXT, input_media_id TEXT,
          output_media_id TEXT, workflow_id TEXT, upstream_batch_id TEXT, operation_name TEXT,
          status TEXT, raw_response_shape TEXT, video_path TEXT, completed_at TEXT, updated_at TEXT
        );
        CREATE TABLE request(id TEXT);
        """
    )
    raw = {"reconciled_remote_binding": {"kept": True}, "reconciled_poll_result": {"poll_result_sha256": POLL_SHA, "remote_state": "remote_completed", "raw_status": "MEDIA_GENERATION_STATUS_SUCCESSFUL"}}
    ag.execute("INSERT INTO omni_test_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (JOB_ID, "reconciled", PROJECT_ID, "input-1", OUTPUT_MEDIA_ID, WORKFLOW_ID, BATCH_ID, None, agent_status, json.dumps(raw), video_path, completed_at, "t0"))
    ag.commit()
    ag.close()


def _poll_result(**overrides):
    value = {
        "task_id": TASK_ID,
        "account_id": ACCOUNT_ID,
        "job_id": JOB_ID,
        "project_id": PROJECT_ID,
        "output_media_id": OUTPUT_MEDIA_ID,
        "remote_state": "remote_completed",
        "remote_query_call_count": 1,
        "submit_called": False,
        "poll_called": True,
        "download_called": False,
        "writes_performed": False,
        "remote_query": {
            "query_ok": True,
            "project_id": PROJECT_ID,
            "output_media_id": OUTPUT_MEDIA_ID,
            "remote_state": "remote_completed",
            "raw_status": "MEDIA_GENERATION_STATUS_SUCCESSFUL",
            "completed": True,
            "download_ready": False,
            "encoded_video_present": False,
            "download_url_present": False,
            "remote_query_call_count": 1,
            "database_writes_performed": False,
        },
    }
    value.update(overrides)
    return value


def _write_poll(path):
    path.write_text(json.dumps(_poll_result(), sort_keys=True), encoding="utf-8")


def _args(gateway_db, agent_db, poll_file, output_dir):
    return argparse.Namespace(
        task_id=TASK_ID,
        poll_result_file=str(poll_file),
        gateway_db=str(gateway_db),
        agent_db=str(agent_db),
        worker_base_url="http://fake-worker",
        output_dir=str(output_dir),
    )


def _setup(tmp_path, monkeypatch):
    gateway_db = tmp_path / "gateway.db"
    agent_db = tmp_path / "agent.db"
    poll_file = tmp_path / "poll-result.json"
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    _seed(gateway_db, agent_db)
    _write_poll(poll_file)
    poll_sha = hashlib.sha256(poll_file.read_bytes()).hexdigest()
    monkeypatch.setattr(preflight, "EXPECTED_POLL_SHA", poll_sha)
    ag = sqlite3.connect(agent_db)
    raw = json.loads(ag.execute("SELECT raw_response_shape FROM omni_test_jobs WHERE job_id=?", (JOB_ID,)).fetchone()[0])
    raw["reconciled_poll_result"]["poll_result_sha256"] = poll_sha
    ag.execute("UPDATE omni_test_jobs SET raw_response_shape=? WHERE job_id=?", (json.dumps(raw), JOB_ID))
    ag.commit()
    ag.close()
    return gateway_db, agent_db, poll_file, output_dir


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_prepare_is_read_only_and_never_calls_remote(monkeypatch, tmp_path):
    gateway_db, agent_db, poll_file, output_dir = _setup(tmp_path, monkeypatch)
    before = (_hash(gateway_db), _hash(agent_db))
    monkeypatch.setattr("gateway.reconciled_remote_download_preflight._inspect_worker", lambda _url: {"health": {"status": "ok", "extension_connected": True}, "extension_connected": True, "download_route_available": True, "metadata_query_route_available": True, "blocking_reasons": []})
    result = prepare_reconciled_remote_download(_args(gateway_db, agent_db, poll_file, output_dir))
    assert result["blocking_reasons"] == []
    assert result["ok"] is True
    assert result["download_preflight_allowed"] is True
    assert result["download_capability_classification"] == "direct_download_is_only_remote_probe"
    assert result["get_media_called"] is False
    assert result["poll_called"] is False
    assert result["submit_called"] is False
    assert result["download_called"] is False
    assert result["network_calls_performed"] == 0
    assert result["writes_performed"] is False
    assert before == (_hash(gateway_db), _hash(agent_db))


def test_rejects_gateway_state_conflict(monkeypatch, tmp_path):
    gateway_db, agent_db, poll_file, output_dir = _setup(tmp_path, monkeypatch)
    conn = sqlite3.connect(gateway_db)
    conn.execute("UPDATE flow_tasks SET status='completed'")
    conn.commit()
    conn.close()
    monkeypatch.setattr("gateway.reconciled_remote_download_preflight._inspect_worker", lambda _url: {"health": {"status": "ok", "extension_connected": True}, "extension_connected": True, "download_route_available": True, "metadata_query_route_available": True, "blocking_reasons": []})
    result = prepare_reconciled_remote_download(_args(gateway_db, agent_db, poll_file, output_dir))
    assert "gateway_state_conflict" in result["blocking_reasons"]


def test_rejects_agent_completed_fields(monkeypatch, tmp_path):
    gateway_db, agent_db, poll_file, output_dir = _setup(tmp_path, monkeypatch)
    conn = sqlite3.connect(agent_db)
    conn.execute("UPDATE omni_test_jobs SET video_path='x.mp4'")
    conn.commit()
    conn.close()
    monkeypatch.setattr("gateway.reconciled_remote_download_preflight._inspect_worker", lambda _url: {"health": {"status": "ok", "extension_connected": True}, "extension_connected": True, "download_route_available": True, "metadata_query_route_available": True, "blocking_reasons": []})
    result = prepare_reconciled_remote_download(_args(gateway_db, agent_db, poll_file, output_dir))
    assert "agent_video_path_exists" in result["blocking_reasons"]


def test_rejects_poll_sha_conflict(monkeypatch, tmp_path):
    gateway_db, agent_db, poll_file, output_dir = _setup(tmp_path, monkeypatch)
    poll_file.write_text(json.dumps(_poll_result(remote_query_call_count=2)), encoding="utf-8")
    monkeypatch.setattr("gateway.reconciled_remote_download_preflight._inspect_worker", lambda _url: {"health": {"status": "ok", "extension_connected": True}, "extension_connected": True, "download_route_available": True, "metadata_query_route_available": True, "blocking_reasons": []})
    result = prepare_reconciled_remote_download(_args(gateway_db, agent_db, poll_file, output_dir))
    assert "poll_result_sha_mismatch" in result["blocking_reasons"]


def test_worker_offline_blocks(monkeypatch, tmp_path):
    gateway_db, agent_db, poll_file, output_dir = _setup(tmp_path, monkeypatch)
    result = prepare_reconciled_remote_download(_args(gateway_db, agent_db, poll_file, output_dir))
    assert "worker_health_unavailable" in result["blocking_reasons"]


def test_existing_file_blocks(monkeypatch, tmp_path):
    gateway_db, agent_db, poll_file, output_dir = _setup(tmp_path, monkeypatch)
    (output_dir / "FLOW-002_3d98ece6_ed7cef70.mp4").write_bytes(b"not real mp4")
    monkeypatch.setattr("gateway.reconciled_remote_download_preflight._inspect_worker", lambda _url: {"health": {"status": "ok", "extension_connected": True}, "extension_connected": True, "download_route_available": True, "metadata_query_route_available": True, "blocking_reasons": []})
    result = prepare_reconciled_remote_download(_args(gateway_db, agent_db, poll_file, output_dir))
    assert "planned_output_file_already_exists" in result["blocking_reasons"]


def test_new_status_is_not_recovered_by_agent_startup():
    from agent.api.omni_test import ACTIVE_STATUSES, DOWNLOAD_RECOVERY_STATUSES

    assert AGENT_REQUIRED_STATUS not in ACTIVE_STATUSES
    assert AGENT_REQUIRED_STATUS not in DOWNLOAD_RECOVERY_STATUSES
    assert AGENT_REQUIRED_STATUS not in {"completed", "waiting_download", "completed_remote"}
