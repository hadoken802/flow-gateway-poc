import argparse
import hashlib
import json
import sqlite3

from gateway.reconciled_poll_result_apply import (
    AGENT_TO_STATUS,
    GATEWAY_TO_STATUS,
    apply_reconciled_poll_result,
)


TASK_ID = "3d98ece6-4ace-4dc6-817c-979d23881bc3"
ACCOUNT_ID = "FLOW-002"
PROJECT_ID = "c23337ee-3be2-4e13-a6b0-d74c87675394"
JOB_ID = "c443c4e8-fff7-59a7-a8b9-9a8f2c8d09eb"
OUTPUT_MEDIA_ID = "ed7cef70-ae56-4f7a-9c7f-2da5aa66d44a"
WORKFLOW_ID = "475f2092-1f91-46e1-ba2b-968e3dfa3aec"
BATCH_ID = "f2b691af-823b-4002-9d4d-d67a22f6a281"


def _seed(gateway_db, agent_db, *, task_status="reconciled_remote_accepted_unknown", agent_status="remote_reconciled_bound", account_status="busy", video_path=None, completed_at=None):
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
    ag.execute("INSERT INTO omni_test_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (JOB_ID, "reconciled", PROJECT_ID, "input-1", OUTPUT_MEDIA_ID, WORKFLOW_ID, BATCH_ID, None, agent_status, json.dumps({"reconciled_remote_binding": {"kept": True}}), video_path, completed_at, "t0"))
    ag.commit()
    ag.close()


def _poll_result(**overrides):
    value = {
        "ok": True,
        "mode": "execute",
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
            "status_evidence": [{"path": "media.0.status", "completed": True}],
            "completed": True,
            "download_ready": False,
            "encoded_video_present": False,
            "download_url_present": False,
            "error_code": None,
            "error_message": None,
            "remote_query_call_count": 1,
            "database_writes_performed": False,
            "query_started_at": "2026-07-29T12:38:56Z",
            "query_finished_at": "2026-07-29T12:38:57Z",
        },
    }
    value.update(overrides)
    return value


def _write_poll(tmp_path, **overrides):
    path = tmp_path / "poll-result.json"
    path.write_text(json.dumps(_poll_result(**overrides), sort_keys=True), encoding="utf-8")
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _args(gateway_db, agent_db, poll_file, *, execute=False, sha=None, **overrides):
    values = {
        "task_id": TASK_ID,
        "poll_result_file": str(poll_file),
        "gateway_db": str(gateway_db),
        "agent_db": str(agent_db),
        "execute": execute,
        "confirm_task_id": TASK_ID if execute else None,
        "confirm_project_id": PROJECT_ID if execute else None,
        "confirm_account_id": ACCOUNT_ID if execute else None,
        "confirm_job_id": JOB_ID if execute else None,
        "confirm_output_media_id": OUTPUT_MEDIA_ID if execute else None,
        "confirm_generation_attempt": 2 if execute else None,
        "confirm_lock_version": 2 if execute else None,
        "confirm_lease_version": 2 if execute else None,
        "confirm_poll_result_sha256": sha if execute else None,
        "allow_real_database": False,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _setup(tmp_path, **seed):
    gateway_db = tmp_path / "gateway.db"
    agent_db = tmp_path / "agent.db"
    _seed(gateway_db, agent_db, **seed)
    poll_file, sha = _write_poll(tmp_path)
    return gateway_db, agent_db, poll_file, sha


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_dry_run_is_strictly_read_only(tmp_path):
    gateway_db, agent_db, poll_file, sha = _setup(tmp_path)
    before = (_hash(gateway_db), _hash(agent_db))
    result = apply_reconciled_poll_result(_args(gateway_db, agent_db, poll_file))
    assert result["ok"] is True
    assert result["mode"] == "dry_run"
    assert result["poll_result_sha256"] == sha
    assert result["planned_gateway_changes"]["status"] == GATEWAY_TO_STATUS
    assert result["planned_agent_changes"]["status"] == AGENT_TO_STATUS
    assert result["planned_network_calls"] == []
    assert result["writes_performed"] is False
    assert result["submit_called"] is False
    assert result["poll_called"] is False
    assert result["download_called"] is False
    assert before == (_hash(gateway_db), _hash(agent_db))


def test_rejects_invalid_poll_result(tmp_path):
    gateway_db, agent_db, poll_file, _sha = _setup(tmp_path)
    poll_file.write_text(json.dumps(_poll_result(remote_query_call_count=2)), encoding="utf-8")
    result = apply_reconciled_poll_result(_args(gateway_db, agent_db, poll_file))
    assert result["allowed_to_execute"] is False
    assert "poll_result_remote_query_call_count_not_one" in result["blocking_reasons"]


def test_execute_requires_confirm_sha(tmp_path):
    gateway_db, agent_db, poll_file, _sha = _setup(tmp_path)
    result = apply_reconciled_poll_result(_args(gateway_db, agent_db, poll_file, execute=True, sha="bad"))
    assert result["ok"] is False
    assert "confirm_poll_result_sha256_mismatch_or_missing" in result["blocking_reasons"]


def test_execute_updates_isolated_statuses_and_preserves_lock_and_download_fields(tmp_path):
    gateway_db, agent_db, poll_file, sha = _setup(tmp_path)
    result = apply_reconciled_poll_result(_args(gateway_db, agent_db, poll_file, execute=True, sha=sha))
    assert result["ok"] is True
    assert result["result"] == "applied"
    gw = sqlite3.connect(gateway_db)
    task = gw.execute("SELECT status,generation_attempts,lease_version,worker_job_id,output_media_id FROM flow_tasks").fetchone()
    account = gw.execute("SELECT status,current_task_id,lock_version FROM flow_accounts").fetchone()
    ag = sqlite3.connect(agent_db)
    job = ag.execute("SELECT status,output_media_id,workflow_id,upstream_batch_id,video_path,completed_at,raw_response_shape FROM omni_test_jobs WHERE job_id=?", (JOB_ID,)).fetchone()
    assert task == (GATEWAY_TO_STATUS, 2, 2, JOB_ID, OUTPUT_MEDIA_ID)
    assert account == ("busy", TASK_ID, 2)
    assert job[:6] == (AGENT_TO_STATUS, OUTPUT_MEDIA_ID, WORKFLOW_ID, BATCH_ID, None, None)
    raw = json.loads(job[6])
    assert raw["reconciled_remote_binding"]["kept"] is True
    assert raw["reconciled_poll_result"]["poll_result_sha256"] == sha


def test_repeated_execute_is_already_applied_and_idempotent(tmp_path):
    gateway_db, agent_db, poll_file, sha = _setup(tmp_path)
    first = apply_reconciled_poll_result(_args(gateway_db, agent_db, poll_file, execute=True, sha=sha))
    before = (_hash(gateway_db), _hash(agent_db))
    second = apply_reconciled_poll_result(_args(gateway_db, agent_db, poll_file, execute=True, sha=sha))
    assert first["ok"] is True
    assert second["ok"] is True
    assert second["result"] == "already_applied"
    assert before == (_hash(gateway_db), _hash(agent_db))


def test_different_poll_sha_after_apply_is_rejected(tmp_path):
    gateway_db, agent_db, poll_file, sha = _setup(tmp_path)
    apply_reconciled_poll_result(_args(gateway_db, agent_db, poll_file, execute=True, sha=sha))
    other = tmp_path / "other.json"
    other.write_text(json.dumps(_poll_result(remote_query={"query_ok": True, "project_id": PROJECT_ID, "output_media_id": OUTPUT_MEDIA_ID, "remote_state": "remote_completed", "raw_status": "MEDIA_GENERATION_STATUS_SUCCESSFUL", "status_evidence": [{"path": "x"}], "completed": True, "download_ready": False, "encoded_video_present": False, "download_url_present": False, "error_code": None, "error_message": None, "database_writes_performed": False, "remote_query_call_count": 1, "query_started_at": "different"}), sort_keys=True), encoding="utf-8")
    other_sha = hashlib.sha256(other.read_bytes()).hexdigest()
    result = apply_reconciled_poll_result(_args(gateway_db, agent_db, other, execute=True, sha=other_sha))
    assert result["ok"] is False
    assert result["error_code"] == "poll_result_conflict"


def test_phase1_after_gateway_failure_can_retry(tmp_path):
    gateway_db, agent_db, poll_file, sha = _setup(tmp_path)
    conn = sqlite3.connect(gateway_db)
    conn.execute("UPDATE flow_tasks SET lease_version=3")
    conn.commit()
    conn.close()
    failed = apply_reconciled_poll_result(_args(gateway_db, agent_db, poll_file, execute=True, sha=sha))
    assert failed["ok"] is False
    conn = sqlite3.connect(agent_db)
    assert conn.execute("SELECT status FROM omni_test_jobs WHERE job_id=?", (JOB_ID,)).fetchone()[0] == AGENT_TO_STATUS
    conn.close()
    conn = sqlite3.connect(gateway_db)
    conn.execute("UPDATE flow_tasks SET lease_version=2")
    conn.commit()
    conn.close()
    retry = apply_reconciled_poll_result(_args(gateway_db, agent_db, poll_file, execute=True, sha=sha))
    assert retry["ok"] is True


def test_rejects_video_path_or_completed_at(tmp_path):
    gateway_db, agent_db, poll_file, _sha = _setup(tmp_path, video_path="x.mp4")
    result = apply_reconciled_poll_result(_args(gateway_db, agent_db, poll_file))
    assert "agent_already_has_local_completion" in result["blocking_reasons"]


def test_new_status_is_not_recovered_by_agent_startup():
    from agent.api.omni_test import ACTIVE_STATUSES, DOWNLOAD_RECOVERY_STATUSES

    assert AGENT_TO_STATUS not in ACTIVE_STATUSES
    assert AGENT_TO_STATUS not in DOWNLOAD_RECOVERY_STATUSES
    assert AGENT_TO_STATUS not in {"completed", "waiting_download", "completed_remote"}


def test_apply_module_does_not_import_network_clients():
    from pathlib import Path

    source = Path("gateway/reconciled_poll_result_apply.py").read_text(encoding="utf-8")
    forbidden = ("WorkerClient", "OmniClient", "FlowClient", "requests", "httpx", "aiohttp", "check_status", "get_media", "submit(")
    for token in forbidden:
        assert token not in source
