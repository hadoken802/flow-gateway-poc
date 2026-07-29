import argparse
import hashlib
import json
import sqlite3

from gateway.remote_result_binding import (
    AGENT_BOUND_STATUS,
    BIND_STATUS,
    bind_reconciled_remote_result,
    deterministic_job_id,
)


TASK_ID = "3d98ece6-4ace-4dc6-817c-979d23881bc3"
ACCOUNT_ID = "FLOW-002"
PROJECT_ID = "c23337ee-3be2-4e13-a6b0-d74c87675394"
INPUT_MEDIA_ID = "d96ce421-d688-46dd-b71e-246e42341758"
SOURCE_JOB_ID = "9a513983-20e1-4933-9cf0-f4effc432694"
WORKFLOW_ID = "475f2092-1f91-46e1-ba2b-968e3dfa3aec"
OUTPUT_MEDIA_ID = "ed7cef70-ae56-4f7a-9c7f-2da5aa66d44a"
BATCH_ID = "f2b691af-823b-4002-9d4d-d67a22f6a281"


def _candidate(**overrides):
    value = {
        "task_id": TASK_ID,
        "account_id": ACCOUNT_ID,
        "project_id": PROJECT_ID,
        "generation_attempt": 2,
        "match_class": "exact_match",
        "match_confidence": "high",
        "remote_state": "remote_accepted_status_unknown",
        "safe_to_bind": True,
        "safe_to_poll": True,
        "safe_to_download": False,
        "workflow_id": WORKFLOW_ID,
        "output_media_id": OUTPUT_MEDIA_ID,
        "upstream_batch_id": BATCH_ID,
        "operation_name": None,
        "primary_media_id": OUTPUT_MEDIA_ID,
        "media_record_id": OUTPUT_MEDIA_ID,
        "poll_identifier": OUTPUT_MEDIA_ID,
        "download_identifier": None,
        "input_media_id": INPUT_MEDIA_ID,
        "conflicts": [],
        "database_safety_evidence": {"all_unchanged": True},
        "flow001_mapping_evidence": {
            "canonical_workflow_id": {"verified": True},
            "canonical_output_media_id": {"verified": True},
            "canonical_upstream_batch_id": {"verified": True},
        },
        "source_capture_dir": "diagnostics/capture",
        "source_procedure": "flow.projectInitialData",
        "source_request_id": "11088.2312",
    }
    value.update(overrides)
    return value


def _write_candidate(tmp_path, **overrides):
    path = tmp_path / "candidate.json"
    path.write_text(json.dumps(_candidate(**overrides), sort_keys=True), encoding="utf-8")
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _args(gateway_db, agent_db, candidate_file, execute=False, sha=None, **overrides):
    values = {
        "task_id": TASK_ID,
        "candidate_file": str(candidate_file),
        "gateway_db": str(gateway_db),
        "agent_db": str(agent_db),
        "execute": execute,
        "confirm_task_id": TASK_ID if execute else None,
        "confirm_project_id": PROJECT_ID if execute else None,
        "confirm_account_id": ACCOUNT_ID if execute else None,
        "confirm_generation_attempt": 2 if execute else None,
        "confirm_output_media_id": OUTPUT_MEDIA_ID if execute else None,
        "confirm_candidate_sha256": sha if execute else None,
        "allow_real_database": False,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _seed_gateway(path, *, status="submission_unknown", lease_version=2, lock_version=2, current_task_id=TASK_ID):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE flow_accounts(
          account_id TEXT PRIMARY KEY, status TEXT, current_task_id TEXT, lock_version INTEGER
        );
        CREATE TABLE flow_tasks(
          task_id TEXT PRIMARY KEY, idempotency_key TEXT, project_id TEXT, image_path TEXT,
          prompt TEXT, duration INTEGER, aspect_ratio TEXT, status TEXT,
          assigned_account_id TEXT, account_id TEXT, lease_version INTEGER, worker_job_id TEXT,
          generation_attempts INTEGER, output_media_id TEXT, workflow_id TEXT, operation_name TEXT,
          upstream_batch_id TEXT, resume_attempt_id TEXT, request_batch_id TEXT,
          remote_submission_state TEXT, remote_result_query_state TEXT, updated_at TEXT
        );
        """
    )
    conn.execute("INSERT INTO flow_accounts VALUES(?,?,?,?)", (ACCOUNT_ID, "busy", current_task_id, lock_version))
    conn.execute(
        """
        INSERT INTO flow_tasks(task_id,idempotency_key,project_id,image_path,prompt,duration,aspect_ratio,status,
          assigned_account_id,account_id,lease_version,worker_job_id,generation_attempts)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (TASK_ID, "idem", PROJECT_ID, "D:/input.png", "prompt", 10, "9:16", status, ACCOUNT_ID, ACCOUNT_ID, lease_version, SOURCE_JOB_ID, 2),
    )
    conn.commit()
    conn.close()


def _seed_agent(path, *, old_schema=False):
    conn = sqlite3.connect(path)
    if old_schema:
        conn.executescript(
            """
            CREATE TABLE omni_test_jobs(
              job_id TEXT PRIMARY KEY, idempotency_key TEXT, project_id TEXT, prompt TEXT,
              image_path TEXT, input_media_id TEXT, output_media_id TEXT, workflow_id TEXT,
              operation_name TEXT, upstream_batch_id TEXT, status TEXT, created_at TEXT, updated_at TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO omni_test_jobs(job_id,idempotency_key,project_id,prompt,image_path,input_media_id,status) VALUES(?,?,?,?,?,?,?)",
            (SOURCE_JOB_ID, "old", PROJECT_ID, "prompt", "D:/input.png", INPUT_MEDIA_ID, "failed"),
        )
    else:
        conn.executescript(
            """
            CREATE TABLE omni_test_jobs(
              job_id TEXT PRIMARY KEY, idempotency_key TEXT, project_id TEXT, prompt TEXT,
              image_path TEXT, input_media_id TEXT, output_media_id TEXT, workflow_id TEXT,
              operation_name TEXT, upstream_batch_id TEXT, status TEXT, gateway_task_id TEXT,
              generation_attempt INTEGER, source_worker_job_id TEXT, resume_attempt_id TEXT,
              request_batch_id TEXT, remote_submission_state TEXT, raw_response_shape TEXT,
              created_at TEXT, updated_at TEXT
            );
            """
        )
        conn.execute(
            """
            INSERT INTO omni_test_jobs(job_id,idempotency_key,project_id,prompt,image_path,input_media_id,status,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (SOURCE_JOB_ID, "old", PROJECT_ID, "prompt", "D:/input.png", INPUT_MEDIA_ID, "failed", "now", "now"),
        )
    conn.commit()
    conn.close()


def _setup(tmp_path, **candidate_overrides):
    gateway_db = tmp_path / "gateway.db"
    agent_db = tmp_path / "agent.db"
    _seed_gateway(gateway_db)
    _seed_agent(agent_db)
    candidate_path, sha = _write_candidate(tmp_path, **candidate_overrides)
    return gateway_db, agent_db, candidate_path, sha


def _file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_dry_run_is_read_only_and_plans_bind(tmp_path):
    gateway_db, agent_db, candidate_path, sha = _setup(tmp_path)
    before = (_file_hash(gateway_db), _file_hash(agent_db))
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path))
    after = (_file_hash(gateway_db), _file_hash(agent_db))
    assert result["ok"] is True
    assert result["mode"] == "dry_run"
    assert result["writes_performed"] is False
    assert result["planned_gateway_changes"]["status"] == BIND_STATUS
    assert result["planned_agent_changes"]["status"] == AGENT_BOUND_STATUS
    assert result["poll_readiness"] is True
    assert result["download_readiness"] is False
    assert result["candidate_sha256"] == sha
    assert before == after


def test_dry_run_does_not_migrate_old_agent_schema(tmp_path):
    gateway_db = tmp_path / "gateway.db"
    agent_db = tmp_path / "agent.db"
    _seed_gateway(gateway_db)
    _seed_agent(agent_db, old_schema=True)
    candidate_path, _sha = _write_candidate(tmp_path)
    before = _file_hash(agent_db)
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path))
    assert result["ok"] is True
    assert result["allowed_to_execute"] is False
    assert "agent_schema_upgrade_required" in result["blocking_reasons"]
    assert before == _file_hash(agent_db)


def test_execute_requires_complete_confirms(tmp_path):
    gateway_db, agent_db, candidate_path, sha = _setup(tmp_path)
    args = _args(gateway_db, agent_db, candidate_path, execute=True, sha=sha, confirm_output_media_id=None)
    result = bind_reconciled_remote_result(args)
    assert result["ok"] is False
    assert "confirm_output_media_id_mismatch_or_missing" in result["blocking_reasons"]


def test_execute_blocks_known_real_database_paths(tmp_path):
    from gateway.remote_result_binding import REAL_FLOW002_AGENT_DB, REAL_GATEWAY_DB

    gateway_db, agent_db, candidate_path, sha = _setup(tmp_path)
    args = _args(gateway_db, agent_db, candidate_path, execute=True, sha=sha)
    args.gateway_db = str(REAL_GATEWAY_DB)
    args.agent_db = str(REAL_FLOW002_AGENT_DB)
    result = bind_reconciled_remote_result(args)
    assert result["ok"] is False
    assert "real_database_execute_blocked" in result["blocking_reasons"]


def test_execute_creates_isolated_agent_job_and_binds_gateway(tmp_path):
    gateway_db, agent_db, candidate_path, sha = _setup(tmp_path)
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path, execute=True, sha=sha))
    assert result["ok"] is True
    assert result["result"] == "bound"
    job_id = deterministic_job_id(TASK_ID, 2)
    gw = sqlite3.connect(gateway_db)
    task = gw.execute("SELECT status, worker_job_id, output_media_id, workflow_id, upstream_batch_id, generation_attempts FROM flow_tasks").fetchone()
    account = gw.execute("SELECT status, current_task_id, lock_version FROM flow_accounts").fetchone()
    agent = sqlite3.connect(agent_db)
    job = agent.execute("SELECT job_id,status,output_media_id,workflow_id,upstream_batch_id,generation_attempt,source_worker_job_id FROM omni_test_jobs WHERE job_id=?", (job_id,)).fetchone()
    assert task == (BIND_STATUS, job_id, OUTPUT_MEDIA_ID, WORKFLOW_ID, BATCH_ID, 2)
    assert account == ("busy", TASK_ID, 2)
    assert job == (job_id, AGENT_BOUND_STATUS, OUTPUT_MEDIA_ID, WORKFLOW_ID, BATCH_ID, 2, SOURCE_JOB_ID)


def test_repeated_execute_is_already_bound_and_idempotent(tmp_path):
    gateway_db, agent_db, candidate_path, sha = _setup(tmp_path)
    first = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path, execute=True, sha=sha))
    before = (_file_hash(gateway_db), _file_hash(agent_db))
    second = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path, execute=True, sha=sha))
    after = (_file_hash(gateway_db), _file_hash(agent_db))
    assert first["ok"] is True
    assert second["ok"] is True
    assert second["result"] == "already_bound"
    assert before == after
    conn = sqlite3.connect(agent_db)
    assert conn.execute("SELECT count(*) FROM omni_test_jobs").fetchone()[0] == 2


def test_phase1_after_gateway_failure_can_retry(tmp_path):
    gateway_db, agent_db, candidate_path, sha = _setup(tmp_path)
    conn = sqlite3.connect(gateway_db)
    conn.execute("UPDATE flow_tasks SET lease_version=3 WHERE task_id=?", (TASK_ID,))
    conn.commit()
    conn.close()
    failed = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path, execute=True, sha=sha))
    assert failed["ok"] is False
    assert failed["error_code"] == "fencing_state_changed"
    conn = sqlite3.connect(agent_db)
    assert conn.execute("SELECT count(*) FROM omni_test_jobs WHERE status=?", (AGENT_BOUND_STATUS,)).fetchone()[0] == 1
    conn.close()
    conn = sqlite3.connect(gateway_db)
    conn.execute("UPDATE flow_tasks SET lease_version=2 WHERE task_id=?", (TASK_ID,))
    conn.commit()
    conn.close()
    retry = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path, execute=True, sha=sha))
    assert retry["ok"] is True


def test_candidate_conflict_rejected(tmp_path):
    gateway_db, agent_db, candidate_path, _sha = _setup(tmp_path, safe_to_bind=False)
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path))
    assert result["allowed_to_execute"] is False
    assert "candidate_safe_to_bind_mismatch" in result["blocking_reasons"]


def test_candidate_sha_confirm_rejected(tmp_path):
    gateway_db, agent_db, candidate_path, _sha = _setup(tmp_path)
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path, execute=True, sha="bad-sha"))
    assert result["ok"] is False
    assert "confirm_candidate_sha256_mismatch_or_missing" in result["blocking_reasons"]


def test_candidate_poll_identifier_must_equal_output_media(tmp_path):
    gateway_db, agent_db, candidate_path, _sha = _setup(tmp_path, poll_identifier="other-media")
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path))
    assert result["allowed_to_execute"] is False
    assert "poll_identifier_mismatch" in result["blocking_reasons"]


def test_candidate_requires_verified_flow001_mapping(tmp_path):
    mapping = {
        "canonical_workflow_id": {"verified": True},
        "canonical_output_media_id": {"verified": False},
        "canonical_upstream_batch_id": {"verified": True},
    }
    gateway_db, agent_db, candidate_path, _sha = _setup(tmp_path, flow001_mapping_evidence=mapping)
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path))
    assert result["allowed_to_execute"] is False
    assert "candidate_mapping_unverified_canonical_output_media_id" in result["blocking_reasons"]


def test_candidate_database_safety_required(tmp_path):
    gateway_db, agent_db, candidate_path, _sha = _setup(tmp_path, database_safety_evidence={"all_unchanged": False})
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path))
    assert result["allowed_to_execute"] is False
    assert "candidate_database_safety_not_all_unchanged" in result["blocking_reasons"]


def test_gateway_status_must_be_submission_unknown_or_already_bound(tmp_path):
    gateway_db = tmp_path / "gateway.db"
    agent_db = tmp_path / "agent.db"
    _seed_gateway(gateway_db, status="submitted")
    _seed_agent(agent_db)
    candidate_path, _sha = _write_candidate(tmp_path)
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path))
    assert result["allowed_to_execute"] is False
    assert "gateway_status_not_bindable" in result["blocking_reasons"]


def test_account_must_remain_busy(tmp_path):
    gateway_db, agent_db, candidate_path, _sha = _setup(tmp_path)
    conn = sqlite3.connect(gateway_db)
    conn.execute("UPDATE flow_accounts SET status='ready' WHERE account_id=?", (ACCOUNT_ID,))
    conn.commit()
    conn.close()
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path))
    assert result["allowed_to_execute"] is False
    assert "account_not_busy" in result["blocking_reasons"]


def test_output_media_conflict_rejected(tmp_path):
    gateway_db, agent_db, candidate_path, _sha = _setup(tmp_path)
    conn = sqlite3.connect(agent_db)
    conn.execute(
        "INSERT INTO omni_test_jobs(job_id,idempotency_key,project_id,prompt,image_path,input_media_id,output_media_id,status) VALUES(?,?,?,?,?,?,?,?)",
        ("other-job", "other", PROJECT_ID, "p", "i", "input-other", OUTPUT_MEDIA_ID, "completed"),
    )
    conn.commit()
    conn.close()
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path))
    assert "output_media_id_bound_to_other_agent_job" in result["blocking_reasons"]


def test_agent_existing_bound_job_conflict_rejected(tmp_path):
    gateway_db, agent_db, candidate_path, sha = _setup(tmp_path)
    job_id = deterministic_job_id(TASK_ID, 2)
    conn = sqlite3.connect(agent_db)
    conn.execute(
        "INSERT INTO omni_test_jobs(job_id,idempotency_key,project_id,prompt,image_path,input_media_id,output_media_id,workflow_id,upstream_batch_id,status,gateway_task_id,generation_attempt) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (job_id, "bad", PROJECT_ID, "p", "i", INPUT_MEDIA_ID, "different", WORKFLOW_ID, BATCH_ID, AGENT_BOUND_STATUS, TASK_ID, 2),
    )
    conn.commit()
    conn.close()
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path, execute=True, sha=sha))
    assert result["ok"] is False
    assert result["error_code"] == "agent_bound_job_conflict"


def test_fencing_rejects_lock_and_current_task_changes(tmp_path):
    gateway_db, agent_db, candidate_path, sha = _setup(tmp_path)
    conn = sqlite3.connect(gateway_db)
    conn.execute("UPDATE flow_accounts SET lock_version=3, current_task_id='other' WHERE account_id=?", (ACCOUNT_ID,))
    conn.commit()
    conn.close()
    result = bind_reconciled_remote_result(_args(gateway_db, agent_db, candidate_path, execute=True, sha=sha))
    assert result["ok"] is False
    assert "account_current_task_mismatch" in result["blocking_reasons"]


def test_bound_status_is_not_agent_auto_recovery_status():
    from agent.api.omni_test import ACTIVE_STATUSES, DOWNLOAD_RECOVERY_STATUSES

    assert AGENT_BOUND_STATUS not in ACTIVE_STATUSES
    assert AGENT_BOUND_STATUS not in DOWNLOAD_RECOVERY_STATUSES
