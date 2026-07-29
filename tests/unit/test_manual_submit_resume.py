import argparse
import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway import crud, db as gateway_db
from gateway import cli
from gateway import manual_submit_resume
from gateway.manual_submit_resume import _resume_manual_submit, preflight_manual_submit_resume
from gateway.worker_client import WorkerClient
from gateway.worker_provider import WorkerSnapshot, WorkerConfig


class Provider:
    def __init__(self, worker=None):
        self.worker = worker or WorkerConfig("FLOW-002", "http://worker", True, "runtime-2")

    def load_workers(self):
        return WorkerSnapshot([self.worker], [], "test", "test", "now")


class FakeClient:
    def __init__(self, submit_result=None, job=None, candidates=0, poll=None):
        self.submit_result = submit_result or {"job_id": "resume-job", "status": "scheduled"}
        self.job = job or {
            "job_id": "old-job",
            "project_id": "project-2",
            "input_media_id": "input-2",
            "status": "failed",
            "error_code": "403",
        }
        self.candidates = candidates
        self.poll = list(poll or [])
        self.submit_calls = 0
        self.retry_calls = 0
        self.submitted_job_id = None

    async def inspect(self, worker):
        return {"status": "ok", "extension_connected": True, "flow_key_present": True}

    async def get_omni_video(self, worker, worker_job_id):
        if self.submitted_job_id and worker_job_id == self.submitted_job_id and self.poll:
            return self.poll.pop(0)
        return self.job

    async def list_manual_flow_results(self, worker, project_id, after=None, exclude_media_ids=None):
        return {"candidate_count": self.candidates, "candidates": []}

    async def resume_omni_video(self, worker, payload):
        self.submit_calls += 1
        self.submitted_job_id = self.submit_result.get("job_id") or self.submit_result.get("worker_job_id")
        assert payload["input_media_id"] == "input-2"
        assert payload["project_id"] == "project-2"
        return self.submit_result

    async def retry_omni_video_download(self, worker, worker_job_id):
        self.retry_calls += 1
        return self.poll.pop(0)


class RecordingHttpxClient:
    last_params = None
    last_url = None

    def __init__(self, timeout):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, params=None):
        RecordingHttpxClient.last_url = url
        RecordingHttpxClient.last_params = params or {}
        return SimpleNamespace(
            raise_for_status=lambda: SimpleNamespace(
                json=lambda: {"project_id": "project-2", "candidate_count": 0, "candidates": []}
            )
        )


def args(db_path, task_id="task-2", execute=False, confirm=None, user_confirmed=False):
    return argparse.Namespace(
        task_id=task_id,
        gateway_db=str(db_path),
        preflight_only=not execute,
        execute=execute,
        confirm_task_id=confirm,
        user_confirmed_verification_cleared=user_confirmed,
    )


async def seed(db_path, *, status="manual_submit_required", error_code="UPSTREAM_UNUSUAL_ACTIVITY", operation_name=None, video_path=None, current_task_id="task-2", project_id="project-2", prompt="prompt"):
    db = await gateway_db.connect(db_path)
    await db.execute(
        """
        INSERT INTO flow_accounts(account_id, api_url, enabled, status, credits, current_task_id, lock_owner, lock_version)
        VALUES('FLOW-002', 'http://worker', 1, 'busy', 100, ?, 'old-owner', 1)
        """,
        (current_task_id,),
    )
    await db.execute(
        """
        INSERT INTO flow_tasks(task_id, idempotency_key, project_id, image_path, prompt, duration, aspect_ratio,
          status, assigned_account_id, account_id, lease_owner, lease_version, worker_job_id,
          operation_name, error_code, generation_attempts, video_path)
        VALUES('task-2', 'idem-2', ?, 'D:/image.png', ?, 10, '9:16',
          ?, 'FLOW-002', 'FLOW-002', 'old-owner', 1, 'old-job',
          ?, ?, 1, ?)
        """,
        (project_id, prompt, status, operation_name, error_code, video_path),
    )
    await db.commit()
    await db.close()


@pytest.mark.asyncio
async def test_preflight_is_read_only_and_allowed(tmp_path):
    db_path = tmp_path / "gateway.db"
    await seed(db_path)
    before = await _task(db_path)
    result = await preflight_manual_submit_resume(db_path, "task-2", FakeClient(), Provider())
    after = await _task(db_path)
    assert result["resume_allowed"] is True
    assert before == after


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs, reason",
    [
        ({"status": "submitted"}, "status_not_manual_submit_required"),
        ({"status": "completed"}, "status_not_manual_submit_required"),
        ({"error_code": "OTHER"}, "error_code_not_upstream_unusual_activity"),
        ({"current_task_id": None}, "account_current_task_mismatch"),
        ({"operation_name": "op-1"}, "has_operation_name"),
        ({"project_id": None}, "missing_project_id"),
        ({"prompt": ""}, "missing_prompt_or_parameters"),
    ],
)
async def test_preflight_rejects_unsafe_rows(tmp_path, kwargs, reason):
    db_path = tmp_path / "gateway.db"
    await seed(db_path, **kwargs)
    result = await preflight_manual_submit_resume(db_path, "task-2", FakeClient(), Provider())
    assert result["resume_allowed"] is False
    assert reason in result["reasons"]


@pytest.mark.asyncio
async def test_preflight_rejects_candidate_count(tmp_path):
    db_path = tmp_path / "gateway.db"
    await seed(db_path)
    result = await preflight_manual_submit_resume(db_path, "task-2", FakeClient(candidates=1), Provider())
    assert result["resume_allowed"] is False
    assert "manual_flow_results_found" in result["reasons"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "candidates",
    [
        {"candidate_count": 0, "candidates": []},
        {"candidate_count": 1, "candidates": [{"media_id": "media-1", "project_id": "project-2", "url": None, "created_at": None, "updated_at": None}]},
        {"candidate_count": 2, "candidates": [{"media_id": None, "project_id": None}, {"media_id": "media-2", "updated_at": "bad-time"}]},
    ],
)
async def test_preflight_handles_manual_flow_result_shapes_without_type_error(tmp_path, candidates):
    class CandidateClient(FakeClient):
        async def list_manual_flow_results(self, worker, project_id, after=None, exclude_media_ids=None):
            return candidates

    db_path = tmp_path / "gateway.db"
    await seed(db_path)
    result = await preflight_manual_submit_resume(db_path, "task-2", CandidateClient(), Provider())
    assert result["reason_code"] in {None, "manual_flow_results_found"}


@pytest.mark.asyncio
async def test_worker_client_ignores_null_exclude_media_ids(monkeypatch):
    import gateway.worker_client as worker_client

    monkeypatch.setattr(worker_client.httpx, "AsyncClient", RecordingHttpxClient)
    client = WorkerClient()
    result = await client.list_manual_flow_results(
        SimpleNamespace(api_url="http://worker"),
        "project-2",
        exclude_media_ids={"media-2", None, "media-1", ""},
    )
    assert result["candidate_count"] == 0
    assert RecordingHttpxClient.last_url == "http://worker/api/test/omni-video/manual-flow-results/project-2"
    assert RecordingHttpxClient.last_params == {"exclude_media_ids": "media-1,media-2"}


@pytest.mark.asyncio
async def test_worker_client_omits_exclude_param_when_all_media_ids_null(monkeypatch):
    import gateway.worker_client as worker_client

    monkeypatch.setattr(worker_client.httpx, "AsyncClient", RecordingHttpxClient)
    client = WorkerClient()
    await client.list_manual_flow_results(
        SimpleNamespace(api_url="http://worker"),
        "project-2",
        exclude_media_ids={None, ""},
    )
    assert RecordingHttpxClient.last_params == {}


@pytest.mark.asyncio
async def test_preflight_rejects_existing_mp4(tmp_path):
    mp4 = tmp_path / "done.mp4"
    mp4.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\0" * 2048)
    db_path = tmp_path / "gateway.db"
    await seed(db_path, video_path=str(mp4))
    result = await preflight_manual_submit_resume(db_path, "task-2", FakeClient(), Provider())
    assert result["resume_allowed"] is False
    assert "valid_mp4_exists" in result["reasons"]


@pytest.mark.asyncio
async def test_execute_requires_confirm_and_user_verification(tmp_path):
    db_path = tmp_path / "gateway.db"
    await seed(db_path)
    client = FakeClient()
    result = await _resume_manual_submit(args(db_path, execute=True, confirm="other", user_confirmed=True), client, Provider())
    assert result["error_code"] == "confirm_task_id_mismatch"
    assert client.submit_calls == 0
    result = await _resume_manual_submit(args(db_path, execute=True, confirm="task-2", user_confirmed=False), client, Provider())
    assert result["error_code"] == "verification_not_confirmed"
    assert client.submit_calls == 0


@pytest.mark.asyncio
async def test_execute_acquires_new_lease_and_increments_attempts_once(tmp_path):
    db_path = tmp_path / "gateway.db"
    await seed(db_path)
    client = FakeClient(submit_result={"job_id": "resume-job", "status": "failed", "error_code": "403", "error_message": "PERMISSION_DENIED reCAPTCHA evaluation failed PUBLIC_ERROR_UNUSUAL_ACTIVITY"})
    result = await _resume_manual_submit(args(db_path, execute=True, confirm="task-2", user_confirmed=True), client, Provider())
    row = await _task(db_path)
    account = await _account(db_path)
    assert result["submit_call_count"] == 1
    assert row["generation_attempts"] == 2
    assert row["lease_version"] == 2
    assert account["lock_version"] == 2
    assert row["status"] == "manual_submit_required"


@pytest.mark.asyncio
async def test_two_concurrent_acquires_only_one_succeeds(tmp_path):
    db_path = tmp_path / "gateway.db"
    await seed(db_path)
    db1 = await gateway_db.connect(db_path)
    db2 = await gateway_db.connect(db_path)
    try:
        results = await asyncio.gather(
            crud.acquire_manual_submit_resume(db1, "task-2"),
            crud.acquire_manual_submit_resume(db2, "task-2"),
        )
        assert sum(1 for item in results if item) == 1
    finally:
        await db1.close()
        await db2.close()


@pytest.mark.asyncio
async def test_remote_evidence_blocks_submit_after_attempt_increment(tmp_path):
    class RemoteAfterPreflightClient(FakeClient):
        def __init__(self):
            super().__init__()
            self.reads = 0

        async def get_omni_video(self, worker, worker_job_id):
            self.reads += 1
            if self.reads >= 2:
                return {"job_id": "old-job", "input_media_id": "input-2", "output_media_id": "media-1"}
            return await super().get_omni_video(worker, worker_job_id)

    db_path = tmp_path / "gateway.db"
    await seed(db_path)
    client = RemoteAfterPreflightClient()
    result = await _resume_manual_submit(args(db_path, execute=True, confirm="task-2", user_confirmed=True), client, Provider())
    row = await _task(db_path)
    assert result["error_code"] == "REMOTE_RESULT_DETECTED_BEFORE_RESUME"
    assert client.submit_calls == 0
    assert row["generation_attempts"] == 2
    assert row["status"] == "submission_unknown"


@pytest.mark.asyncio
async def test_again_403_returns_manual_without_looping(tmp_path):
    db_path = tmp_path / "gateway.db"
    await seed(db_path)
    client = FakeClient(submit_result={"job_id": "resume-job", "status": "failed", "error_code": "403", "error_message": "PERMISSION_DENIED reCAPTCHA evaluation failed PUBLIC_ERROR_UNUSUAL_ACTIVITY"})
    result = await _resume_manual_submit(args(db_path, execute=True, confirm="task-2", user_confirmed=True), client, Provider())
    row = await _task(db_path)
    assert result["error_code"] == "UPSTREAM_UNUSUAL_ACTIVITY"
    assert client.submit_calls == 1
    assert row["status"] == "manual_submit_required"
    assert row["generation_attempts"] == 2


@pytest.mark.asyncio
async def test_submit_exception_enters_submission_unknown(tmp_path):
    class TimeoutClient(FakeClient):
        async def resume_omni_video(self, worker, payload):
            self.submit_calls += 1
            raise TimeoutError("submit timed out")

    db_path = tmp_path / "gateway.db"
    await seed(db_path)
    client = TimeoutClient()
    result = await _resume_manual_submit(args(db_path, execute=True, confirm="task-2", user_confirmed=True), client, Provider())
    row = await _task(db_path)
    assert result["error_code"] == "submission_unknown"
    assert client.submit_calls == 1
    assert row["status"] == "submission_unknown"


@pytest.mark.asyncio
async def test_success_downloads_and_fenced_releases_account(tmp_path):
    mp4 = tmp_path / "out.mp4"
    mp4.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\0" * 2048)
    db_path = tmp_path / "gateway.db"
    await seed(db_path)
    client = FakeClient(
        submit_result={"job_id": "resume-job", "status": "scheduled", "output_media_id": "media", "workflow_id": "wf", "upstream_batch_id": "batch"},
        poll=[
            {"status": "waiting_download"},
            {"status": "completed", "video_path": str(mp4), "remaining_credits": 80},
        ],
    )
    result = await _resume_manual_submit(args(db_path, execute=True, confirm="task-2", user_confirmed=True), client, Provider())
    row = await _task(db_path)
    account = await _account(db_path)
    assert result["ok"] is True
    assert row["status"] == "completed"
    assert row["download_attempts"] == 1
    assert account["current_task_id"] is None
    assert account["lock_owner"] is None


def test_cli_registers_resume_manual_submit_default_preflight(monkeypatch, tmp_path, capsys):
    calls = []

    def fake_resume(parsed):
        calls.append(parsed)
        return {"ok": True, "execute": parsed.execute, "preflight_only": parsed.preflight_only}

    monkeypatch.setattr(manual_submit_resume, "resume_manual_submit", fake_resume)
    code = cli.main([
        "resume-manual-submit",
        "--task-id", "task-2",
        "--gateway-db", str(tmp_path / "gateway.db"),
    ])
    out = capsys.readouterr().out
    assert code == 0
    assert calls[0].execute is False
    assert calls[0].preflight_only is True
    assert '"preflight_only": true' in out


async def _task(db_path):
    db = await gateway_db.connect(db_path)
    try:
        return await crud.get_task(db, "task-2")
    finally:
        await db.close()


async def _account(db_path):
    db = await gateway_db.connect(db_path)
    try:
        return await crud.get_account(db, "FLOW-002")
    finally:
        await db.close()
