import argparse
import sqlite3
from pathlib import Path


def make_db(run_dir: Path, *, video_path=None, status="completed", account_id="FLOW-024", project_id="project-1", worker_job_id="job-1"):
    db_path = run_dir / "gateway.db"
    with sqlite3.connect(db_path) as db:
        db.executescript(
            """
            CREATE TABLE flow_accounts(account_id TEXT PRIMARY KEY, api_url TEXT, current_task_id TEXT);
            CREATE TABLE flow_tasks(
                task_id TEXT PRIMARY KEY,
                assigned_account_id TEXT,
                assigned_runtime_instance_id TEXT,
                project_id TEXT,
                worker_job_id TEXT,
                status TEXT,
                video_path TEXT,
                remaining_credits INTEGER,
                error_code TEXT,
                error_message TEXT,
                created_at TEXT,
                completed_at TEXT,
                updated_at TEXT
            );
            """
        )
        db.execute("INSERT INTO flow_accounts(account_id,api_url,current_task_id) VALUES(?,?,NULL)", (account_id, "http://worker"))
        db.execute(
            """
            INSERT INTO flow_tasks(task_id,assigned_account_id,assigned_runtime_instance_id,project_id,worker_job_id,status,video_path,created_at,updated_at)
            VALUES('task-1',?,?,?,?,?,?, '2026-07-27T00:00:00Z','2026-07-27T00:00:00Z')
            """,
            (account_id, "runtime-1", project_id, worker_job_id, status, str(video_path) if video_path else None),
        )
        db.commit()
    return db_path


class FakeClient:
    def __init__(self, response):
        self.response = response
        self.retry_calls = []

    async def retry_omni_video_download(self, worker, worker_job_id):
        self.retry_calls.append((worker.account_id, worker.api_url, worker_job_id))
        return self.response


def args(run_dir, execute=False):
    return argparse.Namespace(run_dir=str(run_dir), account_id=["FLOW-024"], execute=execute)


def test_retry_downloads_dry_run_does_not_download_or_submit(tmp_path):
    from gateway.download_recovery import retry_downloads

    make_db(tmp_path)
    client = FakeClient({"status": "completed", "video_path": "unused"})

    result = retry_downloads(args(tmp_path), worker_client=client)

    assert result["ok"] is True
    assert result["execute"] is False
    assert result["download_attempt_count"] == 0
    assert result["project_create_call_count"] == 0
    assert result["gateway_task_create_count"] == 0
    assert result["worker_submit_call_count"] == 0
    assert result["credits_consumed"] == 0
    assert client.retry_calls == []


def test_retry_downloads_execute_skips_existing_valid_mp4(tmp_path):
    from gateway.download_recovery import retry_downloads

    video = tmp_path / "job-1.mp4"
    video.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"x" * 2048)
    make_db(tmp_path, video_path=video)
    client = FakeClient({"status": "completed", "video_path": str(video)})

    result = retry_downloads(args(tmp_path, execute=True), worker_client=client)

    assert result["ok"] is True
    assert result["download_attempt_count"] == 0
    assert result["tasks"][0]["skip_reason"] == "valid_mp4_exists"
    assert client.retry_calls == []


def test_retry_downloads_execute_updates_completed_only_for_valid_mp4(tmp_path):
    from gateway.download_recovery import retry_downloads

    video = tmp_path / "job-1.mp4"
    video.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"x" * 2048)
    make_db(tmp_path, status="download_failed")
    client = FakeClient({"status": "completed", "video_path": str(video), "remaining_credits": 20})

    result = retry_downloads(args(tmp_path, execute=True), worker_client=client)

    assert client.retry_calls == [("FLOW-024", "http://worker", "job-1")]
    assert result["ok"] is True
    assert result["download_attempt_count"] == 1
    with sqlite3.connect(tmp_path / "gateway.db") as db:
        row = db.execute("SELECT status, video_path, remaining_credits, error_code FROM flow_tasks WHERE task_id='task-1'").fetchone()
    assert row == ("completed", str(video), 20, None)


def test_retry_downloads_execute_marks_invalid_download_failed(tmp_path):
    from gateway.download_recovery import retry_downloads

    video = tmp_path / "bad.mp4"
    video.write_bytes(b"not-valid" + b"x" * 2048)
    make_db(tmp_path, status="download_failed")
    client = FakeClient({"status": "completed", "video_path": str(video)})

    result = retry_downloads(args(tmp_path, execute=True), worker_client=client)

    assert result["ok"] is False
    assert result["tasks"][0]["gateway_status_after"] == "download_failed"
    with sqlite3.connect(tmp_path / "gateway.db") as db:
        row = db.execute("SELECT status, error_code FROM flow_tasks WHERE task_id='task-1'").fetchone()
    assert row == ("download_failed", "invalid_local_mp4")


def test_retry_downloads_rejects_wrong_project_or_account_shape(tmp_path):
    from gateway.download_recovery import retry_downloads

    make_db(tmp_path, project_id=None)
    result = retry_downloads(args(tmp_path), worker_client=FakeClient({}))

    assert result["ok"] is False
    assert result["tasks"][0]["allowed"] is False
    assert result["tasks"][0]["skip_reason"] == "missing_project_id"
