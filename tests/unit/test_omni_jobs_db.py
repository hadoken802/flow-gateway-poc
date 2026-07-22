import pytest
from pathlib import Path

from agent.db import crud
from agent.db.schema import close_db, init_db


@pytest.mark.asyncio
async def test_omni_test_job_persists_independently(monkeypatch):
    import agent.config as config
    import agent.db.schema as schema

    temp_dir = Path(".tmp") / "tests" / "omni_jobs_db"
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / "flow_agent.db"
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(db_path) + suffix)
        if path.exists():
            path.unlink()
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(schema, "DB_PATH", db_path)
    await close_db()
    await init_db()

    job = await crud.create_omni_test_job(
        "job-1",
        "project-123",
        "A prompt",
        "D:\\image.png",
    )
    assert job["status"] == "queued"

    updated = await crud.update_omni_test_job(
        "job-1",
        input_media_id="input-1",
        output_media_id="output-1",
        workflow_id="workflow-1",
        operation_name="operations/1",
        status="active",
    )

    assert updated["output_media_id"] == "output-1"
    active = await crud.list_omni_test_jobs(["active"])
    assert [row["job_id"] for row in active] == ["job-1"]
    await close_db()


@pytest.mark.asyncio
async def test_omni_test_job_idempotency_key_reuses_original_job(monkeypatch):
    import agent.config as config
    import agent.db.schema as schema

    temp_dir = Path(".tmp") / "tests" / "omni_jobs_idempotency"
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / "flow_agent.db"
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(db_path) + suffix)
        if path.exists():
            path.unlink()
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(schema, "DB_PATH", db_path)
    await close_db()
    await init_db()

    first = await crud.create_omni_test_job("job-1", "project-1", "prompt", "D:\\a.png", "same-key")
    second = await crud.create_omni_test_job("job-2", "project-1", "prompt", "D:\\a.png", "same-key")

    assert first["job_id"] == "job-1"
    assert second["job_id"] == "job-1"
    assert second["idempotency_key"] == "same-key"
    await close_db()
