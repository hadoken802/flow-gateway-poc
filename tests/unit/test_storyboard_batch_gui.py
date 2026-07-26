import argparse
import json
import sqlite3
from pathlib import Path

import pytest


def png(path: Path):
    path.write_bytes(b"\x89PNG\r\n\x1a\n" + b"0" * 16)
    return path


def manifest(tmp_path, rows):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


def test_storyboard_manifest_rejects_duplicate_shot_id(tmp_path):
    from gateway.storyboard_batch import StoryboardBatchError, load_manifest

    image = png(tmp_path / "a.png")
    path = manifest(tmp_path, [
        {"shot_id": "001", "image": str(image), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"},
        {"shot_id": "001", "image": str(image), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"},
    ])
    with pytest.raises(StoryboardBatchError, match="duplicate shot_id"):
        load_manifest(path.resolve())


def test_storyboard_manifest_rejects_missing_image_before_gateway_start(tmp_path, monkeypatch):
    from gateway import storyboard_batch

    path = manifest(tmp_path, [{"shot_id": "001", "image": str(tmp_path / "missing.png"), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}])
    started = False
    monkeypatch.setattr(storyboard_batch, "_start_gateway_for_batch", lambda *_args: (_ for _ in ()).throw(AssertionError("must not start")))
    result = storyboard_batch.run_storyboard_batch(argparse.Namespace(manifest=str(path.resolve()), concurrency=3, output_dir=str(tmp_path), timeout_seconds=1200, gateway_port=8888, test_mode=True))
    assert result["stage"] == "input_validation_failed"
    assert started is False


def test_storyboard_batch_starts_one_gateway_and_writes_one_database(tmp_path, monkeypatch):
    from gateway import storyboard_batch

    image = png(tmp_path / "a.png")
    path = manifest(tmp_path, [{"shot_id": "001", "image": str(image.resolve()), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}])
    calls = {}
    video = tmp_path / "out.mp4"
    video.write_bytes(b"\x00\x00\x00\x18ftypmp42")

    class Proc:
        pid = 123
        def poll(self): return None
        def terminate(self): pass
        def wait(self, timeout=None): return 0

    def start(port, db_path, log_path, concurrency, test_mode):
        calls.setdefault("starts", 0)
        calls["starts"] += 1
        calls["db_path"] = db_path
        calls["concurrency"] = concurrency
        return Proc()

    monkeypatch.setattr(storyboard_batch, "_start_gateway_for_batch", start)
    monkeypatch.setattr(storyboard_batch.cli, "_wait_for_gateway", lambda port: None)
    monkeypatch.setattr(storyboard_batch, "_post_tasks", lambda port, shots: [{"task_id": "task-1"}])
    monkeypatch.setattr(storyboard_batch, "_wait_for_tasks", lambda port, tasks, timeout: [{
        "task_id": "task-1", "project_id": "project-1", "assigned_account_id": "FLOW-024",
        "assigned_runtime_instance_id": "runtime-24", "worker_job_id": "job-1", "attempt_count": 1,
        "status": "completed", "error_code": None, "error_message": None, "video_path": str(video),
    }])
    monkeypatch.setattr(storyboard_batch, "_stop_gateway", lambda proc: True)

    result = storyboard_batch.run_storyboard_batch(argparse.Namespace(manifest=str(path.resolve()), concurrency=3, output_dir=str(tmp_path), timeout_seconds=1200, gateway_port=8888, test_mode=True))

    assert result["ok"] is True
    assert calls["starts"] == 1
    assert calls["db_path"] == Path(result["run_dir"]) / "gateway.db"
    assert calls["concurrency"] == 3
    assert (Path(result["run_dir"]) / "batch-result.json").exists()


def test_storyboard_gui_import_does_not_start_mainloop():
    import gateway.storyboard_gui as gui

    assert hasattr(gui, "StoryboardGui")


def test_gateway_instance_lock_rejects_live_existing_lock(tmp_path, monkeypatch):
    from gateway.instance_lock import GatewayInstanceLock, GatewayInstanceLockError

    lock_path = tmp_path / "gateway.lock"
    lock_path.write_text(json.dumps({"server_pid": 12345}), encoding="utf-8")
    monkeypatch.setattr("gateway.instance_lock._pid_exists", lambda pid: True)

    lock = GatewayInstanceLock(tmp_path / "gateway.db", 8123, lock_path)
    with pytest.raises(GatewayInstanceLockError, match="gateway_instance_already_running"):
        lock.acquire()


def test_gateway_instance_lock_allows_stale_pid(tmp_path, monkeypatch):
    from gateway.instance_lock import GatewayInstanceLock

    lock_path = tmp_path / "gateway.lock"
    lock_path.write_text(json.dumps({"server_pid": 12345}), encoding="utf-8")
    monkeypatch.setattr("gateway.instance_lock._pid_exists", lambda pid: False)

    lock = GatewayInstanceLock(tmp_path / "gateway.db", 8123, lock_path)
    payload = lock.acquire()
    assert payload["gateway_port"] == 8123
    assert lock_path.exists()
    lock.release()
    assert not lock_path.exists()


def test_reconcile_existing_run_copies_db_and_does_not_submit_or_create_project(tmp_path, monkeypatch):
    from gateway import reconcile
    from gateway.worker_provider import WorkerConfig, WorkerSnapshot

    source = tmp_path / "source"
    source.mkdir()
    db_path = source / "gateway.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE flow_accounts(account_id TEXT PRIMARY KEY,status TEXT,credits INTEGER,current_task_id TEXT);
        CREATE TABLE flow_tasks(
            task_id TEXT PRIMARY KEY,idempotency_key TEXT,status TEXT,assigned_account_id TEXT,
            assigned_runtime_instance_id TEXT,project_id TEXT,worker_job_id TEXT,attempt_count INTEGER,
            error_code TEXT,error_message TEXT,video_path TEXT,remaining_credits INTEGER,
            created_at TEXT,assigned_at TEXT,submitted_at TEXT,completed_at TEXT,updated_at TEXT
        );
        INSERT INTO flow_accounts VALUES('FLOW-024','busy',5,'task-1');
        INSERT INTO flow_tasks VALUES('task-1','storyboard-001-1','manual_review','FLOW-024','runtime-24','project-1','job-1',1,'account_not_bound','old',NULL,NULL,'','','','','');
        """
    )
    conn.commit()
    conn.close()
    (source / "batch-result.json").write_text("{}", encoding="utf-8")
    video = tmp_path / "job-1.mp4"
    video.write_bytes(b"\x00\x00\x00\x18ftypmp42")

    class Provider:
        def load_workers(self):
            return WorkerSnapshot([WorkerConfig("FLOW-024", "http://worker", True, "runtime-24")], [], "runtime_registry", "Fake", "now")

    monkeypatch.setattr(reconcile, "RuntimeRegistryWorkerProvider", lambda: Provider())
    calls = {"get": 0, "post": 0}
    monkeypatch.setattr(reconcile, "_get_json", lambda url: calls.__setitem__("get", calls["get"] + 1) or {"status": "completed", "video_path": str(video), "remaining_credits": 5})
    monkeypatch.setattr(reconcile, "_post_json", lambda url: calls.__setitem__("post", calls["post"] + 1) or {})

    output = tmp_path / "reconcile"
    result = reconcile.reconcile_existing_run(argparse.Namespace(run_dir=str(source), output_run_dir=str(output)))

    assert result["ok"] is True
    assert calls == {"get": 1, "post": 0}
    assert result["project_create_call_count"] == 0
    assert result["worker_submit_call_count"] == 0
    assert result["source_hashes_before"] == result["source_hashes_after"]
    assert (output / "batch-result-reconciled.json").exists()
    assert (source / "batch-result-reconciled.json").exists() is False
