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

    def start(port, db_path, log_path, concurrency, test_mode, account_ids=None):
        calls.setdefault("starts", 0)
        calls["starts"] += 1
        calls["db_path"] = db_path
        calls["concurrency"] = concurrency
        calls["account_ids"] = account_ids
        return Proc()

    monkeypatch.setattr(storyboard_batch, "run_preflight", lambda shots, account_ids, run_dir: {
        "ok": True,
        "requested_account_ids": [],
        "eligible_account_ids": [],
        "excluded_account_ids": [],
        "excluded_reasons": {},
    })
    monkeypatch.setattr(storyboard_batch, "_start_gateway_for_batch", start)
    monkeypatch.setattr(storyboard_batch.cli, "_wait_for_gateway", lambda *args, **kwargs: None)
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
    assert calls["account_ids"] == []
    assert (Path(result["run_dir"]) / "batch-result.json").exists()


def test_storyboard_batch_parse_account_ids_dedupes_stably():
    from gateway.storyboard_batch import parse_account_ids

    assert parse_account_ids("FLOW-025,FLOW-026,FLOW-025, FLOW-027 ") == ["FLOW-025", "FLOW-026", "FLOW-027"]


def test_storyboard_preflight_only_does_not_start_gateway_or_post_tasks(tmp_path, monkeypatch):
    from gateway import storyboard_batch
    from gateway.worker_provider import WorkerConfig, WorkerSnapshot

    image = png(tmp_path / "a.png")
    path = manifest(tmp_path, [{"shot_id": "001", "image": str(image.resolve()), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}])

    class Provider:
        def load_workers(self):
            workers = [
                WorkerConfig("FLOW-025", "http://worker-25", True, "runtime-25"),
                WorkerConfig("FLOW-026", "http://worker-26", True, "runtime-26"),
                WorkerConfig("FLOW-027", "http://worker-27", True, "runtime-27"),
            ]
            candidates = [
                {
                    "account_id": worker.account_id,
                    "eligible": True,
                    "registration_status": "login_verified",
                    "runtime_status": "running",
                    "runtime_healthy": True,
                    "extension_ready": True,
                    "account_match": True,
                    "ownership_verified": True,
                    "worker_health_reachable": True,
                    "worker_api_endpoint": worker.api_url,
                    "runtime_instance_id": worker.runtime_instance_id,
                    "current_task_id": None,
                    "exclusion_reasons": [],
                }
                for worker in workers
            ]
            candidates.append({"account_id": "FLOW-024", "eligible": True, "exclusion_reasons": []})
            return WorkerSnapshot(workers, candidates, "runtime_registry", "Fake", "now")

    class Client:
        async def inspect(self, worker):
            return {"status": "ok", "credits": 35, "extension_connected": True, "flow_key_present": True}

    monkeypatch.setattr(storyboard_batch, "RuntimeRegistryWorkerProvider", lambda: Provider())
    monkeypatch.setattr(storyboard_batch, "WorkerClient", lambda: Client())
    monkeypatch.setattr(storyboard_batch, "_start_gateway_for_batch", lambda *_args: (_ for _ in ()).throw(AssertionError("must not start gateway")))
    monkeypatch.setattr(storyboard_batch, "_post_tasks", lambda *_args: (_ for _ in ()).throw(AssertionError("must not create gateway tasks")))

    result = storyboard_batch.run_storyboard_batch(argparse.Namespace(
        manifest=str(path.resolve()),
        concurrency=3,
        output_dir=str(tmp_path),
        timeout_seconds=1200,
        gateway_port=8888,
        test_mode=False,
        account_ids="FLOW-025,FLOW-026,FLOW-025,FLOW-027",
        preflight_only=True,
    ))

    assert result["ok"] is True
    assert result["requested_account_ids"] == ["FLOW-025", "FLOW-026", "FLOW-027"]
    assert set(result["eligible_account_ids"]) == {"FLOW-025", "FLOW-026", "FLOW-027"}
    assert result["excluded_reasons"]["FLOW-024"] == ["excluded_by_allowlist"]
    assert result["gateway_started"] is False
    assert result["project_create_call_count"] == 0
    assert result["worker_submit_call_count"] == 0
    assert (Path(result["run_dir"]) / "preflight-result.json").exists()


def test_storyboard_preflight_blocks_low_credit_account_before_gateway_tasks(tmp_path, monkeypatch):
    from gateway import storyboard_batch
    from gateway.worker_provider import WorkerConfig, WorkerSnapshot

    image = png(tmp_path / "a.png")
    path = manifest(tmp_path, [{"shot_id": "001", "image": str(image.resolve()), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}])

    class Provider:
        def load_workers(self):
            workers = [
                WorkerConfig("FLOW-025", "http://worker-25", True, "runtime-25"),
                WorkerConfig("FLOW-026", "http://worker-26", True, "runtime-26"),
                WorkerConfig("FLOW-027", "http://worker-27", True, "runtime-27"),
            ]
            candidates = [{"account_id": worker.account_id, "eligible": True, "worker_api_endpoint": worker.api_url, "runtime_instance_id": worker.runtime_instance_id, "current_task_id": None, "exclusion_reasons": []} for worker in workers]
            return WorkerSnapshot(workers, candidates, "runtime_registry", "Fake", "now")

    class Client:
        async def inspect(self, worker):
            return {"status": "ok", "credits": 5 if worker.account_id == "FLOW-027" else 35, "extension_connected": True, "flow_key_present": True}

    monkeypatch.setattr(storyboard_batch, "RuntimeRegistryWorkerProvider", lambda: Provider())
    monkeypatch.setattr(storyboard_batch, "WorkerClient", lambda: Client())
    monkeypatch.setattr(storyboard_batch, "_start_gateway_for_batch", lambda *_args: (_ for _ in ()).throw(AssertionError("must not start gateway")))
    monkeypatch.setattr(storyboard_batch, "_post_tasks", lambda *_args: (_ for _ in ()).throw(AssertionError("must not create gateway tasks")))

    result = storyboard_batch.run_storyboard_batch(argparse.Namespace(
        manifest=str(path.resolve()),
        concurrency=3,
        output_dir=str(tmp_path),
        timeout_seconds=1200,
        gateway_port=8888,
        test_mode=False,
        account_ids="FLOW-025,FLOW-026,FLOW-027",
        preflight_only=False,
    ))

    assert result["ok"] is False
    assert result["excluded_reasons"]["FLOW-027"] == ["insufficient_credits"]
    assert result["project_create_call_count"] == 0
    assert result["worker_submit_call_count"] == 0


def test_storyboard_preflight_blocks_missing_and_credit_request_failed_accounts(tmp_path, monkeypatch):
    from gateway import storyboard_batch
    from gateway.worker_provider import WorkerConfig, WorkerSnapshot

    image = png(tmp_path / "a.png")
    shots = storyboard_batch.load_manifest(manifest(tmp_path, [{"shot_id": "001", "image": str(image.resolve()), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}]).resolve())

    class Provider:
        def load_workers(self):
            worker = WorkerConfig("FLOW-025", "http://worker-25", True, "runtime-25")
            candidate = {
                "account_id": "FLOW-025",
                "eligible": True,
                "worker_api_endpoint": worker.api_url,
                "runtime_instance_id": worker.runtime_instance_id,
                "current_task_id": None,
                "exclusion_reasons": [],
            }
            return WorkerSnapshot([worker], [candidate], "runtime_registry", "Fake", "now")

    class Client:
        async def inspect(self, worker):
            raise TimeoutError("credits unavailable")

    monkeypatch.setattr(storyboard_batch, "RuntimeRegistryWorkerProvider", lambda: Provider())
    monkeypatch.setattr(storyboard_batch, "WorkerClient", lambda: Client())

    result = storyboard_batch.run_preflight(shots, ["FLOW-025", "FLOW-027"], tmp_path)

    assert result["ok"] is False
    assert "TimeoutError" in result["excluded_reasons"]["FLOW-025"]
    assert "account_not_found" in result["excluded_reasons"]["FLOW-027"]
    assert "not_ready_worker" in result["excluded_reasons"]["FLOW-027"]
    assert result["accounts"][0]["credits_http_status"] is None


def test_storyboard_preflight_blocks_busy_account_without_changing_current_task(tmp_path, monkeypatch):
    from gateway import storyboard_batch
    from gateway.worker_provider import WorkerConfig, WorkerSnapshot

    image = png(tmp_path / "a.png")
    shots = storyboard_batch.load_manifest(manifest(tmp_path, [{"shot_id": "001", "image": str(image.resolve()), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}]).resolve())

    class Provider:
        def load_workers(self):
            worker = WorkerConfig("FLOW-025", "http://worker-25", True, "runtime-25")
            candidate = {
                "account_id": "FLOW-025",
                "eligible": True,
                "worker_api_endpoint": worker.api_url,
                "runtime_instance_id": worker.runtime_instance_id,
                "current_task_id": "task-existing",
                "exclusion_reasons": [],
            }
            return WorkerSnapshot([worker], [candidate], "runtime_registry", "Fake", "now")

    class Client:
        async def inspect(self, worker):
            return {"status": "ok", "credits": 35, "extension_connected": True, "flow_key_present": True}

    monkeypatch.setattr(storyboard_batch, "RuntimeRegistryWorkerProvider", lambda: Provider())
    monkeypatch.setattr(storyboard_batch, "WorkerClient", lambda: Client())

    result = storyboard_batch.run_preflight(shots, ["FLOW-025"], tmp_path)

    account = result["accounts"][0]
    assert result["ok"] is False
    assert account["current_task_id"] == "task-existing"
    assert "account_busy" in account["exclusion_reasons"]


def test_storyboard_batch_without_allowlist_keeps_existing_path_compatible(tmp_path, monkeypatch):
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

    def start(port, db_path, log_path, concurrency, test_mode, account_ids=None):
        calls["account_ids"] = account_ids
        return Proc()

    monkeypatch.setattr(storyboard_batch, "run_preflight", lambda shots, account_ids, run_dir: {
        "ok": True,
        "requested_account_ids": [],
        "eligible_account_ids": [],
        "excluded_account_ids": [],
        "excluded_reasons": {},
    })
    monkeypatch.setattr(storyboard_batch, "_start_gateway_for_batch", start)
    monkeypatch.setattr(storyboard_batch.cli, "_wait_for_gateway", lambda *args, **kwargs: None)
    monkeypatch.setattr(storyboard_batch, "_post_tasks", lambda port, shots: [{"task_id": "task-1"}])
    monkeypatch.setattr(storyboard_batch, "_wait_for_tasks", lambda port, tasks, timeout: [{
        "task_id": "task-1", "project_id": "project-1", "assigned_account_id": "FLOW-025",
        "assigned_runtime_instance_id": "runtime-25", "worker_job_id": "job-1", "attempt_count": 1,
        "status": "completed", "error_code": None, "error_message": None, "video_path": str(video),
    }])
    monkeypatch.setattr(storyboard_batch, "_stop_gateway", lambda proc: True)

    result = storyboard_batch.run_storyboard_batch(argparse.Namespace(
        manifest=str(path.resolve()),
        concurrency=3,
        output_dir=str(tmp_path),
        timeout_seconds=1200,
        gateway_port=8888,
        test_mode=True,
        account_ids=None,
        preflight_only=False,
    ))

    assert result["ok"] is True
    assert calls["account_ids"] == []


def test_storyboard_gui_import_does_not_start_mainloop():
    import gateway.storyboard_gui as gui

    assert hasattr(gui, "StoryboardGui")


class FakeVar:
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class FakeButton:
    def __init__(self):
        self.state = None

    def configure(self, state):
        self.state = state


class FakeTree:
    def __init__(self, selection=()):
        self._selection = selection

    def selection(self):
        return self._selection


def make_gui_shell(tmp_path, runner=None):
    from gateway.storyboard_gui import StoryboardGui

    gui = object.__new__(StoryboardGui)
    gui.batch_runner = runner or (lambda args: {"ok": True, "tasks": []})
    gui.shots = []
    gui.account_rows = {}
    gui.selected_account_ids = set()
    gui.batch_thread = None
    gui.preflight_thread = None
    gui.last_result = None
    gui.start_button = FakeButton()
    gui.preflight_button = FakeButton()
    gui.concurrency = FakeVar(3)
    gui.timeout_seconds = FakeVar(1200)
    gui.output_dir = FakeVar(str(tmp_path))
    gui.auto_scroll = FakeVar(False)
    gui._render_accounts = lambda: None
    gui._render_shots = lambda: None
    gui._log = lambda text: None
    gui.after = lambda _delay, func: func()
    return gui


def test_storyboard_gui_selected_accounts_are_passed_to_batch(tmp_path):
    from gateway.storyboard_gui import StoryboardGui

    image = png(tmp_path / "a.png")
    calls = []
    gui = make_gui_shell(tmp_path, lambda args: calls.append(args) or {"ok": False, "accounts": []})
    gui.account_rows = {account_id: {"account_id": account_id} for account_id in ["FLOW-024", "FLOW-025", "FLOW-026", "FLOW-027"]}
    gui.selected_account_ids = {"FLOW-025", "FLOW-026", "FLOW-027"}
    gui.shots = [{"shot_id": "001", "image": str(image), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}]

    args = StoryboardGui._build_batch_args(gui, preflight_only=False)

    assert args.account_ids == "FLOW-025,FLOW-026,FLOW-027"
    assert "FLOW-024" not in args.account_ids
    assert args.preflight_only is False


def test_storyboard_gui_preflight_uses_same_batch_service_without_gateway(tmp_path):
    from gateway.storyboard_gui import StoryboardGui

    image = png(tmp_path / "a.png")
    calls = []
    gui = make_gui_shell(tmp_path, lambda args: calls.append(args) or {
        "ok": True,
        "result": "preflight_passed",
        "gateway_started": False,
        "project_create_call_count": 0,
        "worker_submit_call_count": 0,
        "accounts": [
            {"account_id": "FLOW-025", "credits": 35, "credits_http_status": 200, "eligible_for_batch": True},
            {"account_id": "FLOW-026", "credits": 35, "credits_http_status": 200, "eligible_for_batch": True},
            {"account_id": "FLOW-027", "credits": 50, "credits_http_status": 200, "eligible_for_batch": True},
            {"account_id": "FLOW-024", "excluded_by_allowlist": True, "eligible_for_batch": False},
        ],
    })
    gui.account_rows = {account_id: {"account_id": account_id} for account_id in ["FLOW-024", "FLOW-025", "FLOW-026", "FLOW-027"]}
    gui.selected_account_ids = {"FLOW-025", "FLOW-026", "FLOW-027"}
    gui.shots = [{"shot_id": "001", "image": str(image), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}]

    args = StoryboardGui._build_batch_args(gui, preflight_only=True)
    result = gui.batch_runner(args)
    StoryboardGui._preflight_done(gui, result)

    assert calls[0].preflight_only is True
    assert calls[0].account_ids == "FLOW-025,FLOW-026,FLOW-027"
    assert result["gateway_started"] is False
    assert result["project_create_call_count"] == 0
    assert result["worker_submit_call_count"] == 0
    assert gui.account_rows["FLOW-025"]["credits"] == 35
    assert gui.account_rows["FLOW-027"]["credits_http_status"] == 200


def test_storyboard_gui_start_batch_rechecks_preflight_through_batch_service(tmp_path):
    from gateway.storyboard_gui import StoryboardGui

    image = png(tmp_path / "a.png")
    calls = []
    gui = make_gui_shell(tmp_path, lambda args: calls.append(args) or {
        "ok": False,
        "result": "preflight_failed",
        "project_create_call_count": 0,
        "worker_submit_call_count": 0,
        "accounts": [{"account_id": "FLOW-027", "credits": 5, "eligible_for_batch": False, "exclusion_reasons": ["insufficient_credits"]}],
    })
    gui.account_rows = {account_id: {"account_id": account_id} for account_id in ["FLOW-025", "FLOW-026", "FLOW-027"]}
    gui.selected_account_ids = {"FLOW-025", "FLOW-026", "FLOW-027"}
    gui.shots = [{"shot_id": "001", "image": str(image), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}]

    # Exercise the same argument path used by start_batch without starting a real Tk thread.
    args = StoryboardGui._build_batch_args(gui, preflight_only=False)
    result = gui.batch_runner(args)

    assert calls[0].preflight_only is False
    assert calls[0].account_ids == "FLOW-025,FLOW-026,FLOW-027"
    assert result["project_create_call_count"] == 0
    assert result["worker_submit_call_count"] == 0


def test_storyboard_gui_does_not_start_duplicate_batch(tmp_path):
    from gateway.storyboard_gui import StoryboardGui

    class LiveThread:
        def is_alive(self):
            return True

    calls = []
    gui = make_gui_shell(tmp_path, lambda args: calls.append(args) or {"ok": True})
    gui.batch_thread = LiveThread()
    StoryboardGui.start_batch(gui)

    assert calls == []


def test_storyboard_gui_loads_batch_result_without_submit(tmp_path):
    from gateway.storyboard_gui import StoryboardGui

    video = tmp_path / "out.mp4"
    video.write_bytes(b"\x00\x00\x00\x18ftypmp42")
    result_path = tmp_path / "batch-result-reconciled.json"
    result_path.write_text(json.dumps({
        "ok": True,
        "tasks": [
            {
                "shot_id": "001",
                "status": "completed",
                "assigned_account_id": "FLOW-025",
                "project_id": "project-1",
                "worker_job_id": "job-1",
                "video_path": str(video),
                "error_code": None,
            }
        ],
    }), encoding="utf-8")
    calls = []
    gui = make_gui_shell(tmp_path, lambda args: calls.append(args) or {"ok": True})

    loaded = StoryboardGui.load_batch_result_file(gui, result_path)

    assert loaded["ok"] is True
    assert calls == []
    assert gui.shots[0]["video_path"] == str(video)
    assert gui.shots[0]["assigned_account_id"] == "FLOW-025"


def test_storyboard_gui_selected_video_path_from_loaded_result(tmp_path):
    from gateway.storyboard_gui import StoryboardGui

    video = tmp_path / "out.mp4"
    gui = make_gui_shell(tmp_path)
    gui.shots = [{"shot_id": "001", "video_path": str(video)}]
    gui.shot_tree = FakeTree(("0",))

    assert StoryboardGui.selected_video_path(gui) == str(video)


def test_storyboard_gui_has_no_fake_pause_button():
    text = Path("gateway/storyboard_gui.py").read_text(encoding="utf-8")

    assert "pause_not_implemented" not in text
    assert "暂停继续领取新任务" not in text


def test_storyboard_batch_gateway_start_failure_writes_safe_result(tmp_path, monkeypatch):
    from gateway import storyboard_batch

    image = png(tmp_path / "a.png")
    path = manifest(tmp_path, [{"shot_id": "001", "image": str(image.resolve()), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}])

    class Proc:
        pid = 321
        def __init__(self):
            self.code = None
        def poll(self):
            return self.code
        def terminate(self):
            self.code = -15
        def wait(self, timeout=None):
            return self.code

    monkeypatch.setattr(storyboard_batch, "run_preflight", lambda shots, account_ids, run_dir: {
        "ok": True,
        "requested_account_ids": ["FLOW-025"],
        "eligible_account_ids": ["FLOW-025"],
        "excluded_account_ids": [],
        "excluded_reasons": {},
    })
    monkeypatch.setattr(storyboard_batch, "_start_gateway_for_batch", lambda *args, **kwargs: Proc())
    monkeypatch.setattr(storyboard_batch.cli, "_wait_for_gateway", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("Gateway /health did not become ready")))

    result = storyboard_batch.run_storyboard_batch(argparse.Namespace(
        manifest=str(path.resolve()),
        concurrency=1,
        output_dir=str(tmp_path),
        timeout_seconds=1200,
        gateway_port=8888,
        test_mode=False,
        account_ids="FLOW-025",
        preflight_only=False,
    ))

    assert result["ok"] is False
    assert result["stage"] == "gateway_startup"
    assert result["error_code"] == "gateway_start_failed"
    assert result["flow_task_count"] == 0
    assert result["project_create_call_count"] == 0
    assert result["worker_submit_call_count"] == 0
    assert (Path(result["run_dir"]) / "batch-result.json").exists()


def test_storyboard_gui_catches_batch_exception_and_restores_button(tmp_path, monkeypatch):
    from gateway.storyboard_gui import StoryboardGui

    image = png(tmp_path / "a.png")
    gui = make_gui_shell(tmp_path, lambda args: (_ for _ in ()).throw(RuntimeError("Gateway /health did not become ready")))
    gui.account_rows = {"FLOW-025": {"account_id": "FLOW-025"}}
    gui.selected_account_ids = {"FLOW-025"}
    gui.shots = [{"shot_id": "001", "image": str(image), "prompt": "p", "duration": 10, "aspect_ratio": "9:16"}]
    messages = []
    monkeypatch.setattr("gateway.storyboard_gui.messagebox.showerror", lambda title, body: messages.append((title, body)))

    StoryboardGui.start_batch(gui)
    gui.batch_thread.join(timeout=5)

    assert gui.start_button.state == "normal"
    assert gui.last_result["error_code"] == "gateway_start_failed"
    assert gui.shots[0]["shot_id"] == "001"
    assert gui.selected_account_ids == {"FLOW-025"}
    assert messages


def test_gateway_startup_smoke_result_is_zero_task(tmp_path, monkeypatch):
    from gateway import cli

    class Proc:
        pid = 456
        def __init__(self):
            self.code = None
        def poll(self):
            return self.code
        def terminate(self):
            self.code = 0
        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr("gateway.storyboard_batch.run_preflight", lambda shots, account_ids, run_dir: {
        "accounts": [
            {"account_id": "FLOW-025", "credits": 35, "current_task_id": None},
            {"account_id": "FLOW-026", "credits": 35, "current_task_id": None},
            {"account_id": "FLOW-027", "credits": 50, "current_task_id": None},
        ]
    })
    monkeypatch.setattr("gateway.storyboard_batch._start_gateway_for_batch", lambda *args, **kwargs: Proc())
    monkeypatch.setattr(cli, "_wait_for_gateway", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "_get_json", lambda *args, **kwargs: {"status": "ok", "api_port": 9999})

    result = cli.gateway_startup_smoke(argparse.Namespace(
        account_ids="FLOW-025,FLOW-026,FLOW-027",
        output_dir=str(tmp_path),
        timeout_seconds=60,
    ))

    assert result["ok"] is True
    assert result["flow_task_count"] == 0
    assert result["project_create_call_count"] == 0
    assert result["worker_submit_call_count"] == 0
    assert result["current_task_ids"] == {"FLOW-025": None, "FLOW-026": None, "FLOW-027": None}
    assert (Path(result["run_dir"]) / "gateway-startup-smoke-result.json").exists()


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
