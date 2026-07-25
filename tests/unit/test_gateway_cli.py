import argparse
from pathlib import Path

import pytest


def _args(tmp_path, **overrides):
    image = tmp_path / "input.png"
    image.write_bytes(b"\x89PNG\r\n\x1a\n" + b"0" * 16)
    values = {
        "image": str(image),
        "prompt": "A quiet abstract motion study.",
        "duration": 10,
        "aspect_ratio": "9:16",
        "preferred_account": "FLOW-024",
        "output_dir": str(tmp_path),
        "timeout_seconds": 1200,
        "idempotency_key": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_run_video_once_rejects_missing_image_before_gateway_start(tmp_path, monkeypatch):
    from gateway import cli

    started = False

    def fail_start(*_args, **_kwargs):
        nonlocal started
        started = True

    monkeypatch.setattr(cli, "_start_gateway", fail_start)
    result = cli.run_video_once(_args(tmp_path, image=str(tmp_path / "missing.png")))

    assert result["result"] == "input_validation_failed"
    assert started is False


def test_run_video_once_rejects_empty_prompt(tmp_path):
    from gateway import cli

    result = cli.run_video_once(_args(tmp_path, prompt="  "))

    assert result["result"] == "input_validation_failed"


def test_run_video_once_rejects_unsupported_duration(tmp_path):
    from gateway import cli

    result = cli.run_video_once(_args(tmp_path, duration=8))

    assert result["result"] == "input_validation_failed"


def test_run_video_once_rejects_unsupported_aspect_ratio(tmp_path):
    from gateway import cli

    result = cli.run_video_once(_args(tmp_path, aspect_ratio="1:1"))

    assert result["result"] == "input_validation_failed"


def test_run_video_once_creates_isolated_run_dir_and_gateway_db(tmp_path, monkeypatch):
    from gateway import cli

    captured = {}
    video = tmp_path / "out.mp4"
    video.write_bytes(b"\x00\x00\x00\x18ftypmp42")

    class Proc:
        def __init__(self):
            self.terminated = False

        def poll(self):
            return 0 if self.terminated else None

        def terminate(self):
            self.terminated = True

        def wait(self, timeout=None):
            return 0

    def fake_start(port, db_path, log_path):
        captured["port"] = port
        captured["db_path"] = db_path
        captured["log_path"] = log_path
        return Proc()

    monkeypatch.setattr(cli, "_find_free_local_port", lambda: 8765)
    monkeypatch.setattr(cli, "_start_gateway", fake_start)
    monkeypatch.setattr(cli, "_wait_for_gateway", lambda port: None)
    monkeypatch.setattr(cli, "_wait_for_eligible_account", lambda port, preferred: [{"account_id": "FLOW-024", "status": "ready", "api_url": "http://127.0.0.1:8121"}])
    monkeypatch.setattr(cli, "_create_project", lambda api_url, run_dir: {"id": "real-project-id"})
    monkeypatch.setattr(cli, "_post_json", lambda port, path, payload, timeout: {"task_id": "task-1"})
    monkeypatch.setattr(
        cli,
        "_wait_for_task",
        lambda port, task_id, timeout: {
            "task_id": task_id,
            "assigned_account_id": "FLOW-024",
            "worker_job_id": "job-1",
            "attempt_count": 1,
            "status": "completed",
            "error_code": None,
            "error_message": None,
            "video_path": str(video),
        },
    )

    result = cli.run_video_once(_args(tmp_path))

    assert result["ok"] is True
    assert result["gateway_stopped"] is True
    assert Path(result["run_dir"]).name.startswith("flow-video-once-")
    assert captured["db_path"] == Path(result["run_dir"]) / "gateway.db"
    assert captured["port"] == 8765


def test_run_video_once_fails_when_preferred_account_is_not_eligible(tmp_path, monkeypatch):
    from gateway import cli

    class Proc:
        def poll(self):
            return None

        def terminate(self):
            pass

        def kill(self):
            pass

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(cli, "_find_free_local_port", lambda: 8765)
    monkeypatch.setattr(cli, "_start_gateway", lambda *_args: Proc())
    monkeypatch.setattr(cli, "_wait_for_gateway", lambda port: None)
    monkeypatch.setattr(cli, "_wait_for_eligible_account", lambda port, preferred: (_ for _ in ()).throw(cli.RunVideoOnceError("no_eligible_worker", "Preferred account is not eligible")))

    result = cli.run_video_once(_args(tmp_path))

    assert result["result"] == "no_eligible_worker"


def test_validate_inputs_allows_current_aspect_ratios(tmp_path):
    from gateway import cli

    cli._validate_inputs(_args(tmp_path, aspect_ratio="9:16"))
    cli._validate_inputs(_args(tmp_path, aspect_ratio="16:9"))


def test_create_project_uses_worker_response_id(monkeypatch, tmp_path):
    from gateway import cli

    calls = []
    monkeypatch.setattr(cli, "_post_url_json", lambda url, payload, timeout: calls.append((url, payload, timeout)) or {"id": "project-from-worker"})

    result = cli._create_project("http://127.0.0.1:8121", tmp_path)

    assert result["id"] == "project-from-worker"
    assert calls[0][0] == "http://127.0.0.1:8121/api/projects"


def test_validate_completed_task_rejects_stale_error_on_completed(tmp_path):
    from gateway import cli

    video = tmp_path / "out.mp4"
    video.write_bytes(b"\x00\x00\x00\x18ftypmp42")
    with pytest.raises(cli.RunVideoOnceError, match="retained an error"):
        cli._validate_completed_task({
            "task_id": "task-1",
            "worker_job_id": "job-1",
            "attempt_count": 1,
            "status": "completed",
            "error_code": "worker_offline",
            "error_message": "old",
            "video_path": str(video),
        })


def test_validate_completed_task_checks_mp4_ftyp(tmp_path):
    from gateway import cli

    video = tmp_path / "out.mp4"
    video.write_bytes(b"not-mp4")
    with pytest.raises(cli.RunVideoOnceError, match="valid MP4"):
        cli._validate_completed_task({
            "task_id": "task-1",
            "worker_job_id": "job-1",
            "attempt_count": 1,
            "status": "completed",
            "error_code": None,
            "error_message": None,
            "video_path": str(video),
        })
