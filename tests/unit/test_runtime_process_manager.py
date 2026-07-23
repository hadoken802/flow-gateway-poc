import json
from pathlib import Path

import pytest

from runtime import cli
from runtime import worker_entry
from runtime.process_manager import RuntimeManager
from runtime.registry import AccountRecord, AccountRegistry


class FakeProcess:
    next_pid = 1000

    def __init__(self, command, cwd=None, env=None, **_):
        FakeProcess.next_pid += 1
        self.pid = FakeProcess.next_pid
        self.command = command
        self.cwd = cwd
        self.env = env or {}


class FakeInspector:
    def __init__(self):
        self.commands = {}
        self.alive = set()
        self.terminated = []

    def process_alive(self, pid):
        return pid in self.alive

    def command_line(self, pid):
        return self.commands.get(pid, "")

    def terminate(self, pid, timeout_seconds=8.0):
        self.terminated.append(pid)
        self.alive.discard(pid)
        return True


def make_registry(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    workers_path = tmp_path / "workers.json"
    workers_path.write_text("[]", encoding="utf-8")
    return AccountRegistry(
        db_path=tmp_path / "data" / "runtime_registry.db",
        profiles_root=tmp_path / "profiles",
        data_root=tmp_path / "data",
        outputs_root=tmp_path / "outputs",
        workers_json_path=workers_path,
    )


def add_account(registry, account_id="FLOW-005", enabled=True, status="login_required"):
    ports = {
        "FLOW-001": (8100, 9222, 9301),
        "FLOW-002": (8112, 9212, 9302),
        "FLOW-003": (8113, 9213, 9303),
        "FLOW-005": (8101, 9200, 9300),
        "FLOW-006": (8102, 9201, 9304),
        "FLOW-007": (8103, 9202, 9305),
        "FLOW-008": (8104, 9203, 9306),
    }
    worker_api_port, extension_ws_port, chrome_cdp_port = ports[account_id]
    profile = registry.profiles_root / account_id
    profile.mkdir(parents=True, exist_ok=True)
    account = AccountRecord(
        account_id=account_id,
        display_name=account_id,
        profile_path=str(profile),
        worker_api_port=worker_api_port,
        extension_ws_port=extension_ws_port,
        chrome_cdp_port=chrome_cdp_port,
        database_path=str(registry.data_root / f"{account_id}.db"),
        output_dir=str(registry.outputs_root / account_id),
        enabled=enabled,
        status=status,
        created_at="2026-07-23T00:00:00Z",
    )
    registry.register_many([account], create_dirs=False)
    return registry.get(account_id)


def make_manager(tmp_path, registry, inspector=None, health=None, cdp=False):
    chrome = tmp_path / "chrome.exe"
    chrome.write_text("", encoding="utf-8")
    extension = tmp_path / "extension"
    extension.mkdir(exist_ok=True)
    launched = []

    def fake_popen(command, **kwargs):
        proc = FakeProcess(command, **kwargs)
        launched.append(proc)
        if inspector:
            inspector.alive.add(proc.pid)
            inspector.commands[proc.pid] = " ".join(str(part) for part in command)
        return proc

    manager = RuntimeManager(
        registry=registry,
        inspector=inspector or FakeInspector(),
        popen=fake_popen,
        chrome_path=chrome,
        extension_dir=extension,
        python_exe=tmp_path / "python.exe",
        log_dir=tmp_path / "logs" / "runtime",
    )
    manager._worker_health = lambda account: health or {}
    manager._tcp_reachable = lambda port: cdp
    manager.launched = launched
    return manager


def test_open_login_uses_registered_profile_cdp_and_extension(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    manager = make_manager(tmp_path, registry)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)

    result = manager.open_login("FLOW-005")

    command = manager.launched[0].command
    assert result.result == "opened"
    assert f"--user-data-dir={Path(account.profile_path)}" in command
    assert "--remote-debugging-port=9300" in command
    assert any(str(manager.extension_dir) in part for part in command if "--load-extension" in part)
    assert registry.get("FLOW-005").chrome_pid == manager.launched[0].pid


def test_open_login_preflight_errors(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry, enabled=False)
    manager = make_manager(tmp_path, registry)

    assert manager.open_login("FLOW-999").result == "account_not_found"
    assert manager.open_login("FLOW-005").result == "account_disabled"

    registry = make_registry(tmp_path / "missing")
    account = AccountRecord(
        "FLOW-005", "FLOW-005", str(tmp_path / "missing-profile"), 8101, 9200, 9300,
        str(tmp_path / "data.db"), str(tmp_path / "out"), True, "login_required", "now"
    )
    registry.register_many([account], create_dirs=False)
    assert make_manager(tmp_path, registry).open_login("FLOW-005").result == "profile_missing"


def test_open_login_chrome_missing_and_port_conflict(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    manager = RuntimeManager(registry=registry, chrome_path=tmp_path / "missing.exe")
    assert manager.open_login("FLOW-005").result == "chrome_not_found"

    manager = make_manager(tmp_path, registry)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: True)
    assert manager.open_login("FLOW-005").result == "port_conflict"


def test_open_login_already_running_is_idempotent(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=222)
    inspector = FakeInspector()
    inspector.alive.add(222)
    inspector.commands[222] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)

    assert manager.open_login("FLOW-005").result == "already_running"
    assert manager.launched == []


def test_start_one_starts_worker_then_chrome_and_checks_health(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    inspector = FakeInspector()
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=True)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)

    result = manager.start_one("FLOW-005")

    assert result.result == "started"
    assert manager.launched[0].command[1:3] == ["-m", "runtime.worker_entry"]
    assert "agent.main" not in manager.launched[0].command
    assert "--runtime-account-id" in manager.launched[0].command
    assert manager.launched[0].env["FLOW_ACCOUNT_ID"] == "FLOW-005"
    assert manager.launched[0].env["AGENT_API_PORT"] == "8101"
    assert "--remote-debugging-port=9300" in manager.launched[1].command


def test_start_one_rejects_external_port_conflict(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    manager = make_manager(tmp_path, registry)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port == 8101)

    result = manager.start_one("FLOW-005")

    assert result.result == "port_conflict"
    assert result.details["field"] == "worker_api_port"
    assert manager.launched == []


def test_start_one_compensates_on_extension_mismatch(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    inspector = FakeInspector()
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-006", "extension_connected": True}, cdp=True)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)

    result = manager.start_one("FLOW-005")

    assert result.result == "account_mismatch"
    assert len(inspector.terminated) == 2


def test_status_stopped_running_partial_and_stale_pid(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    manager = make_manager(tmp_path, registry)
    assert manager.status("FLOW-005").details["runtime_status"] == "stopped"

    registry.mark_started("FLOW-005", chrome_pid=11, worker_pid=12)
    inspector = FakeInspector()
    inspector.alive.update({11, 12})
    inspector.commands[11] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    inspector.commands[12] = "python -m runtime.worker_entry --runtime-account-id FLOW-005 --runtime-api-port 8101 --runtime-ws-port 9200"
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=True)
    assert manager.status("FLOW-005").details["runtime_status"] == "running"

    inspector.alive.remove(11)
    assert manager.status("FLOW-005").details["runtime_status"] == "partial"
    inspector.alive.clear()
    assert manager.status("FLOW-005").details["worker_process_alive"] is False


def test_pid_reuse_rejects_wrong_owner_and_stop_only_target(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=11, worker_pid=12)
    inspector = FakeInspector()
    inspector.alive.update({11, 12, 99})
    inspector.commands[11] = "chrome --user-data-dir=OTHER --remote-debugging-port=9999"
    inspector.commands[12] = "python -m runtime.worker_entry --runtime-account-id FLOW-006 --runtime-api-port 8101 --runtime-ws-port 9200"
    manager = make_manager(tmp_path, registry, inspector=inspector)

    assert manager.status("FLOW-005").details["runtime_status"] == "stopped"
    assert manager.stop_one("FLOW-005").result == "already_stopped"
    assert inspector.terminated == []


def test_stop_one_stops_owned_processes_and_is_idempotent(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=11, worker_pid=12)
    inspector = FakeInspector()
    inspector.alive.update({11, 12})
    inspector.commands[11] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    inspector.commands[12] = "python -m runtime.worker_entry --runtime-account-id FLOW-005 --runtime-api-port 8101 --runtime-ws-port 9200"
    manager = make_manager(tmp_path, registry, inspector=inspector)

    assert manager.stop_one("FLOW-005").result == "stopped"
    assert inspector.terminated == [12, 11]
    assert manager.stop_one("FLOW-005").result == "already_stopped"


def test_import_existing_safely_corrects_planned_only(tmp_path):
    registry = make_registry(tmp_path)
    planned = add_account(registry, "FLOW-001", status="planned")
    login_required = add_account(registry, "FLOW-005", status="login_required")
    (tmp_path / "workers.json").write_text(json.dumps([{"account_id": "FLOW-001", "api_url": "http://127.0.0.1:8100"}]), encoding="utf-8")

    registry.import_existing_workers(dry_run=False)

    assert registry.get("FLOW-001").status == "registered"
    assert registry.get("FLOW-005").status == "login_required"


def test_cli_runtime_commands_use_manager(tmp_path, monkeypatch, capsys):
    class FakeManager:
        def __init__(self, registry):
            pass

        def status(self, account_id):
            from runtime.process_manager import RuntimeResult
            return RuntimeResult("stopped", account_id, False)

    monkeypatch.setattr(cli, "RuntimeManager", FakeManager)
    assert cli.main(["status", "FLOW-005"]) == 0
    assert json.loads(capsys.readouterr().out)["result"] == "stopped"


def test_worker_entry_runs_agent_main_in_same_process_when_env_matches(monkeypatch):
    calls = []
    monkeypatch.setenv("FLOW_ACCOUNT_ID", "FLOW-005")
    monkeypatch.setenv("AGENT_API_PORT", "8101")
    monkeypatch.setenv("EXTENSION_WS_PORT", "9200")
    monkeypatch.setattr(worker_entry.runpy, "run_module", lambda *args, **kwargs: calls.append((args, kwargs)))

    code = worker_entry.main([
        "--runtime-account-id", "FLOW-005",
        "--runtime-api-port", "8101",
        "--runtime-ws-port", "9200",
    ])

    assert code == 0
    assert calls == [(("agent.main",), {"run_name": "__main__"})]
    assert worker_entry.sys.argv == ["agent.main"]


@pytest.mark.parametrize(
    ("env_name", "env_value", "expected_arg"),
    [
        ("FLOW_ACCOUNT_ID", "FLOW-006", "FLOW-005"),
        ("AGENT_API_PORT", "9999", "8101"),
        ("EXTENSION_WS_PORT", "9999", "9200"),
    ],
)
def test_worker_entry_rejects_env_mismatch(monkeypatch, env_name, env_value, expected_arg):
    monkeypatch.setenv("FLOW_ACCOUNT_ID", "FLOW-005")
    monkeypatch.setenv("AGENT_API_PORT", "8101")
    monkeypatch.setenv("EXTENSION_WS_PORT", "9200")
    monkeypatch.setenv(env_name, env_value)
    monkeypatch.setattr(worker_entry.runpy, "run_module", lambda *args, **kwargs: pytest.fail("agent.main should not run"))

    code = worker_entry.main([
        "--runtime-account-id", "FLOW-005",
        "--runtime-api-port", "8101",
        "--runtime-ws-port", "9200",
    ])

    assert code == 2


def test_worker_ownership_requires_worker_entry_account_api_and_ws(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", worker_pid=12)
    inspector = FakeInspector()
    inspector.alive.add(12)
    manager = make_manager(tmp_path, registry, inspector=inspector)

    inspector.commands[12] = "python -m runtime.worker_entry --runtime-account-id FLOW-005 --runtime-api-port 8101 --runtime-ws-port 9200"
    assert manager.status("FLOW-005").details["worker_process_alive"] is True

    inspector.commands[12] = "python -m runtime.worker_entry --runtime-account-id FLOW-006 --runtime-api-port 8101 --runtime-ws-port 9200"
    assert manager.status("FLOW-005").details["worker_process_alive"] is False

    inspector.commands[12] = "python -m agent.main --runtime-account-id FLOW-005 --runtime-api-port 8101 --runtime-ws-port 9200"
    assert manager.status("FLOW-005").details["worker_process_alive"] is False
