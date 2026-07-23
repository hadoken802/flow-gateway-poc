import json
import subprocess
import ctypes
from pathlib import Path

import pytest

from runtime import cli
from runtime import worker_entry
from runtime.process_manager import BOOL, DWORD, HANDLE, INVALID_HANDLE_VALUE, KERNEL32, PROCESSENTRY32W, ProcessInspector, RuntimeManager
from runtime.registry import AccountRecord, AccountRegistry


@pytest.fixture(autouse=True)
def default_free_bind_probe(monkeypatch):
    monkeypatch.setattr("runtime.process_manager.port_can_bind", lambda port: True)


class FakeProcess:
    next_pid = 1000

    def __init__(self, command, cwd=None, env=None, **_):
        FakeProcess.next_pid += 1
        self.pid = FakeProcess.next_pid
        self.command = command
        self.cwd = cwd
        self.env = env or {}
        self.kwargs = _


class FakeInspector:
    def __init__(self):
        self.commands = {}
        self.alive = set()
        self.terminated = []
        self.listeners = {}
        self.parents = {}

    def process_alive(self, pid):
        return pid in self.alive

    def command_line(self, pid):
        return self.commands.get(pid, "")

    def terminate(self, pid, timeout_seconds=8.0):
        self.terminated.append(pid)
        self.alive.discard(pid)
        for port, owner_pid in list(self.listeners.items()):
            if owner_pid == pid:
                del self.listeners[port]
        return True

    def listening_pid(self, port):
        return self.listeners.get(port)

    def parent_pid(self, pid):
        return self.parents.get(pid)


class ToolhelpInspector(ProcessInspector):
    def __init__(self, entries=None, snapshot=123):
        self.entries = entries or []
        self.snapshot = snapshot
        self.index = 0
        self.closed = []
        self.first_called = False

    def _create_process_snapshot(self):
        return self.snapshot

    def _process_first(self, snapshot, entry):
        self.first_called = True
        self.index = 0
        if not self.entries:
            return False
        self._copy_entry(entry, self.entries[0])
        return True

    def _process_next(self, snapshot, entry):
        self.index += 1
        if self.index >= len(self.entries):
            return False
        self._copy_entry(entry, self.entries[self.index])
        return True

    def _close_handle(self, snapshot):
        self.closed.append(snapshot)

    def _copy_entry(self, entry, pair):
        pid, parent = pair
        entry.th32ProcessID = pid
        entry.th32ParentProcessID = parent


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
    tmp_path.mkdir(parents=True, exist_ok=True)
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


def add_running_worker(inspector, pid, account_id="FLOW-005", api_port=8101, ws_port=9200):
    inspector.alive.add(pid)
    inspector.commands[pid] = f"python -m runtime.worker_entry --runtime-account-id {account_id} --runtime-api-port {api_port} --runtime-ws-port {ws_port}"


def test_parent_pid_uses_toolhelp_not_wmic(monkeypatch):
    calls = []
    monkeypatch.setattr("runtime.process_manager.subprocess.run", lambda *args, **kwargs: calls.append(args) or pytest.fail("parent_pid must not spawn subprocess"))
    inspector = ToolhelpInspector(entries=[(100, 50), (200, 100)])

    assert inspector.parent_pid(200) == 100
    assert calls == []
    assert inspector.closed == [123]


def test_toolhelp_api_signatures_use_pointer_sized_handle():
    assert KERNEL32.CreateToolhelp32Snapshot.argtypes == [DWORD, DWORD]
    assert KERNEL32.CreateToolhelp32Snapshot.restype is HANDLE
    assert KERNEL32.Process32FirstW.argtypes == [HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    assert KERNEL32.Process32FirstW.restype is BOOL
    assert KERNEL32.Process32NextW.argtypes == [HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    assert KERNEL32.Process32NextW.restype is BOOL
    assert KERNEL32.CloseHandle.argtypes == [HANDLE]
    assert KERNEL32.CloseHandle.restype is BOOL
    assert HANDLE is ctypes.c_void_p


def test_command_line_embeds_int_pid_without_args(monkeypatch):
    seen = {}

    def fake_run(command, **kwargs):
        seen["command"] = command
        return subprocess.CompletedProcess(command, 0, stdout='"C:\\Chrome\\chrome.exe" --remote-debugging-port=9300\r\n', stderr="")

    monkeypatch.setattr("runtime.process_manager.subprocess.run", fake_run)

    assert ProcessInspector().command_line("36836") == '"C:\\Chrome\\chrome.exe" --remote-debugging-port=9300'
    script = seen["command"][4]
    assert "$args[0]" not in script
    assert "ProcessId=36836" in script
    assert seen["command"][0] == "powershell.exe"
    assert "-NoProfile" in seen["command"]
    assert "-NonInteractive" in seen["command"]


def test_command_line_returns_empty_on_failure_timeout_or_missing(monkeypatch):
    monkeypatch.setattr(
        "runtime.process_manager.subprocess.run",
        lambda command, **kwargs: subprocess.CompletedProcess(command, 1, stdout="36836", stderr="error"),
    )
    assert ProcessInspector().command_line(36836) == ""

    def raise_timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("powershell.exe", 3)

    monkeypatch.setattr("runtime.process_manager.subprocess.run", raise_timeout)
    assert ProcessInspector().command_line(36836) == ""


def test_parent_pid_returns_none_when_target_missing_and_closes_handle():
    inspector = ToolhelpInspector(entries=[(100, 50), (200, 100)])

    assert inspector.parent_pid(999) is None
    assert inspector.closed == [123]


def test_parent_pid_snapshot_failure_returns_none_without_close():
    inspector = ToolhelpInspector(entries=[(100, 50)], snapshot=INVALID_HANDLE_VALUE)

    assert inspector.parent_pid(100) is None
    assert inspector.closed == []


def test_parent_pid_process_first_failure_closes_handle():
    inspector = ToolhelpInspector(entries=[])

    assert inspector.parent_pid(100) is None
    assert inspector.first_called is True
    assert inspector.closed == [123]


def test_open_login_uses_registered_profile_and_cdp_without_extension_flags(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    manager = make_manager(tmp_path, registry)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)

    result = manager.open_login("FLOW-005")

    command = manager.launched[0].command
    assert result.result == "opened"
    assert f"--user-data-dir={Path(account.profile_path)}" in command
    assert "--remote-debugging-port=9300" in command
    assert not any("--load-extension" in part for part in command)
    assert not any("--disable-extensions-except" in part for part in command)
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


def test_os_port_bind_failure_returns_conflict(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    manager = make_manager(tmp_path, registry)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)
    monkeypatch.setattr("runtime.process_manager.port_can_bind", lambda port: False)

    assert manager.open_login("FLOW-005").result == "port_conflict"


def test_registered_self_ports_do_not_create_conflict_when_os_ports_are_free(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    add_account(registry, "FLOW-006")
    manager = make_manager(tmp_path, registry)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)

    assert manager.open_login("FLOW-005").result == "opened"

    manager = make_manager(tmp_path / "start", registry, health={"account_id": "FLOW-005", "extension_connected": False}, cdp=False)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)
    assert manager.start_one("FLOW-005").result == "extension_not_connected"


def test_other_account_registered_port_conflicts_only_when_os_listens(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    add_account(registry, "FLOW-006")
    manager = make_manager(tmp_path, registry)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port == 9304)

    assert manager.open_login("FLOW-005").result == "opened"


def test_other_account_registered_same_target_port_returns_conflict(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    other = AccountRecord(
        account_id="FLOW-006",
        display_name="FLOW-006",
        profile_path=str(tmp_path / "profiles" / "FLOW-006"),
        worker_api_port=8102,
        extension_ws_port=9201,
        chrome_cdp_port=9300,
        database_path=str(tmp_path / "data" / "FLOW-006.db"),
        output_dir=str(tmp_path / "outputs" / "FLOW-006"),
    )
    monkeypatch.setattr(registry, "list_accounts", lambda: [account, other])
    manager = make_manager(tmp_path, registry)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)

    result = manager.open_login("FLOW-005")

    assert result.result == "port_conflict"
    assert result.details["owner"] == "FLOW-006"


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


def test_stale_chrome_pid_is_repaired_from_cdp_listener(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=35036)
    inspector = FakeInspector()
    inspector.listeners[9300] = 36836
    inspector.alive.add(36836)
    inspector.commands[36836] = f'"C:/Program Files/Google/Chrome/Application/chrome.exe" --user-data-dir={account.profile_path} --remote-debugging-port=9300 --no-first-run'
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=True)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port == 9300)

    status = manager.status("FLOW-005")

    assert status.details["runtime_status"] == "partial"
    assert status.details["chrome_process_alive"] is True
    assert registry.get("FLOW-005").chrome_pid == 36836
    assert manager.open_login("FLOW-005").result == "already_running"


def test_status_running_after_repair_when_worker_is_healthy(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=35036, worker_pid=352)
    inspector = FakeInspector()
    inspector.listeners[9300] = 36836
    inspector.alive.update({36836, 352})
    inspector.commands[36836] = f'chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300'
    inspector.commands[352] = "python -m runtime.worker_entry --runtime-account-id FLOW-005 --runtime-api-port 8101 --runtime-ws-port 9200"
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=True)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port == 9300)

    status = manager.status("FLOW-005")

    assert status.details["runtime_status"] == "running"
    assert status.details["chrome_pid"] == 36836


def test_worker_service_pid_repairs_from_api_and_ws_listeners(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=36836, worker_pid=352)
    inspector = FakeInspector()
    inspector.listeners[8101] = 38820
    inspector.listeners[9200] = 38820
    inspector.alive.update({36836, 352, 38820})
    inspector.commands[36836] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    add_running_worker(inspector, 352)
    add_running_worker(inspector, 38820)
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=True)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in {8101, 9200, 9300})

    status = manager.status("FLOW-005")

    assert status.details["runtime_status"] == "running"
    assert status.details["worker_pid"] == 38820
    assert registry.get("FLOW-005").worker_pid == 38820


def test_worker_service_pid_takes_priority_over_live_launcher_pid(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=36836, worker_pid=352)
    inspector = FakeInspector()
    inspector.listeners[9300] = 36836
    inspector.listeners[8101] = 38820
    inspector.listeners[9200] = 38820
    inspector.parents[38820] = 352
    inspector.alive.update({36836, 352, 38820})
    inspector.commands[36836] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    add_running_worker(inspector, 352)
    add_running_worker(inspector, 38820)
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=True)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in {8101, 9200, 9300})

    status = manager.status("FLOW-005")

    assert status.details["runtime_status"] == "running"
    assert status.details["worker_pid"] == 38820
    assert registry.get("FLOW-005").worker_pid == 38820


def test_worker_launcher_pid_is_temporary_when_ports_are_not_listening(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", worker_pid=352)
    inspector = FakeInspector()
    add_running_worker(inspector, 352)
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)

    status = manager.status("FLOW-005")

    assert status.details["worker_process_alive"] is True
    assert status.details["worker_pid"] == 352
    assert registry.get("FLOW-005").worker_pid == 352


def test_worker_service_pid_rejects_different_api_and_ws_pids(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", worker_pid=352)
    inspector = FakeInspector()
    inspector.listeners[8101] = 38820
    inspector.listeners[9200] = 38821
    inspector.alive.update({38820, 38821})
    add_running_worker(inspector, 38820)
    add_running_worker(inspector, 38821)
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=False)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in {8101, 9200})

    assert manager.status("FLOW-005").details["worker_process_alive"] is False
    assert registry.get("FLOW-005").worker_pid == 352


@pytest.mark.parametrize(
    "command",
    [
        "python -m runtime.worker_entry --runtime-account-id FLOW-006 --runtime-api-port 8101 --runtime-ws-port 9200",
        "python -m runtime.worker_entry --runtime-account-id FLOW-005 --runtime-api-port 9999 --runtime-ws-port 9200",
        "python -m runtime.worker_entry --runtime-account-id FLOW-005 --runtime-api-port 8101 --runtime-ws-port 9999",
    ],
)
def test_worker_service_pid_rejects_wrong_account_or_ports(tmp_path, monkeypatch, command):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", worker_pid=352)
    inspector = FakeInspector()
    inspector.listeners[8101] = 38820
    inspector.listeners[9200] = 38820
    inspector.alive.add(38820)
    inspector.commands[38820] = command
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=False)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in {8101, 9200})

    assert manager.status("FLOW-005").details["worker_process_alive"] is False


def test_worker_listener_child_pid_repairs_to_matching_parent(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", worker_pid=352)
    inspector = FakeInspector()
    inspector.listeners[8101] = 40001
    inspector.listeners[9200] = 40001
    inspector.parents[40001] = 38820
    inspector.alive.update({40001, 38820})
    inspector.commands[40001] = "python child-helper"
    add_running_worker(inspector, 38820)
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=False)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in {8101, 9200})

    manager.status("FLOW-005")

    assert registry.get("FLOW-005").worker_pid == 38820


def test_chrome_listener_child_pid_repairs_to_matching_parent(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=35036, worker_pid=352)
    inspector = FakeInspector()
    inspector.listeners[9300] = 40001
    inspector.parents[40001] = 36836
    inspector.alive.update({40001, 36836, 352})
    inspector.commands[40001] = "chrome --type=renderer"
    inspector.commands[36836] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    add_running_worker(inspector, 352)
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=True)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port == 9300)

    status = manager.status("FLOW-005")

    assert status.details["runtime_status"] == "running"
    assert registry.get("FLOW-005").chrome_pid == 36836


def test_chrome_parent_other_profile_is_not_owned(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=35036)
    inspector = FakeInspector()
    inspector.listeners[9300] = 40001
    inspector.parents[40001] = 36836
    inspector.alive.update({40001, 36836})
    inspector.commands[40001] = "chrome --type=renderer"
    inspector.commands[36836] = "chrome --user-data-dir=D:/other/profile --remote-debugging-port=9300"
    manager = make_manager(tmp_path, registry, inspector=inspector, cdp=True)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port == 9300)

    assert manager.status("FLOW-005").details["chrome_process_alive"] is False
    assert registry.get("FLOW-005").chrome_pid == 35036


def test_chrome_parent_cycle_or_missing_stops_safely(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=35036)
    inspector = FakeInspector()
    inspector.listeners[9300] = 40001
    inspector.parents[40001] = 40002
    inspector.parents[40002] = 40001
    inspector.alive.update({40001, 40002})
    inspector.commands[40001] = "chrome --type=renderer"
    inspector.commands[40002] = "chrome --type=gpu"
    manager = make_manager(tmp_path, registry, inspector=inspector, cdp=True)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port == 9300)

    assert manager.status("FLOW-005").details["chrome_process_alive"] is False

    inspector.parents[40001] = 49999
    inspector.alive.discard(49999)
    assert manager.status("FLOW-005").details["chrome_process_alive"] is False


def test_stale_pid_with_free_cdp_port_allows_open_login(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=222)
    inspector = FakeInspector()
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)

    assert manager.open_login("FLOW-005").result == "opened"


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


def test_start_one_reuses_verified_chrome_listener_and_redirects_worker_logs(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=35036)
    inspector = FakeInspector()
    inspector.listeners[9300] = 36836
    inspector.alive.add(36836)
    inspector.commands[36836] = f'chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300'
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=True)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port == 9300)

    result = manager.start_one("FLOW-005")

    assert result.result == "started"
    assert len(manager.launched) == 1
    assert manager.launched[0].command[1:3] == ["-m", "runtime.worker_entry"]
    assert manager.launched[0].env["FLOW_ACCOUNT_ID"] == "FLOW-005"
    assert manager.launched[0].kwargs["stdout"].name.endswith("FLOW-005-worker.log")
    assert manager.launched[0].kwargs["stdout"].closed is True
    assert manager.launched[0].kwargs["stderr"] is subprocess.STDOUT
    assert manager.launched[0].kwargs["stdin"] is subprocess.DEVNULL
    assert registry.get("FLOW-005").chrome_pid == 36836


def test_start_one_reuses_existing_worker_service_pid(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=36836, worker_pid=352)
    inspector = FakeInspector()
    inspector.alive.update({36836, 38820})
    inspector.commands[36836] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    add_running_worker(inspector, 38820)
    inspector.listeners[8101] = 38820
    inspector.listeners[9200] = 38820
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=True)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in {8101, 9200, 9300})

    result = manager.start_one("FLOW-005")

    assert result.result == "already_running"
    assert manager.launched == []
    assert registry.get("FLOW-005").worker_pid == 38820


def test_start_one_records_service_pid_after_worker_health(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=36836)
    inspector = FakeInspector()
    inspector.listeners[9300] = 36836
    inspector.alive.add(36836)
    inspector.commands[36836] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": True}, cdp=True)
    original_popen = manager.popen

    def fake_popen(command, **kwargs):
        proc = original_popen(command, **kwargs)
        if command[1:3] == ["-m", "runtime.worker_entry"]:
            inspector.listeners[8101] = 38820
            inspector.listeners[9200] = 38820
            inspector.parents[38820] = proc.pid
            add_running_worker(inspector, 38820)
        return proc

    manager.popen = fake_popen
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    result = manager.start_one("FLOW-005")

    assert result.result == "started"
    assert len(manager.launched) == 1
    assert manager.launched[0].pid != 38820
    assert registry.get("FLOW-005").worker_pid == 38820


def test_worker_logs_are_account_specific(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    add_account(registry, "FLOW-006")
    manager = make_manager(tmp_path, registry)
    flow5 = manager._worker_log_file(registry.get("FLOW-005"))
    flow6 = manager._worker_log_file(registry.get("FLOW-006"))

    assert flow5.name == "FLOW-005-worker.log"
    assert flow6.name == "FLOW-006-worker.log"
    assert flow5 != flow6


def test_worker_log_handle_closes_when_popen_raises(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    opened = []

    class TrackingHandle:
        name = str(tmp_path / "logs" / "runtime" / "FLOW-005-worker.log")
        closed = False

        def close(self):
            self.closed = True

    class TrackingPath:
        def open(self, *args, **kwargs):
            handle = TrackingHandle()
            opened.append(handle)
            return handle

    manager = make_manager(tmp_path, registry)
    monkeypatch.setattr(manager, "_worker_log_file", lambda account: TrackingPath())
    manager.popen = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("popen failed"))
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)

    result = manager.start_one("FLOW-005")

    assert result.result == "failed"
    assert opened[0].closed is True


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


def test_stop_one_stops_owned_processes_and_is_idempotent(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=11, worker_pid=12)
    inspector = FakeInspector()
    inspector.alive.update({11, 12})
    inspector.commands[11] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    inspector.commands[12] = "python -m runtime.worker_entry --runtime-account-id FLOW-005 --runtime-api-port 8101 --runtime-ws-port 9200"
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    assert manager.stop_one("FLOW-005").result == "stopped"
    assert inspector.terminated == [12, 11]
    assert manager.stop_one("FLOW-005").result == "already_stopped"


def test_stop_one_uses_repaired_chrome_pid(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=35036)
    inspector = FakeInspector()
    inspector.listeners[9300] = 36836
    inspector.alive.add(36836)
    inspector.commands[36836] = f'chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300'
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port == 9300)

    assert manager.stop_one("FLOW-005").result == "stopped"
    assert inspector.terminated == [36836]


def test_stop_one_uses_repaired_worker_service_pid(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", worker_pid=352)
    inspector = FakeInspector()
    inspector.listeners[8101] = 38820
    inspector.listeners[9200] = 38820
    inspector.alive.add(38820)
    add_running_worker(inspector, 38820)
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    assert manager.stop_one("FLOW-005").result == "stopped"
    assert inspector.terminated == [38820]


def test_stop_one_stops_service_pid_before_verified_launcher_pid(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", worker_pid=352)
    inspector = FakeInspector()
    inspector.listeners[8101] = 38820
    inspector.listeners[9200] = 38820
    inspector.parents[38820] = 352
    inspector.alive.update({352, 38820})
    add_running_worker(inspector, 352)
    add_running_worker(inspector, 38820)
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    assert manager.stop_one("FLOW-005").result == "stopped"
    assert inspector.terminated == [38820, 352]


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
