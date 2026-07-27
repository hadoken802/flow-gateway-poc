import json
import subprocess
import ctypes
from pathlib import Path

import pytest

from runtime import cli
from runtime.ownership import (
    canonical_payload,
    create_runtime_identity,
    generate_challenge,
    sign_challenge,
    verify_challenge_response,
)
from runtime import worker_entry
from runtime.process_manager import BOOL, DWORD, HANDLE, INVALID_HANDLE_VALUE, KERNEL32, MANAGED_CFT_CHROME, PROCESSENTRY32W, ProcessInspector, ProcessProbeResult, RuntimeManager, TerminationResult
from runtime.registry import AccountRecord, AccountRegistry


def write_extension_manifest(extension_dir):
    extension_dir.mkdir(parents=True, exist_ok=True)
    (extension_dir / "manifest.json").write_text('{"manifest_version":3,"name":"Test Extension","version":"1.0"}', encoding="utf-8")


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


class FakeSecretProtector:
    def protect(self, data: bytes) -> bytes:
        return b"protected:" + data

    def unprotect(self, data: bytes) -> bytes:
        assert data.startswith(b"protected:")
        return data.removeprefix(b"protected:")


class FakeInspector:
    def __init__(self):
        self.commands = {}
        self.alive = set()
        self.probe_status = {}
        self.command_status = {}
        self.terminated = []
        self.listeners = {}
        self.parents = {}
        self.fail_terminate = set()
        self.keep_listeners = set()

    def probe_process(self, pid):
        if pid in self.probe_status:
            return ProcessProbeResult(pid=pid, alive=self.probe_status[pid] == "alive", status=self.probe_status[pid], method="fake")
        return ProcessProbeResult(pid=pid, alive=pid in self.alive, status="alive" if pid in self.alive else "not_found", method="fake")

    def process_alive(self, pid):
        return self.probe_process(pid).alive

    def command_line(self, pid):
        return self.commands.get(pid, "")

    def command_line_probe(self, pid):
        from runtime.process_manager import CommandLineProbeResult

        status = self.command_status.get(pid)
        if status:
            return CommandLineProbeResult(pid=pid, command_line="", status=status)
        command = self.commands.get(pid, "")
        return CommandLineProbeResult(pid=pid, command_line=command, status="available" if command else "cim_empty")

    def terminate(self, pid, timeout_seconds=8.0, should_force=None):
        self.terminated.append(pid)
        if pid in self.fail_terminate:
            return TerminationResult(
                success=False,
                pid=pid,
                graceful_attempted=True,
                graceful_returncode=1,
                graceful_stderr="simulated graceful failure",
                forced_attempted=True,
                forced_returncode=1,
                forced_stderr="simulated forced failure",
                process_alive_after=True,
            )
        killed = {pid}
        for child_pid in list(self.alive):
            current = child_pid
            seen = set()
            while current and current not in seen:
                seen.add(current)
                current = self.parents.get(current)
                if current == pid:
                    killed.add(child_pid)
                    break
        self.alive.difference_update(killed)
        for port, owner_pid in list(self.listeners.items()):
            if owner_pid in killed and port not in self.keep_listeners:
                del self.listeners[port]
        return TerminationResult(success=True, pid=pid, graceful_attempted=True, graceful_returncode=0, process_alive_after=False)

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
        "FLOW-012": (8108, 9206, 9310),
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


def test_start_worker_only_uses_account_env_and_does_not_launch_chrome(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-005")
    Path(account.profile_path).mkdir(parents=True, exist_ok=True)
    extension_dir = tmp_path / "extension"
    write_extension_manifest(extension_dir)
    inspector = FakeInspector()
    launched = []

    def fake_popen(command, **kwargs):
        proc = FakeProcess(command, **kwargs)
        launched.append(proc)
        return proc

    manager = RuntimeManager(
        registry,
        inspector=inspector,
        popen=fake_popen,
        chrome_path=tmp_path / "chrome.exe",
        extension_dir=extension_dir,
        ownership_protector=FakeSecretProtector(),
        log_dir=tmp_path / "logs" / "runtime",
    )
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)

    result = manager.start_worker_only("FLOW-005")

    assert result.result == "started"
    assert len(launched) == 1
    assert "-m" in launched[0].command
    assert "runtime.worker_entry" in launched[0].command
    assert not any("chrome" in str(part).lower() for part in launched[0].command)
    assert not any("labs.google" in str(part) for part in launched[0].command)
    assert launched[0].env["FLOW_ACCOUNT_ID"] == "FLOW-005"
    assert launched[0].env["AGENT_API_PORT"] == "8101"
    assert launched[0].env["EXTENSION_WS_PORT"] == "9200"
    assert launched[0].env["FLOW_DB_PATH"] == account.database_path
    assert launched[0].env["OUTPUT_DIR"] == account.output_dir
    assert launched[0].env["FLOW_RUNTIME_INSTANCE_ID"]
    assert launched[0].env["FLOW_RUNTIME_OWNERSHIP_SECRET"]
    assert launched[0].env["FLOW_RUNTIME_OWNERSHIP_VERSION"] == "1"
    assert launched[0].kwargs["stdin"] == subprocess.DEVNULL
    assert launched[0].kwargs["stderr"] == subprocess.STDOUT
    stored = registry.get("FLOW-005")
    assert stored.runtime_instance_id == launched[0].env["FLOW_RUNTIME_INSTANCE_ID"]
    assert stored.runtime_secret_ref
    assert stored.runtime_secret_fingerprint
    assert stored.runtime_ownership_version == 1


def make_manager(tmp_path, registry, inspector=None, health=None, cdp=False, health_sequence=None, chrome_path_marker="default"):
    tmp_path.mkdir(parents=True, exist_ok=True)
    chrome = tmp_path / "chrome.exe"
    if chrome_path_marker == "default":
        chrome.write_text("", encoding="utf-8")
        chrome_path = chrome
    else:
        chrome_path = chrome_path_marker
    extension = tmp_path / "extension"
    write_extension_manifest(extension)
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
        chrome_path=chrome_path,
        extension_dir=extension,
        python_exe=tmp_path / "python.exe",
        log_dir=tmp_path / "logs" / "runtime",
        ownership_protector=FakeSecretProtector(),
    )
    sequence = list(health_sequence or [])
    manager._worker_health = lambda account: sequence.pop(0) if sequence else (health or {})
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


def test_process_alive_uses_get_process_when_tasklist_is_denied(monkeypatch):
    def fake_run(command, **kwargs):
        if command[0] == "powershell.exe":
            return subprocess.CompletedProcess(command, 0, stdout="FOUND\r\n", stderr="")
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="ERROR: Access denied")

    monkeypatch.setattr("runtime.process_manager.subprocess.run", fake_run)

    probe = ProcessInspector().probe_process(35088)

    assert probe.alive is True
    assert probe.status == "alive"
    assert probe.method == "get-process"
    assert ProcessInspector().process_alive(35088) is True


def test_process_probe_unknown_when_all_methods_are_denied(monkeypatch):
    def fake_run(command, **kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="ERROR: Access denied")

    monkeypatch.setattr("runtime.process_manager.subprocess.run", fake_run)

    probe = ProcessInspector().probe_process(35088)

    assert probe.alive is None
    assert probe.status == "access_denied"
    assert ProcessInspector().process_alive(35088) is False


def test_process_probe_not_found_when_get_process_is_empty(monkeypatch):
    def fake_run(command, **kwargs):
        if command[0] == "powershell.exe":
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="INFO: No tasks are running which match the specified criteria.", stderr="")

    monkeypatch.setattr("runtime.process_manager.subprocess.run", fake_run)

    probe = ProcessInspector().probe_process(99999)

    assert probe.alive is False
    assert probe.status == "not_found"


def test_command_line_probe_reports_cim_empty_without_faking_mismatch(monkeypatch):
    monkeypatch.setattr(
        "runtime.process_manager.subprocess.run",
        lambda command, **kwargs: subprocess.CompletedProcess(command, 0, stdout="", stderr=""),
    )

    probe = ProcessInspector().command_line_probe(33036)

    assert probe.command_line == ""
    assert probe.status == "cim_empty"


def test_terminate_uses_graceful_taskkill_without_force_first(monkeypatch):
    calls = []
    alive = {38820}

    def fake_run(command, **kwargs):
        calls.append(command)
        alive.clear()
        return subprocess.CompletedProcess(command, 0, stdout="SUCCESS", stderr="")

    inspector = ProcessInspector()
    monkeypatch.setattr("runtime.process_manager.subprocess.run", fake_run)
    monkeypatch.setattr(inspector, "process_alive", lambda pid: int(pid) in alive)

    result = inspector.terminate(38820)

    assert result.success is True
    assert calls == [["taskkill", "/PID", "38820", "/T"]]
    assert "/IM" not in calls[0]
    assert result.forced_attempted is False


def test_terminate_forces_after_graceful_failure(monkeypatch):
    calls = []
    alive = {38820}

    def fake_run(command, **kwargs):
        calls.append(command)
        if "/F" in command:
            alive.clear()
            return subprocess.CompletedProcess(command, 0, stdout="FORCED", stderr="")
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="Access is denied.")

    inspector = ProcessInspector()
    monkeypatch.setattr("runtime.process_manager.subprocess.run", fake_run)
    monkeypatch.setattr(inspector, "process_alive", lambda pid: int(pid) in alive)

    result = inspector.terminate(38820)

    assert result.success is True
    assert calls == [["taskkill", "/PID", "38820", "/T"], ["taskkill", "/PID", "38820", "/T", "/F"]]
    assert result.graceful_returncode == 1
    assert result.graceful_stderr == "Access is denied."
    assert result.forced_returncode == 0


def test_terminate_forces_when_graceful_succeeds_but_process_remains(monkeypatch):
    calls = []
    alive_checks = [True, True, False]

    def fake_run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    inspector = ProcessInspector()
    monkeypatch.setattr("runtime.process_manager.subprocess.run", fake_run)
    monkeypatch.setattr(inspector, "process_alive", lambda pid: alive_checks.pop(0) if alive_checks else False)

    result = inspector.terminate(38820)

    assert result.success is True
    assert calls[-1] == ["taskkill", "/PID", "38820", "/T", "/F"]


def test_terminate_forces_when_ports_still_listen_after_graceful(monkeypatch):
    calls = []
    alive_checks = [True, False, False]

    def fake_run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    inspector = ProcessInspector()
    monkeypatch.setattr("runtime.process_manager.subprocess.run", fake_run)
    monkeypatch.setattr(inspector, "process_alive", lambda pid: alive_checks.pop(0) if alive_checks else False)

    result = inspector.terminate(38820, should_force=lambda: True)

    assert result.success is True
    assert calls == [["taskkill", "/PID", "38820", "/T"], ["taskkill", "/PID", "38820", "/T", "/F"]]


def test_terminate_reports_forced_failure_details(monkeypatch):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 1, stdout="not found", stderr="failed")

    inspector = ProcessInspector()
    monkeypatch.setattr("runtime.process_manager.subprocess.run", fake_run)
    monkeypatch.setattr(inspector, "process_alive", lambda pid: True)

    result = inspector.terminate(38820)

    assert result.success is False
    assert result.forced_attempted is True
    assert result.forced_returncode == 1
    assert result.process_alive_after is True


def test_terminate_treats_already_gone_process_as_success(monkeypatch):
    calls = []
    inspector = ProcessInspector()
    monkeypatch.setattr("runtime.process_manager.subprocess.run", lambda *args, **kwargs: calls.append(args) or pytest.fail("taskkill should not run"))
    monkeypatch.setattr(inspector, "process_alive", lambda pid: False)

    result = inspector.terminate(38820)

    assert result.success is True
    assert result.graceful_attempted is False
    assert calls == []


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


def test_open_login_uses_registered_profile_cdp_and_extension_flags(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    manager = make_manager(tmp_path, registry)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)

    result = manager.open_login("FLOW-005")

    command = manager.launched[0].command
    assert result.result == "opened"
    assert f"--user-data-dir={Path(account.profile_path)}" in command
    assert "--remote-debugging-port=9300" in command
    assert "--disable-skia-graphite" in command
    assert "--disable-gpu" in command
    assert "--no-sandbox" in command
    assert len([part for part in command if str(part).startswith("--load-extension=")]) == 1
    assert len([part for part in command if str(part).startswith("--disable-extensions-except=")]) == 1
    assert registry.get("FLOW-005").chrome_pid == manager.launched[0].pid


def test_runtime_chrome_command_loads_extra_extensions_from_env(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-005")
    manager = make_manager(tmp_path, registry)
    extra = tmp_path / "cookie-reset"
    write_extension_manifest(extra)
    monkeypatch.setenv("FLOW_EXTRA_EXTENSION_DIRS", str(extra))

    command = manager.chrome_command(account)
    load_arg = next(part for part in command if str(part).startswith("--load-extension="))
    except_arg = next(part for part in command if str(part).startswith("--disable-extensions-except="))
    load_dirs = load_arg.split("=", 1)[1].split(",")
    except_dirs = except_arg.split("=", 1)[1].split(",")

    assert load_dirs == except_dirs
    assert str(manager.extension_dir.resolve()) in load_dirs
    assert str(extra.resolve()) in load_dirs
    assert len(load_dirs) == 2
    assert f"--user-data-dir={Path(account.profile_path)}" in command


def test_runtime_chrome_command_dedupes_extra_extension_dirs(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-005")
    manager = make_manager(tmp_path, registry)
    extra = tmp_path / "extension"
    monkeypatch.setenv("FLOW_EXTRA_EXTENSION_DIRS", str(extra))

    command = manager.chrome_command(account)
    load_arg = next(part for part in command if str(part).startswith("--load-extension="))
    assert load_arg.split("=", 1)[1].split(",") == [str(manager.extension_dir.resolve())]


def test_runtime_preflight_fails_when_extra_extension_manifest_missing(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-005")
    manager = make_manager(tmp_path, registry)
    missing = tmp_path / "missing-extension"
    missing.mkdir()
    monkeypatch.setenv("FLOW_EXTRA_EXTENSION_DIRS", str(missing))

    result = manager.open_login(account.account_id)

    assert result.ok is False
    assert result.error == "extension_manifest_missing"
    assert not manager.launched


def test_runtime_accounts_keep_distinct_user_data_dirs_with_extra_extensions(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    first = add_account(registry, "FLOW-005")
    second = add_account(registry, "FLOW-006")
    manager = make_manager(tmp_path, registry)
    extra = tmp_path / "cookie-reset"
    write_extension_manifest(extra)
    monkeypatch.setenv("FLOW_EXTRA_EXTENSION_DIRS", str(extra))

    first_command = manager.chrome_command(first)
    second_command = manager.chrome_command(second)

    assert f"--user-data-dir={Path(first.profile_path)}" in first_command
    assert f"--user-data-dir={Path(second.profile_path)}" in second_command
    assert Path(first.profile_path) != Path(second.profile_path)


def test_runtime_manager_does_not_fallback_to_system_chrome_when_cft_missing(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    manager = RuntimeManager(registry=registry, chrome_path=tmp_path / "missing.exe", ownership_protector=FakeSecretProtector())

    result = manager.open_login(account.account_id)

    assert result.result == "cft_browser_not_configured"
    assert result.details["cft_required"] is True


def test_runtime_manager_prefers_env_chrome_executable(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    env_chrome = tmp_path / "cft" / "chrome.exe"
    env_chrome.parent.mkdir(parents=True)
    env_chrome.write_text("", encoding="utf-8")
    monkeypatch.setenv("FLOWKIT_CHROME_EXECUTABLE", str(env_chrome))
    manager = make_manager(tmp_path / "env", registry, chrome_path_marker=None)

    command = manager.chrome_command(account)

    assert command[0] == str(env_chrome)


def test_runtime_manager_uses_managed_cft_when_available(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    monkeypatch.delenv("FLOWKIT_CHROME_EXECUTABLE", raising=False)
    manager = RuntimeManager(registry=registry, ownership_protector=FakeSecretProtector())

    command = manager.chrome_command(account)

    assert command[0] == str(MANAGED_CFT_CHROME)


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
    assert manager.open_login("FLOW-005").result == "cft_browser_not_configured"

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


def test_status_running_when_services_healthy_but_ownership_unknown(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry, account_id="FLOW-012")
    registry.mark_started("FLOW-012", chrome_pid=33036, worker_pid=35088)
    inspector = FakeInspector()
    inspector.listeners[9310] = 33036
    inspector.listeners[8108] = 35088
    inspector.listeners[9206] = 35088
    inspector.probe_status[33036] = "alive"
    inspector.probe_status[35088] = "alive"
    inspector.command_status[33036] = "cim_empty"
    inspector.command_status[35088] = "cim_empty"
    manager = make_manager(
        tmp_path,
        registry,
        inspector=inspector,
        health={"account_id": "FLOW-012", "extension_connected": True},
        cdp=True,
    )
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in {8108, 9206, 9310})

    status = manager.status("FLOW-012")

    assert status.result == "running"
    assert status.details["runtime_status"] == "running"
    assert status.details["runtime_healthy"] is True
    assert status.details["chrome_process_alive"] is True
    assert status.details["worker_process_alive"] is True
    assert status.details["chrome_process_probe_status"] == "alive"
    assert status.details["worker_process_probe_status"] == "alive"
    assert status.details["chrome_ownership_verified"] is False
    assert status.details["worker_ownership_verified"] is False
    assert status.details["ownership_status"] == "legacy_unverified"
    assert status.details["stop_safe"] is False
    assert manager.stop_one("FLOW-012").result == "ownership_not_verified"
    assert inspector.terminated == []


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
    assert "--disable-skia-graphite" in manager.launched[1].command
    assert len([part for part in manager.launched[1].command if str(part).startswith("--load-extension=")]) == 1
    assert len([part for part in manager.launched[1].command if str(part).startswith("--disable-extensions-except=")]) == 1


def test_start_one_waits_for_delayed_extension_connection(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    inspector = FakeInspector()
    manager = make_manager(
        tmp_path,
        registry,
        inspector=inspector,
        health_sequence=[
            {},
            {"account_id": "FLOW-005", "extension_connected": False},
            {"account_id": "FLOW-005", "extension_connected": False},
            {"account_id": "FLOW-005", "extension_connected": True},
        ],
        cdp=True,
    )
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)
    monkeypatch.setattr("runtime.process_manager.time.sleep", lambda _: None)

    result = manager.start_one("FLOW-005")

    assert result.result == "started"
    assert result.details["startup_extension_wait_attempts"] == 3
    assert inspector.terminated == []


def test_start_one_compensates_only_after_extension_wait_timeout(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    inspector = FakeInspector()
    manager = make_manager(tmp_path, registry, inspector=inspector, health={"account_id": "FLOW-005", "extension_connected": False}, cdp=True)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: False)
    monkeypatch.setattr("runtime.process_manager.time.sleep", lambda _: None)

    result = manager.start_one("FLOW-005")

    assert result.result == "extension_not_connected"
    assert result.details["startup_extension_wait_attempts"] == 75
    assert len(inspector.terminated) == 2


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
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

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
    assert inspector.terminated == [352]


def test_stop_one_preflight_worker_failure_does_not_stop_chrome(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=22224, worker_pid=38820)
    inspector = FakeInspector()
    inspector.listeners[9300] = 22224
    inspector.listeners[8101] = 123
    inspector.listeners[9200] = 456
    inspector.alive.update({22224, 123, 456})
    inspector.commands[22224] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    add_running_worker(inspector, 123)
    add_running_worker(inspector, 456)
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    result = manager.stop_one("FLOW-005")

    assert result.result == "ownership_not_verified"
    assert result.details["target"] == "worker_service"
    assert result.details["worker_api_listener_pid"] == 123
    assert result.details["worker_ws_listener_pid"] == 456
    assert inspector.terminated == []


def test_stop_one_preflight_chrome_failure_does_not_stop_worker(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=22224, worker_pid=38820)
    inspector = FakeInspector()
    inspector.listeners[9300] = 22224
    inspector.listeners[8101] = 38820
    inspector.listeners[9200] = 38820
    inspector.alive.update({22224, 38820})
    inspector.commands[22224] = "chrome --user-data-dir=OTHER --remote-debugging-port=9300"
    add_running_worker(inspector, 38820)
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    result = manager.stop_one("FLOW-005")

    assert result.result == "ownership_not_verified"
    assert result.details["target"] == "chrome"
    assert result.details["chrome_cdp_listener_pid"] == 22224
    assert inspector.terminated == []


def test_stop_one_handles_chrome_stopped_worker_running(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=22224, worker_pid=38820)
    inspector = FakeInspector()
    inspector.listeners[8101] = 38820
    inspector.listeners[9200] = 38820
    add_running_worker(inspector, 38820)
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    result = manager.stop_one("FLOW-005")

    assert result.result == "stopped"
    assert inspector.terminated == [38820]
    assert registry.get("FLOW-005").worker_pid is None
    assert registry.get("FLOW-005").chrome_pid is None


def test_stop_one_handles_worker_stopped_chrome_running(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=22224, worker_pid=38820)
    inspector = FakeInspector()
    inspector.listeners[9300] = 22224
    inspector.alive.add(22224)
    inspector.commands[22224] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    result = manager.stop_one("FLOW-005")

    assert result.result == "stopped"
    assert inspector.terminated == [22224]


def test_stop_one_skips_dead_or_unverified_launcher(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.mark_started("FLOW-005", worker_pid=352)
    inspector = FakeInspector()
    inspector.listeners[8101] = 38820
    inspector.listeners[9200] = 38820
    inspector.parents[38820] = 352
    add_running_worker(inspector, 38820)
    inspector.alive.add(352)
    inspector.commands[352] = "python -m runtime.worker_entry --runtime-account-id FLOW-006 --runtime-api-port 8101 --runtime-ws-port 9200"
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    result = manager.stop_one("FLOW-005")

    assert result.result == "stopped"
    assert inspector.terminated == [38820]


def test_stop_one_worker_terminate_failure_does_not_stop_chrome(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=22224, worker_pid=38820)
    inspector = FakeInspector()
    inspector.listeners[9300] = 22224
    inspector.listeners[8101] = 38820
    inspector.listeners[9200] = 38820
    inspector.alive.update({22224, 38820})
    inspector.commands[22224] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    add_running_worker(inspector, 38820)
    inspector.fail_terminate.add(38820)
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    result = manager.stop_one("FLOW-005")

    assert result.result == "stop_failed"
    assert result.details["target"] == "worker_service"
    assert result.details["graceful_returncode"] == 1
    assert result.details["forced_returncode"] == 1
    assert result.details["process_alive_after"] is True
    assert result.details["remaining_ports"]["worker_api_listener_pid"] == 38820
    assert inspector.terminated == [38820]
    assert registry.get("FLOW-005").chrome_pid == 22224
    assert registry.get("FLOW-005").worker_pid == 38820


def test_stop_one_worker_ports_not_released_returns_stop_failed(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=22224, worker_pid=38820)
    inspector = FakeInspector()
    inspector.listeners[9300] = 22224
    inspector.listeners[8101] = 38820
    inspector.listeners[9200] = 38820
    inspector.keep_listeners.update({8101, 9200})
    inspector.alive.update({22224, 38820})
    inspector.commands[22224] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    add_running_worker(inspector, 38820)
    manager = make_manager(tmp_path, registry, inspector=inspector)
    manager._wait_worker_ports_released = lambda account: False
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    result = manager.stop_one("FLOW-005")

    assert result.result == "stop_failed"
    assert result.details["stage"] == "wait_ports"
    assert result.details["remaining_ports"]["worker_api_listener_pid"] == 38820
    assert inspector.terminated == [38820]
    assert registry.get("FLOW-005").chrome_pid == 22224


def test_stop_one_chrome_terminate_failure_returns_stop_failed(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=22224)
    inspector = FakeInspector()
    inspector.listeners[9300] = 22224
    inspector.alive.add(22224)
    inspector.commands[22224] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    inspector.fail_terminate.add(22224)
    manager = make_manager(tmp_path, registry, inspector=inspector)
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    result = manager.stop_one("FLOW-005")

    assert result.result == "stop_failed"
    assert result.details["target"] == "chrome"
    assert inspector.terminated == [22224]


def test_stop_one_chrome_port_not_released_returns_stop_failed(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    registry.mark_started("FLOW-005", chrome_pid=22224)
    inspector = FakeInspector()
    inspector.listeners[9300] = 22224
    inspector.keep_listeners.add(9300)
    inspector.alive.add(22224)
    inspector.commands[22224] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    manager = make_manager(tmp_path, registry, inspector=inspector)
    manager._wait_port_released = lambda port: False
    monkeypatch.setattr("runtime.process_manager.port_is_listening", lambda port: port in inspector.listeners)

    result = manager.stop_one("FLOW-005")

    assert result.result == "stop_failed"
    assert result.details["target"] == "chrome"
    assert result.details["remaining_ports"]["chrome_cdp_listener_pid"] == 22224


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


def test_worker_entry_rejects_invalid_runtime_identity(monkeypatch):
    monkeypatch.setenv("FLOW_ACCOUNT_ID", "FLOW-005")
    monkeypatch.setenv("AGENT_API_PORT", "8101")
    monkeypatch.setenv("EXTENSION_WS_PORT", "9200")
    monkeypatch.setenv("FLOW_RUNTIME_INSTANCE_ID", "not-a-uuid")
    monkeypatch.setenv("FLOW_RUNTIME_OWNERSHIP_SECRET", "secret")
    monkeypatch.setenv("FLOW_RUNTIME_OWNERSHIP_VERSION", "1")
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
    assert manager._worker_ownership(registry.get("FLOW-005"), 12, allow_legacy_command_line=True)["verified"] is True

    inspector.commands[12] = "python -m runtime.worker_entry --runtime-account-id FLOW-006 --runtime-api-port 8101 --runtime-ws-port 9200"
    mismatch = manager._worker_ownership(registry.get("FLOW-005"), 12, allow_legacy_command_line=True)
    assert mismatch["verified"] is False
    assert mismatch["reason"] == "account_mismatch"

    inspector.commands[12] = "python -m agent.main --runtime-account-id FLOW-005 --runtime-api-port 8101 --runtime-ws-port 9200"
    missing_entry = manager._worker_ownership(registry.get("FLOW-005"), 12, allow_legacy_command_line=True)
    assert missing_entry["verified"] is False
    assert missing_entry["reason"] == "worker_entry_missing"


def test_runtime_ownership_helpers_sign_and_verify_challenge(tmp_path):
    identity = create_runtime_identity("FLOW-005", tmp_path / "data", FakeSecretProtector())
    challenge = generate_challenge()

    assert len(identity.secret) >= 32
    assert identity.secret not in identity.secret_ref
    assert canonical_payload("FLOW-005", identity.runtime_instance_id, challenge, 1) == canonical_payload("FLOW-005", identity.runtime_instance_id, challenge, 1)
    response = sign_challenge(identity.secret, "FLOW-005", identity.runtime_instance_id, challenge)

    assert verify_challenge_response(identity.secret, "FLOW-005", identity.runtime_instance_id, challenge, response)
    assert not verify_challenge_response("wrong-secret", "FLOW-005", identity.runtime_instance_id, challenge, response)
    assert not verify_challenge_response(identity.secret, "FLOW-006", identity.runtime_instance_id, challenge, response)


def test_worker_runtime_challenge_verifies_without_command_line(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-012")
    inspector = FakeInspector()
    manager = make_manager(tmp_path, registry, inspector=inspector, health={}, cdp=False)
    identity = create_runtime_identity(account.account_id, registry.data_root, FakeSecretProtector())
    registry.mark_worker_runtime_identity(account.account_id, identity.runtime_instance_id, identity.secret_ref, identity.secret_fingerprint, identity.version)
    registry.mark_worker_process_started(account.account_id, 35088)
    account = registry.get(account.account_id)
    inspector.alive.add(35088)
    inspector.listeners[account.worker_api_port] = 35088
    inspector.listeners[account.extension_ws_port] = 35088
    manager._worker_health = lambda _account: {
        "account_id": "FLOW-012",
        "runtime_instance_id": identity.runtime_instance_id,
        "runtime_ownership_version": 1,
        "extension_connected": True,
    }

    def challenge(_account, challenge):
        return {
            "ok": True,
            "account_id": "FLOW-012",
            "runtime_instance_id": identity.runtime_instance_id,
            "challenge_response": sign_challenge(identity.secret, "FLOW-012", identity.runtime_instance_id, challenge),
            "proof_version": 1,
        }

    manager._worker_ownership_challenge = challenge

    status = manager.status("FLOW-012")

    assert status.details["worker_ownership_verified"] is True
    assert status.details["worker_ownership_reason"] == "verified"
    assert status.details["worker_ownership_method"] == "worker_challenge"
    assert status.details["ownership_status"] == "worker_verified"
    assert status.details["stop_safe"] is False


def test_worker_runtime_challenge_does_not_update_when_registry_identity_changes(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-012")
    inspector = FakeInspector()
    manager = make_manager(tmp_path, registry, inspector=inspector, health={}, cdp=False)
    identity = create_runtime_identity(account.account_id, registry.data_root, FakeSecretProtector())
    replacement = create_runtime_identity(account.account_id, registry.data_root, FakeSecretProtector())
    registry.mark_worker_runtime_identity(account.account_id, identity.runtime_instance_id, identity.secret_ref, identity.secret_fingerprint, identity.version)
    registry.mark_worker_process_started(account.account_id, 35088)
    account = registry.get(account.account_id)
    inspector.alive.add(35088)
    inspector.listeners[account.worker_api_port] = 35100
    inspector.listeners[account.extension_ws_port] = 35100
    manager._worker_health = lambda _account: {
        "account_id": "FLOW-012",
        "runtime_instance_id": identity.runtime_instance_id,
        "runtime_ownership_version": 1,
    }

    def challenge(_account, challenge):
        registry.mark_worker_runtime_identity(account.account_id, replacement.runtime_instance_id, replacement.secret_ref, replacement.secret_fingerprint, replacement.version)
        registry.mark_worker_process_started(account.account_id, 36000)
        return {
            "ok": True,
            "account_id": "FLOW-012",
            "runtime_instance_id": identity.runtime_instance_id,
            "challenge_response": sign_challenge(identity.secret, "FLOW-012", identity.runtime_instance_id, challenge),
            "proof_version": 1,
        }

    manager._worker_ownership_challenge = challenge

    status = manager.status("FLOW-012")
    current = registry.get("FLOW-012")

    assert status.details["worker_ownership_verified"] is False
    assert status.details["worker_ownership_reason"] == "runtime_identity_changed"
    assert current.runtime_instance_id == replacement.runtime_instance_id
    assert current.worker_pid == 36000
    assert current.worker_ownership_method is None


def test_worker_runtime_challenge_does_not_update_when_worker_pid_changes(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-012")
    inspector = FakeInspector()
    manager = make_manager(tmp_path, registry, inspector=inspector, health={}, cdp=False)
    identity = create_runtime_identity(account.account_id, registry.data_root, FakeSecretProtector())
    registry.mark_worker_runtime_identity(account.account_id, identity.runtime_instance_id, identity.secret_ref, identity.secret_fingerprint, identity.version)
    registry.mark_worker_process_started(account.account_id, 35088)
    account = registry.get(account.account_id)
    inspector.alive.add(35088)
    inspector.listeners[account.worker_api_port] = 35100
    inspector.listeners[account.extension_ws_port] = 35100
    manager._worker_health = lambda _account: {
        "account_id": "FLOW-012",
        "runtime_instance_id": identity.runtime_instance_id,
        "runtime_ownership_version": 1,
    }

    def challenge(_account, challenge):
        registry.mark_worker_process_started(account.account_id, 36000)
        return {
            "ok": True,
            "account_id": "FLOW-012",
            "runtime_instance_id": identity.runtime_instance_id,
            "challenge_response": sign_challenge(identity.secret, "FLOW-012", identity.runtime_instance_id, challenge),
            "proof_version": 1,
        }

    manager._worker_ownership_challenge = challenge

    status = manager.status("FLOW-012")
    current = registry.get("FLOW-012")

    assert status.details["worker_ownership_verified"] is False
    assert status.details["worker_ownership_reason"] == "runtime_identity_changed"
    assert current.worker_pid == 36000
    assert current.worker_ownership_method is None


def test_worker_runtime_challenge_rejects_pid_and_identity_mismatch(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-012")
    inspector = FakeInspector()
    manager = make_manager(tmp_path, registry, inspector=inspector, health={}, cdp=False)
    identity = create_runtime_identity(account.account_id, registry.data_root, FakeSecretProtector())
    registry.mark_worker_runtime_identity(account.account_id, identity.runtime_instance_id, identity.secret_ref, identity.secret_fingerprint, identity.version)
    registry.mark_worker_process_started(account.account_id, 35088)
    account = registry.get(account.account_id)
    inspector.alive.add(35088)
    inspector.listeners[account.worker_api_port] = 35088
    inspector.listeners[account.extension_ws_port] = 99999

    assert manager.status("FLOW-012").details["worker_ownership_reason"] == "worker_api_ws_pid_mismatch"

    inspector.listeners[account.extension_ws_port] = 35088
    manager._worker_health = lambda _account: {
        "account_id": "FLOW-999",
        "runtime_instance_id": identity.runtime_instance_id,
        "runtime_ownership_version": 1,
    }

    assert manager.status("FLOW-012").details["worker_ownership_reason"] == "worker_identity_mismatch"


def test_worker_runtime_challenge_repairs_listener_pid_when_launcher_probe_fails(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-012")
    inspector = FakeInspector()
    manager = make_manager(tmp_path, registry, inspector=inspector, health={}, cdp=False)
    identity = create_runtime_identity(account.account_id, registry.data_root, FakeSecretProtector())
    registry.mark_worker_runtime_identity(account.account_id, identity.runtime_instance_id, identity.secret_ref, identity.secret_fingerprint, identity.version)
    registry.mark_worker_process_started(account.account_id, 35088)
    account = registry.get(account.account_id)
    inspector.probe_status[35088] = "access_denied"
    inspector.alive.add(35100)
    inspector.listeners[account.worker_api_port] = 35100
    inspector.listeners[account.extension_ws_port] = 35100
    manager._worker_health = lambda _account: {
        "account_id": "FLOW-012",
        "runtime_instance_id": identity.runtime_instance_id,
        "runtime_ownership_version": 1,
    }

    def challenge(_account, challenge):
        return {
            "ok": True,
            "account_id": "FLOW-012",
            "runtime_instance_id": identity.runtime_instance_id,
            "challenge_response": sign_challenge(identity.secret, "FLOW-012", identity.runtime_instance_id, challenge),
            "proof_version": 1,
        }

    manager._worker_ownership_challenge = challenge

    status = manager.status("FLOW-012")
    current = registry.get("FLOW-012")

    assert status.details["worker_ownership_verified"] is True
    assert status.details["worker_ownership_reason"] == "verified"
    assert current.worker_pid == 35100
    assert current.worker_ownership_method == "worker_challenge"


def test_legacy_account_is_not_worker_verified(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-005")
    registry.mark_started("FLOW-005", worker_pid=352)
    inspector = FakeInspector()
    inspector.alive.add(352)
    inspector.listeners[account.worker_api_port] = 352
    inspector.listeners[account.extension_ws_port] = 352
    manager = make_manager(
        tmp_path,
        registry,
        inspector=inspector,
        health={"account_id": "FLOW-005", "extension_connected": True},
        cdp=False,
    )

    status = manager.status("FLOW-005")

    assert status.details["worker_ownership_verified"] is False
    assert status.details["worker_ownership_reason"] == "legacy_runtime_unverified"
    assert status.details["ownership_status"] == "legacy_unverified"
    assert status.details["stop_safe"] is False
