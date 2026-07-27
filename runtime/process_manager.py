"""Single-account local runtime launcher and health checks."""
from __future__ import annotations

import ctypes
import json
import os
import socket
import subprocess
import time
import hashlib
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.request import Request, urlopen

from .ownership import (
    OWNERSHIP_VERSION,
    OwnershipError,
    OwnershipSecretProtector,
    create_runtime_identity,
    delete_secret_ref,
    generate_challenge,
    read_runtime_secret,
    sign_challenge,
    validate_challenge,
    verify_challenge_response,
)
from .extension_paths import chrome_extension_args, runtime_extension_dirs
from .paths import EXTENSION_DIR, FLOWKIT_DIR, POC_ROOT
from .port_allocator import port_can_bind, port_is_listening
from .registry import AccountRecord, AccountRegistry


FLOW_URL = "https://labs.google/fx/tools/flow"
PYTHON_EXE = POC_ROOT / ".venv" / "Scripts" / "python.exe"
MANAGED_CFT_VERSION = "151.0.7922.47"
MANAGED_CFT_CHROME = POC_ROOT / "browsers" / "chrome-for-testing" / MANAGED_CFT_VERSION / "chrome-win64" / "chrome.exe"
TH32CS_SNAPPROCESS = 0x00000002
PROCESS_TERMINATE = 0x0001
DWORD = ctypes.c_ulong
BOOL = ctypes.c_int
HANDLE = ctypes.c_void_p
INVALID_HANDLE_VALUE = HANDLE(-1).value


class PROCESSENTRY32W(ctypes.Structure):
    _fields_ = [
        ("dwSize", ctypes.c_ulong),
        ("cntUsage", ctypes.c_ulong),
        ("th32ProcessID", ctypes.c_ulong),
        ("th32DefaultHeapID", ctypes.c_void_p),
        ("th32ModuleID", ctypes.c_ulong),
        ("cntThreads", ctypes.c_ulong),
        ("th32ParentProcessID", ctypes.c_ulong),
        ("pcPriClassBase", ctypes.c_long),
        ("dwFlags", ctypes.c_ulong),
        ("szExeFile", ctypes.c_wchar * 260),
    ]


def _load_kernel32():
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateToolhelp32Snapshot.argtypes = [DWORD, DWORD]
    kernel32.CreateToolhelp32Snapshot.restype = HANDLE
    kernel32.Process32FirstW.argtypes = [HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    kernel32.Process32FirstW.restype = BOOL
    kernel32.Process32NextW.argtypes = [HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    kernel32.Process32NextW.restype = BOOL
    kernel32.OpenProcess.argtypes = [DWORD, BOOL, DWORD]
    kernel32.OpenProcess.restype = HANDLE
    kernel32.TerminateProcess.argtypes = [HANDLE, ctypes.c_uint]
    kernel32.TerminateProcess.restype = BOOL
    kernel32.CloseHandle.argtypes = [HANDLE]
    kernel32.CloseHandle.restype = BOOL
    return kernel32


KERNEL32 = _load_kernel32()


@dataclass
class RuntimeResult:
    result: str
    account_id: str
    ok: bool = False
    error: str | None = None
    details: dict | None = None

    def to_dict(self) -> dict:
        data = asdict(self)
        data["details"] = data["details"] or {}
        return data


@dataclass
class ProcessProbeResult:
    pid: int | None
    alive: bool | None
    status: str
    method: str | None = None
    error: str | None = None


@dataclass
class CommandLineProbeResult:
    pid: int | None
    command_line: str = ""
    status: str = "unavailable"
    error: str | None = None


@dataclass
class TerminationResult:
    success: bool
    pid: int
    graceful_attempted: bool = False
    graceful_returncode: int | None = None
    graceful_stdout: str = ""
    graceful_stderr: str = ""
    forced_attempted: bool = False
    forced_returncode: int | None = None
    forced_stdout: str = ""
    forced_stderr: str = ""
    winapi_attempted: bool = False
    winapi_success: bool = False
    process_alive_after: bool = False
    timeout: bool = False
    exception_type: str | None = None

    def __bool__(self) -> bool:
        return self.success

    def to_dict(self) -> dict:
        return asdict(self)


class ProcessInspector:
    def probe_process(self, pid: int | None) -> ProcessProbeResult:
        if not pid:
            return ProcessProbeResult(pid, False, "not_found", "none")
        pid = int(pid)
        get_process = self._probe_with_get_process(pid)
        if get_process.status in {"alive", "not_found"}:
            return get_process
        tasklist = self._probe_with_tasklist(pid)
        if tasklist.status in {"alive", "not_found"}:
            return tasklist
        if get_process.status == "access_denied" or tasklist.status == "access_denied":
            return ProcessProbeResult(pid, None, "access_denied", "get-process/tasklist")
        return ProcessProbeResult(pid, None, "unknown", "get-process/tasklist")

    def process_alive(self, pid: int | None) -> bool:
        return self.probe_process(pid).alive is True

    def _probe_with_get_process(self, pid: int) -> ProcessProbeResult:
        try:
            result = subprocess.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    f"$p=Get-Process -Id {pid} -ErrorAction SilentlyContinue; if ($null -ne $p) {{ 'FOUND' }}",
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=3,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )
        except subprocess.TimeoutExpired:
            return ProcessProbeResult(pid, None, "probe_error", "get-process", "timeout")
        except Exception as error:
            return ProcessProbeResult(pid, None, "probe_error", "get-process", type(error).__name__)
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        combined = f"{stdout}\n{stderr}".lower()
        if result.returncode == 0:
            return ProcessProbeResult(pid, "FOUND" in stdout, "alive" if "FOUND" in stdout else "not_found", "get-process")
        if "access is denied" in combined or "access denied" in combined or "拒绝访问" in combined:
            return ProcessProbeResult(pid, None, "access_denied", "get-process")
        return ProcessProbeResult(pid, None, "probe_error", "get-process", f"returncode_{result.returncode}")

    def _probe_with_tasklist(self, pid: int) -> ProcessProbeResult:
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}"],
                capture_output=True,
                text=True,
                check=False,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )
        except Exception as error:
            return ProcessProbeResult(pid, None, "probe_error", "tasklist", type(error).__name__)
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        combined = f"{stdout}\n{stderr}".lower()
        if "access is denied" in combined or "access denied" in combined or "拒绝访问" in combined:
            return ProcessProbeResult(pid, None, "access_denied", "tasklist")
        if str(pid) in stdout:
            return ProcessProbeResult(pid, True, "alive", "tasklist")
        if "no tasks are running" in combined or "没有运行" in combined:
            return ProcessProbeResult(pid, False, "not_found", "tasklist")
        return ProcessProbeResult(pid, None, "unknown", "tasklist")

    def command_line(self, pid: int | None) -> str:
        return self.command_line_probe(pid).command_line

    def command_line_probe(self, pid: int | None) -> CommandLineProbeResult:
        if not pid:
            return CommandLineProbeResult(pid, "", "process_not_found")
        pid = int(pid)
        script = (
            f'$p=Get-CimInstance Win32_Process -Filter "ProcessId={pid}";'
            'if ($null -eq $p) { [Console]::Out.Write("__PROCESS_NOT_FOUND__") }'
            'elseif ($null -ne $p.CommandLine) { [Console]::Out.Write($p.CommandLine) }'
        )
        try:
            result = subprocess.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    script,
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=3,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )
            if result.returncode != 0:
                combined = f"{result.stdout or ''}\n{result.stderr or ''}".lower()
                if "access is denied" in combined or "access denied" in combined or "拒绝访问" in combined:
                    return CommandLineProbeResult(pid, "", "access_denied")
                return CommandLineProbeResult(pid, "", "query_failed", f"returncode_{result.returncode}")
            stdout = (result.stdout or "").strip()
            if stdout == "__PROCESS_NOT_FOUND__":
                return CommandLineProbeResult(pid, "", "process_not_found")
            if not stdout:
                return CommandLineProbeResult(pid, "", "cim_empty")
            return CommandLineProbeResult(pid, stdout, "available")
        except subprocess.TimeoutExpired:
            return CommandLineProbeResult(pid, "", "query_failed", "timeout")
        except Exception as error:
            return CommandLineProbeResult(pid, "", "query_failed", type(error).__name__)

    def listening_pid(self, port: int) -> int | None:
        try:
            result = subprocess.run(
                ["netstat", "-ano", "-p", "tcp"],
                capture_output=True,
                text=True,
                check=False,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )
        except Exception:
            return None
        marker = f":{int(port)}"
        for line in (result.stdout or "").splitlines():
            parts = line.split()
            if len(parts) >= 5 and parts[0].upper().startswith("TCP") and parts[1].endswith(marker) and parts[3].upper() == "LISTENING":
                try:
                    return int(parts[-1])
                except ValueError:
                    return None
        return None

    def parent_pid(self, pid: int | None) -> int | None:
        if not pid:
            return None
        snapshot = self._create_process_snapshot()
        if self._snapshot_failed(snapshot):
            return None
        try:
            entry = PROCESSENTRY32W()
            entry.dwSize = ctypes.sizeof(PROCESSENTRY32W)
            if not self._process_first(snapshot, entry):
                return None
            while True:
                if int(entry.th32ProcessID) == int(pid):
                    return int(entry.th32ParentProcessID)
                if not self._process_next(snapshot, entry):
                    return None
        except Exception:
            return None
        finally:
            self._close_handle(snapshot)

    def _create_process_snapshot(self):
        return KERNEL32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)

    def _snapshot_failed(self, snapshot) -> bool:
        return not snapshot or int(snapshot) == int(INVALID_HANDLE_VALUE)

    def _process_first(self, snapshot, entry) -> bool:
        return bool(KERNEL32.Process32FirstW(snapshot, ctypes.byref(entry)))

    def _process_next(self, snapshot, entry) -> bool:
        return bool(KERNEL32.Process32NextW(snapshot, ctypes.byref(entry)))

    def _close_handle(self, snapshot) -> None:
        KERNEL32.CloseHandle(snapshot)

    def terminate(self, pid: int, timeout_seconds: float = 8.0, should_force=None) -> TerminationResult:
        pid = int(pid)
        result = TerminationResult(success=False, pid=pid)
        if not self.process_alive(pid):
            result.success = True
            result.process_alive_after = False
            return result
        result.graceful_attempted = True
        try:
            graceful = subprocess.run(
                ["taskkill", "/PID", str(pid), "/T"],
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout_seconds,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )
            result.graceful_returncode = graceful.returncode
            result.graceful_stdout = self._truncate_output(graceful.stdout)
            result.graceful_stderr = self._truncate_output(graceful.stderr)
        except subprocess.TimeoutExpired as error:
            result.timeout = True
            result.exception_type = type(error).__name__
        except Exception as error:
            result.exception_type = type(error).__name__

        force_needed = result.graceful_returncode != 0 or self.process_alive(pid)
        if should_force is not None:
            try:
                force_needed = force_needed or bool(should_force())
            except Exception:
                force_needed = True
        if force_needed:
            result.forced_attempted = True
            try:
                forced = subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=timeout_seconds,
                    creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
                )
                result.forced_returncode = forced.returncode
                result.forced_stdout = self._truncate_output(forced.stdout)
                result.forced_stderr = self._truncate_output(forced.stderr)
            except subprocess.TimeoutExpired as error:
                result.timeout = True
                result.exception_type = type(error).__name__
            except Exception as error:
                result.exception_type = type(error).__name__
        if self.process_alive(pid):
            result.winapi_attempted = True
            result.winapi_success = self._terminate_with_winapi(pid)
        result.process_alive_after = self.process_alive(pid)
        result.success = not result.process_alive_after
        return result

    def _terminate_with_winapi(self, pid: int) -> bool:
        handle = KERNEL32.OpenProcess(PROCESS_TERMINATE, False, DWORD(int(pid)))
        if not handle:
            return False
        try:
            return bool(KERNEL32.TerminateProcess(handle, 1))
        finally:
            KERNEL32.CloseHandle(handle)

    def _truncate_output(self, output: str | None, limit: int = 500) -> str:
        text = (output or "").strip()
        return text if len(text) <= limit else text[:limit] + "...<truncated>"


class RuntimeManager:
    def __init__(
        self,
        registry: AccountRegistry | None = None,
        inspector: ProcessInspector | None = None,
        popen=subprocess.Popen,
        chrome_path: Path | str | None = None,
        extension_dir: Path | str = EXTENSION_DIR,
        python_exe: Path | str = PYTHON_EXE,
        flow_url: str = FLOW_URL,
        log_dir: Path | str | None = None,
        ownership_protector: OwnershipSecretProtector | None = None,
    ):
        self.registry = registry or AccountRegistry()
        self.inspector = inspector or ProcessInspector()
        self.popen = popen
        self.chrome_path = Path(chrome_path) if chrome_path else None
        self.extension_dir = Path(extension_dir)
        self.python_exe = Path(python_exe)
        self.flow_url = flow_url
        self.log_dir = Path(log_dir) if log_dir else POC_ROOT / "logs" / "runtime"
        self.ownership_protector = ownership_protector

    def open_login(self, account_id: str) -> RuntimeResult:
        account = self._account_or_error(account_id)
        if isinstance(account, RuntimeResult):
            return account
        base = self._preflight(account, require_profile=True, require_chrome=True)
        if base:
            return base
        if self._owned_chrome_running(account):
            self._log(account.account_id, "open-login", {"result": "already_running", "chrome_pid": account.chrome_pid})
            return RuntimeResult("already_running", account.account_id, True)
        conflict = self._listening_port_conflict(account, "chrome_cdp_port", account.chrome_cdp_port)
        if conflict:
            self._log(account.account_id, "open-login", {"result": "port_conflict", "chrome_cdp_port": account.chrome_cdp_port})
            return conflict
        command = self.chrome_command(account)
        proc = self.popen(command, cwd=str(FLOWKIT_DIR))
        self.registry.mark_started(account.account_id, chrome_pid=proc.pid)
        self._log(account.account_id, "open-login", {"pid": proc.pid, "command": self._safe_command(command)})
        return RuntimeResult("opened", account.account_id, True, details={"chrome_pid": proc.pid, "command": self._safe_command(command)})

    def start_one(self, account_id: str) -> RuntimeResult:
        account = self._account_or_error(account_id)
        if isinstance(account, RuntimeResult):
            return account
        base = self._preflight(account, require_profile=True, require_chrome=True)
        if base:
            return base
        status = self.status(account.account_id)
        if status.details.get("runtime_status") == "running":
            return RuntimeResult("already_running", account.account_id, True, details=status.details)
        conflict = self._port_conflict(account)
        if conflict:
            self._log(account.account_id, "start-one", {"result": "port_conflict", **(conflict.details or {})})
            return conflict
        existing_chrome_pid = self._verified_chrome_pid(account)
        existing_worker_pid = self._verified_worker_pid(account)
        if existing_worker_pid:
            self.registry.mark_started(account.account_id, worker_pid=existing_worker_pid)
        worker_proc = None
        chrome_proc = None
        try:
            if existing_worker_pid:
                self._log(account.account_id, "start-one", {"step": "worker_reused", "pid": existing_worker_pid})
            else:
                worker_proc = self._start_new_worker(account)
                self._log(account.account_id, "start-one", {"step": "worker_started", "pid": worker_proc.pid, "command": self._safe_command(self.worker_command(account))})
            if existing_chrome_pid:
                self.registry.mark_started(account.account_id, chrome_pid=existing_chrome_pid)
                self._log(account.account_id, "start-one", {"step": "chrome_reused", "pid": existing_chrome_pid})
            else:
                command = self.chrome_command(account)
                chrome_proc = self.popen(command, cwd=str(FLOWKIT_DIR))
                self.registry.mark_started(account.account_id, chrome_pid=chrome_proc.pid)
                self._log(account.account_id, "start-one", {"step": "chrome_started", "pid": chrome_proc.pid, "command": self._safe_command(command)})
            health = self._wait_startup_extension_ready(account, getattr(worker_proc, "pid", None), getattr(chrome_proc, "pid", None))
            if health.details.get("account_match"):
                self._log(account.account_id, "start-one", {"result": "started", "health": self._health_log_fields(health.details)})
                return RuntimeResult("started", account.account_id, True, details=health.details)
            self._stop_started(account, getattr(worker_proc, "pid", None), getattr(chrome_proc, "pid", None))
            reason = "extension_not_connected"
            if health.details.get("extension_connected") and not health.details.get("account_match"):
                reason = "account_mismatch"
            self._log(account.account_id, "start-one", {"result": reason, "health": self._health_log_fields(health.details)})
            return RuntimeResult(reason, account.account_id, False, details=health.details)
        except Exception as error:
            self._stop_started(account, getattr(worker_proc, "pid", None), getattr(chrome_proc, "pid", None))
            self._log(account.account_id, "start-one", {"result": "failed", "error": str(error)})
            return RuntimeResult("failed", account.account_id, False, str(error), {"rollback_completed": True})

    def start_worker_only(self, account_id: str) -> RuntimeResult:
        account = self._account_or_error(account_id)
        if isinstance(account, RuntimeResult):
            return account
        base = self._preflight(account, require_profile=True, require_chrome=False)
        if base:
            return base
        for field, port in (("worker_api_port", account.worker_api_port), ("extension_ws_port", account.extension_ws_port)):
            conflict = self._listening_port_conflict(account, field, port)
            if conflict:
                self._log(account.account_id, "start-worker-only", {"result": "port_conflict", **(conflict.details or {})})
                return conflict
        existing_worker_pid = self._verified_worker_pid(account)
        if existing_worker_pid:
            self.registry.mark_started(account.account_id, worker_pid=existing_worker_pid)
            self._log(account.account_id, "start-worker-only", {"result": "already_running", "pid": existing_worker_pid})
            return RuntimeResult("already_running", account.account_id, True, details={"worker_pid": existing_worker_pid})
        try:
            worker_proc = self._start_new_worker(account)
        except Exception as error:
            self._log(account.account_id, "start-worker-only", {"result": "worker_start_failed", "error": str(error)})
            return RuntimeResult("worker_start_failed", account.account_id, False, str(error))
        self._log(account.account_id, "start-worker-only", {"result": "started", "pid": worker_proc.pid, "command": self._safe_command(self.worker_command(account))})
        return RuntimeResult("started", account.account_id, True, details={"worker_pid": worker_proc.pid})

    def status(self, account_id: str) -> RuntimeResult:
        account = self.registry.get(account_id)
        if not account:
            return RuntimeResult("account_not_found", account_id, False)
        self._verified_chrome_pid(account)
        self._verified_worker_pid(account)
        account = self.registry.get(account_id) or account
        chrome_probe = self.inspector.probe_process(account.chrome_pid)
        worker_probe = self.inspector.probe_process(account.worker_pid)
        chrome_ownership = self._chrome_ownership(account, account.chrome_pid)
        worker_ownership = self._worker_runtime_ownership(account, account.worker_pid)
        chrome_alive = chrome_probe.alive is True
        account = self.registry.get(account_id) or account
        worker_alive = worker_probe.alive is True
        account = self.registry.get(account_id) or account
        worker_health = self._worker_health(account)
        cdp_reachable = self._tcp_reachable(account.chrome_cdp_port)
        extension_connected = bool(worker_health.get("extension_connected"))
        extension_account_id = worker_health.get("account_id")
        account_match = extension_connected and extension_account_id == account.account_id
        extension_expected_ws_url = f"ws://127.0.0.1:{account.extension_ws_port}"
        if account_match:
            extension_bootstrap_status = "extension_ready"
        elif extension_connected:
            extension_bootstrap_status = "account_mismatch"
        elif worker_health:
            extension_bootstrap_status = "extension_not_connected"
        else:
            extension_bootstrap_status = "extension_missing"
        profile_exists = Path(account.profile_path).exists()
        worker_health_matches = bool(worker_health) and worker_health.get("account_id") == account.account_id
        worker_api_listener_pid = self.inspector.listening_pid(account.worker_api_port)
        worker_ws_listener_pid = self.inspector.listening_pid(account.extension_ws_port)
        chrome_cdp_listener_pid = self.inspector.listening_pid(account.chrome_cdp_port)
        worker_ports_listening = bool(worker_api_listener_pid and worker_ws_listener_pid and int(worker_api_listener_pid) == int(worker_ws_listener_pid))
        chrome_port_listening = bool(chrome_cdp_listener_pid)
        worker_service_reachable = bool(worker_health_matches and (worker_ports_listening or worker_alive or worker_ownership["verified"]))
        chrome_service_reachable = bool(cdp_reachable and (chrome_port_listening or chrome_ownership["verified"]))
        runtime_healthy = bool(worker_service_reachable and chrome_service_reachable and account_match)
        if runtime_healthy:
            runtime_status = "running"
        elif worker_service_reachable and chrome_service_reachable:
            runtime_status = "unhealthy"
        elif worker_service_reachable or chrome_service_reachable or worker_health or cdp_reachable or (worker_alive and worker_ownership["verified"]) or (chrome_alive and chrome_ownership["verified"]):
            runtime_status = "partial"
        else:
            runtime_status = "stopped"
        ownership_status = self._combined_ownership_status(chrome_ownership, worker_ownership)
        stop_safe = bool(chrome_ownership["verified"] and worker_ownership["verified"])
        details = {
            "account_id": account.account_id,
            "enabled": account.enabled,
            "registration_status": account.status,
            "runtime_status": runtime_status,
            "runtime_healthy": runtime_healthy,
            "chrome_pid": account.chrome_pid,
            "chrome_pid_recorded": account.chrome_pid,
            "chrome_process_alive": chrome_alive,
            "chrome_process_probe_status": chrome_probe.status,
            "chrome_cdp_port": account.chrome_cdp_port,
            "chrome_cdp_reachable": cdp_reachable,
            "chrome_cdp_listener_pid": chrome_cdp_listener_pid,
            "chrome_ownership_verified": chrome_ownership["verified"],
            "chrome_ownership_reason": chrome_ownership["reason"],
            "worker_pid": account.worker_pid,
            "worker_pid_recorded": account.worker_pid,
            "worker_process_alive": worker_alive,
            "worker_process_probe_status": worker_probe.status,
            "worker_api_port": account.worker_api_port,
            "worker_api_listener_pid": worker_api_listener_pid,
            "worker_health_reachable": bool(worker_health),
            "extension_ws_port": account.extension_ws_port,
            "worker_ws_listener_pid": worker_ws_listener_pid,
            "extension_connected": extension_connected,
            "extension_account_id": extension_account_id,
            "account_match": account_match,
            "extension_present": extension_connected,
            "extension_configured": account_match,
            "extension_bootstrap_status": extension_bootstrap_status,
            "extension_expected_account_id": account.account_id,
            "extension_expected_ws_url": extension_expected_ws_url,
            "profile_path": account.profile_path,
            "profile_exists": profile_exists,
            "last_started_at": account.last_started_at,
            "last_stopped_at": account.last_stopped_at,
            "last_health_at": account.last_health_at,
            "last_error": account.last_error,
            "worker_ownership_verified": worker_ownership["verified"],
            "worker_ownership_reason": worker_ownership["reason"],
            "worker_ownership_method": worker_ownership.get("method"),
            "worker_ownership_verified_at": account.worker_ownership_verified_at,
            "runtime_instance_id": account.runtime_instance_id,
            "runtime_ownership_version": account.runtime_ownership_version,
            "ownership_status": ownership_status,
            "stop_safe": stop_safe,
        }
        if worker_health.get("bootstrap_diagnostics"):
            details["bootstrap_diagnostics"] = worker_health.get("bootstrap_diagnostics")
        self.registry.mark_health(account.account_id, None if runtime_status != "unhealthy" else "runtime_unhealthy")
        self._log(account.account_id, "status", {"result": runtime_status, "health": self._health_log_fields(details)})
        return RuntimeResult(runtime_status, account.account_id, runtime_status == "running", details=details)

    def stop_one(self, account_id: str) -> RuntimeResult:
        account = self.registry.get(account_id)
        if not account:
            return RuntimeResult("account_not_found", account_id, False)
        plan = self._build_stop_plan(account)
        if plan.get("error"):
            details = {key: value for key, value in plan.items() if key != "error"}
            self._log(account.account_id, "stop-one", {"result": "ownership_not_verified", **details})
            return RuntimeResult("ownership_not_verified", account.account_id, False, details=details)
        verified_chrome_pid = plan.get("verified_chrome_pid")
        verified_worker_pid = plan.get("verified_worker_service_pid")
        verified_worker_launcher_pid = plan.get("optional_worker_launcher_pid")
        if not verified_chrome_pid and not verified_worker_pid:
            self.registry.mark_stopped(account.account_id)
            self._log(account.account_id, "stop-one", {"result": "already_stopped", **plan})
            return RuntimeResult("already_stopped", account.account_id, True)
        if verified_worker_pid:
            worker_terminate_pid = verified_worker_launcher_pid or verified_worker_pid
            termination = self.inspector.terminate(
                worker_terminate_pid,
                should_force=lambda: port_is_listening(account.worker_api_port) or port_is_listening(account.extension_ws_port),
            )
            if not termination:
                details = {
                    "stage": "terminate",
                    "target": "worker_service",
                    "pid": worker_terminate_pid,
                    "worker_service_pid": verified_worker_pid,
                    "terminate_result": False,
                    "reason": "force_termination_failed",
                    **self._termination_details(termination),
                    "remaining_ports": self._worker_port_details(account),
                }
                self._log(account.account_id, "stop-one", {"result": "stop_failed", **details})
                return RuntimeResult("stop_failed", account.account_id, False, details=details)
            if not self._wait_worker_ports_released(account):
                details = {"stage": "wait_ports", "target": "worker_service", "pid": worker_terminate_pid, "worker_service_pid": verified_worker_pid, "remaining_ports": self._worker_port_details(account)}
                self._log(account.account_id, "stop-one", {"result": "stop_failed", **details})
                return RuntimeResult("stop_failed", account.account_id, False, details=details)
        if verified_chrome_pid:
            termination = self.inspector.terminate(
                verified_chrome_pid,
                should_force=lambda: port_is_listening(account.chrome_cdp_port),
            )
            if not termination:
                details = {
                    "stage": "terminate",
                    "target": "chrome",
                    "pid": verified_chrome_pid,
                    "terminate_result": False,
                    "reason": "force_termination_failed",
                    **self._termination_details(termination),
                    "remaining_ports": {"chrome_cdp_listener_pid": self.inspector.listening_pid(account.chrome_cdp_port)},
                }
                self._log(account.account_id, "stop-one", {"result": "stop_failed", **details})
                return RuntimeResult("stop_failed", account.account_id, False, details=details)
            if not self._wait_port_released(account.chrome_cdp_port):
                details = {"stage": "wait_ports", "target": "chrome", "pid": verified_chrome_pid, "remaining_ports": {"chrome_cdp_listener_pid": self.inspector.listening_pid(account.chrome_cdp_port)}}
                self._log(account.account_id, "stop-one", {"result": "stop_failed", **details})
                return RuntimeResult("stop_failed", account.account_id, False, details=details)
        current = self.registry.get(account.account_id) or account
        delete_secret_ref(current.runtime_secret_ref)
        self.registry.mark_stopped(account.account_id)
        self._log(account.account_id, "stop-one", {"result": "stopped", **plan})
        return RuntimeResult("stopped", account.account_id, True, details=plan)

    def chrome_command(self, account: AccountRecord) -> list[str]:
        chrome = self._find_chrome()
        extension_args = chrome_extension_args(self.extension_dir)
        command = [
            str(chrome),
            f"--user-data-dir={Path(account.profile_path)}",
            f"--remote-debugging-port={account.chrome_cdp_port}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-skia-graphite",
        ]
        command.extend(extension_args)
        command.append(self.flow_url)
        return command

    def _wait_startup_extension_ready(self, account: AccountRecord, worker_pid: int | None, chrome_pid: int | None, attempts: int = 75) -> RuntimeResult:
        last = self.status(account.account_id)
        for attempt in range(1, attempts + 1):
            last.details["startup_extension_wait_attempts"] = attempt
            if last.details.get("account_match"):
                return last
            if (
                (worker_pid and last.details.get("worker_process_alive") is False and last.details.get("worker_health_reachable") is False)
                or (chrome_pid and last.details.get("chrome_process_alive") is False and last.details.get("chrome_cdp_reachable") is False)
            ):
                return last
            if attempt >= attempts:
                return last
            time.sleep(0.2)
            last = self.status(account.account_id)
        return last

    def worker_command(self, account: AccountRecord) -> list[str]:
        return [
            str(self.python_exe),
            "-m",
            "runtime.worker_entry",
            "--runtime-account-id",
            account.account_id,
            "--runtime-api-port",
            str(account.worker_api_port),
            "--runtime-ws-port",
            str(account.extension_ws_port),
        ]

    def worker_env(self, account: AccountRecord) -> dict:
        env = os.environ.copy()
        env.update({
            "FLOW_ACCOUNT_ID": account.account_id,
            "AGENT_API_HOST": "127.0.0.1",
            "AGENT_API_PORT": str(account.worker_api_port),
            "EXTENSION_WS_HOST": "127.0.0.1",
            "EXTENSION_WS_PORT": str(account.extension_ws_port),
            "FLOW_DB_PATH": account.database_path,
            "OUTPUT_DIR": account.output_dir,
        })
        return env

    def _start_new_worker(self, account: AccountRecord):
        identity = create_runtime_identity(account.account_id, self.registry.data_root, self.ownership_protector)
        self.registry.mark_worker_runtime_identity(
            account.account_id,
            identity.runtime_instance_id,
            identity.secret_ref,
            identity.secret_fingerprint,
            identity.version,
        )
        try:
            proc = self._start_worker_process(account, identity)
        except Exception:
            delete_secret_ref(identity.secret_ref)
            raise
        self.registry.mark_worker_process_started(account.account_id, proc.pid)
        return proc

    def _start_worker_process(self, account: AccountRecord, identity=None):
        log_handle = self._worker_log_file(account).open("ab")
        try:
            env = self.worker_env(account)
            if identity is not None:
                env.update({
                    "FLOW_RUNTIME_INSTANCE_ID": identity.runtime_instance_id,
                    "FLOW_RUNTIME_OWNERSHIP_SECRET": identity.secret,
                    "FLOW_RUNTIME_OWNERSHIP_VERSION": str(identity.version),
                })
            return self.popen(
                self.worker_command(account),
                cwd=str(FLOWKIT_DIR),
                env=env,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )
        finally:
            log_handle.close()

    def _account_or_error(self, account_id: str) -> AccountRecord | RuntimeResult:
        account = self.registry.get(account_id)
        if not account:
            return RuntimeResult("account_not_found", account_id, False)
        if not account.enabled:
            return RuntimeResult("account_disabled", account_id, False)
        return account

    def _preflight(self, account: AccountRecord, require_profile: bool, require_chrome: bool) -> RuntimeResult | None:
        if require_profile and not Path(account.profile_path).exists():
            return RuntimeResult("profile_missing", account.account_id, False)
        if not self.extension_dir.exists():
            return RuntimeResult("failed", account.account_id, False, "extension_not_found", {"path": str(self.extension_dir)})
        try:
            runtime_extension_dirs(self.extension_dir)
        except (FileNotFoundError, ValueError) as exc:
            return RuntimeResult("failed", account.account_id, False, "extension_manifest_missing", {"error": str(exc)})
        if require_chrome and not self._find_chrome():
            return RuntimeResult("cft_browser_not_configured", account.account_id, False, details={"cft_required": True, **self.browser_diagnostics()})
        return None

    def _port_conflict(self, account: AccountRecord) -> RuntimeResult | None:
        ports = {
            "worker_api_port": account.worker_api_port,
            "extension_ws_port": account.extension_ws_port,
            "chrome_cdp_port": account.chrome_cdp_port,
        }
        for name, port in ports.items():
            conflict = self._listening_port_conflict(account, name, port)
            if conflict:
                return conflict
        return None

    def _listening_port_conflict(self, account: AccountRecord, field: str, port: int) -> RuntimeResult | None:
        for other in self.registry.list_accounts():
            if other.account_id == account.account_id:
                continue
            if int(getattr(other, field)) == int(port):
                return RuntimeResult("port_conflict", account.account_id, False, details={"field": field, "port": port, "owner": other.account_id})
        if not port_is_listening(port):
            if port_can_bind(port):
                return None
            return RuntimeResult("port_conflict", account.account_id, False, details={"field": field, "port": port})
        if field == "chrome_cdp_port" and self._owned_chrome_running(account):
            return None
        if field in {"worker_api_port", "extension_ws_port"} and self._owned_worker_running(account):
            return None
        return RuntimeResult("port_conflict", account.account_id, False, details={"field": field, "port": port})

    def _find_chrome(self) -> Path | None:
        if self.chrome_path:
            return self.chrome_path if self.chrome_path.exists() else None
        env_path = os.environ.get("FLOWKIT_CHROME_EXECUTABLE")
        if env_path:
            path = Path(env_path)
            return path if path.is_file() else None
        return MANAGED_CFT_CHROME if MANAGED_CFT_CHROME.is_file() else None

    def browser_diagnostics(self) -> dict:
        chrome = self._find_chrome()
        source = "explicit" if self.chrome_path else ("environment" if os.environ.get("FLOWKIT_CHROME_EXECUTABLE") else "managed_cft")
        details = {
            "browser_executable": str(chrome) if chrome else None,
            "browser_kind": "chrome_for_testing" if chrome else None,
            "browser_version": None,
            "browser_sha256": None,
            "browser_source": source,
            "cft_required": True,
        }
        if chrome and chrome.is_file():
            try:
                details["browser_sha256"] = hashlib.sha256(chrome.read_bytes()).hexdigest()
            except Exception:
                details["browser_sha256"] = None
        return details

    def _owned_chrome_running(self, account: AccountRecord) -> bool:
        pid = self._verified_chrome_pid(account)
        return pid is not None

    def _verified_chrome_pid(self, account: AccountRecord) -> int | None:
        return self._discover_chrome_pid(account, repair_registry=True)

    def _discover_chrome_pid(self, account: AccountRecord, repair_registry: bool) -> int | None:
        pid = account.chrome_pid
        if self._chrome_pid_matches(account, pid):
            return int(pid)
        listening_pid = self.inspector.listening_pid(account.chrome_cdp_port)
        verified_pid = self._verified_chrome_pid_from_tree(account, listening_pid)
        if verified_pid is not None:
            if repair_registry and verified_pid != account.chrome_pid:
                self.registry.mark_started(account.account_id, chrome_pid=verified_pid)
            return verified_pid
        return None

    def _verified_chrome_pid_from_tree(self, account: AccountRecord, pid: int | None, max_depth: int = 8) -> int | None:
        seen: set[int] = set()
        current = pid
        for _ in range(max_depth):
            if not current or current in seen:
                return None
            seen.add(current)
            if self._chrome_pid_matches(account, current):
                return int(current)
            if not self.inspector.process_alive(current):
                return None
            current = self.inspector.parent_pid(current)
        return None

    def _chrome_pid_matches(self, account: AccountRecord, pid: int | None) -> bool:
        return self._chrome_ownership(account, pid)["verified"]

    def _chrome_ownership(self, account: AccountRecord, pid: int | None) -> dict:
        probe = self.inspector.probe_process(pid)
        if probe.alive is False:
            return {"verified": False, "reason": "process_not_found"}
        if probe.alive is None:
            return {"verified": False, "reason": f"process_probe_{probe.status}"}
        cmd_probe = self.inspector.command_line_probe(pid)
        if cmd_probe.status != "available":
            return {"verified": False, "reason": f"command_line_{cmd_probe.status}"}
        normalized = self._normalize_command_line(cmd_probe.command_line)
        profile = self._normalize_command_line(str(Path(account.profile_path)))
        if profile not in normalized:
            return {"verified": False, "reason": "profile_mismatch"}
        if f"remote-debugging-port={account.chrome_cdp_port}" not in normalized:
            return {"verified": False, "reason": "cdp_port_mismatch"}
        return {"verified": True, "reason": "verified"}

    def _owned_worker_running(self, account: AccountRecord) -> bool:
        pid = self._verified_worker_pid(account)
        return pid is not None

    def _verified_worker_pid(self, account: AccountRecord) -> int | None:
        return self._discover_worker_pid(account, repair_registry=True)

    def _discover_worker_pid(self, account: AccountRecord, repair_registry: bool) -> int | None:
        api_pid = self.inspector.listening_pid(account.worker_api_port)
        ws_pid = self.inspector.listening_pid(account.extension_ws_port)
        if api_pid or ws_pid:
            if not api_pid or not ws_pid or int(api_pid) != int(ws_pid):
                return None
            verified_pid = self._verified_worker_pid_from_tree(account, api_pid)
            if verified_pid is not None:
                if repair_registry and verified_pid != account.worker_pid:
                    self.registry.mark_started(account.account_id, worker_pid=verified_pid)
                return verified_pid
            return None
        if self._worker_pid_matches(account, account.worker_pid):
            return int(account.worker_pid)
        return None

    def _build_stop_plan(self, account: AccountRecord) -> dict:
        plan = {
            "stage": "preflight",
            "verified_chrome_pid": None,
            "verified_worker_service_pid": None,
            "optional_worker_launcher_pid": None,
            "chrome_cdp_listener_pid": self.inspector.listening_pid(account.chrome_cdp_port),
            **self._worker_port_details(account),
        }
        worker_api_pid = plan["worker_api_listener_pid"]
        worker_ws_pid = plan["worker_ws_listener_pid"]
        if worker_api_pid or worker_ws_pid:
            if not worker_api_pid or not worker_ws_pid or int(worker_api_pid) != int(worker_ws_pid):
                plan.update({"error": True, "target": "worker_service", "reason": "ownership_not_verified"})
                return plan
            worker_pid = self._discover_worker_pid(account, repair_registry=False)
            if not worker_pid:
                plan.update({"error": True, "target": "worker_service", "reason": "ownership_not_verified"})
                return plan
            plan["verified_worker_service_pid"] = worker_pid
            plan["optional_worker_launcher_pid"] = self._verified_worker_launcher_pid(account, worker_pid)
        elif self._worker_pid_matches(account, account.worker_pid):
            plan["verified_worker_service_pid"] = int(account.worker_pid)

        if plan["chrome_cdp_listener_pid"]:
            chrome_pid = self._discover_chrome_pid(account, repair_registry=False)
            if not chrome_pid:
                plan.update({"error": True, "target": "chrome", "reason": "ownership_not_verified"})
                return plan
            plan["verified_chrome_pid"] = chrome_pid
        elif self._chrome_pid_matches(account, account.chrome_pid):
            plan["verified_chrome_pid"] = int(account.chrome_pid)
        return plan

    def _verified_worker_launcher_pid(self, account: AccountRecord, service_pid: int | None, max_depth: int = 8) -> int | None:
        if not service_pid:
            return None
        seen = {int(service_pid)}
        current = self.inspector.parent_pid(service_pid)
        for _ in range(max_depth):
            if not current or current in seen:
                return None
            seen.add(current)
            if self._worker_pid_matches(account, current):
                return int(current)
            if not self.inspector.process_alive(current):
                return None
            current = self.inspector.parent_pid(current)
        return None

    def _verified_worker_pid_from_tree(self, account: AccountRecord, pid: int | None, max_depth: int = 8) -> int | None:
        seen: set[int] = set()
        current = pid
        for _ in range(max_depth):
            if not current or current in seen:
                return None
            seen.add(current)
            if self._worker_pid_matches(account, current):
                return int(current)
            if not self.inspector.process_alive(current):
                return None
            current = self.inspector.parent_pid(current)
        return None

    def _worker_pid_matches(self, account: AccountRecord, pid: int | None) -> bool:
        return self._worker_ownership(account, pid, allow_legacy_command_line=True)["verified"]

    def _worker_ownership(self, account: AccountRecord, pid: int | None, allow_legacy_command_line: bool = False) -> dict:
        probe = self.inspector.probe_process(pid)
        if probe.alive is False:
            return {"verified": False, "reason": "process_not_found"}
        if probe.alive is None:
            return {"verified": False, "reason": f"process_probe_{probe.status}"}
        if not allow_legacy_command_line:
            return {"verified": False, "reason": "legacy_runtime_unverified"}
        cmd_probe = self.inspector.command_line_probe(pid)
        if cmd_probe.status != "available":
            return {"verified": False, "reason": f"command_line_{cmd_probe.status}"}
        normalized = self._normalize_command_line(cmd_probe.command_line)
        if "runtime.worker_entry" not in normalized:
            return {"verified": False, "reason": "worker_entry_missing"}
        if "--runtime-account-id" not in normalized or account.account_id.lower() not in normalized:
            return {"verified": False, "reason": "account_mismatch"}
        if "--runtime-api-port" not in normalized or str(account.worker_api_port) not in normalized:
            return {"verified": False, "reason": "api_port_mismatch"}
        if "--runtime-ws-port" not in normalized or str(account.extension_ws_port) not in normalized:
            return {"verified": False, "reason": "ws_port_mismatch"}
        return {"verified": True, "reason": "verified"}

    def _worker_runtime_ownership(self, account: AccountRecord, pid: int | None) -> dict:
        expected_worker_pid = account.worker_pid
        expected_runtime_instance_id = account.runtime_instance_id
        expected_runtime_ownership_version = account.runtime_ownership_version
        details = {
            "verified": False,
            "reason": "unknown",
            "method": "worker_challenge",
            "worker_launcher_pid": pid,
            "registry_worker_pid": expected_worker_pid,
            "worker_launcher_alive": None,
            "worker_launcher_probe_status": "not_checked",
            "worker_api_reachable": False,
            "worker_ws_reachable": False,
            "worker_api_listener_pid": None,
            "worker_ws_listener_pid": None,
            "worker_listener_pid_consistent": False,
            "worker_identity_match": False,
            "worker_challenge_verified": False,
            "worker_pid_cas_applied": False,
        }
        probe = self.inspector.probe_process(pid)
        details["worker_launcher_alive"] = probe.alive is True
        details["worker_launcher_probe_status"] = probe.status
        has_runtime_identity = bool(expected_runtime_instance_id and account.runtime_secret_ref and expected_runtime_ownership_version)
        if probe.alive is False:
            if not has_runtime_identity:
                return {**details, "reason": "process_not_found", "method": None}
        if probe.alive is None:
            if not has_runtime_identity:
                return {**details, "reason": f"process_probe_{probe.status}", "method": None}
        if not has_runtime_identity:
            return {**details, "reason": "legacy_runtime_unverified", "method": None}
        ports = self._worker_port_details(account)
        api_pid = ports["worker_api_listener_pid"]
        ws_pid = ports["worker_ws_listener_pid"]
        details["worker_api_listener_pid"] = api_pid
        details["worker_ws_listener_pid"] = ws_pid
        details["worker_api_reachable"] = bool(api_pid)
        details["worker_ws_reachable"] = bool(ws_pid)
        if not api_pid:
            return {**details, "reason": "worker_api_not_ready"}
        if not ws_pid:
            return {**details, "reason": "worker_ws_not_ready"}
        if int(api_pid) != int(ws_pid):
            return {**details, "reason": "worker_api_ws_pid_mismatch"}
        details["worker_listener_pid_consistent"] = True
        health = self._worker_health(account)
        if not health:
            return {**details, "reason": "worker_health_unreachable"}
        if health.get("account_id") != account.account_id:
            return {**details, "reason": "worker_identity_mismatch"}
        if health.get("runtime_instance_id") != expected_runtime_instance_id:
            return {**details, "reason": "runtime_instance_mismatch"}
        if int(health.get("runtime_ownership_version") or 0) != int(expected_runtime_ownership_version):
            return {**details, "reason": "ownership_protocol_unsupported"}
        details["worker_identity_match"] = True
        try:
            secret = read_runtime_secret(account.runtime_secret_ref, self.ownership_protector)
        except OwnershipError as error:
            return {**details, "reason": str(error)}
        challenge = generate_challenge()
        response = self._worker_ownership_challenge(account, challenge)
        if not response.get("ok"):
            return {**details, "reason": response.get("reason") or "ownership_challenge_failed"}
        if response.get("account_id") != account.account_id:
            return {**details, "reason": "worker_identity_mismatch"}
        if response.get("runtime_instance_id") != expected_runtime_instance_id:
            return {**details, "reason": "runtime_instance_mismatch"}
        if not verify_challenge_response(secret, account.account_id, expected_runtime_instance_id, challenge, response.get("challenge_response", ""), int(expected_runtime_ownership_version)):
            return {**details, "reason": "ownership_challenge_failed"}
        details["worker_challenge_verified"] = True
        updated = self.registry.mark_worker_ownership_verified_if_current(
            account.account_id,
            expected_runtime_instance_id,
            expected_worker_pid,
            int(expected_runtime_ownership_version),
            int(api_pid),
            "worker_challenge",
        )
        if not updated:
            return {**details, "reason": "runtime_identity_changed"}
        return {
            **details,
            "verified": True,
            "reason": "verified",
            "method": "worker_challenge",
            "worker_pid_cas_applied": int(api_pid) != int(expected_worker_pid or 0),
        }

    def _worker_ownership_challenge(self, account: AccountRecord, challenge: str) -> dict:
        if not validate_challenge(challenge):
            return {"ok": False, "reason": "invalid_challenge"}
        payload = json.dumps(
            {
                "account_id": account.account_id,
                "runtime_instance_id": account.runtime_instance_id,
                "challenge": challenge,
                "proof_version": int(account.runtime_ownership_version or OWNERSHIP_VERSION),
            },
            separators=(",", ":"),
        ).encode("utf-8")
        request = Request(
            f"http://127.0.0.1:{account.worker_api_port}/api/runtime/ownership/challenge",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=1.0) as response:
                data = json.loads(response.read().decode("utf-8"))
        except Exception:
            return {"ok": False, "reason": "ownership_challenge_timeout"}
        if not isinstance(data, dict) or not data.get("ok", True):
            return {"ok": False, "reason": data.get("reason") if isinstance(data, dict) else "ownership_challenge_failed"}
        return {"ok": True, **data}

    def _combined_ownership_status(self, chrome_ownership: dict, worker_ownership: dict) -> str:
        verified = [chrome_ownership["verified"], worker_ownership["verified"]]
        reasons = {chrome_ownership["reason"], worker_ownership["reason"]}
        if all(verified):
            return "verified"
        if worker_ownership["verified"] and not chrome_ownership["verified"]:
            return "worker_verified"
        if chrome_ownership["verified"] and not worker_ownership["verified"]:
            return "chrome_verified"
        if "legacy_runtime_unverified" in reasons:
            return "legacy_unverified"
        if "profile_mismatch" in reasons or "cdp_port_mismatch" in reasons or "account_mismatch" in reasons or "api_port_mismatch" in reasons or "ws_port_mismatch" in reasons:
            return "mismatch"
        if any(verified):
            return "partial"
        return "unknown"

    def _worker_health(self, account: AccountRecord) -> dict:
        try:
            with urlopen(f"http://127.0.0.1:{account.worker_api_port}/health", timeout=1.0) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception:
            return {}

    def _wait_worker_ports_released(self, account: AccountRecord, timeout_seconds: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if not port_is_listening(account.worker_api_port) and not port_is_listening(account.extension_ws_port):
                return True
            time.sleep(0.1)
        return not port_is_listening(account.worker_api_port) and not port_is_listening(account.extension_ws_port)

    def _wait_port_released(self, port: int, timeout_seconds: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if not port_is_listening(port):
                return True
            time.sleep(0.1)
        return not port_is_listening(port)

    def _worker_port_details(self, account: AccountRecord) -> dict:
        return {
            "worker_api_listener_pid": self.inspector.listening_pid(account.worker_api_port),
            "worker_ws_listener_pid": self.inspector.listening_pid(account.extension_ws_port),
        }

    def _termination_details(self, result) -> dict:
        if hasattr(result, "to_dict"):
            return result.to_dict()
        return {}

    def _tcp_reachable(self, port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            return sock.connect_ex(("127.0.0.1", int(port))) == 0

    def _stop_started(self, account: AccountRecord, worker_pid: int | None, chrome_pid: int | None) -> None:
        for pid in self._owned_started_worker_stop_pids(account, worker_pid):
            if self.inspector.process_alive(pid):
                self.inspector.terminate(pid)
        if chrome_pid and self.inspector.process_alive(chrome_pid):
            self.inspector.terminate(chrome_pid)
        current = self.registry.get(account.account_id) or account
        delete_secret_ref(current.runtime_secret_ref)
        self.registry.mark_stopped(account.account_id)

    def _owned_started_worker_stop_pids(self, account: AccountRecord, worker_pid: int | None) -> list[int]:
        candidates: list[int] = []
        if worker_pid:
            candidates.append(int(worker_pid))

        current = self.registry.get(account.account_id) or account
        ports = self._worker_port_details(current)
        api_pid = ports["worker_api_listener_pid"]
        ws_pid = ports["worker_ws_listener_pid"]
        if api_pid and ws_pid and int(api_pid) == int(ws_pid) and self._worker_health_matches_current_runtime(current):
            listener_pid = int(api_pid)
            candidates.append(listener_pid)
            parent_pid = self.inspector.parent_pid(listener_pid)
            if parent_pid:
                candidates.append(int(parent_pid))

        ordered: list[int] = []
        for pid in candidates:
            if pid not in ordered:
                ordered.append(pid)
        return ordered

    def _worker_health_matches_current_runtime(self, account: AccountRecord) -> bool:
        health = self._worker_health(account)
        if health.get("account_id") != account.account_id:
            return False
        if account.runtime_instance_id and health.get("runtime_instance_id") != account.runtime_instance_id:
            return False
        if account.runtime_ownership_version and int(health.get("runtime_ownership_version") or 0) != int(account.runtime_ownership_version):
            return False
        return True

    def _safe_command(self, command: list[str]) -> list[str]:
        return [part for part in command if "token" not in part.lower() and "cookie" not in part.lower()]

    def _worker_log_file(self, account: AccountRecord) -> Path:
        path = self.log_dir / f"{account.account_id}-worker.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _health_log_fields(self, details: dict) -> dict:
        keys = [
            "runtime_status",
            "chrome_pid",
            "chrome_cdp_port",
            "chrome_cdp_reachable",
            "worker_pid",
            "worker_api_port",
            "worker_health_reachable",
            "extension_ws_port",
            "extension_connected",
            "extension_account_id",
            "account_match",
        ]
        return {key: details.get(key) for key in keys}

    def _normalize_command_line(self, value: str) -> str:
        return str(value).replace("\\", "/").replace('"', "").lower()

    def _log(self, account_id: str, action: str, payload: dict) -> None:
        log_dir = self.log_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        line = json.dumps({"account_id": account_id, "action": action, **payload}, ensure_ascii=False)
        with (log_dir / "runtime.log").open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
