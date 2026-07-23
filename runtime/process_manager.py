"""Single-account local runtime launcher and health checks."""
from __future__ import annotations

import ctypes
import json
import os
import socket
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.request import urlopen

from .paths import EXTENSION_DIR, FLOWKIT_DIR, POC_ROOT
from .port_allocator import port_can_bind, port_is_listening
from .registry import AccountRecord, AccountRegistry


FLOW_URL = "https://labs.google/fx/tools/flow"
PYTHON_EXE = POC_ROOT / ".venv" / "Scripts" / "python.exe"
TH32CS_SNAPPROCESS = 0x00000002
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
    process_alive_after: bool = False
    timeout: bool = False
    exception_type: str | None = None

    def __bool__(self) -> bool:
        return self.success

    def to_dict(self) -> dict:
        return asdict(self)


class ProcessInspector:
    def process_alive(self, pid: int | None) -> bool:
        if not pid:
            return False
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {int(pid)}"],
                capture_output=True,
                text=True,
                check=False,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )
            return str(int(pid)) in (result.stdout or "")
        except Exception:
            return False

    def command_line(self, pid: int | None) -> str:
        if not pid:
            return ""
        pid = int(pid)
        script = (
            f'$p=Get-CimInstance Win32_Process -Filter "ProcessId={pid}";'
            'if ($null -ne $p -and $null -ne $p.CommandLine) { [Console]::Out.Write($p.CommandLine) }'
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
                return ""
            return (result.stdout or "").strip()
        except Exception:
            return ""

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
        result.process_alive_after = self.process_alive(pid)
        result.success = not result.process_alive_after
        return result

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
    ):
        self.registry = registry or AccountRegistry()
        self.inspector = inspector or ProcessInspector()
        self.popen = popen
        self.chrome_path = Path(chrome_path) if chrome_path else None
        self.extension_dir = Path(extension_dir)
        self.python_exe = Path(python_exe)
        self.flow_url = flow_url
        self.log_dir = Path(log_dir) if log_dir else POC_ROOT / "logs" / "runtime"

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
                log_handle = self._worker_log_file(account).open("ab")
                try:
                    worker_proc = self.popen(
                        self.worker_command(account),
                        cwd=str(FLOWKIT_DIR),
                        env=self.worker_env(account),
                        stdout=log_handle,
                        stderr=subprocess.STDOUT,
                        stdin=subprocess.DEVNULL,
                        creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
                    )
                finally:
                    log_handle.close()
                self.registry.mark_started(account.account_id, worker_pid=worker_proc.pid)
                self._log(account.account_id, "start-one", {"step": "worker_started", "pid": worker_proc.pid, "command": self._safe_command(self.worker_command(account))})
            if existing_chrome_pid:
                self.registry.mark_started(account.account_id, chrome_pid=existing_chrome_pid)
                self._log(account.account_id, "start-one", {"step": "chrome_reused", "pid": existing_chrome_pid})
            else:
                chrome_proc = self.popen(self.chrome_command(account), cwd=str(FLOWKIT_DIR))
                self.registry.mark_started(account.account_id, chrome_pid=chrome_proc.pid)
                self._log(account.account_id, "start-one", {"step": "chrome_started", "pid": chrome_proc.pid, "command": self._safe_command(self.chrome_command(account))})
            health = self.status(account.account_id)
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

    def status(self, account_id: str) -> RuntimeResult:
        account = self.registry.get(account_id)
        if not account:
            return RuntimeResult("account_not_found", account_id, False)
        chrome_alive = self._owned_chrome_running(account)
        account = self.registry.get(account_id) or account
        worker_alive = self._owned_worker_running(account)
        account = self.registry.get(account_id) or account
        worker_health = self._worker_health(account)
        cdp_reachable = self._tcp_reachable(account.chrome_cdp_port)
        extension_connected = bool(worker_health.get("extension_connected"))
        extension_account_id = worker_health.get("account_id")
        account_match = extension_connected and extension_account_id == account.account_id
        profile_exists = Path(account.profile_path).exists()
        if worker_alive and chrome_alive and worker_health and cdp_reachable:
            runtime_status = "running" if account_match else "unhealthy"
        elif worker_alive or chrome_alive or worker_health or cdp_reachable:
            runtime_status = "partial"
        else:
            runtime_status = "stopped"
        details = {
            "account_id": account.account_id,
            "enabled": account.enabled,
            "registration_status": account.status,
            "runtime_status": runtime_status,
            "chrome_pid": account.chrome_pid,
            "chrome_process_alive": chrome_alive,
            "chrome_cdp_port": account.chrome_cdp_port,
            "chrome_cdp_reachable": cdp_reachable,
            "worker_pid": account.worker_pid,
            "worker_process_alive": worker_alive,
            "worker_api_port": account.worker_api_port,
            "worker_health_reachable": bool(worker_health),
            "extension_ws_port": account.extension_ws_port,
            "extension_connected": extension_connected,
            "extension_account_id": extension_account_id,
            "account_match": account_match,
            "profile_path": account.profile_path,
            "profile_exists": profile_exists,
            "last_started_at": account.last_started_at,
            "last_stopped_at": account.last_stopped_at,
            "last_health_at": account.last_health_at,
            "last_error": account.last_error,
        }
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
        self.registry.mark_stopped(account.account_id)
        self._log(account.account_id, "stop-one", {"result": "stopped", **plan})
        return RuntimeResult("stopped", account.account_id, True, details=plan)

    def chrome_command(self, account: AccountRecord) -> list[str]:
        chrome = self._find_chrome()
        return [
            str(chrome),
            f"--user-data-dir={Path(account.profile_path)}",
            f"--remote-debugging-port={account.chrome_cdp_port}",
            "--no-first-run",
            "--no-default-browser-check",
            self.flow_url,
        ]

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
        if require_chrome and not self._find_chrome():
            return RuntimeResult("chrome_not_found", account.account_id, False)
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
        candidates = [
            Path(os.environ.get("PROGRAMFILES", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
            Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
            Path(os.environ.get("LOCALAPPDATA", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
        ]
        return next((path for path in candidates if path.exists()), None)

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
        cmd = self.inspector.command_line(pid)
        normalized = self._normalize_command_line(cmd)
        profile = self._normalize_command_line(str(Path(account.profile_path)))
        return bool(
            self.inspector.process_alive(pid)
            and profile in normalized
            and f"remote-debugging-port={account.chrome_cdp_port}" in normalized
        )

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
        cmd = self.inspector.command_line(pid)
        normalized = self._normalize_command_line(cmd)
        return bool(
            self.inspector.process_alive(pid)
            and "runtime.worker_entry" in normalized
            and "--runtime-account-id" in normalized
            and account.account_id.lower() in normalized
            and "--runtime-api-port" in normalized
            and str(account.worker_api_port) in normalized
            and "--runtime-ws-port" in normalized
            and str(account.extension_ws_port) in normalized
        )

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
        if worker_pid and self.inspector.process_alive(worker_pid):
            self.inspector.terminate(worker_pid)
        if chrome_pid and self.inspector.process_alive(chrome_pid):
            self.inspector.terminate(chrome_pid)
        self.registry.mark_stopped(account.account_id)

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
