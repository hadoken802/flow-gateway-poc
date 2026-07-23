"""Single-account local runtime launcher and health checks."""
from __future__ import annotations

import json
import os
import socket
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.request import urlopen

from .paths import EXTENSION_DIR, FLOWKIT_DIR, POC_ROOT
from .port_allocator import port_is_listening
from .registry import AccountRecord, AccountRegistry


FLOW_URL = "https://labs.google/fx/tools/flow"
PYTHON_EXE = POC_ROOT / ".venv" / "Scripts" / "python.exe"


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
        try:
            result = subprocess.run(
                ["wmic", "process", "where", f"ProcessId={int(pid)}", "get", "CommandLine", "/value"],
                capture_output=True,
                text=True,
                check=False,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )
            return result.stdout or ""
        except Exception:
            return ""

    def terminate(self, pid: int, timeout_seconds: float = 8.0) -> bool:
        try:
            proc = subprocess.Popen(
                ["taskkill", "/PID", str(int(pid))],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
            )
            proc.wait(timeout=timeout_seconds)
            return proc.returncode == 0
        except Exception:
            return False


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
        if port_is_listening(account.chrome_cdp_port):
            self._log(account.account_id, "open-login", {"result": "port_conflict", "chrome_cdp_port": account.chrome_cdp_port})
            return RuntimeResult("port_conflict", account.account_id, False, details={"port": account.chrome_cdp_port})
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
        worker_proc = None
        chrome_proc = None
        try:
            worker_proc = self.popen(self.worker_command(account), cwd=str(FLOWKIT_DIR), env=self.worker_env(account))
            self.registry.mark_started(account.account_id, worker_pid=worker_proc.pid)
            self._log(account.account_id, "start-one", {"step": "worker_started", "pid": worker_proc.pid, "command": self._safe_command(self.worker_command(account))})
            chrome_proc = self.popen(self.chrome_command(account), cwd=str(FLOWKIT_DIR))
            self.registry.mark_started(account.account_id, chrome_pid=chrome_proc.pid)
            self._log(account.account_id, "start-one", {"step": "chrome_started", "pid": chrome_proc.pid, "command": self._safe_command(self.chrome_command(account))})
            health = self.status(account.account_id)
            if health.details.get("account_match"):
                self._log(account.account_id, "start-one", {"result": "started", "health": self._health_log_fields(health.details)})
                return RuntimeResult("started", account.account_id, True, details=health.details)
            self._stop_started(account, worker_proc.pid, chrome_proc.pid)
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
        worker_alive = self._owned_worker_running(account)
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
        chrome_alive = self._owned_chrome_running(account)
        worker_alive = self._owned_worker_running(account)
        if not chrome_alive and not worker_alive:
            self.registry.mark_stopped(account.account_id)
            self._log(account.account_id, "stop-one", {"result": "already_stopped"})
            return RuntimeResult("already_stopped", account.account_id, True)
        stopped = True
        if worker_alive:
            stopped = self.inspector.terminate(account.worker_pid) and stopped
        if chrome_alive:
            stopped = self.inspector.terminate(account.chrome_pid) and stopped
        if stopped:
            self.registry.mark_stopped(account.account_id)
            self._log(account.account_id, "stop-one", {"result": "stopped", "chrome_pid": account.chrome_pid, "worker_pid": account.worker_pid})
            return RuntimeResult("stopped", account.account_id, True)
        self._log(account.account_id, "stop-one", {"result": "ownership_not_verified", "chrome_pid": account.chrome_pid, "worker_pid": account.worker_pid})
        return RuntimeResult("ownership_not_verified", account.account_id, False)

    def chrome_command(self, account: AccountRecord) -> list[str]:
        chrome = self._find_chrome()
        return [
            str(chrome),
            f"--user-data-dir={Path(account.profile_path)}",
            f"--remote-debugging-port={account.chrome_cdp_port}",
            f"--disable-extensions-except={self.extension_dir}",
            f"--load-extension={self.extension_dir}",
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
            if port_is_listening(port):
                return RuntimeResult("port_conflict", account.account_id, False, details={"field": name, "port": port})
        return None

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
        cmd = self.inspector.command_line(account.chrome_pid)
        normalized = self._normalize_command_line(cmd)
        profile = self._normalize_command_line(str(Path(account.profile_path)))
        return bool(
            self.inspector.process_alive(account.chrome_pid)
            and profile in normalized
            and f"remote-debugging-port={account.chrome_cdp_port}" in normalized
        )

    def _owned_worker_running(self, account: AccountRecord) -> bool:
        cmd = self.inspector.command_line(account.worker_pid)
        normalized = self._normalize_command_line(cmd)
        return bool(
            self.inspector.process_alive(account.worker_pid)
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
