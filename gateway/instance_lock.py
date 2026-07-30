"""Single-instance lock for a Gateway database."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


class GatewayInstanceLockError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class GatewayInstanceLock:
    def __init__(self, db_path: Path, port: int, lock_path: Path | None = None):
        self.db_path = Path(db_path)
        self.port = int(port)
        self.lock_path = lock_path or self.db_path.with_suffix(self.db_path.suffix + ".lock")
        self.acquired = False

    def acquire(self) -> dict:
        if self.lock_path.exists():
            existing = self._read()
            pid = int(existing.get("server_pid") or existing.get("launcher_pid") or 0)
            if pid and _pid_exists(pid) and _pid_listens_on_port(pid, self.port):
                raise GatewayInstanceLockError("gateway_instance_already_running")
            try:
                self.lock_path.unlink()
            except OSError as exc:
                raise GatewayInstanceLockError("gateway_instance_lock_unavailable") from exc
        payload = {
            "launcher_pid": int(os.environ.get("GATEWAY_LAUNCHER_PID") or os.getpid()),
            "server_pid": os.getpid(),
            "gateway_port": self.port,
            "database_path": str(self.db_path),
            "started_at": utc_now(),
        }
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self.lock_path.open("x", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
        except FileExistsError as exc:
            raise GatewayInstanceLockError("gateway_instance_already_running") from exc
        self.acquired = True
        return payload

    def release(self) -> None:
        if not self.acquired:
            return
        try:
            if self.lock_path.exists():
                self.lock_path.unlink()
        finally:
            self.acquired = False

    def _read(self) -> dict:
        try:
            return json.loads(self.lock_path.read_text(encoding="utf-8"))
        except Exception:
            return {}


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    if sys.platform == "win32":
        import ctypes

        SYNCHRONIZE = 0x00100000
        handle = ctypes.windll.kernel32.OpenProcess(SYNCHRONIZE, False, int(pid))
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, SystemError):
        return False


def _pid_listens_on_port(pid: int, port: int) -> bool:
    if pid <= 0 or port <= 0:
        return False
    if sys.platform == "win32":
        try:
            output = subprocess.check_output(["netstat", "-ano"], text=True, timeout=3)
        except Exception:
            return True
        needle = f":{int(port)}"
        pid_text = str(int(pid))
        for line in output.splitlines():
            parts = line.split()
            if len(parts) >= 5 and needle in parts[1] and parts[3].upper() == "LISTENING" and parts[-1] == pid_text:
                return True
        return False
    return True
