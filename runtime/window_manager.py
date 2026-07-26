"""Open Flow in the Chrome instance owned by a runtime account."""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from urllib.parse import quote
from urllib.request import urlopen

from .process_manager import FLOW_URL, RuntimeManager
from .registry import AccountRegistry


@dataclass(frozen=True)
class WindowResult:
    account_id: str
    result: str
    chrome_pid: int | None
    opened_url: str
    focused: bool
    runtime_started: bool

    def to_dict(self) -> dict:
        return asdict(self)


class WindowManager:
    def __init__(self, registry: AccountRegistry | None = None, runtime_manager: RuntimeManager | None = None):
        self.registry = registry or AccountRegistry()
        self.runtime_manager = runtime_manager or RuntimeManager(self.registry)

    def open_or_focus_flow(self, account_id: str) -> WindowResult:
        account = self.registry.get(account_id)
        if not account:
            return WindowResult(account_id, "account_not_found", None, FLOW_URL, False, False)
        runtime_started = False
        status = self.runtime_manager.status(account_id)
        if status.details.get("runtime_status") != "running":
            started = self.runtime_manager.start_one(account_id)
            runtime_started = bool(started.ok)
            if not started.ok:
                return WindowResult(account_id, started.result, account.chrome_pid, FLOW_URL, False, runtime_started)
            self._wait_for_runtime(account_id)
            account = self.registry.get(account_id) or account
        opened = self._open_cdp_tab(int(account.chrome_cdp_port), FLOW_URL)
        return WindowResult(account_id, "opened", account.chrome_pid, opened or FLOW_URL, False, runtime_started)

    def _wait_for_runtime(self, account_id: str) -> None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            status = self.runtime_manager.status(account_id)
            if status.details.get("runtime_status") == "running":
                return
            time.sleep(0.5)

    def _open_cdp_tab(self, cdp_port: int, url: str) -> str | None:
        try:
            with urlopen(f"http://127.0.0.1:{cdp_port}/json/new?{quote(url, safe='')}", timeout=5) as response:
                data = json.loads(response.read().decode("utf-8"))
            return data.get("url") or url
        except Exception:
            return None


def open_or_focus_flow(account_id: str) -> dict:
    return WindowManager().open_or_focus_flow(account_id).to_dict()
