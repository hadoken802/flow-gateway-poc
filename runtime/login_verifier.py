"""Read-only Flow login verification."""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

from .process_manager import FLOW_URL, ProcessInspector
from .registry import AccountRegistry


LOGIN_HOSTS = {"accounts.google.com"}
FLOW_HOSTS = {"labs.google", "aitestkitchen.withgoogle.com"}
LOGIN_TEXT_MARKERS = (
    "sign in",
    "choose an account",
    "use another account",
    "session expired",
    "login",
)


@dataclass
class LoginVerificationResult:
    account_id: str
    browser_running: bool = False
    worker_running: bool = False
    extension_connected: bool = False
    extension_ready: bool = False
    account_match: bool = False
    cdp_connectable: bool = False
    profile_path_matches_registry: bool = False
    google_logged_in: bool = False
    flow_accessible: bool = False
    login_redirect_detected: bool = False
    login_verified: bool = False
    reason: str = "unknown"

    def to_dict(self) -> dict:
        return asdict(self)


class CdpReadOnlyClient:
    def list_targets(self, cdp_port: int) -> list[dict] | None:
        try:
            with urlopen(f"http://127.0.0.1:{int(cdp_port)}/json/list", timeout=2.0) as response:
                data = json.loads(response.read().decode("utf-8"))
        except Exception:
            return None
        return data if isinstance(data, list) else None

    def open_url(self, cdp_port: int, url: str) -> dict | None:
        from .extension_bootstrap import CdpClient

        try:
            return CdpClient().open_url(cdp_port, url)
        except Exception:
            return None


class LoginVerifier:
    def __init__(
        self,
        registry: AccountRegistry | None = None,
        inspector: ProcessInspector | None = None,
        cdp: CdpReadOnlyClient | None = None,
        flow_url: str = FLOW_URL,
        sleep=time.sleep,
    ):
        self.registry = registry or AccountRegistry()
        self.inspector = inspector or ProcessInspector()
        self.cdp = cdp or CdpReadOnlyClient()
        self.flow_url = flow_url
        self.sleep = sleep

    def verify(self, account_id: str, wait_seconds: float = 5.0) -> LoginVerificationResult:
        account = self.registry.get(account_id)
        if not account:
            return LoginVerificationResult(account_id=account_id, reason="account_not_found")

        result = LoginVerificationResult(account_id=account.account_id)
        chrome_probe = self.inspector.probe_process(account.chrome_pid)
        worker_probe = self.inspector.probe_process(account.worker_pid)
        result.browser_running = chrome_probe.alive is True
        result.worker_running = worker_probe.alive is True
        result.profile_path_matches_registry = self._profile_matches_registry(account.profile_path, account.chrome_pid)

        worker_health = self._worker_health(account.worker_api_port)
        result.extension_connected = bool(worker_health.get("extension_connected"))
        result.account_match = result.extension_connected and worker_health.get("account_id") == account.account_id
        result.extension_ready = result.extension_connected and result.account_match

        targets = self.cdp.list_targets(account.chrome_cdp_port)
        result.cdp_connectable = targets is not None
        if targets is None:
            result.reason = "cdp_timeout"
            return self._finalize(result)

        self.cdp.open_url(account.chrome_cdp_port, self.flow_url)
        targets = self._wait_for_flow_or_login(account.chrome_cdp_port, wait_seconds, targets)
        page_state = self._classify_targets(targets)
        result.flow_accessible = page_state["flow_accessible"]
        result.login_redirect_detected = page_state["login_redirect_detected"]
        result.google_logged_in = result.flow_accessible and not result.login_redirect_detected
        result.reason = self._reason(result)
        return self._finalize(result)

    def _worker_health(self, worker_api_port: int) -> dict:
        try:
            with urlopen(f"http://127.0.0.1:{int(worker_api_port)}/health", timeout=1.0) as response:
                data = json.loads(response.read().decode("utf-8"))
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    def _profile_matches_registry(self, profile_path: str, chrome_pid: int | None) -> bool:
        probe = self.inspector.command_line_probe(chrome_pid)
        if probe.status != "available":
            return False
        command = self._normalize_path(probe.command_line)
        expected = self._normalize_path(str(Path(profile_path)))
        return expected in command

    def _wait_for_flow_or_login(self, cdp_port: int, wait_seconds: float, initial_targets: list[dict]) -> list[dict]:
        deadline = time.monotonic() + max(0.0, float(wait_seconds))
        targets = initial_targets
        while time.monotonic() <= deadline:
            state = self._classify_targets(targets)
            if state["flow_accessible"] or state["login_redirect_detected"]:
                return targets
            if time.monotonic() >= deadline:
                break
            self.sleep(0.2)
            refreshed = self.cdp.list_targets(cdp_port)
            if refreshed is None:
                return targets
            targets = refreshed
        return targets

    def _classify_targets(self, targets: list[dict]) -> dict:
        flow_accessible = False
        login_redirect_detected = False
        for target in targets or []:
            url = str(target.get("url") or "")
            title = str(target.get("title") or "")
            parsed = urlparse(url)
            host = parsed.netloc.lower()
            path = parsed.path.lower()
            text = f"{url} {title}".lower()
            if host in LOGIN_HOSTS or "accountchooser" in path or "signin" in path:
                login_redirect_detected = True
            if any(marker in text for marker in LOGIN_TEXT_MARKERS):
                login_redirect_detected = True
            if host in FLOW_HOSTS and "flow" in path and not login_redirect_detected:
                flow_accessible = True
        return {"flow_accessible": flow_accessible, "login_redirect_detected": login_redirect_detected}

    def _reason(self, result: LoginVerificationResult) -> str:
        checks = [
            ("browser_not_running", result.browser_running),
            ("worker_not_running", result.worker_running),
            ("extension_not_connected", result.extension_connected),
            ("account_mismatch", result.account_match),
            ("cdp_timeout", result.cdp_connectable),
            ("profile_path_mismatch", result.profile_path_matches_registry),
            ("google_login_missing", result.google_logged_in),
            ("flow_not_accessible", result.flow_accessible),
        ]
        for reason, ok in checks:
            if not ok:
                return reason
        if result.login_redirect_detected:
            return "login_redirect_detected"
        return "verified"

    def _finalize(self, result: LoginVerificationResult) -> LoginVerificationResult:
        result.login_verified = bool(
            result.extension_ready
            and result.account_match
            and result.google_logged_in
            and result.flow_accessible
            and not result.login_redirect_detected
        )
        if result.login_verified:
            result.reason = "verified"
        return result

    def _normalize_path(self, value: str) -> str:
        return value.replace("\\", "/").replace('"', "").lower()
