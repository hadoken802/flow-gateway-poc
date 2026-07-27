"""Read-only Flow login verification."""
from __future__ import annotations

import json
import asyncio
import re
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
)
FLOW_PATH_RE = re.compile(r"^/fx(?:/[a-z]{2,3}(?:-[A-Za-z]{2})?)?/tools/flow/?$")


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
    page_target_count: int = 0
    flow_target_count: int = 0
    login_target_count: int = 0
    selected_flow_target_title: str | None = None
    selected_flow_target_host: str | None = None
    selected_flow_target_path: str | None = None
    stale_login_target_detected: bool = False
    flow_app_marker_detected: bool = False
    account_ui_marker_detected: bool = False
    login_form_marker_detected: bool = False
    verification_attempts: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ConfirmLoginResult:
    result: str
    ok: bool
    account_id: str
    previous_registration_status: str | None = None
    registration_status: str | None = None
    registry_updated: bool = False
    login_verified: bool = False
    flow_accessible: bool = False
    google_logged_in: bool = False
    extension_ready: bool = False
    account_match: bool = False
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

    def flow_page_evidence(self, target: dict) -> dict:
        websocket_url = target.get("webSocketDebuggerUrl")
        if not websocket_url:
            return {}
        try:
            return asyncio.run(self._evaluate_flow_page_evidence(str(websocket_url)))
        except Exception:
            return {}

    async def _evaluate_flow_page_evidence(self, websocket_url: str) -> dict:
        import websockets

        expression = r"""
(() => {
  const bodyText = (document.body && document.body.innerText || "").toLowerCase();
  const controls = Array.from(document.querySelectorAll("button,[role='button'],a"))
    .map((el) => ((el.innerText || "") + " " + (el.getAttribute("aria-label") || "")).trim().toLowerCase());
  const inputs = Array.from(document.querySelectorAll("input"))
    .map((el) => `${el.type || ""}:${el.name || ""}:${el.getAttribute("aria-label") || ""}`.toLowerCase());
  const flowApp = bodyText.includes("new project")
    || bodyText.includes("新建项目")
    || controls.some((value) => value.includes("new project") || value.includes("新建项目"))
    || !!document.querySelector("[aria-label*='Google Account'],[aria-label*='Google account'],a[href*='myaccount.google.com']");
  const accountUi = bodyText.includes("pro")
    || !!document.querySelector("[aria-label*='Google Account'],[aria-label*='Google account'],a[href*='myaccount.google.com'],img[alt*='profile' i]");
  const loginForm = bodyText.includes("choose an account")
    || bodyText.includes("sign in with google")
    || inputs.some((value) => /email|identifier|password|passwd/.test(value));
  return {
    flow_app_marker_detected: flowApp,
    account_ui_marker_detected: accountUi,
    login_form_marker_detected: loginForm
  };
})()
"""
        async with websockets.connect(websocket_url, open_timeout=2) as websocket:
            await websocket.send(
                json.dumps(
                    {
                        "id": 1,
                        "method": "Runtime.evaluate",
                        "params": {"expression": expression, "returnByValue": True},
                    }
                )
            )
            message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=2))
        value = (((message.get("result") or {}).get("result") or {}).get("value") or {})
        return value if isinstance(value, dict) else {}


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
        chrome_cdp_listener_pid = self.inspector.listening_pid(account.chrome_cdp_port)
        result.browser_running = chrome_probe.alive is True or bool(chrome_cdp_listener_pid)
        result.worker_running = worker_probe.alive is True
        result.profile_path_matches_registry = self._profile_matches_registry(
            account.profile_path,
            account.chrome_pid,
            chrome_cdp_listener_pid,
        )

        worker_health = self._worker_health(account.worker_api_port)
        result.extension_connected = bool(worker_health.get("extension_connected"))
        result.account_match = result.extension_connected and worker_health.get("account_id") == account.account_id
        result.extension_ready = result.extension_connected and result.account_match

        targets = self.cdp.list_targets(account.chrome_cdp_port)
        result.cdp_connectable = targets is not None
        if targets is None:
            result.reason = "cdp_timeout"
            return self._finalize(result)

        page_state = self._verify_flow_targets(account.chrome_cdp_port, wait_seconds, targets)
        for key, value in page_state.items():
            setattr(result, key, value)
        result.reason = self._reason(result)
        return self._finalize(result)

    def _worker_health(self, worker_api_port: int) -> dict:
        try:
            with urlopen(f"http://127.0.0.1:{int(worker_api_port)}/health", timeout=1.0) as response:
                data = json.loads(response.read().decode("utf-8"))
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    def _profile_matches_registry(self, profile_path: str, recorded_pid: int | None, cdp_listener_pid: int | None) -> bool:
        expected = self._normalize_path(str(Path(profile_path)))
        checked: set[int] = set()
        for chrome_pid in [recorded_pid, cdp_listener_pid]:
            if not chrome_pid or int(chrome_pid) in checked:
                continue
            checked.add(int(chrome_pid))
            probe = self.inspector.command_line_probe(chrome_pid)
            if probe.status != "available":
                continue
            command = self._normalize_path(probe.command_line)
            if expected in command:
                return True
        if recorded_pid and cdp_listener_pid and int(recorded_pid) == int(cdp_listener_pid):
            return self.inspector.probe_process(recorded_pid).alive is True
        return False

    def _verify_flow_targets(self, cdp_port: int, wait_seconds: float, initial_targets: list[dict]) -> dict:
        deadline = time.monotonic() + max(0.0, float(wait_seconds))
        targets = initial_targets
        last_state = self._page_target_summary(targets)
        verification_attempts = 0
        while time.monotonic() <= deadline:
            state = self._evaluate_flow_targets(targets)
            verification_attempts += int(state.get("verification_attempts") or 0)
            state["verification_attempts"] = verification_attempts
            last_state = state
            if state["flow_accessible"] and state["google_logged_in"] and not state["login_redirect_detected"]:
                return state
            if state["login_redirect_detected"] and state["flow_target_count"] == 0:
                return state
            if time.monotonic() >= deadline:
                break
            self.sleep(0.2)
            refreshed = self.cdp.list_targets(cdp_port)
            if refreshed is None:
                return last_state
            targets = refreshed
        if last_state["flow_target_count"] > 0 and not last_state["flow_accessible"] and not last_state["login_redirect_detected"]:
            last_state["reason"] = "flow_page_verification_timeout"
        return last_state

    def _page_target_summary(self, targets: list[dict]) -> dict:
        pages = [target for target in targets or [] if target.get("type") == "page"]
        flow_targets = [target for target in pages if self._is_flow_target(target)]
        login_targets = [target for target in pages if self._is_login_target(target)]
        return {
            "page_target_count": len(pages),
            "flow_target_count": len(flow_targets),
            "login_target_count": len(login_targets),
            "stale_login_target_detected": bool(login_targets),
            "flow_accessible": False,
            "google_logged_in": False,
            "login_redirect_detected": bool(login_targets and not flow_targets),
            "selected_flow_target_title": None,
            "selected_flow_target_host": None,
            "selected_flow_target_path": None,
            "flow_app_marker_detected": False,
            "account_ui_marker_detected": False,
            "login_form_marker_detected": False,
            "verification_attempts": 0,
        }

    def _evaluate_flow_targets(self, targets: list[dict]) -> dict:
        state = self._page_target_summary(targets)
        flow_targets = [target for target in (targets or []) if target.get("type") == "page" and self._is_flow_target(target)]
        if not flow_targets:
            return state
        for target in flow_targets:
            state["verification_attempts"] += 1
            evidence = self.cdp.flow_page_evidence(target)
            marker = {
                "flow_app_marker_detected": bool(evidence.get("flow_app_marker_detected")),
                "account_ui_marker_detected": bool(evidence.get("account_ui_marker_detected")),
                "login_form_marker_detected": bool(evidence.get("login_form_marker_detected")),
            }
            if marker["login_form_marker_detected"] or self._is_login_target(target):
                marker["login_form_marker_detected"] = True
                self._select_target(state, target, marker)
                state["login_redirect_detected"] = True
                return state
            if marker["flow_app_marker_detected"] and marker["account_ui_marker_detected"]:
                self._select_target(state, target, marker)
                state["flow_accessible"] = True
                state["google_logged_in"] = True
                state["login_redirect_detected"] = False
                return state
        return state

    def _select_target(self, state: dict, target: dict, marker: dict) -> None:
        parsed = urlparse(str(target.get("url") or ""))
        state["selected_flow_target_title"] = self._safe_title(str(target.get("title") or ""))
        state["selected_flow_target_host"] = parsed.netloc.lower()
        state["selected_flow_target_path"] = parsed.path
        state.update(marker)

    def _is_flow_target(self, target: dict) -> bool:
        parsed = urlparse(str(target.get("url") or ""))
        return (
            parsed.scheme == "https"
            and parsed.netloc.lower() == "labs.google"
            and bool(FLOW_PATH_RE.match(parsed.path))
        )

    def _is_login_target(self, target: dict) -> bool:
        url = str(target.get("url") or "")
        title = str(target.get("title") or "")
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        text = f"{parsed.scheme}://{host}{path} {title}".lower()
        if host in LOGIN_HOSTS or "accountchooser" in path or "signin" in path:
            return True
        return any(marker in text for marker in LOGIN_TEXT_MARKERS)

    def _classify_targets(self, targets: list[dict]) -> dict:
        state = self._evaluate_flow_targets(targets)
        if state["flow_target_count"]:
            return {"flow_accessible": state["flow_accessible"], "login_redirect_detected": state["login_redirect_detected"]}
        login_redirect_detected = False
        for target in targets or []:
            url = str(target.get("url") or "")
            title = str(target.get("title") or "")
            parsed = urlparse(url)
            host = parsed.netloc.lower()
            path = parsed.path.lower()
            text = f"{parsed.scheme}://{host}{path} {title}".lower()
            if host in LOGIN_HOSTS or "accountchooser" in path or "signin" in path:
                login_redirect_detected = True
            if any(marker in text for marker in LOGIN_TEXT_MARKERS):
                login_redirect_detected = True
        return {"flow_accessible": False, "login_redirect_detected": login_redirect_detected}

    def _reason(self, result: LoginVerificationResult) -> str:
        checks = [
            ("browser_not_running", result.browser_running),
            ("worker_not_running", result.worker_running),
            ("extension_not_connected", result.extension_connected),
            ("account_mismatch", result.account_match),
            ("cdp_timeout", result.cdp_connectable),
            ("profile_path_mismatch", result.profile_path_matches_registry),
            ("flow_page_verification_timeout", result.reason != "flow_page_verification_timeout"),
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

    def _safe_title(self, value: str) -> str:
        lowered = value.lower()
        if "@" in value or "token" in lowered or "cookie" in lowered:
            return "<redacted>"
        return value


class ConfirmLoginService:
    def __init__(self, registry: AccountRegistry | None = None, verifier: LoginVerifier | None = None):
        self.registry = registry or AccountRegistry()
        self.verifier = verifier or LoginVerifier(self.registry)

    def confirm(self, account_id: str) -> ConfirmLoginResult:
        account = self.registry.get(account_id)
        if not account:
            return ConfirmLoginResult(
                result="account_not_found",
                ok=False,
                account_id=account_id,
                reason="account_not_found",
            )

        verification = self.verifier.verify(account_id)
        base = self._result_base(account.account_id, account.status, account.status, verification)
        if not self._verification_allows_registry_update(verification):
            result = "login_verification_failed" if account.status == "login_verified" else "login_not_verified"
            return ConfirmLoginResult(result=result, ok=False, registry_updated=False, **base)

        previous_status = account.status
        if previous_status == "login_verified":
            return ConfirmLoginResult(
                result="already_confirmed",
                ok=True,
                previous_registration_status=previous_status,
                registration_status=previous_status,
                registry_updated=False,
                account_id=account.account_id,
                login_verified=verification.login_verified,
                flow_accessible=verification.flow_accessible,
                google_logged_in=verification.google_logged_in,
                extension_ready=verification.extension_ready,
                account_match=verification.account_match,
                reason="verified",
            )

        updated = self.registry.confirm_login_verified(account.account_id)
        registration_status = updated.status if updated else previous_status
        return ConfirmLoginResult(
            result="confirmed",
            ok=True,
            previous_registration_status=previous_status,
            registration_status=registration_status,
            registry_updated=True,
            account_id=account.account_id,
            login_verified=verification.login_verified,
            flow_accessible=verification.flow_accessible,
            google_logged_in=verification.google_logged_in,
            extension_ready=verification.extension_ready,
            account_match=verification.account_match,
            reason="verified",
        )

    def _result_base(self, account_id: str, previous_status: str, registration_status: str, verification: LoginVerificationResult) -> dict:
        return {
            "account_id": account_id,
            "previous_registration_status": previous_status,
            "registration_status": registration_status,
            "login_verified": verification.login_verified,
            "flow_accessible": verification.flow_accessible,
            "google_logged_in": verification.google_logged_in,
            "extension_ready": verification.extension_ready,
            "account_match": verification.account_match,
            "reason": verification.reason,
        }

    def _verification_allows_registry_update(self, verification: LoginVerificationResult) -> bool:
        return bool(
            verification.browser_running
            and verification.worker_running
            and verification.extension_connected
            and verification.extension_ready
            and verification.account_match
            and verification.cdp_connectable
            and verification.profile_path_matches_registry
            and verification.google_logged_in
            and verification.flow_accessible
            and not verification.login_redirect_detected
            and verification.login_verified
        )
