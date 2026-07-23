"""Flow Kit extension template and account bootstrap helpers."""
from __future__ import annotations

import json
import re
import secrets
import hashlib
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen

from .paths import EXTENSION_DIR, PROFILES_ROOT
from .port_allocator import port_can_bind, port_is_listening
from .process_manager import FLOW_URL, RuntimeManager, RuntimeResult
from .registry import AccountRecord, AccountRegistry


TEMPLATE_PROFILE_NAME = "_FLOWKIT_TEMPLATE"
TEMPLATE_READY_FILE = ".flowkit_template_ready.json"
EXPECTED_FLOWKIT_EXTENSION_ID = "behjnghbkgnggenapbhgjoclmngfgaim"
ACCOUNT_ID_RE = re.compile(r"^FLOW-\d{3,}$")
LOCAL_WS_RE = re.compile(r"^ws://127\.0\.0\.1:(\d{1,5})$")
EXTENSION_ID_RE = re.compile(r"^[a-p]{32}$")
EXCLUDED_PROFILE_NAMES = {
    "SingletonLock",
    "SingletonCookie",
    "SingletonSocket",
    "DevToolsActivePort",
    "Crashpad",
    "Cache",
    "Code Cache",
    "GPUCache",
    "ShaderCache",
    "Cookies",
    "Cookies-journal",
    "Login Data",
    "Login Data-journal",
    "Web Data",
    "Network",
}


@dataclass
class ExtensionBootstrapResult:
    result: str
    account_id: str | None = None
    ok: bool = False
    details: dict | None = None

    def to_dict(self) -> dict:
        data = {
            "result": self.result,
            "ok": self.ok,
            "details": self.details or {},
        }
        if self.account_id:
            data["account_id"] = self.account_id
        return data


@dataclass(frozen=True)
class ExtensionIdentity:
    extension_id: str
    extension_name: str
    extension_version: str
    options_page: str
    service_worker: str
    extension_dir: str
    manifest_sha256: str


class CdpError(Exception):
    def __init__(self, result: str, details: dict, error: str | None = None):
        super().__init__(error or result)
        self.result = result
        self.details = details
        self.error = error

    def to_bootstrap_result(self, account_id: str | None = None) -> ExtensionBootstrapResult:
        details = {**self.details}
        if self.error:
            details["error"] = self.error
        return ExtensionBootstrapResult(self.result, account_id, False, details)


class CdpClient:
    def wait_ready(self, cdp_port: int, attempts: int = 20, delay_seconds: float = 0.25, sleep=time.sleep) -> dict:
        started = time.monotonic()
        last_error = None
        for attempt in range(1, attempts + 1):
            try:
                with urlopen(f"http://127.0.0.1:{int(cdp_port)}/json/version", timeout=2.0) as response:
                    data = json.loads(response.read().decode("utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception as error:
                last_error = type(error).__name__
            sleep(delay_seconds)
        raise CdpError(
            "cdp_not_ready",
            {
                "stage": "wait_cdp_ready",
                "cdp_port": int(cdp_port),
                "attempts": attempts,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "last_error": last_error,
            },
        )

    def open_url(self, cdp_port: int, url: str) -> dict:
        target = f"http://127.0.0.1:{int(cdp_port)}/json/new?{quote(url, safe='')}"
        request = Request(target, method="PUT")
        try:
            with urlopen(request, timeout=2.0) as response:
                data = json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            raise CdpError(
                "cdp_open_target_failed",
                {"stage": "open_bootstrap_page", "cdp_port": int(cdp_port), "http_method": "PUT", "http_status": error.code},
                f"HTTP {error.code}",
            ) from error
        except URLError as error:
            raise CdpError(
                "cdp_open_target_failed",
                {"stage": "open_bootstrap_page", "cdp_port": int(cdp_port), "http_method": "PUT", "reason": type(error).__name__},
                str(error.reason),
            ) from error
        except TimeoutError as error:
            raise CdpError(
                "cdp_open_target_failed",
                {"stage": "open_bootstrap_page", "cdp_port": int(cdp_port), "http_method": "PUT", "reason": "timeout"},
                type(error).__name__,
            ) from error
        except json.JSONDecodeError as error:
            raise CdpError(
                "cdp_open_target_failed",
                {"stage": "open_bootstrap_page", "cdp_port": int(cdp_port), "http_method": "PUT", "reason": "invalid_json"},
                type(error).__name__,
            ) from error
        if not isinstance(data, dict) or not (data.get("id") or data.get("webSocketDebuggerUrl")):
            raise CdpError(
                "cdp_open_target_failed",
                {"stage": "open_bootstrap_page", "cdp_port": int(cdp_port), "http_method": "PUT", "reason": "missing_target_id"},
            )
        return data

    def extension_present(self, cdp_port: int) -> bool:
        return self.extension_id(cdp_port) is not None

    def extension_id(self, cdp_port: int) -> str | None:
        discovered = self.discover_extension(cdp_port, options_page="options.html", service_worker="background.js")
        return discovered.get("discovered_extension_id")

    def discover_extension(self, cdp_port: int, options_page: str, service_worker: str, expected_extension_id: str = EXPECTED_FLOWKIT_EXTENSION_ID) -> dict:
        try:
            with urlopen(f"http://127.0.0.1:{int(cdp_port)}/json/list", timeout=2.0) as response:
                targets = json.loads(response.read().decode("utf-8"))
        except Exception:
            return {"discovered_extension_id": None, "options_target_url": None, "service_worker_target_url": None}
        candidates: dict[str, dict] = {}
        for target in targets:
            url = str(target.get("url", ""))
            parsed = urlparse(url)
            if parsed.scheme != "chrome-extension" or not EXTENSION_ID_RE.match(parsed.netloc):
                continue
            path = parsed.path.lstrip("/")
            candidate = candidates.setdefault(parsed.netloc, {"discovered_extension_id": parsed.netloc, "options_target_url": None, "service_worker_target_url": None})
            if path == options_page:
                candidate["options_target_url"] = url
            if path == service_worker:
                candidate["service_worker_target_url"] = url
        if expected_extension_id in candidates:
            return candidates[expected_extension_id]
        return next(iter(candidates.values()), {"discovered_extension_id": None, "options_target_url": None, "service_worker_target_url": None})

    def verify_extension_options(self, cdp_port: int, extension_id: str, options_page: str) -> bool:
        target_url = f"chrome-extension://{extension_id}/{options_page}"
        try:
            target = self.open_url(cdp_port, target_url)
        except CdpError:
            return False
        opened_url = str(target.get("url") or target.get("targetUrl") or target_url)
        parsed = urlparse(opened_url)
        return parsed.scheme == "chrome-extension" and parsed.netloc == extension_id and parsed.path.lstrip("/") == options_page


class ExtensionBootstrapper:
    def __init__(
        self,
        registry: AccountRegistry,
        runtime: RuntimeManager | None = None,
        cdp: CdpClient | None = None,
        profiles_root: Path = PROFILES_ROOT,
        extension_dir: Path = EXTENSION_DIR,
        sleep=time.sleep,
        template_cdp_port: int = 9399,
    ):
        self.registry = registry
        self.runtime = runtime or RuntimeManager(registry)
        self.cdp = cdp or CdpClient()
        self.profiles_root = Path(profiles_root)
        self.extension_dir = Path(extension_dir)
        self.sleep = sleep
        self.template_cdp_port = int(template_cdp_port)

    @property
    def template_path(self) -> Path:
        return self.profiles_root / TEMPLATE_PROFILE_NAME

    def init_template(self) -> ExtensionBootstrapResult:
        manifest = self._manifest_info()
        if not manifest:
            return ExtensionBootstrapResult("extension_missing", ok=False, details={"extension_dir": str(self.extension_dir)})
        status = self.template_status()
        if status["template_ready"]:
            return ExtensionBootstrapResult("template_ready", ok=True, details=status)
        if status["template_marker_present"] and not status["template_marker_valid"]:
            return ExtensionBootstrapResult("template_marker_invalid", ok=False, details=status)
        if (self.template_path / "SingletonLock").exists():
            return ExtensionBootstrapResult("template_profile_running", ok=False, details={"template_path": str(self.template_path)})
        if port_is_listening(self.template_cdp_port) or not port_can_bind(self.template_cdp_port):
            return ExtensionBootstrapResult("port_conflict", ok=False, details={"chrome_cdp_port": self.template_cdp_port})
        chrome = self.runtime._find_chrome()
        if not chrome:
            return ExtensionBootstrapResult("chrome_not_found", ok=False)
        template_existed_before_init = self.template_path.exists()
        self.template_path.mkdir(parents=True, exist_ok=True)
        details = {"first_launch_verified": False, "persistence_verified": False, "verified_without_load_extension": False}
        if template_existed_before_init and self._verify_template_launch(load_extension=False):
            details.update({"first_launch_verified": True, "persistence_verified": True, "verified_without_load_extension": True})
            self.mark_template_ready_for_verified_profile(details)
            return ExtensionBootstrapResult("template_ready", ok=True, details={**self.template_status(), **details})
        first = self.runtime.popen(self.template_chrome_command(load_extension=True), cwd=str(self.extension_dir.parent))
        try:
            self.cdp.wait_ready(self.template_cdp_port, sleep=self.sleep)
            try:
                verified = self._verify_template_extension()
            except CdpError as error:
                result = error.to_bootstrap_result()
                result.details = {**(result.details or {}), **details}
                return result
            if not verified:
                return ExtensionBootstrapResult("extension_template_load_failed", ok=False, details={**details, "chrome_started_by_bootstrap": True, "compensation_result": "stopped", "reason": "first_launch_extension_missing"})
            details["first_launch_verified"] = True
        finally:
            self.runtime.inspector.terminate(first.pid)
            self._wait_port_released(self.template_cdp_port)
        second = self.runtime.popen(self.template_chrome_command(load_extension=False), cwd=str(self.extension_dir.parent))
        try:
            self.cdp.wait_ready(self.template_cdp_port, sleep=self.sleep)
            try:
                verified = self._verify_template_extension()
            except CdpError as error:
                result = error.to_bootstrap_result()
                result.details = {**(result.details or {}), **details}
                return result
            if not verified:
                return ExtensionBootstrapResult("extension_template_manual_install_required", ok=False, details={**details, "chrome_started_by_bootstrap": True, "compensation_result": "stopped", "reason": "extension_not_persisted"})
            details.update({"persistence_verified": True, "verified_without_load_extension": True})
        finally:
            self.runtime.inspector.terminate(second.pid)
            self._wait_port_released(self.template_cdp_port)
        self.mark_template_ready_for_verified_profile(details)
        return ExtensionBootstrapResult("template_ready", ok=True, details={**self.template_status(), **details})

    def template_chrome_command(self, load_extension: bool) -> list[str]:
        chrome = self.runtime._find_chrome()
        command = [
            str(chrome),
            f"--user-data-dir={self.template_path}",
            f"--remote-debugging-port={self.template_cdp_port}",
            "--no-first-run",
            "--no-default-browser-check",
        ]
        if load_extension:
            command.append(f"--load-extension={self.extension_dir}")
        command.append(FLOW_URL)
        return command

    def mark_template_ready_for_verified_profile(self, verification: dict | None = None) -> None:
        manifest = self._manifest_info()
        if not manifest:
            return
        self.template_path.mkdir(parents=True, exist_ok=True)
        marker = {
            "template": TEMPLATE_PROFILE_NAME,
            "ready": True,
            "extension_id": manifest.extension_id,
            "extension_version": manifest.extension_version,
            "options_page": manifest.options_page,
            "service_worker": manifest.service_worker,
            "extension_dir": manifest.extension_dir,
            "manifest_sha256": manifest.manifest_sha256,
            "first_launch_verified": bool((verification or {}).get("first_launch_verified")),
            "persistence_verified": bool((verification or {}).get("persistence_verified")),
            "verified_without_load_extension": bool((verification or {}).get("verified_without_load_extension")),
            "verified_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        (self.template_path / TEMPLATE_READY_FILE).write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")

    def template_ready(self) -> bool:
        return self.template_status()["template_ready"]

    def template_status(self) -> dict:
        marker_path = self.template_path / TEMPLATE_READY_FILE
        manifest = self._manifest_info()
        status = {
            "template_path": str(self.template_path),
            "template_marker_present": marker_path.is_file(),
            "template_marker_valid": False,
            "extension_id_matches": False,
            "manifest_fingerprint_matches": False,
            "persistence_verified": False,
            "template_ready": False,
        }
        if not marker_path.is_file() or not manifest:
            return status
        try:
            marker = json.loads(marker_path.read_text(encoding="utf-8"))
        except Exception:
            return status
        required = {
            "template",
            "ready",
            "extension_id",
            "extension_version",
            "options_page",
            "service_worker",
            "extension_dir",
            "manifest_sha256",
            "first_launch_verified",
            "persistence_verified",
            "verified_without_load_extension",
            "verified_at",
        }
        status["template_marker_valid"] = required.issubset(marker)
        status["extension_id_matches"] = marker.get("extension_id") == manifest.extension_id
        status["manifest_fingerprint_matches"] = marker.get("manifest_sha256") == manifest.manifest_sha256
        status["persistence_verified"] = marker.get("persistence_verified") is True
        status["template_ready"] = bool(
            status["template_marker_valid"]
            and marker.get("ready") is True
            and marker.get("template") == TEMPLATE_PROFILE_NAME
            and marker.get("extension_dir") == manifest.extension_dir
            and marker.get("extension_version") == manifest.extension_version
            and marker.get("options_page") == manifest.options_page
            and marker.get("service_worker") == manifest.service_worker
            and marker.get("first_launch_verified") is True
            and marker.get("verified_without_load_extension") is True
            and status["extension_id_matches"]
            and status["manifest_fingerprint_matches"]
            and status["persistence_verified"]
        )
        return status

    def copy_template_to_profile(self, account: AccountRecord) -> ExtensionBootstrapResult:
        if not self.template_ready():
            return ExtensionBootstrapResult("extension_template_not_ready", account.account_id, False, self.template_status())
        target = Path(account.profile_path)
        if target.exists():
            return ExtensionBootstrapResult("profile_exists", account.account_id, True, {"profile_path": str(target)})
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copytree(self.template_path, target, ignore=self._ignore_profile_entries)
        except Exception as error:
            if target.exists():
                shutil.rmtree(target, ignore_errors=True)
            return ExtensionBootstrapResult("failed", account.account_id, False, {"error": str(error), "profile_path": str(target)})
        return ExtensionBootstrapResult("profile_created_from_template", account.account_id, True, {"profile_path": str(target)})

    def bootstrap_account(self, account_id: str, repair: bool = False) -> ExtensionBootstrapResult:
        account = self.registry.get(account_id)
        if not account:
            return ExtensionBootstrapResult("account_not_found", account_id, False)
        validation = self._validate_account_config(account)
        if validation:
            return validation
        profile = Path(account.profile_path)
        if not profile.exists():
            copied = self.copy_template_to_profile(account)
            if not copied.ok:
                return copied
        elif not repair:
            return ExtensionBootstrapResult("profile_exists", account.account_id, True, {"profile_path": str(profile)})

        chrome = self.runtime.open_login(account.account_id)
        if chrome.result not in {"opened", "already_running"}:
            return ExtensionBootstrapResult(chrome.result, account.account_id, False, chrome.details)
        chrome_started_by_bootstrap = chrome.result == "opened"
        worker_started = False
        try:
            self.cdp.wait_ready(account.chrome_cdp_port, sleep=self.sleep)
            manifest = self._manifest_info()
            if not manifest:
                return ExtensionBootstrapResult("extension_missing", account.account_id, False, {"extension_dir": str(self.extension_dir)})
            discovery = self.cdp.discover_extension(account.chrome_cdp_port, manifest.options_page, manifest.service_worker)
            extension_id = discovery.get("discovered_extension_id")
            if extension_id and extension_id != manifest.extension_id:
                return ExtensionBootstrapResult(
                    "extension_id_mismatch",
                    account.account_id,
                    False,
                    {"expected_extension_id": manifest.extension_id, **discovery},
                )
            if extension_id != manifest.extension_id:
                result = "extension_install_required" if self.template_ready() else "extension_template_not_ready"
                return ExtensionBootstrapResult(
                    result,
                    account.account_id,
                    False,
                    {
                        "account_id": account.account_id,
                        "profile_path": account.profile_path,
                        "extension_id": extension_id,
                        "expected_extension_id": manifest.extension_id,
                        **discovery,
                        "template_ready": self.template_ready(),
                        "repair": repair,
                    },
                )

            nonce = secrets.token_urlsafe(18)
            url = self.bootstrap_url(account, nonce, extension_id)
            self.cdp.open_url(account.chrome_cdp_port, url)

            start = self.runtime.start_one(account.account_id)
            worker_started = start.result == "started"
            if start.result not in {"started", "already_running"}:
                compensation = self._compensate(account.account_id, chrome_started_by_bootstrap, worker_started)
                return ExtensionBootstrapResult(start.result, account.account_id, False, {**(start.details or {}), **compensation})
            status = self._wait_extension_ready(account)
            if status.details.get("account_match"):
                return ExtensionBootstrapResult(
                    "extension_bootstrapped",
                    account.account_id,
                    True,
                    self._extension_details(account, status.details),
                )
            compensation = self._compensate(account.account_id, chrome_started_by_bootstrap, worker_started)
            result = "extension_not_connected"
            if status.details.get("extension_connected") and not status.details.get("account_match"):
                result = "account_mismatch"
            return ExtensionBootstrapResult(result, account.account_id, False, {**self._extension_details(account, status.details), **compensation})
        except CdpError as error:
            compensation = self._compensate(account.account_id, chrome_started_by_bootstrap, worker_started)
            result = error.to_bootstrap_result(account.account_id)
            result.details = {**(result.details or {}), **compensation}
            return result
        except Exception as error:
            compensation = self._compensate(account.account_id, chrome_started_by_bootstrap, worker_started)
            return ExtensionBootstrapResult(
                "failed",
                account.account_id,
                False,
                {"stage": "bootstrap_extension", "error": type(error).__name__, **compensation},
            )

    def bootstrap_batch(self, account_ids: list[str], repair: bool = False) -> ExtensionBootstrapResult:
        results = []
        for account_id in account_ids:
            try:
                results.append(self.bootstrap_account(account_id, repair=repair).to_dict())
            except Exception as error:
                results.append(ExtensionBootstrapResult("failed", account_id, False, {"stage": "bootstrap_batch", "error": type(error).__name__}).to_dict())
        return ExtensionBootstrapResult("bootstrap_batch_completed", ok=all(item["ok"] for item in results), details={"accounts": results})

    def bootstrap_url(self, account: AccountRecord, nonce: str, extension_id: str) -> str:
        query = urlencode({
            "bootstrap": "1",
            "account_id": account.account_id,
            "ws_url": f"ws://127.0.0.1:{account.extension_ws_port}",
            "nonce": nonce,
        })
        return f"chrome-extension://{extension_id}/options.html?{query}"

    def _validate_account_config(self, account: AccountRecord) -> ExtensionBootstrapResult | None:
        if not ACCOUNT_ID_RE.match(account.account_id):
            return ExtensionBootstrapResult("invalid_account_id", account.account_id, False)
        port = int(account.extension_ws_port)
        if port < 1 or port > 65535:
            return ExtensionBootstrapResult("invalid_ws_url", account.account_id, False, {"extension_ws_port": account.extension_ws_port})
        return None

    def _wait_extension_ready(self, account: AccountRecord, attempts: int = 10) -> RuntimeResult:
        status = self.runtime.status(account.account_id)
        for _ in range(attempts - 1):
            if status.details.get("account_match"):
                return status
            self.sleep(0.2)
            status = self.runtime.status(account.account_id)
        return status

    def _wait_extension_present(self, cdp_port: int, attempts: int = 10) -> bool:
        for _ in range(attempts):
            if self.cdp.extension_present(cdp_port):
                return True
            self.sleep(0.2)
        return False

    def _verify_template_launch(self, load_extension: bool) -> bool:
        proc = self.runtime.popen(self.template_chrome_command(load_extension=load_extension), cwd=str(self.extension_dir.parent))
        try:
            self.cdp.wait_ready(self.template_cdp_port, sleep=self.sleep)
            return self._verify_template_extension()
        finally:
            self.runtime.inspector.terminate(proc.pid)
            self._wait_port_released(self.template_cdp_port)

    def _verify_template_extension(self) -> bool:
        manifest = self._manifest_info()
        if not manifest:
            return False
        discovery = self.cdp.discover_extension(self.template_cdp_port, manifest.options_page, manifest.service_worker)
        discovered_id = discovery.get("discovered_extension_id")
        if discovered_id and discovered_id != manifest.extension_id:
            raise CdpError("extension_id_mismatch", {"expected_extension_id": manifest.extension_id, **discovery})
        if discovered_id != manifest.extension_id:
            return False
        return self.cdp.verify_extension_options(self.template_cdp_port, manifest.extension_id, manifest.options_page)

    def _wait_port_released(self, port: int, attempts: int = 20) -> bool:
        for _ in range(attempts):
            if not port_is_listening(port):
                return True
            self.sleep(0.1)
        return not port_is_listening(port)

    def _manifest_info(self) -> ExtensionIdentity | None:
        manifest_path = self.extension_dir / "manifest.json"
        try:
            raw = manifest_path.read_bytes()
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            return None
        options_page = data.get("options_page")
        version = data.get("version")
        service_worker = (((data.get("background") or {}).get("service_worker")) or "").strip()
        if not options_page or not version or not service_worker:
            return None
        return ExtensionIdentity(
            extension_id=EXPECTED_FLOWKIT_EXTENSION_ID,
            extension_name=str(data.get("name") or "Flow Kit"),
            extension_version=str(version),
            options_page=str(options_page),
            service_worker=service_worker,
            extension_dir=str(self.extension_dir.resolve()),
            manifest_sha256=hashlib.sha256(raw).hexdigest(),
        )

    def _compensate(self, account_id: str, chrome_started: bool, worker_started: bool) -> dict:
        details = {
            "chrome_started_by_bootstrap": chrome_started,
            "worker_started_by_bootstrap": worker_started,
            "compensation_result": "not_needed",
        }
        if chrome_started or worker_started:
            stopped = self.runtime.stop_one(account_id)
            details["compensation_result"] = stopped.result
        return details

    def _extension_details(self, account: AccountRecord, status: dict) -> dict:
        extension_status = "extension_not_connected"
        if status.get("extension_connected") and status.get("account_match"):
            extension_status = "extension_ready"
        elif status.get("extension_connected"):
            extension_status = "account_mismatch"
        return {
            **status,
            "extension_bootstrap_status": extension_status,
            "extension_expected_account_id": account.account_id,
            "extension_expected_ws_url": f"ws://127.0.0.1:{account.extension_ws_port}",
        }

    def _ignore_profile_entries(self, directory: str, names: list[str]) -> set[str]:
        ignored = set()
        for name in names:
            if name in EXCLUDED_PROFILE_NAMES or name.endswith(".log") or name.endswith(".tmp"):
                ignored.add(name)
        return ignored
