"""Flow Kit extension template and account bootstrap helpers."""
from __future__ import annotations

import json
import os
import re
import secrets
import hashlib
import shutil
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urlparse
from urllib.request import Request, urlopen

from .paths import EXTENSION_DIR, PROFILES_ROOT
from .port_allocator import port_can_bind, port_is_listening
from .process_manager import RuntimeManager, RuntimeResult
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
CREDENTIAL_STORAGE_POLICY_VERSION = 2
CREDENTIAL_STORAGE_RELATIVE_DIRS = (
    Path("Default") / "Local Extension Settings" / EXPECTED_FLOWKIT_EXTENSION_ID,
    Path("Default") / "Local Storage",
    Path("Default") / "IndexedDB",
    Path("Default") / "Session Storage",
    Path("Default") / "Service Worker",
    Path("Default") / "Sessions",
    Path("Default") / "Storage",
    Path("Default") / "WebStorage",
    Path("Default") / "SharedStorage",
    Path("Default") / "SharedStorage-wal",
    Path("Default") / "SharedStorage-shm",
    Path("Default") / "SharedStorage-journal",
)
ALLOWED_REGISTERED_EMPTY_DIRS = {
    Path("."),
    Path("Default"),
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
        targets = self.list_targets(cdp_port)
        summary = self.target_summary(targets, expected_extension_id, options_page, service_worker)
        if targets is None:
            return {
                "discovered_extension_id": None,
                "options_target_url": None,
                "service_worker_target_url": None,
                "target_summary": summary,
            }
        candidates: dict[str, dict] = {}
        for target in targets:
            url = str(target.get("url", ""))
            parsed = urlparse(url)
            if parsed.scheme != "chrome-extension" or not EXTENSION_ID_RE.match(parsed.netloc):
                continue
            path = parsed.path.lstrip("/")
            candidate = candidates.setdefault(parsed.netloc, {"discovered_extension_id": parsed.netloc, "options_target_url": None, "service_worker_target_url": None})
            if path == options_page:
                candidate["options_target_url"] = f"chrome-extension://{parsed.netloc}/{options_page}?<query-redacted>" if parsed.query else url
            if path == service_worker:
                candidate["service_worker_target_url"] = url
        result = candidates.get(expected_extension_id) or next(iter(candidates.values()), {"discovered_extension_id": None, "options_target_url": None, "service_worker_target_url": None})
        result["target_summary"] = summary
        return result

    def list_targets(self, cdp_port: int) -> list[dict] | None:
        try:
            with urlopen(f"http://127.0.0.1:{int(cdp_port)}/json/list", timeout=2.0) as response:
                data = json.loads(response.read().decode("utf-8"))
        except Exception:
            return None
        return data if isinstance(data, list) else None

    def target_summary(self, targets: list[dict] | None, expected_extension_id: str, options_page: str, service_worker: str) -> dict:
        summary = {
            "options_target_seen": False,
            "service_worker_target_seen": False,
            "target_counts": {},
            "targets": [],
        }
        if not targets:
            return summary
        for target in targets:
            target_type = str(target.get("type") or "")
            url = str(target.get("url", ""))
            parsed = urlparse(url)
            category = "target_non_extension"
            extension_id_matches = False
            safe_url = None
            if parsed.scheme == "chrome-extension" and EXTENSION_ID_RE.match(parsed.netloc):
                extension_id_matches = parsed.netloc == expected_extension_id
                path = parsed.path.lstrip("/")
                if extension_id_matches and path == options_page:
                    category = "target_extension_options"
                    summary["options_target_seen"] = True
                    safe_url = f"chrome-extension://{expected_extension_id}/{options_page}?<query-redacted>"
                elif extension_id_matches and path == service_worker:
                    category = "target_extension_service_worker"
                    summary["service_worker_target_seen"] = True
                    safe_url = f"chrome-extension://{expected_extension_id}/{service_worker}"
                else:
                    category = "target_other_extension"
            elif url == "about:blank":
                category = "target_about_blank"
                safe_url = "about:blank"
            summary["target_counts"][category] = summary["target_counts"].get(category, 0) + 1
            summary["targets"].append({
                "type": target_type,
                "category": category,
                "extension_id_matches": extension_id_matches,
                "url": safe_url,
            })
        return summary

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
        self._bootstrap_chrome_processes: dict[int, object] = {}

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
        command.append("about:blank")
        return command

    def bootstrap_chrome_command(self, account: AccountRecord, bootstrap_url: str) -> list[str]:
        chrome = self.runtime._find_chrome()
        return [
            str(chrome),
            f"--user-data-dir={Path(account.profile_path)}",
            f"--remote-debugging-port={account.chrome_cdp_port}",
            "--no-first-run",
            "--no-default-browser-check",
            bootstrap_url,
        ]

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
            sanitized = self.sanitize_copied_profile(target)
            if not sanitized.ok:
                shutil.rmtree(target, ignore_errors=True)
                return sanitized
        except Exception as error:
            if target.exists():
                shutil.rmtree(target, ignore_errors=True)
            return ExtensionBootstrapResult("failed", account.account_id, False, {"error": str(error), "profile_path": str(target)})
        return ExtensionBootstrapResult(
            "profile_created_from_template",
            account.account_id,
            True,
            {
                "profile_path": str(target),
                "credential_storage_sanitized": True,
                "credential_storage_policy_version": CREDENTIAL_STORAGE_POLICY_VERSION,
            },
        )

    def bootstrap_account(self, account_id: str, repair: bool = False) -> ExtensionBootstrapResult:
        account = self.registry.get(account_id)
        if not account:
            return ExtensionBootstrapResult("account_not_found", account_id, False)
        validation = self._validate_account_config(account)
        if validation:
            return validation
        profile = Path(account.profile_path)
        credential_storage_sanitized = False
        profile_initialization_mode = "repaired_existing_profile" if repair else None
        if not profile.exists():
            copied = self.copy_template_to_profile(account)
            if not copied.ok:
                return copied
            credential_storage_sanitized = bool((copied.details or {}).get("credential_storage_sanitized"))
            profile_initialization_mode = "copied_to_missing_profile"
        elif not repair:
            safety = self._profile_rebuild_safety(account)
            if safety["profile_state"] == "registered_empty" and safety["safe_to_rebuild"]:
                cleanup = self._remove_registered_empty_profile(account, safety)
                if cleanup:
                    return cleanup
                copied = self.copy_template_to_profile(account)
                if not copied.ok:
                    return copied
                credential_storage_sanitized = bool((copied.details or {}).get("credential_storage_sanitized"))
                profile_initialization_mode = "rebuilt_registered_empty_profile"
            elif safety["profile_state"] == "bootstrapped_profile":
                return ExtensionBootstrapResult("profile_exists", account.account_id, True, {"profile_path": str(profile), **safety})
            else:
                return ExtensionBootstrapResult("profile_partial_requires_manual_review", account.account_id, False, safety)

        manifest = self._manifest_info()
        if not manifest:
            return ExtensionBootstrapResult("extension_missing", account.account_id, False, {"extension_dir": str(self.extension_dir)})
        nonce = secrets.token_urlsafe(18)
        url = self.bootstrap_url(account, nonce, manifest.extension_id)
        initialization_details = {
            "profile_initialization_mode": profile_initialization_mode,
            "credential_storage_sanitized": credential_storage_sanitized,
            "credential_storage_policy_version": CREDENTIAL_STORAGE_POLICY_VERSION if credential_storage_sanitized else None,
        }
        worker = self.runtime.start_worker_only(account.account_id)
        worker_started = worker.result == "started"
        worker_pid = (worker.details or {}).get("worker_pid")
        if worker.result not in {"started", "already_running"}:
            return ExtensionBootstrapResult(
                worker.result,
                account.account_id,
                False,
                {
                    **initialization_details,
                    **(worker.details or {}),
                    "stage": "start_worker_only",
                    "chrome_started_by_bootstrap": False,
                    "worker_started_by_bootstrap": worker_started,
                },
            )
        chrome = self._open_bootstrap_chrome(account, url)
        if chrome.result not in {"opened", "already_running"}:
            compensation = self._compensate_owned(account, False, worker_started, None, worker_pid)
            return ExtensionBootstrapResult(
                chrome.result,
                account.account_id,
                False,
                {
                    **initialization_details,
                    **(chrome.details or {}),
                    "stage": "open_bootstrap_chrome",
                    **compensation,
                },
            )
        chrome_started_by_bootstrap = chrome.result == "opened"
        chrome_pid = (chrome.details or {}).get("chrome_pid")
        diagnostics = {
            "chrome_spawned_at": (chrome.details or {}).get("chrome_spawned_at"),
            "chrome_pid": chrome_pid,
            "bootstrap_chrome_command": (chrome.details or {}).get("command"),
            "bootstrap_chrome_stdout_log": (chrome.details or {}).get("stdout_log"),
            "bootstrap_chrome_stderr_log": (chrome.details or {}).get("stderr_log"),
        }
        try:
            self.cdp.wait_ready(account.chrome_cdp_port, sleep=self.sleep)
            diagnostics["cdp_ready_at"] = self._utc_now()
            discovery = self.cdp.discover_extension(account.chrome_cdp_port, manifest.options_page, manifest.service_worker)
            target_summary = discovery.get("target_summary") or {}
            diagnostics.update({
                "target_summary_before_wait": target_summary,
                "options_target_seen": bool(target_summary.get("options_target_seen")),
                "service_worker_target_seen": bool(target_summary.get("service_worker_target_seen")),
            })
            extension_id = discovery.get("discovered_extension_id")
            if extension_id and extension_id != manifest.extension_id:
                return ExtensionBootstrapResult(
                    "extension_id_mismatch",
                    account.account_id,
                    False,
                    {**initialization_details, **diagnostics, "expected_extension_id": manifest.extension_id, **discovery},
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
                        **initialization_details,
                        **diagnostics,
                    },
                )

            if chrome.result == "already_running":
                self.cdp.open_url(account.chrome_cdp_port, self.bootstrap_url(account, nonce, extension_id))

            diagnostics["wait_extension_ready_started_at"] = self._utc_now()
            status = self._wait_extension_ready(account, chrome_pid=chrome_pid, diagnostics=diagnostics)
            if status.details.get("account_match"):
                details = self._extension_details(account, status.details)
                details.update({
                    **initialization_details,
                    **diagnostics,
                    "chrome_started_by_bootstrap": chrome_started_by_bootstrap,
                    "worker_started_by_bootstrap": worker_started,
                })
                return ExtensionBootstrapResult(
                    "extension_bootstrapped",
                    account.account_id,
                    True,
                    details,
                )
            compensation = self._compensate_owned(account, chrome_started_by_bootstrap, worker_started, chrome_pid, worker_pid)
            result = status.result if status.result == "bootstrap_chrome_exited" else "extension_not_connected"
            if status.details.get("extension_connected") and not status.details.get("account_match"):
                result = "account_mismatch"
            return ExtensionBootstrapResult(result, account.account_id, False, {**initialization_details, **diagnostics, **self._extension_details(account, status.details), "stage": "wait_extension_ready", **compensation})
        except CdpError as error:
            compensation = self._compensate_owned(account, chrome_started_by_bootstrap, worker_started, chrome_pid, worker_pid)
            result = error.to_bootstrap_result(account.account_id)
            result.details = {**initialization_details, **diagnostics, **(result.details or {}), **compensation}
            return result
        except Exception as error:
            compensation = self._compensate_owned(account, chrome_started_by_bootstrap, worker_started, chrome_pid, worker_pid)
            return ExtensionBootstrapResult(
                "failed",
                account.account_id,
                False,
                {**initialization_details, **diagnostics, "stage": "bootstrap_extension", "error": type(error).__name__, **compensation},
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
            "api_url": f"http://127.0.0.1:{account.worker_api_port}",
            "nonce": nonce,
        })
        return f"chrome-extension://{extension_id}/options.html?{query}"

    def _open_bootstrap_chrome(self, account: AccountRecord, bootstrap_url: str) -> RuntimeResult:
        if self.runtime._owned_chrome_running(account):
            return RuntimeResult("already_running", account.account_id, True)
        conflict = self.runtime._listening_port_conflict(account, "chrome_cdp_port", account.chrome_cdp_port)
        if conflict:
            return conflict
        command = self.bootstrap_chrome_command(account, bootstrap_url)
        stdout_path, stderr_path = self._bootstrap_chrome_log_files(account)
        stdout_handle = stdout_path.open("ab")
        stderr_handle = stderr_path.open("ab")
        try:
            proc = self.runtime.popen(command, cwd=str(self.extension_dir.parent), stdout=stdout_handle, stderr=stderr_handle)
        except Exception:
            stdout_handle.close()
            stderr_handle.close()
            raise
        finally:
            if not stdout_handle.closed:
                stdout_handle.close()
            if not stderr_handle.closed:
                stderr_handle.close()
        self.registry.mark_started(account.account_id, chrome_pid=proc.pid)
        self._bootstrap_chrome_processes[int(proc.pid)] = proc
        return RuntimeResult(
            "opened",
            account.account_id,
            True,
            details={
                "chrome_pid": proc.pid,
                "chrome_spawned_at": self._utc_now(),
                "command": self._redacted_bootstrap_command(command),
                "stdout_log": str(stdout_path),
                "stderr_log": str(stderr_path),
            },
        )

    def sanitize_copied_profile(self, profile_path: Path) -> ExtensionBootstrapResult:
        cleaned = []
        for relative in CREDENTIAL_STORAGE_RELATIVE_DIRS:
            target = Path(profile_path) / relative
            item = {
                "path": str(relative),
                "existed": target.exists(),
                "removed": False,
            }
            if target.exists():
                try:
                    self._remove_path(target)
                    item["removed"] = True
                except Exception as error:
                    item["error"] = type(error).__name__
                    cleaned.append(item)
                    return ExtensionBootstrapResult(
                        "credential_storage_cleanup_failed",
                        ok=False,
                        details={
                            "profile_path": str(profile_path),
                            "credential_storage_sanitized": False,
                            "credential_storage_policy_version": CREDENTIAL_STORAGE_POLICY_VERSION,
                            "cleaned": cleaned,
                        },
                    )
            cleaned.append(item)
        return ExtensionBootstrapResult(
            "credential_storage_sanitized",
            ok=True,
            details={
                "profile_path": str(profile_path),
                "credential_storage_sanitized": True,
                "credential_storage_policy_version": CREDENTIAL_STORAGE_POLICY_VERSION,
                "cleaned": cleaned,
            },
        )

    def _profile_rebuild_safety(self, account: AccountRecord) -> dict:
        profile = Path(account.profile_path)
        details = {
            "stage": "profile_initialization",
            "account_id": account.account_id,
            "profile_path": str(profile),
            "profile_state": "partial_or_unsafe",
            "safe_to_rebuild": False,
            "blocking_paths": [],
        }
        try:
            profile_resolved = profile.resolve(strict=False)
            profiles_root_resolved = self.profiles_root.resolve(strict=False)
            expected_resolved = (self.profiles_root / account.account_id).resolve(strict=False)
            template_resolved = self.template_path.resolve(strict=False)
        except Exception:
            details["blocking_paths"] = ["<path_resolution_failed>"]
            return details
        if profile_resolved != expected_resolved:
            details["blocking_paths"] = ["<account_profile_path_mismatch>"]
            return details
        try:
            profile_resolved.relative_to(profiles_root_resolved)
        except ValueError:
            details["blocking_paths"] = ["<profile_path_outside_profiles_root>"]
            return details
        if profile_resolved == template_resolved or profile.name == TEMPLATE_PROFILE_NAME:
            details["blocking_paths"] = ["<template_profile_path>"]
            return details
        if not profile.exists():
            details["profile_state"] = "missing"
            return details
        if profile.is_symlink():
            details["blocking_paths"] = ["<profile_path_symlink>"]
            return details
        blocking: list[str] = []
        for child in profile.rglob("*"):
            relative = child.relative_to(profile)
            safe_relative = relative.as_posix()
            if child.is_symlink():
                blocking.append(safe_relative)
                continue
            if child.is_file():
                blocking.append(safe_relative)
                continue
            if child.is_dir():
                try:
                    if any(child.iterdir()):
                        blocking.append(safe_relative)
                    elif relative not in ALLOWED_REGISTERED_EMPTY_DIRS:
                        blocking.append(safe_relative)
                except Exception:
                    blocking.append(safe_relative)
        if blocking:
            details["blocking_paths"] = sorted(blocking)[:25]
            bootstrapped_markers = {
                "Default/Preferences",
                "Default/Secure Preferences",
                f"Default/Local Extension Settings/{EXPECTED_FLOWKIT_EXTENSION_ID}",
            }
            if any(
                path in bootstrapped_markers
                or path.startswith(f"Default/Local Extension Settings/{EXPECTED_FLOWKIT_EXTENSION_ID}/")
                or path.startswith("Default/Extensions")
                for path in blocking
            ):
                details["profile_state"] = "bootstrapped_profile"
            return details
        details["profile_state"] = "registered_empty"
        details["safe_to_rebuild"] = True
        return details

    def _remove_registered_empty_profile(self, account: AccountRecord, safety: dict) -> ExtensionBootstrapResult | None:
        profile = Path(account.profile_path)
        try:
            shutil.rmtree(profile, onerror=self._make_writable_and_retry)
        except Exception as error:
            return ExtensionBootstrapResult(
                "profile_registered_empty_cleanup_failed",
                account.account_id,
                False,
                {
                    **safety,
                    "error": type(error).__name__,
                },
            )
        return None

    def _remove_path(self, target: Path) -> None:
        if target.is_dir():
            shutil.rmtree(target, onerror=self._make_writable_and_retry)
            return
        try:
            target.unlink()
        except PermissionError:
            os.chmod(target, stat.S_IWRITE)
            target.unlink()

    def _make_writable_and_retry(self, function, path, _exc_info) -> None:
        os.chmod(path, stat.S_IWRITE)
        function(path)

    def _validate_account_config(self, account: AccountRecord) -> ExtensionBootstrapResult | None:
        if not ACCOUNT_ID_RE.match(account.account_id):
            return ExtensionBootstrapResult("invalid_account_id", account.account_id, False)
        port = int(account.extension_ws_port)
        if port < 1 or port > 65535:
            return ExtensionBootstrapResult("invalid_ws_url", account.account_id, False, {"extension_ws_port": account.extension_ws_port})
        return None

    def _wait_extension_ready(self, account: AccountRecord, attempts: int = 10, chrome_pid: int | None = None, diagnostics: dict | None = None) -> RuntimeResult:
        status = self.runtime.status(account.account_id)
        for _ in range(attempts - 1):
            if status.details.get("account_match"):
                return status
            if chrome_pid and status.details.get("chrome_process_alive") is False and status.details.get("chrome_cdp_reachable") is False:
                details = {
                    **status.details,
                    **(diagnostics or {}),
                    "chrome_exit_detected_at": self._utc_now(),
                    "chrome_exit_code": self._chrome_exit_code(chrome_pid),
                    "compensation_pre_chrome_alive": False,
                    "compensation_pre_cdp_reachable": False,
                    "compensation_pre_worker_alive": status.details.get("worker_process_alive"),
                    "compensation_pre_worker_health_reachable": status.details.get("worker_health_reachable"),
                }
                target_summary = self._safe_target_summary(account)
                if target_summary:
                    details["target_summary_last_seen"] = target_summary
                return RuntimeResult("bootstrap_chrome_exited", account.account_id, False, details=details)
            self.sleep(0.2)
            status = self.runtime.status(account.account_id)
        return status

    def _safe_target_summary(self, account: AccountRecord) -> dict | None:
        manifest = self._manifest_info()
        if not manifest:
            return None
        try:
            targets = self.cdp.list_targets(account.chrome_cdp_port)
            return self.cdp.target_summary(targets, manifest.extension_id, manifest.options_page, manifest.service_worker)
        except Exception:
            return None

    def _chrome_exit_code(self, chrome_pid: int | None) -> int | None:
        if not chrome_pid:
            return None
        proc = self._bootstrap_chrome_processes.get(int(chrome_pid))
        if proc is not None and hasattr(proc, "poll"):
            try:
                return proc.poll()
            except Exception:
                return None
        return None

    def _bootstrap_chrome_log_files(self, account: AccountRecord) -> tuple[Path, Path]:
        log_dir = Path(getattr(self.runtime, "log_dir", self.profiles_root.parent / "logs" / "runtime"))
        log_dir.mkdir(parents=True, exist_ok=True)
        return (
            log_dir / f"{account.account_id}-bootstrap-chrome-stdout.log",
            log_dir / f"{account.account_id}-bootstrap-chrome-stderr.log",
        )

    def _redacted_bootstrap_command(self, command: list[str]) -> list[str]:
        redacted = []
        for part in command:
            text = str(part)
            if "chrome-extension://" in text and "options.html?" in text:
                parsed = urlparse(text)
                params = dict((key, values[-1]) for key, values in parse_qs(parsed.query).items())
                safe_query = urlencode({
                    "bootstrap": params.get("bootstrap", "1"),
                    "account_id": params.get("account_id", ""),
                    "ws_url": params.get("ws_url", ""),
                    "api_url": params.get("api_url", ""),
                    "nonce": "<redacted>",
                })
                redacted.append(f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{safe_query}")
                continue
            if any(secret in text.lower() for secret in ("token", "cookie", "authorization", "flowkey", "callbacksecret")):
                redacted.append("<redacted>")
                continue
            redacted.append(text)
        return redacted

    def _utc_now(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

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

    def _compensate_owned(self, account: AccountRecord, chrome_started: bool, worker_started: bool, chrome_pid: int | None, worker_pid: int | None) -> dict:
        details = {
            "chrome_started_by_bootstrap": chrome_started,
            "worker_started_by_bootstrap": worker_started,
            "compensation_result": "not_needed",
        }
        if chrome_started or worker_started:
            self.runtime._stop_started(
                account,
                int(worker_pid) if worker_started and worker_pid else None,
                int(chrome_pid) if chrome_started and chrome_pid else None,
            )
            details["compensation_result"] = "stopped"
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
