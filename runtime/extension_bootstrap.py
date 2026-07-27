"""Flow Kit extension template and account bootstrap helpers."""
from __future__ import annotations

import json
import os
import re
import secrets
import hashlib
import shutil
import stat
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urlparse
from urllib.request import Request, urlopen

from .extension_paths import chrome_extension_arg_value, chrome_extension_args
from .paths import EXTENSION_DIR, PROFILES_ROOT
from .port_allocator import port_can_bind, port_is_listening
from .process_manager import RuntimeManager, RuntimeResult
from .registry import AccountRecord, AccountRegistry
from .windows_lock_probe import probe_dawn_cache_lock


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
GPU_CACHE_RELATIVE_DIRS = (
    Path("GPUCache"),
    Path("Default") / "GPUCache",
    Path("GPUPersistentCache"),
    Path("Default") / "GPUPersistentCache",
    Path("GraphiteDawnCache"),
    Path("DawnGraphiteCache"),
    Path("GrShaderCache"),
    Path("ShaderCache"),
    Path("Default") / "GrShaderCache",
    Path("Default") / "ShaderCache",
)
GPU_RETRY_FAILURE_CLASSES = {
    "gpu_process_unusable",
    "gpu_process_crash_loop",
    "gpu_cache_sharing_violation",
}
VOLATILE_BOOTSTRAP_CACHE_DIR = Path("GPUPersistentCache")
DISABLE_SKIA_GRAPHITE_ARG = "--disable-skia-graphite"
ENABLE_SKIA_GRAPHITE_ARG = "--enable-skia-graphite"
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
    def wait_ready(
        self,
        cdp_port: int,
        attempts: int = 20,
        delay_seconds: float = 0.25,
        sleep=time.sleep,
        chrome_process=None,
        chrome_pid: int | None = None,
    ) -> dict:
        started = time.monotonic()
        last_error = None
        for attempt in range(1, attempts + 1):
            exit_code = self._poll_process(chrome_process)
            if exit_code is not None:
                raise CdpError(
                    "bootstrap_chrome_exited",
                    {
                        "stage": "wait_cdp_ready",
                        "cdp_port": int(cdp_port),
                        "attempts": attempt,
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                        "last_error": "chrome_exited",
                        "chrome_pid": chrome_pid,
                        "chrome_exit_code": exit_code,
                    },
                )
            try:
                with urlopen(f"http://127.0.0.1:{int(cdp_port)}/json/version", timeout=2.0) as response:
                    data = json.loads(response.read().decode("utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception as error:
                last_error = self._cdp_probe_error_class(error)
            exit_code = self._poll_process(chrome_process)
            if exit_code is not None:
                raise CdpError(
                    "bootstrap_chrome_exited",
                    {
                        "stage": "wait_cdp_ready",
                        "cdp_port": int(cdp_port),
                        "attempts": attempt,
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                        "last_error": "chrome_exited",
                        "chrome_pid": chrome_pid,
                        "chrome_exit_code": exit_code,
                    },
                )
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

    def _poll_process(self, process) -> int | None:
        if process is None or not hasattr(process, "poll"):
            return None
        try:
            return process.poll()
        except Exception:
            return None

    def _cdp_probe_error_class(self, error: Exception) -> str:
        text = str(error).lower()
        reason = getattr(error, "reason", None)
        if reason is not None:
            text = f"{text} {reason}".lower()
        if "timed out" in text or "timeout" in text:
            return "connection_timeout"
        if "connection refused" in text or "actively refused" in text:
            return "connection_refused"
        if "connection reset" in text or "forcibly closed" in text:
            return "connection_reset"
        if isinstance(error, (json.JSONDecodeError, HTTPError)):
            return "invalid_response"
        return "unknown"

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
                "extension_id": parsed.netloc if parsed.scheme == "chrome-extension" and EXTENSION_ID_RE.match(parsed.netloc) else None,
                "path": parsed.path.lstrip("/") if parsed.scheme == "chrome-extension" else None,
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
        self._dawn_lock_probe_watchers: dict[int, tuple[threading.Event, threading.Thread]] = {}
        self._dawn_lock_probe_results: dict[int, dict] = {}
        self._dawn_lock_probe_attempt_results: dict[int, dict] = {}

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

    def bootstrap_chrome_command(self, account: AccountRecord, bootstrap_url: str, extra_args: list[str] | None = None) -> list[str]:
        chrome = self.runtime._find_chrome()
        command = [
            str(chrome),
            f"--user-data-dir={Path(account.profile_path)}",
            f"--remote-debugging-port={account.chrome_cdp_port}",
            "--no-first-run",
            "--no-default-browser-check",
            DISABLE_SKIA_GRAPHITE_ARG,
        ]
        command.extend(chrome_extension_args(self.extension_dir))
        command.extend(self._normalized_bootstrap_extra_args(extra_args or []))
        command.append("about:blank")
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
                **self._profile_extension_state_gate(target),
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
        profile_initialization_mode = None
        safety = None
        if not profile.exists():
            copied = self.copy_template_to_profile(account)
            if not copied.ok:
                return copied
            credential_storage_sanitized = bool((copied.details or {}).get("credential_storage_sanitized"))
            profile_initialization_mode = "copied_to_missing_profile"
        else:
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
            elif not repair and safety["profile_state"] == "bootstrapped_profile":
                return ExtensionBootstrapResult("profile_exists", account.account_id, True, {"profile_path": str(profile), **safety})
            elif repair:
                profile_initialization_mode = "repaired_existing_profile"
            else:
                return ExtensionBootstrapResult("profile_partial_requires_manual_review", account.account_id, False, safety)

        manifest = self._manifest_info()
        if not manifest:
            return ExtensionBootstrapResult("extension_missing", account.account_id, False, {"extension_dir": str(self.extension_dir)})
        nonce = secrets.token_urlsafe(18)
        url = self.bootstrap_url(account, nonce, EXPECTED_FLOWKIT_EXTENSION_ID)
        profile_extension_gate = self._profile_extension_state_gate(profile)
        initialization_details = {
            "profile_initialization_mode": profile_initialization_mode,
            "registered_empty_safe_to_rebuild": bool((safety or {}).get("profile_state") == "registered_empty" and (safety or {}).get("safe_to_rebuild")),
            "credential_storage_sanitized": credential_storage_sanitized,
            "credential_storage_policy_version": CREDENTIAL_STORAGE_POLICY_VERSION if credential_storage_sanitized else None,
            **profile_extension_gate,
            **self._bootstrap_url_extension_diagnostics(url, EXPECTED_FLOWKIT_EXTENSION_ID),
        }
        if profile_initialization_mode == "rebuilt_registered_empty_profile":
            volatile_cleanup = self._cleanup_bootstrap_volatile_cache(account, initialization_details, attempt=1)
            initialization_details.update(volatile_cleanup["details"])
            if not volatile_cleanup["ok"]:
                return ExtensionBootstrapResult(volatile_cleanup["reason"], account.account_id, False, {**initialization_details, "stage": "volatile_cache_cleanup"})
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
            "bootstrap_chrome_attempts": 1,
            "bootstrap_chrome_retry_used": False,
            "bootstrap_chrome_retry_eligible": False,
            "first_attempt_chrome_pid": chrome_pid,
            "first_attempt_stdout_log": (chrome.details or {}).get("stdout_log"),
            "first_attempt_stderr_log": (chrome.details or {}).get("stderr_log"),
            "bootstrap_disable_skia_graphite": bool((chrome.details or {}).get("bootstrap_disable_skia_graphite")),
            "disable_skia_graphite_present": bool((chrome.details or {}).get("disable_skia_graphite_present")),
            "bootstrap_chrome_attempt_diagnostics": [
                *list(initialization_details.get("bootstrap_chrome_attempt_diagnostics") or []),
                *list((chrome.details or {}).get("bootstrap_chrome_attempt_diagnostics") or []),
            ],
        }
        self._annotate_launch_attempt_diagnostics(diagnostics, initialization_details)
        try:
            try:
                version = self.cdp.wait_ready(
                    account.chrome_cdp_port,
                    sleep=self.sleep,
                    chrome_process=self._bootstrap_chrome_processes.get(int(chrome_pid)) if chrome_pid else None,
                    chrome_pid=chrome_pid,
                )
                diagnostics["first_attempt_cdp_ready"] = True
                diagnostics["browser_version"] = version.get("Browser") if isinstance(version, dict) else None
                self._set_attempt_browser_version(diagnostics, 1, diagnostics["browser_version"])
            except CdpError as first_error:
                diagnostics["first_attempt_cdp_ready"] = False
                diagnostics["first_attempt_exit_code"] = (first_error.details or {}).get("chrome_exit_code")
                diagnostics["first_attempt_failure_class"] = None
                if first_error.result != "bootstrap_chrome_exited":
                    raise
                classification = self._classify_bootstrap_chrome_failure(diagnostics.get("first_attempt_stderr_log"))
                diagnostics.update(self._collect_dawn_lock_probe_details(chrome_pid))
                diagnostics["first_attempt_failure_class"] = classification["failure_class"]
                diagnostics["bootstrap_chrome_retry_reason"] = classification["failure_class"]
                diagnostics["bootstrap_chrome_retry_eligible"] = bool(classification["retry_eligible"])
                diagnostics["bootstrap_chrome_retry_classified_eligible"] = bool(classification["retry_eligible"])
                diagnostics["bootstrap_chrome_failure_key_lines"] = classification["key_lines"]
                if not classification["retry_eligible"]:
                    diagnostics["bootstrap_chrome_retry_permitted"] = False
                    diagnostics["bootstrap_chrome_retry_block_reason"] = "failure_class_not_retryable"
                    raise first_error
                worker_health = self._worker_still_healthy_for_retry(account, worker_pid)
                diagnostics.update(worker_health["details"])
                if not worker_health["healthy"]:
                    diagnostics["bootstrap_chrome_retry_permitted"] = False
                    diagnostics["bootstrap_chrome_retry_block_reason"] = worker_health["reason"]
                    raise first_error
                chrome_ok, chrome_block_reason = self._chrome_retry_safe(account, chrome_pid)
                if not chrome_ok:
                    diagnostics["bootstrap_chrome_retry_permitted"] = False
                    diagnostics["bootstrap_chrome_retry_block_reason"] = chrome_block_reason
                    compensation = self._compensate_owned(account, False, worker_started, None, worker_pid)
                    return ExtensionBootstrapResult(
                        "bootstrap_chrome_retry_unsafe",
                        account.account_id,
                        False,
                        {**initialization_details, **diagnostics, "stage": "wait_cdp_ready", **compensation},
                    )
                rebuild = self._rebuild_registered_empty_profile_for_retry(account, initialization_details)
                diagnostics.update(rebuild["details"])
                if not rebuild["ok"]:
                    diagnostics["bootstrap_chrome_retry_permitted"] = False
                    diagnostics["bootstrap_chrome_retry_block_reason"] = rebuild["reason"]
                    compensation = self._compensate_owned(account, False, worker_started, None, worker_pid)
                    return ExtensionBootstrapResult(
                        rebuild["reason"],
                        account.account_id,
                        False,
                        {**initialization_details, **diagnostics, "stage": "profile_rebuild_retry", **compensation},
                    )
                diagnostics["bootstrap_chrome_retry_permitted"] = True
                diagnostics["bootstrap_chrome_retry_block_reason"] = "none"
                volatile_cleanup = self._cleanup_bootstrap_volatile_cache(account, initialization_details, attempt=2)
                prior_attempt_diagnostics = list(diagnostics.get("bootstrap_chrome_attempt_diagnostics") or [])
                new_attempt_diagnostics = list((volatile_cleanup.get("details") or {}).get("bootstrap_chrome_attempt_diagnostics") or [])
                volatile_cleanup["details"]["bootstrap_chrome_attempt_diagnostics"] = [*prior_attempt_diagnostics, *new_attempt_diagnostics]
                diagnostics.update(volatile_cleanup["details"])
                if not volatile_cleanup["ok"]:
                    compensation = self._compensate_owned(account, False, worker_started, None, worker_pid)
                    return ExtensionBootstrapResult(
                        volatile_cleanup["reason"],
                        account.account_id,
                        False,
                        {**initialization_details, **diagnostics, "stage": "volatile_cache_cleanup_retry", **compensation},
                    )
                nonce = secrets.token_urlsafe(18)
                url = self.bootstrap_url(account, nonce, EXPECTED_FLOWKIT_EXTENSION_ID)
                chrome = self._open_bootstrap_chrome(account, url, attempt=2)
                if chrome.result not in {"opened", "already_running"}:
                    compensation = self._compensate_owned(account, False, worker_started, None, worker_pid)
                    return ExtensionBootstrapResult(
                        chrome.result,
                        account.account_id,
                        False,
                        {**initialization_details, **diagnostics, **(chrome.details or {}), "stage": "open_bootstrap_chrome_retry", **compensation},
                    )
                chrome_started_by_bootstrap = chrome.result == "opened"
                chrome_pid = (chrome.details or {}).get("chrome_pid")
                diagnostics.update({
                    "bootstrap_chrome_attempts": 2,
                    "bootstrap_chrome_retry_used": True,
                    "second_attempt_chrome_pid": chrome_pid,
                    "second_attempt_stdout_log": (chrome.details or {}).get("stdout_log"),
                    "second_attempt_stderr_log": (chrome.details or {}).get("stderr_log"),
                    "chrome_spawned_at": (chrome.details or {}).get("chrome_spawned_at"),
                    "chrome_pid": chrome_pid,
                    "bootstrap_chrome_command": (chrome.details or {}).get("command"),
                    "bootstrap_chrome_stdout_log": (chrome.details or {}).get("stdout_log"),
                    "bootstrap_chrome_stderr_log": (chrome.details or {}).get("stderr_log"),
                    "bootstrap_disable_skia_graphite": bool((chrome.details or {}).get("bootstrap_disable_skia_graphite")),
                    "disable_skia_graphite_present": bool((chrome.details or {}).get("disable_skia_graphite_present")),
                    "bootstrap_chrome_attempt_diagnostics": [
                        *list(diagnostics.get("bootstrap_chrome_attempt_diagnostics") or []),
                        *list((chrome.details or {}).get("bootstrap_chrome_attempt_diagnostics") or []),
                    ],
                })
                self._annotate_launch_attempt_diagnostics(diagnostics, initialization_details)
                try:
                    version = self.cdp.wait_ready(
                        account.chrome_cdp_port,
                        sleep=self.sleep,
                        chrome_process=self._bootstrap_chrome_processes.get(int(chrome_pid)) if chrome_pid else None,
                        chrome_pid=chrome_pid,
                    )
                    diagnostics["second_attempt_cdp_ready"] = True
                    diagnostics["browser_version"] = version.get("Browser") if isinstance(version, dict) else None
                    self._set_attempt_browser_version(diagnostics, 2, diagnostics["browser_version"])
                except CdpError as second_error:
                    diagnostics.update(self._collect_dawn_lock_probe_details(chrome_pid))
                    diagnostics["second_attempt_cdp_ready"] = False
                    diagnostics["second_attempt_exit_code"] = (second_error.details or {}).get("chrome_exit_code")
                    compensation = self._compensate_owned(account, chrome_started_by_bootstrap, worker_started, chrome_pid, worker_pid)
                    result = second_error.to_bootstrap_result(account.account_id)
                    return ExtensionBootstrapResult(
                        "bootstrap_chrome_retry_exhausted",
                        account.account_id,
                        False,
                        {**initialization_details, **diagnostics, **(result.details or {}), **compensation},
                    )
            diagnostics["cdp_ready_at"] = self._utc_now()
            diagnostics["final_chrome_pid"] = chrome_pid
            extension_load = self._wait_expected_extension_loaded(account, manifest, chrome_pid=chrome_pid, command=diagnostics.get("bootstrap_chrome_command"))
            diagnostics.update(extension_load.details or {})
            if not extension_load.ok:
                compensation = self._compensate_owned(account, chrome_started_by_bootstrap, worker_started, chrome_pid, worker_pid)
                return ExtensionBootstrapResult(
                    extension_load.result,
                    account.account_id,
                    False,
                    {**initialization_details, **diagnostics, "template_ready": self.template_ready(), "repair": repair, **compensation},
                )
            extension_id = (extension_load.details or {}).get("runtime_extension_id")
            try:
                runtime_bootstrap_url = self.bootstrap_url(account, nonce, extension_id)
                diagnostics.update(self._bootstrap_url_extension_diagnostics(runtime_bootstrap_url, extension_id))
                diagnostics["bootstrap_url_uses_runtime_extension_id"] = True
                opened = self.cdp.open_url(account.chrome_cdp_port, runtime_bootstrap_url)
                opened_url = str(opened.get("url") or opened.get("targetUrl") or "")
                opened_parsed = urlparse(opened_url)
                diagnostics.update({
                    "bootstrap_options_opened_via_cdp": True,
                    "options_target_seen": opened_parsed.scheme == "chrome-extension" and opened_parsed.netloc == manifest.extension_id,
                    "options_target_url": f"chrome-extension://{manifest.extension_id}/{manifest.options_page}?<query-redacted>",
                })
            except CdpError as open_error:
                diagnostics["bootstrap_options_opened_via_cdp"] = False
                result = open_error.to_bootstrap_result(account.account_id)
                return ExtensionBootstrapResult(
                    result.result,
                    account.account_id,
                    False,
                    {**initialization_details, **diagnostics, **(result.details or {})},
                )

            diagnostics["wait_extension_ready_started_at"] = self._utc_now()
            status = self._wait_extension_ready(account, chrome_pid=chrome_pid, diagnostics=diagnostics)
            diagnostics.update(self._collect_dawn_lock_probe_details(chrome_pid))
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
            if diagnostics.get("bootstrap_chrome_retry_used") and result == "bootstrap_chrome_exited":
                result = "bootstrap_chrome_retry_exhausted"
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

    def _open_bootstrap_chrome(self, account: AccountRecord, bootstrap_url: str, attempt: int = 1, extra_args: list[str] | None = None) -> RuntimeResult:
        if self.runtime._owned_chrome_running(account):
            return RuntimeResult("already_running", account.account_id, True)
        conflict = self.runtime._listening_port_conflict(account, "chrome_cdp_port", account.chrome_cdp_port)
        if conflict:
            return conflict
        command = self.bootstrap_chrome_command(account, bootstrap_url, extra_args=extra_args)
        command_summary = self._bootstrap_command_summary(command)
        browser_diagnostics = self.runtime.browser_diagnostics() if hasattr(self.runtime, "browser_diagnostics") else {}
        stdout_path, stderr_path = self._bootstrap_chrome_log_files(account, attempt=attempt)
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
        self._start_dawn_lock_probe_watcher(account, stderr_path, attempt, int(proc.pid))
        return RuntimeResult(
            "opened",
            account.account_id,
            True,
            details={
                "chrome_pid": proc.pid,
                "bootstrap_chrome_attempt": attempt,
                "chrome_spawned_at": self._utc_now(),
                "command": self._redacted_bootstrap_command(command),
                **command_summary,
                **browser_diagnostics,
                "bootstrap_chrome_attempt_diagnostics": [{
                    "attempt_number": int(attempt),
                    "browser_executable_kind": browser_diagnostics.get("browser_kind", "chrome_for_testing"),
                    "browser_executable": browser_diagnostics.get("browser_executable"),
                    "browser_kind": browser_diagnostics.get("browser_kind"),
                    "browser_sha256": browser_diagnostics.get("browser_sha256"),
                    "browser_source": browser_diagnostics.get("browser_source"),
                    "cft_required": browser_diagnostics.get("cft_required", True),
                    "chrome_pid": proc.pid,
                    "disable_skia_graphite_present": command_summary["disable_skia_graphite_present"],
                }],
                "stdout_log": str(stdout_path),
                "stderr_log": str(stderr_path),
            },
        )

    def _normalized_bootstrap_extra_args(self, extra_args: list[str]) -> list[str]:
        normalized = []
        for arg in extra_args:
            text = str(arg)
            if (
                text == DISABLE_SKIA_GRAPHITE_ARG
                or text.startswith(f"{DISABLE_SKIA_GRAPHITE_ARG}=")
                or text.startswith("--load-extension=")
                or text.startswith("--disable-extensions-except=")
            ):
                continue
            if text == ENABLE_SKIA_GRAPHITE_ARG or text.startswith(f"{ENABLE_SKIA_GRAPHITE_ARG}="):
                continue
            normalized.append(text)
        return normalized

    def _bootstrap_command_summary(self, command: list[str]) -> dict:
        return {
            "bootstrap_disable_skia_graphite": self._command_contains_arg(command, DISABLE_SKIA_GRAPHITE_ARG),
            "disable_skia_graphite_present": self._command_contains_arg(command, DISABLE_SKIA_GRAPHITE_ARG),
            "load_extension_present": self._command_contains_arg(command, "--load-extension"),
            "disable_extensions_except_present": self._command_contains_arg(command, "--disable-extensions-except"),
            "bootstrap_initial_url_is_about_blank": bool(command and command[-1] == "about:blank"),
        }

    def _command_contains_arg(self, command: list[str], arg_name: str) -> bool:
        return any(str(part) == arg_name or str(part).startswith(f"{arg_name}=") for part in command)

    def _set_attempt_browser_version(self, diagnostics: dict, attempt: int, browser_version: str | None) -> None:
        attempts = list(diagnostics.get("bootstrap_chrome_attempt_diagnostics") or [])
        for item in attempts:
            if item.get("attempt_number") == int(attempt) and "disable_skia_graphite_present" in item:
                item["browser_version"] = browser_version
        diagnostics["bootstrap_chrome_attempt_diagnostics"] = attempts

    def _annotate_launch_attempt_diagnostics(self, diagnostics: dict, initialization_details: dict) -> None:
        safe_keys = (
            "profile_initialization_mode",
            "registered_empty_safe_to_rebuild",
            "template_marker_present_before_launch",
            "template_marker_valid",
            "template_marker_extension_id",
            "template_marker_extension_id_match",
            "expected_extension_id",
            "template_preferences_extension_entry_present",
            "expected_extension_id_present_in_preferences",
            "expected_extension_id_present_in_secure_preferences",
            "secure_preferences_extension_manifest_present",
            "secure_preferences_extension_disabled_or_blocked",
            "expected_extension_local_state_present",
            "bootstrap_url_extension_id",
            "bootstrap_url_extension_id_match",
            "credential_storage_sanitized",
        )
        attempts = list(diagnostics.get("bootstrap_chrome_attempt_diagnostics") or [])
        for item in attempts:
            if "disable_skia_graphite_present" not in item:
                continue
            for key in safe_keys:
                item[key] = initialization_details.get(key)
        diagnostics["bootstrap_chrome_attempt_diagnostics"] = attempts

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
                    if relative == Path("Default") / "Local Extension Settings" / EXPECTED_FLOWKIT_EXTENSION_ID:
                        target.mkdir(parents=True, exist_ok=True)
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

    def _profile_extension_state_gate(self, profile_path: Path) -> dict:
        profile = Path(profile_path)
        marker = profile / TEMPLATE_READY_FILE
        preferences = profile / "Default" / "Preferences"
        secure_preferences = profile / "Default" / "Secure Preferences"
        local_state = profile / "Default" / "Local Extension Settings" / EXPECTED_FLOWKIT_EXTENSION_ID
        expected_in_preferences = self._file_contains(preferences, EXPECTED_FLOWKIT_EXTENSION_ID)
        marker_details = self._template_marker_gate(marker)
        secure_details = self._secure_preferences_extension_gate(secure_preferences)
        local_state_present = local_state.is_dir()
        return {
            "template_marker_present_before_launch": marker.is_file(),
            **marker_details,
            "expected_extension_id": EXPECTED_FLOWKIT_EXTENSION_ID,
            "template_preferences_extension_entry_present": expected_in_preferences,
            "expected_extension_id_present_in_preferences": expected_in_preferences,
            **secure_details,
            "expected_extension_local_state_present": local_state_present,
            "template_extension_state_ready": bool(marker_details["template_marker_valid"] and marker_details["template_marker_extension_id_match"]),
            "template_base_validation_passed": bool(marker_details["template_marker_valid"] and marker_details["template_marker_extension_id_match"]),
        }

    def _file_contains(self, path: Path, text: str) -> bool:
        try:
            return text in path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return False

    def _template_marker_gate(self, marker_path: Path) -> dict:
        details = {
            "template_marker_valid": False,
            "template_marker_extension_id": None,
            "template_marker_extension_id_match": False,
        }
        try:
            marker = json.loads(marker_path.read_text(encoding="utf-8"))
        except Exception:
            return details
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
        extension_id = str(marker.get("extension_id") or "")
        details["template_marker_valid"] = bool(required.issubset(marker) and marker.get("ready") is True)
        details["template_marker_extension_id"] = extension_id
        details["template_marker_extension_id_match"] = extension_id == EXPECTED_FLOWKIT_EXTENSION_ID
        return details

    def _secure_preferences_extension_gate(self, secure_preferences: Path) -> dict:
        details = {
            "expected_extension_id_present_in_secure_preferences": False,
            "secure_preferences_extension_manifest_present": False,
            "secure_preferences_extension_disabled_or_blocked": False,
        }
        try:
            data = json.loads(secure_preferences.read_text(encoding="utf-8"))
        except Exception:
            return details
        settings = ((data.get("extensions") or {}).get("settings") or {})
        extension = settings.get(EXPECTED_FLOWKIT_EXTENSION_ID)
        if not isinstance(extension, dict):
            return details
        details["expected_extension_id_present_in_secure_preferences"] = True
        details["secure_preferences_extension_manifest_present"] = isinstance(extension.get("manifest"), dict)
        disable_reasons = extension.get("disable_reasons") or []
        state = extension.get("state")
        blacklist_state = extension.get("blacklist_state")
        blocklist_state = extension.get("blocklist_state")
        details["secure_preferences_extension_disabled_or_blocked"] = bool(
            state not in (None, 1)
            or disable_reasons
            or blacklist_state not in (None, 0)
            or blocklist_state not in (None, 0)
        )
        return details

    def _bootstrap_url_extension_diagnostics(self, bootstrap_url: str, expected_extension_id: str) -> dict:
        parsed = urlparse(bootstrap_url)
        return {
            "bootstrap_url_extension_id": parsed.netloc,
            "bootstrap_url_extension_id_match": parsed.scheme == "chrome-extension" and parsed.netloc == expected_extension_id,
        }

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

    def _is_reparse_point(self, target: Path) -> bool:
        try:
            return bool(target.stat().st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT)
        except AttributeError:
            return False
        except OSError:
            return False

    def _cleanup_bootstrap_volatile_cache(self, account: AccountRecord, initialization_details: dict, attempt: int) -> dict:
        details = {
            "bootstrap_volatile_cache_cleanup_permitted": False,
            "bootstrap_volatile_cache_cleanup_used": False,
            "bootstrap_volatile_cache_cleanup_block_reason": "unknown",
            "bootstrap_volatile_cache_cleanup_paths": [],
            "bootstrap_gpupersistentcache_present_before": False,
            "bootstrap_gpupersistentcache_file_count_before": 0,
            "bootstrap_gpupersistentcache_size_before": 0,
            "bootstrap_dawn_cache_present_before": False,
            "bootstrap_dawn_cache_file_count_before": 0,
            "bootstrap_gpupersistentcache_present_after": None,
            "bootstrap_volatile_cache_cleanup_result": None,
        }

        def blocked(reason: str) -> dict:
            details["bootstrap_volatile_cache_cleanup_block_reason"] = reason
            return {"ok": False, "reason": "profile_volatile_cache_cleanup_failed", "details": self._with_attempt_cleanup(details, attempt)}

        profile = Path(account.profile_path)
        try:
            profile_resolved = profile.resolve(strict=False)
            profiles_root_resolved = self.profiles_root.resolve(strict=False)
            expected_resolved = (self.profiles_root / account.account_id).resolve(strict=False)
        except Exception:
            return blocked("path_resolution_failed")
        if initialization_details.get("profile_initialization_mode") != "rebuilt_registered_empty_profile":
            return blocked("profile_not_registered_empty")
        if initialization_details.get("credential_storage_sanitized") is not True:
            return blocked("credential_storage_not_sanitized")
        if not profile.is_absolute():
            return blocked("profile_path_not_absolute")
        if profile.name != account.account_id or profile_resolved != expected_resolved:
            return blocked("account_profile_path_mismatch")
        try:
            profile_resolved.relative_to(profiles_root_resolved)
        except ValueError:
            return blocked("profile_path_outside_profiles_root")
        if profile.name == TEMPLATE_PROFILE_NAME or "_failed_bootstrap" in {part.lower() for part in profile_resolved.parts}:
            return blocked("profile_path_not_active_account")
        if any((profile / name).exists() for name in ("Cookies", "Login Data", "Web Data")) or any((profile / "Default" / name).exists() for name in ("Cookies", "Login Data", "Web Data")):
            return blocked("google_login_artifacts_present")

        target = profile / VOLATILE_BOOTSTRAP_CACHE_DIR
        try:
            target_resolved = target.resolve(strict=False)
            target_resolved.relative_to(profile_resolved)
        except Exception:
            return blocked("cache_path_not_safe")
        if target.exists() and (target.is_symlink() or self._is_reparse_point(target)):
            return blocked("cache_path_reparse_point")

        files = []
        if target.exists():
            try:
                files = [item for item in target.rglob("*") if item.is_file()]
            except OSError:
                return blocked("cache_metadata_failed")
        dawn = target / "DawnGraphiteCache"
        dawn_files = []
        if dawn.exists():
            try:
                dawn_files = [item for item in dawn.rglob("*") if item.is_file()]
            except OSError:
                dawn_files = []
        details.update({
            "bootstrap_volatile_cache_cleanup_permitted": True,
            "bootstrap_volatile_cache_cleanup_block_reason": "none",
            "bootstrap_gpupersistentcache_present_before": target.exists(),
            "bootstrap_gpupersistentcache_file_count_before": len(files),
            "bootstrap_gpupersistentcache_size_before": sum(item.stat().st_size for item in files if item.exists()),
            "bootstrap_dawn_cache_present_before": dawn.exists(),
            "bootstrap_dawn_cache_file_count_before": len(dawn_files),
            "bootstrap_volatile_cache_cleanup_paths": [VOLATILE_BOOTSTRAP_CACHE_DIR.as_posix()],
        })
        if not target.exists():
            details["bootstrap_gpupersistentcache_present_after"] = False
            details["bootstrap_volatile_cache_cleanup_result"] = "no_op_missing"
            return {"ok": True, "reason": "none", "details": self._with_attempt_cleanup(details, attempt)}
        try:
            shutil.rmtree(target, onerror=self._make_writable_and_retry)
        except Exception as error:
            details["bootstrap_volatile_cache_cleanup_error"] = type(error).__name__
            details["bootstrap_gpupersistentcache_present_after"] = target.exists()
            details["bootstrap_volatile_cache_cleanup_result"] = "delete_failed"
            return {"ok": False, "reason": "profile_volatile_cache_cleanup_failed", "details": self._with_attempt_cleanup(details, attempt)}
        details["bootstrap_volatile_cache_cleanup_used"] = True
        details["bootstrap_gpupersistentcache_present_after"] = target.exists()
        details["bootstrap_volatile_cache_cleanup_result"] = "removed" if not target.exists() else "delete_incomplete"
        if target.exists():
            return {"ok": False, "reason": "profile_volatile_cache_cleanup_failed", "details": self._with_attempt_cleanup(details, attempt)}
        return {"ok": True, "reason": "none", "details": self._with_attempt_cleanup(details, attempt)}

    def _with_attempt_cleanup(self, cleanup_details: dict, attempt: int) -> dict:
        attempt_detail = {
            "attempt_number": int(attempt),
            "volatile_cache_cleanup": {
                key: value
                for key, value in cleanup_details.items()
                if key.startswith("bootstrap_volatile_cache_cleanup")
                or key.startswith("bootstrap_gpupersistentcache")
                or key.startswith("bootstrap_dawn_cache")
            },
        }
        existing = list(cleanup_details.get("bootstrap_chrome_attempt_diagnostics") or [])
        return {**cleanup_details, "bootstrap_chrome_attempt_diagnostics": [*existing, attempt_detail]}

    def _validate_account_config(self, account: AccountRecord) -> ExtensionBootstrapResult | None:
        if not ACCOUNT_ID_RE.match(account.account_id):
            return ExtensionBootstrapResult("invalid_account_id", account.account_id, False)
        port = int(account.extension_ws_port)
        if port < 1 or port > 65535:
            return ExtensionBootstrapResult("invalid_ws_url", account.account_id, False, {"extension_ws_port": account.extension_ws_port})
        return None

    def _classify_bootstrap_chrome_failure(self, stderr_log: str | None) -> dict:
        text = ""
        key_lines = []
        if stderr_log:
            try:
                lines = Path(stderr_log).read_text(encoding="utf-8", errors="ignore").splitlines()
            except Exception:
                lines = []
            safe_terms = (
                "gpu",
                "fatal",
                "profile",
                "singleton",
                "devtools",
                "remote-debugging",
                "user data",
                "extension",
                "access denied",
                "0x20",
                "sharing violation",
            )
            for line in lines:
                lowered = line.lower()
                if any(term in lowered for term in safe_terms):
                    sanitized = re.sub(r"chrome-extension://[^\\s]+", "chrome-extension://<redacted>", line)
                    key_lines.append(sanitized[:300])
            text = "\n".join(lines).lower()
        failure_class = "unknown_early_exit"
        if "gpu process isn't usable" in text:
            failure_class = "gpu_process_unusable"
        elif "gpu process exited unexpectedly" in text:
            failure_class = "gpu_process_crash_loop"
        elif (
            any(term.lower() in text for term in ("GPUPersistentCache", "DawnGraphiteCache", "GPUCache", "GraphiteDawnCache"))
            and any(term in text for term in ("0x20", "sharing violation", "used by another process"))
        ):
            failure_class = "gpu_cache_sharing_violation"
        elif any(term in text for term in ("singletonlock", "profile in use", "user data directory is already in use")):
            failure_class = "profile_lock_error"
        elif "failed to load extension" in text or "extension load" in text:
            failure_class = "extension_load_error"
        elif "remote-debugging-port" in text and ("in use" in text or "bind" in text):
            failure_class = "cdp_port_conflict"
        elif "invalid user data" in text or "cannot create user data" in text:
            failure_class = "invalid_user_data_dir"
        elif "access denied" in text and "google update" not in text:
            failure_class = "access_denied"
        return {
            "failure_class": failure_class,
            "retry_eligible": failure_class in GPU_RETRY_FAILURE_CLASSES,
            "key_lines": key_lines[:10],
        }

    def _cleanup_gpu_caches(self, account: AccountRecord) -> dict:
        profile = Path(account.profile_path)
        profile_root = profile.resolve(strict=False)
        cleaned = []
        ok = True
        for relative in GPU_CACHE_RELATIVE_DIRS:
            target = profile / relative
            item = {
                "path": relative.as_posix(),
                "existed": target.exists(),
                "removed": False,
                "file_count": 0,
            }
            try:
                resolved = target.resolve(strict=False)
                resolved.relative_to(profile_root)
                if target.exists():
                    if target.is_dir():
                        item["file_count"] = sum(1 for child in target.rglob("*") if child.is_file())
                    elif target.is_file():
                        item["file_count"] = 1
                    self._remove_path(target)
                    item["removed"] = True
            except Exception as error:
                item["error"] = type(error).__name__
                ok = False
            cleaned.append(item)
            if not ok:
                break
        return {"ok": ok, "cleaned": cleaned}

    def _rebuild_registered_empty_profile_for_retry(self, account: AccountRecord, initialization_details: dict) -> dict:
        details = {
            "bootstrap_profile_rebuild_used": False,
            "bootstrap_profile_rebuild_permitted": False,
            "bootstrap_profile_rebuild_block_reason": "unknown",
            "bootstrap_profile_stability_wait_ms": None,
            "bootstrap_failed_profile_quarantined": False,
            "bootstrap_failed_profile_quarantine_path_safe": False,
            "bootstrap_profile_rebuild_result": None,
            "bootstrap_profile_rebuild_mode": None,
            "bootstrap_second_attempt_uses_fresh_profile": False,
            "gpu_cache_cleanup_attempted": False,
        }
        if initialization_details.get("profile_initialization_mode") != "rebuilt_registered_empty_profile":
            details["bootstrap_profile_rebuild_block_reason"] = "profile_not_registered_empty"
            return {"ok": False, "reason": "profile_not_registered_empty", "details": details}
        if initialization_details.get("credential_storage_sanitized") is not True:
            details["bootstrap_profile_rebuild_block_reason"] = "credential_storage_not_sanitized"
            return {"ok": False, "reason": "credential_storage_not_sanitized", "details": details}
        stable = self._wait_profile_stable_for_retry(account)
        details["bootstrap_profile_stability_wait_ms"] = stable["wait_ms"]
        if not stable["ok"]:
            details["bootstrap_profile_rebuild_block_reason"] = stable["reason"]
            return {"ok": False, "reason": stable["reason"], "details": details}
        quarantine = self._quarantine_failed_bootstrap_profile(account)
        details.update(quarantine["details"])
        if not quarantine["ok"]:
            details["bootstrap_profile_rebuild_block_reason"] = quarantine["reason"]
            return {"ok": False, "reason": quarantine["reason"], "details": details}
        copied = self.copy_template_to_profile(account)
        if not copied.ok:
            details["bootstrap_profile_rebuild_block_reason"] = "profile_rebuild_failed"
            details["bootstrap_profile_rebuild_result"] = copied.result
            return {"ok": False, "reason": "profile_rebuild_failed", "details": details}
        details.update({
            "bootstrap_profile_rebuild_used": True,
            "bootstrap_profile_rebuild_permitted": True,
            "bootstrap_profile_rebuild_block_reason": "none",
            "bootstrap_profile_rebuild_result": "rebuilt_registered_empty_profile",
            "bootstrap_profile_rebuild_mode": "template_recopy_after_gpu_failure",
            "bootstrap_second_attempt_uses_fresh_profile": True,
        })
        return {"ok": True, "reason": "none", "details": details}

    def _wait_profile_stable_for_retry(self, account: AccountRecord, timeout_seconds: float = 3.0, quiet_seconds: float = 0.5) -> dict:
        started = time.monotonic()
        last_mtime = self._profile_latest_mtime(Path(account.profile_path))
        stable_since = started
        while time.monotonic() - started < timeout_seconds:
            current_mtime = self._profile_latest_mtime(Path(account.profile_path))
            if current_mtime == last_mtime:
                if time.monotonic() - stable_since >= quiet_seconds:
                    return {"ok": True, "reason": "none", "wait_ms": int((time.monotonic() - started) * 1000)}
            else:
                last_mtime = current_mtime
                stable_since = time.monotonic()
            self.sleep(0.1)
        return {"ok": False, "reason": "profile_not_stable_for_retry", "wait_ms": int((time.monotonic() - started) * 1000)}

    def _profile_latest_mtime(self, profile: Path) -> float:
        latest = 0.0
        for relative in (Path("."), Path("Default"), Path("GPUPersistentCache"), Path("Default") / "GPUPersistentCache"):
            target = profile / relative
            if not target.exists():
                continue
            try:
                latest = max(latest, target.stat().st_mtime)
                if target.is_dir():
                    for child in target.rglob("*"):
                        try:
                            latest = max(latest, child.stat().st_mtime)
                        except OSError:
                            continue
            except OSError:
                continue
        return latest

    def _quarantine_failed_bootstrap_profile(self, account: AccountRecord) -> dict:
        profile = Path(account.profile_path)
        details = {
            "bootstrap_failed_profile_quarantine_path": None,
            "bootstrap_failed_profile_size_bytes": None,
            "bootstrap_failed_profile_file_count": None,
        }
        try:
            profile_root = profile.resolve(strict=False)
            profiles_root = self.profiles_root.resolve(strict=False)
            expected = (self.profiles_root / account.account_id).resolve(strict=False)
            if profile_root != expected:
                return {"ok": False, "reason": "profile_not_retryable", "details": details}
            profile_root.relative_to(profiles_root)
            failed_root = self.profiles_root / "_failed_bootstrap"
            failed_root.mkdir(parents=True, exist_ok=True)
            failed_root_resolved = failed_root.resolve(strict=False)
            failed_root_resolved.relative_to(profiles_root)
            timestamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
            target = failed_root / f"{account.account_id}-{timestamp}-attempt-1"
            suffix = 1
            while target.exists():
                target = failed_root / f"{account.account_id}-{timestamp}-attempt-1-{suffix}"
                suffix += 1
            target_resolved = target.resolve(strict=False)
            target_resolved.relative_to(failed_root_resolved)
            size, file_count = self._profile_size_and_file_count(profile)
            profile.rename(target)
            details.update({
                "bootstrap_failed_profile_quarantined": True,
                "bootstrap_failed_profile_quarantine_path_safe": True,
                "bootstrap_failed_profile_quarantine_path": str(target),
                "bootstrap_failed_profile_size_bytes": size,
                "bootstrap_failed_profile_file_count": file_count,
            })
            return {"ok": True, "reason": "none", "details": details}
        except Exception as error:
            details["bootstrap_failed_profile_quarantine_error"] = type(error).__name__
            return {"ok": False, "reason": "failed_profile_quarantine_failed", "details": details}

    def _profile_size_and_file_count(self, profile: Path) -> tuple[int, int]:
        total = 0
        count = 0
        if not profile.exists():
            return total, count
        for child in profile.rglob("*"):
            if child.is_file():
                count += 1
                try:
                    total += child.stat().st_size
                except OSError:
                    continue
        return total, count

    def _worker_still_healthy_for_retry(self, account: AccountRecord, worker_pid: int | None) -> dict:
        started = time.monotonic()
        last_result = self._worker_retry_health_once(account, worker_pid)
        transient = {"worker_api_not_ready", "worker_ws_not_ready", "worker_health_unreachable", "worker_status_unavailable"}
        while not last_result["healthy"] and last_result["reason"] in transient and time.monotonic() - started < 2.0:
            self.sleep(0.2)
            last_result = self._worker_retry_health_once(account, worker_pid)
        return last_result

    def _worker_retry_health_once(self, account: AccountRecord, worker_pid: int | None) -> dict:
        details = {
            "bootstrap_worker_health_reason": "unknown",
            "bootstrap_worker_launcher_alive": None,
            "bootstrap_worker_api_reachable": False,
            "bootstrap_worker_ws_reachable": False,
            "bootstrap_worker_listener_pid_consistent": False,
            "bootstrap_worker_identity_match": False,
            "bootstrap_worker_challenge_verified": False,
            "bootstrap_worker_pid_cas_applied": False,
        }
        if not worker_pid:
            details["bootstrap_worker_health_reason"] = "worker_registry_pid_missing"
            return {"healthy": False, "reason": "worker_registry_pid_missing", "details": details}
        current = self.registry.get(account.account_id) or account
        ownership_fn = getattr(self.runtime, "_worker_runtime_ownership", None)
        if callable(ownership_fn):
            ownership = ownership_fn(current, worker_pid)
            reason = self._worker_retry_reason(ownership.get("reason"))
            details.update({
                "bootstrap_worker_health_reason": "none" if ownership.get("verified") else reason,
                "bootstrap_worker_launcher_alive": ownership.get("worker_launcher_alive"),
                "bootstrap_worker_api_reachable": bool(ownership.get("worker_api_reachable")),
                "bootstrap_worker_ws_reachable": bool(ownership.get("worker_ws_reachable")),
                "bootstrap_worker_listener_pid_consistent": bool(ownership.get("worker_listener_pid_consistent")),
                "bootstrap_worker_identity_match": bool(ownership.get("worker_identity_match")),
                "bootstrap_worker_challenge_verified": bool(ownership.get("worker_challenge_verified")),
                "bootstrap_worker_pid_cas_applied": bool(ownership.get("worker_pid_cas_applied")),
            })
            if ownership.get("worker_api_listener_pid") is not None:
                details["bootstrap_worker_api_listener_pid"] = ownership.get("worker_api_listener_pid")
            if ownership.get("worker_ws_listener_pid") is not None:
                details["bootstrap_worker_ws_listener_pid"] = ownership.get("worker_ws_listener_pid")
            return {"healthy": bool(ownership.get("verified")), "reason": "none" if ownership.get("verified") else reason, "details": details}
        try:
            status = self.runtime.status(account.account_id)
        except Exception:
            details["bootstrap_worker_health_reason"] = "worker_health_check_exception"
            return {"healthy": False, "reason": "worker_health_check_exception", "details": details}
        details = status.details or {}
        retry_details = {
            "bootstrap_worker_health_reason": "none",
            "bootstrap_worker_launcher_alive": None,
            "bootstrap_worker_api_reachable": bool(details.get("worker_health_reachable")),
            "bootstrap_worker_ws_reachable": details.get("worker_ws_listener_pid") is not None,
            "bootstrap_worker_listener_pid_consistent": bool(
                details.get("worker_api_listener_pid")
                and details.get("worker_ws_listener_pid")
                and int(details.get("worker_api_listener_pid")) == int(details.get("worker_ws_listener_pid"))
            ),
            "bootstrap_worker_identity_match": details.get("account_id") in (None, account.account_id),
            "bootstrap_worker_challenge_verified": False,
            "bootstrap_worker_pid_cas_applied": False,
        }
        if details.get("worker_health_reachable") is False:
            retry_details["bootstrap_worker_health_reason"] = "worker_health_unreachable"
            return {"healthy": False, "reason": "worker_health_unreachable", "details": retry_details}
        if details.get("worker_pid") not in (None, worker_pid):
            retry_details["bootstrap_worker_health_reason"] = "worker_pid_changed"
            return {"healthy": False, "reason": "worker_pid_changed", "details": retry_details}
        if details.get("account_id") not in (None, account.account_id):
            retry_details["bootstrap_worker_health_reason"] = "worker_account_mismatch"
            return {"healthy": False, "reason": "worker_account_mismatch", "details": retry_details}
        ok = status.result in {"running", "partial"} or bool(details.get("extension_account_id") == account.account_id)
        if ok:
            return {"healthy": True, "reason": "none", "details": retry_details}
        retry_details["bootstrap_worker_health_reason"] = "worker_not_healthy"
        return {"healthy": False, "reason": "worker_not_healthy", "details": retry_details}

    def _worker_retry_reason(self, reason: str | None) -> str:
        mapping = {
            "process_not_found": "worker_launcher_exited",
            "legacy_runtime_unverified": "legacy_runtime_unverified",
            "worker_api_not_ready": "worker_api_not_ready",
            "worker_ws_not_ready": "worker_ws_not_ready",
            "worker_api_ws_pid_mismatch": "worker_api_ws_pid_mismatch",
            "worker_port_pid_mismatch": "worker_api_ws_pid_mismatch",
            "worker_health_unreachable": "worker_health_unreachable",
            "worker_identity_mismatch": "worker_account_mismatch",
            "runtime_instance_mismatch": "worker_runtime_instance_mismatch",
            "ownership_protocol_unsupported": "worker_ownership_version_mismatch",
            "ownership_secret_unavailable": "worker_secret_unavailable",
            "ownership_challenge_failed": "worker_challenge_failed",
            "ownership_challenge_timeout": "worker_challenge_timeout",
            "runtime_identity_changed": "runtime_identity_changed",
            "verified": "none",
        }
        if not reason:
            return "worker_health_check_exception"
        if reason.startswith("process_probe_"):
            return "worker_launcher_probe_unavailable"
        return mapping.get(reason, reason)

    def _chrome_retry_safe(self, account: AccountRecord, chrome_pid: int | None) -> tuple[bool, str]:
        if not chrome_pid:
            return False, "first_chrome_pid_missing"
        proc = self._bootstrap_chrome_processes.get(int(chrome_pid))
        if proc is not None and hasattr(proc, "poll"):
            try:
                if proc.poll() is None:
                    return False, "first_chrome_not_confirmed_exited"
            except Exception:
                return False, "first_chrome_probe_failed"
        if port_is_listening(account.chrome_cdp_port):
            return False, "cdp_port_not_released"
        try:
            Path(account.profile_path).resolve(strict=False).relative_to(self.profiles_root.resolve(strict=False))
        except Exception:
            return False, "profile_not_retryable"
        if Path(account.profile_path).name != account.account_id:
            return False, "profile_not_retryable"
        return True, "none"

    def _wait_expected_extension_loaded(self, account: AccountRecord, manifest: ExtensionIdentity, chrome_pid: int | None = None, attempts: int = 50, command: list[str] | None = None) -> RuntimeResult:
        started = time.monotonic()
        last_discovery: dict = {}
        seen_candidates: list[str] = []
        for attempt in range(1, attempts + 1):
            discovery = self.cdp.discover_extension(account.chrome_cdp_port, manifest.options_page, manifest.service_worker)
            last_discovery = discovery or {}
            target_summary = last_discovery.get("target_summary") or {}
            candidates = self._runtime_extension_candidates(target_summary, manifest)
            for candidate in candidates:
                if candidate not in seen_candidates:
                    seen_candidates.append(candidate)
            runtime_extension_id = EXPECTED_FLOWKIT_EXTENSION_ID if EXPECTED_FLOWKIT_EXTENSION_ID in candidates else None
            details = {
                "extension_load_wait_attempts": attempt,
                "extension_load_elapsed_ms": int((time.monotonic() - started) * 1000),
                "discovered_extension_id": runtime_extension_id,
                "legacy_expected_extension_id": EXPECTED_FLOWKIT_EXTENSION_ID,
                "runtime_extension_id": runtime_extension_id,
                "runtime_id_matches_legacy_expected": runtime_extension_id == EXPECTED_FLOWKIT_EXTENSION_ID if runtime_extension_id else False,
                "expected_extension_id": EXPECTED_FLOWKIT_EXTENSION_ID,
                "expected_extension_loaded": bool(runtime_extension_id),
                "ignored_extension_ids": [item for item in candidates if item != EXPECTED_FLOWKIT_EXTENSION_ID],
                "extension_loaded_from_command_line": self._extension_loaded_from_command_line(command),
                "service_worker_target_seen": bool(candidates or target_summary.get("service_worker_target_seen") or last_discovery.get("service_worker_target_url")),
                "options_target_seen": bool(target_summary.get("options_target_seen") or last_discovery.get("options_target_url")),
                "target_summary_before_options_open": target_summary,
            }
            if runtime_extension_id and details["extension_loaded_from_command_line"]:
                return RuntimeResult("extension_loaded", account.account_id, True, details=details)
            if chrome_pid and self._chrome_exit_code(chrome_pid) is not None:
                details["chrome_exit_code"] = self._chrome_exit_code(chrome_pid)
                return RuntimeResult("bootstrap_chrome_exited", account.account_id, False, details=details)
            self.sleep(0.2)
        target_summary = (last_discovery.get("target_summary") or {}) if isinstance(last_discovery, dict) else {}
        candidates = seen_candidates or self._runtime_extension_candidates(target_summary, manifest)
        return RuntimeResult(
            "expected_flowkit_extension_not_loaded" if candidates else "extension_load_timeout",
            account.account_id,
            False,
            details={
                "extension_load_wait_attempts": attempts,
                "extension_load_elapsed_ms": int((time.monotonic() - started) * 1000),
                "discovered_extension_id": EXPECTED_FLOWKIT_EXTENSION_ID if EXPECTED_FLOWKIT_EXTENSION_ID in candidates else None,
                "legacy_expected_extension_id": EXPECTED_FLOWKIT_EXTENSION_ID,
                "runtime_extension_id": EXPECTED_FLOWKIT_EXTENSION_ID if EXPECTED_FLOWKIT_EXTENSION_ID in candidates else None,
                "runtime_id_matches_legacy_expected": EXPECTED_FLOWKIT_EXTENSION_ID in candidates,
                "expected_extension_id": EXPECTED_FLOWKIT_EXTENSION_ID,
                "expected_extension_loaded": False,
                "ignored_extension_ids": [item for item in candidates if item != EXPECTED_FLOWKIT_EXTENSION_ID],
                "extension_loaded_from_command_line": self._extension_loaded_from_command_line(command),
                "service_worker_target_seen": bool(target_summary.get("service_worker_target_seen")),
                "options_target_seen": bool(target_summary.get("options_target_seen")),
                "target_summary_before_options_open": target_summary,
            },
        )

    def _runtime_extension_candidates(self, target_summary: dict, manifest: ExtensionIdentity) -> list[str]:
        candidates: list[str] = []
        for target in target_summary.get("targets") or []:
            if target.get("type") != "service_worker":
                continue
            if target.get("path") != manifest.service_worker:
                continue
            extension_id = target.get("extension_id")
            if isinstance(extension_id, str) and EXTENSION_ID_RE.match(extension_id) and extension_id not in candidates:
                candidates.append(extension_id)
        return candidates

    def _extension_loaded_from_command_line(self, command: list[str] | None) -> bool:
        if not command:
            return False
        extension_arg_value = chrome_extension_arg_value(self.extension_dir)
        load_args = [str(part) for part in command if str(part).startswith("--load-extension=")]
        except_args = [str(part) for part in command if str(part).startswith("--disable-extensions-except=")]
        return load_args == [f"--load-extension={extension_arg_value}"] and except_args == [f"--disable-extensions-except={extension_arg_value}"]

    def _wait_extension_ready(self, account: AccountRecord, attempts: int = 75, chrome_pid: int | None = None, diagnostics: dict | None = None) -> RuntimeResult:
        started = time.monotonic()
        status = self.runtime.status(account.account_id)
        wait_attempt = 1
        for wait_attempt in range(1, attempts + 1):
            if status.details.get("account_match"):
                status.details["extension_ready_wait_attempts"] = wait_attempt
                status.details["extension_ready_elapsed_ms"] = int((time.monotonic() - started) * 1000)
                return status
            if chrome_pid and status.details.get("chrome_process_alive") is False and status.details.get("chrome_cdp_reachable") is False:
                details = {
                    **status.details,
                    **(diagnostics or {}),
                    "extension_ready_wait_attempts": wait_attempt,
                    "extension_ready_elapsed_ms": int((time.monotonic() - started) * 1000),
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
            if status.details.get("worker_process_alive") is False and status.details.get("worker_health_reachable") is False:
                details = {
                    **status.details,
                    **(diagnostics or {}),
                    "extension_ready_wait_attempts": wait_attempt,
                    "extension_ready_elapsed_ms": int((time.monotonic() - started) * 1000),
                    "worker_exit_detected_at": self._utc_now(),
                }
                return RuntimeResult("worker_exited", account.account_id, False, details=details)
            if wait_attempt >= attempts:
                break
            self.sleep(0.2)
            status = self.runtime.status(account.account_id)
        status.details["extension_ready_wait_attempts"] = wait_attempt
        status.details["extension_ready_elapsed_ms"] = int((time.monotonic() - started) * 1000)
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

    def _bootstrap_chrome_log_files(self, account: AccountRecord, attempt: int | None = None) -> tuple[Path, Path]:
        log_dir = Path(getattr(self.runtime, "log_dir", self.profiles_root.parent / "logs" / "runtime"))
        log_dir.mkdir(parents=True, exist_ok=True)
        if attempt:
            return (
                log_dir / f"{account.account_id}-bootstrap-chrome-attempt-{attempt}-stdout.log",
                log_dir / f"{account.account_id}-bootstrap-chrome-attempt-{attempt}-stderr.log",
            )
        return (
            log_dir / f"{account.account_id}-bootstrap-chrome-stdout.log",
            log_dir / f"{account.account_id}-bootstrap-chrome-stderr.log",
        )

    def _start_dawn_lock_probe_watcher(self, account: AccountRecord, stderr_path: Path, attempt: int, chrome_pid: int) -> None:
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._dawn_lock_probe_tail_worker,
            args=(account, stderr_path, attempt, chrome_pid, stop_event),
            name=f"dawn-lock-probe-{account.account_id}-{attempt}",
            daemon=True,
        )
        self._dawn_lock_probe_watchers[chrome_pid] = (stop_event, thread)
        thread.start()

    def _dawn_lock_probe_tail_worker(self, account: AccountRecord, stderr_path: Path, attempt: int, chrome_pid: int, stop_event: threading.Event) -> None:
        started = time.monotonic()
        offset = 0
        buffer = ""
        while not stop_event.is_set() and time.monotonic() - started < 60:
            try:
                if stderr_path.exists():
                    with stderr_path.open("r", encoding="utf-8", errors="ignore") as handle:
                        handle.seek(offset)
                        chunk = handle.read()
                        offset = handle.tell()
                    if chunk:
                        buffer += chunk
                        lines = buffer.splitlines(keepends=True)
                        if lines and not lines[-1].endswith(("\n", "\r")):
                            buffer = lines.pop()
                        else:
                            buffer = ""
                        for line in lines:
                            if self._maybe_probe_dawn_lock_from_stderr_line(account, line, attempt, chrome_pid):
                                return
            except Exception as error:
                self._dawn_lock_probe_results[chrome_pid] = {
                    "dawn_lock_probe_triggered": False,
                    "dawn_lock_probe_completed": False,
                    "dawn_lock_probe_timed_out": False,
                    "dawn_lock_probe_error": type(error).__name__,
                }
                return
            self.sleep(0.05)

    def _maybe_probe_dawn_lock_from_stderr_line(self, account: AccountRecord, line: str, attempt: int, chrome_pid: int) -> bool:
        if int(chrome_pid) in self._dawn_lock_probe_results:
            return True
        if not re.search(r"DawnGraphiteCache|GPUPersistentCache", line, re.IGNORECASE):
            return False
        if not re.search(r"0x20|sharing violation|另一个程序正在使用此文件", line, re.IGNORECASE):
            return False
        match = re.search(r"([A-Z]:\\[^\"<>|]+(?:GPUPersistentCache\\DawnGraphiteCache|DawnGraphiteCache)[^\"<>|]*)", line)
        if not match:
            self._dawn_lock_probe_results[int(chrome_pid)] = {
                "dawn_lock_probe_triggered": True,
                "dawn_lock_probe_completed": True,
                "dawn_lock_probe_timed_out": False,
                "dawn_lock_probe_path_safe": False,
                "dawn_lock_probe_error": "dawn_cache_path_not_found",
            }
            return True
        previous_pids = {pid for pid in self._bootstrap_chrome_processes if pid != int(chrome_pid)}
        current_account = self.registry.get(account.account_id) or account
        worker_pids = {pid for pid in (getattr(current_account, "worker_pid", None),) if pid}
        probe = probe_dawn_cache_lock(
            match.group(1),
            account.profile_path,
            current_attempt_pids={int(chrome_pid)},
            previous_attempt_pids=previous_pids,
            worker_pids=worker_pids,
            attempt_number=attempt,
            sleep=self.sleep,
        )
        summary = probe.get("summary") or {}
        probe_details = {
            "dawn_lock_probe_triggered": True,
            "dawn_lock_probe_completed": True,
            "dawn_lock_probe_timed_out": False,
            "dawn_lock_probe_path_safe": bool(probe.get("target_path_safe")),
            "dawn_lock_probe_target_relative": probe.get("target_relative"),
            "dawn_lock_probe_resource_kind": probe.get("resource_kind"),
            "dawn_lock_probe_candidate_count": probe.get("candidate_count"),
            "dawn_lock_probe_candidate_relative_paths": probe.get("candidate_relative_paths"),
            "dawn_lock_probe_no_candidate_files": summary.get("dawn_lock_probe_no_candidate_files"),
            "dawn_lock_probe_error": probe.get("probe_error"),
            "dawn_lock_probe_error_stage": probe.get("probe_error_stage"),
            "dawn_lock_probe_error_function": probe.get("probe_error_function"),
            "dawn_lock_probe_rm_result_code": probe.get("probe_rm_result_code"),
            "dawn_lock_probe_winerror": probe.get("probe_winerror"),
            "dawn_lock_probe_errno": probe.get("probe_errno"),
            **summary,
        }
        self._dawn_lock_probe_results[int(chrome_pid)] = probe_details
        self._dawn_lock_probe_attempt_results[int(attempt)] = {
            "attempt_number": int(attempt),
            "chrome_pid": int(chrome_pid),
            "dawn_lock_probe": probe_details,
        }
        return True

    def _collect_dawn_lock_probe_details(self, chrome_pid: int | None) -> dict:
        if not chrome_pid:
            return {}
        pid = int(chrome_pid)
        watcher = self._dawn_lock_probe_watchers.pop(pid, None)
        if watcher:
            stop_event, thread = watcher
            deadline = time.monotonic() + 0.75
            while pid not in self._dawn_lock_probe_results and thread.is_alive() and time.monotonic() < deadline:
                self.sleep(0.05)
            stop_event.set()
            thread.join(timeout=0.5)
            if pid not in self._dawn_lock_probe_results and thread.is_alive():
                details = {
                    "dawn_lock_probe_triggered": False,
                    "dawn_lock_probe_completed": False,
                    "dawn_lock_probe_timed_out": True,
                    "dawn_lock_probe_path_safe": None,
                    "dawn_lock_probe_sample_count": 0,
                    "dawn_lock_probe_holder_count": None,
                    "dawn_lock_probe_holder_classification": "probe_not_completed",
                    "dawn_lock_probe_current_attempt_chrome_detected": False,
                    "dawn_lock_probe_previous_attempt_chrome_detected": False,
                    "dawn_lock_probe_external_process_detected": False,
                }
                if self._dawn_lock_probe_attempt_results:
                    details["bootstrap_chrome_attempt_diagnostics"] = [
                        self._dawn_lock_probe_attempt_results[key]
                        for key in sorted(self._dawn_lock_probe_attempt_results)
                    ]
                return details
        details = self._dawn_lock_probe_results.get(pid, {
            "dawn_lock_probe_triggered": False,
            "dawn_lock_probe_completed": False,
            "dawn_lock_probe_timed_out": False,
            "dawn_lock_probe_path_safe": None,
            "dawn_lock_probe_sample_count": 0,
            "dawn_lock_probe_holder_count": 0,
            "dawn_lock_probe_holder_classification": "no_holder_found",
            "dawn_lock_probe_current_attempt_chrome_detected": False,
            "dawn_lock_probe_previous_attempt_chrome_detected": False,
            "dawn_lock_probe_external_process_detected": False,
        })
        if self._dawn_lock_probe_attempt_results:
            details = {
                **details,
                "bootstrap_chrome_attempt_diagnostics": [
                    self._dawn_lock_probe_attempt_results[key]
                    for key in sorted(self._dawn_lock_probe_attempt_results)
                ],
            }
        return details

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
        if not (self.extension_dir / str(options_page)).is_file():
            return None
        if not (self.extension_dir / str(service_worker)).is_file():
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
