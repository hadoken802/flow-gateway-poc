import json
import hashlib
from urllib.error import HTTPError
from pathlib import Path
from dataclasses import replace

from runtime import cli
from runtime.extension_bootstrap import (
    CREDENTIAL_STORAGE_POLICY_VERSION,
    CdpError,
    CdpClient,
    EXPECTED_FLOWKIT_EXTENSION_ID,
    ExtensionBootstrapper,
    TEMPLATE_PROFILE_NAME,
    TEMPLATE_READY_FILE,
)
from runtime.process_manager import RuntimeManager, RuntimeResult
from runtime.registry import AccountRecord, AccountRegistry
from agent.services.flow_client import FlowClient


def make_registry(tmp_path):
    return AccountRegistry(
        db_path=tmp_path / "data" / "runtime_registry.db",
        profiles_root=tmp_path / "profiles",
        data_root=tmp_path / "data",
        outputs_root=tmp_path / "outputs",
        workers_json_path=tmp_path / "workers.json",
    )


def add_account(registry, account_id="FLOW-006"):
    ports = {
        "FLOW-005": (8101, 9200, 9300),
        "FLOW-006": (8102, 9201, 9304),
        "FLOW-007": (8103, 9202, 9305),
        "BAD": (8104, 9203, 9306),
    }
    worker_api, extension_ws, chrome_cdp = ports[account_id]
    account = AccountRecord(
        account_id=account_id,
        display_name=account_id,
        profile_path=str(registry.profiles_root / account_id),
        worker_api_port=worker_api,
        extension_ws_port=extension_ws,
        chrome_cdp_port=chrome_cdp,
        database_path=str(registry.data_root / f"{account_id}.db"),
        output_dir=str(registry.outputs_root / account_id),
        enabled=True,
        status="login_required",
        created_at="2026-07-23T00:00:00Z",
    )
    registry.register_many([account], create_dirs=False)
    return registry.get(account_id)


class FakeRuntime:
    def __init__(self, status=None):
        self.opened = []
        self.started = []
        self.stopped = []
        self.status_payload = status or {
            "extension_connected": True,
            "extension_account_id": "FLOW-006",
            "account_match": True,
        }
        self.inspector = type("Inspector", (), {"terminate": lambda self, pid: True})()
        self.launched = []
        self.worker_only_started = []
        self.start_one_calls = []
        self.poll_sequences = []
        self.worker_ownership_payload = {
            "verified": True,
            "reason": "verified",
            "method": "worker_challenge",
            "worker_launcher_alive": True,
            "worker_api_reachable": True,
            "worker_ws_reachable": True,
            "worker_listener_pid_consistent": True,
            "worker_identity_match": True,
            "worker_challenge_verified": True,
            "worker_pid_cas_applied": False,
        }

    def _find_chrome(self):
        return Path("C:/Chrome/chrome.exe")

    def popen(self, command, **kwargs):
        pid = 1000 + len(self.launched)
        self.launched.append((pid, command, kwargs))
        sequence = list(self.poll_sequences.pop(0)) if self.poll_sequences else [None]

        class Proc:
            def __init__(self, proc_pid, poll_values):
                self.pid = proc_pid
                self.poll_values = poll_values

            def poll(self):
                if len(self.poll_values) > 1:
                    return self.poll_values.pop(0)
                return self.poll_values[0]

        return Proc(pid, sequence)

    def open_login(self, account_id):
        self.opened.append(account_id)
        return RuntimeResult("already_running", account_id, True)

    def _owned_chrome_running(self, account):
        return False

    def _listening_port_conflict(self, account, field, port):
        return None

    def _safe_command(self, command):
        return command

    def start_one(self, account_id):
        self.start_one_calls.append(account_id)
        self.started.append(account_id)
        return RuntimeResult("started", account_id, True)

    def start_worker_only(self, account_id):
        self.worker_only_started.append(account_id)
        return RuntimeResult("started", account_id, True, details={"worker_pid": 2000 + len(self.worker_only_started)})

    def stop_one(self, account_id):
        self.stopped.append(account_id)
        return RuntimeResult("stopped", account_id, True)

    def _stop_started(self, account, worker_pid, chrome_pid):
        self.stopped.append(account.account_id)
        self.stopped_pids = {"worker_pid": worker_pid, "chrome_pid": chrome_pid}

    def status(self, account_id):
        payload = {**self.status_payload}
        payload.setdefault("extension_account_id", account_id)
        payload.setdefault("account_match", payload.get("extension_account_id") == account_id and payload.get("extension_connected"))
        return RuntimeResult("running", account_id, bool(payload.get("account_match")), details=payload)

    def _worker_runtime_ownership(self, account, worker_pid):
        return {**self.worker_ownership_payload}


class FakeCdp:
    def __init__(self, extension_id=EXPECTED_FLOWKIT_EXTENSION_ID):
        self.extension_id_value = extension_id
        self.opened_urls = []
        self.ready_calls = []

    def wait_ready(self, cdp_port, **kwargs):
        self.ready_calls.append(cdp_port)
        return {"Browser": "Chrome"}

    def extension_id(self, cdp_port):
        return self.extension_id_value

    def discover_extension(self, cdp_port, options_page, service_worker):
        if not self.extension_id_value:
            return {"discovered_extension_id": None, "options_target_url": None, "service_worker_target_url": None}
        return {
            "discovered_extension_id": self.extension_id_value,
            "options_target_url": f"chrome-extension://{self.extension_id_value}/{options_page}",
            "service_worker_target_url": f"chrome-extension://{self.extension_id_value}/{service_worker}",
            "target_summary": self.target_summary(self.list_targets(cdp_port), self.extension_id_value, options_page, service_worker),
        }

    def extension_present(self, cdp_port):
        return self.extension_id_value is not None

    def list_targets(self, cdp_port):
        if not self.extension_id_value:
            return []
        return [
            {"type": "page", "url": f"chrome-extension://{self.extension_id_value}/options.html?bootstrap=1&nonce=SECRET"},
            {"type": "service_worker", "url": f"chrome-extension://{self.extension_id_value}/background.js"},
        ]

    def target_summary(self, targets, expected_extension_id, options_page, service_worker):
        return CdpClient().target_summary(targets, expected_extension_id, options_page, service_worker)

    def open_url(self, cdp_port, url):
        self.opened_urls.append((cdp_port, url))
        return {"id": "target-1", "url": url}

    def verify_extension_options(self, cdp_port, extension_id, options_page):
        return self.extension_id_value == extension_id


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return self.payload


def ready_template(root):
    template = root / TEMPLATE_PROFILE_NAME
    (template / "Default").mkdir(parents=True)
    (template / "Default" / "Preferences").write_text("{}", encoding="utf-8")
    (template / "Default" / "Secure Preferences").write_text("{}", encoding="utf-8")
    (template / "Default" / "Extensions").mkdir()
    (template / "Default" / "Extension State").mkdir()
    (template / "Default" / "Extension State" / "MANIFEST-000001").write_text("extension-state", encoding="utf-8")
    (template / "Default" / "Local Extension Settings").mkdir()
    (template / "Default" / "SingletonLock").write_text("lock", encoding="utf-8")
    (template / "Default" / "Cookies").write_text("cookie", encoding="utf-8")
    (template / "Default" / "Login Data").write_text("login", encoding="utf-8")
    (template / "Default" / "Cache").mkdir()
    (template / "Default" / "Cache" / "entry").write_text("cache", encoding="utf-8")
    manifest = Path("extension/manifest.json").read_bytes()
    manifest_data = json.loads(manifest.decode("utf-8"))
    marker = {
        "template": TEMPLATE_PROFILE_NAME,
        "ready": True,
        "extension_id": EXPECTED_FLOWKIT_EXTENSION_ID,
        "extension_version": manifest_data["version"],
        "options_page": manifest_data["options_page"],
        "service_worker": manifest_data["background"]["service_worker"],
        "extension_dir": str(Path("extension").resolve()),
        "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
        "first_launch_verified": True,
        "persistence_verified": True,
        "verified_without_load_extension": True,
        "verified_at": "2026-07-23T00:00:00Z",
    }
    (template / TEMPLATE_READY_FILE).write_text(json.dumps(marker), encoding="utf-8")
    return template


def write_sensitive_profile_state(profile):
    ext_storage = profile / "Default" / "Local Extension Settings" / EXPECTED_FLOWKIT_EXTENSION_ID
    ext_storage.mkdir(parents=True, exist_ok=True)
    (ext_storage / "000003.log").write_text("SIMULATED_TOKEN_SHOULD_NOT_COPY", encoding="utf-8")
    for relative in (
        Path("Default") / "Local Storage" / "leveldb",
        Path("Default") / "IndexedDB" / "https_labs.google_0.indexeddb.leveldb",
        Path("Default") / "Session Storage",
        Path("Default") / "Service Worker" / "Database",
        Path("Default") / "Sessions",
        Path("Default") / "Storage",
        Path("Default") / "WebStorage",
    ):
        directory = profile / relative
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "000003.log").write_text("SIMULATED_CALLBACK_SECRET_SHOULD_NOT_COPY", encoding="utf-8")
    for relative in (
        Path("Default") / "SharedStorage",
        Path("Default") / "SharedStorage-wal",
        Path("Default") / "SharedStorage-shm",
        Path("Default") / "SharedStorage-journal",
    ):
        file_path = profile / relative
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text("SIMULATED_TOKEN_SHOULD_NOT_COPY", encoding="utf-8")


def write_manifest(extension_dir, version="0.2.0"):
    extension_dir.mkdir(parents=True, exist_ok=True)
    (extension_dir / "manifest.json").write_text(
        json.dumps({"name": "Flow Kit", "version": version, "options_page": "options.html", "background": {"service_worker": "background.js"}}),
        encoding="utf-8",
    )


def test_template_path_and_missing_template(tmp_path):
    registry = make_registry(tmp_path)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    assert bootstrapper.template_path == registry.profiles_root / TEMPLATE_PROFILE_NAME
    assert bootstrapper.copy_template_to_profile(add_account(registry)).result == "extension_template_not_ready"


def test_cdp_open_url_uses_put_and_encoded_target_url(monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        calls.append((request, timeout))
        return FakeResponse(b'{"id":"target-1","webSocketDebuggerUrl":"ws://debug"}')

    monkeypatch.setattr("runtime.extension_bootstrap.urlopen", fake_urlopen)

    result = CdpClient().open_url(9304, "chrome-extension://flowkitid/options.html?bootstrap=1&account_id=FLOW-006")

    request = calls[0][0]
    assert result["id"] == "target-1"
    assert request.get_method() == "PUT"
    assert "/json/new?chrome-extension%3A%2F%2Fflowkitid%2Foptions.html%3Fbootstrap%3D1%26account_id%3DFLOW-006" in request.full_url
    assert "?url=" not in request.full_url


def test_cdp_open_url_converts_http_errors(monkeypatch):
    def fake_urlopen(request, timeout):
        raise HTTPError(request.full_url, 405, "Method Not Allowed", hdrs=None, fp=None)

    monkeypatch.setattr("runtime.extension_bootstrap.urlopen", fake_urlopen)

    result = CdpClient().open_url
    try:
        result(9304, "chrome-extension://flowkitid/options.html")
        assert False
    except Exception as error:
        assert error.result == "cdp_open_target_failed"
        assert error.details["http_status"] == 405
        assert error.details["http_method"] == "PUT"


def test_cdp_wait_ready_retries_until_json_version(monkeypatch):
    calls = []

    def fake_urlopen(url, timeout):
        calls.append(url)
        if len(calls) == 1:
            raise OSError("not ready")
        return FakeResponse(b'{"Browser":"Chrome"}')

    monkeypatch.setattr("runtime.extension_bootstrap.urlopen", fake_urlopen)

    result = CdpClient().wait_ready(9304, attempts=2, delay_seconds=0, sleep=lambda _: None)

    assert result["Browser"] == "Chrome"
    assert calls == ["http://127.0.0.1:9304/json/version", "http://127.0.0.1:9304/json/version"]


def test_cdp_wait_ready_timeout_returns_structured_error(monkeypatch):
    monkeypatch.setattr("runtime.extension_bootstrap.urlopen", lambda url, timeout: (_ for _ in ()).throw(OSError("closed")))

    try:
        CdpClient().wait_ready(9304, attempts=2, delay_seconds=0, sleep=lambda _: None)
        assert False
    except Exception as error:
        assert error.result == "cdp_not_ready"
        assert error.details["cdp_port"] == 9304
        assert error.details["attempts"] == 2


def test_cdp_wait_ready_stops_when_bootstrap_chrome_exits(monkeypatch):
    calls = []

    def fake_urlopen(url, timeout):
        calls.append(url)
        raise OSError("connection refused")

    proc = type("Proc", (), {"poll": lambda self: 9})()
    monkeypatch.setattr("runtime.extension_bootstrap.urlopen", fake_urlopen)

    try:
        CdpClient().wait_ready(9304, attempts=20, delay_seconds=0, sleep=lambda _: None, chrome_process=proc, chrome_pid=1234)
        assert False
    except Exception as error:
        assert error.result == "bootstrap_chrome_exited"
        assert error.details["chrome_pid"] == 1234
        assert error.details["chrome_exit_code"] == 9
        assert error.details["attempts"] == 1
        assert len(calls) == 0


def test_verify_extension_options_rejects_chrome_error_page(monkeypatch):
    def fake_urlopen(request, timeout):
        return FakeResponse(b'{"id":"target-1","url":"chrome-error://chromewebdata/"}')

    monkeypatch.setattr("runtime.extension_bootstrap.urlopen", fake_urlopen)

    assert CdpClient().verify_extension_options(9304, EXPECTED_FLOWKIT_EXTENSION_ID, "options.html") is False


def test_discover_extension_prefers_real_id_and_rejects_wrong_id(monkeypatch):
    wrong_id = "behjnghbkgnggenapbhgjoclmnfgfaim"
    payload = json.dumps([
        {"type": "page", "url": f"chrome-extension://{wrong_id}/options.html"},
        {"type": "page", "url": f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/options.html"},
        {"type": "service_worker", "url": f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/background.js"},
        {"type": "page", "url": "chrome-error://chromewebdata/"},
    ]).encode("utf-8")
    monkeypatch.setattr("runtime.extension_bootstrap.urlopen", lambda url, timeout: FakeResponse(payload))

    result = CdpClient().discover_extension(9399, "options.html", "background.js")

    assert result["discovered_extension_id"] == EXPECTED_FLOWKIT_EXTENSION_ID
    assert result["options_target_url"] == f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/options.html"
    assert wrong_id not in result["options_target_url"]


def test_discover_extension_accepts_service_worker_when_options_closed(monkeypatch):
    payload = json.dumps([
        {"type": "service_worker", "url": f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/background.js"},
    ]).encode("utf-8")
    monkeypatch.setattr("runtime.extension_bootstrap.urlopen", lambda url, timeout: FakeResponse(payload))

    result = CdpClient().discover_extension(9399, "options.html", "background.js")

    assert result["discovered_extension_id"] == EXPECTED_FLOWKIT_EXTENSION_ID
    assert result["options_target_url"] is None
    assert result["service_worker_target_url"] == f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/background.js"


def test_discover_extension_rejects_non_chrome_extension_ids(monkeypatch):
    payload = json.dumps([
        {"type": "page", "url": "chrome-extension://abc123/options.html"},
        {"type": "page", "url": "chrome-extension://zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz/options.html"},
    ]).encode("utf-8")
    monkeypatch.setattr("runtime.extension_bootstrap.urlopen", lambda url, timeout: FakeResponse(payload))

    result = CdpClient().discover_extension(9399, "options.html", "background.js")

    assert result["discovered_extension_id"] is None


def test_empty_profile_and_old_marker_are_not_template_ready(tmp_path):
    registry = make_registry(tmp_path)
    template = registry.profiles_root / TEMPLATE_PROFILE_NAME
    template.mkdir(parents=True)
    (template / TEMPLATE_READY_FILE).write_text(json.dumps({"ready": True}), encoding="utf-8")
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    status = bootstrapper.template_status()

    assert status["template_marker_present"] is True
    assert status["template_marker_valid"] is False
    assert status["template_ready"] is False
    assert bootstrapper.init_template().result == "template_marker_invalid"


def test_empty_profile_without_marker_cannot_be_copied_to_flow007(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry, "FLOW-007")
    (registry.profiles_root / TEMPLATE_PROFILE_NAME).mkdir(parents=True)
    runtime = FakeRuntime()
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-007")

    assert result.result == "extension_template_not_ready"
    assert not Path(registry.get("FLOW-007").profile_path).exists()
    assert runtime.opened == []


def test_init_template_uses_load_extension_only_for_first_launch(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    runtime = FakeRuntime()
    cdp = FakeCdp(EXPECTED_FLOWKIT_EXTENSION_ID)
    extension_dir = tmp_path / "extension"
    write_manifest(extension_dir)
    bootstrapper = ExtensionBootstrapper(
        registry,
        runtime=runtime,
        cdp=cdp,
        profiles_root=registry.profiles_root,
        extension_dir=extension_dir,
        sleep=lambda _: None,
    )
    monkeypatch.setattr("runtime.extension_bootstrap.port_is_listening", lambda port: False)
    monkeypatch.setattr("runtime.extension_bootstrap.port_can_bind", lambda port: True)

    result = bootstrapper.init_template()

    assert result.result == "template_ready"
    assert len(runtime.launched) == 2
    first_command = runtime.launched[0][1]
    second_command = runtime.launched[1][1]
    assert any("--load-extension=" in part for part in first_command)
    assert not any("--disable-extensions-except" in part for part in first_command)
    assert not any("--load-extension=" in part for part in second_command)
    assert (registry.profiles_root / TEMPLATE_PROFILE_NAME / TEMPLATE_READY_FILE).exists()
    marker = json.loads((registry.profiles_root / TEMPLATE_PROFILE_NAME / TEMPLATE_READY_FILE).read_text(encoding="utf-8"))
    assert marker["extension_id"] == EXPECTED_FLOWKIT_EXTENSION_ID
    assert marker["extension_version"] == "0.2.0"
    assert marker["manifest_sha256"]
    assert marker["persistence_verified"] is True
    assert marker["verified_without_load_extension"] is True


def test_init_template_first_extension_load_failure_does_not_write_marker(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    runtime = FakeRuntime()
    extension_dir = tmp_path / "extension"
    write_manifest(extension_dir)
    bootstrapper = ExtensionBootstrapper(
        registry,
        runtime=runtime,
        cdp=FakeCdp(None),
        profiles_root=registry.profiles_root,
        extension_dir=extension_dir,
        sleep=lambda _: None,
    )
    monkeypatch.setattr("runtime.extension_bootstrap.port_is_listening", lambda port: False)
    monkeypatch.setattr("runtime.extension_bootstrap.port_can_bind", lambda port: True)

    result = bootstrapper.init_template()

    assert result.result == "extension_template_load_failed"
    assert result.details["first_launch_verified"] is False
    assert not (registry.profiles_root / TEMPLATE_PROFILE_NAME / TEMPLATE_READY_FILE).exists()


def test_init_template_reports_manual_install_when_extension_not_persisted(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    runtime = FakeRuntime()
    extension_dir = tmp_path / "extension"
    write_manifest(extension_dir)

    class FlakyCdp:
        def __init__(self):
            self.calls = 0

        def wait_ready(self, cdp_port, **kwargs):
            return {"Browser": "Chrome"}

        def extension_id(self, cdp_port):
            return EXPECTED_FLOWKIT_EXTENSION_ID

        def discover_extension(self, cdp_port, options_page, service_worker):
            return {
                "discovered_extension_id": EXPECTED_FLOWKIT_EXTENSION_ID,
                "options_target_url": f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/{options_page}",
                "service_worker_target_url": f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/{service_worker}",
            }

        def verify_extension_options(self, cdp_port, extension_id, options_page):
            self.calls += 1
            return self.calls == 1

    bootstrapper = ExtensionBootstrapper(
        registry,
        runtime=runtime,
        cdp=FlakyCdp(),
        profiles_root=registry.profiles_root,
        extension_dir=extension_dir,
        sleep=lambda _: None,
    )
    monkeypatch.setattr("runtime.extension_bootstrap.port_is_listening", lambda port: False)
    monkeypatch.setattr("runtime.extension_bootstrap.port_can_bind", lambda port: True)

    result = bootstrapper.init_template()

    assert result.result == "extension_template_manual_install_required"
    assert result.details["reason"] == "extension_not_persisted"
    assert result.details["first_launch_verified"] is True
    assert result.details["persistence_verified"] is False
    assert not (registry.profiles_root / TEMPLATE_PROFILE_NAME / TEMPLATE_READY_FILE).exists()


def test_manifest_change_invalidates_marker(tmp_path):
    registry = make_registry(tmp_path)
    extension_dir = tmp_path / "extension"
    write_manifest(extension_dir, version="0.2.0")
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root, extension_dir=extension_dir)
    bootstrapper.mark_template_ready_for_verified_profile({"first_launch_verified": True, "persistence_verified": True, "verified_without_load_extension": True})

    write_manifest(extension_dir, version="0.2.1")

    status = bootstrapper.template_status()
    assert status["manifest_fingerprint_matches"] is False
    assert status["template_ready"] is False


def test_marker_with_wrong_extension_id_is_invalid(tmp_path):
    registry = make_registry(tmp_path)
    ready_template(registry.profiles_root)
    marker_path = registry.profiles_root / TEMPLATE_PROFILE_NAME / TEMPLATE_READY_FILE
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker["extension_id"] = "behjnghbkgnggenapbhgjoclmnfgfaim"
    marker_path.write_text(json.dumps(marker), encoding="utf-8")
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    status = bootstrapper.template_status()

    assert status["extension_id_matches"] is False
    assert status["template_ready"] is False


def test_profile_created_from_template_without_locks_or_cache(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    ready_template(registry.profiles_root)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper.copy_template_to_profile(account)

    target = Path(account.profile_path)
    assert result.result == "profile_created_from_template"
    assert (target / "Default" / "Preferences").exists()
    assert (target / "Default" / "Extensions").is_dir()
    assert not (target / "Default" / "SingletonLock").exists()
    assert not (target / "Default" / "Cache").exists()
    assert not (target / "Default" / "Cookies").exists()
    assert not (target / "Default" / "Login Data").exists()


def test_profile_created_from_template_sanitizes_credential_storage(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    template = ready_template(registry.profiles_root)
    write_sensitive_profile_state(template)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper.copy_template_to_profile(account)

    target = Path(account.profile_path)
    assert result.result == "profile_created_from_template"
    assert result.details["credential_storage_sanitized"] is True
    assert result.details["credential_storage_policy_version"] == CREDENTIAL_STORAGE_POLICY_VERSION
    assert not (target / "Default" / "Local Extension Settings" / EXPECTED_FLOWKIT_EXTENSION_ID).exists()
    assert not (target / "Default" / "Local Storage").exists()
    assert not (target / "Default" / "IndexedDB").exists()
    assert not (target / "Default" / "Session Storage").exists()
    assert not (target / "Default" / "Service Worker").exists()
    assert not (target / "Default" / "Sessions").exists()
    assert not (target / "Default" / "Storage").exists()
    assert not (target / "Default" / "WebStorage").exists()
    assert not (target / "Default" / "SharedStorage").exists()
    assert not (target / "Default" / "SharedStorage-wal").exists()
    assert not (target / "Default" / "SharedStorage-shm").exists()
    assert not (target / "Default" / "SharedStorage-journal").exists()
    assert (target / "Default" / "Preferences").exists()
    assert (target / "Default" / "Secure Preferences").exists()
    assert (target / "Default" / "Extension State" / "MANIFEST-000001").exists()
    assert (template / "Default" / "Local Extension Settings" / EXPECTED_FLOWKIT_EXTENSION_ID / "000003.log").exists()
    assert (template / "Default" / "Local Storage" / "leveldb" / "000003.log").exists()
    assert (template / "Default" / "SharedStorage").exists()


def test_profile_cleanup_failure_fails_bootstrap_without_leaking_values(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    ready_template(registry.profiles_root)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)
    target = Path(account.profile_path)
    (target / "Default" / "Local Storage").mkdir(parents=True)
    (target / "Default" / "Local Storage" / "secret.txt").write_text("SIMULATED_TOKEN_SHOULD_NOT_LEAK", encoding="utf-8")

    original_rmtree = __import__("shutil").rmtree

    def fail_local_storage(path, *args, **kwargs):
        if Path(path).name == "Local Storage":
            raise OSError("permission denied")
        return original_rmtree(path, *args, **kwargs)

    monkeypatch.setattr("runtime.extension_bootstrap.shutil.rmtree", fail_local_storage)

    result = bootstrapper.sanitize_copied_profile(target)

    rendered = json.dumps(result.to_dict())
    assert result.result == "credential_storage_cleanup_failed"
    assert result.ok is False
    assert result.details["credential_storage_sanitized"] is False
    assert "SIMULATED_TOKEN_SHOULD_NOT_LEAK" not in rendered


def test_profile_cleanup_file_failure_fails_without_leaking_values(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    ready_template(registry.profiles_root)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)
    target = Path(account.profile_path)
    (target / "Default").mkdir(parents=True)
    shared = target / "Default" / "SharedStorage"
    shared.write_text("SIMULATED_CALLBACK_SECRET_SHOULD_NOT_LEAK", encoding="utf-8")

    def fail_unlink(self):
        raise PermissionError("locked")

    monkeypatch.setattr(Path, "unlink", fail_unlink)

    result = bootstrapper.sanitize_copied_profile(target)

    rendered = json.dumps(result.to_dict())
    assert result.result == "credential_storage_cleanup_failed"
    assert result.ok is False
    assert "SIMULATED_CALLBACK_SECRET_SHOULD_NOT_LEAK" not in rendered


def test_existing_profile_is_not_overwritten(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    ready_template(registry.profiles_root)
    target = Path(account.profile_path)
    target.mkdir(parents=True)
    (target / "keep.txt").write_text("login data", encoding="utf-8")
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper.copy_template_to_profile(account)

    assert result.result == "profile_exists"
    assert (target / "keep.txt").read_text(encoding="utf-8") == "login data"


def test_bootstrap_generates_account_specific_url_and_verifies_connection(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-006")
    Path(account.profile_path).mkdir(parents=True)
    runtime = FakeRuntime(status={"extension_connected": True, "extension_account_id": "FLOW-006", "account_match": True})
    cdp = FakeCdp(EXPECTED_FLOWKIT_EXTENSION_ID)
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=cdp, profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_account("FLOW-006", repair=True)

    assert result.result == "extension_bootstrapped"
    assert runtime.start_one_calls == []
    assert runtime.worker_only_started == ["FLOW-006"]
    command = runtime.launched[0][1]
    assert any(f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/options.html?" in part for part in command)
    assert any("account_id=FLOW-006" in part for part in command)
    assert any("ws_url=ws%3A%2F%2F127.0.0.1%3A9201" in part for part in command)
    assert not any("labs.google" in part or "aisandbox" in part for part in command)
    assert result.details["extension_expected_ws_url"] == "ws://127.0.0.1:9201"


def test_new_profile_bootstrap_reports_credential_storage_sanitized(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-006")
    template = ready_template(registry.profiles_root)
    write_sensitive_profile_state(template)
    runtime = FakeRuntime(status={"extension_connected": True, "extension_account_id": "FLOW-006", "account_match": True})
    cdp = FakeCdp(EXPECTED_FLOWKIT_EXTENSION_ID)
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=cdp, profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.result == "extension_bootstrapped"
    assert result.details["credential_storage_sanitized"] is True
    assert result.details["credential_storage_policy_version"] == CREDENTIAL_STORAGE_POLICY_VERSION
    assert not (Path(account.profile_path) / "Default" / "Local Extension Settings" / EXPECTED_FLOWKIT_EXTENSION_ID).exists()


def test_registered_empty_profile_is_rebuilt_from_template_without_repair(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-006")
    Path(account.profile_path).mkdir(parents=True)
    template = ready_template(registry.profiles_root)
    write_sensitive_profile_state(template)
    runtime = FakeRuntime(status={"extension_connected": True, "extension_account_id": "FLOW-006", "account_match": True})
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FakeCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_account("FLOW-006")

    target = Path(account.profile_path)
    assert result.result == "extension_bootstrapped"
    assert result.details["profile_initialization_mode"] == "rebuilt_registered_empty_profile"
    assert result.details["credential_storage_sanitized"] is True
    assert result.details["credential_storage_policy_version"] == CREDENTIAL_STORAGE_POLICY_VERSION
    assert (target / "Default" / "Preferences").exists()
    assert not (target / "Default" / "Local Extension Settings" / EXPECTED_FLOWKIT_EXTENSION_ID).exists()
    assert runtime.launched
    assert runtime.start_one_calls == []
    assert runtime.worker_only_started == ["FLOW-006"]
    assert not any("labs.google" in part or "aisandbox" in part for part in runtime.launched[0][1])


def test_registered_profile_with_empty_default_placeholder_is_rebuilt(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-006")
    (Path(account.profile_path) / "Default").mkdir(parents=True)
    ready_template(registry.profiles_root)
    runtime = FakeRuntime(status={"extension_connected": True, "extension_account_id": "FLOW-006", "account_match": True})
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FakeCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.result == "extension_bootstrapped"
    assert result.details["profile_initialization_mode"] == "rebuilt_registered_empty_profile"


def test_registered_profile_with_file_is_partial_and_not_deleted(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-006")
    target = Path(account.profile_path)
    target.mkdir(parents=True)
    (target / "note.txt").write_text("SIMULATED_TOKEN_SHOULD_NOT_LEAK", encoding="utf-8")
    ready_template(registry.profiles_root)
    runtime = FakeRuntime()
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-006")

    rendered = json.dumps(result.to_dict())
    assert result.result == "profile_partial_requires_manual_review"
    assert result.details["profile_state"] == "partial_or_unsafe"
    assert result.details["safe_to_rebuild"] is False
    assert result.details["blocking_paths"] == ["note.txt"]
    assert "SIMULATED_TOKEN_SHOULD_NOT_LEAK" not in rendered
    assert (target / "note.txt").exists()
    assert runtime.launched == []


def test_partial_chrome_artifacts_are_not_rebuilt_automatically(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-006")
    target = Path(account.profile_path)
    for relative in (
        Path("Local State"),
        Path("Crashpad") / "reports" / "crash.dmp",
        Path("Default") / "Service Worker" / "Database" / "LOG",
        Path("Default") / "Sessions" / "Session_1",
        Path("Default") / "WebStorage" / "QuotaManager",
    ):
        path = target / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("partial", encoding="utf-8")
    ready_template(registry.profiles_root)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.result == "profile_partial_requires_manual_review"
    assert "Local State" in result.details["blocking_paths"]
    assert "Crashpad/reports/crash.dmp" in result.details["blocking_paths"]
    assert "Default/Service Worker/Database/LOG" in result.details["blocking_paths"]
    assert "Default/Sessions/Session_1" in result.details["blocking_paths"]
    assert "Default/WebStorage/QuotaManager" in result.details["blocking_paths"]


def test_failed_chrome_profile_with_extension_state_is_partial_not_bootstrapped(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-006")
    target = Path(account.profile_path)
    for relative in (
        Path("Default") / "Extension State" / "MANIFEST-000001",
        Path("Default") / "Service Worker" / "Database" / "LOG",
        Path("Default") / "Sessions",
        Path("Default") / "WebStorage" / "QuotaManager",
        Path("Crashpad") / "reports" / "crash.dmp",
    ):
        path = target / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix or path.name.startswith("MANIFEST") or path.name == "LOG":
            path.write_text("partial", encoding="utf-8")
        else:
            path.mkdir(parents=True, exist_ok=True)
    ready_template(registry.profiles_root)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.result == "profile_partial_requires_manual_review"
    assert result.details["profile_state"] == "partial_or_unsafe"
    assert (target / "Default" / "Extension State" / "MANIFEST-000001").exists()


def test_real_profile_features_are_never_deleted(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-006")
    target = Path(account.profile_path)
    for relative in (
        Path("Default") / "Preferences",
        Path("Default") / "Secure Preferences",
        Path("Default") / "Cookies",
        Path("Default") / "Login Data",
        Path("Default") / "Local Extension Settings" / EXPECTED_FLOWKIT_EXTENSION_ID / "LOG",
        Path("Default") / "Local Storage" / "leveldb" / "LOG",
        Path("Default") / "IndexedDB" / "db" / "LOG",
    ):
        path = target / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("profile-data", encoding="utf-8")
    ready_template(registry.profiles_root)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.result == "profile_exists"
    assert result.details["profile_state"] == "bootstrapped_profile"
    assert (target / "Default" / "Preferences").exists()
    assert (target / "Default" / "Cookies").exists()


def test_repair_existing_profile_does_not_recopy_template(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-006")
    target = Path(account.profile_path)
    target.mkdir(parents=True)
    (target / "keep.txt").write_text("keep", encoding="utf-8")
    ready_template(registry.profiles_root)
    runtime = FakeRuntime(status={"extension_connected": True, "extension_account_id": "FLOW-006", "account_match": True})
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FakeCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_account("FLOW-006", repair=True)

    assert result.result == "extension_bootstrapped"
    assert result.details["profile_initialization_mode"] == "repaired_existing_profile"
    assert result.details["credential_storage_sanitized"] is False
    assert (target / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_rebuild_rejects_template_path_and_account_path_mismatch(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-006")
    template = ready_template(registry.profiles_root)
    account = replace(account, profile_path=str(template))
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper._profile_rebuild_safety(account)

    assert result["safe_to_rebuild"] is False
    assert result["profile_state"] == "partial_or_unsafe"


def test_rebuild_delete_failure_blocks_bootstrap(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry, "FLOW-006")
    Path(account.profile_path).mkdir(parents=True)
    ready_template(registry.profiles_root)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    def fail_rmtree(path, *args, **kwargs):
        raise OSError("locked")

    monkeypatch.setattr("runtime.extension_bootstrap.shutil.rmtree", fail_rmtree)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.result == "profile_registered_empty_cleanup_failed"
    assert result.details["profile_state"] == "registered_empty"


def test_bootstrap_uses_different_config_per_account(tmp_path):
    registry = make_registry(tmp_path)
    for account_id in ("FLOW-006", "FLOW-007"):
        account = add_account(registry, account_id)
        Path(account.profile_path).mkdir(parents=True)
    cdp = FakeCdp(EXPECTED_FLOWKIT_EXTENSION_ID)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(status={"extension_connected": True, "account_match": True}), cdp=cdp, profiles_root=registry.profiles_root, sleep=lambda _: None)

    bootstrapper.bootstrap_account("FLOW-006", repair=True)
    bootstrapper.bootstrap_account("FLOW-007", repair=True)

    first_command = bootstrapper.runtime.launched[0][1]
    second_command = bootstrapper.runtime.launched[1][1]
    assert any("account_id=FLOW-006" in part for part in first_command)
    assert any("ws_url=ws%3A%2F%2F127.0.0.1%3A9201" in part for part in first_command)
    assert any("account_id=FLOW-007" in part for part in second_command)
    assert any("ws_url=ws%3A%2F%2F127.0.0.1%3A9202" in part for part in second_command)


def test_bootstrap_rejects_invalid_account_id_or_port(tmp_path):
    registry = make_registry(tmp_path)
    bad = AccountRecord(
        account_id="BAD",
        display_name="BAD",
        profile_path=str(registry.profiles_root / "BAD"),
        worker_api_port=8104,
        extension_ws_port=9203,
        chrome_cdp_port=9306,
        database_path=str(registry.data_root / "BAD.db"),
        output_dir=str(registry.outputs_root / "BAD"),
        enabled=True,
        status="login_required",
        created_at="2026-07-23T00:00:00Z",
    )
    registry.register_many([bad], create_dirs=False)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    assert bootstrapper.bootstrap_account("BAD").result == "invalid_account_id"


def test_extension_missing_returns_precise_status(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    Path(account.profile_path).mkdir(parents=True)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(None), profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-006", repair=True)

    assert result.result == "extension_template_not_ready"
    assert result.details["template_ready"] is False
    assert result.details["repair"] is True


def test_discovered_extension_id_mismatch_returns_precise_status(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    Path(account.profile_path).mkdir(parents=True)
    wrong_but_valid_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(wrong_but_valid_id), profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-006", repair=True)

    assert result.result == "extension_id_mismatch"
    assert result.details["expected_extension_id"] == EXPECTED_FLOWKIT_EXTENSION_ID
    assert result.details["discovered_extension_id"] == wrong_but_valid_id


def test_extension_missing_with_ready_template_returns_install_required_without_open_url(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    Path(account.profile_path).mkdir(parents=True)
    ready_template(registry.profiles_root)
    cdp = FakeCdp(None)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=cdp, profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-006", repair=True)

    assert result.result == "extension_install_required"
    assert cdp.opened_urls == []


def test_failure_compensates_started_worker(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    Path(account.profile_path).mkdir(parents=True)
    runtime = FakeRuntime(status={"extension_connected": False, "extension_account_id": None, "account_match": False})
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FakeCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_account("FLOW-006", repair=True)

    assert result.result == "extension_not_connected"
    assert runtime.stopped == ["FLOW-006"]


def test_bootstrap_extension_not_connected_keeps_initialization_context_and_avoids_second_chrome(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    ready_template(registry.profiles_root)
    Path(account.profile_path).mkdir(parents=True)
    runtime = FakeRuntime(status={"extension_connected": False, "extension_account_id": None, "account_match": False})
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FakeCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.result == "extension_not_connected"
    assert runtime.start_one_calls == []
    assert runtime.worker_only_started == ["FLOW-006"]
    assert len(runtime.launched) == 1
    assert not any("labs.google" in part or "aisandbox" in part for part in runtime.launched[0][1])
    assert result.details["chrome_started_by_bootstrap"] is True
    assert result.details["worker_started_by_bootstrap"] is True
    assert result.details["compensation_result"] == "stopped"
    assert result.details["profile_initialization_mode"] == "rebuilt_registered_empty_profile"
    assert result.details["credential_storage_sanitized"] is True
    assert result.details["credential_storage_policy_version"] == CREDENTIAL_STORAGE_POLICY_VERSION


def test_reused_chrome_cdp_open_failure_is_structured_without_stopping_existing_chrome(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    Path(account.profile_path).mkdir(parents=True)

    class ReusedRuntime(FakeRuntime):
        def _owned_chrome_running(self, account):
            return True

    class FailingCdp(FakeCdp):
        def open_url(self, cdp_port, url):
            raise Exception("boom")

    runtime = ReusedRuntime()
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FailingCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-006", repair=True)

    assert result.result == "failed"
    assert result.details["chrome_started_by_bootstrap"] is False
    assert result.details["worker_started_by_bootstrap"] is True
    assert runtime.stopped == ["FLOW-006"]
    assert runtime.stopped_pids["chrome_pid"] is None


def test_reused_chrome_open_failure_does_not_close_existing_chrome(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    Path(account.profile_path).mkdir(parents=True)

    class FailingCdp(FakeCdp):
        def open_url(self, cdp_port, url):
            raise Exception("boom")

    class ReusedRuntime(FakeRuntime):
        def _owned_chrome_running(self, account):
            return True

    runtime = ReusedRuntime()
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FailingCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-006", repair=True)

    assert result.result == "failed"
    assert result.details["chrome_started_by_bootstrap"] is False
    assert runtime.stopped == ["FLOW-006"]
    assert runtime.stopped_pids["chrome_pid"] is None


def test_batch_results_are_isolated(tmp_path):
    registry = make_registry(tmp_path)
    for account_id in ("FLOW-006", "FLOW-007"):
        account = add_account(registry, account_id)
        Path(account.profile_path).mkdir(parents=True)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(status={"extension_connected": True, "account_match": True}), cdp=FakeCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_batch(["FLOW-006", "FLOW-007"], repair=True)

    assert result.result == "bootstrap_batch_completed"
    assert [item["account_id"] for item in result.details["accounts"]] == ["FLOW-006", "FLOW-007"]
    assert result.ok is True


def test_batch_continues_after_account_failure(tmp_path):
    registry = make_registry(tmp_path)
    for account_id in ("FLOW-006", "FLOW-007"):
        account = add_account(registry, account_id)
        Path(account.profile_path).mkdir(parents=True)

    class PartlyFailingBootstrapper(ExtensionBootstrapper):
        def bootstrap_account(self, account_id, repair=False):
            if account_id == "FLOW-006":
                raise RuntimeError("fail")
            return super().bootstrap_account(account_id, repair=repair)

    bootstrapper = PartlyFailingBootstrapper(registry, runtime=FakeRuntime(status={"extension_connected": True, "account_match": True}), cdp=FakeCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_batch(["FLOW-006", "FLOW-007"], repair=True)

    assert result.ok is False
    assert result.details["accounts"][0]["result"] == "failed"
    assert result.details["accounts"][1]["result"] == "extension_bootstrapped"


def test_cli_bootstrap_commands_use_bootstrapper(tmp_path, monkeypatch, capsys):
    registry = make_registry(tmp_path)
    calls = []

    class FakeBootstrapper:
        def __init__(self, registry_arg):
            calls.append(("init", registry_arg))

        def init_template(self):
            return type("Result", (), {"ok": True, "to_dict": lambda self: {"result": "template_ready", "ok": True, "details": {}}})()

        def bootstrap_account(self, account_id, repair=False):
            calls.append(("one", account_id, repair))
            return type("Result", (), {"ok": True, "to_dict": lambda self: {"result": "extension_bootstrapped", "ok": True, "account_id": account_id, "details": {}}})()

        def bootstrap_batch(self, account_ids, repair=False):
            calls.append(("batch", account_ids, repair))
            return type("Result", (), {"ok": True, "to_dict": lambda self: {"result": "bootstrap_batch_completed", "ok": True, "details": {"accounts": account_ids}}})()

    monkeypatch.setattr(cli, "AccountRegistry", lambda: registry)
    monkeypatch.setattr(cli, "ExtensionBootstrapper", FakeBootstrapper)

    assert cli.main(["init-extension-template"]) == 0
    assert json.loads(capsys.readouterr().out)["result"] == "template_ready"
    assert cli.main(["bootstrap-extension", "FLOW-006", "--repair"]) == 0
    assert calls[-1] == ("one", "FLOW-006", True)
    assert cli.main(["bootstrap-batch", "FLOW-006", "FLOW-007"]) == 0
    assert calls[-1] == ("batch", ["FLOW-006", "FLOW-007"], False)


def test_cli_bootstrap_exception_returns_json_without_traceback(tmp_path, monkeypatch, capsys):
    registry = make_registry(tmp_path)

    class FailingBootstrapper:
        def __init__(self, registry_arg):
            pass

        def bootstrap_account(self, account_id, repair=False):
            raise RuntimeError("boom")

    monkeypatch.setattr(cli, "AccountRegistry", lambda: registry)
    monkeypatch.setattr(cli, "ExtensionBootstrapper", FailingBootstrapper)

    assert cli.main(["bootstrap-extension", "FLOW-006"]) == 1
    output = capsys.readouterr()
    data = json.loads(output.out)
    assert data["result"] == "failed"
    assert data["details"]["error"] == "RuntimeError"
    assert "Traceback" not in output.out
    assert output.err == ""


def test_regular_chrome_command_does_not_readd_extension_flags(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    chrome = tmp_path / "chrome.exe"
    chrome.write_text("", encoding="utf-8")
    manager = RuntimeManager(registry, chrome_path=chrome)

    command = manager.chrome_command(account)

    assert not any("--load-extension" in part for part in command)
    assert not any("--disable-extensions-except" in part for part in command)


def test_bootstrap_chrome_command_opens_options_not_flow_url(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    command = bootstrapper.bootstrap_chrome_command(account, f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/options.html?bootstrap=1")

    assert any("chrome-extension://" in part and "options.html?bootstrap=1" in part for part in command)
    assert not any("labs.google" in part or "aisandbox" in part for part in command)


def test_bootstrap_chrome_command_forces_disable_skia_graphite_once_and_removes_conflicts(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    command = bootstrapper.bootstrap_chrome_command(
        account,
        f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/options.html?bootstrap=1",
        extra_args=[
            "--disable-skia-graphite",
            "--enable-skia-graphite",
            "--enable-skia-graphite=true",
            "--no-sandbox",
        ],
    )

    assert command.count("--disable-skia-graphite") == 1
    assert "--enable-skia-graphite" not in command
    assert "--enable-skia-graphite=true" not in command
    assert "--no-sandbox" in command
    assert "--disable-gpu" not in command
    assert "--use-angle" not in command
    assert "--use-gl" not in command
    assert "--skia-graphite-dawn-backend" not in command


def test_bootstrap_chrome_logs_are_account_specific_and_handles_close(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    Path(account.profile_path).mkdir(parents=True)
    runtime = FakeRuntime()
    runtime.log_dir = tmp_path / "logs" / "runtime"
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper._open_bootstrap_chrome(account, bootstrapper.bootstrap_url(account, "SECRET_NONCE", EXPECTED_FLOWKIT_EXTENSION_ID))

    assert result.result == "opened"
    assert result.details["stdout_log"].endswith("FLOW-006-bootstrap-chrome-attempt-1-stdout.log")
    assert result.details["stderr_log"].endswith("FLOW-006-bootstrap-chrome-attempt-1-stderr.log")
    kwargs = runtime.launched[0][2]
    assert kwargs["stdout"].closed is True
    assert kwargs["stderr"].closed is True


def test_bootstrap_retries_once_for_gpu_chrome_exit_before_cdp_ready(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    ready_template(registry.profiles_root)
    template_cache = registry.profiles_root / TEMPLATE_PROFILE_NAME / "GPUPersistentCache" / "DawnGraphiteCache" / "abc" / "cache.db"
    template_cache.parent.mkdir(parents=True)
    template_cache.write_text("cache", encoding="utf-8")
    Path(account.profile_path).mkdir(parents=True)
    runtime = FakeRuntime(status={"extension_connected": True, "extension_account_id": "FLOW-006", "account_match": True})
    runtime.log_dir = tmp_path / "logs" / "runtime"
    runtime.poll_sequences = [[9], [None]]
    original_popen = runtime.popen
    cache_present_at_popen = []

    def record_cache_state(command, **kwargs):
        cache_present_at_popen.append((Path(account.profile_path) / "GPUPersistentCache").exists())
        return original_popen(command, **kwargs)

    runtime.popen = record_cache_state

    class GpuFailThenReadyCdp(FakeCdp):
        def __init__(self):
            super().__init__(EXPECTED_FLOWKIT_EXTENSION_ID)
            self.ready_calls = []

        def wait_ready(self, cdp_port, **kwargs):
            self.ready_calls.append(kwargs)
            if len(self.ready_calls) == 1:
                raise CdpError(
                    "bootstrap_chrome_exited",
                    {
                        "stage": "wait_cdp_ready",
                        "cdp_port": cdp_port,
                        "chrome_pid": 1000,
                        "chrome_exit_code": 9,
                        "attempts": 1,
                    },
                )
            return {"Browser": "Chrome"}

    class StderrBootstrapper(ExtensionBootstrapper):
        def _open_bootstrap_chrome(self, account, bootstrap_url, **kwargs):
            result = super()._open_bootstrap_chrome(account, bootstrap_url, **kwargs)
            if len(runtime.launched) == 1:
                Path(result.details["stderr_log"]).write_text(
                    "GPU process exited unexpectedly\n"
                    "FATAL: GPU process isn't usable. Goodbye.\n"
                    "GPUPersistentCache\\DawnGraphiteCache data_0 failed 0x20\n",
                    encoding="utf-8",
                )
            return result

    bootstrapper = StderrBootstrapper(registry, runtime=runtime, cdp=GpuFailThenReadyCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.ok is True
    assert result.result == "extension_bootstrapped"
    assert runtime.worker_only_started == ["FLOW-006"]
    assert len(runtime.launched) == 2
    forbidden_gpu_args = {"--disable-gpu", "--use-angle", "--use-gl", "--skia-graphite-dawn-backend"}
    assert runtime.launched[0][1].count("--disable-skia-graphite") == 1
    assert runtime.launched[1][1].count("--disable-skia-graphite") == 1
    assert not any(arg in runtime.launched[0][1] for arg in forbidden_gpu_args)
    assert not any(arg in runtime.launched[1][1] for arg in forbidden_gpu_args)
    assert "--disable-gpu" not in runtime.launched[0][1]
    assert "--disable-gpu" not in runtime.launched[1][1]
    assert runtime.launched[0][1][-1] != runtime.launched[1][1][-1]
    assert Path(account.profile_path).exists()
    failed_root = registry.profiles_root / "_failed_bootstrap"
    quarantined = list(failed_root.glob("FLOW-006-*-attempt-1"))
    assert len(quarantined) == 1
    assert result.details["bootstrap_chrome_attempts"] == 2
    assert result.details["bootstrap_chrome_retry_used"] is True
    assert result.details["bootstrap_profile_rebuild_used"] is True
    assert result.details["bootstrap_second_attempt_uses_fresh_profile"] is True
    assert result.details["bootstrap_profile_rebuild_result"] == "rebuilt_registered_empty_profile"
    assert result.details["bootstrap_chrome_retry_permitted"] is True
    assert result.details["bootstrap_chrome_retry_block_reason"] == "none"
    assert result.details["bootstrap_chrome_retry_reason"] == "gpu_process_unusable"
    assert result.details["first_attempt_chrome_pid"] == 1000
    assert result.details["second_attempt_chrome_pid"] == 1001
    assert result.details["final_chrome_pid"] == 1001
    assert cache_present_at_popen == [False, False]
    attempts = result.details["bootstrap_chrome_attempt_diagnostics"]
    cleanup_attempts = [item for item in attempts if "volatile_cache_cleanup" in item]
    assert [item["attempt_number"] for item in cleanup_attempts] == [1, 2]
    assert all(item["volatile_cache_cleanup"]["bootstrap_volatile_cache_cleanup_used"] is True for item in cleanup_attempts)
    launch_attempts = [item for item in attempts if "disable_skia_graphite_present" in item]
    assert [item["attempt_number"] for item in launch_attempts] == [1, 2]
    assert all(item["browser_executable_kind"] == "system_chrome" for item in launch_attempts)
    assert all(item["disable_skia_graphite_present"] is True for item in launch_attempts)
    assert result.details["bootstrap_disable_skia_graphite"] is True


def test_registered_empty_bootstrap_removes_gpupersistentcache_before_first_chrome(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    ready_template(registry.profiles_root)
    template_cache = registry.profiles_root / TEMPLATE_PROFILE_NAME / "GPUPersistentCache" / "DawnGraphiteCache" / "abc" / "cache.db"
    template_cache.parent.mkdir(parents=True)
    template_cache.write_text("cache", encoding="utf-8")
    profile = Path(account.profile_path)
    profile.mkdir(parents=True)
    runtime = FakeRuntime(status={"extension_connected": True, "extension_account_id": "FLOW-006", "account_match": True})
    original_popen = runtime.popen

    def assert_cache_removed_before_popen(command, **kwargs):
        assert not (profile / "GPUPersistentCache").exists()
        return original_popen(command, **kwargs)

    runtime.popen = assert_cache_removed_before_popen
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FakeCdp(EXPECTED_FLOWKIT_EXTENSION_ID), profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.ok is True
    assert result.details["bootstrap_volatile_cache_cleanup_used"] is True
    assert result.details["bootstrap_gpupersistentcache_present_before"] is True
    assert result.details["bootstrap_gpupersistentcache_present_after"] is False


def test_volatile_cache_cleanup_noops_when_gpupersistentcache_missing(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    profile = Path(account.profile_path)
    profile.mkdir(parents=True)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper._cleanup_bootstrap_volatile_cache(
        account,
        {"profile_initialization_mode": "rebuilt_registered_empty_profile", "credential_storage_sanitized": True},
        attempt=1,
    )

    assert result["ok"] is True
    assert result["details"]["bootstrap_volatile_cache_cleanup_result"] == "no_op_missing"
    assert result["details"]["bootstrap_gpupersistentcache_present_after"] is False


def test_volatile_cache_cleanup_blocks_logged_in_profile(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    profile = Path(account.profile_path)
    (profile / "Default").mkdir(parents=True)
    (profile / "Default" / "Login Data").write_text("secretish", encoding="utf-8")
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper._cleanup_bootstrap_volatile_cache(
        account,
        {"profile_initialization_mode": "rebuilt_registered_empty_profile", "credential_storage_sanitized": True},
        attempt=1,
    )

    assert result["ok"] is False
    assert result["reason"] == "profile_volatile_cache_cleanup_failed"
    assert result["details"]["bootstrap_volatile_cache_cleanup_block_reason"] == "google_login_artifacts_present"


def test_volatile_cache_cleanup_blocks_account_path_mismatch(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    wrong = replace(account, profile_path=str(registry.profiles_root / "FLOW-007"))
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper._cleanup_bootstrap_volatile_cache(
        wrong,
        {"profile_initialization_mode": "rebuilt_registered_empty_profile", "credential_storage_sanitized": True},
        attempt=1,
    )

    assert result["ok"] is False
    assert result["details"]["bootstrap_volatile_cache_cleanup_block_reason"] == "account_profile_path_mismatch"


def test_bootstrap_gpu_chrome_exit_does_not_retry_when_worker_unhealthy_and_reports_reason(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    ready_template(registry.profiles_root)
    Path(account.profile_path).mkdir(parents=True)
    runtime = FakeRuntime(status={
        "worker_health_reachable": False,
        "worker_pid": 2001,
        "extension_connected": False,
        "extension_account_id": "FLOW-006",
        "account_match": False,
    })
    runtime.worker_ownership_payload = {
        "verified": False,
        "reason": "worker_health_unreachable",
        "method": "worker_challenge",
        "worker_launcher_alive": True,
        "worker_api_reachable": True,
        "worker_ws_reachable": True,
        "worker_listener_pid_consistent": True,
        "worker_identity_match": True,
        "worker_challenge_verified": False,
        "worker_pid_cas_applied": False,
    }
    runtime.log_dir = tmp_path / "logs" / "runtime"
    runtime.poll_sequences = [[9]]

    class GpuFailCdp(FakeCdp):
        def wait_ready(self, cdp_port, **kwargs):
            raise CdpError(
                "bootstrap_chrome_exited",
                {
                    "stage": "wait_cdp_ready",
                    "cdp_port": cdp_port,
                    "chrome_pid": 1000,
                    "chrome_exit_code": 9,
                    "attempts": 1,
                },
            )

    class StderrBootstrapper(ExtensionBootstrapper):
        def _open_bootstrap_chrome(self, account, bootstrap_url, **kwargs):
            result = super()._open_bootstrap_chrome(account, bootstrap_url, **kwargs)
            Path(result.details["stderr_log"]).write_text(
                "GPU process exited unexpectedly\n"
                "FATAL: GPU process isn't usable. Goodbye.\n",
                encoding="utf-8",
            )
            return result

    bootstrapper = StderrBootstrapper(registry, runtime=runtime, cdp=GpuFailCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.ok is False
    assert result.result == "bootstrap_chrome_exited"
    assert len(runtime.launched) == 1
    assert result.details["bootstrap_chrome_retry_eligible"] is True
    assert result.details["bootstrap_chrome_retry_classified_eligible"] is True
    assert result.details["bootstrap_chrome_retry_permitted"] is False
    assert result.details["bootstrap_chrome_retry_block_reason"] == "worker_health_unreachable"
    assert result.details["bootstrap_worker_health_reason"] == "worker_health_unreachable"
    assert result.details["bootstrap_worker_challenge_verified"] is False
    assert result.details["bootstrap_chrome_retry_used"] is False
    assert result.details["bootstrap_chrome_attempts"] == 1


def test_bootstrap_gpu_retry_uses_worker_challenge_when_status_is_stale(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    ready_template(registry.profiles_root)
    Path(account.profile_path).mkdir(parents=True)
    runtime = FakeRuntime(status={
        "worker_health_reachable": False,
        "worker_pid": 2001,
        "extension_connected": True,
        "extension_account_id": "FLOW-006",
        "account_match": True,
    })
    runtime.log_dir = tmp_path / "logs" / "runtime"
    runtime.poll_sequences = [[9], [None]]

    class GpuFailThenReadyCdp(FakeCdp):
        def __init__(self):
            super().__init__(EXPECTED_FLOWKIT_EXTENSION_ID)
            self.ready_calls = []

        def wait_ready(self, cdp_port, **kwargs):
            self.ready_calls.append(kwargs)
            if len(self.ready_calls) == 1:
                raise CdpError(
                    "bootstrap_chrome_exited",
                    {
                        "stage": "wait_cdp_ready",
                        "cdp_port": cdp_port,
                        "chrome_pid": 1000,
                        "chrome_exit_code": 9,
                        "attempts": 1,
                    },
                )
            return {"Browser": "Chrome"}

    class StderrBootstrapper(ExtensionBootstrapper):
        def _open_bootstrap_chrome(self, account, bootstrap_url, **kwargs):
            result = super()._open_bootstrap_chrome(account, bootstrap_url, **kwargs)
            if len(runtime.launched) == 1:
                Path(result.details["stderr_log"]).write_text(
                    "GPU process exited unexpectedly\n"
                    "FATAL: GPU process isn't usable. Goodbye.\n",
                    encoding="utf-8",
                )
            return result

    bootstrapper = StderrBootstrapper(registry, runtime=runtime, cdp=GpuFailThenReadyCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.ok is True
    assert len(runtime.launched) == 2
    assert result.details["bootstrap_chrome_retry_used"] is True
    assert result.details["bootstrap_chrome_retry_permitted"] is True
    assert result.details["bootstrap_chrome_retry_block_reason"] == "none"
    assert result.details["bootstrap_worker_challenge_verified"] is True
    assert result.details["bootstrap_worker_health_reason"] == "none"


def test_bootstrap_gpu_retry_blocks_when_profile_never_stabilizes(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    ready_template(registry.profiles_root)
    Path(account.profile_path).mkdir(parents=True)
    runtime = FakeRuntime(status={"extension_connected": True, "extension_account_id": "FLOW-006", "account_match": True})
    runtime.log_dir = tmp_path / "logs" / "runtime"
    runtime.poll_sequences = [[9]]

    class GpuFailCdp(FakeCdp):
        def wait_ready(self, cdp_port, **kwargs):
            raise CdpError(
                "bootstrap_chrome_exited",
                {
                    "stage": "wait_cdp_ready",
                    "cdp_port": cdp_port,
                    "chrome_pid": 1000,
                    "chrome_exit_code": 9,
                    "attempts": 1,
                },
            )

    class StderrBootstrapper(ExtensionBootstrapper):
        def _open_bootstrap_chrome(self, account, bootstrap_url, **kwargs):
            result = super()._open_bootstrap_chrome(account, bootstrap_url, **kwargs)
            Path(result.details["stderr_log"]).write_text(
                "GPU process exited unexpectedly\n"
                "FATAL: GPU process isn't usable. Goodbye.\n",
                encoding="utf-8",
            )
            return result

    bootstrapper = StderrBootstrapper(registry, runtime=runtime, cdp=GpuFailCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)
    monkeypatch.setattr(
        bootstrapper,
        "_wait_profile_stable_for_retry",
        lambda account: {"ok": False, "reason": "profile_not_stable_for_retry", "wait_ms": 3000},
    )

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.ok is False
    assert result.result == "profile_not_stable_for_retry"
    assert len(runtime.launched) == 1
    assert Path(account.profile_path).exists()
    assert not (registry.profiles_root / "_failed_bootstrap").exists()
    assert result.details["bootstrap_chrome_retry_used"] is False
    assert result.details["bootstrap_profile_rebuild_used"] is False
    assert result.details["bootstrap_profile_rebuild_block_reason"] == "profile_not_stable_for_retry"


def test_bootstrap_gpu_retry_exhausted_when_second_fresh_profile_chrome_exits(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    ready_template(registry.profiles_root)
    Path(account.profile_path).mkdir(parents=True)
    runtime = FakeRuntime(status={
        "extension_connected": False,
        "extension_account_id": "FLOW-006",
        "account_match": False,
        "chrome_process_alive": False,
        "chrome_cdp_reachable": False,
    })
    runtime.log_dir = tmp_path / "logs" / "runtime"
    runtime.poll_sequences = [[9], [9]]

    class GpuFailTwiceCdp(FakeCdp):
        def __init__(self):
            super().__init__(EXPECTED_FLOWKIT_EXTENSION_ID)
            self.ready_calls = 0

        def wait_ready(self, cdp_port, **kwargs):
            self.ready_calls += 1
            if self.ready_calls == 1:
                raise CdpError(
                    "bootstrap_chrome_exited",
                    {
                        "stage": "wait_cdp_ready",
                        "cdp_port": cdp_port,
                        "chrome_pid": 1000,
                        "chrome_exit_code": 9,
                        "attempts": 1,
                    },
                )
            return {"Browser": "Chrome"}

    class StderrBootstrapper(ExtensionBootstrapper):
        def _open_bootstrap_chrome(self, account, bootstrap_url, **kwargs):
            result = super()._open_bootstrap_chrome(account, bootstrap_url, **kwargs)
            if len(runtime.launched) == 1:
                Path(result.details["stderr_log"]).write_text(
                    "GPU process exited unexpectedly\n"
                    "FATAL: GPU process isn't usable. Goodbye.\n",
                    encoding="utf-8",
                )
            return result

    bootstrapper = StderrBootstrapper(registry, runtime=runtime, cdp=GpuFailTwiceCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.ok is False
    assert result.result == "bootstrap_chrome_retry_exhausted"
    assert len(runtime.launched) == 2
    assert result.details["bootstrap_chrome_attempts"] == 2
    assert result.details["bootstrap_chrome_retry_used"] is True
    assert result.details["bootstrap_profile_rebuild_used"] is True


def test_dawn_lock_probe_triggers_once_from_safe_stderr_path(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    profile = Path(account.profile_path)
    target = profile / "GPUPersistentCache" / "DawnGraphiteCache" / "abc" / "cache.db"
    target.parent.mkdir(parents=True)
    target.write_text("cache", encoding="utf-8")
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)
    calls = []

    def fake_probe(path, expected_profile_root, **kwargs):
        calls.append((path, expected_profile_root, kwargs))
        return {
            "target_path_safe": True,
            "target_relative": "GPUPersistentCache/DawnGraphiteCache/abc/cache.db",
            "summary": {
                "dawn_lock_probe_sample_count": 3,
                "dawn_lock_probe_holder_count": 1,
                "dawn_lock_probe_holder_classification": "external_process",
                "dawn_lock_probe_current_attempt_chrome_detected": False,
                "dawn_lock_probe_previous_attempt_chrome_detected": False,
                "dawn_lock_probe_external_process_detected": True,
            },
        }

    monkeypatch.setattr("runtime.extension_bootstrap.probe_dawn_cache_lock", fake_probe)
    line = (
        f'[1000:2000:0724/191153.365:ERROR:x] Failed to open persistent cache files in directory '
        f'"{target}": sharing violation (0x20)'
    )

    assert bootstrapper._maybe_probe_dawn_lock_from_stderr_line(account, line, attempt=1, chrome_pid=1000) is True
    assert bootstrapper._maybe_probe_dawn_lock_from_stderr_line(account, line, attempt=1, chrome_pid=1000) is True

    details = bootstrapper._collect_dawn_lock_probe_details(1000)
    assert len(calls) == 1
    assert details["dawn_lock_probe_triggered"] is True
    assert details["dawn_lock_probe_path_safe"] is True
    assert details["dawn_lock_probe_target_relative"] == "GPUPersistentCache/DawnGraphiteCache/abc/cache.db"
    assert details["dawn_lock_probe_holder_classification"] == "external_process"
    assert details["dawn_lock_probe_external_process_detected"] is True
    assert details["bootstrap_chrome_attempt_diagnostics"][0]["attempt_number"] == 1


def test_dawn_lock_probe_error_is_not_reported_as_no_holder_found(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    profile = Path(account.profile_path)
    target = profile / "GPUPersistentCache" / "DawnGraphiteCache" / "abc"
    target.mkdir(parents=True)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    def fake_probe(*_args, **_kwargs):
        return {
            "target_path_safe": True,
            "target_relative": "GPUPersistentCache/DawnGraphiteCache/abc",
            "resource_kind": "directory",
            "candidate_count": 1,
            "probe_error": "OSError",
            "probe_error_stage": "RmGetList",
            "probe_rm_result_code": 5,
            "summary": {
                "dawn_lock_probe_sample_count": 1,
                "dawn_lock_probe_holder_count": None,
                "dawn_lock_probe_holder_classification": "probe_error",
                "dawn_lock_probe_current_attempt_chrome_detected": False,
                "dawn_lock_probe_previous_attempt_chrome_detected": False,
                "dawn_lock_probe_external_process_detected": False,
            },
        }

    monkeypatch.setattr("runtime.extension_bootstrap.probe_dawn_cache_lock", fake_probe)
    line = f'[1000:2000:0724/191153.365:ERROR:x] "{target}": sharing violation (0x20)'

    assert bootstrapper._maybe_probe_dawn_lock_from_stderr_line(account, line, attempt=1, chrome_pid=1000) is True

    details = bootstrapper._collect_dawn_lock_probe_details(1000)
    assert details["dawn_lock_probe_error"] == "OSError"
    assert details["dawn_lock_probe_rm_result_code"] == 5
    assert details["dawn_lock_probe_holder_count"] is None
    assert details["dawn_lock_probe_holder_classification"] == "probe_error"


def test_dawn_lock_probe_attempt_diagnostics_are_independent(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    profile = Path(account.profile_path)
    target = profile / "GPUPersistentCache" / "DawnGraphiteCache" / "abc"
    target.mkdir(parents=True)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    def fake_probe(*_args, **kwargs):
        attempt = kwargs["attempt_number"]
        if attempt == 1:
            classification = "probe_error"
            holder_count = None
            probe_error = "OSError"
        else:
            classification = "external_process"
            holder_count = 1
            probe_error = None
        return {
            "target_path_safe": True,
            "target_relative": "GPUPersistentCache/DawnGraphiteCache/abc",
            "resource_kind": "directory",
            "candidate_count": 1,
            "probe_error": probe_error,
            "summary": {
                "dawn_lock_probe_sample_count": 1,
                "dawn_lock_probe_holder_count": holder_count,
                "dawn_lock_probe_holder_classification": classification,
                "dawn_lock_probe_current_attempt_chrome_detected": False,
                "dawn_lock_probe_previous_attempt_chrome_detected": False,
                "dawn_lock_probe_external_process_detected": classification == "external_process",
            },
        }

    monkeypatch.setattr("runtime.extension_bootstrap.probe_dawn_cache_lock", fake_probe)
    line = f'[1000:2000:0724/191153.365:ERROR:x] "{target}": sharing violation (0x20)'

    assert bootstrapper._maybe_probe_dawn_lock_from_stderr_line(account, line, attempt=1, chrome_pid=1000) is True
    assert bootstrapper._maybe_probe_dawn_lock_from_stderr_line(account, line, attempt=2, chrome_pid=2000) is True

    details = bootstrapper._collect_dawn_lock_probe_details(2000)
    attempts = details["bootstrap_chrome_attempt_diagnostics"]
    assert [item["attempt_number"] for item in attempts] == [1, 2]
    assert attempts[0]["dawn_lock_probe"]["dawn_lock_probe_holder_classification"] == "probe_error"
    assert attempts[1]["dawn_lock_probe"]["dawn_lock_probe_holder_classification"] == "external_process"


def test_bootstrap_chrome_failure_classifier_does_not_retry_profile_lock_or_google_update_access(tmp_path):
    registry = make_registry(tmp_path)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)
    stderr = tmp_path / "stderr.log"

    stderr.write_text("Google Update registry access denied\n", encoding="utf-8")
    update_noise = bootstrapper._classify_bootstrap_chrome_failure(str(stderr))
    assert update_noise["failure_class"] == "unknown_early_exit"
    assert update_noise["retry_eligible"] is False

    stderr.write_text("user data directory is already in use: SingletonLock\n", encoding="utf-8")
    profile_lock = bootstrapper._classify_bootstrap_chrome_failure(str(stderr))
    assert profile_lock["failure_class"] == "profile_lock_error"
    assert profile_lock["retry_eligible"] is False


def test_gpu_cache_cleanup_only_removes_whitelisted_profile_caches(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    profile = Path(account.profile_path)
    preserved = [
        profile / "Default" / "Preferences",
        profile / "Default" / "Secure Preferences",
        profile / "Default" / "Extension State" / "LOCK",
        profile / "Default" / "Local Extension Settings" / EXPECTED_FLOWKIT_EXTENSION_ID / "000003.log",
        profile / "Default" / "Service Worker" / "Database" / "CURRENT",
        profile / "Default" / "WebStorage" / "QuotaManager",
    ]
    removed = [
        profile / "GPUCache" / "data_0",
        profile / "Default" / "GPUCache" / "data_1",
        profile / "GPUPersistentCache" / "DawnGraphiteCache" / "data_2",
        profile / "Default" / "ShaderCache" / "data_3",
    ]
    for path in preserved + removed:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")

    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)
    result = bootstrapper._cleanup_gpu_caches(account)

    assert result["ok"] is True
    assert all(not path.exists() for path in removed)
    assert all(path.exists() for path in preserved)


def test_bootstrap_chrome_command_redacts_nonce_and_sensitive_query(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    runtime = FakeRuntime()
    runtime.log_dir = tmp_path / "logs" / "runtime"
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FakeCdp(), profiles_root=registry.profiles_root)
    command = bootstrapper.bootstrap_chrome_command(
        account,
        f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/options.html?bootstrap=1&account_id=FLOW-006&ws_url=ws://127.0.0.1:9201&api_url=http://127.0.0.1:8102&nonce=SECRET_NONCE",
    )

    redacted = bootstrapper._redacted_bootstrap_command(command)

    joined = " ".join(redacted)
    assert "SECRET_NONCE" not in joined
    assert "nonce=%3Credacted%3E" in joined or "nonce=<redacted>" in joined
    assert "FLOW-006" in joined
    assert "127.0.0.1" in joined
    assert "flowKey" not in joined


def test_cdp_target_summary_records_only_safe_categories():
    targets = [
        {"type": "page", "url": f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/options.html?bootstrap=1&nonce=SECRET"},
        {"type": "service_worker", "url": f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/background.js"},
        {"type": "page", "url": "chrome-extension://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/options.html?token=SECRET"},
        {"type": "page", "url": "https://labs.google/fx/tools/flow?token=SECRET"},
        {"type": "page", "url": "about:blank"},
    ]

    summary = CdpClient().target_summary(targets, EXPECTED_FLOWKIT_EXTENSION_ID, "options.html", "background.js")

    assert summary["options_target_seen"] is True
    assert summary["service_worker_target_seen"] is True
    assert summary["target_counts"]["target_extension_options"] == 1
    assert summary["target_counts"]["target_extension_service_worker"] == 1
    assert summary["target_counts"]["target_other_extension"] == 1
    assert summary["target_counts"]["target_non_extension"] == 1
    assert "SECRET" not in json.dumps(summary)


def test_bootstrap_returns_chrome_exited_when_chrome_dies_during_extension_wait(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    ready_template(registry.profiles_root)
    Path(account.profile_path).mkdir(parents=True)
    runtime = FakeRuntime(status={
        "extension_connected": False,
        "extension_account_id": "FLOW-006",
        "account_match": False,
        "chrome_process_alive": False,
        "chrome_cdp_reachable": False,
        "worker_process_alive": True,
        "worker_health_reachable": True,
    })
    runtime.log_dir = tmp_path / "logs" / "runtime"
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FakeCdp(), profiles_root=registry.profiles_root, sleep=lambda _: None)

    result = bootstrapper.bootstrap_account("FLOW-006")

    assert result.result == "bootstrap_chrome_exited"
    assert result.details["stage"] == "wait_extension_ready"
    assert result.details["chrome_exit_detected_at"]
    assert result.details["options_target_seen"] is True
    assert result.details["service_worker_target_seen"] is True
    assert result.details["credential_storage_policy_version"] == CREDENTIAL_STORAGE_POLICY_VERSION


def test_flow_client_bootstrap_diagnostics_are_restricted_and_bounded():
    client = FlowClient()

    assert client.record_bootstrap_diagnostic({"source": "options", "event": "bootstrap_detected", "account_id": "FLOW-999"}, "FLOW-006") is False
    assert client.record_bootstrap_diagnostic({"source": "options", "event": "unknown_event", "account_id": "FLOW-006"}, "FLOW-006") is False
    assert client.record_bootstrap_diagnostic({
        "source": "background",
        "event": "websocket_connect_attempt",
        "account_id": "FLOW-006",
        "ws": {"host": "127.0.0.1", "port": 9201},
        "flowKey": "SIMULATED_TOKEN_SHOULD_NOT_RECORD",
    }, "FLOW-006") is True
    for index in range(60):
        client.record_bootstrap_diagnostic({"source": "options", "event": "bootstrap_detected", "account_id": "FLOW-006", "at": index}, "FLOW-006")

    snapshot = client.bootstrap_diagnostics
    assert snapshot["event_count"] == 50
    assert snapshot["ws_connect_attempted"] is False
    assert "SIMULATED_TOKEN" not in json.dumps(snapshot)


def test_template_persistence_launch_does_not_open_flow_url(tmp_path):
    registry = make_registry(tmp_path)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(), cdp=FakeCdp(), profiles_root=registry.profiles_root)

    first = bootstrapper.template_chrome_command(load_extension=True)
    second = bootstrapper.template_chrome_command(load_extension=False)

    assert first[-1] == "about:blank"
    assert second[-1] == "about:blank"
    assert not any("labs.google" in part or "aisandbox" in part for part in first + second)


def test_regular_start_one_does_not_reference_credential_sanitizer():
    text = Path("runtime/process_manager.py").read_text(encoding="utf-8")

    assert "sanitize_copied_profile" not in text


def test_options_js_contains_bootstrap_validation():
    text = Path("extension/options.js").read_text(encoding="utf-8")

    assert "FLOW-\\d{3,}" in text
    assert "127.0.0.1" in text
    assert "bootstrap_nonces" in text
    assert "history.replaceState" in text
    assert "BOOTSTRAP_RESET" in text
    assert "chrome.storage.local.remove(['flowKey', 'callbackSecret', 'metrics'])" in text
    assert "metrics: emptyMetrics()" in text
    for stage in (
        "bootstrap_detected",
        "parameters_validated",
        "bootstrap_reset_requested",
        "bootstrap_reset_succeeded",
        "storage_cleanup_started",
        "storage_cleanup_succeeded",
        "account_config_write_started",
        "account_config_write_succeeded",
        "nonce_recorded",
        "reconnect_requested",
        "reconnect_acknowledged",
        "bootstrap_completed",
        "bootstrap_failed",
    ):
        assert stage in text
    assert "/api/ext/bootstrap-diagnostic" in text


def test_background_bootstrap_reset_clears_sensitive_memory_without_logging_values():
    text = Path("extension/background.js").read_text(encoding="utf-8")

    assert "BOOTSTRAP_RESET" in text
    assert "flowKey = null" in text
    assert "callbackSecret = null" in text
    assert "tokenCapturedAt: null" in text
    assert "requestCount: 0" in text
    assert "SIMULATED_TOKEN" not in text
    for event in (
        "service_worker_started",
        "bootstrap_reset_received",
        "bootstrap_reset_completed",
        "old_websocket_closed",
        "storage_change_observed",
        "reconnect_received",
        "websocket_connect_attempt",
        "websocket_open",
        "register_sent",
        "extension_ready_sent",
        "websocket_error",
        "websocket_close",
    ):
        assert event in text


def test_template_default_config_does_not_connect_flow001():
    options = Path("extension/options.js").read_text(encoding="utf-8")
    background = Path("extension/background.js").read_text(encoding="utf-8")

    assert "data.account_id || ''" in options
    assert "data.ws_url || ''" in options
    assert "const DEFAULT_ACCOUNT_ID = ''" in background
    assert "const DEFAULT_AGENT_WS_URL = ''" in background
    assert "if (accountId && wsUrl) connectToAgent()" in background
    assert "new WebSocket(wsUrl)" in background
