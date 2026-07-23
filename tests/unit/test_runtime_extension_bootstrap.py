import json
import hashlib
from urllib.error import HTTPError
from pathlib import Path

from runtime import cli
from runtime.extension_bootstrap import CdpClient, EXPECTED_FLOWKIT_EXTENSION_ID, ExtensionBootstrapper, TEMPLATE_PROFILE_NAME, TEMPLATE_READY_FILE
from runtime.process_manager import RuntimeManager, RuntimeResult
from runtime.registry import AccountRecord, AccountRegistry


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

    def _find_chrome(self):
        return Path("C:/Chrome/chrome.exe")

    def popen(self, command, **kwargs):
        pid = 1000 + len(self.launched)
        self.launched.append((pid, command, kwargs))
        return type("Proc", (), {"pid": pid})()

    def open_login(self, account_id):
        self.opened.append(account_id)
        return RuntimeResult("already_running", account_id, True)

    def start_one(self, account_id):
        self.started.append(account_id)
        return RuntimeResult("started", account_id, True)

    def stop_one(self, account_id):
        self.stopped.append(account_id)
        return RuntimeResult("stopped", account_id, True)

    def status(self, account_id):
        payload = {**self.status_payload}
        payload.setdefault("extension_account_id", account_id)
        payload.setdefault("account_match", payload.get("extension_account_id") == account_id and payload.get("extension_connected"))
        return RuntimeResult("running", account_id, bool(payload.get("account_match")), details=payload)


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
        }

    def extension_present(self, cdp_port):
        return self.extension_id_value is not None

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
    (template / "Default" / "Extensions").mkdir()
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
    assert cdp.opened_urls[0][0] == 9304
    assert f"chrome-extension://{EXPECTED_FLOWKIT_EXTENSION_ID}/options.html?" in cdp.opened_urls[0][1]
    assert "account_id=FLOW-006" in cdp.opened_urls[0][1]
    assert "ws_url=ws%3A%2F%2F127.0.0.1%3A9201" in cdp.opened_urls[0][1]
    assert result.details["extension_expected_ws_url"] == "ws://127.0.0.1:9201"


def test_bootstrap_uses_different_config_per_account(tmp_path):
    registry = make_registry(tmp_path)
    for account_id in ("FLOW-006", "FLOW-007"):
        account = add_account(registry, account_id)
        Path(account.profile_path).mkdir(parents=True)
    cdp = FakeCdp(EXPECTED_FLOWKIT_EXTENSION_ID)
    bootstrapper = ExtensionBootstrapper(registry, runtime=FakeRuntime(status={"extension_connected": True, "account_match": True}), cdp=cdp, profiles_root=registry.profiles_root, sleep=lambda _: None)

    bootstrapper.bootstrap_account("FLOW-006", repair=True)
    bootstrapper.bootstrap_account("FLOW-007", repair=True)

    assert "account_id=FLOW-006" in cdp.opened_urls[0][1]
    assert "ws_url=ws%3A%2F%2F127.0.0.1%3A9201" in cdp.opened_urls[0][1]
    assert "account_id=FLOW-007" in cdp.opened_urls[1][1]
    assert "ws_url=ws%3A%2F%2F127.0.0.1%3A9202" in cdp.opened_urls[1][1]


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


def test_cdp_open_failure_is_structured_and_compensates_bootstrap_started_chrome(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    Path(account.profile_path).mkdir(parents=True)

    class OpeningRuntime(FakeRuntime):
        def open_login(self, account_id):
            self.opened.append(account_id)
            return RuntimeResult("opened", account_id, True)

    class FailingCdp(FakeCdp):
        def open_url(self, cdp_port, url):
            raise Exception("boom")

    runtime = OpeningRuntime()
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FailingCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-006", repair=True)

    assert result.result == "failed"
    assert result.details["chrome_started_by_bootstrap"] is True
    assert result.details["worker_started_by_bootstrap"] is False
    assert runtime.stopped == ["FLOW-006"]


def test_reused_chrome_open_failure_does_not_close_existing_chrome(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    Path(account.profile_path).mkdir(parents=True)

    class FailingCdp(FakeCdp):
        def open_url(self, cdp_port, url):
            raise Exception("boom")

    runtime = FakeRuntime()
    bootstrapper = ExtensionBootstrapper(registry, runtime=runtime, cdp=FailingCdp(), profiles_root=registry.profiles_root)

    result = bootstrapper.bootstrap_account("FLOW-006", repair=True)

    assert result.result == "failed"
    assert result.details["chrome_started_by_bootstrap"] is False
    assert runtime.stopped == []


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


def test_options_js_contains_bootstrap_validation():
    text = Path("extension/options.js").read_text(encoding="utf-8")

    assert "FLOW-\\d{3,}" in text
    assert "127.0.0.1" in text
    assert "bootstrap_nonces" in text
    assert "history.replaceState" in text


def test_template_default_config_does_not_connect_flow001():
    options = Path("extension/options.js").read_text(encoding="utf-8")
    background = Path("extension/background.js").read_text(encoding="utf-8")

    assert "data.account_id || ''" in options
    assert "data.ws_url || ''" in options
    assert "const DEFAULT_ACCOUNT_ID = ''" in background
    assert "const DEFAULT_AGENT_WS_URL = ''" in background
    assert "if (accountId && wsUrl) connectToAgent()" in background
    assert "new WebSocket(wsUrl)" in background
