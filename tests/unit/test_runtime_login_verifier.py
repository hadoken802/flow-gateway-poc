import json
import sqlite3
from pathlib import Path

from runtime.login_verifier import ConfirmLoginService, LoginVerificationResult, LoginVerifier
from runtime.process_manager import CommandLineProbeResult, ProcessProbeResult
from runtime.registry import AccountRecord, AccountRegistry


class FakeInspector:
    def __init__(self):
        self.alive = set()
        self.commands = {}
        self.listeners = {}

    def probe_process(self, pid):
        return ProcessProbeResult(pid=pid, alive=pid in self.alive, status="alive" if pid in self.alive else "not_found", method="fake")

    def command_line_probe(self, pid):
        command = self.commands.get(pid, "")
        return CommandLineProbeResult(pid=pid, command_line=command, status="available" if command else "cim_empty")

    def listening_pid(self, port):
        return self.listeners.get(port)


class FakeCdp:
    def __init__(self, targets=None, target_sequence=None, open_ok=True, evidence=None, evidence_sequence=None):
        self.targets = targets or []
        self.target_sequence = list(target_sequence or [])
        self.opened = []
        self.open_ok = open_ok
        self.evidence = evidence or {}
        self.evidence_sequence = list(evidence_sequence or [])
        self.evaluated = []

    def list_targets(self, cdp_port):
        if self.target_sequence:
            return self.target_sequence.pop(0)
        return self.targets

    def open_url(self, cdp_port, url):
        self.opened.append((cdp_port, url))
        return {"id": "target-1"} if self.open_ok else None

    def flow_page_evidence(self, target):
        self.evaluated.append(target.get("id"))
        if self.evidence_sequence:
            return self.evidence_sequence.pop(0)
        return self.evidence.get(target.get("id"), {})


def make_registry(tmp_path):
    workers_path = tmp_path / "workers.json"
    workers_path.write_text("[]", encoding="utf-8")
    return AccountRegistry(
        db_path=tmp_path / "data" / "runtime_registry.db",
        profiles_root=tmp_path / "profiles",
        data_root=tmp_path / "data",
        outputs_root=tmp_path / "outputs",
        workers_json_path=workers_path,
    )


def add_account(registry, account_id="FLOW-005"):
    profile = registry.profiles_root / account_id
    profile.mkdir(parents=True, exist_ok=True)
    account = AccountRecord(
        account_id=account_id,
        display_name=account_id,
        profile_path=str(profile),
        worker_api_port=8101,
        extension_ws_port=9200,
        chrome_cdp_port=9300,
        database_path=str(registry.data_root / f"{account_id}.db"),
        output_dir=str(registry.outputs_root / account_id),
        status="login_required",
        created_at="2026-07-23T00:00:00Z",
        chrome_pid=101,
        worker_pid=202,
    )
    registry.register_many([account], create_dirs=False)
    registry.mark_started(account_id, chrome_pid=101, worker_pid=202)
    return registry.get(account_id)


def make_verifier(tmp_path, health, targets=None, target_sequence=None):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    inspector = FakeInspector()
    inspector.alive.update({101, 202})
    inspector.commands[101] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    cdp = FakeCdp(targets=targets, target_sequence=target_sequence)
    verifier = LoginVerifier(registry, inspector=inspector, cdp=cdp, sleep=lambda _: None)
    verifier._worker_health = lambda _port: dict(health)
    return verifier, registry, cdp


def flow_target(title="Flow"):
    return [{"id": "flow-1", "type": "page", "url": "https://labs.google/fx/tools/flow", "title": title}]


def logged_in_evidence():
    return {"flow_app_marker_detected": True, "account_ui_marker_detected": True, "login_form_marker_detected": False}


def verified_login_result(**overrides):
    data = {
        "account_id": "FLOW-005",
        "browser_running": True,
        "worker_running": True,
        "extension_connected": True,
        "extension_ready": True,
        "account_match": True,
        "cdp_connectable": True,
        "profile_path_matches_registry": True,
        "google_logged_in": True,
        "flow_accessible": True,
        "login_redirect_detected": False,
        "login_verified": True,
        "reason": "verified",
    }
    data.update(overrides)
    return LoginVerificationResult(**data)


class FakeVerifier:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def verify(self, account_id):
        self.calls.append(account_id)
        self.result.account_id = account_id
        return self.result


def test_extension_not_connected_returns_false(tmp_path):
    verifier, _, _ = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": False}, flow_target())

    result = verifier.verify("FLOW-005")

    assert result.login_verified is False
    assert result.reason == "extension_not_connected"


def test_account_mismatch_returns_false(tmp_path):
    verifier, _, _ = make_verifier(tmp_path, {"account_id": "FLOW-006", "extension_connected": True}, flow_target())

    result = verifier.verify("FLOW-005")

    assert result.login_verified is False
    assert result.reason == "account_mismatch"


def test_flow_accessible_without_redirect_returns_true(tmp_path):
    verifier, _, cdp = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, flow_target())
    cdp.evidence = {"flow-1": logged_in_evidence()}

    result = verifier.verify("FLOW-005")

    assert result.flow_accessible is True
    assert result.login_redirect_detected is False
    assert result.google_logged_in is True
    assert result.login_verified is True
    assert result.reason == "verified"
    assert cdp.opened == []


def test_logged_in_flow_page_with_stale_accounts_target_returns_true(tmp_path):
    targets = [
        {"id": "login-1", "type": "page", "url": "https://accounts.google.com/signin/v2/identifier?token=SECRET", "title": "Sign in"},
        {"id": "flow-1", "type": "page", "url": "https://labs.google/fx/tools/flow", "title": "Google Flow"},
    ]
    verifier, _, cdp = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, targets)
    cdp.evidence = {"flow-1": logged_in_evidence()}

    result = verifier.verify("FLOW-005")

    assert result.stale_login_target_detected is True
    assert result.login_redirect_detected is False
    assert result.flow_accessible is True
    assert result.google_logged_in is True
    assert result.login_verified is True
    assert result.selected_flow_target_host == "labs.google"
    assert result.selected_flow_target_path == "/fx/tools/flow"


def test_multiple_flow_targets_succeeds_when_one_is_logged_in(tmp_path):
    targets = [
        {"id": "flow-loading", "type": "page", "url": "https://labs.google/fx/tools/flow", "title": "Loading"},
        {"id": "flow-ready", "type": "page", "url": "https://labs.google/fx/tools/flow", "title": "Google Flow"},
    ]
    verifier, _, cdp = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, targets)
    cdp.evidence = {
        "flow-loading": {"flow_app_marker_detected": False, "account_ui_marker_detected": False, "login_form_marker_detected": False},
        "flow-ready": logged_in_evidence(),
    }

    result = verifier.verify("FLOW-005")

    assert result.login_verified is True
    assert cdp.evaluated == ["flow-loading", "flow-ready"]


def test_accounts_google_redirect_returns_false(tmp_path):
    targets = [{"type": "page", "url": "https://accounts.google.com/signin/v2/identifier", "title": "Sign in"}]
    verifier, _, _ = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, targets)

    result = verifier.verify("FLOW-005")

    assert result.login_redirect_detected is True
    assert result.login_verified is False


def test_login_button_or_account_chooser_returns_false(tmp_path):
    targets = [{"id": "flow-1", "type": "page", "url": "https://labs.google/fx/tools/flow", "title": "Choose an account"}]
    verifier, _, _ = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, targets)

    result = verifier.verify("FLOW-005")

    assert result.login_redirect_detected is True
    assert result.login_verified is False


def test_flow_page_login_form_returns_false(tmp_path):
    verifier, _, cdp = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, flow_target())
    cdp.evidence = {"flow-1": {"flow_app_marker_detected": False, "account_ui_marker_detected": False, "login_form_marker_detected": True}}

    result = verifier.verify("FLOW-005")

    assert result.flow_accessible is False
    assert result.login_redirect_detected is True
    assert result.login_verified is False


def test_flow_page_delayed_app_marker_eventually_succeeds(tmp_path):
    targets = flow_target()
    verifier, _, cdp = make_verifier(
        tmp_path,
        {"account_id": "FLOW-005", "extension_connected": True},
        target_sequence=[targets, targets],
    )
    cdp.evidence_sequence = [
        {"flow_app_marker_detected": False, "account_ui_marker_detected": False, "login_form_marker_detected": False},
        logged_in_evidence(),
    ]

    result = verifier.verify("FLOW-005")

    assert result.login_verified is True
    assert result.verification_attempts == 2


def test_flow_page_verification_timeout_when_app_never_loads(tmp_path):
    verifier, _, cdp = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, flow_target())
    cdp.evidence = {"flow-1": {"flow_app_marker_detected": False, "account_ui_marker_detected": False, "login_form_marker_detected": False}}

    result = verifier.verify("FLOW-005", wait_seconds=0)

    assert result.login_verified is False
    assert result.reason == "flow_page_verification_timeout"


def test_get_started_carousel_button_is_not_login_marker(tmp_path):
    verifier, _, cdp = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, flow_target())
    cdp.evidence = {"flow-1": {"flow_app_marker_detected": True, "account_ui_marker_detected": True, "login_form_marker_detected": False, "get_started_marker_detected": True}}

    result = verifier.verify("FLOW-005")

    assert result.login_verified is True


def test_profile_match_can_use_cdp_listener_pid_when_recorded_pid_is_stale(tmp_path):
    verifier, registry, cdp = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, flow_target())
    account = registry.get("FLOW-005")
    verifier.inspector.alive.discard(101)
    verifier.inspector.commands.pop(101)
    verifier.inspector.alive.add(303)
    verifier.inspector.listeners[account.chrome_cdp_port] = 303
    verifier.inspector.commands[303] = f"chrome --user-data-dir={account.profile_path} --remote-debugging-port=9300"
    cdp.evidence = {"flow-1": logged_in_evidence()}

    result = verifier.verify("FLOW-005")

    assert result.browser_running is True
    assert result.profile_path_matches_registry is True
    assert result.login_verified is True


def test_profile_match_infers_when_recorded_pid_owns_cdp_but_command_line_unavailable(tmp_path):
    verifier, registry, cdp = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, flow_target())
    account = registry.get("FLOW-005")
    verifier.inspector.listeners[account.chrome_cdp_port] = 101
    verifier.inspector.commands.pop(101)
    cdp.evidence = {"flow-1": logged_in_evidence()}

    result = verifier.verify("FLOW-005")

    assert result.browser_running is True
    assert result.profile_path_matches_registry is True
    assert result.login_verified is True


def test_output_does_not_include_email_cookie_or_token(tmp_path):
    targets = [{"type": "page", "url": "https://labs.google/fx/tools/flow?token=SECRET", "title": "Flow user@example.com Cookie"}]
    verifier, _, _ = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, targets)

    output = json.dumps(verifier.verify("FLOW-005").to_dict())

    assert "user@example.com" not in output
    assert "SECRET" not in output
    assert "Cookie" not in output
    assert "token" not in output.lower()


def test_timeout_returns_clear_reason(tmp_path):
    verifier, _, _ = make_verifier(
        tmp_path,
        {"account_id": "FLOW-005", "extension_connected": True},
        target_sequence=[[{"type": "page", "url": "about:blank", "title": ""}]],
    )

    result = verifier.verify("FLOW-005", wait_seconds=0)

    assert result.login_verified is False
    assert result.reason == "google_login_missing"


def test_verify_login_does_not_modify_profile_or_registry(tmp_path):
    verifier, registry, cdp = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, flow_target())
    cdp.evidence = {"flow-1": logged_in_evidence()}
    account = registry.get("FLOW-005")
    profile = Path(account.profile_path)
    before_files = sorted(path.relative_to(profile) for path in profile.rglob("*"))
    before_row = _registry_row(registry, "FLOW-005")

    result = verifier.verify("FLOW-005")

    assert result.login_verified is True
    assert sorted(path.relative_to(profile) for path in profile.rglob("*")) == before_files
    assert _registry_row(registry, "FLOW-005") == before_row


def test_confirm_login_account_not_found_does_not_write_registry(tmp_path):
    registry = make_registry(tmp_path)
    verifier = FakeVerifier(verified_login_result(account_id="FLOW-999"))

    result = ConfirmLoginService(registry, verifier=verifier).confirm("FLOW-999")

    assert result.result == "account_not_found"
    assert result.ok is False
    assert registry.get("FLOW-999") is None
    assert verifier.calls == []


def test_confirm_login_false_does_not_update_status(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry)
    verifier = FakeVerifier(verified_login_result(login_verified=False, reason="google_login_missing"))

    result = ConfirmLoginService(registry, verifier=verifier).confirm("FLOW-005")

    assert result.result == "login_not_verified"
    assert result.registry_updated is False
    assert registry.get("FLOW-005").status == "login_required"


def test_confirm_login_extension_not_ready_does_not_update(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry)
    verifier = FakeVerifier(verified_login_result(extension_connected=False, extension_ready=False, login_verified=False, reason="extension_not_connected"))

    result = ConfirmLoginService(registry, verifier=verifier).confirm("FLOW-005")

    assert result.result == "login_not_verified"
    assert registry.get("FLOW-005").status == "login_required"


def test_confirm_login_account_mismatch_does_not_update(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry)
    verifier = FakeVerifier(verified_login_result(account_match=False, extension_ready=False, login_verified=False, reason="account_mismatch"))

    result = ConfirmLoginService(registry, verifier=verifier).confirm("FLOW-005")

    assert result.result == "login_not_verified"
    assert registry.get("FLOW-005").status == "login_required"


def test_confirm_login_flow_not_accessible_does_not_update(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry)
    verifier = FakeVerifier(verified_login_result(flow_accessible=False, google_logged_in=False, login_verified=False, reason="flow_not_accessible"))

    result = ConfirmLoginService(registry, verifier=verifier).confirm("FLOW-005")

    assert result.result == "login_not_verified"
    assert registry.get("FLOW-005").status == "login_required"


def test_confirm_login_google_not_logged_in_does_not_update(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry)
    verifier = FakeVerifier(verified_login_result(google_logged_in=False, login_verified=False, reason="google_login_missing"))

    result = ConfirmLoginService(registry, verifier=verifier).confirm("FLOW-005")

    assert result.result == "login_not_verified"
    assert registry.get("FLOW-005").status == "login_required"


def test_confirm_login_redirect_detected_does_not_update(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry)
    verifier = FakeVerifier(verified_login_result(login_redirect_detected=True, login_verified=False, reason="login_redirect_detected"))

    result = ConfirmLoginService(registry, verifier=verifier).confirm("FLOW-005")

    assert result.result == "login_not_verified"
    assert registry.get("FLOW-005").status == "login_required"


def test_confirm_login_success_updates_login_required_to_verified_and_clears_error(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.update_status("FLOW-005", "login_required", last_error="previous_error")
    verifier = FakeVerifier(verified_login_result())

    result = ConfirmLoginService(registry, verifier=verifier).confirm("FLOW-005")

    current = registry.get("FLOW-005")
    assert result.result == "confirmed"
    assert result.ok is True
    assert result.previous_registration_status == "login_required"
    assert result.registration_status == "login_verified"
    assert result.registry_updated is True
    assert current.status == "login_verified"
    assert current.last_error is None


def test_confirm_login_already_verified_is_idempotent(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.update_status("FLOW-005", "login_verified", last_error="kept_for_idempotency")
    before = _registry_row(registry, "FLOW-005")
    verifier = FakeVerifier(verified_login_result())

    result = ConfirmLoginService(registry, verifier=verifier).confirm("FLOW-005")

    assert result.result == "already_confirmed"
    assert result.ok is True
    assert result.registry_updated is False
    assert _registry_row(registry, "FLOW-005") == before


def test_confirm_login_already_verified_failure_does_not_downgrade(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry)
    registry.update_status("FLOW-005", "login_verified", last_error=None)
    verifier = FakeVerifier(verified_login_result(login_verified=False, flow_accessible=False, google_logged_in=False, reason="flow_not_accessible"))

    result = ConfirmLoginService(registry, verifier=verifier).confirm("FLOW-005")

    assert result.result == "login_verification_failed"
    assert result.ok is False
    assert result.registry_updated is False
    assert registry.get("FLOW-005").status == "login_verified"


def test_confirm_login_does_not_start_or_stop_processes_or_modify_profile(tmp_path):
    registry = make_registry(tmp_path)
    account = add_account(registry)
    profile = Path(account.profile_path)
    before_files = sorted(path.relative_to(profile) for path in profile.rglob("*"))
    verifier = FakeVerifier(verified_login_result())

    result = ConfirmLoginService(registry, verifier=verifier).confirm("FLOW-005")

    assert result.ok is True
    assert verifier.calls == ["FLOW-005"]
    assert sorted(path.relative_to(profile) for path in profile.rglob("*")) == before_files


def test_confirm_login_output_does_not_include_sensitive_values(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry)
    verifier = FakeVerifier(verified_login_result(reason="verified"))

    output = json.dumps(ConfirmLoginService(registry, verifier=verifier).confirm("FLOW-005").to_dict())

    assert "Cookie" not in output
    assert "token" not in output.lower()
    assert "user@example.com" not in output
    assert "accounts.google.com/signin" not in output


def _registry_row(registry, account_id):
    with sqlite3.connect(registry.db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM flow_account_registry WHERE account_id=?", (account_id,)).fetchone()
    return dict(row)
