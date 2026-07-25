import json
import sqlite3
from pathlib import Path

from runtime.login_verifier import LoginVerifier
from runtime.process_manager import CommandLineProbeResult, ProcessProbeResult
from runtime.registry import AccountRecord, AccountRegistry


class FakeInspector:
    def __init__(self):
        self.alive = set()
        self.commands = {}

    def probe_process(self, pid):
        return ProcessProbeResult(pid=pid, alive=pid in self.alive, status="alive" if pid in self.alive else "not_found", method="fake")

    def command_line_probe(self, pid):
        command = self.commands.get(pid, "")
        return CommandLineProbeResult(pid=pid, command_line=command, status="available" if command else "cim_empty")


class FakeCdp:
    def __init__(self, targets=None, target_sequence=None, open_ok=True):
        self.targets = targets or []
        self.target_sequence = list(target_sequence or [])
        self.opened = []
        self.open_ok = open_ok

    def list_targets(self, cdp_port):
        if self.target_sequence:
            return self.target_sequence.pop(0)
        return self.targets

    def open_url(self, cdp_port, url):
        self.opened.append((cdp_port, url))
        return {"id": "target-1"} if self.open_ok else None


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
    return [{"type": "page", "url": "https://labs.google/fx/tools/flow", "title": title}]


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
    verifier, _, _ = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, flow_target())

    result = verifier.verify("FLOW-005")

    assert result.flow_accessible is True
    assert result.login_redirect_detected is False
    assert result.google_logged_in is True
    assert result.login_verified is True
    assert result.reason == "verified"


def test_accounts_google_redirect_returns_false(tmp_path):
    targets = [{"type": "page", "url": "https://accounts.google.com/signin/v2/identifier", "title": "Sign in"}]
    verifier, _, _ = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, targets)

    result = verifier.verify("FLOW-005")

    assert result.login_redirect_detected is True
    assert result.login_verified is False


def test_login_button_or_account_chooser_returns_false(tmp_path):
    targets = [{"type": "page", "url": "https://labs.google/fx/tools/flow", "title": "Choose an account"}]
    verifier, _, _ = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, targets)

    result = verifier.verify("FLOW-005")

    assert result.login_redirect_detected is True
    assert result.login_verified is False


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
    verifier, registry, _ = make_verifier(tmp_path, {"account_id": "FLOW-005", "extension_connected": True}, flow_target())
    account = registry.get("FLOW-005")
    profile = Path(account.profile_path)
    before_files = sorted(path.relative_to(profile) for path in profile.rglob("*"))
    before_row = _registry_row(registry, "FLOW-005")

    result = verifier.verify("FLOW-005")

    assert result.login_verified is True
    assert sorted(path.relative_to(profile) for path in profile.rglob("*")) == before_files
    assert _registry_row(registry, "FLOW-005") == before_row


def _registry_row(registry, account_id):
    with sqlite3.connect(registry.db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM flow_account_registry WHERE account_id=?", (account_id,)).fetchone()
    return dict(row)
