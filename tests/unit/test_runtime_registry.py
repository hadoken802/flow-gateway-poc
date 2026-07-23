import json
import socket
import sqlite3
from pathlib import Path

import pytest

from runtime import cli
from runtime.port_allocator import PortRanges, allocate_port_triplet
from runtime.registry import AccountRecord, AccountRegistry, format_account_id


def make_registry(tmp_path, workers=None):
    workers_path = tmp_path / "workers.json"
    workers_path.write_text(json.dumps(workers or [], ensure_ascii=False), encoding="utf-8")
    return AccountRegistry(
        db_path=tmp_path / "runtime_registry.db",
        profiles_root=tmp_path / "profiles",
        data_root=tmp_path / "data",
        outputs_root=tmp_path / "outputs",
        workers_json_path=workers_path,
    )


def test_format_account_id():
    assert format_account_id(5) == "FLOW-005"
    assert format_account_id(12) == "FLOW-012"


def test_import_existing_workers_reports_unregistered_profile(tmp_path):
    registry = make_registry(
        tmp_path,
        workers=[
            {"account_id": "FLOW-001", "api_url": "http://127.0.0.1:8100", "enabled": True},
            {"account_id": "FLOW-002", "api_url": "http://127.0.0.1:8112", "enabled": True},
            {"account_id": "FLOW-003", "api_url": "http://127.0.0.1:8113", "enabled": True},
        ],
    )
    (tmp_path / "profiles" / "FLOW-004").mkdir(parents=True)

    plan = registry.import_existing_workers(dry_run=True)

    assert [account.account_id for account in plan.accounts] == ["FLOW-001", "FLOW-002", "FLOW-003"]
    assert {account.worker_api_port for account in plan.accounts} == {8100, 8112, 8113}
    assert {account.extension_ws_port for account in plan.accounts} == {9222, 9212, 9213}
    assert [issue.reason for issue in plan.issues] == ["profile_exists_unregistered"]
    assert plan.issues[0].account_id == "FLOW-004"


def test_register_batch_does_not_overwrite_existing_profile(tmp_path):
    registry = make_registry(tmp_path)
    (tmp_path / "profiles" / "FLOW-004").mkdir(parents=True)

    plan = registry.register_batch(4, 2, dry_run=True)

    assert [issue.account_id for issue in plan.issues] == ["FLOW-004"]
    assert plan.issues[0].reason == "profile_exists_unregistered"
    assert [account.account_id for account in plan.accounts] == ["FLOW-005"]


def test_register_batch_is_atomic_when_issue_exists(tmp_path):
    registry = make_registry(tmp_path)
    (tmp_path / "profiles" / "FLOW-004").mkdir(parents=True)

    plan = registry.register_batch(4, 2, dry_run=False)

    assert plan.issues
    assert registry.list_accounts() == []


def test_register_batch_persists_without_creating_dirs_by_default(tmp_path):
    registry = make_registry(tmp_path)

    plan = registry.register_batch(5, 2, dry_run=False, create_dirs=False)

    assert not plan.issues
    accounts = registry.list_accounts()
    assert [account.account_id for account in accounts] == ["FLOW-005", "FLOW-006"]
    assert not Path(accounts[0].profile_path).exists()


def test_register_batch_creates_profiles_and_login_required(tmp_path):
    registry = make_registry(tmp_path)

    plan = registry.register_batch(5, 4, dry_run=False)

    assert not plan.issues
    assert [account.account_id for account in plan.accounts] == ["FLOW-005", "FLOW-006", "FLOW-007", "FLOW-008"]
    assert {account.status for account in registry.list_accounts()} == {"login_required"}
    for account in registry.list_accounts():
        assert Path(account.profile_path).is_dir()


def test_register_batch_dry_run_writes_nothing_and_creates_no_dirs(tmp_path):
    registry = make_registry(tmp_path)

    plan = registry.register_batch(5, 4, dry_run=True)

    assert [account.account_id for account in plan.accounts] == ["FLOW-005", "FLOW-006", "FLOW-007", "FLOW-008"]
    assert registry.list_accounts() == []
    assert not (tmp_path / "profiles").exists()


def test_repeated_import_existing_is_idempotent(tmp_path):
    registry = make_registry(tmp_path, workers=[{"account_id": "FLOW-001", "api_url": "http://127.0.0.1:8100"}])

    first = registry.import_existing_workers(dry_run=False)
    second = registry.import_existing_workers(dry_run=False)

    assert [account.account_id for account in first.accounts] == ["FLOW-001"]
    assert [issue.reason for issue in second.issues] == ["already_registered"]
    assert [account.account_id for account in registry.list_accounts()] == ["FLOW-001"]


def test_repeated_add_batch_is_idempotent_and_reports_registered(tmp_path):
    registry = make_registry(tmp_path)

    registry.register_batch(5, 2, dry_run=False)
    second = registry.register_batch(5, 2, dry_run=False)

    assert second.accounts == []
    assert [issue.reason for issue in second.issues] == ["already_registered", "already_registered"]
    assert [account.account_id for account in registry.list_accounts()] == ["FLOW-005", "FLOW-006"]


def test_database_unique_constraints(tmp_path):
    registry = make_registry(tmp_path)
    account = registry.plan_batch(5, 1).accounts[0]
    registry.register_many([account], create_dirs=False)

    duplicate_id = AccountRecord(**{**account.__dict__, "profile_path": str(tmp_path / "profiles" / "OTHER")})
    with pytest.raises(sqlite3.IntegrityError):
        registry.register_many([duplicate_id], create_dirs=False)

    duplicate_profile = AccountRecord(**{**account.__dict__, "account_id": "FLOW-006", "worker_api_port": 18101, "extension_ws_port": 18201, "chrome_cdp_port": 18301})
    with pytest.raises(sqlite3.IntegrityError):
        registry.register_many([duplicate_profile], create_dirs=False)


@pytest.mark.parametrize("field", ["worker_api_port", "extension_ws_port", "chrome_cdp_port"])
def test_port_unique_constraints(tmp_path, field):
    registry = make_registry(tmp_path)
    account = registry.plan_batch(5, 1).accounts[0]
    registry.register_many([account], create_dirs=False)
    values = {
        **account.__dict__,
        "account_id": "FLOW-006",
        "profile_path": str(tmp_path / "profiles" / "FLOW-006"),
        "database_path": str(tmp_path / "data" / "FLOW-006.db"),
        "output_dir": str(tmp_path / "outputs" / "FLOW-006"),
        "worker_api_port": account.worker_api_port + 101,
        "extension_ws_port": account.extension_ws_port + 101,
        "chrome_cdp_port": account.chrome_cdp_port + 101,
    }
    values[field] = getattr(account, field)

    with pytest.raises(sqlite3.IntegrityError):
        registry.register_many([AccountRecord(**values)], create_dirs=False)


def test_database_failure_rolls_back_batch(tmp_path):
    registry = make_registry(tmp_path)
    accounts = registry.plan_batch(5, 2).accounts
    duplicate = AccountRecord(**{**accounts[1].__dict__, "profile_path": accounts[0].profile_path})

    with pytest.raises(sqlite3.IntegrityError):
        registry.register_many([accounts[0], duplicate], create_dirs=False)

    assert registry.list_accounts() == []


def test_profile_creation_failure_cleans_only_new_empty_dirs(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    preexisting = tmp_path / "profiles" / "FLOW-004"
    preexisting.mkdir(parents=True)
    accounts = registry.plan_batch(5, 2).accounts
    original_mkdir = Path.mkdir

    def fail_second_profile(self, *args, **kwargs):
        if self.name == "FLOW-006":
            raise OSError("simulated mkdir failure")
        return original_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", fail_second_profile)

    with pytest.raises(OSError):
        registry.register_many(accounts, create_dirs=True)

    assert registry.list_accounts() == []
    assert preexisting.exists()
    assert not (tmp_path / "profiles" / "FLOW-005").exists()


def test_schema_migration_is_idempotent(tmp_path):
    registry = make_registry(tmp_path)

    registry.list_accounts()
    registry.list_accounts()

    with registry.connect() as conn:
        version = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0]
    assert version == "1"


def test_schema_migration_adds_unique_indexes_to_existing_database(tmp_path):
    registry = make_registry(tmp_path)
    registry.db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(registry.db_path) as conn:
        conn.execute(
            """
            CREATE TABLE flow_account_registry (
                account_id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                profile_path TEXT NOT NULL,
                worker_api_port INTEGER NOT NULL,
                extension_ws_port INTEGER NOT NULL,
                chrome_cdp_port INTEGER NOT NULL,
                database_path TEXT NOT NULL,
                output_dir TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'registered',
                created_at TEXT NOT NULL,
                last_started_at TEXT,
                last_stopped_at TEXT,
                chrome_pid INTEGER,
                worker_pid INTEGER,
                last_health_at TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )

    account = registry.plan_batch(5, 1).accounts[0]
    registry.register_many([account], create_dirs=False)
    duplicate = AccountRecord(**{**account.__dict__, "account_id": "FLOW-006"})

    with pytest.raises(sqlite3.IntegrityError):
        registry.register_many([duplicate], create_dirs=False)


def test_cli_show_and_failure_exit_code(tmp_path, monkeypatch, capsys):
    registry = make_registry(tmp_path)
    registry.register_batch(5, 1, dry_run=False)
    monkeypatch.setattr(cli, "AccountRegistry", lambda: registry)

    assert cli.main(["show", "FLOW-005"]) == 0
    assert "FLOW-005" in capsys.readouterr().out
    assert cli.main(["show", "FLOW-999"]) == 1


def test_cli_add_batch_creates_profiles_and_returns_nonzero_on_repeat(tmp_path, monkeypatch, capsys):
    registry = make_registry(tmp_path)
    monkeypatch.setattr(cli, "AccountRegistry", lambda: registry)

    assert cli.main(["add-batch", "--start", "5", "--count", "1"]) == 0
    created = json.loads(capsys.readouterr().out)
    assert created["accounts"][0]["result"] == "created"
    assert Path(created["accounts"][0]["profile_path"]).is_dir()

    assert cli.main(["add-batch", "--start", "5", "--count", "1"]) == 1
    repeated = json.loads(capsys.readouterr().out)
    assert repeated["issues"][0]["result"] == "already_registered"


def test_port_allocator_skips_reserved_and_listening_ports():
    listening_port = None
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        for candidate in range(18100, 18120):
            try:
                sock.bind(("127.0.0.1", candidate))
                listening_port = candidate
                break
            except OSError:
                continue
        assert listening_port is not None
        sock.listen(1)
        ranges = PortRanges(
            worker_api=range(18100, 18120),
            extension_ws=range(18200, 18220),
            chrome_cdp=range(18300, 18320),
        )

        worker_api, extension_ws, chrome_cdp = allocate_port_triplet({18200}, ranges=ranges)

    assert worker_api != listening_port
    assert extension_ws != 18200
    assert chrome_cdp in ranges.chrome_cdp
