import json
import socket
from pathlib import Path

from runtime.port_allocator import PortRanges, allocate_port_triplet
from runtime.registry import AccountRegistry, format_account_id


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

    plan = registry.register_batch(5, 2, dry_run=False)

    assert not plan.issues
    accounts = registry.list_accounts()
    assert [account.account_id for account in accounts] == ["FLOW-005", "FLOW-006"]
    assert not Path(accounts[0].profile_path).exists()


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
