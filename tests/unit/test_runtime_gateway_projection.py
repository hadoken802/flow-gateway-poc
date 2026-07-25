import json

from runtime.gateway_projection import GatewayProjection, ReadOnlyRuntimeStatusProvider
from runtime.process_manager import CommandLineProbeResult, ProcessProbeResult
from runtime.registry import AccountRecord, AccountRegistry


def make_registry(tmp_path):
    return AccountRegistry(
        db_path=tmp_path / "registry.db",
        profiles_root=tmp_path / "profiles",
        data_root=tmp_path / "data",
        outputs_root=tmp_path / "outputs",
        workers_json_path=tmp_path / "workers.json",
    )


def add_account(registry, account_id="FLOW-024", status="login_verified", enabled=True, runtime_instance_id="runtime-1"):
    account = AccountRecord(
        account_id=account_id,
        display_name=account_id,
        profile_path=str(registry.profiles_root / account_id),
        worker_api_port=8100 + int(account_id.rsplit("-", 1)[-1]),
        extension_ws_port=9200 + int(account_id.rsplit("-", 1)[-1]),
        chrome_cdp_port=9300 + int(account_id.rsplit("-", 1)[-1]),
        database_path=str(registry.data_root / f"{account_id}.db"),
        output_dir=str(registry.outputs_root / account_id),
        enabled=enabled,
        status=status,
        created_at="2026-01-01T00:00:00Z",
        runtime_instance_id=runtime_instance_id,
    )
    registry.register_many([account], create_dirs=False)
    return registry.get(account_id)


def ready_details(**overrides):
    details = {
        "runtime_status": "running",
        "runtime_healthy": True,
        "chrome_process_alive": True,
        "worker_process_alive": True,
        "worker_health_reachable": True,
        "extension_connected": True,
        "account_match": True,
        "chrome_ownership_verified": True,
        "worker_ownership_verified": True,
        "worker_ownership_verified_at": "2026-01-01T00:00:00Z",
        "runtime_instance_id": "runtime-1",
        "stop_safe": True,
    }
    details.update(overrides)
    return details


class FakeRuntime:
    def __init__(self, statuses):
        self.statuses = statuses
        self.calls = []
        self.submit_calls = 0
        self.queue_writes = 0

    def status(self, account):
        account_id = account.account_id
        self.calls.append(account_id)
        return dict(self.statuses[account_id])


def candidates_for(tmp_path, statuses, accounts):
    registry = make_registry(tmp_path)
    for account in accounts:
        add_account(registry, **account)
    runtime = FakeRuntime(statuses)
    projection = GatewayProjection(registry, runtime)
    return projection, registry, runtime


def test_login_required_account_cannot_enter_candidate_pool(tmp_path):
    projection, _, runtime = candidates_for(tmp_path, {"FLOW-024": ready_details()}, [{"status": "login_required"}])
    candidate = projection.candidates()[0]
    assert candidate.eligible is False
    assert candidate.gateway_status == "login_required"
    assert "registration_not_verified" in candidate.exclusion_reasons
    assert "runtime_not_running" in candidate.exclusion_reasons
    assert runtime.calls == []


def test_stopped_unhealthy_extension_mismatch_ownership_and_disabled_are_excluded(tmp_path):
    accounts = [
        {"account_id": "FLOW-001", "status": "login_verified"},
        {"account_id": "FLOW-002", "status": "login_verified"},
        {"account_id": "FLOW-003", "status": "login_verified"},
        {"account_id": "FLOW-004", "status": "login_verified"},
        {"account_id": "FLOW-005", "status": "login_verified"},
        {"account_id": "FLOW-006", "status": "login_verified", "enabled": False},
    ]
    statuses = {
        "FLOW-001": ready_details(runtime_status="stopped", runtime_healthy=False, chrome_process_alive=False, worker_process_alive=False, worker_health_reachable=False),
        "FLOW-002": ready_details(runtime_healthy=False),
        "FLOW-003": ready_details(extension_connected=False, account_match=False),
        "FLOW-004": ready_details(account_match=False),
        "FLOW-005": ready_details(chrome_ownership_verified=False),
        "FLOW-006": ready_details(),
    }
    projection, _, _ = candidates_for(tmp_path, statuses, accounts)
    by_id = {candidate.account_id: candidate for candidate in projection.candidates()}
    assert "runtime_not_running" in by_id["FLOW-001"].exclusion_reasons
    assert "runtime_unhealthy" in by_id["FLOW-002"].exclusion_reasons
    assert "extension_not_ready" in by_id["FLOW-003"].exclusion_reasons
    assert "account_mismatch" in by_id["FLOW-004"].exclusion_reasons
    assert "ownership_not_verified" in by_id["FLOW-005"].exclusion_reasons
    assert by_id["FLOW-006"].exclusion_reasons[0] == "disabled"
    assert "FLOW-006" not in projection.status_provider.calls


def test_all_conditions_met_projects_ready(tmp_path):
    projection, _, _ = candidates_for(tmp_path, {"FLOW-024": ready_details()}, [{"account_id": "FLOW-024"}])
    candidate = projection.candidates()[0]
    assert candidate.gateway_status == "ready"
    assert candidate.eligible is True
    assert candidate.exclusion_reasons == []
    assert candidate.worker_api_endpoint == "http://127.0.0.1:8124"
    assert candidate.worker_ws_endpoint == "ws://127.0.0.1:9224"


def test_exclusion_reasons_are_complete_and_stable(tmp_path):
    projection, _, _ = candidates_for(
        tmp_path,
        {"FLOW-024": ready_details(runtime_status="stopped", runtime_healthy=False, chrome_process_alive=False, worker_process_alive=False, worker_health_reachable=False, extension_connected=False, account_match=False, chrome_ownership_verified=False, worker_ownership_verified=False, runtime_instance_id="", stop_safe=False)},
        [{"account_id": "FLOW-024", "status": "login_required", "enabled": False}],
    )
    assert projection.candidates()[0].exclusion_reasons == [
        "disabled",
        "registration_not_verified",
        "runtime_not_running",
        "runtime_unhealthy",
        "chrome_not_running",
        "worker_not_running",
        "worker_unreachable",
        "extension_not_ready",
        "account_mismatch",
        "ownership_not_verified",
        "runtime_instance_missing",
        "stop_not_safe",
    ]


def test_repeated_reads_do_not_create_duplicate_workers_or_modify_registry(tmp_path):
    projection, registry, _ = candidates_for(tmp_path, {"FLOW-024": ready_details()}, [{"account_id": "FLOW-024"}])
    before = [account.account_id for account in registry.list_accounts()]
    assert [candidate.account_id for candidate in projection.candidates()] == ["FLOW-024"]
    assert [candidate.account_id for candidate in projection.candidates()] == ["FLOW-024"]
    assert [account.account_id for account in registry.list_accounts()] == before


def test_runtime_registry_is_the_only_identity_source(tmp_path):
    projection, _, runtime = candidates_for(tmp_path, {"FLOW-024": ready_details()}, [{"account_id": "FLOW-024"}])
    assert [candidate.account_id for candidate in projection.candidates()] == ["FLOW-024"]
    assert runtime.calls == ["FLOW-024"]


def test_disabled_account_does_not_call_runtime_status_provider(tmp_path):
    projection, _, runtime = candidates_for(tmp_path, {"FLOW-024": ready_details()}, [{"account_id": "FLOW-024", "enabled": False}])
    candidate = projection.candidates()[0]
    assert candidate.eligible is False
    assert candidate.gateway_status == "disabled"
    assert runtime.calls == []


def test_twenty_two_accounts_probe_only_login_verified_enabled_account(tmp_path):
    registry = make_registry(tmp_path)
    statuses = {"FLOW-024": ready_details()}
    for idx in list(range(1, 20)) + [22, 23, 24]:
        account_id = f"FLOW-{idx:03d}"
        add_account(
            registry,
            account_id=account_id,
            status="login_verified" if account_id == "FLOW-024" else "login_required",
            enabled=True,
        )
    runtime = FakeRuntime(statuses)
    candidates = GatewayProjection(registry, runtime).candidates()
    by_id = {candidate.account_id: candidate for candidate in candidates}
    assert len(candidates) == 22
    assert runtime.calls == ["FLOW-024"]
    assert by_id["FLOW-024"].gateway_status == "ready"
    assert by_id["FLOW-024"].eligible is True
    assert by_id["FLOW-022"].eligible is False
    assert by_id["FLOW-023"].eligible is False


def test_runtime_instance_id_is_required_to_prevent_stale_worker_selection(tmp_path):
    projection, _, _ = candidates_for(tmp_path, {"FLOW-024": ready_details(runtime_instance_id=None)}, [{"account_id": "FLOW-024"}])
    candidate = projection.candidates()[0]
    assert candidate.eligible is False
    assert "runtime_instance_missing" in candidate.exclusion_reasons


def test_safe_output_contains_no_sensitive_material(tmp_path):
    projection, _, _ = candidates_for(tmp_path, {"FLOW-024": ready_details()}, [{"account_id": "FLOW-024"}])
    payload = json.dumps({
        "candidates": [candidate.to_dict() for candidate in projection.candidates()],
    })
    lowered = payload.lower()
    assert "cookie" not in lowered
    assert "token" not in lowered
    assert "secret" not in lowered
    assert "nonce" not in lowered
    assert "@" not in payload
    assert "profile_path" not in lowered


class FakeInspector:
    def probe_process(self, pid):
        return ProcessProbeResult(pid, True, "alive", "fake")

    def listening_pid(self, port):
        return {9324: 17972, 8124: 7608, 9224: 7608}.get(port)

    def command_line(self, pid):
        raise AssertionError("matching CDP listener PID should not require command line access")


def test_chrome_ownership_allows_matching_cdp_listener_pid_without_command_line(tmp_path, monkeypatch):
    registry = make_registry(tmp_path)
    account = add_account(registry, account_id="FLOW-024")
    account = AccountRecord(**{**account.__dict__, "chrome_pid": 17972, "worker_pid": 7608, "runtime_instance_id": "runtime-1", "runtime_ownership_version": 1, "worker_ownership_verified_at": "2026-01-01T00:00:00Z"})

    provider = ReadOnlyRuntimeStatusProvider(FakeInspector())
    monkeypatch.setattr(provider, "_worker_health", lambda _port: {
        "account_id": "FLOW-024",
        "extension_connected": True,
        "runtime_instance_id": "runtime-1",
        "runtime_ownership_version": 1,
    })

    details = provider.status(account)
    assert details["chrome_ownership_verified"] is True
    assert details["worker_ownership_verified"] is True
    assert details["runtime_status"] == "running"
