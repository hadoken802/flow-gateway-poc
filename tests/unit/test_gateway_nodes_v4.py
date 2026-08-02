import pytest

from gateway.worker_provider import WorkerConfig
from runtime.process_manager import RuntimeResult
from runtime.registry import AccountRegistry


class FakeScheduler:
    def __init__(self, db):
        self.db = db
        self.refresh_calls = 0

    async def async_refresh_worker_snapshot(self):
        self.refresh_calls += 1

    async def refresh_workers(self):
        self.refresh_calls += 1


class FakeManager:
    def __init__(self, registry):
        self.registry = registry
        self.started = []
        self.stopped = []
        self.opened = []
        self.inspector = type("Inspector", (), {"listening_pid": lambda self, port: None})()
        self.status_payload = {
            "runtime_status": "running",
            "worker_health_reachable": True,
            "chrome_cdp_reachable": True,
            "extension_bootstrap_status": "extension_ready",
            "extension_connected": True,
            "account_match": True,
            "worker_service_reachable": True,
            "flow_key_present": True,
        }

    def status(self, account_id):
        account = self.registry.get(account_id)
        return RuntimeResult(
            "running",
            account_id,
            True,
            details={
                **self.status_payload,
                "runtime_status": "running",
                "worker_health_reachable": True,
                "chrome_cdp_reachable": True,
                "extension_bootstrap_status": "extension_ready",
                "extension_connected": True,
                "worker_pid": account.worker_pid,
                "chrome_pid": account.chrome_pid,
            },
        )

    def start_worker_only(self, account_id):
        self.started.append(account_id)
        self.registry.mark_worker_runtime_identity(account_id, "runtime-test", "secret-ref", "fingerprint", 1)
        return RuntimeResult("already_running", account_id, True, details={"worker_pid": 1234})

    def open_login(self, account_id):
        self.opened.append(account_id)
        account = self.registry.get(account_id)
        if account:
            self.registry.mark_started(account_id, chrome_pid=5678)
        return RuntimeResult("opened", account_id, True, details={"chrome_pid": 5678})

    def stop_worker_only(self, account_id):
        self.stopped.append(account_id)
        return RuntimeResult("stopped", account_id, True, details={"chrome_preserved": True})


@pytest.fixture
def registry(tmp_path):
    return AccountRegistry(
        db_path=tmp_path / "runtime_registry.db",
        profiles_root=tmp_path / "profiles",
        data_root=tmp_path / "data",
        outputs_root=tmp_path / "outputs",
        workers_json_path=tmp_path / "workers.json",
    )


@pytest.fixture
async def gateway_db(tmp_path):
    from gateway.db import connect

    db = await connect(tmp_path / "gateway.db")
    yield db
    await db.close()


async def _upsert_gateway_account(db, account_id, port, status="ready", credits=1000, confidence="live", enabled=True):
    from gateway import crud

    await crud.upsert_account(
        db,
        WorkerConfig(account_id, f"http://127.0.0.1:{port}", enabled, "runtime-test"),
        status=status,
        credits=credits,
        quota_confidence=confidence,
    )


@pytest.mark.asyncio
async def test_add_single_node_and_duplicate_is_idempotent(gateway_db, registry):
    from gateway import nodes

    scheduler = FakeScheduler(gateway_db)
    first = await nodes.create_node(scheduler, {"account_id": "FLOW-004", "worker_port": 8104, "cdp_port": 9303}, registry)
    second = await nodes.create_node(scheduler, {"account_id": "FLOW-004", "worker_port": 8104, "cdp_port": 9303}, registry)

    assert first["result"] == "created"
    assert second["result"] == "duplicate"
    assert registry.get("FLOW-004").extension_ws_port == 9203


@pytest.mark.asyncio
async def test_duplicate_worker_and_cdp_ports_are_rejected(gateway_db, registry):
    from gateway import nodes

    scheduler = FakeScheduler(gateway_db)
    await nodes.create_node(scheduler, {"account_id": "FLOW-004", "worker_port": 8104, "cdp_port": 9303}, registry)

    with pytest.raises(ValueError, match="port_conflict"):
        await nodes.create_node(scheduler, {"account_id": "FLOW-005", "worker_port": 8104, "cdp_port": 9304}, registry)
    with pytest.raises(ValueError, match="port_conflict"):
        await nodes.create_node(scheduler, {"account_id": "FLOW-006", "worker_port": 8106, "cdp_port": 9303}, registry)


@pytest.mark.asyncio
async def test_csv_and_json_node_import_keep_row_errors_isolated(gateway_db, registry):
    from gateway import nodes

    scheduler = FakeScheduler(gateway_db)
    csv_result = await nodes.import_nodes(
        scheduler,
        {"format": "csv", "content": "account_id,worker_port,cdp_port\nFLOW-004,8104,9303\nFLOW-005,8104,9304\n"},
        registry,
    )
    json_result = await nodes.import_nodes(scheduler, {"nodes": [{"account_id": "FLOW-006", "worker_port": 8106, "cdp_port": 9305}]}, registry)

    assert csv_result["success"] == 1
    assert csv_result["failed"] == 1
    assert "port_conflict" in csv_result["errors"][0]["error"]
    assert json_result["success"] == 1


@pytest.mark.asyncio
async def test_runtime_actions_use_worker_only_and_preserve_chrome(gateway_db, registry):
    from gateway import nodes

    scheduler = FakeScheduler(gateway_db)
    await nodes.create_node(scheduler, {"account_id": "FLOW-004", "worker_port": 8104, "cdp_port": 9303}, registry)
    manager = FakeManager(registry)

    started = await nodes.runtime_action(scheduler, "FLOW-004", "start", registry, manager)
    stopped = await nodes.runtime_action(scheduler, "FLOW-004", "stop", registry, manager)

    assert started["result"] == "already_running"
    assert stopped["details"]["chrome_preserved"] is True
    assert manager.started == ["FLOW-004"]
    assert manager.stopped == ["FLOW-004"]


@pytest.mark.asyncio
async def test_node_list_merges_runtime_registry_and_gateway_account(gateway_db, registry):
    from gateway import nodes

    scheduler = FakeScheduler(gateway_db)
    await nodes.create_node(scheduler, {"account_id": "FLOW-004", "worker_port": 8104, "cdp_port": 9303, "enabled": True}, registry)
    await _upsert_gateway_account(gateway_db, "FLOW-004", 8104, credits=1000, confidence="live")

    listed = await nodes.list_nodes(scheduler, registry, FakeManager(registry))

    assert listed[0]["account_id"] == "FLOW-004"
    assert listed[0]["worker_port"] == 8104
    assert listed[0]["cdp_port"] == 9303
    assert listed[0]["credits"] == 1000
    assert listed[0]["quota_confidence"] == "live"


@pytest.mark.asyncio
async def test_delete_node_removes_orphan_gateway_account(gateway_db, registry):
    from gateway import crud, nodes

    scheduler = FakeScheduler(gateway_db)
    await nodes.create_node(scheduler, {"account_id": "FLOW-UI-TEST", "worker_port": 8199, "cdp_port": 9399}, registry)

    result = await nodes.delete_node(scheduler, "FLOW-UI-TEST", registry)

    assert result["gateway_account_cleanup"]["result"] == "deleted"
    assert await crud.get_account(gateway_db, "FLOW-UI-TEST") is None


@pytest.mark.asyncio
async def test_delete_orphan_account_refuses_history_references(gateway_db):
    from gateway import crud

    await _upsert_gateway_account(gateway_db, "FLOW-UI-TEST", 8199, status="offline", credits=None, confidence=None, enabled=False)
    await crud.create_task(
        gateway_db,
        {
            "idempotency_key": "ui-test-history",
            "image_path": "D:/img.png",
            "prompt": "p",
            "preferred_account_id": "FLOW-UI-TEST",
        },
    )

    result = await crud.delete_orphan_account(gateway_db, "FLOW-UI-TEST", known_registry_account_ids=set())

    assert result["ok"] is False
    assert result["result"] == "has_history"
    assert await crud.get_account(gateway_db, "FLOW-UI-TEST") is not None


def test_dynamic_concurrency_counts_only_live_ready_accounts():
    from gateway.config import GatewaySettings
    from gateway.scheduler import GatewayScheduler
    from gateway.worker_provider import WorkerSnapshot

    scheduler = GatewayScheduler(GatewaySettings(dry_run=False, max_concurrency=10))
    accounts = [
        {"account_id": "FLOW-001", "status": "ready", "credits": 1000, "quota_confidence": "live"},
        {"account_id": "FLOW-002", "status": "ready", "credits": 1000, "quota_confidence": "stale"},
        {"account_id": "FLOW-003", "status": "offline", "credits": 1000, "quota_confidence": "live"},
        {"account_id": "FLOW-004", "status": "ready", "credits": 1000, "quota_confidence": "live", "manual_paused": 1},
        {"account_id": "FLOW-005", "status": "ready", "credits": 1000, "quota_confidence": "live"},
    ]
    scheduler.worker_snapshot = WorkerSnapshot(
        [WorkerConfig(item["account_id"], "http://worker", True) for item in accounts],
        [],
        "fake",
        "Fake",
        "now",
    )

    assert scheduler.effective_max_concurrency_from_accounts(accounts) == 2


def test_dynamic_concurrency_respects_configured_upper_bound():
    from gateway.config import GatewaySettings
    from gateway.scheduler import GatewayScheduler
    from gateway.worker_provider import WorkerSnapshot

    scheduler = GatewayScheduler(GatewaySettings(dry_run=False, max_concurrency=5))
    accounts = [{"account_id": f"FLOW-{i:03d}", "status": "ready", "credits": 1000, "quota_confidence": "live"} for i in range(1, 11)]
    scheduler.worker_snapshot = WorkerSnapshot(
        [WorkerConfig(item["account_id"], "http://worker", True) for item in accounts],
        [],
        "fake",
        "Fake",
        "now",
    )

    assert scheduler.effective_max_concurrency_from_accounts(accounts) == 5


def test_quick_add_formats_account_number_and_ports(monkeypatch, registry):
    from gateway import nodes

    monkeypatch.setattr("gateway.nodes.port_is_available", lambda port, reserved=(), host="127.0.0.1": int(port) == 8107)

    assert nodes.normalize_flow_account_id("7") == "FLOW-007"
    assert nodes.normalize_flow_account_id("007") == "FLOW-007"
    assert nodes.normalize_flow_account_id("FLOW-007") == "FLOW-007"

    preview = nodes.quick_add_preview({"flow_account_number": "7"}, registry, FakeManager(registry))

    assert preview["account_id"] == "FLOW-007"
    assert preview["worker_port"] == 8107
    assert preview["extension_ws_port"] == 9206
    assert preview["cdp_port"] == 9306
    assert preview["enabled"] is False
    assert preview["profile_path"].endswith("profiles\\FLOW-007") or preview["profile_path"].endswith("profiles/FLOW-007")


def test_quick_add_uses_fallback_when_preferred_worker_port_unavailable(monkeypatch, registry):
    from gateway import nodes

    monkeypatch.setattr("gateway.nodes.worker_fallback_range", lambda: range(18100, 18110))
    monkeypatch.setattr("gateway.nodes.port_is_available", lambda port, reserved=(), host="127.0.0.1": int(port) == 18100)

    preview = nodes.quick_add_preview({"flow_account_number": "7"}, registry, FakeManager(registry))

    assert preview["worker_port"] == 18100


def test_quick_add_uses_fallback_when_preferred_worker_port_is_excluded(monkeypatch, registry):
    from gateway import nodes
    from runtime import port_allocator

    port_allocator.windows_excluded_tcp_port_ranges.cache_clear()
    monkeypatch.setattr("runtime.port_allocator.windows_excluded_tcp_port_ranges", lambda: ((8107, 8107),))
    monkeypatch.setattr("runtime.port_allocator.port_is_listening", lambda port, host="127.0.0.1": False)
    monkeypatch.setattr("runtime.port_allocator.port_can_bind", lambda port, host="127.0.0.1": True)
    monkeypatch.setattr("gateway.nodes.worker_fallback_range", lambda: range(18100, 18110))
    monkeypatch.setattr("gateway.nodes.port_is_available", port_allocator.port_is_available)

    preview = nodes.quick_add_preview({"flow_account_number": "7"}, registry, FakeManager(registry))

    assert preview["worker_port"] == 18100


def test_quick_add_fallback_skips_existing_node_ports(monkeypatch, gateway_db, registry):
    from gateway import nodes

    registry.upsert_account("FLOW-008", worker_api_port=8108, extension_ws_port=9207, chrome_cdp_port=9307, status="login_verified")
    monkeypatch.setattr("gateway.nodes.worker_fallback_range", lambda: range(18100, 18103))

    def fake_available(port, reserved=(), host="127.0.0.1"):
        return int(port) not in {8107, 18100} and int(port) not in {int(item) for item in reserved}

    monkeypatch.setattr("gateway.nodes.port_is_available", fake_available)

    preview = nodes.quick_add_preview({"flow_account_number": "7"}, registry, FakeManager(registry))
    flow_008 = registry.get("FLOW-008")

    assert preview["worker_port"] == 18101
    assert flow_008 is not None
    assert flow_008.worker_api_port == 8108


@pytest.mark.asyncio
async def test_quick_create_login_saves_allocated_worker_port(monkeypatch, gateway_db, registry):
    from gateway import nodes

    scheduler = FakeScheduler(gateway_db)
    manager = FakeManager(registry)
    monkeypatch.setattr("gateway.nodes.worker_fallback_range", lambda: range(18100, 18110))
    monkeypatch.setattr("gateway.nodes.port_is_available", lambda port, reserved=(), host="127.0.0.1": int(port) == 18100)
    monkeypatch.setattr("gateway.nodes.port_is_listening", lambda port, host="127.0.0.1": False)
    monkeypatch.setattr("gateway.nodes.port_can_bind", lambda port, host="127.0.0.1": True)

    result = await nodes.quick_create_login(scheduler, {"flow_account_number": "7"}, registry, manager)
    account = registry.get("FLOW-007")

    assert result["ok"] is True
    assert account is not None
    assert account.worker_api_port == 18100


def test_quick_add_skips_flow_006_and_allows_flow_007(registry):
    from gateway import nodes

    preview = nodes.quick_add_preview({"flow_account_number": "FLOW-007"}, registry, FakeManager(registry))

    assert preview["account_id"] == "FLOW-007"
    assert registry.get("FLOW-006") is None


@pytest.mark.asyncio
async def test_quick_add_rejects_duplicate_account_and_ports(gateway_db, registry):
    from gateway import nodes

    scheduler = FakeScheduler(gateway_db)
    await nodes.create_node(scheduler, {"account_id": "FLOW-007", "worker_port": 8107, "extension_ws_port": 9206, "cdp_port": 9306}, registry)

    duplicate = nodes.quick_add_preview({"flow_account_number": "7"}, registry, FakeManager(registry))
    worker_conflict = nodes.quick_add_preview({"flow_account_number": "8", "worker_port": 8107}, registry, FakeManager(registry))
    ws_conflict = nodes.quick_add_preview({"flow_account_number": "8", "extension_ws_port": 9206}, registry, FakeManager(registry))
    cdp_conflict = nodes.quick_add_preview({"flow_account_number": "8", "cdp_port": 9306}, registry, FakeManager(registry))

    assert any(item["reason"] == "account_exists" for item in duplicate["checks"]["errors"])
    assert any(item["field"] == "worker_api_port" for item in worker_conflict["checks"]["errors"])
    assert any(item["field"] == "extension_ws_port" for item in ws_conflict["checks"]["errors"])
    assert any(item["field"] == "chrome_cdp_port" for item in cdp_conflict["checks"]["errors"])


@pytest.mark.asyncio
async def test_quick_create_login_creates_disabled_node_and_starts_runtime(gateway_db, registry):
    from gateway import nodes

    scheduler = FakeScheduler(gateway_db)
    manager = FakeManager(registry)

    result = await nodes.quick_create_login(
        scheduler,
        {"flow_account_number": "7", "worker_port": 18107, "extension_ws_port": 19206, "cdp_port": 19306},
        registry,
        manager,
    )

    account = registry.get("FLOW-007")
    assert result["result"] == "waiting_for_manual_login"
    assert account is not None
    assert account.enabled is False
    assert manager.opened == ["FLOW-007"]
    assert manager.started == ["FLOW-007"]


@pytest.mark.asyncio
async def test_check_login_enable_requires_live_quota(gateway_db, registry):
    from gateway import nodes

    scheduler = FakeScheduler(gateway_db)
    manager = FakeManager(registry)
    await nodes.create_node(scheduler, {"account_id": "FLOW-007", "worker_port": 18107, "extension_ws_port": 19206, "cdp_port": 19306}, registry)
    manager.status_payload["flow_key_present"] = False

    result = await nodes.check_login_and_enable(scheduler, "FLOW-007", registry, manager)

    assert result["ok"] is False
    assert result["result"] == "manual_login_required"
    assert registry.get("FLOW-007").enabled is False


@pytest.mark.asyncio
async def test_check_login_enable_turns_on_live_node(gateway_db, registry, monkeypatch):
    from gateway import nodes

    scheduler = FakeScheduler(gateway_db)
    manager = FakeManager(registry)
    await nodes.create_node(scheduler, {"account_id": "FLOW-007", "worker_port": 18107, "extension_ws_port": 19206, "cdp_port": 19306}, registry)

    async def fake_read_worker_credits(account):
        return {"ok": True, "credits": 1000}

    monkeypatch.setattr(nodes, "_read_worker_credits", fake_read_worker_credits)

    result = await nodes.check_login_and_enable(scheduler, "FLOW-007", registry, manager)

    assert result["ok"] is True
    assert result["result"] == "enabled"
    assert registry.get("FLOW-007").enabled is True
    assert registry.get("FLOW-007").status == "login_verified"
