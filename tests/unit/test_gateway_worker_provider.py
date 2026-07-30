import json
import asyncio
import time

import pytest

from gateway.config import GatewaySettings
from gateway.scheduler import GatewayScheduler
from gateway.worker_provider import RuntimeRegistryWorkerProvider, StaticJsonWorkerProvider, WorkerConfig, WorkerSnapshot
from runtime.registry import AccountRecord, AccountRegistry


class FakeProvider:
    provider_kind = "FakeProvider"
    worker_source = "runtime_registry"

    def __init__(self, snapshots):
        self.snapshots = list(snapshots)
        self.calls = 0

    def load_workers(self):
        self.calls += 1
        if isinstance(self.snapshots[0], Exception):
            raise self.snapshots[0]
        if len(self.snapshots) == 1:
            return self.snapshots[0]
        return self.snapshots.pop(0)


class SubmitGuard:
    async def submit_omni_video(self, *_args, **_kwargs):
        raise AssertionError("dry-run must not submit worker tasks")


def snapshot(workers, candidates=None):
    return WorkerSnapshot(
        workers=workers,
        candidates=candidates if candidates is not None else [{"account_id": worker.account_id, "eligible": worker.enabled} for worker in workers],
        worker_source="runtime_registry",
        provider_kind="RuntimeRegistryWorkerProvider",
        registry_snapshot_time="2026-01-01T00:00:00Z",
    )


def make_registry(tmp_path):
    return AccountRegistry(
        db_path=tmp_path / "registry.db",
        profiles_root=tmp_path / "profiles",
        data_root=tmp_path / "data",
        outputs_root=tmp_path / "outputs",
        workers_json_path=tmp_path / "workers.json",
    )


def add_account(registry, account_id="FLOW-024", status="login_verified"):
    account = AccountRecord(
        account_id=account_id,
        display_name=account_id,
        profile_path=str(registry.profiles_root / account_id),
        worker_api_port=8121,
        extension_ws_port=9220,
        chrome_cdp_port=9321,
        database_path=str(registry.data_root / f"{account_id}.db"),
        output_dir=str(registry.outputs_root / account_id),
        enabled=True,
        status=status,
        created_at="2026-01-01T00:00:00Z",
    )
    registry.register_many([account], create_dirs=False)


def test_default_gateway_scheduler_uses_runtime_registry_provider():
    scheduler = GatewayScheduler(GatewaySettings())
    assert isinstance(scheduler.worker_provider, RuntimeRegistryWorkerProvider)


def test_explicit_static_json_provider_reads_static_config(tmp_path):
    workers = tmp_path / "workers.json"
    workers.write_text(json.dumps([{"account_id": "FLOW-001", "api_url": "http://127.0.0.1:8100", "enabled": True}]), encoding="utf-8")
    provider = StaticJsonWorkerProvider(workers)
    loaded = provider.load_workers()
    assert loaded.provider_kind == "StaticJsonWorkerProvider"
    assert loaded.workers[0].account_id == "FLOW-001"


def test_runtime_provider_failure_does_not_fall_back_to_static_json(tmp_path):
    workers = tmp_path / "workers.json"
    workers.write_text(json.dumps([{"account_id": "FLOW-999", "api_url": "http://127.0.0.1:8999"}]), encoding="utf-8")
    settings = GatewaySettings(workers_path=workers, worker_source="runtime_registry")
    with pytest.raises(RuntimeError, match="runtime_worker_provider_unavailable"):
        GatewayScheduler(settings, worker_provider=FakeProvider([RuntimeError("boom")]))


def test_scheduler_select_worker_is_production_selection_function():
    scheduler = GatewayScheduler(GatewaySettings(), worker_provider=FakeProvider([snapshot([
        WorkerConfig("FLOW-025", "http://127.0.0.1:8125", True, "runtime-25"),
        WorkerConfig("FLOW-024", "http://127.0.0.1:8124", True, "runtime-24"),
    ])]))
    result = scheduler.select_worker({"required_capability": "flow"})
    assert result["selected_account_id"] == "FLOW-024"
    assert result["selection_function"] == "GatewayScheduler.select_worker"


def test_real_scheduler_excludes_stale_quota_by_default():
    scheduler = GatewayScheduler(GatewaySettings(dry_run=False), worker_provider=FakeProvider([snapshot([
        WorkerConfig("FLOW-001", "http://127.0.0.1:8101", True, "runtime-1"),
        WorkerConfig("FLOW-002", "http://127.0.0.1:8102", True, "runtime-2"),
    ])]))
    result = scheduler.select_worker(
        {"required_capability": "flow"},
        account_states=[
            {"account_id": "FLOW-001", "status": "ready", "credits": 1020, "reserved_credits": 0, "health_score": 100, "account_weight": 1.0, "quota_confidence": "stale"},
            {"account_id": "FLOW-002", "status": "ready", "credits": 1035, "reserved_credits": 0, "health_score": 100, "account_weight": 1.0, "quota_confidence": "live"},
        ],
    )
    reasons = {candidate["account_id"]: candidate["reason"] for candidate in result["candidates"]}
    assert result["selected_account_id"] == "FLOW-002"
    assert reasons["FLOW-001"] == "quota_not_live"


def test_dry_run_reuses_scheduler_selection_function_without_side_effects():
    provider = FakeProvider([snapshot([WorkerConfig("FLOW-024", "http://127.0.0.1:8121", True, "runtime-24")])])
    scheduler = GatewayScheduler(GatewaySettings(), worker_client=SubmitGuard(), worker_provider=provider)
    result = scheduler.dispatch_dry_run("DRYRUN-UNIT")
    assert result["result"] == "dry_run_selected"
    assert result["selected_account_id"] == "FLOW-024"
    assert result["scheduler_path_used"] is True
    assert result["selection_function"] == "GatewayScheduler.select_worker"
    assert result["side_effects"] is False
    assert result["would_acquire_lease"] is True
    assert scheduler.db is None


def test_no_eligible_worker_returns_no_eligible_worker():
    scheduler = GatewayScheduler(GatewaySettings(), worker_provider=FakeProvider([snapshot([])]))
    result = scheduler.dispatch_dry_run("DRYRUN-NONE")
    assert result["result"] == "no_eligible_worker"
    assert result["side_effects"] is False
    assert result["would_acquire_lease"] is False


def test_stale_runtime_instance_returns_stale_runtime_instance():
    first = snapshot([WorkerConfig("FLOW-024", "http://127.0.0.1:8121", True, "runtime-old")])
    second = snapshot([WorkerConfig("FLOW-024", "http://127.0.0.1:8121", True, "runtime-new")])
    scheduler = GatewayScheduler(GatewaySettings(), worker_provider=FakeProvider([first, first, second]))
    result = scheduler.dispatch_dry_run("DRYRUN-STALE")
    assert result["result"] == "stale_runtime_instance"
    assert result["selected_runtime_instance_id"] == "runtime-old"
    assert result["current_runtime_instance_id"] == "runtime-new"


def test_runtime_provider_reuses_gateway_projection_and_excludes_login_required(tmp_path):
    registry = make_registry(tmp_path)
    add_account(registry, status="login_required")

    class FakeProjection:
        def candidates(self):
            class Candidate:
                def to_dict(self):
                    return {
                        "account_id": "FLOW-024",
                        "eligible": False,
                        "worker_api_endpoint": "http://127.0.0.1:8121",
                        "runtime_instance_id": "runtime-24",
                    }
            return [Candidate()]

    provider = RuntimeRegistryWorkerProvider(registry, projection=FakeProjection())
    loaded = provider.load_workers()
    assert loaded.candidate_count == 1
    assert loaded.eligible_count == 0
    assert loaded.workers == []


def test_static_json_overlay_cannot_create_runtime_provider_workers_or_override_ports(tmp_path):
    workers = tmp_path / "workers.json"
    workers.write_text(json.dumps([{"account_id": "FLOW-999", "api_url": "http://127.0.0.1:8999"}]), encoding="utf-8")
    registry = make_registry(tmp_path)
    add_account(registry)

    class FakeProjection:
        def candidates(self):
            class Candidate:
                def to_dict(self):
                    return {
                        "account_id": "FLOW-024",
                        "eligible": True,
                        "worker_api_endpoint": "http://127.0.0.1:8121",
                        "runtime_instance_id": "runtime-24",
                    }
            return [Candidate()]

    loaded = RuntimeRegistryWorkerProvider(registry, projection=FakeProjection()).load_workers()
    assert [worker.account_id for worker in loaded.workers] == ["FLOW-024"]
    assert loaded.workers[0].api_url == "http://127.0.0.1:8121"


def test_provider_output_contains_no_sensitive_material():
    loaded = snapshot([WorkerConfig("FLOW-024", "http://127.0.0.1:8121", True, "runtime-24")])
    payload = json.dumps(loaded.diagnostics())
    lowered = payload.lower()
    assert "cookie" not in lowered
    assert "token" not in lowered
    assert "secret" not in lowered
    assert "nonce" not in lowered
    assert "@" not in payload


@pytest.mark.asyncio
async def test_async_worker_snapshot_refresh_does_not_block_event_loop():
    class SlowProvider(FakeProvider):
        def load_workers(self):
            self.calls += 1
            if self.calls == 1:
                return snapshot([])
            time.sleep(0.2)
            return snapshot([WorkerConfig("FLOW-024", "http://127.0.0.1:8121", True, "runtime-24")])

    scheduler = GatewayScheduler(GatewaySettings(), worker_provider=SlowProvider([snapshot([])]))
    refresh = asyncio.create_task(scheduler.async_refresh_worker_snapshot())
    started = time.perf_counter()
    await asyncio.sleep(0.02)
    elapsed = time.perf_counter() - started
    await refresh
    assert elapsed < 0.1
    assert scheduler.workers[0].account_id == "FLOW-024"


@pytest.mark.asyncio
async def test_async_worker_snapshot_refresh_allows_only_one_provider_scan():
    class SlowProvider(FakeProvider):
        def __init__(self):
            super().__init__([snapshot([])])
            self.active = 0
            self.max_active = 0

        def load_workers(self):
            self.calls += 1
            if self.calls == 1:
                return snapshot([])
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                time.sleep(0.1)
                return snapshot([WorkerConfig("FLOW-024", "http://127.0.0.1:8121", True, f"runtime-{self.calls}")])
            finally:
                self.active -= 1

    provider = SlowProvider()
    scheduler = GatewayScheduler(GatewaySettings(), worker_provider=provider)
    await asyncio.gather(scheduler.async_refresh_worker_snapshot(), scheduler.async_refresh_worker_snapshot())
    assert provider.max_active == 1
    assert provider.calls == 3


@pytest.mark.asyncio
async def test_run_loop_records_refresh_time_after_scan_finishes():
    scheduler = GatewayScheduler(GatewaySettings(worker_refresh_interval_seconds=10.0), worker_provider=FakeProvider([snapshot([])]))
    finished_at = 0.0

    async def slow_refresh():
        nonlocal finished_at
        await asyncio.sleep(0.05)
        finished_at = asyncio.get_running_loop().time()

    async def no_schedule():
        return None

    scheduler.refresh_workers = slow_refresh
    scheduler.schedule_once = no_schedule
    scheduler._last_worker_refresh = 0.0
    task = asyncio.create_task(scheduler._run_loop())
    await asyncio.sleep(0.09)
    scheduler._stopping = True
    await task
    assert scheduler._last_worker_refresh >= finished_at
