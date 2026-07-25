"""Worker providers for Gateway scheduling."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from runtime.gateway_projection import GatewayProjection
from runtime.registry import AccountRegistry


@dataclass(frozen=True)
class WorkerConfig:
    account_id: str
    api_url: str
    enabled: bool = True
    runtime_instance_id: str | None = None


@dataclass(frozen=True)
class WorkerSnapshot:
    workers: list[WorkerConfig]
    candidates: list[dict]
    worker_source: str
    provider_kind: str
    registry_snapshot_time: str

    @property
    def candidate_count(self) -> int:
        return len(self.candidates)

    @property
    def eligible_count(self) -> int:
        return sum(1 for candidate in self.candidates if candidate.get("eligible"))

    def diagnostics(self) -> dict:
        return {
            "worker_source": self.worker_source,
            "provider_kind": self.provider_kind,
            "candidate_count": self.candidate_count,
            "eligible_count": self.eligible_count,
            "registry_snapshot_time": self.registry_snapshot_time,
        }


class GatewayWorkerProvider:
    worker_source = "unknown"
    provider_kind = "GatewayWorkerProvider"

    def load_workers(self) -> WorkerSnapshot:
        raise NotImplementedError


class RuntimeRegistryWorkerProvider(GatewayWorkerProvider):
    worker_source = "runtime_registry"
    provider_kind = "RuntimeRegistryWorkerProvider"

    def __init__(self, registry: AccountRegistry | None = None, projection: GatewayProjection | None = None):
        self.registry = registry or AccountRegistry()
        self.projection = projection or GatewayProjection(self.registry)

    def load_workers(self) -> WorkerSnapshot:
        candidates = [candidate.to_dict() for candidate in self.projection.candidates()]
        workers = [
            WorkerConfig(
                account_id=candidate["account_id"],
                api_url=candidate["worker_api_endpoint"],
                enabled=bool(candidate["eligible"]),
                runtime_instance_id=candidate.get("runtime_instance_id"),
            )
            for candidate in candidates
            if candidate.get("eligible")
        ]
        return WorkerSnapshot(
            workers=workers,
            candidates=candidates,
            worker_source=self.worker_source,
            provider_kind=self.provider_kind,
            registry_snapshot_time=_utc_now(),
        )


class StaticJsonWorkerProvider(GatewayWorkerProvider):
    worker_source = "static_json"
    provider_kind = "StaticJsonWorkerProvider"

    def __init__(self, workers_path: Path | str):
        self.workers_path = Path(workers_path)

    def load_workers(self) -> WorkerSnapshot:
        data = json.loads(self.workers_path.read_text(encoding="utf-8"))
        workers = [WorkerConfig(**item) for item in data]
        candidates = [
            {
                "account_id": worker.account_id,
                "eligible": bool(worker.enabled),
                "gateway_status": "ready" if worker.enabled else "disabled",
                "exclusion_reasons": [] if worker.enabled else ["disabled"],
                "worker_api_endpoint": worker.api_url,
                "runtime_instance_id": worker.runtime_instance_id,
            }
            for worker in workers
        ]
        return WorkerSnapshot(
            workers=workers,
            candidates=candidates,
            worker_source=self.worker_source,
            provider_kind=self.provider_kind,
            registry_snapshot_time=_utc_now(),
        )


def build_worker_provider(settings) -> GatewayWorkerProvider:
    source = settings.worker_source
    if source == "runtime_registry":
        return RuntimeRegistryWorkerProvider()
    if source == "static_json":
        return StaticJsonWorkerProvider(settings.workers_path)
    raise ValueError("runtime_worker_provider_unavailable")


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
