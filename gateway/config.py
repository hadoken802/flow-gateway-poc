"""Gateway configuration."""
import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_GATEWAY_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "gateway.db"


def _port(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be a valid TCP port") from None
    if value < 1 or value > 65535:
        raise ValueError(f"{name} must be a valid TCP port")
    return value


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _float_tuple(name: str, default: tuple[float, float, float]) -> tuple[float, float, float]:
    raw = os.environ.get(name)
    if not raw:
        return default
    parts = [part.strip() for part in raw.split(",")]
    if len(parts) != 3:
        raise ValueError(f"{name} must contain three comma-separated seconds")
    return (float(parts[0]), float(parts[1]), float(parts[2]))


def _positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive integer") from None
    if value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive number") from None
    if value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return value


@dataclass
class GatewaySettings:
    api_host: str = "127.0.0.1"
    api_port: int = 8200
    max_concurrency: int = 2
    dry_run: bool = True
    canary_only: bool = False
    canary_limit: int = 2
    omni_10s_credit_cost: int = 15
    db_path: Path = DEFAULT_GATEWAY_DB_PATH
    workers_path: Path = Path(__file__).parent / "workers.json"
    worker_source: str = "runtime_registry"
    dry_run_step_seconds: tuple[float, float, float] = (1.0, 2.0, 3.0)
    worker_refresh_interval_seconds: float = 1.0
    real_submit_max_attempts: int = 1
    worker_submit_timeout_seconds: float = 300.0
    allowed_account_ids: tuple[str, ...] = ()
    startup_timeout_seconds: float = 60.0
    lease_duration_seconds: float = 15 * 60
    heartbeat_interval_seconds: float = 30.0
    lease_sweeper_interval_seconds: float = 30.0
    allow_stale_quota_scheduling: bool = False

    @classmethod
    def from_env(cls) -> "GatewaySettings":
        return cls(
            api_host=os.environ.get("GATEWAY_API_HOST", "127.0.0.1"),
            api_port=_port("GATEWAY_API_PORT", 8200),
            max_concurrency=int(os.environ.get("POOL_MAX_CONCURRENCY", "2")),
            dry_run=_bool("POOL_DRY_RUN", True),
            canary_only=_bool("CANARY_ONLY", False),
            canary_limit=int(os.environ.get("CANARY_LIMIT", "2")),
            omni_10s_credit_cost=int(os.environ.get("OMNI_10S_CREDIT_COST", "15")),
            db_path=Path(os.environ.get("GATEWAY_DB_PATH", str(DEFAULT_GATEWAY_DB_PATH))),
            worker_source=os.environ.get("FLOWKIT_GATEWAY_WORKER_SOURCE", "runtime_registry"),
            dry_run_step_seconds=_float_tuple("DRY_RUN_STEP_SECONDS", (1.0, 2.0, 3.0)),
            real_submit_max_attempts=_positive_int("REAL_SUBMIT_MAX_ATTEMPTS", 1),
            worker_submit_timeout_seconds=_positive_float("GATEWAY_WORKER_SUBMIT_TIMEOUT_SECONDS", 300.0),
            allowed_account_ids=_account_ids("GATEWAY_ALLOWED_ACCOUNT_IDS"),
            startup_timeout_seconds=_positive_float("GATEWAY_STARTUP_TIMEOUT_SECONDS", 60.0),
            lease_duration_seconds=_positive_float("GATEWAY_LEASE_DURATION_SECONDS", 15 * 60),
            heartbeat_interval_seconds=_positive_float("GATEWAY_HEARTBEAT_INTERVAL_SECONDS", 30.0),
            lease_sweeper_interval_seconds=_positive_float("GATEWAY_LEASE_SWEEPER_INTERVAL_SECONDS", 30.0),
            allow_stale_quota_scheduling=_bool("GATEWAY_ALLOW_STALE_QUOTA_SCHEDULING", False),
        )


def _account_ids(name: str) -> tuple[str, ...]:
    raw = os.environ.get(name, "")
    seen = set()
    result = []
    for item in raw.split(","):
        account_id = item.strip()
        if account_id and account_id not in seen:
            seen.add(account_id)
            result.append(account_id)
    return tuple(result)
