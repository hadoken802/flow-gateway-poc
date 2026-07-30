"""Read-only Gateway projection for verified runtime accounts."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from urllib.request import urlopen

from .process_manager import ProcessInspector
from .registry import AccountRegistry


EXCLUSION_ORDER = [
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


@dataclass(frozen=True)
class GatewayCandidate:
    account_id: str
    registration_status: str | None
    runtime_status: str | None
    runtime_healthy: bool
    extension_connected: bool
    extension_ready: bool
    account_match: bool
    ownership_verified: bool
    chrome_ownership_verified: bool
    worker_ownership_verified: bool
    chrome_process_alive: bool
    worker_process_alive: bool
    worker_health_reachable: bool
    chrome_ownership_verified_at: str | None
    worker_ownership_verified_at: str | None
    runtime_instance_id: str | None
    stop_safe: bool
    gateway_status: str
    eligible: bool
    exclusion_reasons: list[str]
    worker_api_endpoint: str
    worker_ws_endpoint: str
    current_task_id: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


class GatewayProjection:
    def __init__(self, registry: AccountRegistry | None = None, status_provider: object | None = None):
        self.registry = registry or AccountRegistry()
        self.status_provider = status_provider or ReadOnlyRuntimeStatusProvider()

    def candidates(self) -> list[GatewayCandidate]:
        return [self._candidate_for(account) for account in self.registry.list_accounts()]

    def _candidate_for(self, account) -> GatewayCandidate:
        if not account.enabled or account.status != "login_verified":
            details = self._unprobed_details(account)
        else:
            details = self.status_provider.status(account)
        extension_connected = bool(details.get("extension_connected"))
        account_match = bool(details.get("account_match"))
        extension_ready = bool(extension_connected and account_match)
        chrome_ownership_verified = bool(details.get("chrome_ownership_verified"))
        worker_ownership_verified = bool(details.get("worker_ownership_verified"))
        ownership_verified = bool(chrome_ownership_verified and worker_ownership_verified)
        values = {
            "disabled": not account.enabled,
            "registration_not_verified": account.status != "login_verified",
            "runtime_not_running": details.get("runtime_status") != "running",
            "runtime_unhealthy": not bool(details.get("runtime_healthy")),
            "chrome_not_running": not bool(details.get("chrome_process_alive")),
            "worker_not_running": not bool(details.get("worker_process_alive")),
            "worker_unreachable": not bool(details.get("worker_health_reachable")),
            "extension_not_ready": not extension_ready,
            "account_mismatch": not account_match,
            "ownership_not_verified": not ownership_verified,
            "runtime_instance_missing": not bool(details.get("runtime_instance_id")),
            "stop_not_safe": not bool(details.get("stop_safe")),
        }
        reasons = [reason for reason in EXCLUSION_ORDER if values[reason]]
        eligible = not reasons
        return GatewayCandidate(
            account_id=account.account_id,
            registration_status=account.status,
            runtime_status=details.get("runtime_status"),
            runtime_healthy=bool(details.get("runtime_healthy")),
            extension_connected=extension_connected,
            extension_ready=extension_ready,
            account_match=account_match,
            ownership_verified=ownership_verified,
            chrome_ownership_verified=chrome_ownership_verified,
            worker_ownership_verified=worker_ownership_verified,
            chrome_process_alive=bool(details.get("chrome_process_alive")),
            worker_process_alive=bool(details.get("worker_process_alive")),
            worker_health_reachable=bool(details.get("worker_health_reachable")),
            chrome_ownership_verified_at=None,
            worker_ownership_verified_at=details.get("worker_ownership_verified_at"),
            runtime_instance_id=details.get("runtime_instance_id"),
            stop_safe=bool(details.get("stop_safe")),
            gateway_status=self._gateway_status(account.enabled, account.status, details, eligible, reasons),
            eligible=eligible,
            exclusion_reasons=reasons,
            worker_api_endpoint=f"http://127.0.0.1:{int(account.worker_api_port)}",
            worker_ws_endpoint=f"ws://127.0.0.1:{int(account.extension_ws_port)}",
            current_task_id=None,
        )

    def _unprobed_details(self, account) -> dict:
        return {
            "runtime_status": "stopped",
            "runtime_healthy": False,
            "chrome_process_alive": False,
            "worker_process_alive": False,
            "worker_health_reachable": False,
            "extension_connected": False,
            "account_match": False,
            "chrome_ownership_verified": False,
            "worker_ownership_verified": False,
            "worker_ownership_verified_at": account.worker_ownership_verified_at,
            "runtime_instance_id": account.runtime_instance_id,
            "stop_safe": False,
        }

    def _gateway_status(self, enabled: bool, registration_status: str, details: dict, eligible: bool, reasons: list[str]) -> str:
        if not enabled:
            return "disabled"
        if registration_status != "login_verified":
            return "login_required"
        if eligible:
            return "ready"
        if "runtime_unhealthy" in reasons or "extension_not_ready" in reasons or "account_mismatch" in reasons:
            return "unhealthy"
        return "offline"


class ReadOnlyRuntimeStatusProvider:
    def __init__(self, inspector: ProcessInspector | None = None):
        self.inspector = inspector or ProcessInspector()

    def status(self, account) -> dict:
        chrome_probe = self.inspector.probe_process(account.chrome_pid)
        worker_probe = self.inspector.probe_process(account.worker_pid)
        worker_health = self._worker_health(account.worker_api_port)
        chrome_cdp_listener_pid = self.inspector.listening_pid(account.chrome_cdp_port)
        worker_api_listener_pid = self.inspector.listening_pid(account.worker_api_port)
        worker_ws_listener_pid = self.inspector.listening_pid(account.extension_ws_port)
        chrome_alive = bool(chrome_probe.alive is True or chrome_cdp_listener_pid)
        worker_alive = bool(worker_probe.alive is True or (worker_api_listener_pid and worker_ws_listener_pid))
        chrome_ownership = self._chrome_ownership(account, chrome_alive, chrome_cdp_listener_pid)
        worker_ownership = self._worker_ownership(account, worker_health, worker_alive, worker_api_listener_pid, worker_ws_listener_pid)
        cdp_reachable = bool(chrome_cdp_listener_pid)
        extension_connected = bool(worker_health.get("extension_connected"))
        account_match = extension_connected and worker_health.get("account_id") == account.account_id
        worker_health_reachable = bool(worker_health)
        worker_ports_listening = bool(worker_api_listener_pid and worker_ws_listener_pid and int(worker_api_listener_pid) == int(worker_ws_listener_pid))
        worker_service_reachable = bool(worker_health_reachable and worker_ports_listening and worker_ownership)
        chrome_service_reachable = bool(cdp_reachable and chrome_ownership)
        runtime_healthy = bool(worker_service_reachable and chrome_service_reachable and account_match)
        if runtime_healthy:
            runtime_status = "running"
        elif worker_service_reachable and chrome_service_reachable:
            runtime_status = "unhealthy"
        elif worker_health_reachable or cdp_reachable or worker_alive or chrome_alive:
            runtime_status = "partial"
        else:
            runtime_status = "stopped"
        return {
            "runtime_status": runtime_status,
            "runtime_healthy": runtime_healthy,
            "chrome_process_alive": chrome_alive,
            "worker_process_alive": worker_alive,
            "worker_health_reachable": worker_health_reachable,
            "extension_connected": extension_connected,
            "flow_key_present": bool(worker_health.get("flow_key_present")),
            "account_match": account_match,
            "chrome_ownership_verified": chrome_ownership,
            "worker_ownership_verified": worker_ownership,
            "worker_ownership_verified_at": account.worker_ownership_verified_at,
            "runtime_instance_id": account.runtime_instance_id,
            "stop_safe": bool(chrome_ownership and worker_ownership),
        }

    def _chrome_ownership(self, account, chrome_alive: bool, listener_pid: int | None) -> bool:
        if not chrome_alive:
            return False
        if listener_pid and account.chrome_pid and int(listener_pid) == int(account.chrome_pid):
            return True
        command = self.inspector.command_line(listener_pid or account.chrome_pid)
        normalized = self._normalize(command)
        return self._normalize(str(Path(account.profile_path))) in normalized and f"remote-debugging-port={account.chrome_cdp_port}" in normalized

    def _worker_ownership(self, account, health: dict, worker_alive: bool, api_pid: int | None, ws_pid: int | None) -> bool:
        if not worker_alive or not health or not api_pid or not ws_pid or int(api_pid) != int(ws_pid):
            return False
        if not account.runtime_instance_id or not account.worker_ownership_verified_at:
            return False
        if health.get("account_id") != account.account_id:
            return False
        if health.get("runtime_instance_id") != account.runtime_instance_id:
            return False
        return int(health.get("runtime_ownership_version") or 0) == int(account.runtime_ownership_version or 0)

    def _worker_health(self, port: int) -> dict:
        try:
            with urlopen(f"http://127.0.0.1:{int(port)}/health", timeout=1.0) as response:
                data = json.loads(response.read().decode("utf-8"))
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    def _normalize(self, value: str) -> str:
        return str(value).replace("\\", "/").replace('"', "").lower()
