"""V4 account node management for the local Gateway."""
from __future__ import annotations

import asyncio
import csv
import json
from dataclasses import asdict
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from runtime.process_manager import RuntimeManager
from runtime.gateway_projection import ReadOnlyRuntimeStatusProvider
from runtime.port_allocator import port_can_bind, port_is_listening
from runtime.registry import AccountRegistry

from . import crud
from .worker_provider import WorkerConfig


FLOW_URL = "https://labs.google/fx/tools/flow"


def examples() -> dict:
    rows = [
        {"account_id": "FLOW-004", "worker_port": 8104, "cdp_port": 9303, "enabled": False, "display_name": "FLOW-004"},
        {"account_id": "FLOW-005", "worker_port": 8105, "cdp_port": 9304, "enabled": False, "display_name": "FLOW-005"},
    ]
    return {
        "csv": "account_id,worker_port,cdp_port,enabled,display_name\n"
        + "\n".join(f"{r['account_id']},{r['worker_port']},{r['cdp_port']},{str(r['enabled']).lower()},{r['display_name']}" for r in rows),
        "json": {"nodes": rows},
    }


async def list_nodes(scheduler, registry: AccountRegistry | None = None, manager: RuntimeManager | None = None) -> list[dict]:
    registry = registry or AccountRegistry()
    status_provider = manager or ReadOnlyRuntimeStatusProvider()
    gateway_accounts = {item["account_id"]: item for item in await crud.list_accounts(scheduler.db)}
    nodes = []
    for account in registry.list_accounts():
        nodes.append(await _node_snapshot(account.account_id, scheduler, registry, status_provider, gateway_accounts.get(account.account_id)))
    return nodes


async def get_node(scheduler, account_id: str, registry: AccountRegistry | None = None, manager: RuntimeManager | None = None) -> dict | None:
    registry = registry or AccountRegistry()
    if not registry.get(account_id):
        return None
    status_provider = manager or ReadOnlyRuntimeStatusProvider()
    gateway_account = await crud.get_account(scheduler.db, account_id)
    return await _node_snapshot(account_id, scheduler, registry, status_provider, gateway_account)


async def create_node(scheduler, payload: dict, registry: AccountRegistry | None = None) -> dict:
    registry = registry or AccountRegistry()
    worker_port = payload.get("worker_port", payload.get("worker_api_port"))
    cdp_port = payload.get("cdp_port", payload.get("chrome_cdp_port"))
    record, result = registry.upsert_account(
        account_id=payload.get("account_id"),
        display_name=payload.get("display_name"),
        worker_api_port=worker_port,
        chrome_cdp_port=cdp_port,
        extension_ws_port=payload.get("extension_ws_port"),
        enabled=_as_bool(payload.get("enabled", False)),
        profile_path=payload.get("profile_path"),
    )
    if record and scheduler.db:
        await crud.upsert_account(
            scheduler.db,
            WorkerConfig(record.account_id, f"http://127.0.0.1:{record.worker_api_port}", bool(record.enabled), record.runtime_instance_id),
            status="offline",
            credits=None,
        )
    node = await get_node(scheduler, record.account_id, registry) if record else None
    return {"ok": True, "result": result, "node": node}


def normalize_flow_account_id(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        raise ValueError("flow account number is required")
    if raw.startswith("FLOW-"):
        raw = raw.split("FLOW-", 1)[1]
    if not raw.isdigit():
        raise ValueError("flow account number must be numeric or FLOW-###")
    number = int(raw)
    if number <= 0 or number > 999:
        raise ValueError("flow account number must be between 1 and 999")
    return f"FLOW-{number:03d}"


def quick_add_preview(payload: dict, registry: AccountRegistry | None = None, manager: RuntimeManager | None = None) -> dict:
    registry = registry or AccountRegistry()
    account_id = normalize_flow_account_id(payload.get("flow_account_number") or payload.get("account_id"))
    number = int(account_id.rsplit("-", 1)[-1])
    worker_port = int(payload.get("worker_port") or payload.get("worker_api_port") or (8100 + number))
    extension_ws_port = int(payload.get("extension_ws_port") or payload.get("ws_port") or (9199 + number))
    cdp_port = int(payload.get("cdp_port") or payload.get("chrome_cdp_port") or (9299 + number))
    profile_path = str(Path(payload.get("profile_path") or registry.profiles_root / account_id))
    database_path = str(registry.data_root / f"{account_id}.db")
    output_dir = str(registry.outputs_root / account_id)
    preview = {
        "account_id": account_id,
        "display_name": payload.get("display_name") or account_id,
        "worker_host": payload.get("worker_host") or "127.0.0.1",
        "worker_port": worker_port,
        "worker_api_port": worker_port,
        "extension_ws_port": extension_ws_port,
        "ws_port": extension_ws_port,
        "cdp_host": payload.get("cdp_host") or "127.0.0.1",
        "cdp_port": cdp_port,
        "chrome_cdp_port": cdp_port,
        "profile_path": profile_path,
        "database_path": database_path,
        "output_dir": output_dir,
        "enabled": False,
    }
    preview["checks"] = _quick_add_checks(preview, registry, manager or RuntimeManager(registry))
    preview["ok"] = not preview["checks"]["errors"]
    return preview


async def quick_create_login(scheduler, payload: dict, registry: AccountRegistry | None = None, manager: RuntimeManager | None = None) -> dict:
    registry = registry or AccountRegistry()
    manager = manager or RuntimeManager(registry)
    preview = quick_add_preview(payload, registry, manager)
    if not preview["ok"]:
        return {"ok": False, "result": "preflight_failed", "preview": preview}
    for path in (preview["profile_path"], preview["database_path"], preview["output_dir"]):
        target = Path(path)
        (target.parent if target.suffix else target).mkdir(parents=True, exist_ok=True)
    created = await create_node(
        scheduler,
        {
            "account_id": preview["account_id"],
            "display_name": preview["display_name"],
            "worker_port": preview["worker_port"],
            "extension_ws_port": preview["extension_ws_port"],
            "cdp_port": preview["cdp_port"],
            "profile_path": preview["profile_path"],
            "enabled": False,
        },
        registry,
    )
    chrome = await asyncio.to_thread(manager.open_login, preview["account_id"])
    worker = await asyncio.to_thread(manager.start_worker_only, preview["account_id"])
    await _wait_runtime_identity(registry, preview["account_id"])
    registry.sync_workers_json()
    await _open_or_refresh_flow_page(preview["cdp_port"])
    await scheduler.async_refresh_worker_snapshot()
    return {
        "ok": bool(created.get("ok")),
        "result": "waiting_for_manual_login",
        "message": "Waiting for manual Google/Flow login",
        "preview": preview,
        "created": created,
        "chrome": chrome.to_dict(),
        "worker": worker.to_dict(),
        "node": await get_node(scheduler, preview["account_id"], registry, manager),
    }


async def check_login_and_enable(scheduler, account_id: str, registry: AccountRegistry | None = None, manager: RuntimeManager | None = None) -> dict | None:
    registry = registry or AccountRegistry()
    manager = manager or RuntimeManager(registry)
    if not registry.get(account_id):
        return None
    refreshed = await refresh_session(scheduler, account_id, registry)
    node = await get_node(scheduler, account_id, registry, manager)
    runtime = (node or {}).get("runtime") or {}
    checks = {
        "chrome_cdp_online": bool(runtime.get("chrome_cdp_reachable") or runtime.get("chrome_process_alive")),
        "worker_online": bool(runtime.get("worker_health_reachable")),
        "extension_connected": bool(runtime.get("extension_connected")),
        "account_match": bool(runtime.get("account_match")),
        "ownership_verified": bool(runtime.get("worker_ownership_verified") or runtime.get("worker_service_reachable")),
        "flow_key_present": bool(runtime.get("flow_key_present") or runtime.get("token_captured") or runtime.get("flow_key_captured")),
        "quota_live": bool(node and node.get("quota_confidence") == "live" and node.get("credits") is not None),
    }
    ok = all(checks.values())
    if ok:
        enabled = await set_node_enabled(scheduler, account_id, True, registry)
        return {"ok": True, "result": "enabled", "checks": checks, "refresh": refreshed, "node": enabled}
    return {
        "ok": False,
        "result": "manual_login_required",
        "checks": checks,
        "refresh": refreshed,
        "account_id": account_id,
        "chrome_pid": node.get("chrome_pid") if node else None,
        "cdp_port": node.get("cdp_port") if node else None,
        "node": node,
    }


async def patch_node(scheduler, account_id: str, payload: dict, registry: AccountRegistry | None = None) -> dict | None:
    registry = registry or AccountRegistry()
    updates = {
        "display_name": payload.get("display_name"),
        "worker_api_port": payload.get("worker_port", payload.get("worker_api_port")),
        "chrome_cdp_port": payload.get("cdp_port", payload.get("chrome_cdp_port")),
        "extension_ws_port": payload.get("extension_ws_port"),
        "profile_path": payload.get("profile_path"),
        "enabled": payload.get("enabled") if "enabled" in payload else None,
    }
    record = registry.update_account(account_id, **updates)
    if not record:
        return None
    await scheduler.async_refresh_worker_snapshot()
    await crud.upsert_account(
        scheduler.db,
        WorkerConfig(record.account_id, f"http://127.0.0.1:{record.worker_api_port}", bool(record.enabled), record.runtime_instance_id),
        status=(await crud.get_account(scheduler.db, account_id) or {}).get("status") or "offline",
        credits=(await crud.get_account(scheduler.db, account_id) or {}).get("credits"),
    )
    return await get_node(scheduler, account_id, registry)


async def set_node_enabled(scheduler, account_id: str, enabled: bool, registry: AccountRegistry | None = None) -> dict | None:
    registry = registry or AccountRegistry()
    record = registry.set_enabled(account_id, enabled)
    if not record:
        return None
    await crud.upsert_account(
        scheduler.db,
        WorkerConfig(record.account_id, f"http://127.0.0.1:{record.worker_api_port}", bool(enabled), record.runtime_instance_id),
        status=(await crud.get_account(scheduler.db, account_id) or {}).get("status") or "offline",
        credits=(await crud.get_account(scheduler.db, account_id) or {}).get("credits"),
    )
    await scheduler.async_refresh_worker_snapshot()
    return await get_node(scheduler, account_id, registry)


async def delete_node(scheduler, account_id: str, registry: AccountRegistry | None = None) -> dict | None:
    registry = registry or AccountRegistry()
    if not registry.get(account_id):
        return None
    await set_node_enabled(scheduler, account_id, False, registry)
    removed = registry.remove_account(account_id)
    cleanup = await crud.delete_orphan_account(
        scheduler.db,
        account_id,
        known_registry_account_ids={account.account_id for account in registry.list_accounts()},
    )
    await scheduler.async_refresh_worker_snapshot()
    return {"ok": removed, "account_id": account_id, "profile_deleted": False, "history_deleted": False, "gateway_account_cleanup": cleanup}


async def runtime_action(scheduler, account_id: str, action: str, registry: AccountRegistry | None = None, manager: RuntimeManager | None = None) -> dict | None:
    registry = registry or AccountRegistry()
    if not registry.get(account_id):
        return None
    manager = manager or RuntimeManager(registry)
    if action == "start":
        result = await asyncio.to_thread(manager.start_worker_only, account_id)
    elif action == "stop":
        result = await asyncio.to_thread(manager.stop_worker_only, account_id)
    elif action == "restart":
        stopped = await asyncio.to_thread(manager.stop_worker_only, account_id)
        started = await asyncio.to_thread(manager.start_worker_only, account_id)
        return {"ok": bool(started.ok), "account_id": account_id, "stop": stopped.to_dict(), "start": started.to_dict(), "node": await get_node(scheduler, account_id, registry)}
    else:
        raise ValueError("unsupported_runtime_action")
    return {"ok": bool(result.ok), **result.to_dict(), "node": await get_node(scheduler, account_id, registry)}


async def refresh_session(scheduler, account_id: str, registry: AccountRegistry | None = None) -> dict | None:
    registry = registry or AccountRegistry()
    account = registry.get(account_id)
    if not account:
        return None
    cdp_result = await _open_or_refresh_flow_page(account.chrome_cdp_port)
    await asyncio.sleep(2)
    worker = WorkerConfig(account.account_id, f"http://127.0.0.1:{account.worker_api_port}", bool(account.enabled), account.runtime_instance_id)
    try:
        await scheduler.refresh_workers()
        gateway_account = await crud.get_account(scheduler.db, account_id)
        ok = bool(gateway_account and gateway_account.get("quota_confidence") == "live")
        return {"ok": ok, "account_id": account_id, "cdp": cdp_result, "node": await get_node(scheduler, account_id, registry)}
    except Exception as exc:
        await crud.update_account_controls(scheduler.db, account_id, quota_confidence="stale")
        return {"ok": False, "account_id": account_id, "error_category": "manual_login_required", "error": str(exc)[:300], "worker": asdict(worker), "cdp": cdp_result}


async def import_nodes(scheduler, payload: dict, registry: AccountRegistry | None = None) -> dict:
    registry = registry or AccountRegistry()
    rows = _parse_import_payload(payload)
    result = {"total": len(rows), "success": 0, "duplicate": 0, "failed": 0, "errors": [], "nodes": []}
    for index, row in enumerate(rows, start=1):
        try:
            created = await create_node(scheduler, row, registry)
            if created["result"] == "duplicate":
                result["duplicate"] += 1
            else:
                result["success"] += 1
            result["nodes"].append(created["node"])
        except Exception as exc:
            result["failed"] += 1
            result["errors"].append({"row": index, "account_id": row.get("account_id"), "error": str(exc)})
    return result


async def _node_snapshot(account_id: str, scheduler, registry: AccountRegistry, status_provider, gateway_account: dict | None) -> dict:
    record = registry.get(account_id)
    status_arg = record if isinstance(status_provider, ReadOnlyRuntimeStatusProvider) else account_id
    status = await asyncio.to_thread(status_provider.status, status_arg)
    details = status.details if hasattr(status, "details") else status
    result = status.result if hasattr(status, "result") else details.get("runtime_status")
    gateway_account = gateway_account or await crud.get_account(scheduler.db, account_id)
    return {
        **asdict(record),
        "worker_host": "127.0.0.1",
        "worker_port": record.worker_api_port,
        "worker_api_url": f"http://127.0.0.1:{record.worker_api_port}",
        "cdp_host": "127.0.0.1",
        "cdp_port": record.chrome_cdp_port,
        "worker_status": details.get("runtime_status") or result,
        "worker_online": bool(details.get("worker_health_reachable")),
        "chrome_online": bool(details.get("chrome_cdp_reachable") or details.get("chrome_process_alive")),
        "extension_status": details.get("extension_bootstrap_status") or ("extension_ready" if details.get("extension_connected") else "extension_missing"),
        "extension_connected": bool(details.get("extension_connected")),
        "oauth_status": "live" if gateway_account and gateway_account.get("quota_confidence") == "live" else (gateway_account or {}).get("last_error") or "unknown",
        "quota_confidence": (gateway_account or {}).get("quota_confidence"),
        "credits": (gateway_account or {}).get("credits"),
        "reserved_credits": (gateway_account or {}).get("reserved_credits"),
        "health_score": (gateway_account or {}).get("health_score"),
        "manual_paused": bool((gateway_account or {}).get("manual_paused")),
        "cooldown_until": (gateway_account or {}).get("cooldown_until"),
        "current_task_id": (gateway_account or {}).get("current_task_id"),
        "last_gateway_error": (gateway_account or {}).get("last_error"),
        "runtime": details,
    }


async def _open_or_refresh_flow_page(cdp_port: int) -> dict:
    base = f"http://127.0.0.1:{int(cdp_port)}"
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            pages = (await client.get(f"{base}/json")).json()
        except Exception as exc:
            return {"ok": False, "stage": "cdp_list", "error": str(exc)[:200], "manual_login_required": True}
        for page in pages if isinstance(pages, list) else []:
            page_id = page.get("id")
            url = page.get("url") or ""
            if page_id and "labs.google" in url and "/flow" in url:
                await client.put(f"{base}/json/activate/{quote(page_id, safe='')}")
                return {"ok": True, "stage": "activated_existing_page", "page_id": page_id}
        response = await client.put(f"{base}/json/new?{quote(FLOW_URL, safe=':/?=&')}")
        return {"ok": response.status_code < 400, "stage": "opened_flow_page", "status_code": response.status_code}


def _parse_import_payload(payload: dict) -> list[dict]:
    if isinstance(payload.get("nodes"), list):
        return [dict(item) for item in payload["nodes"]]
    fmt = (payload.get("format") or "").lower()
    content = payload.get("content")
    if fmt == "json":
        data = json.loads(content) if isinstance(content, str) else content
        if isinstance(data, dict):
            data = data.get("nodes", [])
        return [dict(item) for item in data]
    if fmt == "csv":
        return [dict(row) for row in csv.DictReader(StringIO(content or ""))]
    raise ValueError("nodes, JSON content, or CSV content is required")


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    return bool(value)


def _quick_add_checks(preview: dict, registry: AccountRegistry, manager: RuntimeManager) -> dict:
    errors = []
    warnings = []
    account_id = preview["account_id"]
    if registry.get(account_id):
        errors.append({"field": "account_id", "reason": "account_exists", "owner": account_id})
    profile = Path(preview["profile_path"]).resolve(strict=False)
    for other in registry.list_accounts():
        if Path(other.profile_path).resolve(strict=False) == profile and other.account_id != account_id:
            errors.append({"field": "profile_path", "reason": "profile_belongs_to_other_account", "owner": other.account_id})
    for field, port in (
        ("worker_api_port", preview["worker_port"]),
        ("extension_ws_port", preview["extension_ws_port"]),
        ("chrome_cdp_port", preview["cdp_port"]),
    ):
        for other in registry.list_accounts():
            if int(getattr(other, field)) == int(port):
                errors.append({"field": field, "port": port, "reason": "registry_port_conflict", "owner": other.account_id})
        pid = manager.inspector.listening_pid(port)
        if pid:
            errors.append({"field": field, "port": port, "reason": "port_in_use", "pid": pid})
        elif port_is_listening(port) or not port_can_bind(port):
            errors.append({"field": field, "port": port, "reason": "port_unavailable"})
    if profile.exists():
        warnings.append({"field": "profile_path", "reason": "profile_path_already_exists", "path": str(profile)})
    return {"errors": errors, "warnings": warnings}


async def _wait_runtime_identity(registry: AccountRegistry, account_id: str, attempts: int = 20) -> None:
    for _ in range(attempts):
        account = registry.get(account_id)
        if account and account.runtime_instance_id:
            return
        await asyncio.sleep(0.25)
