"""Central Gateway dry-run API."""
from contextlib import asynccontextmanager
import asyncio
import logging
import sys

import uvicorn
from fastapi import FastAPI, HTTPException

from . import crud
from .config import GatewaySettings
from .instance_lock import GatewayInstanceLock, GatewayInstanceLockError
from .scheduler import GatewayScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
logger = logging.getLogger(__name__)

settings = GatewaySettings.from_env()
scheduler = GatewayScheduler(settings)
instance_lock = GatewayInstanceLock(settings.db_path, settings.api_port)


@asynccontextmanager
async def lifespan(_app):
    lock_payload = None
    logger.info("gateway_audit %s", scheduler.safe_json({"event": "app_startup_begin"}))
    try:
        logger.info("gateway_audit %s", scheduler.safe_json({"event": "instance_lock_acquire_started", "database_path": str(settings.db_path), "gateway_port": settings.api_port}))
        lock_payload = instance_lock.acquire()
        scheduler._audit("instance_lock_acquired", **lock_payload)
    except GatewayInstanceLockError as exc:
        scheduler._audit("gateway_instance_lock_rejected", database_path=str(settings.db_path), gateway_port=settings.api_port)
        scheduler._audit("app_startup_failed", stage="instance_lock", exception_type=type(exc).__name__, error_message=str(exc))
        raise RuntimeError(str(exc)) from exc
    try:
        await asyncio.wait_for(scheduler.start(), timeout=settings.startup_timeout_seconds)
        scheduler._audit("app_startup_completed")
    except Exception as exc:
        scheduler._audit("app_startup_failed", stage=scheduler.startup_stage or "scheduler_start", exception_type=type(exc).__name__, error_message=str(exc)[:500])
        if lock_payload:
            instance_lock.release()
        raise
    try:
        yield
    finally:
        await scheduler.stop()
        instance_lock.release()


app = FastAPI(title="Flow Gateway Dry Run", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "dry_run": settings.dry_run, "api_port": settings.api_port}


@app.get("/api/pool/status")
async def pool_status():
    return await scheduler.pool_status()


@app.get("/api/pool/accounts")
async def pool_accounts():
    return await scheduler.list_accounts()


@app.post("/api/pool/accounts/{account_id}/pause")
async def pause_account(account_id: str, payload: dict | None = None):
    payload = payload or {}
    account = await crud.update_account_controls(
        scheduler.db,
        account_id,
        manual_paused=1,
        manual_pause_reason=payload.get("reason") or "manual_pause",
    )
    if not account:
        raise HTTPException(404, "Account not found")
    return account


@app.post("/api/pool/accounts/{account_id}/resume")
async def resume_account(account_id: str):
    account = await crud.update_account_controls(
        scheduler.db,
        account_id,
        manual_paused=0,
        manual_pause_reason=None,
    )
    if not account:
        raise HTTPException(404, "Account not found")
    return account


@app.post("/api/pool/accounts/{account_id}/cooldown")
async def set_account_cooldown(account_id: str, payload: dict):
    account = await crud.update_account_controls(
        scheduler.db,
        account_id,
        cooldown_until=payload.get("cooldown_until"),
        cooldown_reason=payload.get("reason") or "manual_cooldown",
    )
    if not account:
        raise HTTPException(404, "Account not found")
    return account


@app.post("/api/pool/accounts/{account_id}/cooldown/clear")
async def clear_account_cooldown(account_id: str):
    account = await crud.update_account_controls(
        scheduler.db,
        account_id,
        cooldown_until=None,
        cooldown_reason=None,
    )
    if not account:
        raise HTTPException(404, "Account not found")
    return account


@app.post("/api/pool/accounts/{account_id}/weight")
async def set_account_weight(account_id: str, payload: dict):
    try:
        weight = float(payload.get("account_weight"))
    except (TypeError, ValueError):
        raise HTTPException(400, "account_weight must be a number") from None
    account = await crud.update_account_controls(scheduler.db, account_id, account_weight=weight)
    if not account:
        raise HTTPException(404, "Account not found")
    return account


@app.post("/api/pool/accounts/{account_id}/credits")
async def calibrate_account_credits(account_id: str, payload: dict):
    credits = payload.get("credits")
    if isinstance(credits, bool) or not isinstance(credits, int) or credits < 0:
        raise HTTPException(400, "credits must be a non-negative integer")
    account = await crud.update_account_controls(
        scheduler.db,
        account_id,
        credits=credits,
        credits_total=credits,
        quota_source=payload.get("source") or "manual_calibration",
        quota_confidence=payload.get("confidence") or "manual",
    )
    if not account:
        raise HTTPException(404, "Account not found")
    return account


@app.get("/api/pool/tasks")
async def pool_tasks():
    return await scheduler.list_tasks()


@app.get("/api/pool/tasks/{task_id}")
async def pool_task(task_id: str):
    task = await scheduler.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task


@app.post("/api/pool/tasks")
async def create_task(payload: dict):
    try:
        return await scheduler.create_task(payload)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/pool/tasks/batch")
async def create_tasks(payload: dict):
    tasks = payload.get("tasks", payload if isinstance(payload, list) else [])
    try:
        return {"tasks": await scheduler.create_tasks(tasks)}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


if __name__ == "__main__":
    logger.info("gateway_audit %s", scheduler.safe_json({
        "event": "gateway_server_entry",
        "python_executable": sys.executable,
        "gateway_port": settings.api_port,
        "database_path": str(settings.db_path),
    }))
    uvicorn.run(app, host=settings.api_host, port=settings.api_port, reload=False)
