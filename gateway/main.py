"""Central Gateway dry-run API."""
from contextlib import asynccontextmanager
import asyncio
import logging
import sys

import uvicorn
from fastapi import FastAPI, HTTPException

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
