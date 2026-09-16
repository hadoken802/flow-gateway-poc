"""Central Gateway dry-run API."""
from contextlib import asynccontextmanager
import asyncio
import logging
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from . import crud
from .config import GatewaySettings
from .instance_lock import GatewayInstanceLock, GatewayInstanceLockError
from .scheduler import GatewayScheduler
from . import task_center
from . import nodes
from . import client_files

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


app = FastAPI(title="Flow Gateway V3 Task Center", version="0.3.0", lifespan=lifespan)


@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
    path = request.url.path
    try:
        if _client_api_protected(path):
            _require_client_key(request)
        elif _admin_api_protected(path):
            _require_admin_key(request)
    except HTTPException as exc:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    return await call_next(request)


@app.get("/", response_class=HTMLResponse)
async def task_center_page():
    return TASK_CENTER_HTML


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


@app.post("/api/v1/tasks")
async def v1_create_task(payload: dict):
    task = await create_task(payload)
    return {
        "task_id": task["task_id"],
        "batch_id": task.get("batch_id"),
        "idempotency_key": task["idempotency_key"],
        "status": task["status"],
        "duplicate": bool(task.get("reused")),
        "created_at": task.get("created_at"),
    }


@app.post("/api/v1/tasks/import")
async def v1_import_tasks(payload: dict):
    try:
        return await task_center.import_tasks(scheduler, payload)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/v1/tasks")
async def v1_list_tasks(
    status: str | None = Query(default=None),
    batch_id: str | None = Query(default=None),
    account_id: str | None = Query(default=None),
    error_category: str | None = Query(default=None),
):
    return await task_center.list_tasks(
        scheduler,
        {
            "status": status,
            "batch_id": batch_id,
            "account_id": account_id,
            "error_category": error_category,
        },
    )


@app.get("/api/v1/tasks/{task_id}")
async def v1_task_detail(task_id: str):
    detail = await task_center.task_detail(scheduler, task_id)
    if not detail:
        raise HTTPException(404, "Task not found")
    return detail


@app.post("/api/v1/tasks/{task_id}/pause")
async def v1_pause_task(task_id: str):
    task = await task_center.pause_task(scheduler, task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task


@app.post("/api/v1/tasks/{task_id}/resume")
async def v1_resume_task(task_id: str):
    task = await task_center.resume_task(scheduler, task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task


@app.post("/api/v1/tasks/{task_id}/cancel")
async def v1_cancel_task(task_id: str):
    task = await task_center.cancel_task(scheduler, task_id)
    if not task:
        raise HTTPException(409, "Only queued tasks can be cancelled")
    return task


@app.post("/api/v1/tasks/{task_id}/priority")
async def v1_set_task_priority(task_id: str, payload: dict):
    try:
        priority = int(payload.get("priority"))
    except (TypeError, ValueError):
        raise HTTPException(400, "priority must be an integer") from None
    task = await task_center.set_priority(scheduler, task_id, priority)
    if not task:
        raise HTTPException(409, "Only queued tasks can change priority")
    return task


@app.post("/api/v1/tasks/{task_id}/requeue")
async def v1_requeue_task(task_id: str):
    task = await task_center.requeue_task(scheduler, task_id)
    if not task:
        raise HTTPException(409, "Task is not eligible for requeue")
    return task


@app.post("/api/v1/tasks/{task_id}/retry-download")
async def v1_retry_download(task_id: str):
    task = await scheduler.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    if task.get("status") not in {"download_failed", "download_pending", "generated"}:
        raise HTTPException(409, "Task is not eligible for retry-download")
    return {"ok": True, "task_id": task_id, "planned_action": "scheduler_download_recovery", "submit_called": False}


@app.post("/api/v1/tasks/{task_id}/reconcile")
async def v1_reconcile_task(task_id: str):
    task = await scheduler.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    if task.get("status") != "submission_unknown":
        raise HTTPException(409, "Task is not submission_unknown")
    return {"ok": True, "task_id": task_id, "planned_action": "submission_unknown_reconcile", "submit_called": False}


@app.post("/api/v1/tasks/{task_id}/need-manual")
async def v1_mark_need_manual(task_id: str, payload: dict | None = None):
    payload = payload or {}
    task = await crud.update_task_status(
        scheduler.db,
        task_id,
        "manual_review",
        error_code=payload.get("error_code") or "NEED_MANUAL",
        error_message=payload.get("error_message") or "Marked for manual review",
        last_error_category="need_manual",
    )
    return task


@app.get("/api/v1/batches")
async def v1_batches():
    return await crud.list_batches(scheduler.db)


@app.get("/api/v1/batches/{batch_id}")
async def v1_batch_detail(batch_id: str):
    return await task_center.batch_detail(scheduler, batch_id)


@app.get("/api/v1/accounts")
async def v1_accounts():
    return await scheduler.list_accounts()


@app.get("/api/v1/nodes")
async def v1_nodes():
    return await nodes.list_nodes(scheduler)


@app.post("/api/v1/nodes")
async def v1_create_node(payload: dict):
    try:
        return await nodes.create_node(scheduler, payload)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/v1/nodes/quick-preview")
async def v1_quick_preview_node(payload: dict):
    try:
        return nodes.quick_add_preview(payload)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/v1/nodes/quick-create-login")
async def v1_quick_create_login_node(payload: dict):
    try:
        return await nodes.quick_create_login(scheduler, payload)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/v1/nodes/import")
async def v1_import_nodes(payload: dict):
    return await nodes.import_nodes(scheduler, payload)


@app.get("/api/v1/nodes/{account_id}")
async def v1_get_node(account_id: str):
    node = await nodes.get_node(scheduler, account_id)
    if not node:
        raise HTTPException(404, "Node not found")
    return node


@app.patch("/api/v1/nodes/{account_id}")
async def v1_patch_node(account_id: str, payload: dict):
    try:
        node = await nodes.patch_node(scheduler, account_id, payload)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    if not node:
        raise HTTPException(404, "Node not found")
    return node


@app.post("/api/v1/nodes/{account_id}/start")
async def v1_start_node(account_id: str):
    result = await nodes.runtime_action(scheduler, account_id, "start")
    if not result:
        raise HTTPException(404, "Node not found")
    return result


@app.post("/api/v1/nodes/{account_id}/stop")
async def v1_stop_node(account_id: str):
    result = await nodes.runtime_action(scheduler, account_id, "stop")
    if not result:
        raise HTTPException(404, "Node not found")
    return result


@app.post("/api/v1/nodes/{account_id}/restart")
async def v1_restart_node(account_id: str):
    result = await nodes.runtime_action(scheduler, account_id, "restart")
    if not result:
        raise HTTPException(404, "Node not found")
    return result


@app.post("/api/v1/nodes/{account_id}/open-login")
async def v1_open_node_login(account_id: str):
    result = await nodes.open_login(scheduler, account_id)
    if not result:
        raise HTTPException(404, "Node not found")
    return result


@app.post("/api/v1/nodes/{account_id}/refresh-session")
async def v1_refresh_node_session(account_id: str):
    result = await nodes.refresh_session(scheduler, account_id)
    if not result:
        raise HTTPException(404, "Node not found")
    return result


@app.post("/api/v1/nodes/{account_id}/enable")
async def v1_enable_node(account_id: str):
    node = await nodes.set_node_enabled(scheduler, account_id, True)
    if not node:
        raise HTTPException(404, "Node not found")
    return node


@app.post("/api/v1/nodes/{account_id}/check-login-enable")
async def v1_check_login_enable_node(account_id: str):
    result = await nodes.check_login_and_enable(scheduler, account_id)
    if not result:
        raise HTTPException(404, "Node not found")
    return result


@app.post("/api/v1/nodes/{account_id}/disable")
async def v1_disable_node(account_id: str):
    node = await nodes.set_node_enabled(scheduler, account_id, False)
    if not node:
        raise HTTPException(404, "Node not found")
    return node


@app.delete("/api/v1/nodes/{account_id}")
async def v1_delete_node(account_id: str):
    result = await nodes.delete_node(scheduler, account_id)
    if not result:
        raise HTTPException(404, "Node not found")
    return result


@app.get("/api/v1/system/status")
async def v1_system_status():
    status = await scheduler.pool_status()
    return {
        **status,
        "gateway": await health(),
        "workers": scheduler.worker_snapshot.diagnostics(),
        "examples": task_center.examples(),
        "node_examples": nodes.examples(),
    }


@app.get("/api/v1/client/system/ready")
async def v1_client_ready():
    status = await scheduler.pool_status()
    worker_eligible = scheduler.worker_snapshot.eligible_count
    active_statuses = {
        "queued", "leased", "assigning", "project_create_pending", "project_create_in_progress",
        "project_creation_unknown", "project_created", "submit_pending", "submit_in_progress",
        "submission_unknown", "submitted", "processing", "download_pending", "downloading",
        "waiting_recovery",
    }
    tasks = await scheduler.list_tasks()
    active_task_counts = {}
    for task in tasks:
        task_status = task.get("status")
        if task_status in active_statuses:
            active_task_counts[task_status] = active_task_counts.get(task_status, 0) + 1
    return {
        "ready": True,
        "status": "ok",
        "api_version": "v1",
        "dry_run": settings.dry_run,
        "eligible_accounts": worker_eligible,
        "eligible_count": worker_eligible,
        "accounts_ready": status.get("accounts_ready", 0),
        "effective_max_concurrency": status.get("effective_max_concurrency"),
        "queued_count": status.get("queued_count"),
        "active_count": status.get("active_count"),
        "active_task_counts": active_task_counts,
    }


@app.post("/api/v1/client/tasks")
async def v1_client_create_task(payload: dict):
    return await v1_create_task(payload)


@app.get("/api/v1/client/tasks/{task_id}")
async def v1_client_task_detail(task_id: str):
    detail = await v1_task_detail(task_id)
    task = detail["task"]
    return {
        "task_id": task["task_id"],
        "idempotency_key": task.get("idempotency_key"),
        "status": task.get("status"),
        "created_at": task.get("created_at"),
        "updated_at": task.get("updated_at"),
        "completed_at": task.get("completed_at"),
        "video_ready": task.get("status") == "completed" and bool(task.get("video_path")),
        "error_code": task.get("error_code") or task.get("last_error_code"),
        "error_message": task.get("error_message") or task.get("last_error_message"),
        "input_media": detail.get("input_media", []),
    }


@app.post("/api/v1/client/tasks/{task_id}/cancel")
async def v1_client_cancel_task(task_id: str):
    return await v1_cancel_task(task_id)


@app.get("/api/v1/client/tasks/{task_id}/download")
async def v1_client_download_task(task_id: str):
    task = await scheduler.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    if task.get("status") != "completed":
        raise HTTPException(409, "Task is not completed")
    check = _downloadable_mp4(task.get("video_path"))
    if not check["ok"]:
        raise HTTPException(409, check["error"])
    filename = task.get("output_filename") or f"{task_id}.mp4"
    return FileResponse(check["path"], media_type="video/mp4", filename=Path(filename).name)


@app.post("/api/v1/client/files")
async def v1_upload_client_files(request: Request):
    form = await request.form()
    uploads = [item for key, item in form.multi_items() if key == "files" or key == "file"]
    if not uploads:
        raise HTTPException(400, "file or files multipart field is required")
    results = [await client_files.save_upload(scheduler.db, settings.db_path, upload) for upload in uploads]
    return {"files": [_public_upload_result(item) for item in results]}


@app.post("/api/v1/client/files/batch")
async def v1_upload_client_files_batch(request: Request):
    return await v1_upload_client_files(request)


def _public_upload_result(item: dict) -> dict:
    if not item.get("ok"):
        return {
            "ok": False,
            "original_filename": item.get("original_filename"),
            "error": item.get("error"),
        }
    return {
        "file_id": item["file_id"],
        "original_filename": item["original_filename"],
        "mime_type": item["mime_type"],
        "size_bytes": item["size_bytes"],
        "sha256": item["sha256"],
    }


def _client_api_protected(path: str) -> bool:
    return path.startswith("/api/v1/client/") and bool(settings.client_api_key or settings.admin_api_key)


def _admin_api_protected(path: str) -> bool:
    if not settings.admin_api_key:
        return False
    if path.startswith("/api/v1/client/") or path in {"/", "/health"}:
        return False
    return path.startswith("/api/")


def _request_api_key(request: Request) -> str:
    header = request.headers.get("x-api-key") or request.headers.get("authorization") or ""
    if header.lower().startswith("bearer "):
        return header[7:].strip()
    return header.strip()


def _require_client_key(request: Request) -> None:
    key = _request_api_key(request)
    allowed = {item for item in (settings.client_api_key, settings.admin_api_key) if item}
    if key not in allowed:
        raise HTTPException(401, "Invalid client API key")


def _require_admin_key(request: Request) -> None:
    if _request_api_key(request) != settings.admin_api_key:
        raise HTTPException(401, "Invalid admin API key")


def _downloadable_mp4(video_path: str | None) -> dict:
    if not video_path:
        return {"ok": False, "error": "completed task has no video_path"}
    path = Path(video_path)
    try:
        resolved = path.resolve()
        output_root = settings.output_root.resolve()
        if output_root not in resolved.parents and resolved != output_root:
            return {"ok": False, "error": "video_path is outside allowed output directory"}
        if not resolved.exists() or not resolved.is_file():
            return {"ok": False, "error": "video file not found"}
        if resolved.suffix.lower() != ".mp4":
            return {"ok": False, "error": "video file is not mp4"}
        if resolved.stat().st_size < 8:
            return {"ok": False, "error": "video file is too small"}
        with resolved.open("rb") as fh:
            header = fh.read(8)
        if len(header) < 8 or header[4:8] != b"ftyp":
            return {"ok": False, "error": "video file is not a valid MP4"}
        return {"ok": True, "path": str(resolved)}
    except OSError as exc:
        return {"ok": False, "error": str(exc)[:200]}


@app.get("/api/v1/docs/examples")
async def v1_examples():
    return {**task_center.examples(), "nodes": nodes.examples()}


TASK_CENTER_HTML = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Flow Gateway</title>
  <style>
    *{box-sizing:border-box}
    body{font-family:"Segoe UI","Microsoft YaHei",Arial,sans-serif;margin:0;background:#fff;color:#18212b;font-size:14px}
    main{width:min(1180px,calc(100% - 48px));margin:0 auto;padding:28px 0 48px}
    header{display:flex;align-items:center;justify-content:space-between;margin-bottom:22px}
    h1{font-size:22px;font-weight:650;margin:0} h2{font-size:17px;margin:0}
    section{border-top:1px solid #e8ebef;padding:22px 0}
    .section-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
    .muted{color:#768291}
    .status-grid{display:grid;grid-template-columns:1.4fr repeat(4,1fr);gap:10px}
    .metric{background:#f7f8fa;border-radius:9px;padding:12px 14px;color:#687483}
    .metric b{display:block;color:#18212b;font-size:20px;font-weight:650;margin-top:5px}
    .dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:8px;background:#1eae68}
    .dot.warn{background:#e58a19}.dot.danger{background:#d94b4b}
    .simple-list{border:1px solid #e8ebef;border-radius:10px;overflow:hidden}
    .list-row{display:grid;grid-template-columns:minmax(150px,1.3fr) minmax(170px,1fr) minmax(90px,.7fr) minmax(100px,auto);align-items:center;gap:18px;min-height:48px;padding:8px 14px;border-bottom:1px solid #eef0f3}
    .list-row:last-child{border-bottom:0}.list-head{min-height:38px;background:#fafbfc;color:#768291;font-size:12px}
    .task-row{grid-template-columns:minmax(180px,1.5fr) minmax(130px,.8fr) minmax(130px,.8fr) minmax(110px,auto)}
    .state{display:inline-flex;align-items:center}.credits{font-variant-numeric:tabular-nums}
    .empty{padding:24px;text-align:center;color:#8a95a3}
    input,select,textarea,button{font:inherit}
    input,select,textarea{border:1px solid #cfd5dc;border-radius:7px;padding:8px 10px;background:#fff}
    textarea{width:100%;min-height:120px;margin-top:10px}
    button,.link-button{border:1px solid #c9d0d8;background:#fff;color:#283746;border-radius:7px;padding:6px 11px;cursor:pointer;text-decoration:none;display:inline-block}
    button:hover,.link-button:hover{background:#f5f7f9}
    button.primary{background:#1769d2;color:#fff;border-color:#1769d2}
    button.danger{color:#b53a3a}
    .row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
    .add-row{margin-top:12px}.add-panel{display:none;margin-top:10px}.add-panel.open{display:flex}
    .error{color:#b53a3a}.ok{color:#118451}pre{white-space:pre-wrap;word-break:break-word}
    details.advanced{border-top:1px solid #e8ebef;padding:18px 0}
    details.advanced>summary{cursor:pointer;font-weight:600;list-style:none;color:#4f5c69}
    details.advanced>summary::before{content:'›';display:inline-block;margin-right:8px;transition:transform .15s}
    details.advanced[open]>summary::before{transform:rotate(90deg)}
    .advanced-body{display:grid;gap:18px;margin-top:16px}.advanced-card{background:#f8f9fb;border-radius:9px;padding:14px;overflow:auto}
    .advanced-card h3{font-size:14px;margin:0 0 10px}
    table{width:100%;border-collapse:collapse;font-size:12px;min-width:900px}
    th,td{border-bottom:1px solid #e4e8ed;padding:7px;text-align:left;vertical-align:top}th{color:#667382}
    code{background:#edf1f5;padding:1px 4px;border-radius:4px}
    @media(max-width:760px){main{width:calc(100% - 28px)}.status-grid{grid-template-columns:1fr 1fr}.status-grid .metric:first-child{grid-column:1/-1}.list-row,.task-row{grid-template-columns:1fr 1fr;gap:8px}.list-head{display:none}}
  </style>
</head>
<body>
<main>
  <header><h1>Flow Gateway</h1><span class="muted">自动刷新</span></header>
  <section>
    <div class="status-grid" id="metrics"></div>
  </section>
  <section>
    <div class="section-head"><h2>账号</h2><span class="muted" id="accountSummary"></span></div>
    <div class="simple-list" id="accountList"></div>
    <div class="add-row"><button type="button" onclick="toggleQuickAdd()">＋ 添加账号</button></div>
    <div class="row add-panel" id="quickAddPanel">
      <input id="quickFlowNumber" placeholder="账号编号，例如 009" inputmode="numeric">
      <button id="quickAddButton" type="button" class="primary" onclick="quickCreateLogin()">创建并打开登录窗口</button>
    </div>
    <pre id="quickNodePreview"></pre><pre id="nodeResult"></pre>
  </section>
  <section>
    <div class="section-head"><h2>最近任务</h2><button type="button" id="allTasksButton" onclick="toggleAllTasks()">查看全部任务</button></div>
    <div class="simple-list" id="taskList"></div>
  </section>
  <details class="advanced">
    <summary>高级设置</summary>
    <div class="advanced-body">
      <div class="advanced-card"><h3>账号控制</h3><table id="accounts"></table></div>
      <div class="advanced-card"><h3>节点与诊断</h3><table id="nodesTable"></table></div>
      <div class="advanced-card">
        <h3>节点配置</h3>
        <div class="row">
          <input id="nodeAccountId" placeholder="账号编号"><input id="nodeDisplayName" placeholder="显示名称">
          <input id="nodeWorkerHost" placeholder="Worker 地址" value="127.0.0.1"><input id="nodeWorkerPort" placeholder="Worker 端口">
          <input id="nodeCdpHost" placeholder="CDP 地址" value="127.0.0.1"><input id="nodeCdpPort" placeholder="CDP 端口">
          <label><input id="nodeEnabled" type="checkbox"> 启用</label>
          <button id="addNodeButton" type="button" class="primary" onclick="addNode()">添加节点</button>
          <button onclick="quickPreviewNode()">预览快速添加</button><button onclick="loadNodeExample()">加载节点示例</button><button onclick="importNodes()">导入节点</button>
        </div>
        <textarea id="nodeImportContent" placeholder="CSV 或 JSON 节点配置"></textarea>
      </div>
      <div class="advanced-card">
        <h3>任务管理</h3>
        <div class="row"><input id="filterStatus" placeholder="任务状态"><input id="filterBatch" placeholder="批次编号"><input id="filterAccount" placeholder="账号编号"><button onclick="loadTasks()">刷新</button><button onclick="exportTasks()">导出</button></div>
        <table id="tasks"></table>
      </div>
      <div class="advanced-card">
        <h3>批量导入</h3>
        <div class="row"><select id="importFormat"><option value="csv">CSV</option><option value="json">JSON</option></select><input id="importFile" type="file" accept=".csv,.json" onchange="loadImportFile()"><button onclick="loadExample()">加载示例</button><button class="primary" onclick="importTasks()">导入任务</button></div>
        <textarea id="importContent"></textarea><pre id="importResult"></pre>
      </div>
    </div>
  </details>
</main>
<script>
async function api(path, options){const r=await fetch(path, options); if(!r.ok) throw new Error(await r.text()); return await r.json();}
function td(v){return `<td>${v??''}</td>`}
function esc(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
let accountRows=[],nodeRows=[],taskRows=[],showAllTasks=false;
async function refresh(){
  try{
    const [s,accountsData,tasksData,nodesData]=await Promise.all([api('/api/v1/system/status'),api('/api/v1/accounts'),api('/api/v1/tasks'),api('/api/v1/nodes')]);
    accountRows=accountsData;taskRows=tasksData;nodeRows=nodesData;
    renderAccounts();renderTasks();renderAdvanced();renderMetrics(s);
  }catch(e){metrics.innerHTML=`<div class="metric"><span class="dot danger"></span>Gateway<b>异常</b></div>`;console.error(e)}
}
function mergedAccounts(){
  const merged=new Map();accountRows.forEach(a=>merged.set(a.account_id,{account:a,node:null}));nodeRows.forEach(n=>merged.set(n.account_id,{account:merged.get(n.account_id)?.account||null,node:n}));return [...merged.entries()].sort((a,b)=>a[0].localeCompare(b[0]));
}
function cooldownRemaining(value){const end=Date.parse(value);if(!value||Number.isNaN(end)||end<=Date.now())return '';const sec=Math.ceil((end-Date.now())/1000);return sec<60?`${sec} 秒`:`${Math.ceil(sec/60)} 分钟`}
function accountState(account,node){
  const cooldown=cooldownRemaining(account?.cooldown_until||node?.cooldown_until);if(cooldown)return {key:'cooldown',label:`冷却中 · 剩余 ${cooldown}`,tone:'warn'};
  const status=String(account?.status||'').toLowerCase(), worker=String(node?.worker_status||'').toLowerCase(), oauth=String(node?.oauth_status||'').toLowerCase();
  if(node&&!node.worker_online)return {key:'offline',label:'本地服务未启动',tone:'danger'};
  if(status==='offline')return {key:'offline',label:'离线',tone:'danger'};
  if(node?.worker_online&&!node?.extension_connected)return {key:'extension',label:'浏览器扩展未连接',tone:'warn'};
  if(node?.extension_connected&&node?.runtime?.account_match===false)return {key:'mismatch',label:'登录账号不匹配',tone:'danger'};
  if(node?.extension_connected&&node?.runtime?.account_match&&(account?.quota_confidence==='stale'||oauth.includes('credits_http_')))return {key:'quota',label:'登录正常，额度验证失败',tone:'warn'};
  if(status==='needs_login')return {key:'login',label:'需要登录',tone:'warn'};
  if(oauth.includes('oauth')||oauth.includes('login')||node?.extension_status==='extension_missing')return {key:'login',label:'需要登录',tone:'warn'};
  if(node&&oauth!=='live'&&oauth!=='unknown'&&oauth)return {key:'oauth',label:'OAuth 异常',tone:'danger'};
  if(status==='ready'||status==='busy')return {key:'ready',label:status==='busy'?'生成中':'正常',tone:''};
  return {key:'other',label:status||worker||'未知',tone:'warn'};
}
function accountAction(id,state){if(state.key==='login'||state.key==='oauth'||state.key==='mismatch')return `<button onclick="openNodeLogin('${esc(id)}',this)">${state.key==='login'?'登录':'重新登录'}</button>`;if(state.key==='extension')return `<button onclick="openNodeLogin('${esc(id)}',this)">重新连接</button>`;if(state.key==='quota')return `<button onclick="refreshNodeSession('${esc(id)}')">重新验证</button>`;if(state.key==='offline')return `<button onclick="startNode('${esc(id)}')">启动</button>`;return ''}
function renderAccounts(){
  const rows=mergedAccounts();accountSummary.textContent=`${rows.length} 个账号`;
  accountList.innerHTML='<div class="list-row list-head"><span>账号</span><span>状态</span><span>积分</span><span></span></div>'+(rows.length?rows.map(([id,v])=>{const state=accountState(v.account,v.node),credits=v.account?.credits??v.node?.credits??'—';return `<div class="list-row"><strong>${esc(id)}</strong><span class="state"><i class="dot ${state.tone}"></i>${esc(state.label)}</span><span class="credits">${esc(credits)}</span><span>${accountAction(id,state)}</span></div>`}).join(''):'<div class="empty">暂无账号</div>');
}
function renderMetrics(s){const rows=mergedAccounts(),bad=rows.filter(([,v])=>accountState(v.account,v.node).key!=='ready').length,total=rows.length,gatewayOk=s.gateway?.status==='ok';metrics.innerHTML=`<div class="metric"><span class="dot ${gatewayOk?'':'danger'}"></span>Gateway<b>${gatewayOk?'正常':'异常'}</b></div><div class="metric">可用账号<b>${s.accounts_ready||0} / ${total}</b></div><div class="metric">当前生成<b>${s.active_count||0}</b></div><div class="metric">排队<b>${s.queued_count||0}</b></div><div class="metric">异常账号<b>${bad}</b></div>`}
function taskState(t){const map={completed:'已完成',failed:'失败',queued:'排队中',processing:'生成中',downloading:'下载中',cancelled:'已取消'};return map[t.status]||t.status||'未知'}
function zhStatus(value){const map={ready:'正常',busy:'生成中',offline:'离线',needs_login:'需要登录',low_credits:'积分不足',paused:'已暂停',cooldown:'冷却中',completed:'已完成',failed:'失败',queued:'排队中',processing:'生成中',downloading:'下载中',cancelled:'已取消',live:'正常',unknown:'未知',extension_ready:'扩展已就绪',extension_missing:'缺少扩展'};return map[String(value??'').toLowerCase()]??value??''}
function zhBool(value){return value===true||value===1?'是':value===false||value===0?'否':value??''}
function taskAction(t){if(t.status==='completed'&&t.video_path)return `<a class="link-button" target="_blank" href="/api/v1/client/tasks/${encodeURIComponent(t.task_id)}/download">打开视频</a>`;if(t.status==='failed'||t.error_code||t.last_error_category)return `<button onclick="showTaskReason('${esc(t.task_id)}')">查看原因</button>`;return ''}
function renderTasks(){const newest=[...taskRows].reverse(),rows=showAllTasks?newest:newest.slice(0,8);taskList.innerHTML='<div class="list-row task-row list-head"><span>任务</span><span>状态</span><span>账号</span><span></span></div>'+(rows.length?rows.map(t=>`<div class="list-row task-row"><strong>${esc(t.name||t.external_task_id||'未命名任务')}</strong><span>${esc(taskState(t))}</span><span>${esc(t.assigned_account_id||t.account_id||'—')}</span><span>${taskAction(t)}</span></div>`).join(''):'<div class="empty">暂无任务</div>');allTasksButton.textContent=showAllTasks?'只看最近任务':'查看全部任务'}
function toggleAllTasks(){showAllTasks=!showAllTasks;renderTasks()}
function toggleQuickAdd(){quickAddPanel.classList.toggle('open');if(quickAddPanel.classList.contains('open'))quickFlowNumber.focus()}
async function showTaskReason(id){const data=await api(`/api/v1/tasks/${id}`),t=data.task||data;alert(t.error_message||t.last_error_message||t.error_code||t.last_error_category||'没有可用的失败原因')}
function renderAdvanced(){
  accounts.innerHTML='<tr><th>账号</th><th>状态</th><th>积分</th><th>预留积分</th><th>健康度</th><th>当前任务</th><th>冷却截止</th><th>权重</th><th>操作</th></tr>'+accountRows.map(a=>`<tr>${td(a.account_id)}${td(zhStatus(a.status))}${td(a.credits)}${td(a.reserved_credits)}${td(a.health_score)}${td(a.current_task_id)}${td(a.cooldown_until)}${td(a.account_weight)}<td><button onclick="pauseAccount('${a.account_id}')">暂停</button> <button onclick="resumeAccount('${a.account_id}')">恢复</button> <button onclick="cooldownAccount('${a.account_id}')">设置冷却</button> <button onclick="clearCooldown('${a.account_id}')">清除冷却</button> <button onclick="setWeight('${a.account_id}')">设置权重</button> <button onclick="setCredits('${a.account_id}')">设置积分</button></td></tr>`).join('');
  nodesTable.innerHTML='<tr><th>账号</th><th>Worker</th><th>CDP/WS</th><th>进程</th><th>运行状态</th><th>OAuth</th><th>账号匹配 / 所有权</th><th>额度可信度</th><th>已启用</th><th>操作</th></tr>'+nodeRows.map(n=>`<tr>${td(n.account_id)}${td(`${n.worker_host}:${n.worker_port}`)}${td(`${n.cdp_host}:${n.cdp_port}<br>WS ${n.extension_ws_port||''}`)}${td(`Worker ${n.worker_pid||''}<br>Chrome ${n.chrome_pid||''}`)}${td(zhStatus(n.worker_status))}${td(zhStatus(n.oauth_status))}${td(`${zhBool(n.runtime?.account_match)}<br>${zhStatus(n.runtime?.ownership_status||n.runtime?.worker_ownership_verified)}`)}${td(zhStatus(n.quota_confidence))}${td(zhBool(n.enabled))}<td><button onclick="startNode('${n.account_id}')">启动</button> <button onclick="stopNode('${n.account_id}')">停止</button> <button onclick="restartNode('${n.account_id}')">重启</button> <button onclick="refreshNodeSession('${n.account_id}')">刷新登录</button> <button onclick="checkLoginEnable('${n.account_id}')">检查登录并启用</button> <button onclick="enableNode('${n.account_id}')">启用</button> <button onclick="disableNode('${n.account_id}')">停用</button> <button onclick="editNode('${n.account_id}',${n.worker_port},${n.cdp_port})">编辑端口</button> <button onclick="nodeDetail('${n.account_id}')">诊断</button></td></tr>`).join('');
  renderAdvancedTasks();
}
function renderAdvancedTasks(){tasks.innerHTML='<tr><th>任务编号</th><th>外部编号</th><th>批次</th><th>状态</th><th>账号</th><th>Worker 任务</th><th>项目</th><th>生成次数</th><th>下载次数</th><th>输出</th><th>错误</th><th>操作</th></tr>'+taskRows.map(t=>`<tr>${td(`<code>${t.task_id}</code>`)}${td(t.external_task_id)}${td(t.batch_id)}${td(zhStatus(t.status))}${td(t.assigned_account_id||t.account_id)}${td(t.worker_job_id)}${td(t.project_id)}${td(t.generation_attempts)}${td(t.download_attempts)}${td(t.video_path||((t.output_directory||'')+'/'+(t.output_filename||'')))}${td(t.error_code||t.last_error_category||'')}<td><button onclick="detail('${t.task_id}')">详情</button> <button onclick="pauseTask('${t.task_id}')">暂停</button> <button onclick="resumeTask('${t.task_id}')">恢复</button> <button onclick="cancelTask('${t.task_id}')">取消</button> <button onclick="priorityTask('${t.task_id}')">优先级</button> <button onclick="requeueTask('${t.task_id}')">重新排队</button> <button onclick="retryDownload('${t.task_id}')">重试下载</button> <button onclick="reconcileTask('${t.task_id}')">校准</button> <button onclick="manualTask('${t.task_id}')">转人工</button></td></tr>`).join('')}
async function loadAccounts(){accountRows=await api('/api/v1/accounts');renderAccounts();renderAdvanced()}
async function loadTasks(){
  const q=new URLSearchParams(); if(filterStatus.value) q.set('status',filterStatus.value); if(filterBatch.value) q.set('batch_id',filterBatch.value); if(filterAccount.value) q.set('account_id',filterAccount.value);
  taskRows=await api('/api/v1/tasks?'+q.toString());renderTasks();renderAdvancedTasks();
}
async function loadExample(){const e=await api('/api/v1/docs/examples'); importContent.value=importFormat.value==='csv'?e.csv:JSON.stringify(e.json,null,2);}
async function loadImportFile(){const f=importFile.files[0]; if(!f) return; importContent.value=await f.text(); importFormat.value=f.name.toLowerCase().endsWith('.json')?'json':'csv';}
async function importTasks(){try{const format=importFormat.value; const content=importContent.value; importResult.textContent=JSON.stringify(await api('/api/v1/tasks/import',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({format,content})}),null,2); await refresh();}catch(e){importResult.textContent=e.message}}
async function pauseAccount(id){await api(`/api/pool/accounts/${id}/pause`,{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); await refresh()}
async function resumeAccount(id){await api(`/api/pool/accounts/${id}/resume`,{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); await refresh()}
async function cooldownAccount(id){const seconds=prompt('冷却秒数','300'); if(!seconds) return; await api(`/api/pool/accounts/${id}/cooldown`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({seconds:Number(seconds),reason:'task_center'})}); await refresh()}
async function clearCooldown(id){await api(`/api/pool/accounts/${id}/cooldown/clear`,{method:'POST'}); await refresh()}
async function setWeight(id){const weight=prompt('账号权重','1'); if(!weight) return; await api(`/api/pool/accounts/${id}/weight`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({account_weight:Number(weight)})}); await refresh()}
async function setCredits(id){const credits=prompt('账号积分'); if(!credits) return; await api(`/api/pool/accounts/${id}/credits`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({credits:Number(credits),source:'task_center'})}); await refresh()}
async function pauseTask(id){await api(`/api/v1/tasks/${id}/pause`,{method:'POST'}); await loadTasks()}
async function resumeTask(id){await api(`/api/v1/tasks/${id}/resume`,{method:'POST'}); await loadTasks()}
async function cancelTask(id){await api(`/api/v1/tasks/${id}/cancel`,{method:'POST'}); await loadTasks()}
async function priorityTask(id){const priority=prompt('任务优先级'); if(priority===null) return; await api(`/api/v1/tasks/${id}/priority`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({priority:Number(priority)})}); await loadTasks()}
async function requeueTask(id){await api(`/api/v1/tasks/${id}/requeue`,{method:'POST'}); await loadTasks()}
async function retryDownload(id){alert(JSON.stringify(await api(`/api/v1/tasks/${id}/retry-download`,{method:'POST'}),null,2))}
async function reconcileTask(id){alert(JSON.stringify(await api(`/api/v1/tasks/${id}/reconcile`,{method:'POST'}),null,2))}
async function manualTask(id){await api(`/api/v1/tasks/${id}/need-manual`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({error_code:'NEED_MANUAL',error_message:'Marked from task center'})}); await loadTasks()}
async function detail(id){alert(JSON.stringify(await api(`/api/v1/tasks/${id}`),null,2))}
async function exportTasks(){const rows=await api('/api/v1/tasks'); const blob=new Blob([JSON.stringify(rows,null,2)],{type:'application/json'}); const a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download='flow-gateway-tasks.json'; a.click();}
async function loadNodes(){
  nodeRows=await api('/api/v1/nodes');renderAccounts();renderAdvanced();
}
function showNodeResult(message, isError){
  nodeResult.className=isError?'error':'ok';
  nodeResult.textContent=message;
}
function quickPayload(){
  const base={flow_account_number:quickFlowNumber.value.trim()};
  if(nodeWorkerPort.value) base.worker_port=Number(nodeWorkerPort.value);
  if(nodeCdpPort.value) base.cdp_port=Number(nodeCdpPort.value);
  if(nodeDisplayName.value) base.display_name=nodeDisplayName.value.trim();
  return base;
}
async function quickPreviewNode(){
  try{
    const data=await api('/api/v1/nodes/quick-preview',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(quickPayload())});
    quickNodePreview.className=data.ok?'ok':'error';
    quickNodePreview.textContent=JSON.stringify(data,null,2);
    return data;
  }catch(e){
    quickNodePreview.className='error';
    quickNodePreview.textContent=e.message;
  }
}
async function quickCreateLogin(){
  const button=document.getElementById('quickAddButton');
  button.disabled=true;
  const oldText=button.textContent;
  button.textContent='正在创建…';
  showNodeResult('正在创建账号并打开登录窗口…', false);
  try{
    const r=await fetch('/api/v1/nodes/quick-create-login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(quickPayload())});
    const text=await r.text();
    let data; try{data=JSON.parse(text)}catch(_){data={raw:text}}
    quickNodePreview.className=(r.ok&&data.ok)?'ok':'error';
    quickNodePreview.textContent=JSON.stringify(data,null,2);
    if(!r.ok || !data.ok){
      showNodeResult(`快速添加失败：HTTP ${r.status}\n${JSON.stringify(data,null,2)}`, true); // Quick Add failed: HTTP
      return;
    }
    showNodeResult(`${data.preview.account_id}：等待手动登录 Google/Flow`, false);
    await refresh();
  }catch(e){
    showNodeResult(`快速添加失败，服务未返回结果\n${e.message}`, true); // Quick Add failed before response
  }finally{
    button.disabled=false;
    button.textContent=oldText;
  }
}
async function addNode(){
  const button=document.getElementById('addNodeButton');
  const payload={
    account_id:nodeAccountId.value.trim(),
    display_name:(nodeDisplayName.value||nodeAccountId.value).trim(),
    worker_host:(nodeWorkerHost.value||'127.0.0.1').trim(),
    worker_port:Number(nodeWorkerPort.value),
    cdp_host:(nodeCdpHost.value||'127.0.0.1').trim(),
    cdp_port:Number(nodeCdpPort.value),
    enabled:nodeEnabled.checked
  };
  if(!payload.account_id || !payload.worker_port || !payload.cdp_port){
    showNodeResult('请填写账号编号、Worker 端口和 CDP 端口', true);
    return;
  }
  button.disabled=true;
  const oldText=button.textContent;
  button.textContent='正在添加…';
  showNodeResult('正在提交节点…', false);
  try{
    const r=await fetch('/api/v1/nodes',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    const text=await r.text();
    let data; try{data=JSON.parse(text)}catch(_){data={raw:text}}
    if(!r.ok){
      showNodeResult(`添加节点失败：HTTP ${r.status}\n${JSON.stringify(data,null,2)}`, true); // POST /api/v1/nodes failed
      return;
    }
    if(data.result==='duplicate'){
      showNodeResult(`账号 ${payload.account_id} 已存在\n${JSON.stringify(data,null,2)}`, true); // POST /api/v1/nodes returned duplicate
    }else{
      showNodeResult(`节点添加成功：${payload.account_id}\n${JSON.stringify(data,null,2)}`, false); // POST /api/v1/nodes succeeded
      nodeAccountId.value='';
      nodeDisplayName.value='';
      nodeWorkerPort.value='';
      nodeCdpPort.value='';
      nodeEnabled.checked=false;
    }
    await loadNodes();
  }catch(e){
    showNodeResult(`添加节点失败，服务未返回结果\n${e.message}`, true); // POST /api/v1/nodes failed before response
  }finally{
    button.disabled=false;
    button.textContent=oldText;
  }
}
async function loadNodeExample(){const e=await api('/api/v1/docs/examples'); nodeImportContent.value=e.nodes.csv;}
async function importNodes(){
  const content=nodeImportContent.value;
  const format=content.trim().startsWith('{')||content.trim().startsWith('[')?'json':'csv';
  nodeResult.textContent=JSON.stringify(await api('/api/v1/nodes/import',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({format,content})}),null,2);
  await loadNodes();
}
async function startNode(id){nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}/start`,{method:'POST'}),null,2); await refresh()}
async function stopNode(id){nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}/stop`,{method:'POST'}),null,2); await refresh()}
async function restartNode(id){nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}/restart`,{method:'POST'}),null,2); await refresh()}
async function openNodeLogin(id,button){const oldText=button.textContent;button.disabled=true;button.textContent='正在打开…';nodeResult.className='muted';nodeResult.textContent=`正在打开 ${id} 的登录窗口…`;try{const data=await api(`/api/v1/nodes/${id}/open-login`,{method:'POST'});nodeResult.className=data.ok?'ok':'error';nodeResult.textContent=data.ok?`${id} 的登录窗口已打开`:`${id} 的登录窗口打开失败`;await refresh()}catch(e){nodeResult.className='error';nodeResult.textContent=`打开登录窗口失败：${e.message}`}finally{button.disabled=false;button.textContent=oldText}}
async function refreshNodeSession(id){nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}/refresh-session`,{method:'POST'}),null,2); await refresh()}
async function checkLoginEnable(id){nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}/check-login-enable`,{method:'POST'}),null,2); await refresh()}
async function enableNode(id){nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}/enable`,{method:'POST'}),null,2); await refresh()}
async function disableNode(id){nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}/disable`,{method:'POST'}),null,2); await refresh()}
async function editNode(id,oldWorker,oldCdp){const worker=prompt('Worker 端口',oldWorker); if(!worker) return; const cdp=prompt('CDP 端口',oldCdp); if(!cdp) return; nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}`,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({worker_port:Number(worker),cdp_port:Number(cdp)})}),null,2); await refresh()}
async function nodeDetail(id){nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}`),null,2)}
refresh();
setInterval(refresh,15000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    logger.info("gateway_audit %s", scheduler.safe_json({
        "event": "gateway_server_entry",
        "python_executable": sys.executable,
        "gateway_port": settings.api_port,
        "database_path": str(settings.db_path),
    }))
    uvicorn.run(app, host=settings.api_host, port=settings.api_port, reload=False)
