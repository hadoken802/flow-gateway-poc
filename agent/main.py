"""Flow Kit — FastAPI + WebSocket server entry point."""
import asyncio
import json
import logging
import signal
from contextlib import asynccontextmanager

import websockets
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from agent.config import FLOW_ACCOUNT_ID, API_HOST, API_PORT, WS_HOST, WS_PORT, EXTENSION_WS_MAX_SIZE_BYTES, DB_PATH, OUTPUT_DIR
from agent.db.schema import init_db, close_db
from agent.api.characters import router as characters_router
from agent.api.projects import router as projects_router
from agent.api.videos import router as videos_router
from agent.api.scenes import router as scenes_router
from agent.api.requests import router as requests_router
from agent.api.flow import router as flow_router
from agent.api.reviews import router as reviews_router
from agent.api.tts import router as tts_router
from agent.api.materials import router as materials_router
from agent.api.music import router as music_router
from agent.api.models import router as models_router
from agent.api.active_project import router as active_project_router
from agent.api.omni_test import router as omni_test_router, recover_omni_jobs, shutdown_omni_jobs
from agent.worker.processor import get_worker_controller
from agent.services.flow_client import get_flow_client
from agent.services.event_bus import event_bus
from agent.sdk import init_sdk

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


# ─── WebSocket Server for Extension ─────────────────────────

async def ws_handler(websocket):
    """Handle a Chrome extension WebSocket connection."""
    client = get_flow_client()
    logger.info("Extension connected from %s", websocket.remote_address)
    registered = False

    try:
        async for raw in websocket:
            try:
                data = json.loads(raw)
                if data.get("type") == "keepalive":
                    await websocket.send(json.dumps({"type": "keepalive_ack", "at": data.get("at")}))
                    continue
                if data.get("type") == "bootstrap_diagnostic":
                    ok = client.record_bootstrap_diagnostic(data, FLOW_ACCOUNT_ID)
                    await websocket.send(json.dumps({"type": "bootstrap_diagnostic_ack", "ok": ok}))
                    continue
                if not registered:
                    registered = await register_extension(client, data, websocket)
                    if not registered:
                        await websocket.close(code=4003, reason="Account mismatch")
                        return
                    await websocket.send(json.dumps({"type": "callback_secret", "secret": _CALLBACK_SECRET}))
                    continue
                await client.handle_message(data)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON from extension")
            except Exception as e:
                logger.exception("Error handling extension message: %s", e)
    except websockets.ConnectionClosed as exc:
        logger.warning(
            "Extension websocket closed code=%s reason=%s",
            getattr(exc, "code", None),
            str(getattr(exc, "reason", ""))[:200],
        )
        if getattr(exc, "code", None) == 1009:
            client.note_ws_error("websocket_message_too_large")
    finally:
        if registered:
            close_code = getattr(websocket, "close_code", None)
            close_reason = getattr(websocket, "close_reason", None)
            client.clear_extension(close_code=close_code, close_reason=close_reason)
        logger.info("Extension disconnected code=%s reason=%s", getattr(websocket, "close_code", None), str(getattr(websocket, "close_reason", ""))[:200])


async def register_extension(client, data: dict, websocket=None) -> bool:
    """Register an extension connection after account_id handshake."""
    if data.get("type") != "register":
        logger.warning("Extension rejected before register for account=%s", FLOW_ACCOUNT_ID)
        return False
    account_id = str(data.get("account_id") or "")
    if account_id != FLOW_ACCOUNT_ID:
        logger.warning("Extension account mismatch expected=%s got=%s", FLOW_ACCOUNT_ID, account_id)
        return False
    client.set_extension(websocket, registered=False)
    logger.info("Extension registered account=%s profile=%s", account_id, data.get("profile_id") or "")
    return True


async def run_ws_server():
    """Run WebSocket server for extension connections."""
    async with websockets.serve(
        ws_handler,
        WS_HOST,
        WS_PORT,
        max_size=EXTENSION_WS_MAX_SIZE_BYTES,
        max_queue=4,
    ):
        logger.info("WebSocket server listening on ws://%s:%d max_size=%d", WS_HOST, WS_PORT, EXTENSION_WS_MAX_SIZE_BYTES)
        await asyncio.Future()  # run forever


# ─── FastAPI App ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()

    # Load custom materials from DB into in-memory registry
    from agent.db.crud import list_materials as db_list_materials
    from agent.materials import register_material, _BUILTIN_IDS
    try:
        custom_materials = await db_list_materials()
        for m in custom_materials:
            if m["id"] not in _BUILTIN_IDS:
                register_material(m)
                logger.info("Loaded custom material from DB: %s", m["id"])
    except Exception as e:
        logger.warning("Failed to load custom materials: %s", e)

    ops = init_sdk(get_flow_client())
    logger.info("SDK initialized (OperationService ready)")
    logger.info(
        "Flow Kit starting account=%s api=%s:%d ws=%s:%d db=%s output=%s",
        FLOW_ACCOUNT_ID, API_HOST, API_PORT, WS_HOST, WS_PORT, DB_PATH, OUTPUT_DIR
    )

    controller = get_worker_controller()

    # SIGTERM handler for graceful shutdown
    loop = asyncio.get_event_loop()
    try:
     loop.add_signal_handler(signal.SIGTERM, controller.request_shutdown)
    except NotImplementedError:
     logger.info(
        "SIGTERM signal handler is not supported on Windows; "
        "using normal application shutdown handling"
    )

    # Start background tasks
    ws_task = asyncio.create_task(run_ws_server())
    worker_task = asyncio.create_task(controller.start())
    await recover_omni_jobs()
    logger.info("WS server + worker started")

    try:
        yield
    finally:
        controller.request_shutdown()
        await shutdown_omni_jobs()
        await controller.drain()
        ws_task.cancel()
        worker_task.cancel()
        await asyncio.gather(ws_task, worker_task, return_exceptions=True)
        await get_flow_client().shutdown()
        await close_db()
        logger.info("Flow Kit stopped")


app = FastAPI(title="Flow Kit", version="1.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(characters_router, prefix="/api")
app.include_router(projects_router, prefix="/api")
app.include_router(videos_router, prefix="/api")
app.include_router(scenes_router, prefix="/api")
app.include_router(requests_router, prefix="/api")
app.include_router(flow_router, prefix="/api")
app.include_router(reviews_router, prefix="/api")
app.include_router(tts_router, prefix="/api")
app.include_router(materials_router, prefix="/api")
app.include_router(music_router, prefix="/api")
app.include_router(models_router)
app.include_router(active_project_router)
app.include_router(omni_test_router, prefix="/api")


import secrets as _secrets
_CALLBACK_SECRET = _secrets.token_urlsafe(32)


@app.post("/api/ext/callback")
async def ext_callback(request: Request):
    """HTTP callback for extension to deliver API responses.

    Replaces ws.send() for response delivery — immune to WS disconnect.
    Extension POSTs {id, status, data, error} here instead of sending via WS.
    Requires X-Callback-Secret header matching the secret sent to extension on WS connect.
    """
    data = await request.json()
    client = get_flow_client()
    req_id = data.get("id")
    logger.info("ext/callback: id=%s pending=%d match=%s",
                str(req_id)[:8] if req_id else "none",
                len(client._pending),
                "yes" if req_id and req_id in client._pending else "no")
    if req_id and req_id in client._pending:
        future = client._pending[req_id]
        try:
            future.set_result(data)
        except asyncio.InvalidStateError:
            pass
        return {"ok": True}
    return {"ok": False, "reason": "no matching pending request"}


@app.post("/api/ext/bootstrap-diagnostic")
async def ext_bootstrap_diagnostic(request: Request):
    raw = await request.body()
    if len(raw) > 2048:
        return {"ok": False, "reason": "payload_too_large"}
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        return {"ok": False, "reason": "invalid_json"}
    client = get_flow_client()
    return {"ok": client.record_bootstrap_diagnostic(data, FLOW_ACCOUNT_ID)}


@app.get("/health")
async def health():
    client = get_flow_client()
    return {
        "status": "ok",
        "version": "0.2.0",
        "account_id": FLOW_ACCOUNT_ID,
        "api_port": API_PORT,
        "ws_port": WS_PORT,
        "ws_max_size_bytes": EXTENSION_WS_MAX_SIZE_BYTES,
        "extension_connected": client.connected,
        "ws": client.ws_stats,
        "bootstrap_diagnostics": client.bootstrap_diagnostics,
    }


@app.get("/api/worker/info")
async def worker_info():
    client = get_flow_client()
    return {
        "account_id": FLOW_ACCOUNT_ID,
        "api_host": API_HOST,
        "api_port": API_PORT,
        "ws_host": WS_HOST,
        "ws_port": WS_PORT,
        "ws_max_size_bytes": EXTENSION_WS_MAX_SIZE_BYTES,
        "database_path": str(DB_PATH),
        "output_dir": str(OUTPUT_DIR),
        "extension_connected": client.connected,
        "flow_key_present": bool(client._flow_key),
    }


# ─── Dashboard WebSocket ──────────────────────────────────────

@app.websocket("/ws/dashboard")
async def dashboard_ws(websocket: WebSocket):
    """WebSocket endpoint for dashboard clients (Chrome extension side panel)."""
    # Reject cross-origin connections (only allow localhost)
    origin = (websocket.headers.get("origin") or "").lower()
    if origin and not any(origin.startswith(p) for p in (
        "http://127.0.0.1", "http://localhost", "chrome-extension://",
    )):
        await websocket.close(code=4003, reason="Origin not allowed")
        return
    await websocket.accept()

    q = event_bus.subscribe()
    try:
        # Send initial snapshot
        client = get_flow_client()
        controller = get_worker_controller()
        from agent.db import crud
        pending_requests = await crud.list_requests(status="PENDING")
        processing_requests = await crud.list_requests(status="PROCESSING")
        snapshot = {
            "type": "snapshot",
            "health": {
                "status": "ok",
                "extension_connected": client.connected,
            },
            "requests": pending_requests + processing_requests,
            "worker": {
                "active": controller.active_count,
                "slots": max(0, 5 - controller.active_count),
            },
        }
        await websocket.send_text(json.dumps(snapshot))

        # Forward events from event_bus to this client
        while True:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=30.0)
                await websocket.send_text(msg)
            except asyncio.TimeoutError:
                # Send keepalive ping
                await websocket.send_text(json.dumps({"type": "ping"}))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug("Dashboard WS client disconnected: %s", e)
    finally:
        event_bus.unsubscribe(q)


if __name__ == "__main__":
    import os
    import sys
    import uvicorn
    reload_enabled = os.environ.get("GLA_RELOAD", "0") == "1"
    try:
        uvicorn.run(
            "agent.main:app",
            host=API_HOST,
            port=API_PORT,
            reload=reload_enabled,
            reload_excludes=["*.db", "*.db-wal", "*.db-shm", "output/*"],
        )
    except KeyboardInterrupt:
        sys.exit(0)
    except SystemExit as exc:
        if exc.code == 3:
            sys.exit(0)
        raise
