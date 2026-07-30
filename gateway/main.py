"""Central Gateway dry-run API."""
from contextlib import asynccontextmanager
import asyncio
import logging
import sys

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from . import crud
from .config import GatewaySettings
from .instance_lock import GatewayInstanceLock, GatewayInstanceLockError
from .scheduler import GatewayScheduler
from . import task_center
from . import nodes

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


@app.get("/api/v1/docs/examples")
async def v1_examples():
    return {**task_center.examples(), "nodes": nodes.examples()}


TASK_CENTER_HTML = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Flow Gateway Task Center</title>
  <style>
    body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#f6f7f9;color:#1d2733}
    header{background:#102033;color:white;padding:14px 20px}
    main{padding:16px;display:grid;gap:16px}
    section{background:white;border:1px solid #d7dde5;border-radius:6px;padding:14px}
    h1{font-size:20px;margin:0} h2{font-size:16px;margin:0 0 10px}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}
    .metric{border:1px solid #e1e5ea;border-radius:6px;padding:10px}
    .metric b{display:block;font-size:22px;margin-top:4px}
    table{width:100%;border-collapse:collapse;font-size:13px}
    th,td{border-bottom:1px solid #e5e9ef;padding:7px;text-align:left;vertical-align:top}
    th{background:#f2f4f7}
    input,select,textarea,button{font:inherit}
    textarea{width:100%;min-height:130px}
    button{border:1px solid #9aa7b5;background:#fff;border-radius:5px;padding:6px 10px;cursor:pointer}
    button.primary{background:#0f5cc0;color:#fff;border-color:#0f5cc0}
    .row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
    .error{color:#a10000}
    .ok{color:#087443}
    code{background:#edf1f5;padding:1px 4px;border-radius:4px}
  </style>
</head>
<body>
<header><h1>Flow Gateway Task Center</h1></header>
<main>
  <section>
    <h2>Overview</h2>
    <div class="grid" id="metrics"></div>
  </section>
  <section>
    <h2>Batch Import</h2>
    <div class="row">
      <select id="importFormat"><option value="csv">CSV</option><option value="json">JSON</option></select>
      <input id="importFile" type="file" accept=".csv,.json" onchange="loadImportFile()">
      <button onclick="loadExample()">Load Example</button>
      <button class="primary" onclick="importTasks()">Import Tasks</button>
    </div>
    <textarea id="importContent"></textarea>
    <pre id="importResult"></pre>
  </section>
  <section>
    <h2>Tasks</h2>
    <div class="row">
      <input id="filterStatus" placeholder="status">
      <input id="filterBatch" placeholder="batch_id">
      <input id="filterAccount" placeholder="account_id">
      <button onclick="loadTasks()">Refresh</button>
      <button onclick="exportTasks()">Export</button>
    </div>
    <div style="overflow:auto"><table id="tasks"></table></div>
  </section>
  <section>
    <h2>Accounts</h2>
    <div style="overflow:auto"><table id="accounts"></table></div>
  </section>
  <section>
    <h2>Account Nodes</h2>
    <div class="row">
      <input id="nodeAccountId" placeholder="FLOW-004">
      <input id="nodeWorkerPort" placeholder="worker port">
      <input id="nodeCdpPort" placeholder="cdp port">
      <input id="nodeDisplayName" placeholder="display name">
      <button class="primary" onclick="addNode()">Add Node</button>
      <button onclick="loadNodeExample()">Load Node Example</button>
      <button onclick="importNodes()">Import Nodes</button>
    </div>
    <textarea id="nodeImportContent" placeholder="CSV or JSON node config"></textarea>
    <pre id="nodeResult"></pre>
    <div style="overflow:auto"><table id="nodesTable"></table></div>
  </section>
</main>
<script>
async function api(path, options){const r=await fetch(path, options); if(!r.ok) throw new Error(await r.text()); return await r.json();}
function td(v){return `<td>${v??''}</td>`}
async function refresh(){
  const s=await api('/api/v1/system/status');
  const ms=[['queued',s.queued_count],['running',s.active_count],['completed',s.completed_count],['failed',s.failed_count],['ready accounts',s.accounts_ready],['cooldown',s.accounts_cooldown||0],['paused',s.accounts_paused||0]];
  metrics.innerHTML=ms.map(m=>`<div class="metric">${m[0]}<b>${m[1]}</b></div>`).join('');
  await loadAccounts(); await loadTasks(); await loadNodes();
}
async function loadAccounts(){
  const rows=await api('/api/v1/accounts');
  accounts.innerHTML='<tr><th>account</th><th>status</th><th>credits</th><th>reserved</th><th>health</th><th>current_task</th><th>cooldown</th><th>weight</th><th>actions</th></tr>'+
    rows.map(a=>`<tr>${td(a.account_id)}${td(a.status)}${td(a.credits)}${td(a.reserved_credits)}${td(a.health_score)}${td(a.current_task_id)}${td(a.cooldown_until)}${td(a.account_weight)}<td><button onclick="pauseAccount('${a.account_id}')">pause</button> <button onclick="resumeAccount('${a.account_id}')">resume</button> <button onclick="cooldownAccount('${a.account_id}')">cooldown</button> <button onclick="clearCooldown('${a.account_id}')">clear</button> <button onclick="setWeight('${a.account_id}')">weight</button> <button onclick="setCredits('${a.account_id}')">credits</button></td></tr>`).join('');
}
async function loadTasks(){
  const q=new URLSearchParams(); if(filterStatus.value) q.set('status',filterStatus.value); if(filterBatch.value) q.set('batch_id',filterBatch.value); if(filterAccount.value) q.set('account_id',filterAccount.value);
  const rows=await api('/api/v1/tasks?'+q.toString());
  tasks.innerHTML='<tr><th>task_id</th><th>external</th><th>batch</th><th>status</th><th>account</th><th>job</th><th>project</th><th>gen</th><th>dl</th><th>priority</th><th>output</th><th>error</th><th>actions</th></tr>'+
    rows.map(t=>`<tr>${td(`<code>${t.task_id}</code>`)}${td(t.external_task_id)}${td(t.batch_id)}${td(t.status)}${td(t.assigned_account_id||t.account_id)}${td(t.worker_job_id)}${td(t.project_id)}${td(t.generation_attempts)}${td(t.download_attempts)}${td(t.priority)}${td(t.video_path||((t.output_directory||'')+'/'+(t.output_filename||'')))}${td(t.error_code||t.last_error_category||'')}<td><button onclick="detail('${t.task_id}')">detail</button> <button onclick="pauseTask('${t.task_id}')">pause</button> <button onclick="resumeTask('${t.task_id}')">resume</button> <button onclick="cancelTask('${t.task_id}')">cancel</button> <button onclick="priorityTask('${t.task_id}')">priority</button> <button onclick="requeueTask('${t.task_id}')">requeue</button> <button onclick="retryDownload('${t.task_id}')">retry download</button> <button onclick="reconcileTask('${t.task_id}')">reconcile</button> <button onclick="manualTask('${t.task_id}')">need manual</button></td></tr>`).join('');
}
async function loadExample(){const e=await api('/api/v1/docs/examples'); importContent.value=importFormat.value==='csv'?e.csv:JSON.stringify(e.json,null,2);}
async function loadImportFile(){const f=importFile.files[0]; if(!f) return; importContent.value=await f.text(); importFormat.value=f.name.toLowerCase().endsWith('.json')?'json':'csv';}
async function importTasks(){try{const format=importFormat.value; const content=importContent.value; importResult.textContent=JSON.stringify(await api('/api/v1/tasks/import',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({format,content})}),null,2); await refresh();}catch(e){importResult.textContent=e.message}}
async function pauseAccount(id){await api(`/api/pool/accounts/${id}/pause`,{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); await refresh()}
async function resumeAccount(id){await api(`/api/pool/accounts/${id}/resume`,{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); await refresh()}
async function cooldownAccount(id){const seconds=prompt('cooldown seconds','300'); if(!seconds) return; await api(`/api/pool/accounts/${id}/cooldown`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({seconds:Number(seconds),reason:'task_center'})}); await refresh()}
async function clearCooldown(id){await api(`/api/pool/accounts/${id}/cooldown/clear`,{method:'POST'}); await refresh()}
async function setWeight(id){const weight=prompt('account weight','1'); if(!weight) return; await api(`/api/pool/accounts/${id}/weight`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({account_weight:Number(weight)})}); await refresh()}
async function setCredits(id){const credits=prompt('credits'); if(!credits) return; await api(`/api/pool/accounts/${id}/credits`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({credits:Number(credits),source:'task_center'})}); await refresh()}
async function pauseTask(id){await api(`/api/v1/tasks/${id}/pause`,{method:'POST'}); await loadTasks()}
async function resumeTask(id){await api(`/api/v1/tasks/${id}/resume`,{method:'POST'}); await loadTasks()}
async function cancelTask(id){await api(`/api/v1/tasks/${id}/cancel`,{method:'POST'}); await loadTasks()}
async function priorityTask(id){const priority=prompt('priority'); if(priority===null) return; await api(`/api/v1/tasks/${id}/priority`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({priority:Number(priority)})}); await loadTasks()}
async function requeueTask(id){await api(`/api/v1/tasks/${id}/requeue`,{method:'POST'}); await loadTasks()}
async function retryDownload(id){alert(JSON.stringify(await api(`/api/v1/tasks/${id}/retry-download`,{method:'POST'}),null,2))}
async function reconcileTask(id){alert(JSON.stringify(await api(`/api/v1/tasks/${id}/reconcile`,{method:'POST'}),null,2))}
async function manualTask(id){await api(`/api/v1/tasks/${id}/need-manual`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({error_code:'NEED_MANUAL',error_message:'Marked from task center'})}); await loadTasks()}
async function detail(id){alert(JSON.stringify(await api(`/api/v1/tasks/${id}`),null,2))}
async function exportTasks(){const rows=await api('/api/v1/tasks'); const blob=new Blob([JSON.stringify(rows,null,2)],{type:'application/json'}); const a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download='flow-gateway-tasks.json'; a.click();}
async function loadNodes(){
  const rows=await api('/api/v1/nodes');
  nodesTable.innerHTML='<tr><th>account</th><th>worker</th><th>cdp</th><th>pids</th><th>runtime</th><th>extension</th><th>oauth/quota</th><th>credits</th><th>current task</th><th>enabled</th><th>paused/cooldown</th><th>error</th><th>actions</th></tr>'+
    rows.map(n=>`<tr>${td(n.account_id)}${td(`${n.worker_host}:${n.worker_port}`)}${td(`${n.cdp_host}:${n.cdp_port}`)}${td(`worker ${n.worker_pid||''}<br>chrome ${n.chrome_pid||''}`)}${td(n.worker_status)}${td(n.extension_status)}${td(`${n.oauth_status||''}<br>${n.quota_confidence||''}`)}${td(n.credits)}${td(n.current_task_id)}${td(n.enabled)}${td(`${n.manual_paused?'paused':''}<br>${n.cooldown_until||''}`)}${td(n.last_gateway_error||n.last_error||'')}<td><button onclick="startNode('${n.account_id}')">start</button> <button onclick="stopNode('${n.account_id}')">stop</button> <button onclick="restartNode('${n.account_id}')">restart</button> <button onclick="refreshNodeSession('${n.account_id}')">refresh session</button> <button onclick="enableNode('${n.account_id}')">enable</button> <button onclick="disableNode('${n.account_id}')">disable</button> <button onclick="editNode('${n.account_id}',${n.worker_port},${n.cdp_port})">edit ports</button> <button onclick="nodeDetail('${n.account_id}')">diagnostics</button></td></tr>`).join('');
}
async function addNode(){
  const payload={account_id:nodeAccountId.value,worker_port:Number(nodeWorkerPort.value),cdp_port:Number(nodeCdpPort.value),display_name:nodeDisplayName.value||nodeAccountId.value,enabled:false};
  nodeResult.textContent=JSON.stringify(await api('/api/v1/nodes',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)}),null,2);
  await loadNodes();
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
async function refreshNodeSession(id){nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}/refresh-session`,{method:'POST'}),null,2); await refresh()}
async function enableNode(id){nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}/enable`,{method:'POST'}),null,2); await refresh()}
async function disableNode(id){nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}/disable`,{method:'POST'}),null,2); await refresh()}
async function editNode(id,oldWorker,oldCdp){const worker=prompt('worker port',oldWorker); if(!worker) return; const cdp=prompt('cdp port',oldCdp); if(!cdp) return; nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}`,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({worker_port:Number(worker),cdp_port:Number(cdp)})}),null,2); await refresh()}
async function nodeDetail(id){nodeResult.textContent=JSON.stringify(await api(`/api/v1/nodes/${id}`),null,2)}
refresh();
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
