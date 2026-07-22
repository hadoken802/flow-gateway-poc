"""Dry-run Gateway scheduler."""
import asyncio
import json
from dataclasses import dataclass
from pathlib import Path

from . import crud
from .config import GatewaySettings
from .db import connect
from .models import ACTIVE_TASK_STATUSES
from .worker_client import WorkerClient


@dataclass
class WorkerConfig:
    account_id: str
    api_url: str
    enabled: bool = True


class GatewayScheduler:
    def __init__(self, settings: GatewaySettings, worker_client=None):
        self.settings = settings
        self.worker_client = worker_client or WorkerClient()
        self.db = None
        self.workers = self._load_workers()
        self.account_locks = {worker.account_id: asyncio.Lock() for worker in self.workers}
        self.assignment_lock = asyncio.Lock()
        self._runner_task = None
        self._dry_tasks: dict[str, asyncio.Task] = {}
        self._real_tasks: dict[str, asyncio.Task] = {}
        self._stopping = False
        self.assignment_history: list[tuple[str, str]] = []
        self._last_worker_refresh = 0.0

    def _load_workers(self):
        data = json.loads(Path(self.settings.workers_path).read_text(encoding="utf-8"))
        return [WorkerConfig(**item) for item in data]

    async def start(self):
        self._stopping = False
        self.db = await connect(self.settings.db_path)
        for worker in self.workers:
            await crud.upsert_account(self.db, worker)
        await self.refresh_workers()
        await self.recover_tasks()
        self._runner_task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._stopping = True
        if self._runner_task:
            self._runner_task.cancel()
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
        for task in list(self._dry_tasks.values()):
            task.cancel()
        for task in list(self._real_tasks.values()):
            task.cancel()
        if self._dry_tasks:
            await asyncio.gather(*self._dry_tasks.values(), return_exceptions=True)
        if self._real_tasks:
            await asyncio.gather(*self._real_tasks.values(), return_exceptions=True)
        if self.db:
            await self.db.close()

    async def refresh_workers(self):
        async with self.assignment_lock:
            for worker in self.workers:
                if not worker.enabled:
                    await crud.upsert_account(self.db, worker, status="offline", credits=None)
                    continue
                try:
                    info = await self.worker_client.inspect(worker)
                    credits = info.get("credits")
                    if info.get("status") == "offline":
                        status = "offline"
                    elif not info.get("extension_connected") or not info.get("flow_key_present"):
                        status = "needs_login"
                    elif credits is not None and credits < self.settings.omni_10s_credit_cost:
                        status = "low_credits"
                    else:
                        account = await crud.get_account(self.db, worker.account_id)
                        status = "busy" if account and account.get("current_task_id") else "ready"
                    await crud.upsert_account(self.db, worker, status=status, credits=credits)
                    if status in {"ready", "busy"} and self.settings.dry_run:
                        recovery_task = await crud.get_waiting_recovery_task_for_account(self.db, worker.account_id)
                        if recovery_task and recovery_task["task_id"] not in self._dry_tasks:
                            await self.db.execute(
                                """
                                UPDATE flow_accounts
                                SET status='busy', current_task_id=?,
                                    updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                                WHERE account_id=?
                                """,
                                (recovery_task["task_id"], worker.account_id),
                            )
                            await self.db.commit()
                            self._dry_tasks[recovery_task["task_id"]] = asyncio.create_task(
                                self._run_dry_task(recovery_task["task_id"], worker.account_id)
                            )
                except Exception as exc:
                    account = await crud.get_account(self.db, worker.account_id)
                    if account and account.get("current_task_id"):
                        await crud.update_task_status(self.db, account["current_task_id"], "waiting_recovery", error_code="worker_offline", error_message=str(exc)[:500])
                    await crud.upsert_account(self.db, worker, status="offline", credits=(account or {}).get("credits"), last_error=str(exc)[:500])

    async def recover_tasks(self):
        for task in await crud.list_tasks(self.db):
            if self.settings.dry_run and task["status"] in {"assigning", "submitted", "processing", "waiting_recovery"}:
                await crud.update_task_status(self.db, task["task_id"], "queued")
            elif not self.settings.dry_run and task["status"] in {"assigning", "submitted", "processing", "waiting_recovery", "manual_review"}:
                if task.get("worker_job_id"):
                    self._real_tasks[task["task_id"]] = asyncio.create_task(self._run_real_task(task["task_id"], task["assigned_account_id"]))
                else:
                    await crud.update_task_status(self.db, task["task_id"], "queued")
        await self.db.execute("UPDATE flow_accounts SET current_task_id=NULL WHERE current_task_id IS NOT NULL")
        await self.db.commit()
        await self.refresh_workers()
        await self.schedule_once()

    async def _run_loop(self):
        while not self._stopping:
            now = asyncio.get_running_loop().time()
            if now - self._last_worker_refresh >= self.settings.worker_refresh_interval_seconds:
                self._last_worker_refresh = now
                await self.refresh_workers()
            await self.schedule_once()
            await asyncio.sleep(0.05)

    async def create_task(self, payload):
        if not self.settings.dry_run and self.settings.canary_only:
            existing = sum(1 for task in await crud.list_tasks(self.db) if task.get("submitted_at") != "dry-run")
            if existing >= self.settings.canary_limit:
                raise ValueError(f"CANARY_ONLY accepts at most {self.settings.canary_limit} real tasks")
        async with self.assignment_lock:
            task = await crud.create_task(self.db, payload)
        await self.schedule_once()
        return task

    async def create_tasks(self, payloads):
        return [await self.create_task(payload) for payload in payloads]

    async def list_accounts(self):
        return await crud.list_accounts(self.db)

    async def list_tasks(self):
        return await crud.list_tasks(self.db)

    async def get_task(self, task_id):
        return await crud.get_task(self.db, task_id)

    async def pool_status(self):
        tasks = await crud.list_tasks(self.db)
        accounts = await crud.list_accounts(self.db)
        return {
            "dry_run": self.settings.dry_run,
            "max_concurrency": self.settings.max_concurrency,
            "active_count": sum(1 for task in tasks if task["status"] in ACTIVE_TASK_STATUSES),
            "queued_count": sum(1 for task in tasks if task["status"] == "queued"),
            "completed_count": sum(1 for task in tasks if task["status"] == "completed"),
            "failed_count": sum(1 for task in tasks if task["status"] == "failed"),
            "accounts_ready": sum(1 for account in accounts if account["status"] == "ready"),
            "accounts_busy": sum(1 for account in accounts if account["status"] == "busy"),
            "accounts_low_credits": sum(1 for account in accounts if account["status"] == "low_credits"),
            "accounts_offline": sum(1 for account in accounts if account["status"] == "offline"),
        }

    async def schedule_once(self):
        async with self.assignment_lock:
            await self._schedule_once_locked()

    async def _schedule_once_locked(self):
        status = await self.pool_status()
        if status["active_count"] >= self.settings.max_concurrency:
            return
        accounts = await crud.list_accounts(self.db)
        for account in accounts:
            if status["active_count"] >= self.settings.max_concurrency:
                return
            if account["status"] != "ready" or account.get("current_task_id"):
                continue
            lock = self.account_locks.get(account["account_id"])
            if not lock:
                continue
            async with lock:
                task = await crud.assign_next_task(self.db, account["account_id"], self.settings.omni_10s_credit_cost)
                if not task:
                    continue
                self.assignment_history.append((task["task_id"], account["account_id"]))
                status["active_count"] += 1
                if self.settings.dry_run:
                    self._dry_tasks[task["task_id"]] = asyncio.create_task(self._run_dry_task(task["task_id"], account["account_id"]))
                else:
                    self._real_tasks[task["task_id"]] = asyncio.create_task(self._run_real_task(task["task_id"], account["account_id"]))

    async def _run_dry_task(self, task_id, account_id):
        try:
            a, b, c = self.settings.dry_run_step_seconds
            await asyncio.sleep(a)
            if not await self._account_online(account_id):
                await crud.update_task_status(self.db, task_id, "waiting_recovery", error_code="worker_offline")
                return
            await crud.update_task_status(self.db, task_id, "submitted", submitted_at=_now_marker())
            await asyncio.sleep(b)
            if not await self._account_online(account_id):
                await crud.update_task_status(self.db, task_id, "waiting_recovery", error_code="worker_offline")
                return
            await crud.update_task_status(self.db, task_id, "processing")
            await asyncio.sleep(c)
            if not await self._account_online(account_id):
                await crud.update_task_status(self.db, task_id, "waiting_recovery", error_code="worker_offline")
                return
            result_dir = self.settings.db_path.parent / "dry_run_results"
            result_dir.mkdir(parents=True, exist_ok=True)
            result_path = result_dir / f"{task_id}.dry-run.txt"
            result_path.write_text("dry-run result; no real mp4 generated\n", encoding="utf-8")
            async with self.assignment_lock:
                await crud.complete_task(self.db, task_id, account_id, self.settings.omni_10s_credit_cost, str(result_path))
            await self.schedule_once()
        finally:
            self._dry_tasks.pop(task_id, None)

    async def _run_real_task(self, task_id, account_id):
        try:
            task = await crud.get_task(self.db, task_id)
            worker = self._worker_by_account(account_id)
            if not task or not worker:
                return
            while not task.get("worker_job_id") and not self._stopping:
                payload = {
                    "idempotency_key": task["idempotency_key"],
                    "project_id": task.get("project_id") or f"gateway-{task_id}",
                    "image_path": task["image_path"],
                    "prompt": task["prompt"],
                    "duration": task["duration"],
                    "aspect_ratio": task["aspect_ratio"],
                }
                try:
                    result = await self.worker_client.submit_omni_video(worker, payload)
                except Exception as exc:
                    async with self.assignment_lock:
                        await crud.update_task_status(self.db, task_id, "waiting_recovery", error_code=type(exc).__name__, error_message=str(exc)[:500])
                    await asyncio.sleep(2)
                    task = await crud.get_task(self.db, task_id)
                    continue
                worker_job_id = result.get("job_id") or result.get("worker_job_id")
                if not worker_job_id:
                    await crud.update_task_status(self.db, task_id, "manual_review", error_code="missing_worker_job_id", error_message=str(result)[:500])
                    return
                async with self.assignment_lock:
                    await crud.mark_submitted(
                        self.db,
                        task_id,
                        worker_job_id,
                        remaining_credits=result.get("remaining_credits"),
                    )
                task = await crud.get_task(self.db, task_id)
            while not self._stopping:
                task = await crud.get_task(self.db, task_id)
                if not task or task["status"] == "completed":
                    return
                try:
                    result = await self.worker_client.get_omni_video(worker, task["worker_job_id"])
                    if result.get("status") in {"waiting_download", "completed_remote"}:
                        result = await self.worker_client.retry_omni_video_download(worker, task["worker_job_id"])
                except Exception as exc:
                    async with self.assignment_lock:
                        await crud.update_task_status(self.db, task_id, "waiting_recovery", error_code=type(exc).__name__, error_message=str(exc)[:500])
                    await asyncio.sleep(2)
                    continue
                mapped = _map_worker_status(result.get("status"))
                if mapped == "completed":
                    video_path = result.get("video_path")
                    if not video_path or not Path(video_path).exists():
                        async with self.assignment_lock:
                            await crud.release_task_for_manual_review(
                                self.db,
                                task_id,
                                account_id,
                                error_code=result.get("error_code") or "missing_video_path",
                                error_message=result.get("error_message") or "Worker completed without a local video_path",
                                remaining_credits=result.get("remaining_credits"),
                            )
                        return
                    async with self.assignment_lock:
                        await crud.complete_real_task(
                            self.db,
                            task_id,
                            account_id,
                            video_path,
                            result.get("remaining_credits"),
                        )
                    await self.schedule_once()
                    return
                if mapped == "manual_review":
                    async with self.assignment_lock:
                        await crud.update_task_status(self.db, task_id, "manual_review", error_code=result.get("error_code"), error_message=result.get("error_message"))
                    return
                async with self.assignment_lock:
                    await crud.update_task_status(self.db, task_id, mapped)
                await asyncio.sleep(5)
        finally:
            self._real_tasks.pop(task_id, None)

    async def _account_online(self, account_id):
        account = await crud.get_account(self.db, account_id)
        if not account or account["status"] not in {"busy", "ready"}:
            return False
        worker = self._worker_by_account(account_id)
        if not worker:
            return False
        try:
            info = await self.worker_client.inspect(worker)
        except Exception as exc:
            await crud.upsert_account(self.db, worker, status="offline", credits=account.get("credits"), last_error=str(exc)[:500])
            return False
        return info.get("status") != "offline"

    def _worker_by_account(self, account_id):
        for worker in self.workers:
            if worker.account_id == account_id:
                return worker
        return None


def _now_marker():
    return "dry-run"


def _map_worker_status(status):
    if status in {"waiting_download", "completed_remote"}:
        return "processing"
    if status in {"queued", "scheduled"}:
        return "submitted"
    if status in {"active", "processing"}:
        return "processing"
    if status == "completed":
        return "completed"
    if status == "failed":
        return "manual_review"
    return "submitted"
