"""Dry-run Gateway scheduler."""
import asyncio
import json
import logging
import uuid
from pathlib import Path

from . import crud
from . import scheduler_kernel
from .config import GatewaySettings
from .db import connect
from .models import ACTIVE_TASK_STATUSES
from .worker_client import WorkerClient
from .worker_provider import WorkerConfig, WorkerSnapshot, build_worker_provider

logger = logging.getLogger(__name__)


REMOTE_ID_FIELDS = ("project_id", "workflow_id", "output_media_id", "upstream_batch_id", "operation_name")


class RemoteIdConflict(RuntimeError):
    def __init__(self, field: str, current: str, incoming: str):
        self.field = field
        self.current = current
        self.incoming = incoming
        super().__init__(f"remote_id_conflict:{field}")


class GatewayScheduler:
    def __init__(self, settings: GatewaySettings, worker_client=None, worker_provider=None):
        self.settings = settings
        self.worker_client = worker_client or WorkerClient(settings.worker_submit_timeout_seconds)
        self.db = None
        self.worker_provider = worker_provider or build_worker_provider(settings)
        self.worker_snapshot = self._load_worker_snapshot()
        self.workers = self.worker_snapshot.workers
        self.account_locks = {worker.account_id: asyncio.Lock() for worker in self.workers}
        self.assignment_lock = asyncio.Lock()
        self._runner_task = None
        self._dry_tasks: dict[str, asyncio.Task] = {}
        self._real_tasks: dict[str, asyncio.Task] = {}
        self._heartbeat_tasks: dict[str, asyncio.Task] = {}
        self._lease_sweeper_task = None
        self._fencing_lost: set[str] = set()
        self._stopping = False
        self.assignment_history: list[tuple[str, str]] = []
        self._last_worker_refresh = 0.0
        self._worker_refresh_lock = asyncio.Lock()
        self.startup_stage: str | None = None
        self.boot_id = str(uuid.uuid4())
        self.scheduler_instance_id = str(uuid.uuid4())

    def _load_worker_snapshot(self) -> WorkerSnapshot:
        try:
            snapshot = self.worker_provider.load_workers()
            if self.settings.allowed_account_ids:
                allowed = set(self.settings.allowed_account_ids)
                return WorkerSnapshot(
                    workers=[worker for worker in snapshot.workers if worker.account_id in allowed],
                    candidates=snapshot.candidates,
                    worker_source=snapshot.worker_source,
                    provider_kind=snapshot.provider_kind,
                    registry_snapshot_time=snapshot.registry_snapshot_time,
                )
            return snapshot
        except Exception as exc:
            raise RuntimeError("runtime_worker_provider_unavailable") from exc

    def refresh_worker_snapshot(self) -> WorkerSnapshot:
        self._apply_worker_snapshot(self._load_worker_snapshot())
        return self.worker_snapshot

    async def async_refresh_worker_snapshot(self) -> WorkerSnapshot:
        async with self._worker_refresh_lock:
            snapshot = await asyncio.to_thread(self._load_worker_snapshot)
            self._apply_worker_snapshot(snapshot)
            return self.worker_snapshot

    def _apply_worker_snapshot(self, snapshot: WorkerSnapshot) -> None:
        self.worker_snapshot = snapshot
        self.workers = snapshot.workers
        self.account_locks = {worker.account_id: self.account_locks.get(worker.account_id, asyncio.Lock()) for worker in self.workers}

    async def start(self):
        self._stopping = False
        self.startup_stage = "database_connect"
        self._audit("database_connect_started", database_path=str(self.settings.db_path))
        self.db = await connect(self.settings.db_path)
        self._audit("database_connect_completed", database_path=str(self.settings.db_path))
        self.startup_stage = "upsert_accounts"
        self._audit("accounts_upsert_started", account_count=len(self.workers))
        for worker in self.workers:
            await crud.upsert_account(self.db, worker)
        self._audit("accounts_upsert_completed", account_count=len(self.workers))
        self.startup_stage = "refresh_workers"
        self._audit("refresh_workers_started")
        await self.refresh_workers()
        self._audit("refresh_workers_completed")
        self.startup_stage = "recover_tasks"
        self._audit("recover_tasks_started")
        await self.sweep_expired_leases_once(reason="startup")
        await self.recover_tasks()
        self._audit("recover_tasks_completed")
        self.startup_stage = "scheduler_loop_start"
        self._runner_task = asyncio.create_task(self._run_loop())
        self._lease_sweeper_task = asyncio.create_task(self._lease_sweeper_loop())
        self._audit("scheduler_loop_started")
        self.startup_stage = "completed"

    async def stop(self):
        self._stopping = True
        if self._runner_task:
            self._runner_task.cancel()
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
        if self._lease_sweeper_task:
            self._lease_sweeper_task.cancel()
            try:
                await self._lease_sweeper_task
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
            await self.async_refresh_worker_snapshot()
            for worker in self.workers:
                if not worker.enabled:
                    await crud.upsert_account(self.db, worker, status="offline", credits=None)
                    continue
                try:
                    info = await self.worker_client.inspect(worker)
                    credits = info.get("credits")
                    account = await crud.get_account(self.db, worker.account_id)
                    stored_credits = (account or {}).get("credits")
                    last_known_credits = stored_credits if stored_credits is not None else (account or {}).get("credits_total")
                    write_credits = credits if credits is not None else stored_credits
                    if write_credits is None:
                        write_credits = last_known_credits
                    if info.get("status") == "offline":
                        status = "offline"
                    elif not info.get("extension_connected") or not info.get("flow_key_present"):
                        status = "needs_login"
                    elif credits is not None and credits < self.settings.omni_10s_credit_cost:
                        status = "low_credits"
                    elif credits is None and account and account.get("status") == "low_credits":
                        status = "low_credits"
                    elif credits is None and last_known_credits is not None and last_known_credits < self.settings.omni_10s_credit_cost:
                        status = "low_credits"
                    else:
                        status = "busy" if account and account.get("current_task_id") else "ready"
                    await crud.upsert_account(
                        self.db,
                        worker,
                        status=status,
                        credits=write_credits,
                        last_error=info.get("credits_error") if credits is None else None,
                        quota_source=info.get("credits_source") if credits is not None else info.get("credits_error"),
                        quota_confidence="live" if credits is not None else "stale",
                    )
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
        tasks = await crud.list_tasks(self.db)
        if not tasks:
            return
        if self.settings.dry_run:
            for task in tasks:
                if task["status"] in {"assigning", "reserving", "leased", "submitting", "submitted", "processing", "waiting_recovery"}:
                    await crud.update_task_status(self.db, task["task_id"], "queued")
            await self.db.execute(
                """
                UPDATE flow_accounts
                SET current_task_id=NULL, lock_owner=NULL, lock_expires_at=NULL
                WHERE current_task_id IN (
                    SELECT task_id FROM flow_tasks WHERE status='queued'
                )
                """
            )
            await self.db.execute(
                """
                UPDATE account_leases
                SET status='released'
                WHERE status='active'
                  AND task_id IN (SELECT task_id FROM flow_tasks WHERE status='queued')
                """
            )
            await self.db.commit()
            await self.refresh_workers()
            await self.schedule_once()
            return

        recoverable_statuses = {"submitted", "processing", "download_pending", "downloading", "waiting_recovery"}
        active_statuses = {"leased", "assigning", "project_create_pending", "project_create_in_progress", "project_created", "submit_pending", "submit_in_progress", "submitted", "processing", "download_pending", "downloading", "waiting_recovery"}
        active_by_account: dict[str, list[dict]] = {}
        terminal_task_ids = {task["task_id"] for task in tasks if task["status"] in {"completed", "failed", "failed_before_remote_submit", "cancelled", "download_failed"}}
        task_ids = {task["task_id"] for task in tasks}
        for task in tasks:
            if task["status"] in active_statuses and task.get("assigned_account_id"):
                active_by_account.setdefault(task["assigned_account_id"], []).append(task)

        for account in await crud.list_accounts(self.db):
            current_task_id = account.get("current_task_id")
            active = active_by_account.get(account["account_id"], [])
            if len(active) > 1:
                self._audit("recovery_conflict_detected", account_id=account["account_id"], task_ids=[task["task_id"] for task in active])
                for task in active:
                    await crud.release_task_for_manual_review(
                        self.db,
                        task["task_id"],
                        account["account_id"],
                        error_code="multiple_active_tasks_for_account",
                        error_message="Multiple active tasks were assigned to one account during recovery",
                    )
                continue
            if len(active) == 1:
                task = active[0]
                if current_task_id != task["task_id"]:
                    await self.db.execute(
                        """
                        UPDATE flow_accounts
                        SET current_task_id=?, status='busy',
                            updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                        WHERE account_id=? AND (current_task_id IS NULL OR current_task_id=? OR current_task_id NOT IN (SELECT task_id FROM flow_tasks WHERE status IN ('assigning','submitted','processing','waiting_recovery')))
                        """,
                        (task["task_id"], account["account_id"], current_task_id),
                    )
                    await self.db.commit()
                    self._audit("recovery_binding_restored", account_id=account["account_id"], task_id=task["task_id"])
                continue
            if current_task_id and (current_task_id not in task_ids or current_task_id in terminal_task_ids):
                await self.db.execute(
                    """
                    UPDATE flow_accounts
                    SET current_task_id=NULL,
                        updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                    WHERE account_id=? AND current_task_id=?
                    """,
                    (account["account_id"], current_task_id),
                )
                await self.db.commit()
                self._audit("recovery_stale_binding_cleared", account_id=account["account_id"], task_id=current_task_id)
            elif current_task_id:
                task = next((item for item in tasks if item["task_id"] == current_task_id), None)
                if task and task["status"] == "manual_submit_required":
                    await crud.release_task_for_manual_review(
                        self.db,
                        current_task_id,
                        account["account_id"],
                        error_code=task.get("error_code"),
                        error_message=task.get("error_message"),
                        remaining_credits=task.get("remaining_credits"),
                        status="manual_submit_required",
                    )
                    self._audit("recovery_manual_submit_runtime_lock_cleared", account_id=account["account_id"], task_id=current_task_id)

        for task in await crud.list_tasks(self.db):
            if task["status"] in {"manual_review", "manual_submit_required", "project_creation_unknown", "submission_unknown", "remote_state_unknown"}:
                continue
            if task["status"] == "project_create_pending":
                await crud.update_task_status(self.db, task["task_id"], "queued")
                continue
            if task["status"] == "project_create_in_progress":
                await crud.update_task_status(self.db, task["task_id"], "project_creation_unknown", last_error_code="restart_during_project_create")
                continue
            if task["status"] == "submit_in_progress":
                await crud.update_task_status(self.db, task["task_id"], "submission_unknown", last_error_code="restart_during_submit")
                continue
            if task["status"] == "assigning" and task.get("project_id") and not task.get("worker_job_id"):
                await crud.update_task_status(self.db, task["task_id"], "submission_unknown", last_error_code="restart_after_project_before_job")
                continue
            if task["status"] == "download_failed":
                continue
            if task["status"] == "downloading":
                await crud.update_task_status(self.db, task["task_id"], "download_pending")
                continue
            if task["status"] in recoverable_statuses:
                if task.get("worker_job_id"):
                    task = await crud.ensure_active_lease(self.db, task["task_id"], task["assigned_account_id"])
                    self._real_tasks[task["task_id"]] = asyncio.create_task(self._run_real_task(task["task_id"], task["assigned_account_id"]))
                elif task.get("project_id"):
                    self._audit("account_released", task_id=task["task_id"], account_id=task.get("assigned_account_id"), release_reason="submit_state_unknown")
                    await crud.release_task_for_manual_review(
                        self.db,
                        task["task_id"],
                        task.get("assigned_account_id"),
                        error_code="submit_state_unknown",
                        error_message="Task has project_id but no worker_job_id during recovery",
                    )
                else:
                    await crud.update_task_status(self.db, task["task_id"], "queued")
        await self.refresh_workers()
        await self.schedule_once()

    async def _run_loop(self):
        while not self._stopping:
            now = asyncio.get_running_loop().time()
            if now - self._last_worker_refresh >= self.settings.worker_refresh_interval_seconds:
                try:
                    await self.refresh_workers()
                finally:
                    self._last_worker_refresh = asyncio.get_running_loop().time()
            await self.schedule_once()
            await asyncio.sleep(0.05)

    async def _lease_sweeper_loop(self):
        while not self._stopping:
            await asyncio.sleep(self.settings.lease_sweeper_interval_seconds)
            await self.sweep_expired_leases_once(reason="periodic")

    async def sweep_expired_leases_once(self, reason="manual"):
        if not self.db:
            return []
        results = await scheduler_kernel.sweep_expired_leases(
            self.db,
            recovery_owner=self.scheduler_instance_id,
        )
        if results:
            self._audit("lease_sweeper_completed", reason=reason, recovered=sum(1 for item in results if item.get("ok")), results=results)
            await self.schedule_once()
        return results

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
            "accounts_paused": sum(1 for account in accounts if int(account.get("manual_paused") or 0)),
            "accounts_cooldown": sum(1 for account in accounts if account.get("cooldown_until")),
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
            today_count, total_count = await scheduler_kernel.usage_counts(self.db, account["account_id"])
            account["_task_count_today"] = today_count
            account["_total_task_count"] = total_count
        while status["active_count"] < self.settings.max_concurrency:
            next_task = self._next_schedulable_task(await crud.list_tasks(self.db))
            if not next_task:
                return
            selected = self.select_worker(next_task, self.worker_snapshot, account_states=accounts)
            if not selected.get("ok"):
                return
            account_id = selected["selected_account_id"]
            account = next((item for item in accounts if item["account_id"] == account_id), None)
            if not account:
                return
            if status["active_count"] >= self.settings.max_concurrency:
                return
            lock = self.account_locks.get(account_id)
            if not lock:
                return
            async with lock:
                task = await crud.assign_next_task(
                    self.db,
                    account_id,
                    selected.get("selected_runtime_instance_id"),
                    self.settings.omni_10s_credit_cost,
                    self.settings.lease_duration_seconds,
                )
                if not task:
                    return
                self.assignment_history.append((task["task_id"], account_id))
                self._audit("account_reserved", task_id=task["task_id"], account_id=account_id, runtime_instance_id=selected.get("selected_runtime_instance_id"))
                account["status"] = "busy"
                account["current_task_id"] = task["task_id"]
                status["active_count"] += 1
                if self.settings.dry_run:
                    self._dry_tasks[task["task_id"]] = asyncio.create_task(self._run_dry_task(task["task_id"], account_id))
                else:
                    self._real_tasks[task["task_id"]] = asyncio.create_task(self._run_real_task(task["task_id"], account_id))

    def select_worker(self, task: dict, worker_snapshot: WorkerSnapshot | None = None, account_states: list[dict] | None = None) -> dict:
        snapshot = worker_snapshot or self.worker_snapshot
        states = {account["account_id"]: account for account in account_states or []}
        preferred_account_id = task.get("preferred_account_id")
        eligible = []
        diagnostics = []
        for worker in snapshot.workers:
            candidate = {
                "account_id": worker.account_id,
                "api_url": worker.api_url,
                "selected": False,
                "score": None,
                "reason": None,
            }
            if self.settings.allowed_account_ids and worker.account_id not in self.settings.allowed_account_ids:
                candidate["reason"] = "not_allowed"
                diagnostics.append(candidate)
                continue
            if preferred_account_id and worker.account_id != preferred_account_id:
                candidate["reason"] = "not_preferred"
                diagnostics.append(candidate)
                continue
            account = states.get(worker.account_id)
            if not account:
                account = {
                    "account_id": worker.account_id,
                    "status": "ready",
                    "credits": self.settings.omni_10s_credit_cost,
                    "reserved_credits": 0,
                    "health_score": 100,
                    "account_weight": 1.0,
                }
            ok, reason = scheduler_kernel.account_is_schedulable(account)
            if not ok:
                candidate["reason"] = reason
                diagnostics.append(candidate)
                continue
            if (
                not self.settings.dry_run
                and not self.settings.allow_stale_quota_scheduling
                and account.get("quota_confidence") != "live"
            ):
                candidate["reason"] = "quota_not_live"
                diagnostics.append(candidate)
                continue
            score = scheduler_kernel.score_account(
                account,
                task_count_today=int(account.get("_task_count_today") or 0),
                total_task_count=int(account.get("_total_task_count") or 0),
                required_credits=self.settings.omni_10s_credit_cost,
            )
            if score < -999999:
                candidate["reason"] = "insufficient_available_credits"
                candidate["score"] = score
                diagnostics.append(candidate)
                continue
            candidate["score"] = score
            candidate["reason"] = "eligible"
            eligible.append((score, worker, candidate))
            diagnostics.append(candidate)
        eligible.sort(key=lambda item: (-item[0], states.get(item[1].account_id, {}).get("last_used_at") or "", item[1].account_id))
        if not eligible:
            return {
                "result": "no_eligible_worker",
                "ok": False,
                "selection_reason": "no eligible runtime worker",
                "candidates": diagnostics,
                **snapshot.diagnostics(),
            }
        _, selected, selected_diag = eligible[0]
        selected_diag["selected"] = True
        return {
            "result": "worker_selected",
            "ok": True,
            "selected_account_id": selected.account_id,
            "selected_runtime_instance_id": selected.runtime_instance_id,
            "selected_worker_api_endpoint": selected.api_url,
            "selection_reason": "weighted least-used eligible account",
            "selection_function": "GatewayScheduler.select_worker",
            "candidates": diagnostics,
            **snapshot.diagnostics(),
        }

    def _next_schedulable_task(self, tasks: list[dict]) -> dict | None:
        now = crud.utc_now()
        queued = [
            task for task in tasks
            if task.get("status") == "queued"
            and (task.get("state") in {None, "queued"} or task.get("state") == "queued")
            and not int(task.get("manual_paused") or 0)
            and (not task.get("not_before") or task.get("not_before") <= now)
        ]
        queued.sort(key=lambda task: (-int(task.get("priority") or 0), task.get("created_at") or "", task.get("task_id") or ""))
        if queued:
            return queued[0]
        return None

    def dispatch_dry_run(self, task_id: str = "DRYRUN-001") -> dict:
        snapshot = self.refresh_worker_snapshot()
        selection = self.select_worker({"task_id": task_id, "required_capability": "flow"}, snapshot)
        if not selection.get("ok"):
            return {
                **selection,
                "task_id": task_id,
                "task_type": "flow_video",
                "required_capability": "flow",
                "estimated_cost": "unknown",
                "no_payload": True,
                "scheduler_path_used": True,
                "side_effects": False,
                "would_acquire_lease": False,
            }
        worker = self._worker_by_account(selection["selected_account_id"])
        current = self.worker_provider.load_workers()
        latest = next((item for item in current.workers if item.account_id == worker.account_id), None)
        if latest and latest.runtime_instance_id != worker.runtime_instance_id:
            return {
                "result": "stale_runtime_instance",
                "ok": False,
                "task_id": task_id,
                "selected_account_id": worker.account_id,
                "selected_runtime_instance_id": worker.runtime_instance_id,
                "current_runtime_instance_id": latest.runtime_instance_id,
                "scheduler_path_used": True,
                "selection_function": "GatewayScheduler.select_worker",
                "side_effects": False,
                "would_acquire_lease": False,
                **snapshot.diagnostics(),
            }
        return {
            "result": "dry_run_selected",
            "ok": True,
            "task_id": task_id,
            "task_type": "flow_video",
            "required_capability": "flow",
            "estimated_cost": "unknown",
            "no_payload": True,
            "scheduler_path_used": True,
            **{key: value for key, value in selection.items() if key not in {"result", "ok"}},
            "would_acquire_lease": True,
            "side_effects": False,
        }

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

    async def _increment_attempt_count(self, task_id):
        async with self.assignment_lock:
            await self.db.execute(
                """
                UPDATE flow_tasks
                SET attempt_count=attempt_count+1,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE task_id=?
                """,
                (task_id,),
            )
            await self.db.commit()

    async def _run_real_task(self, task_id, account_id):
        try:
            task = await crud.get_task(self.db, task_id)
            worker = self._worker_by_account(account_id)
            if not task or not worker:
                return
            token = _lease_token(task)
            heartbeat_task = self._start_task_heartbeat(task_id, account_id, token)
            while not task.get("worker_job_id") and not self._stopping:
                current_worker = await self._bound_ready_worker(task_id, account_id)
                if not current_worker:
                    return
                worker = current_worker
                if int(task.get("attempt_count") or 0) >= self.settings.real_submit_max_attempts:
                    async with self.assignment_lock:
                        account = await crud.get_account(self.db, account_id)
                        await crud.guarded_release_account(
                            self.db,
                            task_id,
                            account_id,
                            token["lease_owner"],
                            token["lease_version"],
                            int((account or {}).get("lock_version") or 0),
                            "failed_before_remote_submit",
                            error_code="real_submit_attempts_exhausted",
                            error_message="Worker submit attempt limit reached",
                            remaining_credits=task.get("remaining_credits"),
                            last_error_code="real_submit_attempts_exhausted",
                            last_error_message="Worker submit attempt limit reached",
                        )
                    return
                project_id = task.get("project_id")
                if not isinstance(project_id, str) or not project_id.strip():
                    try:
                        async with self.assignment_lock:
                            task = await crud.guarded_update_task(self.db, task_id, token["lease_owner"], token["lease_version"], "project_create_pending", scheduler_instance_id=self.scheduler_instance_id, worker_instance_id=worker.runtime_instance_id, boot_id=self.boot_id)
                        if not task:
                            self._audit("fencing_lost", task_id=task_id, action="project_create_pending")
                            return
                        async with self.assignment_lock:
                            task = await crud.guarded_update_task(self.db, task_id, token["lease_owner"], token["lease_version"], "project_create_in_progress")
                        if not task:
                            self._audit("fencing_lost", task_id=task_id, action="project_create_in_progress")
                            return
                        self._audit("project_create_started", task_id=task_id, worker_account_id=worker.account_id, runtime_instance_id=worker.runtime_instance_id)
                        project = await self.worker_client.create_project(worker, _project_payload(task_id))
                        project_id = project.get("id")
                        if not isinstance(project_id, str) or not project_id.strip() or project_id.startswith("gateway-"):
                            raise ValueError("Worker project response did not include a real Flow project id")
                        self._audit("project_create_completed", task_id=task_id, worker_account_id=worker.account_id, project_id=project_id)
                    except Exception as exc:
                        async with self.assignment_lock:
                            self._audit("account_released", task_id=task_id, account_id=account_id, release_reason="project_create_failed")
                            account = await crud.get_account(self.db, account_id)
                            await crud.guarded_release_account(
                                self.db,
                                task_id,
                                account_id,
                                token["lease_owner"],
                                token["lease_version"],
                                int((account or {}).get("lock_version") or 0),
                                "failed_before_remote_submit",
                                error_code="project_create_failed",
                                error_message=str(exc)[:500],
                                remaining_credits=task.get("remaining_credits"),
                                last_error_code="project_create_failed",
                                last_error_message=str(exc)[:500],
                            )
                        return
                    async with self.assignment_lock:
                        task = await crud.guarded_update_task(self.db, task_id, token["lease_owner"], token["lease_version"], "project_created", project_id=project_id, project_created_by_gateway=1, project_created_at=crud.utc_now())
                    if not task:
                        self._audit("fencing_lost", task_id=task_id, action="project_created")
                        return
                    current_worker = await self._bound_ready_worker(task_id, account_id)
                    if not current_worker:
                        return
                    worker = current_worker
                payload = {
                    "idempotency_key": task["idempotency_key"],
                    "project_id": project_id,
                    "image_path": task["image_path"],
                    "prompt": task["prompt"],
                    "duration": task["duration"],
                    "aspect_ratio": task["aspect_ratio"],
                }
                try:
                    async with self.assignment_lock:
                        task = await crud.guarded_update_task(self.db, task_id, token["lease_owner"], token["lease_version"], "submit_pending")
                    if not task:
                        self._audit("fencing_lost", task_id=task_id, action="submit_pending")
                        return
                    async with self.assignment_lock:
                        task = await crud.guarded_transition(
                            self.db,
                            task_id,
                            token["lease_owner"],
                            token["lease_version"],
                            "submit_pending",
                            "submit_in_progress",
                            generation_attempts=("increment", 1),
                            attempt_count=("increment", 1),
                            submission_started_at=crud.utc_now(),
                        )
                    if not task:
                        self._audit("fencing_lost", task_id=task_id, action="submit_in_progress")
                        return
                    self._audit("worker_submit_started", task_id=task_id, worker_account_id=worker.account_id, project_id=project_id)
                    result = await self.worker_client.submit_omni_video(worker, payload)
                    if task_id in self._fencing_lost:
                        self._audit("fencing_lost", task_id=task_id, action="after_submit")
                        return
                    self._audit("worker_submit_completed", task_id=task_id, worker_account_id=worker.account_id, worker_job_id=result.get("job_id") or result.get("worker_job_id"))
                except Exception as exc:
                    async with self.assignment_lock:
                        self._audit("account_released", task_id=task_id, account_id=account_id, release_reason="worker_submit_failed")
                        await crud.guarded_update_task(
                            self.db,
                            task_id,
                            token["lease_owner"],
                            token["lease_version"],
                            "submission_unknown",
                            error_code=type(exc).__name__[:120],
                            error_message=str(exc)[:500],
                            last_error_code=type(exc).__name__[:120],
                            last_error_message=str(exc)[:500],
                            remaining_credits=task.get("remaining_credits"),
                        )
                    return
                worker_job_id = result.get("job_id") or result.get("worker_job_id")
                if not worker_job_id:
                    self._audit("account_released", task_id=task_id, account_id=account_id, release_reason="missing_worker_job_id")
                    await crud.guarded_update_task(
                        self.db,
                        task_id,
                        token["lease_owner"],
                        token["lease_version"],
                        "submission_unknown",
                        error_code="missing_worker_job_id",
                        error_message=str(result)[:500],
                        last_error_code="missing_worker_job_id",
                        last_error_message=str(result)[:500],
                        remaining_credits=result.get("remaining_credits"),
                    )
                    return
                try:
                    task = await self._sync_remote_ids(task_id, token, result, "submit")
                except RemoteIdConflict as exc:
                    await self._fail_remote_id_conflict(task_id, account_id, token, exc)
                    return
                if not task:
                    return
                if _requires_manual_submit(result):
                    async with self.assignment_lock:
                        self._audit("account_released", task_id=task_id, account_id=account_id, release_reason="manual_submit_required")
                        account = await crud.get_account(self.db, account_id)
                        await crud.guarded_release_manual_submit_required(
                            self.db,
                            task_id,
                            account_id,
                            token["lease_owner"],
                            token["lease_version"],
                            int((account or {}).get("lock_version") or 0),
                            worker_job_id=worker_job_id,
                            submission_confirmed_at=crud.utc_now(),
                            remaining_credits=result.get("remaining_credits"),
                        )
                    return
                async with self.assignment_lock:
                    await crud.guarded_update_task(
                        self.db,
                        task_id,
                        token["lease_owner"],
                        token["lease_version"],
                        "submitted",
                        worker_job_id=worker_job_id,
                        submission_confirmed_at=crud.utc_now(),
                        remaining_credits=result.get("remaining_credits"),
                    )
                task = await crud.get_task(self.db, task_id)
            while not self._stopping:
                task = await crud.get_task(self.db, task_id)
                if not task or task["status"] == "completed":
                    return
                current_worker = await self._bound_ready_worker(task_id, account_id)
                if not current_worker:
                    return
                worker = current_worker
                try:
                    token = _lease_token(task)
                    account = await crud.get_account(self.db, account_id)
                    async with self.assignment_lock:
                        ok = await crud.heartbeat(self.db, task_id, account_id, token["lease_owner"], token["lease_version"], int((account or {}).get("lock_version") or 0))
                    if not ok:
                        self._audit("fencing_lost", task_id=task_id, action="heartbeat")
                        return
                    result = await self.worker_client.get_omni_video(worker, task["worker_job_id"])
                    if task_id in self._fencing_lost:
                        self._audit("fencing_lost", task_id=task_id, action="after_poll")
                        return
                    try:
                        task = await self._sync_remote_ids(task_id, token, result, "poll")
                    except RemoteIdConflict as exc:
                        await self._fail_remote_id_conflict(task_id, account_id, token, exc)
                        return
                    if not task:
                        return
                    if result.get("status") == "completed" and _valid_local_mp4(result.get("video_path"))["ok"]:
                        pass
                    elif result.get("status") in {"waiting_download", "completed_remote"}:
                        async with self.assignment_lock:
                            downloading = await crud.guarded_transition(
                                self.db,
                                task_id,
                                token["lease_owner"],
                                token["lease_version"],
                                task.get("status"),
                                "downloading",
                                download_attempts=("increment", 1),
                            )
                        if not downloading:
                            self._audit("fencing_lost", task_id=task_id, action="downloading")
                            return
                        result = await self.worker_client.retry_omni_video_download(worker, task["worker_job_id"])
                        try:
                            task = await self._sync_remote_ids(task_id, token, result, "download")
                        except RemoteIdConflict as exc:
                            await self._fail_remote_id_conflict(task_id, account_id, token, exc)
                            return
                        if not task:
                            return
                except Exception as exc:
                    async with self.assignment_lock:
                        await crud.update_task_status(self.db, task_id, "waiting_recovery", error_code=type(exc).__name__, error_message=str(exc)[:500])
                    await asyncio.sleep(2)
                    continue
                mapped = _map_worker_status(result.get("status"))
                if mapped == "completed":
                    video_path = result.get("video_path")
                    video_check = _valid_local_mp4(video_path)
                    if not video_check["ok"]:
                        async with self.assignment_lock:
                            self._audit("account_released", task_id=task_id, account_id=account_id, release_reason=video_check["error_code"])
                            account = await crud.get_account(self.db, account_id)
                            await crud.guarded_release_account(
                                self.db,
                                task_id,
                                account_id,
                                token["lease_owner"],
                                token["lease_version"],
                                int((account or {}).get("lock_version") or 0),
                                "download_failed",
                                error_code=result.get("error_code") or video_check["error_code"],
                                error_message=result.get("error_message") or video_check["error_message"],
                                remaining_credits=result.get("remaining_credits"),
                                last_error_code=result.get("error_code") or video_check["error_code"],
                                last_error_message=result.get("error_message") or video_check["error_message"],
                            )
                        return
                    async with self.assignment_lock:
                        self._audit("account_released", task_id=task_id, account_id=account_id, release_reason="completed")
                        account = await crud.get_account(self.db, account_id)
                        completed = await crud.guarded_complete_real_task(
                            self.db,
                            task_id,
                            account_id,
                            token["lease_owner"],
                            token["lease_version"],
                            int((account or {}).get("lock_version") or 0),
                            video_path,
                            result.get("remaining_credits"),
                        )
                        if not completed:
                            self._audit("fencing_lost", task_id=task_id, action="completed")
                            return
                    await self.schedule_once()
                    return
                if mapped == "manual_review":
                    async with self.assignment_lock:
                        manual_status = "manual_submit_required" if _requires_manual_submit(result) else "manual_review"
                        self._audit("account_released", task_id=task_id, account_id=account_id, release_reason=manual_status)
                        if manual_status == "manual_submit_required":
                            account = await crud.get_account(self.db, account_id)
                            await crud.guarded_release_manual_submit_required(
                                self.db,
                                task_id,
                                account_id,
                                token["lease_owner"],
                                token["lease_version"],
                                int((account or {}).get("lock_version") or 0),
                                remaining_credits=result.get("remaining_credits"),
                            )
                        else:
                            await crud.guarded_update_task(
                                self.db,
                                task_id,
                                token["lease_owner"],
                                token["lease_version"],
                                manual_status,
                                error_code=result.get("error_code"),
                                error_message=result.get("error_message"),
                                remaining_credits=result.get("remaining_credits"),
                            )
                    return
                async with self.assignment_lock:
                    await crud.guarded_update_task(self.db, task_id, token["lease_owner"], token["lease_version"], mapped)
                await asyncio.sleep(5)
        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
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

    def _audit(self, event, **fields):
        safe = {"event": event, **fields}
        logger.info("gateway_audit %s", self.safe_json(safe))

    def safe_json(self, payload: dict) -> str:
        safe = {}
        for key, value in payload.items():
            text = str(value).lower()
            if any(blocked in text for blocked in ("cookie", "token", "authorization", "secret", "nonce")):
                safe[key] = "[redacted]"
            else:
                safe[key] = value
        return json.dumps(safe, sort_keys=True)

    async def _sync_remote_ids(self, task_id: str, token: dict, result: dict, stage: str):
        updates = {}
        task = await crud.get_task(self.db, task_id)
        if not task:
            return None
        for field in REMOTE_ID_FIELDS:
            incoming = result.get(field)
            if not incoming:
                continue
            current = task.get(field)
            if current and current != incoming:
                self._audit("remote_id_conflict", task_id=task_id, stage=stage, field=field)
                raise RemoteIdConflict(field, current, incoming)
            if not current:
                updates[field] = incoming
        if not updates:
            return task
        async with self.assignment_lock:
            synced = await crud.guarded_update_task(self.db, task_id, token["lease_owner"], token["lease_version"], **updates)
        if not synced:
            self._audit("fencing_lost", task_id=task_id, action=f"sync_remote_ids_{stage}")
            return None
        self._audit("remote_ids_synced", task_id=task_id, stage=stage, fields=sorted(updates))
        return synced

    async def _fail_remote_id_conflict(self, task_id: str, account_id: str, token: dict, exc: RemoteIdConflict):
        async with self.assignment_lock:
            account = await crud.get_account(self.db, account_id)
            await crud.guarded_release_account(
                self.db,
                task_id,
                account_id,
                token["lease_owner"],
                token["lease_version"],
                int((account or {}).get("lock_version") or 0),
                "manual_review",
                error_code="remote_id_conflict",
                error_message=f"{exc.field} changed during worker response handling",
                last_error_code="remote_id_conflict",
                last_error_message=f"{exc.field} changed during worker response handling",
                lease_owner=None,
                lease_expires_at=None,
            )

    def _start_task_heartbeat(self, task_id, account_id, token):
        task = asyncio.create_task(self._heartbeat_loop(task_id, account_id, token))
        self._heartbeat_tasks[task_id] = task
        task.add_done_callback(lambda _task, _task_id=task_id: self._heartbeat_tasks.pop(_task_id, None))
        return task

    async def _heartbeat_loop(self, task_id, account_id, token):
        try:
            while not self._stopping:
                await asyncio.sleep(self.settings.heartbeat_interval_seconds)
                account = await crud.get_account(self.db, account_id)
                async with self.assignment_lock:
                    ok = await crud.heartbeat(
                        self.db,
                        task_id,
                        account_id,
                        token["lease_owner"],
                        token["lease_version"],
                        int((account or {}).get("lock_version") or 0),
                        self.settings.lease_duration_seconds,
                    )
                if not ok:
                    self._fencing_lost.add(task_id)
                    self._audit("fencing_lost", task_id=task_id, action="heartbeat_loop")
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._audit("heartbeat_error", task_id=task_id, exception_type=type(exc).__name__, error_message=str(exc)[:500])
            self._fencing_lost.add(task_id)
        finally:
            self._heartbeat_tasks.pop(task_id, None)

    async def _bound_ready_worker(self, task_id, account_id):
        task = await crud.get_task(self.db, task_id)
        worker = self._worker_by_account(account_id)
        if not task or not worker:
            return None
        if task.get("status") in {"project_creation_unknown", "submission_unknown"}:
            self._audit("remote_state_unknown_no_auto_action", task_id=task_id, status=task.get("status"))
            return None
        if worker.account_id != task.get("assigned_account_id"):
            await crud.release_task_for_manual_review(self.db, task_id, account_id, error_code="assigned_account_mismatch", error_message="Assigned account changed before submit")
            return None
        if task.get("assigned_runtime_instance_id") and worker.runtime_instance_id != task.get("assigned_runtime_instance_id"):
            await crud.release_task_for_manual_review(self.db, task_id, account_id, error_code="stale_runtime_instance", error_message="Runtime instance changed after assignment")
            return None
        account = await crud.get_account(self.db, account_id)
        if not account or account.get("current_task_id") != task_id:
            self._audit(
                "account_binding_lost",
                task_id=task_id,
                expected_account_id=account_id,
                actual_current_task_id=(account or {}).get("current_task_id"),
                account_status=(account or {}).get("status"),
            )
            self._audit("account_released", task_id=task_id, account_id=account_id, release_reason="account_not_bound")
            await crud.release_task_for_manual_review(self.db, task_id, account_id, error_code="account_not_bound", error_message="Account is no longer bound to this task")
            return None
        try:
            info = await self.worker_client.inspect(worker)
        except Exception as exc:
            await crud.release_task_for_manual_review(self.db, task_id, account_id, error_code="worker_offline", error_message=str(exc)[:500])
            return None
        if info.get("status") == "offline" or not info.get("extension_connected") or not info.get("flow_key_present"):
            await crud.release_task_for_manual_review(self.db, task_id, account_id, error_code="worker_not_ready", error_message="Worker is no longer ready")
            return None
        return worker


def _now_marker():
    return "dry-run"


def _lease_token(task: dict) -> dict:
    return {
        "lease_owner": task.get("lease_owner"),
        "lease_version": int(task.get("lease_version") or 0),
    }


def _project_payload(task_id):
    return {
        "name": f"Flow Gateway Task {task_id}",
        "language": "en",
        "material": "realistic",
        "allow_music": False,
        "allow_voice": False,
    }


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


def _requires_manual_submit(result: dict) -> bool:
    text = f"{result.get('error_code') or ''} {result.get('error_message') or ''}".lower()
    return (
        "403" in text
        and "permission_denied" in text
        and "recaptcha evaluation failed" in text
        and "public_error_unusual_activity" in text
    )


def _valid_local_mp4(video_path, min_size_bytes: int = 1024) -> dict:
    if not video_path:
        return {"ok": False, "error_code": "missing_video_path", "error_message": "Worker completed without a local video_path"}
    path = Path(video_path)
    try:
        if not path.exists():
            return {"ok": False, "error_code": "missing_video_path", "error_message": "Worker completed but local video_path does not exist"}
        if path.stat().st_size < min_size_bytes:
            return {"ok": False, "error_code": "video_too_small", "error_message": "Local video is too small to be valid"}
        with path.open("rb") as fh:
            header = fh.read(8)
        if len(header) < 8 or header[4:8] != b"ftyp":
            return {"ok": False, "error_code": "invalid_local_mp4", "error_message": "Local video is not a valid MP4 ftyp file"}
    except OSError as exc:
        return {"ok": False, "error_code": "video_path_error", "error_message": str(exc)[:500]}
    return {"ok": True, "error_code": None, "error_message": None}
