"""Tkinter storyboard batch GUI."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
from types import SimpleNamespace
from pathlib import Path
from tkinter import filedialog, messagebox
import tkinter as tk
from tkinter import ttk

from runtime.gateway_projection import GatewayProjection
from runtime.process_manager import RuntimeManager
from runtime.window_manager import WindowManager

from .storyboard_batch import run_storyboard_batch
from .worker_client import WorkerClient


DEFAULT_ACCOUNT_IDS = {"FLOW-025", "FLOW-026", "FLOW-027"}
REQUIRED_CREDITS = 15
SELECTION_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "storyboard_gui_selection.db"


class StoryboardGui(tk.Tk):
    def __init__(self, batch_runner=run_storyboard_batch):
        super().__init__()
        self.title("Flow Storyboard Video Maker")
        self.geometry("1200x760")
        self.batch_runner = batch_runner
        self.shots: list[dict] = []
        self.account_rows: dict[str, dict] = {}
        self.selection_store = AccountSelectionStore(SELECTION_DB_PATH)
        self.selected_account_ids: set[str] = set()
        self.batch_thread: threading.Thread | None = None
        self.preflight_thread: threading.Thread | None = None
        self.last_result: dict | None = None
        self.start_button: ttk.Button | None = None
        self.preflight_button: ttk.Button | None = None
        self._build()
        self.refresh_accounts()

    def _build(self) -> None:
        root = ttk.Frame(self, padding=8)
        root.pack(fill="both", expand=True)
        panes = ttk.PanedWindow(root, orient="vertical")
        panes.pack(fill="both", expand=True)

        account_frame = ttk.LabelFrame(panes, text="账号状态")
        panes.add(account_frame, weight=1)
        account_columns = (
            "selected_for_batch",
            "registration_status",
            "runtime_status",
            "eligible",
            "credits",
            "current_task_id",
            "exclusion_reasons",
        )
        self.account_tree = ttk.Treeview(account_frame, columns=account_columns, show="tree headings", height=6)
        self.account_tree.heading("#0", text="account_id")
        self.account_tree.column("#0", width=100)
        for col in account_columns:
            self.account_tree.heading(col, text=col)
            self.account_tree.column(col, width=135)
        self.account_tree.pack(side="left", fill="both", expand=True)
        self.account_tree.bind("<Double-1>", lambda _event: self.toggle_selected_account())

        account_buttons = ttk.Frame(account_frame)
        account_buttons.pack(side="right", fill="y")
        ttk.Button(account_buttons, text="刷新账号", command=self.refresh_accounts).pack(fill="x")
        ttk.Button(account_buttons, text="切换参与", command=self.toggle_selected_account).pack(fill="x")
        self.preflight_button = ttk.Button(account_buttons, text="检查账号与额度", command=self.check_preflight)
        self.preflight_button.pack(fill="x")
        ttk.Button(account_buttons, text="打开所选账号Flow窗口", command=self.open_selected_flow).pack(fill="x")
        ttk.Button(account_buttons, text="启动所选账号", command=self.start_selected_account).pack(fill="x")
        ttk.Button(account_buttons, text="停止所选账号", command=self.stop_selected_account).pack(fill="x")

        shot_frame = ttk.LabelFrame(panes, text="分镜任务")
        panes.add(shot_frame, weight=4)
        columns = (
            "image_path",
            "prompt",
            "duration",
            "aspect_ratio",
            "status",
            "assigned_account_id",
            "project_id",
            "worker_job_id",
            "progress",
            "video_path",
            "error_code",
        )
        self.shot_tree = ttk.Treeview(shot_frame, columns=columns, show="tree headings")
        self.shot_tree.heading("#0", text="shot_id")
        self.shot_tree.column("#0", width=80)
        for col in columns:
            self.shot_tree.heading(col, text=col)
            self.shot_tree.column(col, width=120)
        self.shot_tree.pack(fill="both", expand=True)
        shot_buttons = ttk.Frame(shot_frame)
        shot_buttons.pack(fill="x")
        for text, command in (
            ("添加分镜", self.add_shot),
            ("编辑分镜", self.edit_shot),
            ("删除分镜", self.delete_shot),
            ("清空", self.clear_shots),
            ("从JSON导入", self.import_json),
            ("导出JSON", self.export_json),
            ("加载批次结果", self.load_batch_result),
            ("上移", lambda: self.move_shot(-1)),
            ("下移", lambda: self.move_shot(1)),
            ("打开对应Flow项目", self.open_selected_flow_project),
            ("人工生成后检查结果", self.check_manual_result),
            ("开始制作", self.start_batch),
            ("打开输出目录", self.open_output_dir),
            ("打开所选视频", self.open_selected_video),
        ):
            button = ttk.Button(shot_buttons, text=text, command=command)
            button.pack(side="left")
            if text == "开始制作":
                self.start_button = button
        ttk.Button(shot_buttons, text="重新下载选中视频", command=self.retry_download_selected_video).pack(side="left")

        settings = ttk.LabelFrame(root, text="批量设置")
        settings.pack(fill="x")
        self.concurrency = tk.IntVar(value=3)
        self.timeout_seconds = tk.IntVar(value=1200)
        self.output_dir = tk.StringVar(value="")
        ttk.Label(settings, text="最大并发").pack(side="left")
        ttk.Combobox(settings, textvariable=self.concurrency, values=(1, 2, 3), width=4, state="readonly").pack(side="left")
        ttk.Label(settings, text="每任务超时").pack(side="left")
        ttk.Entry(settings, textvariable=self.timeout_seconds, width=8).pack(side="left")
        ttk.Label(settings, text="输出目录").pack(side="left")
        ttk.Entry(settings, textvariable=self.output_dir, width=60).pack(side="left", fill="x", expand=True)
        ttk.Button(settings, text="浏览", command=self.choose_output_dir).pack(side="left")
        self.auto_scroll = tk.BooleanVar(value=True)
        ttk.Checkbutton(settings, text="自动滚动日志", variable=self.auto_scroll).pack(side="left")

        self.log = tk.Text(root, height=7, state="disabled")
        self.log.pack(fill="x")

    def refresh_accounts(self) -> None:
        self.account_rows = {}
        for candidate in GatewayProjection().candidates():
            data = candidate.to_dict()
            account_id = data["account_id"]
            selected = self.selection_store.get(account_id)
            if selected is None:
                selected = account_id in DEFAULT_ACCOUNT_IDS
                self.selection_store.set(account_id, selected)
            self.account_rows[account_id] = {
                "account_id": account_id,
                "registration_status": data.get("registration_status"),
                "runtime_status": data.get("runtime_status"),
                "eligible": data.get("eligible"),
                "credits": data.get("credits"),
                "current_task_id": data.get("current_task_id"),
                "exclusion_reasons": data.get("exclusion_reasons"),
            }
            if selected:
                self.selected_account_ids.add(account_id)
            else:
                self.selected_account_ids.discard(account_id)
        self._render_accounts()

    def _render_accounts(self) -> None:
        self.account_tree.delete(*self.account_tree.get_children())
        for account_id in sorted(self.account_rows):
            row = self.account_rows[account_id]
            self.account_tree.insert("", "end", iid=account_id, text=account_id, values=(
                "yes" if account_id in self.selected_account_ids else "no",
                row.get("registration_status"),
                row.get("runtime_status"),
                row.get("eligible"),
                row.get("credits"),
                row.get("current_task_id"),
                row.get("exclusion_reasons"),
            ))

    def selected_account(self) -> str | None:
        selection = self.account_tree.selection()
        return selection[0] if selection else None

    def get_batch_account_ids(self) -> list[str]:
        order = [account_id for account_id in sorted(self.account_rows) if account_id in self.selected_account_ids]
        extras = sorted(self.selected_account_ids.difference(self.account_rows))
        return order + extras

    def toggle_selected_account(self) -> None:
        account_id = self.selected_account()
        if not account_id:
            return
        selected = account_id not in self.selected_account_ids
        try:
            self.selection_store.set(account_id, selected)
        except Exception as exc:
            messagebox.showerror("保存失败", f"账号参与状态保存失败：{self._safe(exc)}")
            self._render_accounts()
            return
        if selected:
            self.selected_account_ids.add(account_id)
        else:
            self.selected_account_ids.discard(account_id)
        self._log(f"account selection saved account_id={account_id} selected_for_batch={selected}")
        self._render_accounts()

    def open_selected_flow(self) -> None:
        account_id = self.selected_account()
        if account_id:
            self._threaded("open_flow", lambda: WindowManager().open_or_focus_flow(account_id).to_dict())

    def start_selected_account(self) -> None:
        account_id = self.selected_account()
        if account_id:
            self._threaded("start_account", lambda: RuntimeManager().start_one(account_id).__dict__)

    def stop_selected_account(self) -> None:
        account_id = self.selected_account()
        if account_id:
            self._threaded("stop_account", lambda: RuntimeManager().stop_one(account_id).__dict__)

    def add_shot(self) -> None:
        item = self._shot_dialog()
        if item:
            self.shots.append(item)
            self._render_shots()

    def edit_shot(self) -> None:
        index = self._selected_shot_index()
        if index is None:
            return
        item = self._shot_dialog(self.shots[index])
        if item:
            self.shots[index] = item
            self._render_shots()

    def delete_shot(self) -> None:
        index = self._selected_shot_index()
        if index is not None:
            self.shots.pop(index)
            self._render_shots()

    def clear_shots(self) -> None:
        self.shots.clear()
        self._render_shots()

    def move_shot(self, delta: int) -> None:
        index = self._selected_shot_index()
        if index is None:
            return
        new = index + delta
        if 0 <= new < len(self.shots):
            self.shots[index], self.shots[new] = self.shots[new], self.shots[index]
            self._render_shots()

    def import_json(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("JSON", "*.json")])
        if path:
            self.shots = json.loads(Path(path).read_text(encoding="utf-8"))
            self._render_shots()

    def export_json(self) -> None:
        path = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON", "*.json")])
        if path:
            Path(path).write_text(json.dumps(self.shots, indent=2, ensure_ascii=False), encoding="utf-8")

    def load_batch_result(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("Batch result", "batch-result*.json"), ("JSON", "*.json")])
        if path:
            self.load_batch_result_file(Path(path))

    def load_batch_result_file(self, path: Path) -> dict:
        result = json.loads(path.read_text(encoding="utf-8"))
        self.last_result = result
        self._apply_result_tasks(result)
        self._log(f"loaded result ok={result.get('ok')} path={path}")
        return result

    def check_preflight(self) -> None:
        if self.preflight_thread and self.preflight_thread.is_alive():
            return
        if not self.shots:
            messagebox.showerror("错误", "请先添加或导入分镜")
            return
        self._set_button_state(self.preflight_button, "disabled")
        args = self._build_batch_args(preflight_only=True)

        def work():
            result = self.batch_runner(args)
            self.after(0, lambda: self._preflight_done(result))

        self.preflight_thread = threading.Thread(target=work, daemon=True)
        self.preflight_thread.start()
        self._log("preflight started")

    def _preflight_done(self, result: dict) -> None:
        self._set_button_state(self.preflight_button, "normal")
        self.last_result = result
        self._apply_preflight_accounts(result)
        self._log(f"preflight result={result.get('result')} ok={result.get('ok')}")

    def start_batch(self) -> None:
        if self.batch_thread and self.batch_thread.is_alive():
            return
        if not self.shots:
            messagebox.showerror("错误", "请先添加或导入分镜")
            return
        if not self.get_batch_account_ids():
            messagebox.showerror("错误", "请选择至少一个账号")
            return
        self._set_button_state(self.start_button, "disabled")
        args = self._build_batch_args(preflight_only=False)

        def work():
            try:
                result = self.batch_runner(args)
            except Exception as exc:
                result = {
                    "ok": False,
                    "result": "gateway_start_failed",
                    "stage": "gateway_startup",
                    "error_code": "gateway_start_failed",
                    "error_message": self._safe(exc),
                    "run_dir": str(Path(args.output_dir or ".").resolve()),
                }
            self.after(0, lambda: self._batch_done(result))

        self.batch_thread = threading.Thread(target=work, daemon=True)
        self.batch_thread.start()
        self._log(f"batch started accounts={','.join(self.get_batch_account_ids())}")

    def _batch_done(self, result: dict) -> None:
        self.last_result = result
        self._set_button_state(self.start_button, "normal")
        self._apply_preflight_accounts(result)
        self._apply_result_tasks(result)
        self._log(f"batch done ok={result.get('ok')} result={result.get('result')} stage={result.get('stage')} error_code={result.get('error_code')} run_dir={result.get('run_dir')}")
        if not result.get("ok"):
            title = self._error_title(result)
            details = self._manual_submit_details(result)
            messagebox.showerror(
                title,
                f"stage={result.get('stage')}\nerror_code={result.get('error_code')}\nerror_message={self._safe(result.get('error_message'))}\nrun_dir={result.get('run_dir')}{details}",
            )

    def _build_batch_args(self, preflight_only: bool) -> argparse.Namespace:
        manifest = self._write_manifest()
        return argparse.Namespace(
            manifest=str(manifest),
            concurrency=int(self.concurrency.get()),
            output_dir=self.output_dir.get() or None,
            timeout_seconds=int(self.timeout_seconds.get()),
            gateway_port=None,
            test_mode=False,
            account_ids=",".join(self.get_batch_account_ids()),
            preflight_only=preflight_only,
        )

    def _write_manifest(self) -> Path:
        base = Path(self.output_dir.get()).resolve() if self.output_dir.get() else Path("tmp_multi_storyboard_gui").resolve()
        base.mkdir(parents=True, exist_ok=True)
        manifest = base / "storyboard-manifest.json"
        manifest.write_text(json.dumps(self.shots, indent=2, ensure_ascii=False), encoding="utf-8")
        return manifest

    def _apply_preflight_accounts(self, result: dict) -> None:
        for account in result.get("accounts", []):
            account_id = account.get("account_id")
            if account_id:
                self.account_rows[account_id] = {**self.account_rows.get(account_id, {}), **account}
        self._render_accounts()

    def _apply_result_tasks(self, result: dict) -> None:
        rows = result.get("tasks", [])
        if not rows:
            return
        by_id = {str(shot.get("shot_id")): shot for shot in self.shots}
        for row in rows:
            shot_id = str(row.get("shot_id") or "")
            if shot_id in by_id:
                by_id[shot_id].update(row)
            else:
                self.shots.append({
                    "shot_id": shot_id,
                    "image": row.get("image") or row.get("image_path") or "",
                    "prompt": row.get("prompt") or "",
                    "duration": row.get("duration") or 10,
                    "aspect_ratio": row.get("aspect_ratio") or "9:16",
                    **row,
                })
        self._render_shots()

    def choose_output_dir(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.output_dir.set(path)

    def open_output_dir(self) -> None:
        path = self.last_result.get("run_dir") if self.last_result else self.output_dir.get()
        if path:
            Path(path).mkdir(parents=True, exist_ok=True)
            os.startfile(path)

    def open_selected_video(self) -> None:
        path = self.selected_video_path()
        if path:
            os.startfile(path)

    def open_selected_flow_project(self) -> None:
        shot = self._selected_shot()
        if not shot:
            return
        account_id = shot.get("assigned_account_id")
        project_id = shot.get("project_id")
        if account_id and project_id:
            self._threaded("open_flow_project", lambda: WindowManager().open_or_focus_flow_project(account_id, project_id).to_dict())

    def check_manual_result(self) -> None:
        shot = self._selected_shot()
        if not shot:
            return
        self._threaded("manual_result_check", lambda: self._check_manual_result_for_shot(shot))

    def _check_manual_result_for_shot(self, shot: dict) -> dict:
        account_id = shot.get("assigned_account_id")
        project_id = shot.get("project_id")
        account = self.account_rows.get(account_id or "", {})
        api_url = account.get("worker_api_endpoint") or account.get("api_url")
        if not project_id or not api_url:
            return {"result": "manual_result_not_checkable", "shot_id": shot.get("shot_id"), "project_id": shot.get("project_id")}
        worker = SimpleNamespace(api_url=api_url)
        exclude_media_ids = self._failed_job_media_ids(worker, shot.get("worker_job_id"))
        client = WorkerClient()
        candidates = self._run_async(client.list_manual_flow_results(
            worker,
            project_id,
            after=shot.get("manual_submit_required_at"),
            exclude_media_ids=exclude_media_ids,
        )).get("candidates", [])
        if not candidates:
            shot["status"] = shot.get("status") or "manual_submit_required"
            self.after(0, self._render_shots)
            return {"result": "not_completed", "status": shot.get("status"), "candidate_count": 0}
        if len(candidates) > 1:
            shot["status"] = "manual_result_ambiguous"
            shot["error_code"] = "manual_result_ambiguous"
            shot["error_message"] = f"Multiple manual video candidates: {len(candidates)}"
            self._update_gateway_task_from_shot(shot)
            self.after(0, self._render_shots)
            return {"result": "manual_result_ambiguous", "candidate_count": len(candidates)}
        candidate = candidates[0]
        result = self._run_async(client.download_manual_flow_result(worker, candidate["media_id"]))
        video_path = result.get("video_path")
        if result.get("status") == "completed" and video_path and self._valid_mp4(Path(video_path)):
            shot.update({
                "status": "completed",
                "video_path": video_path,
                "error_code": None,
                "error_message": None,
                "manual_result_media_id": candidate.get("media_id"),
                "manual_result_operation_id": candidate.get("operation_id"),
                "manual_result_source": result.get("source") or "manual_project_media",
            })
            self._update_gateway_task_from_shot(shot)
            self.after(0, self._render_shots)
            return {"result": "completed", "video_path": video_path, "media_id": candidate.get("media_id")}
        shot["status"] = "manual_submit_required"
        self.after(0, self._render_shots)
        return {"result": "not_completed", "status": shot.get("status"), "error_code": result.get("error_code")}

    def _failed_job_media_ids(self, worker, worker_job_id: str | None) -> set[str]:
        if not worker_job_id:
            return set()
        try:
            result = self._run_async(WorkerClient().get_omni_video(worker, worker_job_id))
        except Exception:
            return set()
        return {value for value in (result.get("input_media_id"), result.get("output_media_id")) if value}

    def _update_gateway_task_from_shot(self, shot: dict) -> None:
        task_id = shot.get("task_id")
        if not task_id:
            return
        db_path = self._gateway_db_path()
        if not db_path or not db_path.exists():
            return
        with sqlite3.connect(db_path) as db:
            db.execute(
                """
                UPDATE flow_tasks
                SET status=?, video_path=?, error_code=?, error_message=?,
                    manual_result_media_id=?, manual_result_operation_id=?, manual_result_source=?,
                    completed_at=CASE WHEN ?='completed' THEN strftime('%Y-%m-%dT%H:%M:%SZ', 'now') ELSE completed_at END,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE task_id=?
                """,
                (
                    shot.get("status"),
                    shot.get("video_path"),
                    shot.get("error_code"),
                    shot.get("error_message"),
                    shot.get("manual_result_media_id"),
                    shot.get("manual_result_operation_id"),
                    shot.get("manual_result_source"),
                    shot.get("status"),
                    task_id,
                ),
            )
            db.commit()

    def _gateway_db_path(self) -> Path | None:
        if self.last_result and self.last_result.get("run_dir"):
            return Path(self.last_result["run_dir"]) / "gateway.db"
        return None

    def selected_video_path(self) -> str | None:
        index = self._selected_shot_index()
        if index is not None:
            return self.shots[index].get("video_path")
        return None

    def retry_download_selected_video(self) -> None:
        shot = self._selected_shot()
        db_path = self._gateway_db_path()
        if not shot or not db_path:
            return
        account_id = shot.get("assigned_account_id")
        if not account_id:
            return
        run_dir = db_path.parent

        def work():
            from .download_recovery import retry_downloads

            result = retry_downloads(argparse.Namespace(run_dir=str(run_dir), account_id=[account_id], execute=True))
            self.after(0, lambda: self._apply_retry_download_tasks(result.get("tasks", [])))
            return result

        self._threaded("retry_download", work)

    def _apply_retry_download_tasks(self, rows: list[dict]) -> None:
        by_task_id = {str(shot.get("task_id")): shot for shot in self.shots if shot.get("task_id")}
        for row in rows:
            task_id = str(row.get("task_id") or "")
            shot = by_task_id.get(task_id)
            if not shot:
                continue
            if row.get("gateway_status_after"):
                shot["status"] = row.get("gateway_status_after")
            if row.get("video_path_after"):
                shot["video_path"] = row.get("video_path_after")
            if row.get("error_code"):
                shot["error_code"] = row.get("error_code")
            if row.get("error_message"):
                shot["error_message"] = row.get("error_message")
        self._render_shots()

    def _selected_shot(self) -> dict | None:
        index = self._selected_shot_index()
        return self.shots[index] if index is not None else None

    def _shot_dialog(self, current: dict | None = None) -> dict | None:
        dialog = tk.Toplevel(self)
        dialog.title("分镜")
        values = current or {"shot_id": f"{len(self.shots)+1:03d}", "image": "", "prompt": "", "duration": 10, "aspect_ratio": "9:16"}
        shot_id = tk.StringVar(value=values.get("shot_id", ""))
        image = tk.StringVar(value=values.get("image") or values.get("image_path", ""))
        aspect = tk.StringVar(value=values.get("aspect_ratio", "9:16"))
        result: dict | None = None
        ttk.Label(dialog, text="分镜编号").pack(fill="x")
        ttk.Entry(dialog, textvariable=shot_id).pack(fill="x")
        ttk.Label(dialog, text="图片路径").pack(fill="x")
        row = ttk.Frame(dialog)
        row.pack(fill="x")
        ttk.Entry(row, textvariable=image).pack(side="left", fill="x", expand=True)
        ttk.Button(row, text="浏览图片", command=lambda: image.set(filedialog.askopenfilename(filetypes=[("Images", "*.png;*.jpg;*.jpeg;*.webp")]) or image.get())).pack(side="left")
        ttk.Label(dialog, text="完整提示词").pack(fill="x")
        prompt = tk.Text(dialog, height=8)
        prompt.insert("1.0", values.get("prompt", ""))
        prompt.pack(fill="both", expand=True)
        ttk.Label(dialog, text="时长固定10秒").pack(fill="x")
        ttk.Combobox(dialog, textvariable=aspect, values=("9:16", "16:9"), state="readonly").pack(fill="x")

        def ok():
            nonlocal result
            result = {
                "shot_id": shot_id.get().strip(),
                "image": image.get().strip(),
                "prompt": prompt.get("1.0", "end").strip(),
                "duration": 10,
                "aspect_ratio": aspect.get(),
            }
            dialog.destroy()

        ttk.Button(dialog, text="确定", command=ok).pack()
        dialog.wait_window()
        return result

    def _selected_shot_index(self) -> int | None:
        selection = self.shot_tree.selection()
        return int(selection[0]) if selection else None

    def _render_shots(self) -> None:
        self.shot_tree.delete(*self.shot_tree.get_children())
        for index, shot in enumerate(self.shots):
            prompt = (shot.get("prompt") or "")[:40]
            self.shot_tree.insert("", "end", iid=str(index), text=shot.get("shot_id"), values=(
                shot.get("image") or shot.get("image_path"),
                prompt,
                shot.get("duration"),
                shot.get("aspect_ratio"),
                shot.get("status"),
                shot.get("assigned_account_id"),
                shot.get("project_id"),
                shot.get("worker_job_id"),
                shot.get("progress"),
                shot.get("video_path"),
                shot.get("error_code"),
            ))

    def _threaded(self, name: str, func) -> None:
        def work():
            try:
                result = func()
                self.after(0, lambda: self._log(f"{name} ok {self._safe(result)}"))
            except Exception as exc:
                self.after(0, lambda: self._log(f"{name} failed {self._safe(exc)}"))

        threading.Thread(target=work, daemon=True).start()

    def _set_button_state(self, button, state: str) -> None:
        if button is not None:
            button.configure(state=state)

    def _run_async(self, coro):
        import asyncio

        return asyncio.run(coro)

    def _valid_mp4(self, path: Path) -> bool:
        try:
            if not path.exists() or path.stat().st_size <= 0:
                return False
            with path.open("rb") as fh:
                return fh.read(8)[4:8] == b"ftyp"
        except OSError:
            return False

    def _log(self, text: str) -> None:
        self.log.configure(state="normal")
        self.log.insert("end", self._safe(text) + "\n")
        self.log.configure(state="disabled")
        if self.auto_scroll.get():
            self.log.see("end")

    def _safe(self, value) -> str:
        text = str(value)[:500]
        lowered = text.lower()
        for blocked in ("cookie", "token", "authorization", "secret", "nonce"):
            if blocked in lowered:
                return "[redacted]"
        return text

    def _error_title(self, result: dict) -> str:
        code = result.get("error_code")
        stage = result.get("stage")
        if code == "UPSTREAM_UNUSUAL_ACTIVITY":
            return "视频自动提交未被Google接受"
        if stage == "gateway_startup":
            return "Gateway启动失败"
        if stage == "project_create":
            return "Flow Project创建失败"
        if stage == "worker_submit":
            return "视频提交失败"
        if stage == "task_download":
            return "视频下载失败"
        return "批量制作失败"

    def _manual_submit_details(self, result: dict) -> str:
        rows = [row for row in result.get("tasks", []) if row.get("status") == "manual_submit_required"]
        if not rows:
            return ""
        lines = ["", "manual_submit_required:"]
        for row in rows:
            lines.append(
                f"shot_id={row.get('shot_id')} account={row.get('assigned_account_id')} project_id={row.get('project_id')}"
            )
        return "\n" + "\n".join(lines)


class AccountSelectionStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as db:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS storyboard_account_selection (
                    account_id TEXT PRIMARY KEY,
                    selected_for_batch INTEGER NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                )
                """
            )
            db.commit()

    def get(self, account_id: str) -> bool | None:
        with sqlite3.connect(self.path) as db:
            row = db.execute(
                "SELECT selected_for_batch FROM storyboard_account_selection WHERE account_id=?",
                (account_id,),
            ).fetchone()
        if row is None:
            return None
        return bool(row[0])

    def set(self, account_id: str, selected: bool) -> None:
        with sqlite3.connect(self.path) as db:
            db.execute(
                """
                INSERT INTO storyboard_account_selection(account_id, selected_for_batch, updated_at)
                VALUES(?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                ON CONFLICT(account_id) DO UPDATE SET
                  selected_for_batch=excluded.selected_for_batch,
                  updated_at=excluded.updated_at
                """,
                (account_id, int(selected)),
            )
            db.commit()


def main() -> None:
    StoryboardGui().mainloop()


if __name__ == "__main__":
    main()
