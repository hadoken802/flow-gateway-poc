"""Tkinter storyboard batch GUI."""
from __future__ import annotations

import argparse
import json
import threading
from pathlib import Path
from tkinter import filedialog, messagebox
import tkinter as tk
from tkinter import ttk

from runtime.gateway_projection import GatewayProjection
from runtime.process_manager import RuntimeManager
from runtime.window_manager import WindowManager

from .storyboard_batch import run_storyboard_batch


class StoryboardGui(tk.Tk):
    def __init__(self, batch_runner=run_storyboard_batch):
        super().__init__()
        self.title("Flow Storyboard Video Maker")
        self.geometry("1200x760")
        self.batch_runner = batch_runner
        self.shots: list[dict] = []
        self.batch_thread: threading.Thread | None = None
        self.last_result: dict | None = None
        self._build()
        self.refresh_accounts()

    def _build(self) -> None:
        root = ttk.Frame(self, padding=8)
        root.pack(fill="both", expand=True)
        top = ttk.PanedWindow(root, orient="vertical")
        top.pack(fill="both", expand=True)

        account_frame = ttk.LabelFrame(top, text="账号状态")
        top.add(account_frame, weight=1)
        self.account_tree = ttk.Treeview(account_frame, columns=("registration_status", "runtime_status", "eligible", "credits", "current_task_id", "last_error"), show="tree headings", height=5)
        self.account_tree.heading("#0", text="account_id")
        for col in self.account_tree["columns"]:
            self.account_tree.heading(col, text=col)
            self.account_tree.column(col, width=130)
        self.account_tree.pack(side="left", fill="both", expand=True)
        account_buttons = ttk.Frame(account_frame)
        account_buttons.pack(side="right", fill="y")
        ttk.Button(account_buttons, text="刷新账号", command=self.refresh_accounts).pack(fill="x")
        ttk.Button(account_buttons, text="打开所选账号Flow窗口", command=self.open_selected_flow).pack(fill="x")
        ttk.Button(account_buttons, text="启动所选账号", command=self.start_selected_account).pack(fill="x")
        ttk.Button(account_buttons, text="停止所选账号", command=self.stop_selected_account).pack(fill="x")

        shot_frame = ttk.LabelFrame(top, text="分镜任务")
        top.add(shot_frame, weight=4)
        columns = ("image_path", "prompt", "duration", "aspect_ratio", "status", "assigned_account_id", "project_id", "worker_job_id", "progress", "video_path", "error_code")
        self.shot_tree = ttk.Treeview(shot_frame, columns=columns, show="tree headings")
        self.shot_tree.heading("#0", text="shot_id")
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
            ("上移", lambda: self.move_shot(-1)),
            ("下移", lambda: self.move_shot(1)),
            ("开始制作", self.start_batch),
            ("暂停继续领取新任务", self.pause_not_implemented),
            ("打开输出目录", self.open_output_dir),
            ("打开所选视频", self.open_selected_video),
        ):
            button = ttk.Button(shot_buttons, text=text, command=command)
            button.pack(side="left")
            if text == "开始制作":
                self.start_button = button

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
        self.account_tree.delete(*self.account_tree.get_children())
        for candidate in GatewayProjection().candidates():
            data = candidate.to_dict()
            self.account_tree.insert("", "end", iid=data["account_id"], text=data["account_id"], values=(
                data.get("registration_status"), data.get("runtime_status"), data.get("eligible"), "", data.get("current_task_id"), data.get("exclusion_reasons"),
            ))

    def selected_account(self) -> str | None:
        selection = self.account_tree.selection()
        return selection[0] if selection else None

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

    def start_batch(self) -> None:
        if self.batch_thread and self.batch_thread.is_alive():
            return
        if not self.shots:
            messagebox.showerror("错误", "请先添加分镜")
            return
        self.start_button.configure(state="disabled")
        manifest = Path(self.output_dir.get() or ".").resolve() / "storyboard-manifest.json"
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps(self.shots, indent=2, ensure_ascii=False), encoding="utf-8")
        args = argparse.Namespace(
            manifest=str(manifest),
            concurrency=int(self.concurrency.get()),
            output_dir=self.output_dir.get() or None,
            timeout_seconds=int(self.timeout_seconds.get()),
            gateway_port=None,
            test_mode=False,
        )
        def work():
            result = self.batch_runner(args)
            self.after(0, lambda: self._batch_done(result))
        self.batch_thread = threading.Thread(target=work, daemon=True)
        self.batch_thread.start()
        self._log("batch started")

    def _batch_done(self, result: dict) -> None:
        self.last_result = result
        self.start_button.configure(state="normal")
        for row in result.get("tasks", []):
            for shot in self.shots:
                if shot.get("shot_id") == row.get("shot_id"):
                    shot.update(row)
        self._render_shots()
        self._log(f"batch done ok={result.get('ok')}")

    def pause_not_implemented(self) -> None:
        self._log("pause requested; queued pickup pause is handled by batch service in a future control endpoint")

    def choose_output_dir(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.output_dir.set(path)

    def open_output_dir(self) -> None:
        path = self.last_result.get("run_dir") if self.last_result else self.output_dir.get()
        if path:
            Path(path).mkdir(parents=True, exist_ok=True)
            import os
            os.startfile(path)

    def open_selected_video(self) -> None:
        index = self._selected_shot_index()
        if index is not None and self.shots[index].get("video_path"):
            import os
            os.startfile(self.shots[index]["video_path"])

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
            result = {"shot_id": shot_id.get().strip(), "image": image.get().strip(), "prompt": prompt.get("1.0", "end").strip(), "duration": 10, "aspect_ratio": aspect.get()}
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
                shot.get("image") or shot.get("image_path"), prompt, shot.get("duration"), shot.get("aspect_ratio"),
                shot.get("status"), shot.get("assigned_account_id"), shot.get("project_id"), shot.get("worker_job_id"),
                shot.get("progress"), shot.get("video_path"), shot.get("error_code"),
            ))

    def _threaded(self, name: str, func) -> None:
        def work():
            try:
                result = func()
                self.after(0, lambda: self._log(f"{name} ok {self._safe(result)}"))
            except Exception as exc:
                self.after(0, lambda: self._log(f"{name} failed {self._safe(exc)}"))
        threading.Thread(target=work, daemon=True).start()

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


def main() -> None:
    StoryboardGui().mainloop()


if __name__ == "__main__":
    main()
