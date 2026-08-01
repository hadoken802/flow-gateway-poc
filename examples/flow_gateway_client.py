"""Small Python client for Flow Gateway reference-image jobs."""
from __future__ import annotations

import hashlib
import json
import subprocess
import time
from pathlib import Path

import httpx


class FlowGatewayClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8200", api_key: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key or ""

    @property
    def headers(self) -> dict[str, str]:
        return {"X-API-Key": self.api_key} if self.api_key else {}

    def ensure_engine_running(self, project_dir: str | None = None, timeout_seconds: float = 30.0) -> dict:
        if self.ready().get("ready"):
            return {"started": False, "ready": True}
        root = Path(project_dir) if project_dir else Path(__file__).resolve().parents[1]
        subprocess.Popen(
            ["cmd", "/c", str(root / "start_embedded_engine.bat")],
            cwd=str(root),
            creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0),
        )
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            status = self.ready()
            if status.get("ready"):
                status["started"] = True
                return status
            time.sleep(1)
        raise TimeoutError("Flow Gateway engine did not become ready")

    def ready(self) -> dict:
        try:
            response = httpx.get(f"{self.base_url}/api/v1/client/system/ready", headers=self.headers, timeout=5)
            if response.status_code == 401:
                response.raise_for_status()
            if response.status_code >= 400:
                return {"ready": False, "status_code": response.status_code}
            return response.json()
        except httpx.RequestError as exc:
            return {"ready": False, "error": str(exc)}

    def upload_images(self, images: list[str]) -> list[dict]:
        files = []
        handles = []
        try:
            for image in images:
                path = Path(image)
                fh = path.open("rb")
                handles.append(fh)
                files.append(("files", (path.name, fh, _mime_for(path))))
            response = httpx.post(f"{self.base_url}/api/v1/client/files/batch", files=files, headers=self.headers, timeout=120)
            response.raise_for_status()
            items = response.json()["files"]
            failed = [item for item in items if item.get("ok") is False]
            if failed:
                raise RuntimeError(f"Upload failed: {failed}")
            return items
        finally:
            for fh in handles:
                fh.close()

    def create_task(
        self,
        input_file_ids: list[str],
        prompt: str,
        output_path: str | None = None,
        duration: int = 10,
        aspect_ratio: str = "9:16",
        estimated_quota_cost: int = 15,
        priority: int = 10,
        idempotency_key: str | None = None,
        uploaded_files: list[dict] | None = None,
    ) -> dict:
        output = Path(output_path) if output_path else None
        payload = {
            "idempotency_key": idempotency_key or _stable_key(uploaded_files or [{"file_id": item, "sha256": ""} for item in input_file_ids], prompt, duration, aspect_ratio),
            "input_file_ids": input_file_ids,
            "prompt": prompt,
            "duration": duration,
            "aspect_ratio": aspect_ratio,
            "estimated_quota_cost": estimated_quota_cost,
            "priority": priority,
        }
        if output:
            payload["output_directory"] = str(output.parent)
            payload["output_filename"] = output.name
        response = httpx.post(f"{self.base_url}/api/v1/client/tasks", json=payload, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_task(self, task_id: str) -> dict:
        response = httpx.get(f"{self.base_url}/api/v1/client/tasks/{task_id}", headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.json()

    def wait_for_task(self, task_id: str, timeout_seconds: float = 1800.0, poll_seconds: float = 5.0) -> dict:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            task = self.get_task(task_id)
            if task.get("status") in {"completed", "failed", "failed_before_remote_submit", "cancelled", "download_failed", "manual_review", "manual_submit_required"}:
                return task
            time.sleep(poll_seconds)
        raise TimeoutError(f"Task did not finish: {task_id}")

    def cancel_task(self, task_id: str) -> dict:
        response = httpx.post(f"{self.base_url}/api/v1/client/tasks/{task_id}/cancel", headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.json()

    def download_video(self, task_id: str, output_path: str) -> str:
        response = httpx.get(f"{self.base_url}/api/v1/client/tasks/{task_id}/download", headers=self.headers, timeout=120)
        response.raise_for_status()
        dest = Path(output_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(response.content)
        return str(dest)

    def generate(
        self,
        images: list[str],
        prompt: str,
        output_path: str,
        duration: int = 10,
        aspect_ratio: str = "9:16",
        estimated_quota_cost: int = 15,
        priority: int = 10,
        idempotency_key: str | None = None,
        timeout_seconds: float = 1800.0,
        poll_seconds: float = 5.0,
    ) -> dict:
        uploaded = self.upload_images(images)
        task = self.create_task(
            input_file_ids=[item["file_id"] for item in uploaded],
            prompt=prompt,
            output_path=output_path,
            duration=duration,
            aspect_ratio=aspect_ratio,
            estimated_quota_cost=estimated_quota_cost,
            priority=priority,
            idempotency_key=idempotency_key,
            uploaded_files=uploaded,
        )
        final = self.wait_for_task(task["task_id"], timeout_seconds=timeout_seconds, poll_seconds=poll_seconds)
        if final.get("status") == "completed":
            self.download_video(task["task_id"], output_path)
        return final

    def generate_reference_images(self, *args, **kwargs) -> dict:
        return self.generate(*args, **kwargs)


def _stable_key(uploaded: list[dict], prompt: str, duration: int, aspect_ratio: str) -> str:
    material = {
        "files": [{"file_id": item["file_id"], "sha256": item["sha256"]} for item in uploaded],
        "prompt": prompt,
        "duration": duration,
        "aspect_ratio": aspect_ratio,
    }
    digest = hashlib.sha256(json.dumps(material, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    return f"client-multi-ref:{digest}"


def _mime_for(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".webp":
        return "image/webp"
    return "image/png"
