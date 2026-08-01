"""Small Python client for Flow Gateway reference-image jobs."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

import httpx


class FlowGatewayClient:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8200",
        api_key: str | None = None,
        engine_mode: str = "existing_pool",
        engine_root: str | None = None,
    ):
        if engine_mode not in {"existing_pool", "clean_embedded"}:
            raise ValueError("engine_mode must be existing_pool or clean_embedded")
        self.base_url = base_url.rstrip("/")
        self.engine_mode = engine_mode
        self.engine_root = Path(engine_root) if engine_root else Path(__file__).resolve().parents[1]
        self.api_key = api_key or _client_key_from_env_file(self.engine_root) or ""
        self.attached_existing_engine = False
        self.started_by_this_client = False
        self._engine_process = None

    @property
    def headers(self) -> dict[str, str]:
        return {"X-API-Key": self.api_key} if self.api_key else {}

    def ensure_engine_running(self, project_dir: str | None = None, timeout_seconds: float = 30.0) -> dict:
        current = self.ready()
        if current.get("ready"):
            current["started"] = False
            current["attached_existing_engine"] = not self.started_by_this_client
            current["started_by_this_client"] = self.started_by_this_client
            self.attached_existing_engine = not self.started_by_this_client
            return current
        health = self.health()
        if health.get("status") == "ok":
            current["started"] = False
            current["health"] = health
            current["engine_running"] = True
            current["attached_existing_engine"] = True
            current["started_by_this_client"] = False
            self.attached_existing_engine = True
            return current
        root = Path(project_dir) if project_dir else self.engine_root
        if self.engine_mode == "clean_embedded":
            command = ["cmd", "/c", str(root / "start_embedded_engine.bat")]
            self._engine_process = subprocess.Popen(command, cwd=str(root), creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0))
        else:
            self._start_existing_pool(root)
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            status = self.ready()
            if status.get("ready"):
                status["started"] = True
                status["engine_mode"] = self.engine_mode
                status["attached_existing_engine"] = False
                status["started_by_this_client"] = True
                self.attached_existing_engine = False
                self.started_by_this_client = True
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

    def health(self) -> dict:
        try:
            response = httpx.get(f"{self.base_url}/health", timeout=5)
            if response.status_code >= 400:
                return {"status": "unavailable", "status_code": response.status_code}
            return response.json()
        except httpx.RequestError as exc:
            return {"status": "unavailable", "error": str(exc)}

    def get_system_status(self) -> dict:
        return self.ready()

    def shutdown(self, timeout_seconds: float = 10.0) -> dict:
        if not self.started_by_this_client:
            return {"stopped": False, "reason": "not_started_by_this_client"}
        status = self.ready()
        active_counts = status.get("active_task_counts") or {}
        active_counts = {key: value for key, value in active_counts.items() if value}
        if active_counts:
            return {"stopped": False, "reason": "active_tasks_present", "active_task_counts": active_counts}
        if status.get("active_count") or status.get("queued_count"):
            return {
                "stopped": False,
                "reason": "active_tasks_present",
                "active_count": status.get("active_count"),
                "queued_count": status.get("queued_count"),
            }
        if self._engine_process is None:
            return {"stopped": False, "reason": "missing_process_handle"}
        self._engine_process.terminate()
        try:
            self._engine_process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            return {"stopped": False, "reason": "shutdown_timeout"}
        self.started_by_this_client = False
        self.attached_existing_engine = False
        return {"stopped": True}

    def _start_existing_pool(self, root: Path) -> None:
        root = root.resolve()
        python = _python_for_existing_pool(root)
        log_dir = root / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env.update({
            "GATEWAY_API_HOST": _host_from_base_url(self.base_url),
            "GATEWAY_API_PORT": _port_from_base_url(self.base_url),
            "POOL_DRY_RUN": env.get("POOL_DRY_RUN", "false"),
            "POOL_MAX_CONCURRENCY": env.get("POOL_MAX_CONCURRENCY", "10"),
            "FLOWKIT_GATEWAY_WORKER_SOURCE": "static_json",
            "GATEWAY_WORKERS_PATH": str(root / "gateway" / "workers.json"),
            "GATEWAY_DB_PATH": str(root / "data" / "gateway.db"),
            "FLOW_GATEWAY_OUTPUT_DIR": str(root / "outputs"),
        })
        self._engine_process = subprocess.Popen(
            [str(python), "-m", "gateway.main"],
            cwd=str(root),
            env=env,
            stdout=(log_dir / "gateway.log").open("ab"),
            stderr=(log_dir / "gateway-startup-error.log").open("ab"),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )

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


def _python_for_existing_pool(root: Path) -> Path:
    candidates = [
        root.parent / ".venv" / "Scripts" / "python.exe",
        root / ".venv" / "Scripts" / "python.exe",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return Path("python")


def _host_from_base_url(base_url: str) -> str:
    from urllib.parse import urlparse

    return urlparse(base_url).hostname or "127.0.0.1"


def _port_from_base_url(base_url: str) -> str:
    from urllib.parse import urlparse

    parsed = urlparse(base_url)
    return str(parsed.port or (443 if parsed.scheme == "https" else 80))


def _client_key_from_env_file(root: Path) -> str:
    env_path = root / ".env"
    if not env_path.exists():
        return ""
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == "FLOW_GATEWAY_CLIENT_API_KEY":
            return value.strip().strip('"').strip("'")
    return ""
