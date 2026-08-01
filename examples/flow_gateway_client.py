"""Small Python client for Flow Gateway reference-image jobs."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import httpx


class FlowGatewayClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8200"):
        self.base_url = base_url.rstrip("/")

    def upload_images(self, images: list[str]) -> list[dict]:
        files = []
        handles = []
        try:
            for image in images:
                path = Path(image)
                fh = path.open("rb")
                handles.append(fh)
                files.append(("files", (path.name, fh, _mime_for(path))))
            response = httpx.post(f"{self.base_url}/api/v1/client/files/batch", files=files, timeout=120)
            response.raise_for_status()
            items = response.json()["files"]
            failed = [item for item in items if item.get("ok") is False]
            if failed:
                raise RuntimeError(f"Upload failed: {failed}")
            return items
        finally:
            for fh in handles:
                fh.close()

    def generate_reference_images(
        self,
        images: list[str],
        prompt: str,
        output_path: str,
        duration: int = 10,
        aspect_ratio: str = "9:16",
        estimated_quota_cost: int = 15,
        priority: int = 10,
        idempotency_key: str | None = None,
    ) -> dict:
        uploaded = self.upload_images(images)
        output = Path(output_path)
        payload = {
            "idempotency_key": idempotency_key or _stable_key(uploaded, prompt, duration, aspect_ratio),
            "input_file_ids": [item["file_id"] for item in uploaded],
            "prompt": prompt,
            "duration": duration,
            "aspect_ratio": aspect_ratio,
            "estimated_quota_cost": estimated_quota_cost,
            "priority": priority,
            "output_directory": str(output.parent),
            "output_filename": output.name,
        }
        response = httpx.post(f"{self.base_url}/api/v1/tasks", json=payload, timeout=30)
        response.raise_for_status()
        return response.json()


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
