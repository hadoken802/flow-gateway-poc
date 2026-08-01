"""Task Center V3 helpers built on the existing Gateway Scheduler."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import uuid
from pathlib import Path
from typing import Any

from . import crud


DEFAULT_OUTPUT_ROOT = Path("outputs")
_BAD_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def stable_idempotency_key(item: dict[str, Any]) -> str:
    material = {
        "external_task_id": item.get("external_task_id"),
        "prompt": item.get("prompt"),
        "input_media_path": item.get("input_media_path") or item.get("image_path"),
        "input_media_id": item.get("input_media_id"),
        "duration": item.get("duration") or 10,
        "aspect_ratio": item.get("aspect_ratio") or "9:16",
        "generation_parameters": item.get("generation_parameters") or {},
        "output_directory": item.get("output_directory"),
        "output_filename": item.get("output_filename"),
    }
    digest = hashlib.sha256(json.dumps(material, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    return f"gateway-v3:{digest}"


def safe_filename(value: str | None, fallback: str) -> str:
    name = (value or fallback).strip()
    name = _BAD_FILENAME_CHARS.sub("_", name)
    name = name.strip(" .")
    if not name:
        name = fallback
    if not name.lower().endswith(".mp4"):
        name += ".mp4"
    return name


def safe_output_plan(item: dict[str, Any], task_id_hint: str | None = None) -> dict[str, str]:
    batch_id = item.get("batch_id") or "unbatched"
    external = item.get("external_task_id") or task_id_hint or "task"
    root = Path(item.get("output_directory") or DEFAULT_OUTPUT_ROOT)
    filename = safe_filename(item.get("output_filename"), "video.mp4")
    if ".." in Path(filename).parts:
        raise ValueError("output_filename must not contain path traversal")
    directory = root / safe_filename(str(batch_id), "batch").removesuffix(".mp4") / safe_filename(str(external), "task").removesuffix(".mp4")
    return {
        "output_directory": str(directory),
        "output_filename": filename,
        "output_path": str(directory / filename),
        "temp_path": str(directory / f".{filename}.part"),
        "task_json": str(directory / "task.json"),
        "result_json": str(directory / "result.json"),
    }


def parse_import_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if "tasks" in payload and isinstance(payload["tasks"], list):
        return [dict(item) for item in payload["tasks"]]
    fmt = (payload.get("format") or "").lower()
    content = payload.get("content")
    if fmt == "json":
        loaded = json.loads(content or "[]")
        if isinstance(loaded, dict):
            loaded = loaded.get("tasks", [])
        if not isinstance(loaded, list):
            raise ValueError("JSON import content must be a list or {'tasks': [...]}")
        return [dict(item) for item in loaded]
    if fmt == "csv":
        reader = csv.DictReader(io.StringIO(content or ""))
        return [dict(row) for row in reader]
    raise ValueError("Import payload must provide tasks or format=csv/json with content")


def normalize_import_item(item: dict[str, Any], batch_id: str) -> dict[str, Any]:
    prompt = (item.get("prompt") or "").strip()
    image_path = item.get("input_media_path") or item.get("image_path")
    input_media_id = item.get("input_media_id")
    if not prompt:
        raise ValueError("prompt is required")
    if not image_path and not input_media_id:
        raise ValueError("input_media_path or input_media_id is required")
    if image_path and not Path(image_path).exists():
        raise ValueError(f"input_media_path does not exist: {image_path}")
    normalized = {
        "external_task_id": item.get("external_task_id") or item.get("task_id"),
        "batch_id": batch_id,
        "prompt": prompt,
        "image_path": str(image_path or input_media_id),
        "input_media_id": input_media_id,
        "duration": int(item.get("duration") or item.get("seconds") or 10),
        "aspect_ratio": item.get("aspect_ratio") or "9:16",
        "priority": int(item.get("priority") or 0),
        "not_before": item.get("not_before") or None,
        "estimated_quota_cost": int(item.get("estimated_quota_cost") or 15),
        "preferred_account_id": item.get("preferred_account_id") or None,
        "output_directory": item.get("output_directory") or None,
        "output_filename": item.get("output_filename") or None,
        "metadata_json": json.dumps(item.get("metadata") or {}, ensure_ascii=False, sort_keys=True),
        "generation_parameters_json": json.dumps(item.get("generation_parameters") or {}, ensure_ascii=False, sort_keys=True),
    }
    normalized["idempotency_key"] = item.get("idempotency_key") or stable_idempotency_key(normalized)
    plan = safe_output_plan(normalized, normalized.get("external_task_id"))
    normalized["output_directory"] = plan["output_directory"]
    normalized["output_filename"] = plan["output_filename"]
    return normalized


async def import_tasks(scheduler, payload: dict[str, Any]) -> dict[str, Any]:
    batch_id = payload.get("batch_id") or f"batch-{uuid.uuid4()}"
    raw_items = parse_import_payload(payload)
    rows = []
    created = duplicate = failed = 0
    for index, raw in enumerate(raw_items):
        try:
            item = normalize_import_item(raw, batch_id)
            task = await scheduler.create_task(item)
            is_duplicate = bool(task.get("reused"))
            created += 0 if is_duplicate else 1
            duplicate += 1 if is_duplicate else 0
            rows.append({
                "index": index,
                "ok": True,
                "duplicate": is_duplicate,
                "task_id": task["task_id"],
                "idempotency_key": task["idempotency_key"],
                "status": task["status"],
                "external_task_id": item.get("external_task_id"),
            })
        except Exception as exc:
            failed += 1
            rows.append({"index": index, "ok": False, "error": str(exc), "external_task_id": raw.get("external_task_id")})
    return {
        "batch_id": batch_id,
        "total": len(raw_items),
        "created": created,
        "duplicates": duplicate,
        "failed": failed,
        "items": rows,
    }


async def list_tasks(scheduler, filters: dict[str, Any]) -> list[dict[str, Any]]:
    return await crud.list_tasks_filtered(
        scheduler.db,
        batch_id=filters.get("batch_id"),
        status=filters.get("status"),
        account_id=filters.get("account_id"),
        error_category=filters.get("error_category"),
    )


async def batch_detail(scheduler, batch_id: str) -> dict[str, Any]:
    return {"batch_id": batch_id, "tasks": await crud.list_tasks_filtered(scheduler.db, batch_id=batch_id)}


async def task_detail(scheduler, task_id: str) -> dict[str, Any] | None:
    task = await crud.get_task(scheduler.db, task_id)
    if not task:
        return None
    events = await _query(scheduler.db, "SELECT * FROM task_state_events WHERE task_id=? ORDER BY created_at,event_id", (task_id,))
    leases = await _query(scheduler.db, "SELECT * FROM account_leases WHERE task_id=? ORDER BY acquired_at", (task_id,))
    attempts = await _query(scheduler.db, "SELECT * FROM task_attempts WHERE task_id=? ORDER BY started_at", (task_id,))
    ledger = await _query(scheduler.db, "SELECT * FROM quota_ledger WHERE task_id=? ORDER BY created_at", (task_id,))
    input_media = await crud.list_task_input_media(scheduler.db, task_id)
    public_input_media = [
        {
            "file_id": item["file_id"],
            "uploaded_media_id": item.get("uploaded_media_id"),
            "position": item["position"],
            "original_filename": item["original_filename"],
            "mime_type": item["mime_type"],
            "size_bytes": item["size_bytes"],
            "sha256": item["sha256"],
        }
        for item in input_media
    ]
    return {"task": task, "input_media": public_input_media, "state_events": events, "leases": leases, "attempts": attempts, "quota_ledger": ledger}


async def _query(db, sql: str, params=()):
    cursor = await db.execute(sql, params)
    return [dict(row) for row in await cursor.fetchall()]


async def pause_task(scheduler, task_id: str):
    return await crud.set_task_manual_pause(scheduler.db, task_id, True)


async def resume_task(scheduler, task_id: str):
    task = await crud.set_task_manual_pause(scheduler.db, task_id, False)
    await scheduler.schedule_once()
    return task


async def cancel_task(scheduler, task_id: str):
    return await crud.cancel_queued_task(scheduler.db, task_id)


async def set_priority(scheduler, task_id: str, priority: int):
    task = await crud.update_task_priority(scheduler.db, task_id, priority)
    await scheduler.schedule_once()
    return task


async def requeue_task(scheduler, task_id: str):
    task = await crud.requeue_task(scheduler.db, task_id)
    await scheduler.schedule_once()
    return task


def examples() -> dict[str, Any]:
    return {
        "csv": "external_task_id,prompt,input_media_path,output_directory,output_filename,priority,estimated_quota_cost\nshot-001,A gentle camera move,D:/media/shot-001.png,outputs,video.mp4,10,15\n",
        "json": {
            "tasks": [
                {
                    "external_task_id": "shot-001",
                    "prompt": "A gentle camera move",
                    "input_media_path": "D:/media/shot-001.png",
                    "output_directory": "outputs",
                    "output_filename": "video.mp4",
                    "priority": 10,
                    "estimated_quota_cost": 15,
                    "generation_parameters": {"duration": 10, "aspect_ratio": "9:16"},
                    "metadata": {"source": "example"},
                }
            ]
        },
    }
