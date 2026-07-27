"""POC endpoints for single-account Omni Flash 10s reference video generation."""
import asyncio
import base64
import binascii
import json
import logging
import mimetypes
import uuid
from pathlib import Path

import aiohttp
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from agent.config import OUTPUT_DIR
from agent.db import crud
from agent.services.flow_client import get_flow_client
from agent.services.omni_client import (
    OMNI_CREDIT_COST,
    OmniClient,
    extract_status_fields,
    extract_submit_fields,
    extract_video_url,
    resolve_encoded_video,
    sanitized_response_shape,
    response_shape,
    unwrap_response,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/test/omni-video", tags=["omni-test"])

POLL_INTERVAL_SECONDS = 10
POLL_TIMEOUT_SECONDS = 15 * 60
ACTIVE_STATUSES = ["queued", "scheduled", "active", "processing"]
DOWNLOAD_RECOVERY_STATUSES = ["completed", "waiting_download", "completed_remote"]

_poll_tasks: dict[str, asyncio.Task] = {}
_download_locks: dict[str, asyncio.Lock] = {}
_shutdown_event = asyncio.Event()


def _track_omni_task(job_id: str, coro) -> asyncio.Task | None:
    if _shutdown_event.is_set():
        return None
    task = asyncio.create_task(coro)
    _poll_tasks[job_id] = task
    task.add_done_callback(lambda done: _poll_tasks.pop(job_id, None) if _poll_tasks.get(job_id) is done else None)
    return task


async def shutdown_omni_jobs() -> None:
    _shutdown_event.set()
    for task in list(_poll_tasks.values()):
        task.cancel()
    if _poll_tasks:
        await asyncio.gather(*list(_poll_tasks.values()), return_exceptions=True)
    _poll_tasks.clear()


class OmniVideoRequest(BaseModel):
    idempotency_key: str
    project_id: str
    image_path: str
    prompt: str
    duration: int
    aspect_ratio: str


@router.post("")
async def submit_omni_video(body: OmniVideoRequest):
    if body.duration != 10:
        raise HTTPException(400, "Only duration=10 is supported in this POC")
    if body.aspect_ratio != "9:16":
        raise HTTPException(400, "Only aspect_ratio=9:16 is supported in this POC")

    existing = await crud.get_omni_test_job_by_idempotency_key(body.idempotency_key)
    if existing:
        response = _public_job(existing)
        response["reused"] = True
        return response

    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")
    if client._flow_key is None:
        raise HTTPException(503, "Flow key not present")

    image_path = Path(body.image_path)
    if not image_path.exists() or not image_path.is_file():
        raise HTTPException(404, f"File not found: {body.image_path}")

    credits_before = await client.get_credits()
    if credits_before.get("error"):
        raise HTTPException(502, credits_before["error"])
    credits_value = _extract_credits(unwrap_response(credits_before))
    if credits_value is None:
        raise HTTPException(502, "Unable to read current credits")
    if credits_value < OMNI_CREDIT_COST:
        raise HTTPException(400, f"Insufficient credits: {credits_value}, need {OMNI_CREDIT_COST}")

    upload_result = await _upload_image(client, image_path, body.project_id)
    input_media_id = upload_result.get("_mediaId")
    if not input_media_id:
        raise HTTPException(502, "Upload succeeded but mediaId was not returned")

    job_id = str(uuid.uuid4())
    await crud.create_omni_test_job(job_id, body.project_id, body.prompt, str(image_path), body.idempotency_key)
    await crud.update_omni_test_job(job_id, input_media_id=input_media_id, status="queued")

    omni = OmniClient(client)
    result = await omni.submit_reference_video(
        project_id=body.project_id,
        reference_media_ids=[input_media_id],
        prompt=body.prompt,
        user_paygate_tier="PAYGATE_TIER_NOT_PAID",
    )
    if result.get("error") or (isinstance(result.get("status"), int) and result["status"] >= 400):
        job = await crud.update_omni_test_job(
            job_id,
            status="failed",
            error_code=str(result.get("status") or "submit_error"),
            error_message=str(result.get("error") or result.get("data") or "Submit failed"),
            raw_response_shape=json.dumps(response_shape(unwrap_response(result))),
        )
        response = _public_job(job)
        response["reused"] = False
        return response

    data = unwrap_response(result)
    fields = extract_submit_fields(data)
    status = fields.pop("upstream_status", None)
    await crud.update_omni_test_job(
        job_id,
        **fields,
        status=_initial_status(status),
        submitted_at=crud._now(),
        raw_response_shape=json.dumps(response_shape(data)),
    )
    _ensure_polling(job_id)
    job = await crud.get_omni_test_job(job_id)
    logger.info(
        "Omni submitted job=%s project=%s input=%s output=%s status=%s remaining=%s",
        job_id, body.project_id[:8], input_media_id[:8],
        (job.get("output_media_id") or "")[:8], job.get("status"), job.get("remaining_credits"))
    response = _public_job(job)
    response["reused"] = False
    return response


@router.get("/{job_id}")
async def get_omni_video(job_id: str):
    job = await crud.get_omni_test_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return _public_job(job)


@router.post("/{job_id}/retry-download")
async def retry_omni_video_download(job_id: str):
    job = await crud.get_omni_test_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    lock = _download_locks.setdefault(job_id, asyncio.Lock())
    async with lock:
        job = await crud.get_omni_test_job(job_id)
        if job.get("video_path") and _valid_existing_mp4(Path(job["video_path"])):
            job = await crud.update_omni_test_job(
                job_id,
                status="completed",
                error_code=None,
                error_message=None,
            )
            response = _public_job(job)
            response["reused"] = True
            return response
        await _retry_download_existing_job(job)
        updated = await crud.get_omni_test_job(job_id)
        response = _public_job(updated)
        response["reused"] = False
        return response


@router.get("/manual-flow-results/{project_id}")
async def list_manual_flow_results(
    project_id: str,
    after: str | None = None,
    exclude_media_ids: str = Query(default=""),
):
    excluded = {item.strip() for item in exclude_media_ids.split(",") if item.strip()}
    requests = await crud.list_requests(project_id=project_id, status="COMPLETED")
    candidates = []
    for item in requests:
        media_id = item.get("media_id")
        if not media_id or media_id in excluded:
            continue
        if item.get("type") not in {"GENERATE_VIDEO", "REGENERATE_VIDEO", "GENERATE_VIDEO_REFS"}:
            continue
        timestamp = item.get("updated_at") or item.get("created_at")
        if after and timestamp and timestamp <= after:
            continue
        candidates.append({
            "media_id": media_id,
            "operation_id": item.get("request_id"),
            "project_id": item.get("project_id"),
            "request_id": item.get("id"),
            "type": item.get("type"),
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
        })
    return {"project_id": project_id, "candidate_count": len(candidates), "candidates": candidates}


@router.get("/manual-flow-results/media/{media_id}/download")
async def download_manual_flow_result(media_id: str):
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")
    if client._flow_key is None:
        raise HTTPException(503, "Flow key not present")
    result = await client.get_media(media_id)
    if result.get("error"):
        raise HTTPException(502, result["error"])
    data = unwrap_response(result)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    dest = OUTPUT_DIR / f"manual-{media_id}.mp4"
    encoded = resolve_encoded_video(data)
    if encoded:
        video_bytes = _decode_encoded_video_candidate(encoded, media_id)
        _write_valid_mp4_bytes_atomic(video_bytes, dest)
        return {"status": "completed", "media_id": media_id, "video_path": str(dest), "source": "manual_project_media"}
    url = extract_video_url(data)
    if not url:
        return {"status": "waiting_download", "media_id": media_id, "error_code": "missing_video_url", "error_message": "Manual media did not include downloadable video"}
    await _download_mp4(url, dest)
    return {"status": "completed", "media_id": media_id, "video_path": str(dest), "source": "manual_project_media"}


async def recover_omni_jobs() -> None:
    if _shutdown_event.is_set():
        return
    active = await crud.list_omni_test_jobs(ACTIVE_STATUSES)
    for job in active:
        if job.get("output_media_id") or job.get("workflow_id") or job.get("operation_name"):
            _ensure_polling(job["job_id"])
    completed = await crud.list_omni_test_jobs(["completed"])
    for job in completed:
        if not job.get("video_path"):
            _ensure_polling(job["job_id"])
    for job in await crud.list_omni_test_jobs(["waiting_download", "completed_remote"]):
        _ensure_polling(job["job_id"])
    if active or completed:
        logger.info("Recovered %d Omni polling/download jobs", len(active) + len(completed))


def _ensure_polling(job_id: str) -> None:
    if _shutdown_event.is_set():
        return
    task = _poll_tasks.get(job_id)
    if task and not task.done():
        return
    _track_omni_task(job_id, _poll_job(job_id))


async def _poll_job(job_id: str) -> None:
    try:
        start = asyncio.get_running_loop().time()
        client = get_flow_client()
        omni = OmniClient(client)
        while not _shutdown_event.is_set():
            job = await crud.get_omni_test_job(job_id)
            if not job:
                return
            if job.get("status") == "failed":
                return
            if job.get("status") == "completed":
                if job.get("video_path"):
                    return
                await _retry_download_existing_job(job)
                return
            if not job.get("output_media_id"):
                await crud.update_omni_test_job(job_id, status="failed", error_code="missing_output_media_id",
                                                error_message="Cannot poll without output_media_id")
                return
            if asyncio.get_running_loop().time() - start > POLL_TIMEOUT_SECONDS:
                await crud.update_omni_test_job(job_id, status="failed", error_code="poll_timeout",
                                                error_message="Omni generation timed out")
                return

            try:
                result = await omni.check_status(job["project_id"], job["output_media_id"])
                if result.get("error"):
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue
                data = unwrap_response(result)
                fields = extract_status_fields(data)
                status = fields["status"]
                updates = {
                    "status": status,
                    "raw_response_shape": json.dumps(response_shape(data)),
                }
                if fields.get("output_media_id"):
                    updates["output_media_id"] = fields["output_media_id"]
                if fields.get("operation_name"):
                    updates["operation_name"] = fields["operation_name"]
                if status == "completed":
                    updates["completed_at"] = crud._now()
                await crud.update_omni_test_job(job_id, **updates)
                logger.info("Omni poll job=%s status=%s output=%s", job_id, status, (job["output_media_id"] or "")[:8])
                if status == "completed":
                    await _retry_download_existing_job(await crud.get_omni_test_job(job_id))
                    return
                if status == "failed":
                    credits = await client.get_credits()
                    remaining = _extract_credits(unwrap_response(credits))
                    await crud.update_omni_test_job(job_id, remaining_credits=remaining,
                                                    error_code="upstream_failed",
                                                    error_message="Omni generation failed upstream")
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not _shutdown_event.is_set():
                    await crud.update_omni_test_job(job_id, error_code=type(exc).__name__, error_message=str(exc)[:500])
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        raise
    return


async def _download_completed(job: dict) -> None:
    client = get_flow_client()
    output_media_id = job.get("output_media_id")
    if not output_media_id:
        return
    result = await client.get_media(output_media_id)
    if result.get("error"):
        logger.warning(
            "Omni get_media failed job=%s media=%s error=%s",
            job["job_id"],
            output_media_id[:8],
            str(result.get("error"))[:200],
        )
        await crud.update_omni_test_job(
            job["job_id"],
            status="waiting_download",
            error_code="get_media_error",
            error_message=str(result.get("error"))[:500],
            raw_response_shape=json.dumps(sanitized_response_shape(result)),
        )
        return
    data = unwrap_response(result)
    await crud.update_omni_test_job(job["job_id"], raw_response_shape=json.dumps(sanitized_response_shape(data)))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    dest = OUTPUT_DIR / f"{job['job_id']}.mp4"
    encoded = resolve_encoded_video(data)
    if encoded:
        logger.info(
            "Omni encodedVideo found job=%s media=%s path=%s chars=%d",
            job["job_id"],
            output_media_id[:8],
            encoded.path,
            len(encoded.encoded_video),
        )
        video_bytes = _decode_encoded_video_candidate(encoded, output_media_id)
        _write_valid_mp4_bytes_atomic(video_bytes, dest)
        await crud.update_omni_test_job(
            job["job_id"],
            video_path=str(dest),
            status="completed",
            error_code=None,
            error_message=None,
            completed_at=crud._now(),
        )
        logger.info("Omni downloaded job=%s output=%s path=%s", job["job_id"], output_media_id[:8], dest)
        return
    logger.info("Omni encodedVideo missing job=%s media=%s", job["job_id"], output_media_id[:8])
    url = extract_video_url(data)
    if not url:
        await crud.update_omni_test_job(job["job_id"], status="waiting_download", error_code="missing_video_url",
                                        error_message="get_media response did not include fifeUrl/servingUri/video URL")
        return
    await _download_mp4(url, dest)
    await crud.update_omni_test_job(job["job_id"], video_path=str(dest), status="completed", completed_at=crud._now())
    logger.info("Omni downloaded job=%s output=%s path=%s", job["job_id"], output_media_id[:8], dest)


async def _retry_download_existing_job(job: dict, wait_schedule=None) -> None:
    wait_schedule = wait_schedule if wait_schedule is not None else [0, 5, 10, 20, 30] + [30] * 17
    client = get_flow_client()
    omni = OmniClient(client)
    started = asyncio.get_running_loop().time()
    for delay in wait_schedule:
        if _shutdown_event.is_set():
            return
        if delay:
            await asyncio.sleep(delay)
        current = await crud.get_omni_test_job(job["job_id"])
        if current.get("video_path") and _valid_existing_mp4(Path(current["video_path"])):
            return
        existing_dest = OUTPUT_DIR / f"{current['job_id']}.mp4"
        if _valid_existing_mp4(existing_dest):
            await crud.update_omni_test_job(
                current["job_id"],
                status="completed",
                video_path=str(existing_dest),
                error_code=None,
                error_message=None,
                completed_at=crud._now(),
            )
            return
        await crud.update_omni_test_job(current["job_id"], status="waiting_download")
        if current.get("output_media_id"):
            status_result = await omni.check_status(current["project_id"], current["output_media_id"])
            status_data = unwrap_response(status_result)
            fields = extract_status_fields(status_data)
            updates = {"raw_response_shape": json.dumps(sanitized_response_shape(status_data))}
            if fields.get("output_media_id") and fields["output_media_id"] != current.get("output_media_id"):
                updates["output_media_id"] = fields["output_media_id"]
            if fields.get("operation_name"):
                updates["operation_name"] = fields["operation_name"]
            await crud.update_omni_test_job(current["job_id"], **updates)
        await _download_completed(await crud.get_omni_test_job(job["job_id"]))
        latest = await crud.get_omni_test_job(job["job_id"])
        if latest.get("video_path") and _valid_existing_mp4(Path(latest["video_path"])):
            return
        if asyncio.get_running_loop().time() - started >= 600:
            break
    latest = await crud.get_omni_test_job(job["job_id"])
    if latest.get("error_code") == "get_media_error":
        await crud.update_omni_test_job(job["job_id"], status="failed")
    else:
        await crud.update_omni_test_job(job["job_id"], status="failed", error_code="missing_video_url",
                                        error_message="No downloadable video URL after retry window")


async def _upload_image(client, image_path: Path, project_id: str) -> dict:
    image_bytes = image_path.read_bytes()
    b64 = base64.b64encode(image_bytes).decode()
    mime = mimetypes.guess_type(str(image_path))[0] or "image/png"
    result = await client.upload_image(b64, mime_type=mime, project_id=project_id, file_name=image_path.name)
    if result.get("error") or (isinstance(result.get("status"), int) and result["status"] >= 400):
        raise HTTPException(result.get("status", 502), result.get("error", result.get("data")))
    return result


async def _download_mp4(url: str, dest: Path) -> None:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status < 200 or resp.status >= 300:
                raise ValueError(f"Video download failed: HTTP {resp.status}")
            content_type = (resp.headers.get("Content-Type") or "").lower()
            data = await resp.read()
    if not data:
        raise ValueError("Video download failed: empty file")
    if data[:64].lstrip().lower().startswith(b"<!doctype html") or data[:64].lstrip().lower().startswith(b"<html"):
        raise ValueError("Video download failed: response is HTML")
    if content_type and not ("video" in content_type or "octet-stream" in content_type or "mp4" in content_type):
        raise ValueError(f"Unexpected Content-Type: {content_type}")
    _write_valid_mp4_bytes_atomic(data, dest)


def _decode_encoded_video_candidate(candidate, media_id: str) -> bytes:
    encoded = candidate.encoded_video.strip()
    if encoded.startswith("data:"):
        header, sep, payload = encoded.partition(",")
        if not sep or "base64" not in header.lower() or "video" not in header.lower():
            raise ValueError("Video download failed: invalid data URL")
        encoded = payload.strip()
    if not encoded:
        raise ValueError("Video download failed: empty encodedVideo")
    try:
        data = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Video download failed: invalid Base64 encodedVideo") from exc
    logger.info(
        "Omni encodedVideo resolved path=%s chars=%d bytes=%d media=%s",
        candidate.path,
        len(candidate.encoded_video),
        len(data),
        (media_id or "")[:8],
    )
    _validate_mp4_bytes(data)
    logger.info("Omni encodedVideo MP4 valid path=%s bytes=%d media=%s", candidate.path, len(data), (media_id or "")[:8])
    return data


def _write_valid_mp4_bytes_atomic(data: bytes, dest: Path) -> None:
    part = dest.with_suffix(dest.suffix + ".part")
    try:
        _write_valid_mp4_bytes(data, part)
        part.replace(dest)
    except Exception:
        try:
            if part.exists():
                part.unlink()
        finally:
            pass
        raise


def _write_valid_mp4_bytes(data: bytes, dest: Path) -> None:
    _validate_mp4_bytes(data)
    dest.write_bytes(data)


def _validate_mp4_bytes(data: bytes) -> None:
    if not data:
        raise ValueError("Video download failed: empty file")
    head = data[:128].lstrip().lower()
    if head.startswith(b"<!doctype html") or head.startswith(b"<html"):
        raise ValueError("Video download failed: response is HTML")
    if head.startswith(b"{") or head.startswith(b"["):
        raise ValueError("Video download failed: response is JSON")
    if len(data) < 8 or data[4:8] != b"ftyp":
        raise ValueError("Video download failed: missing MP4 ftyp header")


def _valid_existing_mp4(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size <= 0:
            return False
        with path.open("rb") as fh:
            header = fh.read(12)
        return len(header) >= 8 and header[4:8] == b"ftyp"
    except OSError:
        return False


def _extract_credits(data: dict) -> int | None:
    if not isinstance(data, dict):
        return None
    for key in ("credits", "remainingCredits", "remaining_credits"):
        value = data.get(key)
        if isinstance(value, int):
            return value
    return None


def _initial_status(upstream_status: str | None) -> str:
    if not upstream_status:
        return "scheduled"
    from agent.services.omni_client import normalize_generation_status
    return normalize_generation_status(upstream_status)


def _public_job(job: dict) -> dict:
    keys = [
        "job_id", "idempotency_key", "project_id", "input_media_id", "output_media_id", "workflow_id",
        "operation_name", "upstream_batch_id", "status", "remaining_credits",
        "image_path", "video_path", "error_code", "error_message", "submitted_at",
        "updated_at", "completed_at",
    ]
    return {key: job.get(key) for key in keys}
