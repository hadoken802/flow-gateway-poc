"""One-shot remote status query helpers for reconciled Flow jobs."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse, parse_qsl

from agent.services.flow_client import get_flow_client
from agent.services.omni_client import OmniClient, extract_status_fields, response_shape, resolve_encoded_video, resolve_video_candidates, unwrap_response


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def query_bound_remote_status_once(*, project_id: str, output_media_id: str, omni: OmniClient | None = None) -> dict[str, Any]:
    started = utc_now()
    call_count = 0
    try:
        client = omni or OmniClient(get_flow_client())
        call_count += 1
        result = await client.check_status(project_id, output_media_id)
        finished = utc_now()
        if result.get("error"):
            return _query_error(project_id, output_media_id, started, finished, call_count, result.get("error"), result)
        data = unwrap_response(result)
        return normalize_remote_status_response(project_id, output_media_id, data, started, finished, call_count)
    except Exception as exc:
        return _query_error(project_id, output_media_id, started, utc_now(), call_count, str(exc), {"exception_type": type(exc).__name__})


def normalize_remote_status_response(project_id: str, output_media_id: str, data: dict[str, Any], started: str | None = None, finished: str | None = None, call_count: int = 1) -> dict[str, Any]:
    fields = extract_status_fields(data if isinstance(data, dict) else {})
    raw_status = fields.get("raw_status")
    normalized = fields.get("status") or "processing"
    evidence = _status_evidence(data)
    encoded = resolve_encoded_video(data) if isinstance(data, dict) else None
    urls = resolve_video_candidates(data) if isinstance(data, dict) else []
    error_info = _find_error(data)
    not_found = _looks_not_found(data, raw_status, error_info)
    failed = normalized == "failed" or any(bool(value) for value in error_info.values())
    completed = bool(encoded) or normalized == "completed" or any(item.get("completed") for item in evidence)
    processing = not completed and not failed and not not_found and raw_status is not None and (normalized in {"processing", "active"} or any(item.get("processing") for item in evidence))
    if not_found:
        remote_state = "remote_not_found"
    elif completed:
        remote_state = "remote_completed"
    elif failed:
        remote_state = "remote_failed"
    elif processing:
        remote_state = "remote_processing"
    else:
        remote_state = "remote_unknown"
    return {
        "query_ok": True,
        "query_started_at": started,
        "query_finished_at": finished,
        "project_id": project_id,
        "output_media_id": output_media_id,
        "remote_state": remote_state,
        "raw_status": raw_status,
        "status_evidence": evidence,
        "completed": completed,
        "processing": processing,
        "failed": failed,
        "not_found": not_found,
        "download_ready": bool(completed and (encoded or urls)),
        "encoded_video_present": encoded is not None,
        "download_url_present": bool(urls),
        "download_urls": [_safe_url(item.url, item.path) for item in urls[:3]],
        "error_code": error_info.get("code"),
        "error_message": error_info.get("message"),
        "response_shape": response_shape(data),
        "remote_query_call_count": call_count,
        "submit_called": False,
        "download_called": False,
        "database_writes_performed": False,
    }


def _query_error(project_id: str, output_media_id: str, started: str, finished: str, call_count: int, message: Any, payload: Any) -> dict[str, Any]:
    text = str(message or "")
    lower = text.lower()
    code = "remote_query_error"
    if "recaptcha" in lower or "captcha" in lower:
        code = "recaptcha_required"
    elif "permission" in lower or "forbidden" in lower or "403" in lower:
        code = "permission_denied"
    elif "not found" in lower or "404" in lower:
        code = "not_found"
    return {
        "query_ok": False,
        "query_started_at": started,
        "query_finished_at": finished,
        "project_id": project_id,
        "output_media_id": output_media_id,
        "remote_state": "remote_not_found" if code == "not_found" else "remote_query_error",
        "raw_status": None,
        "status_evidence": [],
        "completed": False,
        "processing": False,
        "failed": False,
        "not_found": code == "not_found",
        "download_ready": False,
        "encoded_video_present": False,
        "download_url_present": False,
        "error_code": code,
        "error_message": text[:500],
        "response_shape": response_shape(payload),
        "remote_query_call_count": call_count,
        "submit_called": False,
        "download_called": False,
        "database_writes_performed": False,
    }


def _status_evidence(value: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def walk(obj: Any, path: list[str]) -> None:
        if isinstance(obj, dict):
            for key, item in obj.items():
                next_path = path + [key]
                if "status" in key.lower() or "state" in key.lower():
                    text = str(item)
                    lower = text.lower()
                    found.append({
                        "path": ".".join(next_path),
                        "value": text[:120],
                        "completed": any(token in lower for token in ("complete", "success", "succeed")),
                        "processing": any(token in lower for token in ("process", "running", "pending", "progress")),
                        "failed": any(token in lower for token in ("fail", "error", "reject", "cancel")),
                    })
                walk(item, next_path)
        elif isinstance(obj, list):
            for index, item in enumerate(obj):
                walk(item, path + [str(index)])

    walk(value, [])
    return found[:20]


def _find_error(value: Any) -> dict[str, str | None]:
    result = {"code": None, "message": None}

    def walk(obj: Any, path: list[str]) -> None:
        if result["code"] or result["message"]:
            return
        if isinstance(obj, dict):
            for key, item in obj.items():
                lower = key.lower()
                if lower in {"error", "errors"}:
                    if isinstance(item, dict):
                        result["code"] = str(item.get("code") or "remote_error")[:120]
                        result["message"] = str(item.get("message") or response_shape(item))[:500]
                    else:
                        result["code"] = "remote_error"
                        result["message"] = str(item)[:500]
                    return
                if lower in {"errorcode"} and item:
                    result["code"] = str(item)[:120]
                if lower in {"errormessage"} and item:
                    result["message"] = str(item)[:500]
                walk(item, path + [key])
        elif isinstance(obj, list):
            for index, item in enumerate(obj):
                walk(item, path + [str(index)])

    walk(value, [])
    return result


def _looks_not_found(data: Any, raw_status: Any, error_info: dict[str, Any]) -> bool:
    text = " ".join(str(item or "") for item in (raw_status, error_info.get("code"), error_info.get("message"))).lower()
    return "not_found" in text or "not found" in text or "404" in text


def _safe_url(url: str, source_path: str) -> dict[str, Any]:
    parsed = urlparse(url)
    return {
        "source_path": source_path,
        "scheme": parsed.scheme,
        "host": parsed.netloc,
        "path": parsed.path,
        "query_parameter_names": sorted({key for key, _value in parse_qsl(parsed.query, keep_blank_values=True)}),
    }
