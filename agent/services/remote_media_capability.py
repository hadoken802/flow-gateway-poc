"""Sanitize one get_media response into a small download capability summary."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlparse

from agent.services.omni_client import resolve_encoded_video, extract_video_url, unwrap_response


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def query_download_capability_once(*, client: Any, media_id: str) -> dict[str, Any]:
    started = now_iso()
    get_media_call_count = 0
    try:
        get_media_call_count += 1
        result = await client.get_media(media_id)
        finished = now_iso()
        if result.get("error"):
            return _error("media_query_error", str(result.get("error")), media_id, started, finished, get_media_call_count)
        status = result.get("status")
        if isinstance(status, int) and status >= 400:
            if status == 404:
                code = "media_not_found"
            elif status in {401, 403}:
                code = "media_access_denied"
            else:
                code = "media_query_error"
            return _error(code, f"HTTP {status}", media_id, started, finished, get_media_call_count, http_status=status)
        data = unwrap_response(result)
        summary = summarize_media_capability(data, media_id=media_id)
        summary.update({
            "query_ok": True,
            "query_started_at": started,
            "query_finished_at": finished,
            "media_id": media_id,
            "media_found": True,
            "get_media_call_count": get_media_call_count,
            "submit_called": False,
            "poll_called": False,
            "download_called": False,
            "database_writes_performed": False,
        })
        return summary
    except Exception as exc:
        return _error("media_query_error", f"{type(exc).__name__}: {exc}", media_id, started, now_iso(), get_media_call_count)


def summarize_media_capability(data: Any, *, media_id: str) -> dict[str, Any]:
    encoded = resolve_encoded_video(data)
    url = extract_video_url(data)
    encoded_len = len(encoded.encoded_video) if encoded else 0
    url_summary = _safe_url(url) if url else None
    response_shape = _shape(data)
    if encoded and encoded_len > 0:
        capability = "encoded_video_available"
    elif url_summary and url_summary.get("scheme") == "https":
        capability = "signed_video_url_available"
    elif _looks_not_ready(data):
        capability = "media_not_ready"
    elif isinstance(data, dict):
        capability = "media_available_unknown_transport"
    else:
        capability = "media_query_error"
    return {
        "download_capability": capability,
        "response_type": type(data).__name__,
        "remote_status": _find_first(data, {"status", "state", "generationStatus"}),
        "mime_type": _find_first(data, {"mimeType", "mime_type", "contentType"}),
        "content_type": _find_first(data, {"contentType", "content_type"}),
        "encoded_video_present": bool(encoded),
        "encoded_video_length": encoded_len if encoded else None,
        "encoded_video_sha256": hashlib.sha256(encoded.encoded_video.encode("utf-8")).hexdigest() if encoded else None,
        "video_url_present": bool(url_summary),
        "video_url_scheme": (url_summary or {}).get("scheme"),
        "video_url_host": (url_summary or {}).get("host"),
        "video_url_path": (url_summary or {}).get("path"),
        "video_url_query_parameter_names": (url_summary or {}).get("query_parameter_names"),
        "video_url_expires_at": (url_summary or {}).get("expires_at"),
        "file_size": _find_first(data, {"sizeBytes", "fileSize", "file_size", "contentLength"}),
        "duration": _find_first(data, {"duration", "durationSeconds"}),
        "width": _find_first(data, {"width"}),
        "height": _find_first(data, {"height"}),
        "error_code": None,
        "error_class": None,
        "error_message_sanitized": None,
        "response_shape": response_shape,
    }


def _error(code: str, message: str, media_id: str, started: str, finished: str, count: int, http_status: int | None = None) -> dict[str, Any]:
    return {
        "query_ok": False,
        "query_started_at": started,
        "query_finished_at": finished,
        "media_id": media_id,
        "media_found": False if code == "media_not_found" else None,
        "download_capability": code,
        "error_code": code,
        "error_class": "http" if http_status else "exception",
        "error_message_sanitized": message[:300],
        "http_status": http_status,
        "get_media_call_count": count,
        "submit_called": False,
        "poll_called": False,
        "download_called": False,
        "database_writes_performed": False,
    }


def _safe_url(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    params = sorted({key for key, _ in parse_qsl(parsed.query, keep_blank_values=True)})
    expires = None
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key in ("Expires", "expires", "X-Goog-Expires"):
        if key in query:
            expires = "present"
    return {"scheme": parsed.scheme, "host": parsed.netloc, "path": parsed.path, "query_parameter_names": params, "expires_at": expires}


def _find_first(value: Any, keys: set[str]) -> Any:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in keys and (isinstance(item, (str, int, float, bool)) or item is None):
                return item
            found = _find_first(item, keys)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _find_first(item, keys)
            if found is not None:
                return found
    return None


def _looks_not_ready(data: Any) -> bool:
    text = str(_find_first(data, {"status", "state", "generationStatus"}) or "").lower()
    return any(word in text for word in ("pending", "processing", "running", "not_ready"))


def _shape(value: Any, depth: int = 0) -> Any:
    if depth > 4:
        return type(value).__name__
    if isinstance(value, dict):
        return {str(k): _shape(v, depth + 1) for k, v in sorted(value.items())[:50]}
    if isinstance(value, list):
        return [_shape(value[0], depth + 1)] if value else []
    if isinstance(value, str):
        return {"type": "str", "length": len(value), "sha256": hashlib.sha256(value.encode("utf-8")).hexdigest()}
    return type(value).__name__
