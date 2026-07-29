"""One-shot reconciled encodedVideo fetch helpers.

This module intentionally does not write files or databases. It calls
get_media at most once through the provided client, verifies the exact
encodedVideo fingerprint from the prior capability query, and returns MP4
bytes plus safe metadata.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlparse

import aiohttp

from agent.services.omni_client import resolve_encoded_video, resolve_video_candidates, response_shape, unwrap_response


MAX_ENCODED_VIDEO_CHARS = 32 * 1024 * 1024
MAX_DECODED_VIDEO_BYTES = 32 * 1024 * 1024
MIN_MP4_BYTES = 16


@dataclass(frozen=True)
class EncodedVideoFetchResult:
    ok: bool
    media_id: str
    video_bytes: bytes
    headers: dict[str, str]
    manifest: dict[str, Any]


def encoded_video_fingerprint(encoded: str) -> dict[str, Any]:
    return {
        "encoded_video_length": len(encoded),
        "encoded_video_sha256": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        "fingerprint_object": "complete encodedVideo string encoded as UTF-8",
    }


async def fetch_reconciled_encoded_video_once(
    *,
    client: Any,
    media_id: str,
    expected_encoded_video_length: int,
    expected_encoded_video_sha256: str,
) -> EncodedVideoFetchResult:
    started = _now()
    get_media_call_count = 0
    try:
        get_media_call_count += 1
        result = await client.get_media(media_id)
        if result.get("error"):
            return _failure(media_id, "get_media_error", str(result.get("error")), started, get_media_call_count)
        data = unwrap_response(result)
        candidate = resolve_encoded_video(data)
        if not candidate:
            return _failure(media_id, "encoded_video_missing", "get_media response did not include encodedVideo", started, get_media_call_count)
        encoded = candidate.encoded_video
        actual_fp = encoded_video_fingerprint(encoded)
        if actual_fp["encoded_video_length"] != expected_encoded_video_length or actual_fp["encoded_video_sha256"] != expected_encoded_video_sha256:
            manifest = _base_manifest(media_id, started, get_media_call_count)
            manifest.update({
                "ok": False,
                "error_code": "encoded_video_fingerprint_changed",
                "stage": "post_get_media_fingerprint_validation",
                "transport_detected": "encoded_video",
                "encoded_video_present": True,
                "video_url_present": bool(resolve_video_candidates(data)),
                "actual_encoded_video_length": actual_fp["encoded_video_length"],
                "actual_encoded_video_sha256": actual_fp["encoded_video_sha256"],
                "expected_encoded_video_length": expected_encoded_video_length,
                "expected_encoded_video_sha256": expected_encoded_video_sha256,
                "url_download_call_count": 0,
            })
            return EncodedVideoFetchResult(False, media_id, b"", _headers(manifest), manifest)
        video_bytes, mime_type, data_uri_present, base64_format = decode_encoded_video(encoded)
        validate_mp4_bytes(video_bytes)
        decoded_sha = hashlib.sha256(video_bytes).hexdigest()
        manifest = _base_manifest(media_id, started, get_media_call_count)
        manifest.update({
            "ok": True,
            "transport": "encoded_video",
            "transport_detected": "encoded_video",
            "encoded_video_present": True,
            "video_url_present": bool(resolve_video_candidates(data)),
            "encoded_video_length": expected_encoded_video_length,
            "encoded_video_sha256": expected_encoded_video_sha256,
            "encoded_fingerprint_matched": True,
            "data_uri_present": data_uri_present,
            "mime_type": mime_type,
            "base64_format": base64_format,
            "decoded_byte_length": len(video_bytes),
            "decoded_sha256": decoded_sha,
            "mp4_valid": True,
            "url_download_call_count": 0,
            "submit_called": False,
            "poll_called": False,
            "download_called": False,
            "database_writes_performed": False,
        })
        return EncodedVideoFetchResult(True, media_id, video_bytes, _headers(manifest), manifest)

    except Exception as exc:
        return _failure(media_id, type(exc).__name__, str(exc), started, get_media_call_count)


async def fetch_reconciled_media_video_once(
    *,
    client: Any,
    media_id: str,
    expected_encoded_video_length: int | None = None,
    expected_encoded_video_sha256: str | None = None,
    url_fetcher: Any | None = None,
) -> EncodedVideoFetchResult:
    started = _now()
    get_media_call_count = 0
    try:
        get_media_call_count += 1
        result = await client.get_media(media_id)
        if result.get("error"):
            return _failure(media_id, "get_media_error", str(result.get("error")), started, get_media_call_count)
        data = unwrap_response(result)
        encoded = resolve_encoded_video(data)
        urls = resolve_video_candidates(data)
        if encoded:
            fp = encoded_video_fingerprint(encoded.encoded_video)
            if expected_encoded_video_length is not None and expected_encoded_video_sha256 is not None:
                if fp["encoded_video_length"] != expected_encoded_video_length or fp["encoded_video_sha256"] != expected_encoded_video_sha256:
                    manifest = _base_manifest(media_id, started, get_media_call_count)
                    manifest.update({
                        "ok": False,
                        "error_code": "encoded_video_fingerprint_changed",
                        "stage": "post_get_media_fingerprint_validation",
                        "transport_detected": "encoded_video",
                        "encoded_video_present": True,
                        "video_url_present": bool(urls),
                        "actual_encoded_video_length": fp["encoded_video_length"],
                        "actual_encoded_video_sha256": fp["encoded_video_sha256"],
                        "expected_encoded_video_length": expected_encoded_video_length,
                        "expected_encoded_video_sha256": expected_encoded_video_sha256,
                        "url_download_call_count": 0,
                    })
                    return EncodedVideoFetchResult(False, media_id, b"", _headers(manifest), manifest)
            video_bytes, mime_type, data_uri_present, base64_format = decode_encoded_video(encoded.encoded_video)
            validate_mp4_bytes(video_bytes)
            manifest = _base_manifest(media_id, started, get_media_call_count)
            manifest.update({
                "ok": True,
                "transport": "encoded_video",
                "transport_detected": "encoded_video",
                "encoded_video_present": True,
                "encoded_video_path": encoded.path,
                "video_url_present": bool(urls),
                "encoded_video_length": fp["encoded_video_length"],
                "encoded_video_sha256": fp["encoded_video_sha256"],
                "encoded_fingerprint_matched": expected_encoded_video_sha256 is None or fp["encoded_video_sha256"] == expected_encoded_video_sha256,
                "data_uri_present": data_uri_present,
                "mime_type": mime_type,
                "base64_format": base64_format,
                "decoded_byte_length": len(video_bytes),
                "decoded_sha256": hashlib.sha256(video_bytes).hexdigest(),
                "mp4_valid": True,
                "url_download_call_count": 0,
                "response_shape": response_shape(data),
                "submit_called": False,
                "poll_called": False,
                "download_called": False,
                "database_writes_performed": False,
            })
            return EncodedVideoFetchResult(True, media_id, video_bytes, _headers(manifest), manifest)
        if urls:
            return await _fetch_url_transport(media_id, urls[0], started, get_media_call_count, data, url_fetcher=url_fetcher)
        manifest = _base_manifest(media_id, started, get_media_call_count)
        manifest.update({
            "ok": False,
            "error_code": "media_transport_missing",
            "stage": "post_get_media_transport_detection",
            "transport_detected": None,
            "encoded_video_present": False,
            "video_url_present": False,
            "response_shape": response_shape(data),
            "url_download_call_count": 0,
        })
        return EncodedVideoFetchResult(False, media_id, b"", _headers(manifest), manifest)
    except Exception as exc:
        return _failure(media_id, type(exc).__name__, str(exc), started, get_media_call_count)


def decode_encoded_video(encoded: str) -> tuple[bytes, str | None, bool, str]:
    if len(encoded) > MAX_ENCODED_VIDEO_CHARS:
        raise ValueError("encodedVideo exceeds maximum encoded length")
    text = encoded.strip()
    mime_type = None
    data_uri_present = False
    if text.startswith("data:"):
        header, sep, payload = text.partition(",")
        if not sep:
            raise ValueError("invalid data URI encodedVideo")
        lower = header.lower()
        if ";base64" not in lower:
            raise ValueError("data URI encodedVideo is not base64")
        mime_type = header[5:].split(";", 1)[0] or None
        if mime_type and not mime_type.startswith("video/"):
            raise ValueError("data URI encodedVideo MIME is not video")
        text = payload.strip()
        data_uri_present = True
    if not text:
        raise ValueError("empty encodedVideo")
    if any(ch.isspace() for ch in text):
        raise ValueError("encodedVideo contains whitespace")
    try:
        data = base64.b64decode(text, validate=True)
        base64_format = "standard"
    except (binascii.Error, ValueError) as exc:
        raise ValueError("invalid standard base64 encodedVideo") from exc
    if len(data) > MAX_DECODED_VIDEO_BYTES:
        raise ValueError("decoded video exceeds maximum decoded length")
    return data, mime_type, data_uri_present, base64_format


def validate_mp4_bytes(data: bytes) -> dict[str, Any]:
    if len(data) < MIN_MP4_BYTES:
        raise ValueError("Video download failed: file too small")
    head = data[:128].lstrip().lower()
    if head.startswith(b"<!doctype html") or head.startswith(b"<html"):
        raise ValueError("Video download failed: response is HTML")
    if head.startswith(b"{") or head.startswith(b"["):
        raise ValueError("Video download failed: response is JSON")
    if len(data) < 8 or data[4:8] != b"ftyp":
        raise ValueError("Video download failed: missing MP4 ftyp header")
    major_brand = data[8:12].decode("ascii", errors="replace") if len(data) >= 12 else None
    return {"mp4_valid": True, "major_brand": major_brand}


async def _fetch_url_transport(media_id: str, candidate: Any, started: str, get_media_call_count: int, data: Any, url_fetcher: Any | None = None) -> EncodedVideoFetchResult:
    url_summary = _safe_url(candidate.url)
    manifest = _base_manifest(media_id, started, get_media_call_count)
    manifest.update({
        "transport_detected": "video_url",
        "encoded_video_present": False,
        "video_url_present": True,
        "video_url_path_field": candidate.path,
        "video_url_scheme": url_summary["scheme"],
        "video_url_host": url_summary["host"],
        "video_url_path": url_summary["path"],
        "video_url_query_parameter_names": url_summary["query_parameter_names"],
        "response_shape": response_shape(data),
        "url_download_call_count": 0,
    })
    if url_summary["scheme"] != "https":
        manifest.update({"ok": False, "error_code": "video_url_not_https", "stage": "pre_url_download_validation"})
        return EncodedVideoFetchResult(False, media_id, b"", _headers(manifest), manifest)
    try:
        if url_fetcher is not None:
            manifest["url_download_call_count"] = 1
            status, content_type, video_bytes = await url_fetcher(candidate.url)
            if 300 <= status < 400:
                manifest.update({"ok": False, "error_code": "video_url_redirect_blocked", "stage": "url_download_http", "http_status": status})
                return EncodedVideoFetchResult(False, media_id, b"", _headers(manifest), manifest)
            if status < 200 or status >= 300:
                manifest.update({"ok": False, "error_code": "video_url_http_error", "stage": "url_download_http", "http_status": status})
                return EncodedVideoFetchResult(False, media_id, b"", _headers(manifest), manifest)
        else:
            timeout = aiohttp.ClientTimeout(total=120)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                manifest["url_download_call_count"] = 1
                async with session.get(candidate.url, allow_redirects=False) as resp:
                    status = resp.status
                    content_type = (resp.headers.get("Content-Type") or "").lower()
                    video_bytes = await resp.read()
                if 300 <= status < 400:
                    manifest.update({"ok": False, "error_code": "video_url_redirect_blocked", "stage": "url_download_http", "http_status": status})
                    return EncodedVideoFetchResult(False, media_id, b"", _headers(manifest), manifest)
                if status < 200 or status >= 300:
                    manifest.update({"ok": False, "error_code": "video_url_http_error", "stage": "url_download_http", "http_status": status})
                    return EncodedVideoFetchResult(False, media_id, b"", _headers(manifest), manifest)
        if content_type and not ("video" in content_type or "octet-stream" in content_type or "mp4" in content_type):
            manifest.update({"ok": False, "error_code": "video_url_content_type_invalid", "stage": "url_download_content_validation", "content_type": content_type[:100]})
            return EncodedVideoFetchResult(False, media_id, b"", _headers(manifest), manifest)
        validate_mp4_bytes(video_bytes)
        manifest.update({
            "ok": True,
            "transport": "video_url",
            "content_type": content_type,
            "decoded_byte_length": len(video_bytes),
            "decoded_sha256": hashlib.sha256(video_bytes).hexdigest(),
            "mp4_valid": True,
            "submit_called": False,
            "poll_called": False,
            "download_called": False,
            "database_writes_performed": False,
        })
        return EncodedVideoFetchResult(True, media_id, video_bytes, _headers(manifest), manifest)
    except Exception as exc:
        manifest.update({"ok": False, "error_code": type(exc).__name__, "stage": "url_download_exception", "error_message_sanitized": str(exc)[:300]})
        return EncodedVideoFetchResult(False, media_id, b"", _headers(manifest), manifest)


def _safe_url(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    return {
        "scheme": parsed.scheme,
        "host": parsed.netloc,
        "path": parsed.path,
        "query_parameter_names": sorted({key for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}),
    }


def _base_manifest(media_id: str, started: str, count: int) -> dict[str, Any]:
    return {
        "query_started_at": started,
        "query_finished_at": _now(),
        "media_id": media_id,
        "get_media_call_count": count,
        "submit_called": False,
        "poll_called": False,
        "download_called": False,
        "database_writes_performed": False,
    }


def _failure(media_id: str, code: str, message: str, started: str, count: int) -> EncodedVideoFetchResult:
    manifest = _base_manifest(media_id, started, count)
    manifest.update({"ok": False, "error_code": code, "stage": "get_media_or_media_validation", "error_message_sanitized": message[:300], "url_download_call_count": 0})
    return EncodedVideoFetchResult(False, media_id, b"", _headers(manifest), manifest)


def _headers(manifest: dict[str, Any]) -> dict[str, str]:
    headers = {
        "X-Get-Media-Call-Count": str(manifest.get("get_media_call_count", 0)),
        "X-Submit-Called": "false",
        "X-Poll-Called": "false",
        "X-Download-Called": "false",
        "X-Database-Writes-Performed": "false",
    }
    if manifest.get("encoded_video_length") is not None:
        headers["X-Encoded-Video-Length"] = str(manifest["encoded_video_length"])
    if manifest.get("encoded_video_sha256"):
        headers["X-Encoded-Video-Sha256"] = str(manifest["encoded_video_sha256"])
    if manifest.get("decoded_byte_length") is not None:
        headers["X-Decoded-Byte-Length"] = str(manifest["decoded_byte_length"])
    if manifest.get("decoded_sha256"):
        headers["X-Decoded-Sha256"] = str(manifest["decoded_sha256"])
    if manifest.get("error_code"):
        headers["X-Error-Code"] = str(manifest["error_code"])
    if manifest.get("transport_detected"):
        headers["X-Transport-Detected"] = str(manifest["transport_detected"])
    headers["X-Url-Download-Call-Count"] = str(manifest.get("url_download_call_count", 0))
    return headers


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
