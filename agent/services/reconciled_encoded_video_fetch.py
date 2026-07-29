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

from agent.services.omni_client import resolve_encoded_video, unwrap_response


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
                "actual_encoded_video_length": actual_fp["encoded_video_length"],
                "actual_encoded_video_sha256": actual_fp["encoded_video_sha256"],
                "expected_encoded_video_length": expected_encoded_video_length,
                "expected_encoded_video_sha256": expected_encoded_video_sha256,
            })
            return EncodedVideoFetchResult(False, media_id, b"", _headers(manifest), manifest)
        video_bytes, mime_type, data_uri_present, base64_format = decode_encoded_video(encoded)
        validate_mp4_bytes(video_bytes)
        decoded_sha = hashlib.sha256(video_bytes).hexdigest()
        manifest = _base_manifest(media_id, started, get_media_call_count)
        manifest.update({
            "ok": True,
            "transport": "encoded_video",
            "encoded_video_length": expected_encoded_video_length,
            "encoded_video_sha256": expected_encoded_video_sha256,
            "encoded_fingerprint_matched": True,
            "data_uri_present": data_uri_present,
            "mime_type": mime_type,
            "base64_format": base64_format,
            "decoded_byte_length": len(video_bytes),
            "decoded_sha256": decoded_sha,
            "mp4_valid": True,
            "submit_called": False,
            "poll_called": False,
            "download_called": False,
            "database_writes_performed": False,
        })
        return EncodedVideoFetchResult(True, media_id, video_bytes, _headers(manifest), manifest)
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
    manifest.update({"ok": False, "error_code": code, "error_message_sanitized": message[:300]})
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
    return headers


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
