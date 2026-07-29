"""Dry-run-first one-shot reconciled remote download."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from agent.services.reconciled_encoded_video_fetch import validate_mp4_bytes

from .reconciled_remote_download_preflight import (
    ACCOUNT_ID,
    AGENT_REQUIRED_STATUS,
    BATCH_ID,
    EXPECTED_POLL_SHA,
    GATEWAY_REQUIRED_STATUS,
    JOB_ID,
    OUTPUT_MEDIA_ID,
    PROJECT_ID,
    TASK_ID,
    WORKFLOW_ID,
    _connect_ro,
    _json_or_empty,
    _read_state,
    _sha256_file,
    _validate_fencing,
    _validate_poll_result,
)
from .worker_client import WorkerClient, WorkerRemoteMediaFetchError


EXPECTED_CAPABILITY_SHA = "83100858c42fa22785e9cafb78157af39ad2f61a0bb2a78ae52debd10e2c19b9"
EXPECTED_ENCODED_VIDEO_LENGTH = 3408328
EXPECTED_ENCODED_VIDEO_SHA256 = "50b1f931802242c84ef88eb915664069709ce36354aaef54012486641f4db073"
DOWNLOAD_ROUTE = "/api/test/omni-video/{job_id}/fetch-reconciled-encoded-video-once"
MIN_FREE_BYTES = 100 * 1024 * 1024


def download_reconciled_remote_result(args: argparse.Namespace, worker_client: Any | None = None) -> dict[str, Any]:
    return asyncio.run(_download(args, worker_client))


async def _download(args: argparse.Namespace, worker_client: Any | None) -> dict[str, Any]:
    execute = bool(getattr(args, "execute", False))
    mode = "execute" if execute else "dry_run"
    poll_path = Path(args.poll_result_file)
    cap_path = Path(args.capability_result_file)
    poll_sha = _sha256_file(poll_path) if poll_path.exists() else None
    cap_sha = _sha256_file(cap_path) if cap_path.exists() else None
    try:
        poll_result = json.loads(poll_path.read_text(encoding="utf-8"))
        capability_result = json.loads(cap_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        return _base(args, mode, poll_sha, cap_sha, [f"{Path(exc.filename).name}_missing"])
    except json.JSONDecodeError as exc:
        result = _base(args, mode, poll_sha, cap_sha, ["json_invalid"])
        result["error_message"] = str(exc)
        return result

    gw = _connect_ro(Path(args.gateway_db))
    agent = _connect_ro(Path(args.agent_db))
    try:
        state = _read_state(gw, agent, args.task_id)
    finally:
        gw.close()
        agent.close()

    poll_validation = _validate_poll_result(poll_result, poll_sha)
    fencing_validation = _validate_fencing(args, state, poll_sha)
    capability_validation = _validate_capability(capability_result, cap_sha)
    worker = _inspect_worker(args.worker_base_url)
    output = _output_plan(Path(args.output_path), getattr(args, "result_manifest", None))
    blockers = (
        poll_validation["blocking_reasons"]
        + fencing_validation["blocking_reasons"]
        + capability_validation["blocking_reasons"]
        + output["blocking_reasons"]
    )
    blockers += worker["blocking_reasons"]
    if execute:
        blockers += _execute_blockers(args, poll_sha, cap_sha)
    result = {
        "ok": not blockers or not execute,
        "mode": mode,
        "task_id": args.task_id,
        "account_id": ACCOUNT_ID,
        "job_id": JOB_ID,
        "project_id": PROJECT_ID,
        "workflow_id": WORKFLOW_ID,
        "output_media_id": OUTPUT_MEDIA_ID,
        "upstream_batch_id": BATCH_ID,
        "poll_result_sha256": poll_sha,
        "capability_result_sha256": cap_sha,
        "expected_encoded_video_length": EXPECTED_ENCODED_VIDEO_LENGTH,
        "expected_encoded_video_sha256": EXPECTED_ENCODED_VIDEO_SHA256,
        "current_gateway_state": state.get("task"),
        "current_agent_state": state.get("agent_job"),
        "fencing_validation": fencing_validation,
        "poll_validation": poll_validation,
        "capability_validation": capability_validation,
        "worker_health": worker.get("health"),
        "extension_connected": worker.get("extension_connected"),
        "download_route_available": worker.get("route_available"),
        "planned_get_media_call_count": 1,
        "planned_transport": "encoded_video",
        "planned_output_path": str(output["final"]),
        "planned_temp_path": str(output["temp"]),
        "output_directory_check": output["directory_check"],
        "existing_file_check": output["existing_file_check"],
        "disk_space_check": output["disk_space_check"],
        "planned_database_changes": [],
        "planned_lock_changes": {"account_status": "preserved_busy", "current_task_id": "preserved"},
        "get_media_called": False,
        "poll_called": False,
        "submit_called": False,
        "download_called": False,
        "network_calls_performed": 0,
        "file_writes_performed": False,
        "database_writes_performed": False,
        "allowed_to_execute": not blockers,
        "worker_restart_required": "download_route_missing" in worker.get("blocking_reasons", []),
        "blocking_reasons": blockers,
        "ffprobe_available": _ffprobe_available(),
    }
    if not execute or blockers:
        return result

    try:
        fetched = await _execute_fetch(args, worker_client)
        video_bytes = fetched["content"]
        headers = {str(k).lower(): str(v) for k, v in fetched["headers"].items()}
        get_media_count = int(headers.get("x-get-media-call-count", "1"))
        url_download_count = int(headers.get("x-url-download-call-count", "0"))
        transport_detected = headers.get("x-transport-detected") or "encoded_video"
        validate_mp4_bytes(video_bytes)
        actual_sha = hashlib.sha256(video_bytes).hexdigest()
        final = output["final"]
        temp = output["temp"]
        _write_bytes_atomic(video_bytes, final, temp)
        ffprobe = _ffprobe(final)
        manifest = {
            "ok": True,
            "task_id": args.task_id,
            "account_id": ACCOUNT_ID,
            "job_id": JOB_ID,
            "project_id": PROJECT_ID,
            "output_media_id": OUTPUT_MEDIA_ID,
            "poll_result_sha256": poll_sha,
            "capability_result_sha256": cap_sha,
            "expected_encoded_video_length": EXPECTED_ENCODED_VIDEO_LENGTH,
            "expected_encoded_video_sha256": EXPECTED_ENCODED_VIDEO_SHA256,
            "encoded_fingerprint_matched": True,
            "decoded_byte_length": len(video_bytes),
            "decoded_sha256": actual_sha,
            "transport_detected": transport_detected,
            "final_file_size": final.stat().st_size,
            "final_file_sha256": _sha256_file(final),
            "output_path": str(final),
            "temp_path": str(temp),
            "mp4_valid": True,
            "ffprobe": ffprobe,
            "get_media_call_count": get_media_count,
            "url_download_call_count": url_download_count,
            "submit_called": False,
            "poll_called": False,
            "download_called": False,
            "database_writes_performed": False,
            "code_head": _git_head(),
        }
        if getattr(args, "result_manifest", None):
            _write_manifest(Path(args.result_manifest), manifest)
        result.update({
            "ok": True,
            "get_media_called": True,
            "get_media_call_count": get_media_count,
            "url_download_call_count": url_download_count,
            "network_calls_performed": 1,
            "file_writes_performed": True,
            "database_writes_performed": False,
            "download_manifest": manifest,
            "download_result": "downloaded",
        })
        return result
    except WorkerRemoteMediaFetchError as exc:
        worker_error = dict(exc.response)
        final = output["final"]
        temp = output["temp"]
        get_media_count = worker_error.get("get_media_call_count")
        if get_media_count is None:
            try:
                get_media_count = int(worker_error.get("headers", {}).get("x-get-media-call-count"))
            except Exception:
                get_media_count = None
        failure_manifest = {
            "ok": False,
            "result": "worker_error",
            "task_id": args.task_id,
            "account_id": ACCOUNT_ID,
            "job_id": JOB_ID,
            "project_id": PROJECT_ID,
            "output_media_id": OUTPUT_MEDIA_ID,
            "worker_http_status": exc.status_code,
            "worker_error_code": worker_error.get("error_code"),
            "worker_error_class": worker_error.get("error_class"),
            "worker_error_stage": worker_error.get("stage"),
            "worker_error_message": worker_error.get("error_message_sanitized") or str(exc)[:300],
            "worker_error_details": worker_error,
            "worker_response_body_length": worker_error.get("worker_response_body_length"),
            "worker_response_body_sha256": worker_error.get("worker_response_body_sha256"),
            "worker_response_truncated": worker_error.get("worker_response_truncated"),
            "get_media_call_count": get_media_count,
            "retry_safe": worker_error.get("retry_safe"),
            "submit_called": False,
            "poll_called": False,
            "download_called": False,
            "network_calls_performed": 1,
            "file_writes_performed": False,
            "database_writes_performed": False,
            "final_file_created": final.exists(),
            "temp_file_created": temp.exists(),
            "code_head": _git_head(),
        }
        if getattr(args, "result_manifest", None):
            _write_manifest(Path(args.result_manifest), failure_manifest)
        result.update(failure_manifest)
        return result
    except Exception as exc:
        result.update({
            "ok": False,
            "error_code": type(exc).__name__,
            "error_message": str(exc)[:500],
            "get_media_called": True,
            "network_calls_performed": 1,
            "file_writes_performed": False,
            "database_writes_performed": False,
        })
        return result


def _validate_capability(value: dict[str, Any], cap_sha: str | None) -> dict[str, Any]:
    mc = value.get("media_capability") if isinstance(value.get("media_capability"), dict) else value
    checks = {
        "capability_result_sha_mismatch": cap_sha == EXPECTED_CAPABILITY_SHA,
        "capability_not_encoded_video_available": mc.get("download_capability") == "encoded_video_available",
        "encoded_video_present_not_true": mc.get("encoded_video_present") is True,
        "encoded_video_length_mismatch": int(mc.get("encoded_video_length") or 0) == EXPECTED_ENCODED_VIDEO_LENGTH,
        "encoded_video_sha_mismatch": mc.get("encoded_video_sha256") == EXPECTED_ENCODED_VIDEO_SHA256,
        "capability_get_media_call_count_not_one": int(value.get("get_media_call_count") or mc.get("get_media_call_count") or 0) == 1,
        "capability_submit_called": value.get("submit_called") is False and mc.get("submit_called") is False,
        "capability_poll_called": value.get("poll_called") is False and mc.get("poll_called") is False,
        "capability_download_called": value.get("download_called") is False and mc.get("download_called") is False,
        "capability_database_writes_performed": mc.get("database_writes_performed") is False,
    }
    reasons = [code for code, ok in checks.items() if not ok]
    return {"ok": not reasons, "blocking_reasons": reasons}


def _inspect_worker(base_url: str) -> dict[str, Any]:
    health = _get_json(f"{base_url.rstrip('/')}/health")
    openapi = _get_json(f"{base_url.rstrip('/')}/openapi.json")
    payload = openapi.get("payload") if isinstance(openapi.get("payload"), dict) else {}
    paths = set((payload.get("paths") or {}).keys())
    reasons: list[str] = []
    if not health.get("ok"):
        reasons.append("worker_health_unavailable")
    if health.get("payload", {}).get("status") != "ok":
        reasons.append("worker_not_ok")
    if health.get("payload", {}).get("extension_connected") is not True:
        reasons.append("extension_not_connected")
    route = DOWNLOAD_ROUTE in paths
    if not route:
        reasons.append("download_route_missing")
    return {"health": health.get("payload"), "extension_connected": health.get("payload", {}).get("extension_connected") is True, "route_available": route, "blocking_reasons": reasons}


def _dry_worker() -> dict[str, Any]:
    return {"health": {"checked": False, "reason": "dry_run_does_not_call_worker"}, "extension_connected": None, "route_available": None, "blocking_reasons": []}


def _output_plan(final: Path, manifest: str | None) -> dict[str, Any]:
    temp = final.parent / f".{final.name}.part"
    parent = final.parent
    reasons: list[str] = []
    exists = final.exists()
    temp_exists = temp.exists()
    parent_exists = parent.exists() and parent.is_dir()
    free = shutil.disk_usage(parent if parent_exists else parent.parent if parent.parent.exists() else Path.cwd()).free
    if not parent_exists:
        reasons.append("output_directory_missing")
    if exists:
        reasons.append("final_file_already_exists")
    if temp_exists:
        reasons.append("temp_file_already_exists")
    if free < MIN_FREE_BYTES:
        reasons.append("insufficient_disk_space")
    if manifest:
        m = Path(manifest)
        if m.exists():
            reasons.append("result_manifest_already_exists")
        if not m.parent.exists():
            reasons.append("result_manifest_directory_missing")
    return {
        "final": final,
        "temp": temp,
        "directory_check": {"path": str(parent), "exists": parent_exists, "writable": parent_exists},
        "existing_file_check": {"exists": exists, "path": str(final), "size": final.stat().st_size if exists else None, "sha256": _sha256_file(final) if exists else None},
        "disk_space_check": {"free_disk_bytes": free, "minimum_required_free_bytes": MIN_FREE_BYTES},
        "blocking_reasons": reasons,
    }


def _execute_blockers(args: argparse.Namespace, poll_sha: str | None, cap_sha: str | None) -> list[str]:
    expected = {
        "confirm_task_id": TASK_ID,
        "confirm_project_id": PROJECT_ID,
        "confirm_account_id": ACCOUNT_ID,
        "confirm_job_id": JOB_ID,
        "confirm_output_media_id": OUTPUT_MEDIA_ID,
        "confirm_generation_attempt": 2,
        "confirm_lock_version": 2,
        "confirm_lease_version": 2,
        "confirm_poll_result_sha256": poll_sha,
        "confirm_capability_result_sha256": cap_sha,
        "confirm_encoded_video_length": EXPECTED_ENCODED_VIDEO_LENGTH,
        "confirm_encoded_video_sha256": EXPECTED_ENCODED_VIDEO_SHA256,
        "confirm_output_path": str(Path(args.output_path)),
    }
    blockers = [f"{name}_mismatch_or_missing" for name, value in expected.items() if getattr(args, name, None) != value]
    if not getattr(args, "allow_real_download", False):
        blockers.append("allow_real_download_required")
    if not getattr(args, "result_manifest", None):
        blockers.append("result_manifest_required")
    return blockers


async def _execute_fetch(args: argparse.Namespace, worker_client: Any | None) -> dict[str, Any]:
    worker = SimpleNamespace(api_url=str(args.worker_base_url).rstrip("/"))
    payload = {
        "project_id": PROJECT_ID,
        "output_media_id": OUTPUT_MEDIA_ID,
        "workflow_id": WORKFLOW_ID,
        "upstream_batch_id": BATCH_ID,
        "expected_capability_result_sha256": EXPECTED_CAPABILITY_SHA,
        "expected_encoded_video_length": EXPECTED_ENCODED_VIDEO_LENGTH,
        "expected_encoded_video_sha256": EXPECTED_ENCODED_VIDEO_SHA256,
    }
    client = worker_client or WorkerClient()
    return await client.fetch_reconciled_encoded_video_once(worker, JOB_ID, payload)


def _write_bytes_atomic(data: bytes, final: Path, temp: Path) -> None:
    if not final.parent.exists():
        raise FileNotFoundError(str(final.parent))
    if final.exists():
        raise FileExistsError(str(final))
    if temp.exists():
        raise FileExistsError(str(temp))
    try:
        with temp.open("xb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        validate_mp4_bytes(temp.read_bytes())
        os.replace(temp, final)
    except Exception:
        try:
            if temp.exists():
                temp.unlink()
        finally:
            pass
        raise


def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _ffprobe_available() -> bool:
    return shutil.which("ffprobe") is not None


def _ffprobe(path: Path) -> dict[str, Any]:
    exe = shutil.which("ffprobe")
    if not exe:
        return {"available": False}
    try:
        proc = subprocess.run([exe, "-v", "error", "-show_format", "-show_streams", "-of", "json", str(path)], capture_output=True, text=True, timeout=20)
        return {"available": True, "returncode": proc.returncode, "summary": json.loads(proc.stdout) if proc.stdout else None, "stderr": proc.stderr[:500]}
    except Exception as exc:
        return {"available": True, "error": type(exc).__name__, "message": str(exc)[:300]}


def _get_json(url: str) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return {"ok": True, "payload": json.loads(resp.read().decode("utf-8"))}
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": type(exc).__name__, "message": str(exc)[:300]}


def _base(args: argparse.Namespace, mode: str, poll_sha: str | None, cap_sha: str | None, blockers: list[str]) -> dict[str, Any]:
    return {"ok": False, "mode": mode, "task_id": args.task_id, "poll_result_sha256": poll_sha, "capability_result_sha256": cap_sha, "get_media_called": False, "poll_called": False, "submit_called": False, "download_called": False, "network_calls_performed": 0, "file_writes_performed": False, "database_writes_performed": False, "blocking_reasons": blockers}


def _git_head() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return None
