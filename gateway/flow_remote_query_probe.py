"""Read-only Google Flow remote project query probe.

This module is deliberately isolated from submission reconciliation. It parses
candidate page/query payloads and reports confidence without calling Flow.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from typing import Any


@dataclass(frozen=True)
class RemoteProjectRecord:
    created_at: str | None = None
    updated_at: str | None = None
    status: str | None = None
    workflow_id: str | None = None
    operation_name: str | None = None
    batch_id: str | None = None
    output_media_id: str | None = None
    input_media_id: str | None = None
    media_type: str | None = None
    duration: int | None = None
    aspect_ratio: str | None = None
    downloadable: bool = False
    prompt_hash: str | None = None
    source_path: str | None = None
    confidence: str = "low"


@dataclass(frozen=True)
class RemoteProjectQueryResult:
    query_available: bool
    query_source: str
    query_level: int
    query_complete: bool
    project_id: str
    records: list[RemoteProjectRecord] = field(default_factory=list)
    exact_matches: list[RemoteProjectRecord] = field(default_factory=list)
    ambiguous_matches: list[RemoteProjectRecord] = field(default_factory=list)
    pagination_complete: bool = False
    query_errors: list[str] = field(default_factory=list)


def build_next_data_project_path(build_id: str | None, locale: str | None, project_id: str) -> str | None:
    """Build the Next.js project data path from dynamic page metadata."""
    if not build_id or not project_id:
        return None
    safe_locale = locale or "en"
    return f"/fx/_next/data/{build_id}/{safe_locale}/tools/flow/project/{project_id}.json"


def query_remote_project_results(
    project_id: str,
    submitted_after: datetime | None = None,
    submitted_before: datetime | None = None,
    input_media_id: str | None = None,
    request_batch_id: str | None = None,
    workflow_id: str | None = None,
    output_media_id: str | None = None,
    prompt_hash: str | None = None,
    *,
    source: str = "unavailable",
    payload: Any | None = None,
) -> RemoteProjectQueryResult:
    """Parse an already-fetched read-only payload.

    No HTTP, database, mutation, generation, upload, or navigation is performed
    here. A payload that only proves project metadata remains Level 1.
    """
    if payload is None:
        return RemoteProjectQueryResult(
            query_available=False,
            query_source=source,
            query_level=0,
            query_complete=False,
            project_id=project_id,
            query_errors=["remote_query_unavailable"],
        )

    try:
        return _parse_payload(
            project_id,
            payload,
            submitted_after=submitted_after,
            submitted_before=submitted_before,
            input_media_id=input_media_id,
            request_batch_id=request_batch_id,
            workflow_id=workflow_id,
            output_media_id=output_media_id,
            prompt_hash=prompt_hash,
            source=source,
        )
    except (TypeError, ValueError) as exc:
        return RemoteProjectQueryResult(
            query_available=False,
            query_source=source,
            query_level=0,
            query_complete=False,
            project_id=project_id,
            query_errors=[f"payload_unreadable:{type(exc).__name__}"],
        )


def _parse_payload(
    project_id: str,
    payload: Any,
    *,
    submitted_after: datetime | None,
    submitted_before: datetime | None,
    input_media_id: str | None,
    request_batch_id: str | None,
    workflow_id: str | None,
    output_media_id: str | None,
    prompt_hash: str | None,
    source: str,
) -> RemoteProjectQueryResult:
    if not isinstance(payload, dict):
        raise TypeError("payload must be a JSON object")

    records = _extract_records(payload)
    has_project = _contains_string(payload, project_id)
    has_pagination = _has_key_fragment(payload, ("cursor", "nextCursor", "hasNextPage", "pageInfo"))
    page_shell_only = source == "next_data" and not records

    if page_shell_only:
        return RemoteProjectQueryResult(
            query_available=True,
            query_source=source,
            query_level=1,
            query_complete=False,
            project_id=project_id,
            records=[],
            pagination_complete=not has_pagination,
            query_errors=[] if has_project else ["project_id_not_found"],
        )

    exact, ambiguous = _match_records(
        records,
        submitted_after=submitted_after,
        submitted_before=submitted_before,
        input_media_id=input_media_id,
        request_batch_id=request_batch_id,
        workflow_id=workflow_id,
        output_media_id=output_media_id,
        prompt_hash=prompt_hash,
    )
    level = 2 if records else 1
    if exact:
        level = 3
    query_complete = bool(records) and not has_pagination
    return RemoteProjectQueryResult(
        query_available=True,
        query_source=source,
        query_level=level,
        query_complete=query_complete,
        project_id=project_id,
        records=records,
        exact_matches=exact,
        ambiguous_matches=ambiguous,
        pagination_complete=not has_pagination,
    )


def _extract_records(payload: Any) -> list[RemoteProjectRecord]:
    records: list[RemoteProjectRecord] = []

    def walk(value: Any, path: str) -> None:
        if isinstance(value, dict):
            record = _record_from_dict(value, path)
            if record is not None:
                records.append(record)
            for key, item in value.items():
                walk(item, f"{path}.{key}" if path else key)
        elif isinstance(value, list):
            for index, item in enumerate(value):
                walk(item, f"{path}.{index}" if path else str(index))

    walk(payload, "")
    return _dedupe_records(records)


def _record_from_dict(item: dict[str, Any], path: str) -> RemoteProjectRecord | None:
    output_id = _first_str(item, ("output_media_id", "outputMediaId", "mediaId", "primaryMediaId", "name"))
    workflow = _first_str(item, ("workflow_id", "workflowId", "workflow"))
    operation = _first_str(item, ("operation_name", "operationName", "operation"))
    batch = _first_str(item, ("batch_id", "batchId", "upstream_batch_id", "request_batch_id"))
    input_id = _first_str(item, ("input_media_id", "inputMediaId"))
    status = _first_str(item, ("status", "state"))
    media_type = _first_str(item, ("media_type", "mediaType", "type"))
    prompt = _first_prompt(item)
    created = _first_str(item, ("created_at", "createdAt", "createTime", "submitted_at", "submittedAt"))
    updated = _first_str(item, ("updated_at", "updatedAt", "updateTime", "completed_at", "completedAt"))
    looks_like_record = bool(
        status and (output_id or workflow or operation or batch or input_id)
        or media_type and "video" in media_type.lower() and (output_id or workflow)
        or batch and (workflow or output_id)
    )
    if not looks_like_record:
        return None
    return RemoteProjectRecord(
        created_at=created,
        updated_at=updated,
        status=status,
        workflow_id=workflow,
        operation_name=operation,
        batch_id=batch,
        output_media_id=output_id,
        input_media_id=input_id,
        media_type=media_type,
        duration=_first_int(item, ("duration", "durationSeconds")),
        aspect_ratio=_first_str(item, ("aspect_ratio", "aspectRatio")),
        downloadable=_has_downloadable(item),
        prompt_hash=_hash_prompt(prompt) if prompt else None,
        source_path=path,
        confidence="medium" if output_id or workflow or batch else "low",
    )


def _match_records(
    records: list[RemoteProjectRecord],
    *,
    submitted_after: datetime | None,
    submitted_before: datetime | None,
    input_media_id: str | None,
    request_batch_id: str | None,
    workflow_id: str | None,
    output_media_id: str | None,
    prompt_hash: str | None,
) -> tuple[list[RemoteProjectRecord], list[RemoteProjectRecord]]:
    exact: list[RemoteProjectRecord] = []
    ambiguous: list[RemoteProjectRecord] = []
    for record in records:
        in_window = _record_in_window(record, submitted_after, submitted_before)
        strong = any(
            [
                input_media_id and record.input_media_id == input_media_id,
                request_batch_id and record.batch_id == request_batch_id,
                workflow_id and record.workflow_id == workflow_id,
                output_media_id and record.output_media_id == output_media_id,
                prompt_hash and record.prompt_hash == prompt_hash,
            ]
        )
        if strong and (in_window or (submitted_after is None and submitted_before is None)):
            exact.append(record)
        elif in_window:
            ambiguous.append(record)
    return exact, ambiguous


def _record_in_window(record: RemoteProjectRecord, after: datetime | None, before: datetime | None) -> bool:
    if after is None and before is None:
        return True
    value = _parse_datetime(record.created_at or record.updated_at)
    if value is None:
        return False
    if after is not None and value < after:
        return False
    if before is not None and value > before:
        return False
    return True


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _dedupe_records(records: list[RemoteProjectRecord]) -> list[RemoteProjectRecord]:
    seen: set[tuple[str | None, str | None, str | None, str | None]] = set()
    result: list[RemoteProjectRecord] = []
    for record in records:
        key = (record.output_media_id, record.workflow_id, record.batch_id, record.source_path)
        if key in seen:
            continue
        seen.add(key)
        result.append(record)
    return result


def _first_str(item: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = item.get(key)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, dict):
            nested = _first_str(value, ("name", "id"))
            if nested:
                return nested
    return None


def _first_int(item: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = item.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _first_prompt(item: dict[str, Any]) -> str | None:
    value = item.get("prompt")
    if isinstance(value, str) and value:
        return value
    text_input = item.get("textInput")
    if isinstance(text_input, dict):
        structured = text_input.get("structuredPrompt")
        if isinstance(structured, dict):
            parts = structured.get("parts")
            if isinstance(parts, list):
                texts = [part.get("text") for part in parts if isinstance(part, dict) and isinstance(part.get("text"), str)]
                return "\n".join(texts) if texts else None
    return None


def _hash_prompt(prompt: str) -> str:
    return sha256(prompt.encode("utf-8")).hexdigest()


def _has_downloadable(item: dict[str, Any]) -> bool:
    for value in item.values():
        if isinstance(value, str) and value.startswith("http") and any(token in value.lower() for token in ("video", "mp4", "googleusercontent", "storage.googleapis")):
            return True
    return False


def _contains_string(value: Any, needle: str) -> bool:
    if isinstance(value, str):
        return needle in value
    if isinstance(value, dict):
        return any(_contains_string(item, needle) for item in value.values())
    if isinstance(value, list):
        return any(_contains_string(item, needle) for item in value)
    return False


def _has_key_fragment(value: Any, fragments: tuple[str, ...]) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if any(fragment.lower() in key.lower() for fragment in fragments):
                return True
            if _has_key_fragment(item, fragments):
                return True
    if isinstance(value, list):
        return any(_has_key_fragment(item, fragments) for item in value)
    return False
