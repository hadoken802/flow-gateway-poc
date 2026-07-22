"""Omni Flash 10s reference-image video adapter."""
import random
import time
import uuid
from dataclasses import dataclass
from urllib.parse import urlparse
from typing import Any

from agent.config import GOOGLE_API_KEY, GOOGLE_FLOW_API
from agent.services.flow_client import FlowClient, get_flow_client
from agent.services.headers import random_headers


OMNI_VIDEO_MODEL_KEY = "abra_r2v_10s"
OMNI_ASPECT_RATIO = "VIDEO_ASPECT_RATIO_PORTRAIT"
OMNI_CREDIT_COST = 15


class OmniClient:
    def __init__(self, flow_client: FlowClient | None = None):
        self.flow_client = flow_client or get_flow_client()

    async def submit_reference_video(
        self,
        project_id: str,
        reference_media_ids: list[str],
        prompt: str,
        user_paygate_tier: str = "PAYGATE_TIER_NOT_PAID",
    ) -> dict:
        if not reference_media_ids:
            return {"error": "reference_media_ids is required"}

        batch_id = str(uuid.uuid4())
        ts = int(time.time() * 1000)
        body = {
            "clientContext": {
                "projectId": project_id,
                "tool": "PINHOLE",
                "recaptchaContext": {
                    "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
                    "token": "",
                },
                "sessionId": f";{ts}",
                "userPaygateTier": user_paygate_tier,
            },
            "mediaGenerationContext": {
                "batchId": batch_id,
                "audioFailurePreference": "BLOCK_SILENCED_VIDEOS",
            },
            "requests": [{
                "aspectRatio": OMNI_ASPECT_RATIO,
                "metadata": {},
                "referenceImages": [
                    {"mediaId": media_id, "imageUsageType": "IMAGE_USAGE_TYPE_ASSET"}
                    for media_id in reference_media_ids
                ],
                "seed": random.randint(1, 2_147_483_647),
                "textInput": {"structuredPrompt": {"parts": [{"text": prompt}]}},
                "videoModelKey": OMNI_VIDEO_MODEL_KEY,
            }],
            "useV2ModelConfig": True,
        }

        url = f"{GOOGLE_FLOW_API}/v1/video:batchAsyncGenerateVideoReferenceImages?key={GOOGLE_API_KEY}"
        return await self.flow_client._send("api_request", {
            "url": url,
            "method": "POST",
            "headers": random_headers(),
            "body": body,
            "captchaAction": "VIDEO_GENERATION",
        }, timeout=60)

    async def check_status(self, project_id: str, media_name: str) -> dict:
        url = f"{GOOGLE_FLOW_API}/v1/video:batchCheckAsyncVideoGenerationStatus?key={GOOGLE_API_KEY}"
        return await self.flow_client._send("api_request", {
            "url": url,
            "method": "POST",
            "headers": random_headers(),
            "body": {"media": [{"name": media_name, "projectId": project_id}]},
        }, timeout=30)


def get_omni_client() -> OmniClient:
    return OmniClient()


def unwrap_response(result: dict) -> dict:
    return result.get("data", result) if isinstance(result, dict) else {}


def extract_submit_fields(data: dict[str, Any]) -> dict[str, Any]:
    workflows = data.get("workflows") if isinstance(data, dict) else None
    media = data.get("media") if isinstance(data, dict) else None
    workflow = workflows[0] if isinstance(workflows, list) and workflows else {}
    media_item = media[0] if isinstance(media, list) and media else {}
    metadata = workflow.get("metadata") if isinstance(workflow, dict) else {}
    operation = media_item.get("operation") if isinstance(media_item, dict) else {}
    if not operation and isinstance(media_item, dict):
        video = media_item.get("video")
        generated = video.get("generatedVideo") if isinstance(video, dict) else None
        operation = generated.get("operation") if isinstance(generated, dict) else {}
    return {
        "output_media_id": media_item.get("name"),
        "workflow_id": workflow.get("name") or media_item.get("workflowId"),
        "operation_name": operation.get("name") if isinstance(operation, dict) else None,
        "upstream_batch_id": metadata.get("batchId") if isinstance(metadata, dict) else None,
        "remaining_credits": data.get("remainingCredits"),
        "upstream_status": _extract_media_status(media_item),
    }


def normalize_generation_status(status: str | None) -> str:
    value = (status or "").upper()
    if "SCHEDULED" in value or "QUEUED" in value:
        return "scheduled"
    if "ACTIVE" in value or "PROCESSING" in value or "RUNNING" in value:
        return "active"
    if any(token in value for token in ("SUCCESS", "SUCCEEDED", "COMPLETE", "COMPLETED")):
        return "completed"
    if any(token in value for token in ("FAIL", "CANCEL", "ERROR", "REJECT")):
        return "failed"
    return "processing"


def extract_status_fields(data: dict[str, Any]) -> dict[str, Any]:
    media = data.get("media") if isinstance(data, dict) else None
    item = media[0] if isinstance(media, list) and media else data
    status = _extract_media_status(item)
    return {
        "status": normalize_generation_status(status),
        "output_media_id": find_completed_video_media_id(data) or (item.get("name") if isinstance(item, dict) else None),
        "operation_name": _extract_operation_name(item),
        "raw_status": status,
    }


@dataclass
class VideoCandidate:
    url: str
    path: str
    priority: int
    domain: str


@dataclass
class EncodedVideoCandidate:
    encoded_video: str
    path: str
    priority: int


def resolve_encoded_video(payload: Any) -> EncodedVideoCandidate | None:
    candidates: list[EncodedVideoCandidate] = []

    def walk(obj: Any, path: list[str], in_video_media: bool = False):
        if isinstance(obj, dict):
            path_text = ".".join(path).lower()
            media_type = " ".join(str(obj.get(k, "")) for k in ("type", "mimeType", "contentType", "mediaType", "mediaCategory")).lower()
            is_video = in_video_media or "video" in path_text or "video" in media_type
            for key, value in obj.items():
                next_path = path + [key]
                next_text = ".".join(next_path).lower()
                if key == "encodedVideo" and isinstance(value, str) and is_video:
                    priority = 10 if next_text == "video.encodedvideo" else 20
                    candidates.append(EncodedVideoCandidate(value, ".".join(next_path), priority))
                else:
                    walk(value, next_path, is_video)
        elif isinstance(obj, list):
            for index, item in enumerate(obj):
                walk(item, path + [str(index)], in_video_media)

    walk(payload, [])
    candidates.sort(key=lambda item: item.priority)
    return candidates[0] if candidates else None


def resolve_video_candidates(payload: Any) -> list[VideoCandidate]:
    candidates: list[VideoCandidate] = []
    url_keys = {"fifeUrl", "servingUri", "videoUrl", "downloadUrl", "downloadUri", "playbackUrl", "signedUrl", "uri", "url"}
    excluded = {"thumbnail", "poster", "image", "avatar", "preview"}

    def walk(obj: Any, path: list[str], in_video_media: bool = False):
        if isinstance(obj, dict):
            lowered_path = ".".join(path).lower()
            media_type = " ".join(str(obj.get(k, "")) for k in ("type", "mimeType", "contentType", "mediaType", "mediaCategory")).lower()
            is_video = in_video_media or "video" in lowered_path or "video" in media_type
            for key, value in obj.items():
                next_path = path + [key]
                key_lower = key.lower()
                path_lower = ".".join(next_path).lower()
                if isinstance(value, str) and key in url_keys and value.startswith("http"):
                    if any(token in path_lower for token in excluded) and "video" not in path_lower:
                        continue
                    priority = 50
                    if "video" in path_lower and key in {"fifeUrl", "servingUri", "downloadUrl", "downloadUri"}:
                        priority = 10
                    elif is_video:
                        priority = 20
                    elif key in {"fifeUrl", "servingUri"}:
                        priority = 30
                    candidates.append(VideoCandidate(value, ".".join(next_path), priority, urlparse(value).netloc))
                else:
                    walk(value, next_path, is_video)
        elif isinstance(obj, list):
            for index, item in enumerate(obj):
                walk(item, path + [str(index)], in_video_media)

    walk(payload, [])
    return sorted(candidates, key=lambda item: item.priority)


def find_completed_video_media_id(payload: Any) -> str | None:
    found: list[tuple[int, str]] = []

    def walk(obj: Any, path: list[str], parent: dict | None = None):
        if isinstance(obj, dict):
            status_text = " ".join(str(v) for k, v in obj.items() if "status" in k.lower()).lower()
            type_text = " ".join(str(obj.get(k, "")) for k in ("type", "mimeType", "contentType", "mediaType", "mediaCategory", "videoModelKey")).lower()
            path_text = ".".join(path).lower()
            is_completed = any(token in status_text for token in ("completed", "complete", "succeeded", "successful", "success"))
            is_video = "video" in type_text or "video" in path_text or "abra_r2v_10s" in type_text
            for key in ("mediaId", "name", "primaryMediaId"):
                value = obj.get(key)
                if isinstance(value, str) and is_completed and is_video:
                    priority = 10 if key in {"mediaId", "name"} else 20
                    found.append((priority, value))
            for key, value in obj.items():
                walk(value, path + [key], obj)
        elif isinstance(obj, list):
            for index, item in enumerate(obj):
                walk(item, path + [str(index)], parent)

    walk(payload, [])
    found.sort(key=lambda item: item[0])
    return found[0][1] if found else None


def extract_video_url(data: dict[str, Any]) -> str | None:
    if not isinstance(data, dict):
        return None
    candidates = resolve_video_candidates(data)
    return candidates[0].url if candidates else None


def sanitized_response_shape(value: Any, depth: int = 0) -> Any:
    if depth >= 6:
        return type(value).__name__
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            lower = key.lower()
            if any(secret in lower for secret in ("authorization", "cookie", "token", "key")):
                result[key] = "redacted"
            elif isinstance(item, str) and item.startswith("http"):
                parsed = urlparse(item)
                result[key] = {"type": "url", "domain": parsed.netloc}
            elif lower in {"mediaid", "name", "primarymediaid", "output_media_id"} and isinstance(item, str):
                result[key] = f"{item[:8]}..."
            else:
                result[key] = sanitized_response_shape(item, depth + 1)
        return result
    if isinstance(value, list):
        return [sanitized_response_shape(value[0], depth + 1)] if value else []
    return type(value).__name__


def response_shape(value: Any, depth: int = 0) -> Any:
    if depth >= 4:
        return type(value).__name__
    if isinstance(value, dict):
        return {k: response_shape(v, depth + 1) for k, v in value.items()}
    if isinstance(value, list):
        return [response_shape(value[0], depth + 1)] if value else []
    return type(value).__name__


def _extract_media_status(item: dict[str, Any]) -> str | None:
    if not isinstance(item, dict):
        return None
    media_status = item.get("mediaStatus")
    if isinstance(media_status, dict):
        return media_status.get("mediaGenerationStatus") or media_status.get("status")
    metadata = item.get("mediaMetadata")
    if isinstance(metadata, dict):
        nested_status = metadata.get("mediaStatus")
        if isinstance(nested_status, dict):
            return nested_status.get("mediaGenerationStatus") or nested_status.get("status")
    return item.get("mediaGenerationStatus") or item.get("status")


def _extract_operation_name(item: dict[str, Any]) -> str | None:
    if not isinstance(item, dict):
        return None
    operation = item.get("operation")
    if isinstance(operation, dict) and operation.get("name"):
        return operation["name"]
    video = item.get("video")
    generated = video.get("generatedVideo") if isinstance(video, dict) else None
    operation = generated.get("operation") if isinstance(generated, dict) else None
    if isinstance(operation, dict):
        return operation.get("name")
    return None
