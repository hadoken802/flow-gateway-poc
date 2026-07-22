from unittest.mock import AsyncMock

import pytest

from agent.services.omni_client import (
    OMNI_ASPECT_RATIO,
    OMNI_VIDEO_MODEL_KEY,
    OmniClient,
    extract_submit_fields,
    extract_status_fields,
    extract_video_url,
    find_completed_video_media_id,
    normalize_generation_status,
    resolve_encoded_video,
    resolve_video_candidates,
)


@pytest.mark.asyncio
async def test_submit_reference_video_uses_omni_flash_payload(sample_uuid):
    flow_client = type("FakeFlowClient", (), {})()
    flow_client._send = AsyncMock(return_value={"data": {"ok": True}})
    client = OmniClient(flow_client)

    await client.submit_reference_video(
        project_id="project-123",
        reference_media_ids=[sample_uuid],
        prompt="A short motion prompt",
        user_paygate_tier="PAYGATE_TIER_NOT_PAID",
    )

    method, params = flow_client._send.await_args.args[:2]
    assert method == "api_request"
    assert params["captchaAction"] == "VIDEO_GENERATION"
    body = params["body"]
    assert body["clientContext"]["userPaygateTier"] == "PAYGATE_TIER_NOT_PAID"
    assert body["clientContext"]["recaptchaContext"]["token"] == ""
    assert body["clientContext"]["tool"] == "PINHOLE"
    request = body["requests"][0]
    assert request["videoModelKey"] == OMNI_VIDEO_MODEL_KEY
    assert request["aspectRatio"] == OMNI_ASPECT_RATIO
    assert body["useV2ModelConfig"] is True
    assert request["referenceImages"] == [
        {"mediaId": sample_uuid, "imageUsageType": "IMAGE_USAGE_TYPE_ASSET"}
    ]
    assert body["mediaGenerationContext"]["audioFailurePreference"] == "BLOCK_SILENCED_VIDEOS"


def test_extract_submit_fields_reads_confirmed_omni_response(sample_uuid):
    data = {
        "remainingCredits": 503505,
        "workflows": [{"name": "workflow-1", "metadata": {"batchId": "batch-1"}}],
        "media": [{
            "name": sample_uuid,
            "workflowId": "workflow-1",
            "mediaStatus": {"mediaGenerationStatus": "MEDIA_GENERATION_STATUS_SCHEDULED"},
            "operation": {"name": "operations/video-1"},
        }],
    }

    fields = extract_submit_fields(data)

    assert fields["remaining_credits"] == 503505
    assert fields["output_media_id"] == sample_uuid
    assert fields["workflow_id"] == "workflow-1"
    assert fields["operation_name"] == "operations/video-1"
    assert fields["upstream_batch_id"] == "batch-1"


def test_extract_submit_fields_reads_nested_omni_status(sample_uuid):
    data = {
        "remainingCredits": 5,
        "workflows": [{"name": "workflow-1", "metadata": {"batchId": "batch-1"}}],
        "media": [{
            "name": sample_uuid,
            "mediaMetadata": {
                "mediaStatus": {"mediaGenerationStatus": "MEDIA_GENERATION_STATUS_SUCCESSFUL"}
            },
            "video": {"generatedVideo": {"operation": {"name": "operations/video-1"}}},
        }],
    }

    fields = extract_submit_fields(data)

    assert fields["operation_name"] == "operations/video-1"
    assert fields["upstream_status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"


def test_extract_status_fields_reads_nested_operation(sample_uuid):
    fields = extract_status_fields({
        "media": [{
            "name": sample_uuid,
            "mediaMetadata": {
                "mediaStatus": {"mediaGenerationStatus": "MEDIA_GENERATION_STATUS_SUCCESSFUL"}
            },
            "video": {"generatedVideo": {"operation": {"name": "operations/video-1"}}},
        }]
    })

    assert fields["status"] == "completed"
    assert fields["operation_name"] == "operations/video-1"


@pytest.mark.parametrize(
    ("upstream", "local"),
    [
        ("MEDIA_GENERATION_STATUS_SCHEDULED", "scheduled"),
        ("MEDIA_GENERATION_STATUS_ACTIVE", "active"),
        ("MEDIA_GENERATION_STATUS_SUCCESSFUL", "completed"),
        ("MEDIA_GENERATION_STATUS_FAILED", "failed"),
    ],
)
def test_normalize_generation_status(upstream, local):
    assert normalize_generation_status(upstream) == local


def test_resolve_video_url_from_top_level():
    assert extract_video_url({"servingUri": "https://video.example/a.mp4"}) == "https://video.example/a.mp4"


def test_resolve_video_url_from_nested_dict():
    data = {"media": {"video": {"generatedVideo": {"downloadUrl": "https://cdn.example/v.mp4"}}}}
    assert resolve_video_candidates(data)[0].path == "media.video.generatedVideo.downloadUrl"


def test_resolve_video_url_from_media_list():
    data = {"media": [{"mediaType": "video/mp4", "url": "https://media.example/v.mp4"}]}
    assert extract_video_url(data) == "https://media.example/v.mp4"


def test_resolve_video_url_excludes_thumbnail_image():
    data = {"thumbnail": {"url": "https://img.example/thumb.jpg"}, "video": {"servingUri": "https://v.example/a.mp4"}}
    assert extract_video_url(data) == "https://v.example/a.mp4"


def test_find_completed_video_media_id_from_nested_payload():
    data = {
        "media": [
            {"mediaId": "image-1", "mediaType": "image/png", "mediaStatus": {"mediaGenerationStatus": "COMPLETED"}},
            {"mediaId": "video-1", "mediaType": "video/mp4", "mediaStatus": {"mediaGenerationStatus": "MEDIA_GENERATION_STATUS_SUCCESSFUL"}},
        ]
    }
    assert find_completed_video_media_id(data) == "video-1"


def test_resolve_encoded_video_from_confirmed_media_shape():
    candidate = resolve_encoded_video({"name": "media-1", "video": {"encodedVideo": "QUFBQQ=="}})

    assert candidate.path == "video.encodedVideo"
    assert candidate.encoded_video == "QUFBQQ=="


def test_resolve_encoded_video_from_generated_video_shape():
    candidate = resolve_encoded_video({"generatedVideo": {"encodedVideo": "QUFBQQ=="}})

    assert candidate.path == "generatedVideo.encodedVideo"


def test_resolve_encoded_video_from_media_video_shape():
    candidate = resolve_encoded_video({"media": {"video": {"encodedVideo": "QUFBQQ=="}}})

    assert candidate.path == "media.video.encodedVideo"


def test_resolve_encoded_video_from_nested_video_media():
    data = {"media": [{"mediaType": "video/mp4", "video": {"encodedVideo": "QUFBQQ=="}}]}

    assert resolve_encoded_video(data).path == "media.0.video.encodedVideo"


def test_resolve_encoded_video_prefers_explicit_path_over_recursive_match():
    data = {
        "media": [{"mediaType": "video/mp4", "encodedVideo": "recursive"}],
        "video": {"encodedVideo": "explicit"},
    }

    candidate = resolve_encoded_video(data)

    assert candidate.path == "video.encodedVideo"
    assert candidate.encoded_video == "explicit"


def test_resolve_encoded_video_ignores_non_video_long_string():
    data = {"debug": {"encodedVideo": "A" * 1000}}

    assert resolve_encoded_video(data) is None
