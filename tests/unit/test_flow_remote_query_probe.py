from datetime import datetime, timezone

from gateway.flow_remote_query_probe import build_next_data_project_path, query_remote_project_results


FLOW001_OUTPUT = "723ecc0d-1bad-4cff-a87e-f7a636742a0b"
FLOW001_WORKFLOW = "ec7e9eba-8560-4299-a800-0faba39ef79d"
FLOW001_BATCH = "ceef7ac1-cca9-45c9-b4bd-ba9c2e9f003a"


def dt(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def test_query_available_false_is_distinct_from_empty_records():
    result = query_remote_project_results("project-1", payload=None)
    assert result.query_available is False
    assert result.records == []
    assert result.query_complete is False
    assert result.query_errors == ["remote_query_unavailable"]


def test_next_data_build_id_path_is_dynamic():
    assert build_next_data_project_path("build-1", "en", "project-1") == "/fx/_next/data/build-1/en/tools/flow/project/project-1.json"
    assert build_next_data_project_path(None, "en", "project-1") is None


def test_project_metadata_without_history_is_level_1_incomplete():
    payload = {"pageProps": {"project": {"id": "project-1", "title": "Demo"}}}
    result = query_remote_project_results("project-1", source="next_data", payload=payload)
    assert result.query_available is True
    assert result.records == []
    assert result.query_level == 1
    assert result.query_complete is False


def test_complete_remote_history_can_reach_level_2():
    payload = {
        "projectId": "project-1",
        "generations": [
            {
                "status": "completed",
                "createdAt": "2026-07-29T04:50:13+00:00",
                "mediaId": "media-1",
                "workflowId": "workflow-1",
                "batchId": "batch-1",
                "mediaType": "video",
            }
        ],
    }
    result = query_remote_project_results("project-1", source="project_history", payload=payload)
    assert result.query_available is True
    assert result.query_level == 2
    assert result.query_complete is True
    assert result.pagination_complete is True
    assert result.records[0].output_media_id == "media-1"


def test_strong_identifier_match_reaches_level_3():
    payload = {
        "projectId": "project-1",
        "history": [
            {
                "status": "completed",
                "createdAt": "2026-07-29T04:50:13+00:00",
                "mediaId": FLOW001_OUTPUT,
                "workflowId": FLOW001_WORKFLOW,
                "batchId": FLOW001_BATCH,
                "mediaType": "video",
            }
        ],
    }
    result = query_remote_project_results(
        "project-1",
        source="project_history",
        payload=payload,
        submitted_after=dt("2026-07-29T04:49:00"),
        submitted_before=dt("2026-07-29T04:51:00"),
        output_media_id=FLOW001_OUTPUT,
    )
    assert result.query_level == 3
    assert len(result.exact_matches) == 1
    assert result.exact_matches[0].workflow_id == FLOW001_WORKFLOW


def test_time_window_only_is_ambiguous_not_exact():
    payload = {
        "projectId": "project-1",
        "items": [
            {
                "status": "processing",
                "createdAt": "2026-07-29T06:32:21+00:00",
                "mediaId": "media-2",
                "mediaType": "video",
            }
        ],
    }
    result = query_remote_project_results(
        "project-1",
        source="project_history",
        payload=payload,
        submitted_after=dt("2026-07-29T06:27:00"),
        submitted_before=dt("2026-07-29T06:45:00"),
    )
    assert result.query_level == 2
    assert result.exact_matches == []
    assert len(result.ambiguous_matches) == 1


def test_incomplete_pagination_cannot_confirm_remote_empty():
    payload = {"projectId": "project-1", "generations": [], "pageInfo": {"hasNextPage": True}}
    result = query_remote_project_results("project-1", source="project_history", payload=payload)
    assert result.query_available is True
    assert result.query_complete is False
    assert result.pagination_complete is False
    assert result.records == []


def test_prompt_is_only_exposed_as_hash():
    payload = {
        "projectId": "project-1",
        "history": [
            {
                "status": "completed",
                "createdAt": "2026-07-29T04:50:13+00:00",
                "mediaId": "media-1",
                "mediaType": "video",
                "prompt": "secret prompt text",
            }
        ],
    }
    result = query_remote_project_results("project-1", source="project_history", payload=payload)
    assert result.records[0].prompt_hash
    assert result.records[0].prompt_hash != "secret prompt text"


def test_unreadable_payload_fails_closed():
    result = query_remote_project_results("project-1", source="next_data", payload=["not", "object"])
    assert result.query_available is False
    assert result.query_complete is False
    assert result.query_errors[0].startswith("payload_unreadable")


def test_flow001_positive_fixture_finds_known_success_identifiers():
    payload = {
        "projectId": "2b04c8fc-62b9-441c-8082-57e89e6f7d1f",
        "remoteHistory": [
            {
                "status": "completed",
                "createdAt": "2026-07-29T04:50:13+00:00",
                "mediaId": FLOW001_OUTPUT,
                "workflowId": FLOW001_WORKFLOW,
                "batchId": FLOW001_BATCH,
                "mediaType": "video",
                "duration": 10,
                "aspectRatio": "9:16",
            }
        ],
    }
    result = query_remote_project_results(
        "2b04c8fc-62b9-441c-8082-57e89e6f7d1f",
        source="project_history",
        payload=payload,
        output_media_id=FLOW001_OUTPUT,
    )
    assert result.query_level == 3
    assert result.exact_matches[0].output_media_id == FLOW001_OUTPUT
    assert result.exact_matches[0].workflow_id == FLOW001_WORKFLOW
    assert result.exact_matches[0].batch_id == FLOW001_BATCH


def test_next_data_positive_validation_failure_must_not_mark_level_2():
    payload = {"pageProps": {"project": {"id": "2b04c8fc-62b9-441c-8082-57e89e6f7d1f"}}}
    result = query_remote_project_results(
        "2b04c8fc-62b9-441c-8082-57e89e6f7d1f",
        source="next_data",
        payload=payload,
        output_media_id=FLOW001_OUTPUT,
    )
    assert result.query_level == 1
    assert result.exact_matches == []


def test_poc_never_models_mutation_submit_project_or_upload():
    result = query_remote_project_results("project-1", source="project_history", payload={"projectId": "project-1", "history": []})
    assert result.query_available is True
    assert not hasattr(result, "submit")
    assert not hasattr(result, "create_project")
    assert not hasattr(result, "upload")
