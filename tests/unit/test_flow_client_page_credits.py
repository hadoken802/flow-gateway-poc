import time
import asyncio
import json
from pathlib import Path

import pytest

from agent.services.flow_client import FlowClient


@pytest.mark.asyncio
async def test_page_credits_replace_obsolete_oauth_credits_request():
    client = FlowClient()
    await client.handle_message(
        {
            "type": "credits_captured",
            "credits": 1050,
            "capturedAt": int(time.time() * 1000),
        }
    )

    result = await client.get_credits()

    assert result["credits"] == 1050
    assert result["creditsSource"] == "flow_page"


@pytest.mark.asyncio
async def test_invalid_page_credits_are_ignored():
    client = FlowClient()
    await client.handle_message({"type": "credits_captured", "credits": -1})

    assert client.page_credits is None


def test_extension_reads_current_flow_page_credits_and_returns_over_websocket():
    content = Path("extension/content.js").read_text(encoding="utf-8")
    background = Path("extension/background.js").read_text(encoding="utf-8")

    assert "GET_PAGE_CREDITS" in content
    assert "Google Flow" in content
    assert "msg.method === 'page_credits'" in background
    assert "fallbackToWebSocket({\n      id: msg.id,\n      status: 200" in background


def test_extension_version_invalidates_stale_service_worker_cache():
    manifest = json.loads(Path("extension/manifest.json").read_text(encoding="utf-8"))

    assert tuple(map(int, manifest["version"].split("."))) >= (0, 2, 3)


@pytest.mark.asyncio
async def test_create_project_uses_authenticated_flow_page(monkeypatch):
    client = FlowClient()
    calls = []

    async def fake_send(method, params, timeout=300, request_id=None):
        calls.append((method, params, timeout))
        return {"data": {"projectId": "eb7ee09c-bd8d-4d7c-bbbb-a8191b4a0dc0"}}

    monkeypatch.setattr(client, "_send", fake_send)

    result = await client.create_project("测试项目")

    assert calls == [("page_create_project", {"projectTitle": "测试项目", "toolName": "PINHOLE"}, 75)]
    assert result["data"]["result"]["data"]["json"]["result"]["projectId"] == "eb7ee09c-bd8d-4d7c-bbbb-a8191b4a0dc0"


def test_extension_creates_project_by_clicking_current_flow_page():
    content = Path("extension/content.js").read_text(encoding="utf-8")
    background = Path("extension/background.js").read_text(encoding="utf-8")

    assert "CLICK_NEW_PROJECT" in content
    assert "const deadline = Date.now() + 15000" in content
    assert "新建项目|创建项目|New project|Create project" in content
    assert "msg.method === 'page_create_project'" in background
    assert "const currentTabs = await chrome.tabs.query" in background
    assert "data: { projectId: match[1] }" in background


@pytest.mark.asyncio
async def test_concurrent_credit_checks_share_one_page_read(monkeypatch):
    client = FlowClient()
    calls = 0

    async def fake_send(method, params, timeout=300, request_id=None):
        nonlocal calls
        assert method == "page_credits"
        calls += 1
        await asyncio.sleep(0.01)
        return {"data": {"credits": 580, "creditsSource": "flow_page", "capturedAt": int(time.time() * 1000)}}

    monkeypatch.setattr(client, "_send", fake_send)

    first, second = await asyncio.gather(client.get_credits(), client.get_credits())

    assert first["credits"] == second["credits"] == 580
    assert calls == 1


@pytest.mark.asyncio
async def test_transient_page_read_failure_keeps_last_verified_credits(monkeypatch):
    client = FlowClient()
    client._page_credits = 580
    client._page_credits_at = int(time.time() * 1000) - 61_000

    async def fake_send(method, params, timeout=300, request_id=None):
        assert method == "page_credits"
        return {"status": 503, "error": "PAGE_CREDITS_NOT_FOUND"}

    monkeypatch.setattr(client, "_send", fake_send)

    result = await client.get_credits()

    assert result["credits"] == 580
    assert result["creditsSource"] == "flow_page_cached"
    assert result["capturedAt"] == client._page_credits_at
