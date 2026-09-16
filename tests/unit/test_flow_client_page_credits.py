import time
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
