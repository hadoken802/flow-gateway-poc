import importlib
import json

import pytest
import websockets


def reload_for_account(monkeypatch, account_id):
    monkeypatch.setenv("FLOW_ACCOUNT_ID", account_id)
    import agent.config as config
    import agent.services.flow_client as flow_client
    import agent.main as main
    importlib.reload(config)
    importlib.reload(flow_client)
    importlib.reload(main)
    return flow_client, main


@pytest.mark.asyncio
async def test_extension_account_match_registers(monkeypatch):
    flow_client, main = reload_for_account(monkeypatch, "FLOW-002")
    client = flow_client.FlowClient()
    ok = await main.register_extension(client, {"type": "register", "account_id": "FLOW-002", "profile_id": "FLOW-002"}, object())
    assert ok is True
    assert client.connected is False
    await client.handle_message({"type": "extension_ready", "flowKeyPresent": False})
    assert client.connected is True


@pytest.mark.asyncio
async def test_extension_account_mismatch_fails(monkeypatch):
    flow_client, main = reload_for_account(monkeypatch, "FLOW-002")
    client = flow_client.FlowClient()
    ok = await main.register_extension(client, {"type": "register", "account_id": "FLOW-001", "profile_id": "FLOW-001"})
    assert ok is False
    assert client.connected is False


@pytest.mark.asyncio
async def test_unregistered_extension_cannot_execute_api_request(monkeypatch):
    flow_client, _main = reload_for_account(monkeypatch, "FLOW-002")

    class FakeWebSocket:
        async def send(self, _payload):
            raise AssertionError("unregistered websocket must not receive api_request")

    client = flow_client.FlowClient()
    client.set_extension(FakeWebSocket(), registered=False)
    result = await client._send("api_request", {"url": "https://aisandbox-pa.googleapis.com/v1/credits"})
    assert result["error"] == "Extension not registered"


@pytest.mark.asyncio
async def test_extension_websocket_accepts_large_media_json_under_limit():
    received = []

    async def handler(websocket):
        async for raw in websocket:
            received.append(len(raw))
            await websocket.send("ok")

    async with websockets.serve(handler, "127.0.0.1", 0, max_size=16 * 1024 * 1024, max_queue=4) as server:
        port = server.sockets[0].getsockname()[1]
        payload = json.dumps({"video": {"encodedVideo": "A" * (3300 * 1024)}})
        async with websockets.connect(f"ws://127.0.0.1:{port}", max_size=16 * 1024 * 1024) as ws:
            await ws.send(payload)
            assert await ws.recv() == "ok"

    assert received == [len(payload)]


@pytest.mark.asyncio
async def test_extension_websocket_rejects_message_over_limit():
    async def handler(websocket):
        async for _raw in websocket:
            pass

    async with websockets.serve(handler, "127.0.0.1", 0, max_size=1024, max_queue=4) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://127.0.0.1:{port}", max_size=2048) as ws:
            await ws.send("A" * 2048)
            with pytest.raises(websockets.ConnectionClosedError) as exc:
                await ws.recv()

    assert exc.value.code == 1009
