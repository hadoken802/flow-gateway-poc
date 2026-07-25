import asyncio
from pathlib import Path

import pytest


def test_extension_http_callback_does_not_use_ws_port():
    text = Path("extension/background.js").read_text(encoding="utf-8")
    assert "mappedPorts" not in text
    assert "fetch(wsUrl" not in text
    assert "fetch(agentHttpUrl + '/api/ext/callback'" in text
    assert "url.port === '9213' ? '8113'" not in text
    assert "new URL(apiUrl)" in text
    assert "url.hostname !== '127.0.0.1'" in text
    assert "http://127.0.0.1:${port}" in text
    assert "fallbackToWebSocket(msg)" in text
    assert "data?.ok !== true" in text


def test_extension_http_callback_targets_flow_024_api_url():
    text = Path("extension/background.js").read_text(encoding="utf-8")
    assert "accountId] ||" not in text
    assert "apiUrl" in text
    assert "8121" not in text


def test_extension_reconnect_uses_single_timer_and_single_socket():
    text = Path("extension/background.js").read_text(encoding="utf-8")
    assert "let reconnectTimer = null" in text
    assert "if (reconnectTimer) return" in text
    assert "clearReconnectTimer()" in text
    assert "let activeSocketId = 0" in text


def test_agent_websocket_uses_explicit_large_message_limits():
    config = Path("agent/config.py").read_text(encoding="utf-8")
    main = Path("agent/main.py").read_text(encoding="utf-8")

    assert "EXTENSION_WS_MAX_SIZE_BYTES" in config
    assert "16 * 1024 * 1024" in config
    assert "max_size=EXTENSION_WS_MAX_SIZE_BYTES" in main
    assert "max_queue=4" in main
    assert "ws_max_size_bytes" in main


def test_extension_uses_offscreen_executor_for_api_requests():
    manifest = Path("extension/manifest.json").read_text(encoding="utf-8")
    background = Path("extension/background.js").read_text(encoding="utf-8")
    offscreen = Path("extension/offscreen.js").read_text(encoding="utf-8")

    assert '"offscreen"' in manifest
    assert '"offscreen.html"' in manifest
    assert "ensureOffscreenDocument" in background
    assert "chrome.runtime.sendMessage({ type: 'OFFSCREEN_API_REQUEST'" in background
    assert "async function handleApiRequest" in offscreen
    assert "fetch(url" in offscreen


def test_extension_keepalive_is_single_js_timer_and_uses_ack():
    text = Path("extension/background.js").read_text(encoding="utf-8")

    assert "const KEEPALIVE_INTERVAL_MS = 20000" in text
    assert "let keepaliveTimer = null" in text
    assert "if (keepaliveTimer) return" in text
    assert "clearKeepaliveTimer()" in text
    assert "type: 'keepalive'" in text
    assert "keepalive_ack" in text


def test_extension_records_close_code_reason_and_message_too_large():
    text = Path("extension/background.js").read_text(encoding="utf-8")

    assert "event.code" in text
    assert "event.reason" in text
    assert "websocket_message_too_large" in text
    assert "1009" in text


def test_gateway_worker_client_uses_api_url_only():
    text = Path("gateway/worker_client.py").read_text(encoding="utf-8")
    assert "worker.api_url" in text
    assert "ws_url" not in text
    assert "9213" not in text


def test_flow_003_start_script_uses_expected_ports_and_venv_python():
    text = Path("start_worker_FLOW-003.bat").read_text(encoding="utf-8")
    assert "FLOW_ACCOUNT_ID=FLOW-003" in text
    assert "AGENT_API_PORT=8113" in text
    assert "EXTENSION_WS_PORT=9213" in text
    assert r"D:\Codex\projects\flow_gateway_poc\.venv\Scripts\python.exe" in text
    assert "if %errorlevel%==3 exit /b 0" in text


def test_worker_start_scripts_normalize_ctrl_c_exit_code():
    for script in ("start_worker_FLOW-001.bat", "start_worker_FLOW-002.bat", "start_worker_FLOW-003.bat"):
        text = Path(script).read_text(encoding="utf-8")
        assert "if %errorlevel%==3 exit /b 0" in text


@pytest.mark.asyncio
async def test_omni_shutdown_cancels_background_tasks_before_db_close():
    import agent.api.omni_test as omni_test

    touched_after_shutdown = False

    async def sleeper():
        nonlocal touched_after_shutdown
        try:
            await asyncio.sleep(30)
            touched_after_shutdown = True
        except asyncio.CancelledError:
            raise

    omni_test._shutdown_event.clear()
    task = omni_test._track_omni_task("job-shutdown", sleeper())
    assert task is not None
    await omni_test.shutdown_omni_jobs()
    assert task.cancelled()
    assert touched_after_shutdown is False
    omni_test._shutdown_event.clear()


def test_register_does_not_mark_extension_connected_until_ready():
    from agent.services.flow_client import FlowClient

    client = FlowClient()
    client.set_extension(object(), registered=False)
    assert client.connected is False
    client.mark_extension_ready()
    assert client.connected is True
