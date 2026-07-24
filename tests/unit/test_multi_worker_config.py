import importlib
import json
from pathlib import Path

import pytest


def reload_config(monkeypatch, **env):
    for key in (
        "FLOW_ACCOUNT_ID",
        "AGENT_API_HOST",
        "AGENT_API_PORT",
        "EXTENSION_WS_HOST",
        "EXTENSION_WS_PORT",
        "FLOW_DB_PATH",
        "OUTPUT_DIR",
        "FLOW_RUNTIME_INSTANCE_ID",
        "FLOW_RUNTIME_OWNERSHIP_SECRET",
        "FLOW_RUNTIME_OWNERSHIP_VERSION",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))
    import agent.config as config
    return importlib.reload(config)


def local_tmp(name):
    path = Path(".tmp") / "tests" / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_default_config_keeps_single_account_ports(monkeypatch):
    config = reload_config(monkeypatch)
    assert config.FLOW_ACCOUNT_ID == "FLOW-001"
    assert config.API_HOST == "127.0.0.1"
    assert config.API_PORT == 8100
    assert config.WS_HOST == "127.0.0.1"
    assert config.WS_PORT == 9222


def test_env_overrides_ports(monkeypatch):
    config = reload_config(monkeypatch, AGENT_API_PORT="8112", EXTENSION_WS_PORT="9212")
    assert config.API_PORT == 8112
    assert config.WS_PORT == 9212


def test_invalid_port_has_clear_error(monkeypatch):
    with pytest.raises(ValueError, match="AGENT_API_PORT must be a valid TCP port"):
        reload_config(monkeypatch, AGENT_API_PORT="bad")


def test_worker_database_paths_are_isolated(monkeypatch):
    base = local_tmp("db")
    flow1 = reload_config(monkeypatch, FLOW_DB_PATH=base / "FLOW-001.db")
    path1 = flow1.DB_PATH
    flow2 = reload_config(monkeypatch, FLOW_DB_PATH=base / "FLOW-002.db")
    assert path1 != flow2.DB_PATH


def test_worker_output_dirs_are_isolated(monkeypatch):
    base = local_tmp("outputs")
    flow1 = reload_config(monkeypatch, OUTPUT_DIR=base / "FLOW-001")
    path1 = flow1.OUTPUT_DIR
    flow2 = reload_config(monkeypatch, OUTPUT_DIR=base / "FLOW-002")
    assert path1 != flow2.OUTPUT_DIR


def test_omni_uses_configured_output_dir(monkeypatch):
    config = reload_config(monkeypatch, OUTPUT_DIR=local_tmp("outputs") / "FLOW-002")
    import agent.api.omni_test as omni_test
    importlib.reload(omni_test)
    assert omni_test.OUTPUT_DIR == config.OUTPUT_DIR
    assert "FLOW-002" in str(omni_test.OUTPUT_DIR)


@pytest.mark.asyncio
async def test_health_returns_worker_identity(monkeypatch):
    config = reload_config(
        monkeypatch,
        FLOW_ACCOUNT_ID="FLOW-002",
        AGENT_API_PORT="8112",
        EXTENSION_WS_PORT="9212",
    )
    import agent.main as main
    importlib.reload(main)
    response = await main.health()
    assert response["account_id"] == "FLOW-002"
    assert response["api_port"] == 8112
    assert response["ws_port"] == 9212
    assert response["runtime_ownership_capable"] is False
    assert "FLOW_RUNTIME_OWNERSHIP_SECRET" not in response


@pytest.mark.asyncio
async def test_health_and_challenge_return_safe_runtime_ownership_fields(monkeypatch):
    config = reload_config(
        monkeypatch,
        FLOW_ACCOUNT_ID="FLOW-002",
        AGENT_API_PORT="8112",
        EXTENSION_WS_PORT="9212",
        FLOW_RUNTIME_INSTANCE_ID="11111111-1111-4111-8111-111111111111",
        FLOW_RUNTIME_OWNERSHIP_SECRET="test-secret",
        FLOW_RUNTIME_OWNERSHIP_VERSION="1",
    )
    import agent.main as main
    from runtime.ownership import generate_challenge, sign_challenge

    importlib.reload(main)
    health = await main.health()
    assert health["runtime_instance_id"] == "11111111-1111-4111-8111-111111111111"
    assert health["runtime_ownership_version"] == 1
    assert health["runtime_ownership_capable"] is True
    assert "test-secret" not in json.dumps(health)

    class FakeRequest:
        def __init__(self, payload):
            self.payload = payload

        async def body(self):
            return json.dumps(self.payload).encode("utf-8")

    challenge = generate_challenge()
    response = await main.runtime_ownership_challenge(FakeRequest({
        "account_id": "FLOW-002",
        "runtime_instance_id": "11111111-1111-4111-8111-111111111111",
        "challenge": challenge,
        "proof_version": 1,
    }))
    assert response["ok"] is True
    assert response["challenge_response"] == sign_challenge("test-secret", "FLOW-002", "11111111-1111-4111-8111-111111111111", challenge)
    assert "test-secret" not in json.dumps(response)


@pytest.mark.asyncio
async def test_challenge_rejects_account_runtime_and_invalid_challenge(monkeypatch):
    reload_config(
        monkeypatch,
        FLOW_ACCOUNT_ID="FLOW-002",
        FLOW_RUNTIME_INSTANCE_ID="11111111-1111-4111-8111-111111111111",
        FLOW_RUNTIME_OWNERSHIP_SECRET="test-secret",
        FLOW_RUNTIME_OWNERSHIP_VERSION="1",
    )
    import agent.main as main

    importlib.reload(main)

    class FakeRequest:
        def __init__(self, payload):
            self.payload = payload

        async def body(self):
            return json.dumps(self.payload).encode("utf-8")

    assert (await main.runtime_ownership_challenge(FakeRequest({
        "account_id": "FLOW-999",
        "runtime_instance_id": "11111111-1111-4111-8111-111111111111",
        "challenge": "A" * 43,
        "proof_version": 1,
    })))["reason"] == "account_mismatch"
    assert (await main.runtime_ownership_challenge(FakeRequest({
        "account_id": "FLOW-002",
        "runtime_instance_id": "22222222-2222-4222-8222-222222222222",
        "challenge": "A" * 43,
        "proof_version": 1,
    })))["reason"] == "runtime_instance_mismatch"
    assert (await main.runtime_ownership_challenge(FakeRequest({
        "account_id": "FLOW-002",
        "runtime_instance_id": "11111111-1111-4111-8111-111111111111",
        "challenge": "bad",
        "proof_version": 1,
    })))["reason"] == "invalid_challenge"


def test_extension_options_files_are_declared():
    manifest = json.loads(open("extension/manifest.json", encoding="utf-8").read())
    assert manifest["options_page"] == "options.html"
    background = open("extension/background.js", encoding="utf-8").read()
    assert "chrome.storage.local.get" in background
    assert "ws_url" in background
    assert "account_id" in background
