from pathlib import Path


def test_sdk_fake_generate_flow(monkeypatch, tmp_path):
    from examples.flow_gateway_client import FlowGatewayClient

    image = tmp_path / "input.png"
    output = tmp_path / "result.mp4"
    image.write_bytes(b"\x89PNG\r\n\x1a\n" + b"x" * 32)
    mp4 = b"\x00\x00\x00\x18ftypmp42" + b"x" * 256
    calls = []

    class FakeResponse:
        def __init__(self, status_code=200, payload=None, content=b""):
            self.status_code = status_code
            self._payload = payload or {}
            self.content = content

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(self.status_code)

    def fake_post(url, **kwargs):
        calls.append(("POST", url, kwargs))
        if url.endswith("/files/batch"):
            return FakeResponse(payload={"files": [{"file_id": "file-a", "sha256": "sha-a", "original_filename": "input.png"}]})
        if url.endswith("/tasks"):
            return FakeResponse(payload={"task_id": "task-a", "status": "queued"})
        raise AssertionError(url)

    def fake_get(url, **kwargs):
        calls.append(("GET", url, kwargs))
        if url.endswith("/tasks/task-a"):
            return FakeResponse(payload={"task_id": "task-a", "status": "completed"})
        if url.endswith("/tasks/task-a/download"):
            return FakeResponse(content=mp4)
        if url.endswith("/system/ready"):
            return FakeResponse(payload={"ready": True})
        raise AssertionError(url)

    monkeypatch.setattr("examples.flow_gateway_client.httpx.post", fake_post)
    monkeypatch.setattr("examples.flow_gateway_client.httpx.get", fake_get)

    client = FlowGatewayClient("http://gateway.local", api_key="client-key")
    result = client.generate(images=[str(image)], prompt="p", output_path=str(output), poll_seconds=0)

    assert result["status"] == "completed"
    assert output.read_bytes() == mp4
    assert all(call[2]["headers"] == {"X-API-Key": "client-key"} for call in calls)


def test_existing_pool_starts_existing_workers_json_without_embedded_workers(monkeypatch, tmp_path):
    from examples.flow_gateway_client import FlowGatewayClient

    root = tmp_path / "flowkit root"
    (root / "logs").mkdir(parents=True)
    (root / "data").mkdir()
    calls = []
    ready_calls = {"count": 0}

    def fake_get(url, **kwargs):
        ready_calls["count"] += 1
        if ready_calls["count"] == 1:
            return _FakeResponse(status_code=503)
        return _FakeResponse(payload={"ready": True, "eligible_accounts": 7})

    class FakePopen:
        def __init__(self, args, **kwargs):
            calls.append((args, kwargs))

    monkeypatch.setattr("examples.flow_gateway_client.httpx.get", fake_get)
    monkeypatch.setattr("examples.flow_gateway_client.subprocess.Popen", FakePopen)

    client = FlowGatewayClient(engine_root=str(root), engine_mode="existing_pool")
    result = client.ensure_engine_running(timeout_seconds=1)

    assert result["eligible_accounts"] == 7
    assert calls[0][0][-2:] == ["-m", "gateway.main"]
    assert calls[0][1]["cwd"] == str(root.resolve())
    assert calls[0][1]["env"]["FLOWKIT_GATEWAY_WORKER_SOURCE"] == "static_json"
    assert calls[0][1]["env"]["GATEWAY_WORKERS_PATH"] == str(root.resolve() / "gateway" / "workers.json")
    assert calls[0][1]["env"]["GATEWAY_DB_PATH"] == str(root.resolve() / "data" / "gateway.db")
    assert not (root / "runtime" / "embedded_workers.json").exists()


def test_clean_embedded_starts_embedded_script(monkeypatch, tmp_path):
    from examples.flow_gateway_client import FlowGatewayClient

    calls = []
    ready_calls = {"count": 0}

    def fake_get(url, **kwargs):
        ready_calls["count"] += 1
        if ready_calls["count"] == 1:
            return _FakeResponse(status_code=503)
        return _FakeResponse(payload={"ready": True, "eligible_accounts": 0})

    class FakePopen:
        def __init__(self, args, **kwargs):
            calls.append((args, kwargs))

    monkeypatch.setattr("examples.flow_gateway_client.httpx.get", fake_get)
    monkeypatch.setattr("examples.flow_gateway_client.subprocess.Popen", FakePopen)

    client = FlowGatewayClient(engine_root=str(tmp_path), engine_mode="clean_embedded")
    result = client.ensure_engine_running(timeout_seconds=1)

    assert result["eligible_accounts"] == 0
    assert calls[0][0] == ["cmd", "/c", str(tmp_path / "start_embedded_engine.bat")]


def test_existing_pool_does_not_start_second_gateway_when_health_is_ok(monkeypatch, tmp_path):
    from examples.flow_gateway_client import FlowGatewayClient

    calls = []

    def fake_get(url, **kwargs):
        if url.endswith("/health"):
            return _FakeResponse(payload={"status": "ok"})
        return _FakeResponse(status_code=404, payload={"detail": "not found"})

    class FakePopen:
        def __init__(self, args, **kwargs):
            calls.append((args, kwargs))

    monkeypatch.setattr("examples.flow_gateway_client.httpx.get", fake_get)
    monkeypatch.setattr("examples.flow_gateway_client.subprocess.Popen", FakePopen)

    result = FlowGatewayClient(engine_root=str(tmp_path), engine_mode="existing_pool").ensure_engine_running(timeout_seconds=1)

    assert result["engine_running"] is True
    assert result["started"] is False
    assert calls == []


def test_sdk_reads_only_client_key_from_env_file(tmp_path):
    from examples.flow_gateway_client import FlowGatewayClient

    (tmp_path / ".env").write_text(
        "FLOW_GATEWAY_CLIENT_API_KEY=client-secret\nFLOW_GATEWAY_ADMIN_API_KEY=admin-secret\n",
        encoding="utf-8",
    )
    client = FlowGatewayClient(engine_root=str(tmp_path))
    assert client.headers == {"X-API-Key": "client-secret"}


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, content=b""):
        self.status_code = status_code
        self._payload = payload or {}
        self.content = content

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)
