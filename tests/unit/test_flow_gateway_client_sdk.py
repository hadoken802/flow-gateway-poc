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
