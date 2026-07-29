"""Read-only Worker API client for Gateway."""
import hashlib
import httpx


class WorkerSubmitError(RuntimeError):
    def __init__(self, status_code: int, message: str, response: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response or {}


class WorkerRemoteMediaFetchError(RuntimeError):
    def __init__(self, status_code: int, message: str, response: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response or {}


class WorkerClient:
    def __init__(self, submit_timeout_seconds: float = 300.0):
        if submit_timeout_seconds <= 0:
            raise ValueError("submit_timeout_seconds must be positive")
        self.submit_timeout_seconds = submit_timeout_seconds

    async def inspect(self, worker):
        async with httpx.AsyncClient(timeout=5.0) as client:
            health = (await client.get(f"{worker.api_url}/health")).json()
            info = (await client.get(f"{worker.api_url}/api/worker/info")).json()
            flow_status = (await client.get(f"{worker.api_url}/api/flow/status")).json()
            credits = (await client.get(f"{worker.api_url}/api/flow/credits")).json()
        return {
            "status": health.get("status", "error"),
            "account_id": info.get("account_id"),
            "extension_connected": bool(flow_status.get("connected")),
            "flow_key_present": bool(flow_status.get("flow_key_present")),
            "credits": int(credits.get("credits", credits.get("remainingCredits", 0)) or 0),
        }

    async def submit_omni_video(self, *_args, **_kwargs):
        worker, payload = _args[0], _args[1]
        async with httpx.AsyncClient(timeout=self.submit_timeout_seconds) as client:
            response = await client.post(f"{worker.api_url}/api/test/omni-video", json=payload)
        try:
            data = response.json()
        except ValueError:
            data = None
        if response.status_code >= 400:
            if isinstance(data, dict) and data.get("job_id"):
                return data
            raise WorkerSubmitError(response.status_code, f"Worker submit failed: HTTP {response.status_code}", data if isinstance(data, dict) else None)
        return data

    async def resume_omni_video(self, worker, payload):
        async with httpx.AsyncClient(timeout=self.submit_timeout_seconds) as client:
            response = await client.post(f"{worker.api_url}/api/test/omni-video/resume-submit", json=payload)
        try:
            data = response.json()
        except ValueError:
            data = None
        if response.status_code >= 400:
            if isinstance(data, dict) and data.get("job_id"):
                return data
            raise WorkerSubmitError(response.status_code, f"Worker resume submit failed: HTTP {response.status_code}", data if isinstance(data, dict) else None)
        return data

    async def create_project(self, worker, payload):
        async with httpx.AsyncClient(timeout=120.0) as client:
            return (await client.post(f"{worker.api_url}/api/projects", json=payload)).raise_for_status().json()

    async def get_omni_video(self, worker, worker_job_id):
        async with httpx.AsyncClient(timeout=10.0) as client:
            return (await client.get(f"{worker.api_url}/api/test/omni-video/{worker_job_id}")).raise_for_status().json()

    async def retry_omni_video_download(self, worker, worker_job_id):
        async with httpx.AsyncClient(timeout=120.0) as client:
            return (await client.post(f"{worker.api_url}/api/test/omni-video/{worker_job_id}/retry-download")).raise_for_status().json()

    async def query_omni_video_remote_status_once(self, worker, worker_job_id, project_id, output_media_id):
        payload = {"project_id": project_id, "output_media_id": output_media_id}
        async with httpx.AsyncClient(timeout=60.0) as client:
            return (await client.post(f"{worker.api_url}/api/test/omni-video/{worker_job_id}/query-remote-status-once", json=payload)).raise_for_status().json()

    async def query_omni_video_download_capability_once(self, worker, worker_job_id, project_id, output_media_id):
        payload = {"project_id": project_id, "output_media_id": output_media_id}
        async with httpx.AsyncClient(timeout=60.0) as client:
            return (await client.post(f"{worker.api_url}/api/test/omni-video/{worker_job_id}/query-download-capability-once", json=payload)).raise_for_status().json()

    async def fetch_reconciled_encoded_video_once(self, worker, worker_job_id, payload):
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{worker.api_url}/api/test/omni-video/{worker_job_id}/fetch-reconciled-encoded-video-once",
                json=payload,
            )
        if response.status_code >= 400:
            data = _safe_worker_error(response)
            raise WorkerRemoteMediaFetchError(response.status_code, f"Worker encoded video fetch failed: HTTP {response.status_code}", data)
        return {"content": response.content, "headers": dict(response.headers), "status_code": response.status_code}

    async def list_manual_flow_results(self, worker, project_id, after=None, exclude_media_ids=None):
        params = {}
        if after:
            params["after"] = after
        if exclude_media_ids:
            excluded = sorted({str(item) for item in exclude_media_ids if item})
            if excluded:
                params["exclude_media_ids"] = ",".join(excluded)
        async with httpx.AsyncClient(timeout=30.0) as client:
            return (await client.get(f"{worker.api_url}/api/test/omni-video/manual-flow-results/{project_id}", params=params)).raise_for_status().json()

    async def download_manual_flow_result(self, worker, media_id):
        async with httpx.AsyncClient(timeout=120.0) as client:
            return (await client.get(f"{worker.api_url}/api/test/manual-flow-results/media/{media_id}/download")).raise_for_status().json()


def _safe_worker_error(response) -> dict:
    body = response.content or b""
    limit = 64 * 1024
    truncated = len(body) > limit
    sample = body[:limit]
    parsed = None
    try:
        parsed = response.json()
    except ValueError:
        parsed = None
    safe = _sanitize_worker_error(parsed) if isinstance(parsed, dict) else {
        "error_code": "worker_non_json_error",
        "error_class": "worker_http_error",
        "error_message_sanitized": sample.decode("utf-8", errors="replace")[:500],
    }
    if isinstance(safe.get("detail"), dict):
        nested = safe.pop("detail")
        for key, value in nested.items():
            safe.setdefault(key, value)
    elif isinstance(safe.get("detail"), str):
        safe.setdefault("error_message_sanitized", safe.pop("detail")[:500])
    safe.update({
        "worker_http_status": response.status_code,
        "worker_response_body_length": len(body),
        "worker_response_body_sha256": hashlib.sha256(body).hexdigest(),
        "worker_response_truncated": truncated,
    })
    return safe


def _sanitize_worker_error(value):
    if isinstance(value, dict):
        clean = {}
        for key, item in value.items():
            lower = str(key).lower()
            normalized = lower.replace("_", "").replace("-", "")
            if any(token in normalized for token in ("authorization", "cookie", "token", "recaptcha", "encodedvideo", "base64", "prompt")):
                if lower in {"encoded_video_length", "encoded_video_sha256", "expected_encoded_video_length", "expected_encoded_video_sha256", "actual_encoded_video_length", "actual_encoded_video_sha256"}:
                    clean[key] = item
                else:
                    clean[key] = "redacted"
            elif isinstance(item, str) and item.startswith(("http://", "https://")):
                clean[key] = _safe_url(item)
            else:
                clean[key] = _sanitize_worker_error(item)
        return clean
    if isinstance(value, list):
        return [_sanitize_worker_error(item) for item in value[:50]]
    if isinstance(value, str):
        if len(value) > 1000:
            return {"type": "str", "length": len(value), "sha256": hashlib.sha256(value.encode("utf-8")).hexdigest()}
        return value
    return value


def _safe_url(url: str) -> dict:
    from urllib.parse import parse_qsl, urlparse

    parsed = urlparse(url)
    return {
        "scheme": parsed.scheme,
        "host": parsed.netloc,
        "path": parsed.path,
        "query_parameter_names": sorted({key for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}),
    }
