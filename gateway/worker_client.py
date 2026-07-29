"""Read-only Worker API client for Gateway."""
import httpx


class WorkerSubmitError(RuntimeError):
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
