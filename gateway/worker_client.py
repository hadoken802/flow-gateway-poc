"""Read-only Worker API client for Gateway."""
import httpx


class WorkerClient:
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
        async with httpx.AsyncClient(timeout=30.0) as client:
            return (await client.post(f"{worker.api_url}/api/test/omni-video", json=payload)).raise_for_status().json()

    async def get_omni_video(self, worker, worker_job_id):
        async with httpx.AsyncClient(timeout=10.0) as client:
            return (await client.get(f"{worker.api_url}/api/test/omni-video/{worker_job_id}")).raise_for_status().json()

    async def retry_omni_video_download(self, worker, worker_job_id):
        async with httpx.AsyncClient(timeout=120.0) as client:
            return (await client.post(f"{worker.api_url}/api/test/omni-video/{worker_job_id}/retry-download")).raise_for_status().json()
