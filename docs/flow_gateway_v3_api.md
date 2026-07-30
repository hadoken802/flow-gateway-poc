# Flow Gateway V3 Local API

Base URL: `http://127.0.0.1:8200`

## Import Tasks

`POST /api/v1/tasks/import`

JSON body can use either `tasks` or `format/content`.

```json
{
  "tasks": [
    {
      "external_task_id": "shot-001",
      "prompt": "A slow cinematic camera move",
      "input_media_path": "D:/media/shot-001.png",
      "output_directory": "outputs",
      "output_filename": "video.mp4",
      "priority": 10,
      "estimated_quota_cost": 15,
      "generation_parameters": {"duration": 10, "aspect_ratio": "9:16"},
      "metadata": {"source": "example"}
    }
  ]
}
```

CSV content:

```csv
external_task_id,prompt,input_media_path,output_directory,output_filename,priority,estimated_quota_cost
shot-001,A slow cinematic camera move,D:/media/shot-001.png,outputs,video.mp4,10,15
```

## Task APIs

- `POST /api/v1/tasks`
- `POST /api/v1/tasks/import`
- `GET /api/v1/tasks`
- `GET /api/v1/tasks/{task_id}`
- `POST /api/v1/tasks/{task_id}/pause`
- `POST /api/v1/tasks/{task_id}/resume`
- `POST /api/v1/tasks/{task_id}/cancel`
- `POST /api/v1/tasks/{task_id}/priority`
- `POST /api/v1/tasks/{task_id}/requeue`
- `POST /api/v1/tasks/{task_id}/retry-download`
- `POST /api/v1/tasks/{task_id}/reconcile`
- `GET /api/v1/batches`
- `GET /api/v1/batches/{batch_id}`
- `GET /api/v1/accounts`
- `GET /api/v1/system/status`

## Python Example

```python
import requests

base = "http://127.0.0.1:8200"
payload = {
    "tasks": [{
        "external_task_id": "shot-001",
        "prompt": "A slow cinematic camera move",
        "input_media_path": "D:/media/shot-001.png",
        "output_directory": "outputs",
        "output_filename": "video.mp4",
        "priority": 10,
        "estimated_quota_cost": 15,
    }]
}

created = requests.post(f"{base}/api/v1/tasks/import", json=payload, timeout=30).json()
print(created)
tasks = requests.get(f"{base}/api/v1/tasks", timeout=30).json()
print(tasks)
```
