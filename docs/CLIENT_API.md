# Client API

## Upload Files

`POST /api/v1/client/files/batch`

Multipart fields:

- `files`: repeat for each image
- or `file`: single image

Supports JPG, PNG, and WebP. Each item is validated independently and returned in request order.

Response:

```json
{
  "files": [
    {"file_id": "file_...", "original_filename": "img1.jpg", "mime_type": "image/jpeg", "size_bytes": 123, "sha256": "..."}
  ]
}
```

Failed items include `ok: false` and `error`. Server absolute paths are not returned.

## Create Task

`POST /api/v1/tasks`

```json
{
  "external_task_id": "shot-001",
  "idempotency_key": "client-key",
  "input_file_ids": ["file_a", "file_b", "file_c"],
  "prompt": "Use all uploaded images as ordered reference inputs.",
  "duration": 10,
  "aspect_ratio": "9:16",
  "estimated_quota_cost": 15,
  "priority": 10,
  "output_filename": "result.mp4"
}
```

`input_file_ids` must contain at least one file. Order is meaningful and preserved.

重复 `idempotency_key` 会返回原 `task_id`，不会重复提交。
