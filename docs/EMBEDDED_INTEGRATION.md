# Embedded Integration

Embedded callers should upload images first, then create a task with `input_file_ids`.

## Engine Modes

There are two local engine modes.

### existing_pool

Use this on the user's own machine when the production Flow account pool already exists.

```python
client = FlowGatewayClient(
    base_url="http://127.0.0.1:8200",
    api_key="client-key",
    engine_mode="existing_pool",
    engine_root="D:/Codex/projects/flow_gateway_poc/flowkit",
)
client.ensure_engine_running()
```

Behavior:

- Reuses a running `http://127.0.0.1:8200` Gateway when `/health` is OK.
- If Gateway is not running, starts `python -m gateway.main` from `engine_root`.
- Uses `FLOWKIT_GATEWAY_WORKER_SOURCE=static_json`.
- Uses the existing production `gateway/workers.json`.
- Uses `data/gateway.db` and `outputs` under `engine_root`.
- Does not create or overwrite `runtime/embedded_workers.json`.
- Does not initialize a blank account pool.
- Does not restart Chrome or Worker processes.
- Does not modify `FLOW-001` to `FLOW-008` profiles, cookies, OAuth data, or account state.

This is the mode for embedding FlowKit into a user's existing local tool.

### clean_embedded

Use this for a friend or a new computer that should start from an empty local install.

```python
client = FlowGatewayClient(
    base_url="http://127.0.0.1:8200",
    api_key="client-key",
    engine_mode="clean_embedded",
    engine_root="D:/apps/flowkit",
)
client.ensure_engine_running()
```

Behavior:

- Uses `start_embedded_engine.bat`.
- Uses `runtime/embedded_workers.json`.
- Starts with zero accounts (`[]`) until accounts are added.
- Keeps data isolated from an existing production pool.

## Clean Embedded Install

Run:

```bat
setup_embedded_windows.bat
start_embedded_engine.bat
status_embedded_engine.bat
```

The scripts calculate paths from their own directory, so the project can live in any Windows folder, including paths with spaces. A new embedded install creates:

- `data`
- `profiles`
- `outputs`
- `logs`
- `runtime`
- `runtime\embedded_workers.json`

The initial embedded worker list is empty (`[]`), so it does not read existing `FLOW-001` to `FLOW-008` data. Do not use this mode for a machine that should reuse an existing production account pool.

Configure `.env` from `.env.example`:

```text
FLOW_GATEWAY_CLIENT_API_KEY=change-me-client-key
FLOW_GATEWAY_ADMIN_API_KEY=change-me-admin-key
```

Client tools should send `X-API-Key` with the client key.

## Python SDK

Python:

```python
from examples.flow_gateway_client import FlowGatewayClient

client = FlowGatewayClient("http://127.0.0.1:8200", api_key="change-me-client-key", engine_mode="existing_pool")
client.ensure_engine_running()
client.generate(
    images=[
        "D:/images/img1.jpg",
        "D:/images/img2.jpg",
        "D:/images/img3.jpg",
    ],
    prompt="Use all uploaded images as reference inputs...",
    output_path="D:/videos/result.mp4",
)
```

The Gateway keeps image order in `task_input_media.position`. Worker accounts upload each original file to Flow on their own account before Omni submission. If a task switches accounts, images are uploaded again for the new account because `media_id` cross-account reuse is not assumed.

The current version treats all images as reference images. Start frame, end frame, main image, and detail image semantics belong in the prompt.

SDK methods:

- `ensure_engine_running()`
- `ready()`
- `upload_images()`
- `create_task()`
- `get_task()`
- `wait_for_task()`
- `cancel_task()`
- `download_video()`
- `generate()`
