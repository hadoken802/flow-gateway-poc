# Embedded Integration

Embedded callers should upload images first, then create a task with `input_file_ids`.

Python:

```python
from examples.flow_gateway_client import FlowGatewayClient

client = FlowGatewayClient("http://127.0.0.1:8200")
client.generate_reference_images(
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
