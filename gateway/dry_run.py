"""Manual dry-run helper."""
import asyncio

from .config import GatewaySettings
from .scheduler import GatewayScheduler


async def main():
    scheduler = GatewayScheduler(GatewaySettings.from_env())
    await scheduler.start()
    try:
        await scheduler.create_tasks([
            {"idempotency_key": f"manual-dry-run-{i}", "image_path": f"D:/dry-{i}.png", "prompt": f"dry prompt {i}", "duration": 10, "aspect_ratio": "9:16"}
            for i in range(5)
        ])
    finally:
        await scheduler.stop()


if __name__ == "__main__":
    asyncio.run(main())
