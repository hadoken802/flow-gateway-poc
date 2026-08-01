"""CLI example for Flow Gateway multi-reference-image generation."""
from __future__ import annotations

import argparse
import json

from flow_gateway_client import FlowGatewayClient


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8200")
    parser.add_argument("--api-key")
    parser.add_argument("--image", action="append", required=True, help="JPG, PNG, or WebP image. Repeat to keep order.")
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--idempotency-key")
    args = parser.parse_args()

    client = FlowGatewayClient(args.base_url, api_key=args.api_key)
    result = client.generate(
        images=args.image,
        prompt=args.prompt,
        output_path=args.output,
        idempotency_key=args.idempotency_key,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
