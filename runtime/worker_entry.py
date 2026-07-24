"""Runtime-owned Worker entry that validates process ownership markers."""
from __future__ import annotations

import argparse
import os
import runpy
import sys
import uuid


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m runtime.worker_entry")
    parser.add_argument("--runtime-account-id", required=True)
    parser.add_argument("--runtime-api-port", required=True)
    parser.add_argument("--runtime-ws-port", required=True)
    return parser


def _matches_env(name: str, expected: str) -> bool:
    return str(os.environ.get(name, "")).strip() == str(expected).strip()


def _valid_runtime_identity() -> bool:
    instance_id = os.environ.get("FLOW_RUNTIME_INSTANCE_ID", "").strip()
    secret = os.environ.get("FLOW_RUNTIME_OWNERSHIP_SECRET", "").strip()
    version = os.environ.get("FLOW_RUNTIME_OWNERSHIP_VERSION", "").strip()
    if not instance_id and not secret and not version:
        return True
    try:
        uuid.UUID(instance_id)
    except ValueError:
        return False
    return bool(secret) and version == "1"


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    checks = [
        ("FLOW_ACCOUNT_ID", args.runtime_account_id),
        ("AGENT_API_PORT", args.runtime_api_port),
        ("EXTENSION_WS_PORT", args.runtime_ws_port),
    ]
    for env_name, expected in checks:
        if not _matches_env(env_name, expected):
            print(f"{env_name} mismatch", file=sys.stderr)
            return 2
    if not _valid_runtime_identity():
        print("FLOW_RUNTIME_OWNERSHIP identity invalid", file=sys.stderr)
        return 2

    sys.argv = ["agent.main"]
    runpy.run_module("agent.main", run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
