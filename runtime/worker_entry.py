"""Runtime-owned Worker entry that validates process ownership markers."""
from __future__ import annotations

import argparse
import os
import runpy
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m runtime.worker_entry")
    parser.add_argument("--runtime-account-id", required=True)
    parser.add_argument("--runtime-api-port", required=True)
    parser.add_argument("--runtime-ws-port", required=True)
    return parser


def _matches_env(name: str, expected: str) -> bool:
    return str(os.environ.get(name, "")).strip() == str(expected).strip()


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

    sys.argv = ["agent.main"]
    runpy.run_module("agent.main", run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
