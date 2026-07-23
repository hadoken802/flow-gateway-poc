"""CLI for local Flow account registration."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict

from .registry import AccountRegistry, plan_to_dict


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m runtime.cli")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list")

    import_existing = sub.add_parser("import-existing")
    import_existing.add_argument("--dry-run", action="store_true")

    add_batch = sub.add_parser("add-batch")
    add_batch.add_argument("--start", type=int, required=True)
    add_batch.add_argument("--count", type=int, required=True)
    add_batch.add_argument("--dry-run", action="store_true")
    add_batch.add_argument("--create-dirs", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    registry = AccountRegistry()

    if args.command == "list":
        print(json.dumps([asdict(account) for account in registry.list_accounts()], ensure_ascii=False, indent=2))
        return 0

    if args.command == "import-existing":
        plan = registry.import_existing_workers(dry_run=args.dry_run)
        print(json.dumps(plan_to_dict(plan), ensure_ascii=False, indent=2))
        return 1 if plan.issues and not plan.accounts else 0

    if args.command == "add-batch":
        plan = registry.register_batch(
            args.start,
            args.count,
            dry_run=args.dry_run,
            create_dirs=args.create_dirs,
        )
        print(json.dumps(plan_to_dict(plan), ensure_ascii=False, indent=2))
        return 1 if plan.issues else 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

