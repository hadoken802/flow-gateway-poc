"""CLI for local Flow account registration."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict

from .extension_bootstrap import ExtensionBootstrapResult, ExtensionBootstrapper
from .login_verifier import ConfirmLoginService, LoginVerifier
from .process_manager import RuntimeManager
from .registry import AccountRegistry, plan_to_dict


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m runtime.cli")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list")

    show = sub.add_parser("show")
    show.add_argument("account_id")

    for name in ("open-login", "start-one", "status", "stop-one"):
        parser_one = sub.add_parser(name)
        parser_one.add_argument("account_id")

    verify_login = sub.add_parser("verify-login")
    verify_login.add_argument("account_id")

    confirm_login = sub.add_parser("confirm-login")
    confirm_login.add_argument("account_id")

    sub.add_parser("init-extension-template")

    bootstrap = sub.add_parser("bootstrap-extension")
    bootstrap.add_argument("account_id")
    bootstrap.add_argument("--repair", action="store_true")

    bootstrap_batch = sub.add_parser("bootstrap-batch")
    bootstrap_batch.add_argument("account_ids", nargs="+")
    bootstrap_batch.add_argument("--repair", action="store_true")

    import_existing = sub.add_parser("import-existing")
    import_existing.add_argument("--dry-run", action="store_true")

    add_batch = sub.add_parser("add-batch")
    add_batch.add_argument("--start", type=int, required=True)
    add_batch.add_argument("--count", type=int, required=True)
    add_batch.add_argument("--dry-run", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    registry = AccountRegistry()

    if args.command == "list":
        print(json.dumps([asdict(account) for account in registry.list_accounts()], ensure_ascii=False, indent=2))
        return 0

    if args.command == "show":
        account = registry.get(args.account_id)
        if not account:
            print(json.dumps({"account_id": args.account_id, "result": "failed"}, ensure_ascii=False, indent=2))
            return 1
        print(json.dumps(asdict(account), ensure_ascii=False, indent=2))
        return 0

    if args.command in {"open-login", "start-one", "status", "stop-one"}:
        manager = RuntimeManager(registry)
        result = getattr(manager, args.command.replace("-", "_"))(args.account_id)
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        return 0 if result.ok or result.result in {"stopped", "already_stopped", "already_running", "opened"} else 1

    if args.command == "verify-login":
        result = LoginVerifier(registry).verify(args.account_id)
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        return 0 if result.login_verified else 1

    if args.command == "confirm-login":
        result = ConfirmLoginService(registry).confirm(args.account_id)
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        return 0 if result.ok else 1

    if args.command == "init-extension-template":
        manager = RuntimeManager(registry)
        result = _run_bootstrap_command(lambda: ExtensionBootstrapper(registry, runtime=manager).init_template())
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        return 0 if result.ok else 1

    if args.command == "bootstrap-extension":
        manager = RuntimeManager(registry)
        result = _run_bootstrap_command(lambda: ExtensionBootstrapper(registry, runtime=manager).bootstrap_account(args.account_id, repair=args.repair), args.account_id)
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        return 0 if result.ok else 1

    if args.command == "bootstrap-batch":
        manager = RuntimeManager(registry)
        result = _run_bootstrap_command(lambda: ExtensionBootstrapper(registry, runtime=manager).bootstrap_batch(args.account_ids, repair=args.repair))
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        return 0 if result.ok else 1

    if args.command == "import-existing":
        plan = registry.import_existing_workers(dry_run=args.dry_run)
        print(json.dumps(plan_to_dict(plan), ensure_ascii=False, indent=2))
        return 1 if plan.issues and not plan.accounts else 0

    if args.command == "add-batch":
        plan = registry.register_batch(
            args.start,
            args.count,
            dry_run=args.dry_run,
        )
        print(json.dumps(plan_to_dict(plan), ensure_ascii=False, indent=2))
        return 1 if plan.issues else 0

    return 2


def _run_bootstrap_command(action, account_id: str | None = None):
    try:
        return action()
    except Exception as error:
        return ExtensionBootstrapResult(
            "failed",
            account_id,
            False,
            {"stage": "cli_bootstrap", "error": type(error).__name__},
        )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
