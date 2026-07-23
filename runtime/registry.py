"""SQLite account registry for local Chrome profile Flow accounts."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from .paths import DATA_ROOT, OUTPUTS_ROOT, PROFILES_ROOT, REGISTRY_DB_PATH, WORKERS_JSON_PATH
from .port_allocator import PortRanges, allocate_port_triplet


SCHEMA = """
CREATE TABLE IF NOT EXISTS flow_account_registry (
    account_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    profile_path TEXT NOT NULL UNIQUE,
    worker_api_port INTEGER NOT NULL UNIQUE,
    extension_ws_port INTEGER NOT NULL UNIQUE,
    chrome_cdp_port INTEGER NOT NULL UNIQUE,
    database_path TEXT NOT NULL UNIQUE,
    output_dir TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'registered',
    created_at TEXT NOT NULL,
    last_started_at TEXT,
    last_stopped_at TEXT,
    chrome_pid INTEGER,
    worker_pid INTEGER,
    last_health_at TEXT,
    last_error TEXT,
    updated_at TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class AccountRecord:
    account_id: str
    display_name: str
    profile_path: str
    worker_api_port: int
    extension_ws_port: int
    chrome_cdp_port: int
    database_path: str
    output_dir: str
    enabled: bool = True
    status: str = "registered"
    created_at: str = ""
    last_started_at: str | None = None


@dataclass(frozen=True)
class RegistrationIssue:
    account_id: str
    reason: str
    path: str = ""
    port: int | None = None


@dataclass(frozen=True)
class RegistrationPlan:
    accounts: list[AccountRecord]
    issues: list[RegistrationIssue]


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_account_id(number: int) -> str:
    return f"FLOW-{int(number):03d}"


def _port_from_url(value: str) -> int:
    parsed = urlparse(value)
    if parsed.port is None:
        raise ValueError(f"missing port in URL: {value}")
    return int(parsed.port)


class AccountRegistry:
    def __init__(
        self,
        db_path: Path | str = REGISTRY_DB_PATH,
        profiles_root: Path | str = PROFILES_ROOT,
        data_root: Path | str = DATA_ROOT,
        outputs_root: Path | str = OUTPUTS_ROOT,
        workers_json_path: Path | str = WORKERS_JSON_PATH,
    ):
        self.db_path = Path(db_path)
        self.profiles_root = Path(profiles_root)
        self.data_root = Path(data_root)
        self.outputs_root = Path(outputs_root)
        self.workers_json_path = Path(workers_json_path)

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA)
        conn.commit()
        return conn

    def list_accounts(self) -> list[AccountRecord]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM flow_account_registry ORDER BY account_id").fetchall()
        return [self._row_to_record(row) for row in rows]

    def existing_ports(self) -> set[int]:
        ports: set[int] = set()
        for account in self.list_accounts():
            ports.update({account.worker_api_port, account.extension_ws_port, account.chrome_cdp_port})
        ports.update(self._ports_from_workers_json())
        return ports

    def import_existing_workers(self, dry_run: bool = False) -> RegistrationPlan:
        workers = self._load_workers_json()
        accounts: list[AccountRecord] = []
        issues: list[RegistrationIssue] = []
        reserved = self.existing_ports()
        for item in workers:
            account_id = str(item.get("account_id") or "").strip()
            if not account_id:
                continue
            existing = self.get(account_id)
            if existing:
                issues.append(RegistrationIssue(account_id, "already_registered"))
                continue
            worker_port = _port_from_url(item["api_url"])
            extension_ws_port = self._known_ws_port(account_id)
            chrome_cdp_port = self._known_chrome_cdp_port(account_id, reserved | {worker_port, extension_ws_port})
            record = AccountRecord(
                account_id=account_id,
                display_name=account_id,
                profile_path=str(self.profiles_root / account_id),
                worker_api_port=worker_port,
                extension_ws_port=extension_ws_port,
                chrome_cdp_port=chrome_cdp_port,
                database_path=str(self.data_root / f"{account_id}.db"),
                output_dir=str(self.outputs_root / account_id),
                enabled=bool(item.get("enabled", True)),
                created_at=utc_now(),
            )
            accounts.append(record)
            reserved.update({worker_port, extension_ws_port, chrome_cdp_port})
        self._append_unregistered_profile_issues({account.account_id for account in accounts}, issues)
        if not dry_run and accounts:
            self.register_many(accounts)
        return RegistrationPlan(accounts, issues)

    def plan_batch(self, start_number: int, count: int, ranges: PortRanges | None = None) -> RegistrationPlan:
        accounts: list[AccountRecord] = []
        issues: list[RegistrationIssue] = []
        reserved = self.existing_ports()
        planned_ids = set()
        for number in range(int(start_number), int(start_number) + int(count)):
            account_id = format_account_id(number)
            planned_ids.add(account_id)
            if self.get(account_id):
                issues.append(RegistrationIssue(account_id, "already_registered"))
                continue
            profile_path = self.profiles_root / account_id
            if profile_path.exists():
                issues.append(RegistrationIssue(account_id, "profile_exists_unregistered", str(profile_path)))
                continue
            try:
                worker_api_port, extension_ws_port, chrome_cdp_port = allocate_port_triplet(reserved, ranges=ranges)
            except RuntimeError:
                issues.append(RegistrationIssue(account_id, "no_available_port"))
                continue
            record = AccountRecord(
                account_id=account_id,
                display_name=account_id,
                profile_path=str(profile_path),
                worker_api_port=worker_api_port,
                extension_ws_port=extension_ws_port,
                chrome_cdp_port=chrome_cdp_port,
                database_path=str(self.data_root / f"{account_id}.db"),
                output_dir=str(self.outputs_root / account_id),
                created_at=utc_now(),
            )
            accounts.append(record)
            reserved.update({worker_api_port, extension_ws_port, chrome_cdp_port})
        return RegistrationPlan(accounts, issues)

    def register_many(self, accounts: list[AccountRecord], create_dirs: bool = False) -> None:
        with self.connect() as conn:
            try:
                conn.execute("BEGIN")
                for account in accounts:
                    self._insert(conn, account)
                    if create_dirs:
                        Path(account.profile_path).mkdir(parents=True, exist_ok=False)
                        Path(account.output_dir).mkdir(parents=True, exist_ok=True)
                        Path(account.database_path).parent.mkdir(parents=True, exist_ok=True)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def register_batch(self, start_number: int, count: int, dry_run: bool = False, create_dirs: bool = False) -> RegistrationPlan:
        plan = self.plan_batch(start_number, count)
        if not dry_run and not plan.issues:
            self.register_many(plan.accounts, create_dirs=create_dirs)
        return plan

    def get(self, account_id: str) -> AccountRecord | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM flow_account_registry WHERE account_id=?", (account_id,)).fetchone()
        return self._row_to_record(row) if row else None

    def _insert(self, conn: sqlite3.Connection, account: AccountRecord) -> None:
        now = utc_now()
        conn.execute(
            """
            INSERT INTO flow_account_registry(
              account_id, display_name, profile_path, worker_api_port, extension_ws_port,
              chrome_cdp_port, database_path, output_dir, enabled, status, created_at,
              last_started_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account.account_id,
                account.display_name,
                account.profile_path,
                account.worker_api_port,
                account.extension_ws_port,
                account.chrome_cdp_port,
                account.database_path,
                account.output_dir,
                int(account.enabled),
                account.status,
                account.created_at or now,
                account.last_started_at,
                now,
            ),
        )

    def _load_workers_json(self) -> list[dict]:
        if not self.workers_json_path.exists():
            return []
        data = json.loads(self.workers_json_path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []

    def _ports_from_workers_json(self) -> set[int]:
        ports: set[int] = set()
        for item in self._load_workers_json():
            api_url = item.get("api_url")
            if api_url:
                ports.add(_port_from_url(api_url))
        return ports

    def _known_ws_port(self, account_id: str) -> int:
        return {"FLOW-001": 9222, "FLOW-002": 9212, "FLOW-003": 9213}.get(account_id, 0)

    def _known_chrome_cdp_port(self, account_id: str, reserved: set[int]) -> int:
        number = int(account_id.rsplit("-", 1)[-1]) if "-" in account_id else 0
        candidate = 9300 + number
        if candidate in reserved:
            return allocate_port_triplet(reserved, ranges=PortRanges(chrome_cdp=range(9300, 9400)))[2]
        return candidate

    def _append_unregistered_profile_issues(self, planned_account_ids: set[str], issues: list[RegistrationIssue]) -> None:
        if not self.profiles_root.exists():
            return
        registered = {account.account_id for account in self.list_accounts()} | planned_account_ids
        for child in sorted(self.profiles_root.iterdir()):
            if child.is_dir() and child.name.startswith("FLOW-") and child.name not in registered:
                issues.append(RegistrationIssue(child.name, "profile_exists_unregistered", str(child)))

    def _row_to_record(self, row: sqlite3.Row) -> AccountRecord:
        return AccountRecord(
            account_id=row["account_id"],
            display_name=row["display_name"],
            profile_path=row["profile_path"],
            worker_api_port=int(row["worker_api_port"]),
            extension_ws_port=int(row["extension_ws_port"]),
            chrome_cdp_port=int(row["chrome_cdp_port"]),
            database_path=row["database_path"],
            output_dir=row["output_dir"],
            enabled=bool(row["enabled"]),
            status=row["status"],
            created_at=row["created_at"],
            last_started_at=row["last_started_at"],
        )


def plan_to_dict(plan: RegistrationPlan) -> dict:
    return {
        "accounts": [asdict(account) for account in plan.accounts],
        "issues": [asdict(issue) for issue in plan.issues],
    }

