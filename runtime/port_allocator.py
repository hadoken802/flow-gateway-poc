"""Port allocation for local Flow account runtimes."""
from __future__ import annotations

import os
import platform
import re
import socket
import subprocess
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable


@dataclass(frozen=True)
class PortRanges:
    worker_api: range = range(8100, 8200)
    extension_ws: range = range(9200, 9300)
    chrome_cdp: range = range(9300, 9400)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return int(default)


def worker_port_base() -> int:
    return _env_int("FLOW_WORKER_PORT_BASE", 8100)


def worker_fallback_range() -> range:
    start = _env_int("FLOW_WORKER_FALLBACK_PORT_START", 18100)
    end = _env_int("FLOW_WORKER_FALLBACK_PORT_END", 18999)
    if end < start:
        raise ValueError("FLOW_WORKER_FALLBACK_PORT_END must be >= FLOW_WORKER_FALLBACK_PORT_START")
    return range(start, end + 1)


def port_is_listening(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex((host, int(port))) == 0


def port_can_bind(port: int, host: str = "127.0.0.1") -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, int(port)))
            return True
    except OSError:
        return False


@lru_cache(maxsize=1)
def windows_excluded_tcp_port_ranges() -> tuple[tuple[int, int], ...]:
    if platform.system().lower() != "windows":
        return ()
    try:
        result = subprocess.run(
            ["netsh", "int", "ipv4", "show", "excludedportrange", "protocol=tcp"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except Exception:
        return ()
    ranges: list[tuple[int, int]] = []
    for line in result.stdout.splitlines():
        match = re.match(r"\s*(\d+)\s+(\d+)\b", line)
        if match:
            ranges.append((int(match.group(1)), int(match.group(2))))
    return tuple(ranges)


def port_is_excluded(port: int, excluded_ranges: Iterable[tuple[int, int]] | None = None) -> bool:
    port = int(port)
    ranges = windows_excluded_tcp_port_ranges() if excluded_ranges is None else excluded_ranges
    return any(int(start) <= port <= int(end) for start, end in ranges)


def port_is_available(port: int, reserved: Iterable[int] = (), host: str = "127.0.0.1") -> bool:
    port = int(port)
    return port not in {int(item) for item in reserved} and not port_is_excluded(port) and not port_is_listening(port, host) and port_can_bind(port, host)


def first_available_port(candidates: Iterable[int], reserved: Iterable[int] = (), host: str = "127.0.0.1") -> int:
    for port in candidates:
        if port_is_available(int(port), reserved, host):
            return int(port)
    raise RuntimeError("no_available_port")


def allocate_port_triplet(reserved_ports: Iterable[int], ranges: PortRanges | None = None, host: str = "127.0.0.1") -> tuple[int, int, int]:
    ranges = ranges or PortRanges()
    reserved = {int(port) for port in reserved_ports}
    worker_api = first_available_port(ranges.worker_api, reserved, host)
    reserved.add(worker_api)
    extension_ws = first_available_port(ranges.extension_ws, reserved, host)
    reserved.add(extension_ws)
    chrome_cdp = first_available_port(ranges.chrome_cdp, reserved, host)
    return worker_api, extension_ws, chrome_cdp
