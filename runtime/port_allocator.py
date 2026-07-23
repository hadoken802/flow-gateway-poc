"""Port allocation for local Flow account runtimes."""
from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class PortRanges:
    worker_api: range = range(8100, 8200)
    extension_ws: range = range(9200, 9300)
    chrome_cdp: range = range(9300, 9400)


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


def port_is_available(port: int, reserved: Iterable[int] = (), host: str = "127.0.0.1") -> bool:
    port = int(port)
    return port not in {int(item) for item in reserved} and not port_is_listening(port, host) and port_can_bind(port, host)


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

