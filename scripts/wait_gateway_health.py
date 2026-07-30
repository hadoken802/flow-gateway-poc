"""Wait for the local Gateway health endpoint."""
import sys
import time
import urllib.request


def main() -> int:
    url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8200/health"
    timeout_seconds = float(sys.argv[2]) if len(sys.argv) > 2 else 30.0
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return 0
        except Exception as exc:
            last_error = exc
        time.sleep(1)
    if last_error:
        print(f"Gateway health check failed: {last_error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
