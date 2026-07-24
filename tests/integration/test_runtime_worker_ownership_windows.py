import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen

import pytest

from runtime.ownership import (
    OWNERSHIP_VERSION,
    WindowsDpapiSecretProtector,
    generate_challenge,
    read_runtime_secret,
    secret_fingerprint,
    sign_challenge,
    verify_challenge_response,
)
from runtime.process_manager import ProcessInspector, RuntimeManager
from runtime.registry import AccountRecord, AccountRegistry


pytestmark = [
    pytest.mark.skipif(os.name != "nt", reason="requires real Windows DPAPI and Windows process probing"),
]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _registry(tmp_path: Path) -> AccountRegistry:
    return AccountRegistry(
        db_path=tmp_path / "registry" / "runtime_registry.db",
        profiles_root=tmp_path / "profiles",
        data_root=tmp_path / "data",
        outputs_root=tmp_path / "outputs",
        workers_json_path=tmp_path / "workers.json",
    )


def _account(registry: AccountRegistry, account_id: str) -> AccountRecord:
    api_port = _free_port()
    ws_port = _free_port()
    cdp_port = _free_port()
    return AccountRecord(
        account_id=account_id,
        display_name=account_id,
        profile_path=str(registry.profiles_root / account_id),
        worker_api_port=api_port,
        extension_ws_port=ws_port,
        chrome_cdp_port=cdp_port,
        database_path=str(registry.data_root / f"{account_id}.db"),
        output_dir=str(registry.outputs_root / account_id),
        enabled=True,
        status="login_required",
        created_at="2026-07-24T00:00:00Z",
    )


def _register_temp_account(registry: AccountRegistry, account_id: str) -> AccountRecord:
    account = _account(registry, account_id)
    registry.register_many([account], create_dirs=True)
    return registry.get(account_id)


def _http_json(url: str, payload: dict | None = None) -> dict:
    if payload is None:
        with urlopen(url, timeout=2.0) as response:
            return json.loads(response.read().decode("utf-8"))
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=2.0) as response:
        return json.loads(response.read().decode("utf-8"))


def _wait_for_health(account: AccountRecord, timeout: float = 20.0) -> dict:
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            health = _http_json(f"http://127.0.0.1:{account.worker_api_port}/health")
            if health.get("account_id") == account.account_id:
                return health
        except Exception as error:
            last_error = type(error).__name__
        time.sleep(0.2)
    raise AssertionError(f"worker health was not reachable: {last_error}")


def _wait_port_released(port: int, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            if sock.connect_ex(("127.0.0.1", int(port))) != 0:
                return True
        time.sleep(0.2)
    return False


def _owned_runtime(registry: AccountRegistry, tmp_path: Path):
    procs = []

    def popen(command, **kwargs):
        proc = subprocess.Popen(command, **kwargs)
        procs.append(proc)
        return proc

    manager = RuntimeManager(
        registry,
        popen=popen,
        python_exe=Path(sys.executable),
        log_dir=tmp_path / "logs" / "runtime",
    )
    return manager, procs


def _stop_owned(procs, registry: AccountRegistry, account: AccountRecord) -> None:
    for proc in procs:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=8)
    _wait_port_released(account.worker_api_port)
    _wait_port_released(account.extension_ws_port)
    registry.mark_stopped(account.account_id)


def _topology(proc, account: AccountRecord) -> dict:
    inspector = ProcessInspector()
    api_pid = inspector.listening_pid(account.worker_api_port)
    ws_pid = inspector.listening_pid(account.extension_ws_port)
    related = sorted({pid for pid in (proc.pid, api_pid, ws_pid) if pid})
    return {
        "popen_pid": proc.pid,
        "popen_poll": proc.poll(),
        "api_listener_pid": api_pid,
        "ws_listener_pid": ws_pid,
        "same_listener_pid": bool(api_pid and ws_pid and int(api_pid) == int(ws_pid)),
        "pids": {
            int(pid): {
                "parent_pid": inspector.parent_pid(int(pid)),
                "alive": inspector.process_alive(int(pid)),
            }
            for pid in related
        },
    }


def _bootstrap_style_compensate(manager: RuntimeManager, account: AccountRecord, worker_pid: int | None) -> dict:
    manager._stop_started(account, worker_pid, None)
    return {
        "api_released": _wait_port_released(account.worker_api_port),
        "ws_released": _wait_port_released(account.extension_ws_port),
    }


def _safe_independent_verifier_script(path: Path) -> None:
    path.write_text(
        """
import json
import sys
from runtime.ownership import generate_challenge, read_runtime_secret, verify_challenge_response, OWNERSHIP_VERSION
from runtime.process_manager import RuntimeManager
from runtime.registry import AccountRegistry
from urllib.request import Request, urlopen

registry_path, profiles_root, data_root, outputs_root, workers_json, account_id = sys.argv[1:]
registry = AccountRegistry(registry_path, profiles_root, data_root, outputs_root, workers_json)
account = registry.get(account_id)
manager = RuntimeManager(registry)
health_response = urlopen(f"http://127.0.0.1:{account.worker_api_port}/health", timeout=2.0)
health = json.loads(health_response.read().decode("utf-8"))
verification = manager._worker_runtime_ownership(account, account.worker_pid)
secret = read_runtime_secret(account.runtime_secret_ref)
challenge = generate_challenge()
request = Request(
    f"http://127.0.0.1:{account.worker_api_port}/api/runtime/ownership/challenge",
    data=json.dumps({
        "account_id": account.account_id,
        "runtime_instance_id": account.runtime_instance_id,
        "challenge": challenge,
        "proof_version": OWNERSHIP_VERSION,
    }).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urlopen(request, timeout=2.0) as response:
    challenge_result = json.loads(response.read().decode("utf-8"))
verified = bool(challenge_result.get("ok")) and verify_challenge_response(
    secret,
    account.account_id,
    account.runtime_instance_id,
    challenge,
    challenge_result.get("challenge_response", ""),
    OWNERSHIP_VERSION,
)
print(json.dumps({
    "ok": bool(verification.get("verified") and verified),
    "account_id_match": health.get("account_id") == account_id,
    "runtime_instance_id_match": health.get("runtime_instance_id") == account.runtime_instance_id,
    "ownership_version_match": health.get("runtime_ownership_version") == OWNERSHIP_VERSION,
    "dpapi_decrypt_success": bool(secret),
    "challenge_verified": verified,
    "worker_ownership_method": verification.get("method"),
    "reason": verification.get("reason"),
}, sort_keys=True))
""".strip(),
        encoding="utf-8",
    )


def _challenge(account: AccountRecord, challenge_value: str, **overrides) -> dict:
    payload = {
        "account_id": account.account_id,
        "runtime_instance_id": account.runtime_instance_id,
        "challenge": challenge_value,
        "proof_version": OWNERSHIP_VERSION,
    }
    payload.update(overrides)
    return _http_json(f"http://127.0.0.1:{account.worker_api_port}/api/runtime/ownership/challenge", payload)


def test_windows_worker_runtime_ownership_uses_dpapi_and_independent_verifier(tmp_path):
    account_id = f"TEST-OWNERSHIP-{os.getpid()}"
    registry = _registry(tmp_path)
    account = _register_temp_account(registry, account_id)
    manager, procs = _owned_runtime(registry, tmp_path)
    first_secret_ref = None
    first_fingerprint = None
    first_instance_id = None

    try:
        started = manager.start_worker_only(account_id)
        assert started.result == "started"
        assert len(procs) == 1
        worker_pid = int(started.details["worker_pid"])
        account = registry.get(account_id)
        first_instance_id = account.runtime_instance_id
        first_secret_ref = account.runtime_secret_ref
        first_fingerprint = account.runtime_secret_fingerprint

        assert first_instance_id
        assert account.runtime_ownership_version == OWNERSHIP_VERSION
        assert account.worker_process_started_at
        assert first_secret_ref and Path(first_secret_ref).is_file()
        assert account.runtime_secret_fingerprint
        assert "FLOW_RUNTIME_OWNERSHIP_SECRET" not in " ".join(manager.worker_command(account))

        secret_file = Path(first_secret_ref)
        protected_bytes = secret_file.read_bytes()
        secret = read_runtime_secret(first_secret_ref, WindowsDpapiSecretProtector())
        assert len(secret.encode("utf-8")) >= 32
        assert secret.encode("utf-8") not in protected_bytes
        assert secret_fingerprint(secret) == first_fingerprint
        assert first_fingerprint in json.dumps(account.__dict__)
        assert secret not in json.dumps(account.__dict__)

        health = _wait_for_health(account)
        initial_topology = _topology(procs[0], account)
        assert initial_topology["same_listener_pid"] is True
        listener_pid = int(initial_topology["api_listener_pid"])
        popen_pid = int(initial_topology["popen_pid"])
        assert initial_topology["ws_listener_pid"] == listener_pid
        if listener_pid != popen_pid:
            assert initial_topology["pids"][listener_pid]["parent_pid"] == popen_pid
        assert initial_topology["popen_poll"] is None
        assert health["account_id"] == account_id
        assert health["runtime_instance_id"] == first_instance_id
        assert health["runtime_ownership_version"] == OWNERSHIP_VERSION
        assert health["runtime_ownership_capable"] is True
        forbidden_health = json.dumps(health)
        assert secret not in forbidden_health
        assert first_secret_ref not in forbidden_health
        assert "challenge_response" not in health

        inspector = ProcessInspector()
        api_pid = inspector.listening_pid(account.worker_api_port)
        ws_pid = inspector.listening_pid(account.extension_ws_port)
        assert api_pid == ws_pid
        assert api_pid is not None
        status = manager.status(account_id)
        assert status.details["worker_ownership_verified"] is True
        assert status.details["worker_ownership_method"] == "worker_challenge"
        assert status.details["ownership_status"] == "worker_verified"
        account = registry.get(account_id)
        worker_pid = int(account.worker_pid)
        assert worker_pid == api_pid

        verifier = tmp_path / "independent_verify.py"
        _safe_independent_verifier_script(verifier)
        completed = subprocess.run(
            [
                sys.executable,
                str(verifier),
                str(registry.db_path),
                str(registry.profiles_root),
                str(registry.data_root),
                str(registry.outputs_root),
                str(registry.workers_json_path),
                account_id,
            ],
            cwd=str(Path(__file__).resolve().parents[2]),
            env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2])},
            text=True,
            capture_output=True,
            timeout=20,
            check=True,
        )
        independent = json.loads(completed.stdout)
        assert independent == {
            "ok": True,
            "account_id_match": True,
            "runtime_instance_id_match": True,
            "ownership_version_match": True,
            "dpapi_decrypt_success": True,
            "challenge_verified": True,
            "worker_ownership_method": "worker_challenge",
            "reason": "verified",
        }
        assert secret not in completed.stdout
        assert "challenge_response" not in completed.stdout

        challenges = [generate_challenge() for _ in range(3)]
        assert len(set(challenges)) == 3
        secret_mtime = secret_file.stat().st_mtime_ns
        for challenge in challenges:
            response = _challenge(account, challenge)
            assert response["ok"] is True
            assert verify_challenge_response(secret, account_id, first_instance_id, challenge, response["challenge_response"], OWNERSHIP_VERSION)
        assert secret_file.stat().st_mtime_ns == secret_mtime
        assert registry.get(account_id).runtime_instance_id == first_instance_id
        assert registry.get(account_id).worker_pid == worker_pid

        negative_cases = [
            ("wrong_account", {"account_id": "TEST-OWNERSHIP-WRONG"}, "account_mismatch"),
            ("wrong_instance", {"runtime_instance_id": "00000000-0000-0000-0000-000000000000"}, "runtime_instance_mismatch"),
            ("wrong_version", {"proof_version": 999}, "ownership_protocol_unsupported"),
            ("empty_challenge", {"challenge": ""}, "invalid_challenge"),
            ("bad_challenge", {"challenge": "***not-valid***"}, "invalid_challenge"),
            ("short_challenge", {"challenge": "abc"}, "invalid_challenge"),
            ("extra_field", {"unexpected": "value"}, "invalid_fields"),
        ]
        for _, override, reason in negative_cases:
            response = _challenge(account, generate_challenge(), **override)
            assert response["ok"] is False
            assert response["reason"] == reason
            assert "challenge_response" not in response
        status_after_negative = manager.status(account_id)
        assert status_after_negative.details["worker_ownership_verified"] is True
        assert status_after_negative.details["worker_ownership_method"] == "worker_challenge"
        assert status_after_negative.details["ownership_status"] == "worker_verified"
        assert status_after_negative.details["stop_safe"] is False

        worker_log = (tmp_path / "logs" / "runtime" / f"{account_id}-worker.log").read_text(encoding="utf-8", errors="ignore")
        leak_haystack = "\n".join([worker_log, completed.stdout, completed.stderr, registry.db_path.read_bytes().decode("latin1", errors="ignore")])
        assert secret not in leak_haystack
        assert "FLOW_RUNTIME_OWNERSHIP_SECRET=" not in leak_haystack
        assert "response_hmac" not in leak_haystack

    finally:
        account = registry.get(account_id) or account
        _stop_owned(procs, registry, account)

    first_secret_exists_after_stop = Path(first_secret_ref).exists()
    manager, second_procs = _owned_runtime(registry, tmp_path)
    try:
        started = manager.start_worker_only(account_id)
        assert started.result == "started"
        second = registry.get(account_id)
        assert second.runtime_instance_id != first_instance_id
        assert second.runtime_secret_ref != first_secret_ref
        assert second.runtime_secret_fingerprint != first_fingerprint
        assert second.runtime_ownership_version == OWNERSHIP_VERSION
        second_health = _wait_for_health(second)
        second_api_pid = ProcessInspector().listening_pid(second.worker_api_port)
        second_ws_pid = ProcessInspector().listening_pid(second.extension_ws_port)
        assert second_api_pid == second_ws_pid
        assert second_api_pid != worker_pid
        assert second_health["runtime_instance_id"] == second.runtime_instance_id
        new_secret = read_runtime_secret(second.runtime_secret_ref, WindowsDpapiSecretProtector())
        old_secret = read_runtime_secret(first_secret_ref, WindowsDpapiSecretProtector()) if first_secret_exists_after_stop else secret
        challenge = generate_challenge()
        response = _challenge(second, challenge)
        assert response["ok"] is True
        assert verify_challenge_response(new_secret, account_id, second.runtime_instance_id, challenge, response["challenge_response"], OWNERSHIP_VERSION)
        assert not verify_challenge_response(old_secret, account_id, second.runtime_instance_id, challenge, response["challenge_response"], OWNERSHIP_VERSION)
        assert len(list((registry.data_root / "runtime_secrets" / account_id).glob("*.secret"))) >= 1
    finally:
        second = registry.get(account_id) or account
        _stop_owned(second_procs, registry, second)

    assert _wait_port_released(account.worker_api_port)
    assert _wait_port_released(account.extension_ws_port)


def test_bootstrap_style_compensation_releases_worker_before_pid_cas(tmp_path):
    account_id = f"TEST-TOPOLOGY-NOCAS-{os.getpid()}"
    registry = _registry(tmp_path)
    account = _register_temp_account(registry, account_id)
    manager, procs = _owned_runtime(registry, tmp_path)
    compensation = None

    try:
        started = manager.start_worker_only(account_id)
        assert started.result == "started"
        account = registry.get(account_id)
        _wait_for_health(account)
        topology = _topology(procs[0], account)
        assert topology["same_listener_pid"] is True
        if topology["api_listener_pid"] != procs[0].pid:
            assert topology["pids"][int(topology["api_listener_pid"])]["parent_pid"] == procs[0].pid
        assert registry.get(account_id).worker_pid == procs[0].pid

        compensation = _bootstrap_style_compensate(manager, account, started.details["worker_pid"])

        assert compensation == {"api_released": True, "ws_released": True}
        assert procs[0].poll() is not None
        assert ProcessInspector().process_alive(procs[0].pid) is False
        assert registry.get(account_id).worker_pid is None
    finally:
        if compensation is None:
            account = registry.get(account_id) or account
            _stop_owned(procs, registry, account)


def test_bootstrap_style_compensation_releases_worker_after_pid_cas(tmp_path):
    account_id = f"TEST-TOPOLOGY-CAS-{os.getpid()}"
    registry = _registry(tmp_path)
    account = _register_temp_account(registry, account_id)
    manager, procs = _owned_runtime(registry, tmp_path)
    compensation = None

    try:
        started = manager.start_worker_only(account_id)
        assert started.result == "started"
        account = registry.get(account_id)
        _wait_for_health(account)
        status = manager.status(account_id)
        assert status.details["worker_ownership_verified"] is True
        account = registry.get(account_id)
        topology = _topology(procs[0], account)
        assert topology["same_listener_pid"] is True
        assert topology["api_listener_pid"] == account.worker_pid
        if topology["api_listener_pid"] != procs[0].pid:
            assert topology["pids"][int(topology["api_listener_pid"])]["parent_pid"] == procs[0].pid

        compensation = _bootstrap_style_compensate(manager, account, account.worker_pid)

        assert compensation == {"api_released": True, "ws_released": True}
        assert procs[0].poll() is not None
        assert ProcessInspector().process_alive(topology["api_listener_pid"]) is False
        assert registry.get(account_id).worker_pid is None
    finally:
        if compensation is None:
            account = registry.get(account_id) or account
            _stop_owned(procs, registry, account)
