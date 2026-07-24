"""Runtime ownership identity helpers."""
from __future__ import annotations

import base64
import ctypes
import hashlib
import hmac
import json
import os
import secrets
import uuid
from dataclasses import dataclass
from pathlib import Path


OWNERSHIP_VERSION = 1
WORKER_OWNERSHIP_PURPOSE = "worker-stop-ownership"


class OwnershipError(RuntimeError):
    """Raised when an ownership secret cannot be safely protected or read."""


@dataclass(frozen=True)
class RuntimeOwnershipIdentity:
    runtime_instance_id: str
    secret: str
    secret_ref: str
    secret_fingerprint: str
    version: int = OWNERSHIP_VERSION


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def generate_runtime_instance_id() -> str:
    return str(uuid.uuid4())


def generate_secret() -> str:
    return _b64url(secrets.token_bytes(32))


def generate_challenge() -> str:
    return _b64url(secrets.token_bytes(32))


def secret_fingerprint(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()[:16]


def canonical_payload(account_id: str, runtime_instance_id: str, challenge: str, proof_version: int, purpose: str = WORKER_OWNERSHIP_PURPOSE) -> bytes:
    payload = {
        "account_id": str(account_id),
        "challenge": str(challenge),
        "proof_version": int(proof_version),
        "purpose": purpose,
        "runtime_instance_id": str(runtime_instance_id),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign_challenge(secret: str, account_id: str, runtime_instance_id: str, challenge: str, proof_version: int = OWNERSHIP_VERSION) -> str:
    digest = hmac.new(
        secret.encode("utf-8"),
        canonical_payload(account_id, runtime_instance_id, challenge, proof_version),
        hashlib.sha256,
    ).hexdigest()
    return digest


def verify_challenge_response(secret: str, account_id: str, runtime_instance_id: str, challenge: str, response: str, proof_version: int = OWNERSHIP_VERSION) -> bool:
    expected = sign_challenge(secret, account_id, runtime_instance_id, challenge, proof_version)
    return hmac.compare_digest(expected, str(response or ""))


def validate_challenge(value: str) -> bool:
    if not isinstance(value, str) or len(value) < 32 or len(value) > 128:
        return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
    return all(ch in allowed for ch in value)


class OwnershipSecretProtector:
    def protect(self, data: bytes) -> bytes:
        raise NotImplementedError

    def unprotect(self, data: bytes) -> bytes:
        raise NotImplementedError


class WindowsDpapiSecretProtector(OwnershipSecretProtector):
    class _DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", ctypes.c_ulong), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]

    def __init__(self):
        if os.name != "nt":
            raise OwnershipError("dpapi_unavailable")
        self.crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
        self.kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self.crypt32.CryptProtectData.argtypes = [
            ctypes.POINTER(self._DATA_BLOB),
            ctypes.c_wchar_p,
            ctypes.POINTER(self._DATA_BLOB),
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_ulong,
            ctypes.POINTER(self._DATA_BLOB),
        ]
        self.crypt32.CryptProtectData.restype = ctypes.c_bool
        self.crypt32.CryptUnprotectData.argtypes = [
            ctypes.POINTER(self._DATA_BLOB),
            ctypes.POINTER(ctypes.c_wchar_p),
            ctypes.POINTER(self._DATA_BLOB),
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_ulong,
            ctypes.POINTER(self._DATA_BLOB),
        ]
        self.crypt32.CryptUnprotectData.restype = ctypes.c_bool
        self.kernel32.LocalFree.argtypes = [ctypes.c_void_p]
        self.kernel32.LocalFree.restype = ctypes.c_void_p

    def _blob_from_bytes(self, data: bytes):
        buffer = (ctypes.c_ubyte * len(data)).from_buffer_copy(data)
        return self._DATA_BLOB(len(data), buffer)

    def _bytes_from_blob(self, blob) -> bytes:
        try:
            return ctypes.string_at(blob.pbData, blob.cbData)
        finally:
            if blob.pbData:
                self.kernel32.LocalFree(blob.pbData)

    def protect(self, data: bytes) -> bytes:
        in_blob = self._blob_from_bytes(data)
        out_blob = self._DATA_BLOB()
        if not self.crypt32.CryptProtectData(ctypes.byref(in_blob), None, None, None, None, 0, ctypes.byref(out_blob)):
            raise OwnershipError("ownership_secret_protection_failed")
        return self._bytes_from_blob(out_blob)

    def unprotect(self, data: bytes) -> bytes:
        in_blob = self._blob_from_bytes(data)
        out_blob = self._DATA_BLOB()
        if not self.crypt32.CryptUnprotectData(ctypes.byref(in_blob), None, None, None, None, 0, ctypes.byref(out_blob)):
            raise OwnershipError("ownership_secret_unavailable")
        return self._bytes_from_blob(out_blob)


def create_runtime_identity(account_id: str, data_root: Path, protector: OwnershipSecretProtector | None = None) -> RuntimeOwnershipIdentity:
    protector = protector or WindowsDpapiSecretProtector()
    runtime_instance_id = generate_runtime_instance_id()
    secret = generate_secret()
    protected = protector.protect(secret.encode("utf-8"))
    directory = Path(data_root) / "runtime_secrets" / account_id
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"runtime-{runtime_instance_id}.secret"
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_bytes(protected)
    os.replace(tmp_path, path)
    return RuntimeOwnershipIdentity(
        runtime_instance_id=runtime_instance_id,
        secret=secret,
        secret_ref=str(path),
        secret_fingerprint=secret_fingerprint(secret),
    )


def read_runtime_secret(secret_ref: str | None, protector: OwnershipSecretProtector | None = None) -> str:
    if not secret_ref:
        raise OwnershipError("ownership_secret_unavailable")
    path = Path(secret_ref)
    if not path.exists():
        raise OwnershipError("ownership_secret_unavailable")
    protector = protector or WindowsDpapiSecretProtector()
    return protector.unprotect(path.read_bytes()).decode("utf-8")


def delete_secret_ref(secret_ref: str | None) -> None:
    if not secret_ref:
        return
    try:
        Path(secret_ref).unlink(missing_ok=True)
    except Exception:
        pass
