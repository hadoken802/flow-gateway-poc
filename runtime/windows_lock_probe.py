"""Read-only Windows Restart Manager file lock diagnostics."""
from __future__ import annotations

import ctypes
import os
import time
from ctypes import wintypes
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ERROR_MORE_DATA = 234
ERROR_NO_MORE_FILES = 18
RM_SESSION_KEY_LEN = 32
RM_MAX_APP_NAME = 255
RM_MAX_SVC_NAME = 63
TH32CS_SNAPPROCESS = 0x00000002
INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000

SENSITIVE_PROFILE_NAMES = {
    "cookies",
    "login data",
    "web data",
    "local storage",
    "indexeddb",
    "session storage",
}


class RestartManagerProbeError(OSError):
    def __init__(self, stage: str, function: str, rm_result_code: int | None = None, original: BaseException | None = None):
        message = f"{function} failed"
        super().__init__(rm_result_code or getattr(original, "errno", None) or 0, message)
        self.stage = stage
        self.function = function
        self.rm_result_code = rm_result_code
        self.winerror = getattr(original, "winerror", None)
        self.errno = getattr(original, "errno", None)
        self.message_safe = message
        self.secondary_cleanup_error: str | None = None


@dataclass(frozen=True)
class LockHolder:
    pid: int
    process_name: str | None = None
    application_type: int | None = None
    session_id: int | None = None
    restartable: bool | None = None
    process_start_time: int | None = None
    parent_pid: int | None = None
    classification: str | None = None

    def to_dict(self) -> dict:
        return {
            "holder_pid": self.pid,
            "holder_process_name": self.process_name,
            "holder_application_type": self.application_type,
            "holder_session_id": self.session_id,
            "holder_restartable": self.restartable,
            "holder_process_start_time": self.process_start_time,
            "holder_parent_pid": self.parent_pid,
            "classification": self.classification,
        }


class FILETIME(ctypes.Structure):
    _fields_ = [
        ("dwLowDateTime", wintypes.DWORD),
        ("dwHighDateTime", wintypes.DWORD),
    ]


class RM_UNIQUE_PROCESS(ctypes.Structure):
    _fields_ = [
        ("dwProcessId", wintypes.DWORD),
        ("ProcessStartTime", FILETIME),
    ]


class RM_PROCESS_INFO(ctypes.Structure):
    _fields_ = [
        ("Process", RM_UNIQUE_PROCESS),
        ("strAppName", wintypes.WCHAR * (RM_MAX_APP_NAME + 1)),
        ("strServiceShortName", wintypes.WCHAR * (RM_MAX_SVC_NAME + 1)),
        ("ApplicationType", wintypes.DWORD),
        ("AppStatus", wintypes.ULONG),
        ("TSSessionId", wintypes.DWORD),
        ("bRestartable", wintypes.BOOL),
    ]


class PROCESSENTRY32W(ctypes.Structure):
    _fields_ = [
        ("dwSize", wintypes.DWORD),
        ("cntUsage", wintypes.DWORD),
        ("th32ProcessID", wintypes.DWORD),
        ("th32DefaultHeapID", ctypes.c_size_t),
        ("th32ModuleID", wintypes.DWORD),
        ("cntThreads", wintypes.DWORD),
        ("th32ParentProcessID", wintypes.DWORD),
        ("pcPriClassBase", ctypes.c_long),
        ("dwFlags", wintypes.DWORD),
        ("szExeFile", wintypes.WCHAR * 260),
    ]


def is_safe_dawn_cache_path(target_path: str | Path, expected_profile_root: str | Path) -> dict:
    try:
        target = Path(target_path)
        profile = Path(expected_profile_root)
        if not target.is_absolute():
            return {"ok": False, "reason": "path_not_absolute"}
        target_resolved = target.resolve(strict=False)
        profile_resolved = profile.resolve(strict=False)
        relative = target_resolved.relative_to(profile_resolved)
    except ValueError:
        return {"ok": False, "reason": "path_outside_profile"}
    except Exception:
        return {"ok": False, "reason": "path_resolution_failed"}

    parts_lower = [part.lower() for part in relative.parts]
    if any(name in parts_lower for name in SENSITIVE_PROFILE_NAMES):
        return {"ok": False, "reason": "sensitive_profile_path"}
    if len(parts_lower) < 3 or parts_lower[0] != "gpupersistentcache" or parts_lower[1] != "dawngraphitecache":
        return {"ok": False, "reason": "path_not_dawn_cache"}
    return {
        "ok": True,
        "reason": "none",
        "target_path": str(target_resolved),
        "relative_path": relative.as_posix(),
        "dawn_root": str(profile_resolved / "GPUPersistentCache" / "DawnGraphiteCache"),
    }


def filetime_to_int(value: FILETIME) -> int:
    return (int(value.dwHighDateTime) << 32) + int(value.dwLowDateTime)


def get_parent_pid(pid: int) -> int | None:
    if os.name != "nt":
        return None
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel32.Process32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    kernel32.Process32FirstW.restype = wintypes.BOOL
    kernel32.Process32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    kernel32.Process32NextW.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    snapshot = kernel32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)
    if snapshot == INVALID_HANDLE_VALUE:
        return None
    try:
        entry = PROCESSENTRY32W()
        entry.dwSize = ctypes.sizeof(PROCESSENTRY32W)
        if not kernel32.Process32FirstW(snapshot, ctypes.byref(entry)):
            return None
        while True:
            if int(entry.th32ProcessID) == int(pid):
                return int(entry.th32ParentProcessID)
            if not kernel32.Process32NextW(snapshot, ctypes.byref(entry)):
                return None
    finally:
        kernel32.CloseHandle(snapshot)


def get_process_start_time(pid: int) -> int | None:
    if os.name != "nt":
        return None
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
    ]
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
    if not handle:
        return None
    try:
        created = FILETIME()
        exited = FILETIME()
        kernel = FILETIME()
        user = FILETIME()
        if not kernel32.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited), ctypes.byref(kernel), ctypes.byref(user)):
            return None
        return filetime_to_int(created)
    finally:
        kernel32.CloseHandle(handle)


def classify_lock_holder(
    holder: LockHolder,
    current_attempt_pids: set[int] | None = None,
    previous_attempt_pids: set[int] | None = None,
    worker_pids: set[int] | None = None,
    known_process_start_times: dict[int, int | None] | None = None,
) -> str:
    current_attempt_pids = current_attempt_pids or set()
    previous_attempt_pids = previous_attempt_pids or set()
    worker_pids = worker_pids or set()
    known_process_start_times = known_process_start_times or {}
    if holder.pid in known_process_start_times and holder.process_start_time and known_process_start_times.get(holder.pid):
        if int(holder.process_start_time) != int(known_process_start_times[holder.pid]):
            return "pid_reused_or_unknown"
    pid_candidates = {holder.pid}
    if holder.parent_pid:
        pid_candidates.add(holder.parent_pid)
    if pid_candidates & current_attempt_pids:
        return "current_attempt_chrome"
    if pid_candidates & previous_attempt_pids:
        return "previous_attempt_chrome"
    if pid_candidates & worker_pids:
        return "worker_process"
    if holder.pid:
        return "external_process"
    return "pid_reused_or_unknown"


def summarize_probe_samples(samples: list[dict]) -> dict:
    classifications: list[str] = []
    holder_count = 0
    completed_samples = [sample for sample in samples if sample.get("rm_get_list_completed")]
    error_samples = [sample for sample in samples if sample.get("probe_error_type") or sample.get("probe_error_stage")]
    no_candidate_samples = [sample for sample in samples if sample.get("classification") == "no_candidate_files"]
    for sample in samples:
        holder_count += int(sample.get("holder_count") or 0)
        for holder in sample.get("holders") or []:
            classification = holder.get("classification")
            if classification:
                classifications.append(classification)
    priority = [
        "external_process",
        "previous_attempt_chrome",
        "current_attempt_chrome",
        "worker_process",
        "pid_reused_or_unknown",
    ]
    selected = None
    if classifications:
        for item in priority:
            if item in classifications:
                selected = item
                break
    elif error_samples and not completed_samples:
        selected = "probe_error"
    elif error_samples and completed_samples:
        selected = "partial_probe_error"
    elif no_candidate_samples and len(no_candidate_samples) == len(samples):
        selected = "no_candidate_files"
    elif completed_samples:
        selected = "no_holder_found"
    elif not samples:
        selected = "probe_not_completed"
    else:
        selected = "probe_not_completed"
    reported_holder_count = holder_count if completed_samples or classifications else None
    return {
        "dawn_lock_probe_sample_count": len(samples),
        "dawn_lock_probe_holder_count": reported_holder_count,
        "dawn_lock_probe_holder_classification": selected,
        "dawn_lock_probe_no_candidate_files": selected == "no_candidate_files",
        "dawn_lock_probe_current_attempt_chrome_detected": "current_attempt_chrome" in classifications,
        "dawn_lock_probe_previous_attempt_chrome_detected": "previous_attempt_chrome" in classifications,
        "dawn_lock_probe_external_process_detected": "external_process" in classifications,
    }


class RestartManagerLockProbe:
    def __init__(self):
        if os.name != "nt":
            raise RuntimeError("restart_manager_windows_only")
        self._rstrtmgr = ctypes.WinDLL("Rstrtmgr")
        self._rstrtmgr.RmStartSession.argtypes = [ctypes.POINTER(wintypes.DWORD), wintypes.DWORD, wintypes.LPWSTR]
        self._rstrtmgr.RmStartSession.restype = wintypes.DWORD
        self._rstrtmgr.RmRegisterResources.argtypes = [
            wintypes.DWORD,
            wintypes.UINT,
            ctypes.POINTER(wintypes.LPCWSTR),
            wintypes.UINT,
            ctypes.c_void_p,
            wintypes.UINT,
            ctypes.c_void_p,
        ]
        self._rstrtmgr.RmRegisterResources.restype = wintypes.DWORD
        self._rstrtmgr.RmGetList.argtypes = [
            wintypes.DWORD,
            ctypes.POINTER(wintypes.UINT),
            ctypes.POINTER(wintypes.UINT),
            ctypes.POINTER(RM_PROCESS_INFO),
            ctypes.POINTER(wintypes.DWORD),
        ]
        self._rstrtmgr.RmGetList.restype = wintypes.DWORD
        self._rstrtmgr.RmEndSession.argtypes = [wintypes.DWORD]
        self._rstrtmgr.RmEndSession.restype = wintypes.DWORD

    def holders(self, target_path: str | Path) -> list[LockHolder]:
        return self.holders_for_paths([target_path])

    def holders_for_paths(self, target_paths: Iterable[str | Path]) -> list[LockHolder]:
        paths = [str(path) for path in target_paths]
        if not paths:
            return []
        session = wintypes.DWORD()
        key = ctypes.create_unicode_buffer(RM_SESSION_KEY_LEN + 1)
        result = self._rstrtmgr.RmStartSession(ctypes.byref(session), 0, key)
        if result != 0:
            raise RestartManagerProbeError("RmStartSession", "RmStartSession", int(result))
        primary_error: RestartManagerProbeError | None = None
        try:
            path_array = (wintypes.LPCWSTR * len(paths))(*paths)
            result = self._rstrtmgr.RmRegisterResources(session, len(paths), path_array, 0, None, 0, None)
            if result != 0:
                raise RestartManagerProbeError("RmRegisterResources", "RmRegisterResources", int(result))
            infos = None
            count = wintypes.UINT(0)
            for _ in range(2):
                needed = wintypes.UINT(0)
                count = wintypes.UINT(0)
                reboot_reasons = wintypes.DWORD(0)
                result = self._rstrtmgr.RmGetList(session, ctypes.byref(needed), ctypes.byref(count), None, ctypes.byref(reboot_reasons))
                if result == 0 and needed.value == 0:
                    return []
                if result not in (ERROR_MORE_DATA, 0):
                    raise RestartManagerProbeError("RmGetList", "RmGetList", int(result))
                count = wintypes.UINT(needed.value)
                infos = (RM_PROCESS_INFO * max(1, needed.value))()
                result = self._rstrtmgr.RmGetList(session, ctypes.byref(needed), ctypes.byref(count), infos, ctypes.byref(reboot_reasons))
                if result == ERROR_MORE_DATA:
                    continue
                if result != 0:
                    raise RestartManagerProbeError("RmGetList", "RmGetList", int(result))
                break
            else:
                raise RestartManagerProbeError("RmGetList", "RmGetList", ERROR_MORE_DATA)
            holders: list[LockHolder] = []
            for index in range(count.value):
                info = infos[index]
                pid = int(info.Process.dwProcessId)
                holders.append(
                    LockHolder(
                        pid=pid,
                        process_name=str(info.strAppName) or None,
                        application_type=int(info.ApplicationType),
                        session_id=int(info.TSSessionId),
                        restartable=bool(info.bRestartable),
                        process_start_time=filetime_to_int(info.Process.ProcessStartTime),
                        parent_pid=get_parent_pid(pid),
                    )
                )
            return holders
        except RestartManagerProbeError as error:
            primary_error = error
            raise
        finally:
            cleanup_result = self._rstrtmgr.RmEndSession(session)
            if cleanup_result != 0 and primary_error is not None:
                primary_error.secondary_cleanup_error = f"RmEndSession:{int(cleanup_result)}"


def probe_candidates(target_path: str | Path, expected_profile_root: str | Path | None = None, max_files: int = 32, max_depth: int = 2) -> dict:
    target = Path(target_path)
    profile = Path(expected_profile_root) if expected_profile_root is not None else None
    safe = is_safe_dawn_cache_path(target, profile or target.parents[2])
    if not safe.get("ok"):
        return {"ok": False, "reason": safe.get("reason"), "resource_kind": "unknown", "candidates": [], "candidate_relative_paths": []}
    target_resolved = Path(safe["target_path"])
    dawn_root = Path(safe["dawn_root"]).resolve(strict=False)

    def safe_file(path: Path) -> Path | None:
        try:
            if path.is_symlink():
                return None
            resolved = path.resolve(strict=False)
            resolved.relative_to(dawn_root)
            if not resolved.is_file():
                return None
            return resolved
        except Exception:
            return None

    if target_resolved.is_file():
        return {
            "ok": True,
            "reason": "none",
            "resource_kind": "file",
            "candidates": [target_resolved],
            "candidate_relative_paths": [target_resolved.relative_to(dawn_root).as_posix()],
        }
    if not target_resolved.is_dir():
        return {"ok": True, "reason": "none", "resource_kind": "missing", "candidates": [], "candidate_relative_paths": []}

    candidates: list[Path] = []
    try:
        for child in target_resolved.rglob("*"):
            try:
                rel = child.resolve(strict=False).relative_to(target_resolved)
                if len(rel.parts) > max_depth:
                    continue
                candidate = safe_file(child)
                if candidate is not None:
                    candidates.append(candidate)
            except Exception:
                continue
    except OSError:
        return {"ok": True, "reason": "candidate_enumeration_failed", "resource_kind": "directory", "candidates": [], "candidate_relative_paths": []}
    candidates = sorted(set(candidates), key=lambda path: path.stat().st_mtime if path.exists() else 0, reverse=True)[:max_files]
    return {
        "ok": True,
        "reason": "none",
        "resource_kind": "directory",
        "candidates": candidates,
        "candidate_relative_paths": [path.relative_to(dawn_root).as_posix() for path in candidates],
    }


def _safe_error_details(error: BaseException, stage: str | None = None, function: str | None = None) -> dict:
    return {
        "probe_error_type": type(error).__name__,
        "probe_error_stage": getattr(error, "stage", stage),
        "probe_error_function": getattr(error, "function", function),
        "probe_rm_result_code": getattr(error, "rm_result_code", None),
        "probe_winerror": getattr(error, "winerror", None),
        "probe_errno": getattr(error, "errno", None),
        "probe_error_message_safe": getattr(error, "message_safe", str(error).splitlines()[0][:160]),
        "probe_secondary_cleanup_error": getattr(error, "secondary_cleanup_error", None),
    }


def probe_dawn_cache_lock(
    target_path: str | Path,
    expected_profile_root: str | Path,
    current_attempt_pids: set[int] | None = None,
    previous_attempt_pids: set[int] | None = None,
    worker_pids: set[int] | None = None,
    attempt_number: int | None = None,
    samples: tuple[float, ...] = (0.0, 0.1, 0.3),
    sleep=time.sleep,
    probe: RestartManagerLockProbe | None = None,
) -> dict:
    safe = is_safe_dawn_cache_path(target_path, expected_profile_root)
    result = {
        "attempt_number": attempt_number,
        "target_path_safe": bool(safe.get("ok")),
        "target_relative": safe.get("relative_path"),
        "path_reason": safe.get("reason"),
        "samples": [],
    }
    if not safe.get("ok"):
        result["probe_error"] = "path_not_safe"
        result["summary"] = {
            **summarize_probe_samples([]),
            "dawn_lock_probe_holder_count": None,
            "dawn_lock_probe_holder_classification": "path_not_safe",
        }
        return result
    try:
        probe = probe or RestartManagerLockProbe()
        known_process_start_times = {
            int(pid): get_process_start_time(int(pid))
            for pid in set(current_attempt_pids or set()) | set(previous_attempt_pids or set()) | set(worker_pids or set())
        }
        for index, delay in enumerate(samples):
            if delay > 0:
                sleep(delay)
            started = time.perf_counter()
            sample = {
                "sample_index": index,
                "sampled_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "holders": [],
                "rm_session_started": False,
                "rm_resources_registered": False,
                "rm_get_list_completed": False,
            }
            candidates = probe_candidates(safe["target_path"], expected_profile_root)
            sample["resource_kind"] = candidates.get("resource_kind")
            sample["candidate_count"] = len(candidates.get("candidates") or [])
            sample["candidate_relative_paths"] = candidates.get("candidate_relative_paths") or []
            try:
                if not candidates.get("ok"):
                    sample["classification"] = "path_not_safe"
                elif not candidates.get("candidates"):
                    sample["classification"] = "no_candidate_files"
                else:
                    sample["rm_session_started"] = True
                    sample["rm_resources_registered"] = True
                    holders_by_pid: dict[int, LockHolder] = {}
                    for holder in probe.holders_for_paths(candidates["candidates"]):
                        holders_by_pid.setdefault(holder.pid, holder)
                    sample["rm_get_list_completed"] = True
                    holders = list(holders_by_pid.values())
                    for holder in holders:
                        classification = classify_lock_holder(
                            holder,
                            current_attempt_pids,
                            previous_attempt_pids,
                            worker_pids,
                            known_process_start_times,
                        )
                        sample["holders"].append(LockHolder(**{**holder.__dict__, "classification": classification}).to_dict())
                    sample["holder_count"] = len(sample["holders"])
            except Exception as error:
                sample.update(_safe_error_details(error))
                sample["classification"] = "probe_error"
            finally:
                sample["duration_ms"] = int((time.perf_counter() - started) * 1000)
            result["samples"].append(sample)
        result["summary"] = summarize_probe_samples(result["samples"])
    except Exception as error:
        result.update(_safe_error_details(error))
        result["probe_error"] = type(error).__name__
        result["summary"] = summarize_probe_samples(result["samples"])
    errors = [sample for sample in result["samples"] if sample.get("probe_error_type") or sample.get("probe_error_stage")]
    if errors:
        first = errors[0]
        result["probe_error"] = first.get("probe_error_type")
        for key in (
            "probe_error_type",
            "probe_error_stage",
            "probe_error_function",
            "probe_rm_result_code",
            "probe_winerror",
            "probe_errno",
            "probe_error_message_safe",
            "probe_secondary_cleanup_error",
        ):
            result[key] = first.get(key)
    if result["samples"]:
        first_sample = result["samples"][0]
        result["resource_kind"] = first_sample.get("resource_kind")
        result["candidate_count"] = max(int(sample.get("candidate_count") or 0) for sample in result["samples"])
        result["candidate_relative_paths"] = next((sample.get("candidate_relative_paths") for sample in result["samples"] if sample.get("candidate_relative_paths")), [])
    return result
