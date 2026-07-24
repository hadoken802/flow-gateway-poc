import subprocess
import sys
import time
from pathlib import Path

import pytest

from runtime.windows_lock_probe import probe_dawn_cache_lock


pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="Windows Restart Manager integration only")


def test_restart_manager_probe_detects_synthetic_file_lock_holder(tmp_path):
    profile = tmp_path / "profiles" / "TEST-LOCK"
    target = profile / "GPUPersistentCache" / "DawnGraphiteCache" / "abc" / "cache.db"
    ready = tmp_path / "holder.ready"
    target.parent.mkdir(parents=True)
    target.write_text("cache", encoding="utf-8")
    child_code = r"""
import ctypes
import os
import sys
import time
from pathlib import Path

path = sys.argv[1]
ready = Path(sys.argv[2])
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
handle = kernel32.CreateFileW(
    ctypes.c_wchar_p(path),
    0x80000000,
    0,
    None,
    3,
    0x80,
    None,
)
if handle == -1 or handle == 0:
    raise SystemExit(ctypes.get_last_error())
ready.write_text(str(os.getpid()), encoding="utf-8")
try:
    time.sleep(30)
finally:
    kernel32.CloseHandle(handle)
"""
    proc = subprocess.Popen([sys.executable, "-c", child_code, str(target), str(ready)])
    try:
        deadline = time.time() + 10
        while time.time() < deadline and not ready.exists():
            time.sleep(0.05)
        assert ready.exists()
        holder_pid = int(ready.read_text(encoding="utf-8"))
        with pytest.raises(OSError):
            with target.open("rb"):
                pass

        result = probe_dawn_cache_lock(
            target,
            profile,
            current_attempt_pids={999999},
            samples=(0.0,),
            sleep=lambda _: None,
        )

        holders = result["samples"][0]["holders"]
        holder_pids = {holder["holder_pid"] for holder in holders}
        assert holder_pid in holder_pids
        assert result["summary"]["dawn_lock_probe_holder_count"] >= 1
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    deadline = time.time() + 5
    released = None
    while time.time() < deadline:
        released = probe_dawn_cache_lock(target, profile, samples=(0.0,), sleep=lambda _: None)
        if released["summary"]["dawn_lock_probe_holder_count"] == 0:
            break
        time.sleep(0.1)
    assert released is not None
    assert released["summary"]["dawn_lock_probe_holder_classification"] == "no_holder_found"
