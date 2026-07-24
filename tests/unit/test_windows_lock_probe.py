from pathlib import Path

import pytest

from runtime.windows_lock_probe import (
    LockHolder,
    RestartManagerProbeError,
    classify_lock_holder,
    is_safe_dawn_cache_path,
    probe_candidates,
    probe_dawn_cache_lock,
    summarize_probe_samples,
)


def test_safe_profile_dawn_cache_path_is_allowed(tmp_path):
    profile = tmp_path / "profiles" / "FLOW-001"
    target = profile / "GPUPersistentCache" / "DawnGraphiteCache" / "cache.db"

    result = is_safe_dawn_cache_path(target, profile)

    assert result["ok"] is True
    assert result["relative_path"] == "GPUPersistentCache/DawnGraphiteCache/cache.db"


def test_profile_outside_and_sensitive_paths_are_rejected(tmp_path):
    profile = tmp_path / "profiles" / "FLOW-001"

    assert is_safe_dawn_cache_path(tmp_path / "other" / "cache.db", profile)["reason"] == "path_outside_profile"
    assert is_safe_dawn_cache_path(profile / "Default" / "Cookies", profile)["reason"] == "sensitive_profile_path"
    assert is_safe_dawn_cache_path(profile / "Default" / "Login Data", profile)["reason"] == "sensitive_profile_path"


def test_classifies_known_and_external_holders():
    current = LockHolder(pid=100, process_name="chrome.exe", process_start_time=10)
    previous = LockHolder(pid=90, process_name="chrome.exe", process_start_time=9)
    worker = LockHolder(pid=50, process_name="python.exe", process_start_time=8)
    external = LockHolder(pid=777, process_name="MsMpEng.exe", process_start_time=7)

    assert classify_lock_holder(current, current_attempt_pids={100}, previous_attempt_pids={90}, worker_pids={50}) == "current_attempt_chrome"
    assert classify_lock_holder(previous, current_attempt_pids={100}, previous_attempt_pids={90}, worker_pids={50}) == "previous_attempt_chrome"
    assert classify_lock_holder(worker, current_attempt_pids={100}, previous_attempt_pids={90}, worker_pids={50}) == "worker_process"
    assert classify_lock_holder(external, current_attempt_pids={100}, previous_attempt_pids={90}, worker_pids={50}) == "external_process"


def test_classifies_child_process_by_parent_pid():
    gpu_child = LockHolder(pid=200, process_name="chrome.exe", parent_pid=100)

    assert classify_lock_holder(gpu_child, current_attempt_pids={100}, previous_attempt_pids=set(), worker_pids=set()) == "current_attempt_chrome"


def test_classifies_pid_reuse_when_start_time_mismatches():
    reused = LockHolder(pid=100, process_name="chrome.exe", process_start_time=222)

    assert (
        classify_lock_holder(
            reused,
            current_attempt_pids={100},
            previous_attempt_pids=set(),
            worker_pids=set(),
            known_process_start_times={100: 111},
        )
        == "pid_reused_or_unknown"
    )


def test_summarizes_multiple_samples_without_expanding_sensitive_data():
    samples = [
        {
            "holder_count": 1,
            "holders": [{"pid": 100, "classification": "current_attempt_chrome"}],
        },
        {
            "holder_count": 1,
            "holders": [{"pid": 777, "classification": "external_process"}],
        },
    ]

    summary = summarize_probe_samples(samples)

    assert summary["dawn_lock_probe_sample_count"] == 2
    assert summary["dawn_lock_probe_holder_count"] == 2
    assert summary["dawn_lock_probe_current_attempt_chrome_detected"] is True
    assert summary["dawn_lock_probe_external_process_detected"] is True
    assert summary["dawn_lock_probe_holder_classification"] == "external_process"


def test_directory_probe_candidates_include_cache_files(tmp_path):
    profile = tmp_path / "profile"
    target = profile / "GPUPersistentCache" / "DawnGraphiteCache" / "abc"
    (target / "cache.db").parent.mkdir(parents=True)
    (target / "cache.db").write_text("cache", encoding="utf-8")

    candidates = probe_candidates(target, profile)

    assert candidates["resource_kind"] == "directory"
    assert target / "cache.db" in candidates["candidates"]


def test_directory_candidates_are_bounded_and_sorted_by_mtime(tmp_path):
    profile = tmp_path / "profile"
    target = profile / "GPUPersistentCache" / "DawnGraphiteCache" / "abc"
    target.mkdir(parents=True)
    for index in range(40):
        item = target / f"cache-{index}.db"
        item.write_text("cache", encoding="utf-8")
        item.touch()

    candidates = probe_candidates(target, profile, max_files=32)

    assert len(candidates["candidates"]) == 32


def test_empty_directory_reports_no_candidate_files(tmp_path):
    profile = tmp_path / "profile"
    target = profile / "GPUPersistentCache" / "DawnGraphiteCache" / "abc"
    target.mkdir(parents=True)

    result = probe_dawn_cache_lock(target, profile, samples=(0.0,), sleep=lambda _: None)

    assert result["summary"]["dawn_lock_probe_holder_count"] is None
    assert result["summary"]["dawn_lock_probe_holder_classification"] == "no_candidate_files"


def test_probe_error_is_not_reported_as_no_holder_found(tmp_path):
    profile = tmp_path / "profile"
    target = profile / "GPUPersistentCache" / "DawnGraphiteCache" / "abc"
    (target / "cache.db").parent.mkdir(parents=True)
    (target / "cache.db").write_text("cache", encoding="utf-8")

    class FailingProbe:
        def holders_for_paths(self, _paths):
            raise OSError(5, "RmGetList failed")

    result = probe_dawn_cache_lock(target, profile, samples=(0.0,), sleep=lambda _: None, probe=FailingProbe())

    assert result["summary"]["dawn_lock_probe_holder_count"] is None
    assert result["summary"]["dawn_lock_probe_holder_classification"] == "probe_error"
    assert result["probe_error_type"] == "OSError"


@pytest.mark.parametrize(
    ("stage", "code"),
    [
        ("RmStartSession", 1001),
        ("RmRegisterResources", 1002),
        ("RmGetList", 1003),
    ],
)
def test_restart_manager_errors_preserve_stage_and_code(tmp_path, stage, code):
    profile = tmp_path / "profile"
    target = profile / "GPUPersistentCache" / "DawnGraphiteCache" / "abc"
    (target / "cache.db").parent.mkdir(parents=True)
    (target / "cache.db").write_text("cache", encoding="utf-8")

    class FailingProbe:
        def holders_for_paths(self, _paths):
            raise RestartManagerProbeError(stage, stage, code)

    result = probe_dawn_cache_lock(target, profile, samples=(0.0,), sleep=lambda _: None, probe=FailingProbe())

    assert result["probe_error_stage"] == stage
    assert result["probe_error_function"] == stage
    assert result["probe_rm_result_code"] == code
    assert result["summary"]["dawn_lock_probe_holder_count"] is None
    assert result["summary"]["dawn_lock_probe_holder_classification"] == "probe_error"


def test_partial_probe_error_is_distinct_from_no_holder_found(tmp_path):
    profile = tmp_path / "profile"
    target = profile / "GPUPersistentCache" / "DawnGraphiteCache" / "abc"
    (target / "cache.db").parent.mkdir(parents=True)
    (target / "cache.db").write_text("cache", encoding="utf-8")

    class FlakyProbe:
        def __init__(self):
            self.calls = 0

        def holders_for_paths(self, _paths):
            self.calls += 1
            if self.calls == 1:
                raise OSError(5, "RmGetList failed")
            return []

    result = probe_dawn_cache_lock(target, profile, samples=(0.0, 0.0), sleep=lambda _: None, probe=FlakyProbe())

    assert result["summary"]["dawn_lock_probe_holder_count"] == 0
    assert result["summary"]["dawn_lock_probe_holder_classification"] == "partial_probe_error"
