from pathlib import Path

import pytest

from runtime.windows_lock_probe import (
    LockHolder,
    classify_lock_holder,
    is_safe_dawn_cache_path,
    probe_candidates,
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
    target = tmp_path / "profile" / "GPUPersistentCache" / "DawnGraphiteCache" / "abc"
    (target / "cache.db").parent.mkdir(parents=True)
    (target / "cache.db").write_text("cache", encoding="utf-8")

    candidates = probe_candidates(target)

    assert candidates[0] == target
    assert target / "cache.db" in candidates
