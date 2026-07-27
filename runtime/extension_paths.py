"""Chrome extension path handling for Runtime-owned Chrome instances."""
from __future__ import annotations

import os
from pathlib import Path


EXTRA_EXTENSION_DIRS_ENV = "FLOW_EXTRA_EXTENSION_DIRS"


def runtime_extension_dirs(required_extension_dir: Path | str, extra_dirs_value: str | None = None) -> list[Path]:
    raw_paths = [Path(required_extension_dir)]
    value = os.environ.get(EXTRA_EXTENSION_DIRS_ENV, "") if extra_dirs_value is None else extra_dirs_value
    for item in value.split(os.pathsep):
        item = item.strip()
        if item:
            raw_paths.append(Path(item))
    result: list[Path] = []
    seen: set[str] = set()
    for path in raw_paths:
        if _looks_like_url(str(path)):
            raise ValueError(f"extension_dir_must_be_local_path:{path}")
        resolved = path.resolve()
        key = str(resolved).casefold()
        if key in seen:
            continue
        if not (resolved / "manifest.json").is_file():
            raise FileNotFoundError(f"extension_manifest_missing:{resolved}")
        seen.add(key)
        result.append(resolved)
    return result


def chrome_extension_arg_value(required_extension_dir: Path | str, extra_dirs_value: str | None = None) -> str:
    return ",".join(str(path) for path in runtime_extension_dirs(required_extension_dir, extra_dirs_value))


def chrome_extension_args(required_extension_dir: Path | str, extra_dirs_value: str | None = None) -> list[str]:
    value = chrome_extension_arg_value(required_extension_dir, extra_dirs_value)
    return [
        f"--disable-extensions-except={value}",
        f"--load-extension={value}",
    ]


def _looks_like_url(value: str) -> bool:
    lowered = value.lower()
    return lowered.startswith("http://") or lowered.startswith("https://")
