"""Runtime path defaults for the local Flow Gateway POC."""
from __future__ import annotations

from pathlib import Path


FLOWKIT_DIR = Path(__file__).resolve().parents[1]
POC_ROOT = FLOWKIT_DIR.parent
PROFILES_ROOT = POC_ROOT / "profiles"
DATA_ROOT = POC_ROOT / "data"
OUTPUTS_ROOT = POC_ROOT / "outputs"
EXTENSION_DIR = FLOWKIT_DIR / "extension"
REGISTRY_DB_PATH = DATA_ROOT / "runtime_registry.db"
WORKERS_JSON_PATH = FLOWKIT_DIR / "gateway" / "workers.json"

