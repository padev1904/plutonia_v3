from __future__ import annotations

import os
from pathlib import Path


def _repo_root() -> Path:
    explicit = str(os.getenv("PLUTONIA_ROOT", "")).strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    return Path(__file__).resolve().parent.parent


PROJECT_ROOT = _repo_root()
DATA_DIR = Path(os.getenv("PLUTONIA_DATA_DIR", str(PROJECT_ROOT / "data"))).expanduser().resolve()
LOG_DIR = Path(os.getenv("PLUTONIA_LOG_DIR", str(PROJECT_ROOT / "logs"))).expanduser().resolve()
RUN_DIR = Path(os.getenv("PLUTONIA_RUN_DIR", str(PROJECT_ROOT / "run"))).expanduser().resolve()
BACKUP_DIR = Path(os.getenv("PLUTONIA_BACKUP_DIR", str(PROJECT_ROOT / "backups"))).expanduser().resolve()
STATIC_DIR = Path(os.getenv("PLUTONIA_STATIC_DIR", str(PROJECT_ROOT / "staticfiles"))).expanduser().resolve()
REVIEW_DIR = Path(os.getenv("PLUTONIA_REVIEW_DIR", str(DATA_DIR / "review"))).expanduser().resolve()
WATCHDOG_STATUS_FILE = Path(
    os.getenv("WATCHDOG_STATUS_FILE", str(REVIEW_DIR / "ops_watchdog_status.json"))
).expanduser().resolve()


def ensure_runtime_dirs() -> None:
    for path in (DATA_DIR, LOG_DIR, RUN_DIR, BACKUP_DIR, STATIC_DIR, REVIEW_DIR):
        path.mkdir(parents=True, exist_ok=True)
