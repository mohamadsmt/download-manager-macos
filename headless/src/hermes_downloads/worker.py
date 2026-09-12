"""Safe bootstrap console entry point for the download worker."""

from __future__ import annotations

import os
from pathlib import Path

from hermes_downloads.store import SQLiteStore


_STATE_DATABASE_NAME = "state.db"


def main() -> int:
    state_root = Path(os.environ["HERMES_DOWNLOADS_STATE_ROOT"])
    store = SQLiteStore(state_root / _STATE_DATABASE_NAME)
    try:
        store.initialize_cold_start()
    finally:
        store.close()
    return 0
