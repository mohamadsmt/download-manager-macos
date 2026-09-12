"""Worker bootstrap persistence tests."""

from __future__ import annotations

from pathlib import Path

from hermes_downloads import worker
from hermes_downloads.store import SQLiteStore


def test_worker_entrypoint_persists_paused_gate_in_configured_state_root(
    private_roots: dict[str, Path],
) -> None:
    state_root = private_roots["state"]
    database_path = state_root / "state.db"

    assert list(state_root.iterdir()) == []
    assert worker.main() == 0
    assert database_path.is_file()
    assert list(private_roots["home"].iterdir()) == []
    assert list(private_roots["hermes_home"].iterdir()) == []
    assert list(private_roots["output"].iterdir()) == []

    store = SQLiteStore(database_path)
    try:
        assert store.queue_gate() == "paused"
    finally:
        store.close()
