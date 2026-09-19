"""Worker bootstrap persistence tests."""

from __future__ import annotations

from pathlib import Path
import stat
import threading

import pytest

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


_WATCHDOG_SECONDS = 5.0


def test_run_worker_sets_ready_only_after_recovery_and_stops_on_shutdown(
    private_roots: dict[str, Path], monkeypatch
) -> None:
    state_root = private_roots["state"]
    recovery_started = threading.Event()
    allow_recovery = threading.Event()
    ready = threading.Event()
    shutdown = threading.Event()
    stopped = threading.Event()
    errors: list[BaseException] = []
    store_type = SQLiteStore

    class BlockingStore(store_type):
        def recover_cold_start(self) -> int:
            recovery_started.set()
            assert allow_recovery.wait(_WATCHDOG_SECONDS)
            return super().recover_cold_start()

    monkeypatch.setattr(worker, "SQLiteStore", BlockingStore)

    def run() -> None:
        try:
            worker.run_worker(
                state_root,
                ready_event=ready,
                shutdown_event=shutdown,
                stopped_event=stopped,
            )
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert recovery_started.wait(_WATCHDOG_SECONDS)
        assert not ready.is_set()
        assert not stopped.is_set()

        allow_recovery.set()
        assert ready.wait(_WATCHDOG_SECONDS)
        assert not stopped.is_set()

        shutdown.set()
        assert stopped.wait(_WATCHDOG_SECONDS)
    finally:
        allow_recovery.set()
        shutdown.set()
        thread.join(_WATCHDOG_SECONDS)

    assert not thread.is_alive()
    assert errors == []

    store = SQLiteStore(state_root / "state.db")
    try:
        assert store.worker_epoch() == 1
        assert store.queue_gate() == "paused"
    finally:
        store.close()


@pytest.mark.parametrize("shape", ("relative", "missing", "file", "symlink"))
def test_run_worker_rejects_unsafe_state_root_shapes(
    private_roots: dict[str, Path], tmp_path: Path, shape: str
) -> None:
    if shape == "relative":
        state_root = Path("relative-state")
    elif shape == "missing":
        state_root = tmp_path / "missing-state"
    elif shape == "file":
        state_root = tmp_path / "state-file"
        state_root.write_text("not a directory", encoding="utf-8")
    else:
        state_root = tmp_path / "state-link"
        state_root.symlink_to(private_roots["state"], target_is_directory=True)

    ready = threading.Event()
    shutdown = threading.Event()
    stopped = threading.Event()

    with pytest.raises(worker.WorkerStateError) as failure:
        worker.run_worker(
            state_root,
            ready_event=ready,
            shutdown_event=shutdown,
            stopped_event=stopped,
        )

    assert str(failure.value) == "worker_state_invalid"
    assert not ready.is_set()
    assert stopped.is_set()
    assert list(private_roots["state"].iterdir()) == []


@pytest.mark.parametrize("shape", ("directory", "symlink", "world_readable"))
def test_run_worker_rejects_unsafe_lease_file_shapes(
    private_roots: dict[str, Path], tmp_path: Path, shape: str
) -> None:
    state_root = private_roots["state"]
    lease_path = state_root / ".worker.lock"
    if shape == "directory":
        lease_path.mkdir()
    elif shape == "symlink":
        lease_path.symlink_to(tmp_path / "elsewhere")
    else:
        lease_path.write_text("not private", encoding="utf-8")
        lease_path.chmod(0o644)

    ready = threading.Event()
    shutdown = threading.Event()
    stopped = threading.Event()

    with pytest.raises(worker.WorkerStateError) as failure:
        worker.run_worker(
            state_root,
            ready_event=ready,
            shutdown_event=shutdown,
            stopped_event=stopped,
        )

    assert str(failure.value) == "worker_state_invalid"
    assert not ready.is_set()
    assert stopped.is_set()
    assert list(state_root.iterdir()) == [lease_path]


def test_main_does_not_bypass_an_active_worker_lease(
    private_roots: dict[str, Path]
) -> None:
    state_root = private_roots["state"]
    ready = threading.Event()
    shutdown = threading.Event()
    stopped = threading.Event()
    errors: list[BaseException] = []

    def run() -> None:
        try:
            worker.run_worker(
                state_root,
                ready_event=ready,
                shutdown_event=shutdown,
                stopped_event=stopped,
            )
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert ready.wait(_WATCHDOG_SECONDS)
        assert worker.main() == 1
        store = SQLiteStore(state_root / "state.db")
        try:
            assert store.worker_epoch() == 1
        finally:
            store.close()
    finally:
        shutdown.set()
        assert stopped.wait(_WATCHDOG_SECONDS)
        thread.join(_WATCHDOG_SECONDS)

    assert not thread.is_alive()
    assert errors == []


def test_run_worker_rejects_a_nonprivate_state_root(
    private_roots: dict[str, Path]
) -> None:
    state_root = private_roots["state"]
    state_root.chmod(0o755)
    ready = threading.Event()
    shutdown = threading.Event()
    stopped = threading.Event()

    errors: list[BaseException] = []

    def run() -> None:
        try:
            worker.run_worker(
                state_root,
                ready_event=ready,
                shutdown_event=shutdown,
                stopped_event=stopped,
            )
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert stopped.wait(_WATCHDOG_SECONDS)
    finally:
        shutdown.set()
        thread.join(_WATCHDOG_SECONDS)

    assert not thread.is_alive()
    assert not ready.is_set()
    assert len(errors) == 1
    assert isinstance(errors[0], worker.WorkerStateError)
    assert str(errors[0]) == "worker_state_invalid"
    assert list(state_root.iterdir()) == []


def test_run_worker_creates_a_private_regular_lease_file(
    private_roots: dict[str, Path]
) -> None:
    state_root = private_roots["state"]
    ready = threading.Event()
    shutdown = threading.Event()
    stopped = threading.Event()
    errors: list[BaseException] = []

    def run() -> None:
        try:
            worker.run_worker(
                state_root,
                ready_event=ready,
                shutdown_event=shutdown,
                stopped_event=stopped,
            )
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert ready.wait(_WATCHDOG_SECONDS)
        lease = state_root / ".worker.lock"
        lease_stat = lease.lstat()
        assert stat.S_ISREG(lease_stat.st_mode)
        assert stat.S_IMODE(lease_stat.st_mode) == 0o600
    finally:
        shutdown.set()
        assert stopped.wait(_WATCHDOG_SECONDS)
        thread.join(_WATCHDOG_SECONDS)

    assert not thread.is_alive()
    assert errors == []
