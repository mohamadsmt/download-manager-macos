"""Safe bootstrap console entry point and durable worker lifecycle."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path
import stat
import threading
from typing import Final, Protocol

from hermes_downloads.ipc import (
    HealthServer,
    IPCStateError,
    WorkerHealth,
    validate_available_socket_path,
)
from hermes_downloads.store import SQLiteStore

__all__ = ["WorkerStateError", "main", "run_worker", "worker_busy"]

_STATE_DATABASE_NAME: Final = "state.db"
_LEASE_FILE_NAME: Final = ".worker.lock"
_IPC_SOCKET_FILE_NAME: Final = "worker.sock"
_WORKER_STATE_INVALID: Final = "worker_state_invalid"
worker_busy: Final = "worker_busy"
_LEASE_FLAGS: Final = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC
_LEASE_MODE: Final = 0o600
_IPC_POLL_SECONDS: Final = 0.05


class _LifecycleEvent(Protocol):
    def set(self) -> None: ...

    def wait(self, timeout: float | None = None) -> bool: ...


class WorkerStateError(ValueError):
    """Raised when a configured worker state root or lease is unsafe."""

    def __init__(self) -> None:
        super().__init__(_WORKER_STATE_INVALID)


def _validate_state_root(state_root: str | Path) -> Path:
    try:
        root = Path(state_root)
    except (TypeError, ValueError):
        raise WorkerStateError from None
    if not root.is_absolute():
        raise WorkerStateError
    try:
        root_stat = root.lstat()
    except OSError:
        raise WorkerStateError from None
    if (
        stat.S_ISLNK(root_stat.st_mode)
        or not stat.S_ISDIR(root_stat.st_mode)
        or root_stat.st_uid != os.geteuid()
        or stat.S_IMODE(root_stat.st_mode) & 0o077
    ):
        raise WorkerStateError
    return root


def _is_private_regular_lease(lease_stat: os.stat_result) -> bool:
    return (
        stat.S_ISREG(lease_stat.st_mode)
        and stat.S_IMODE(lease_stat.st_mode) == _LEASE_MODE
        and lease_stat.st_nlink == 1
        and lease_stat.st_uid == os.geteuid()
    )


def _close_descriptor(descriptor: int) -> None:
    try:
        os.close(descriptor)
    except OSError:
        pass


def _acquire_worker_lease(state_root: Path) -> int | None:
    lease_path = state_root / _LEASE_FILE_NAME
    try:
        existing = lease_path.lstat()
    except FileNotFoundError:
        pass
    except OSError:
        raise WorkerStateError from None
    else:
        if not _is_private_regular_lease(existing):
            raise WorkerStateError

    try:
        descriptor = os.open(lease_path, _LEASE_FLAGS, _LEASE_MODE)
    except OSError:
        raise WorkerStateError from None
    try:
        if not _is_private_regular_lease(os.fstat(descriptor)):
            raise WorkerStateError
    except BaseException:
        _close_descriptor(descriptor)
        raise

    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        _close_descriptor(descriptor)
        return None
    except OSError:
        _close_descriptor(descriptor)
        raise WorkerStateError from None
    return descriptor


def _release_worker_lease(descriptor: int) -> None:
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        except OSError:
            pass
    finally:
        _close_descriptor(descriptor)


def _validate_ipc_socket_path(state_root: Path, socket_path: str | Path) -> Path:
    try:
        path = Path(socket_path)
    except (TypeError, ValueError):
        raise WorkerStateError from None
    if path != state_root / _IPC_SOCKET_FILE_NAME:
        raise WorkerStateError
    return path


def _health_from_store(store: SQLiteStore) -> WorkerHealth:
    epoch = store.worker_epoch()
    queue_gate = store.queue_gate()
    if epoch is None or queue_gate is None:
        raise IPCStateError("ipc_health_invalid")
    return WorkerHealth(worker_epoch=epoch, queue_gate=queue_gate)


def run_worker(
    state_root: str | Path,
    *,
    socket_path: str | Path | None = None,
    ready_event: _LifecycleEvent,
    shutdown_event: _LifecycleEvent,
    stopped_event: _LifecycleEvent,
) -> str | None:
    """Recover one exclusively owned paused worker until shutdown is requested."""

    lease_descriptor: int | None = None
    store: SQLiteStore | None = None
    health_server: HealthServer | None = None
    try:
        root = _validate_state_root(state_root)
        requested_socket_path: Path | None = None
        if socket_path is not None:
            try:
                requested_socket_path = validate_available_socket_path(
                    _validate_ipc_socket_path(root, socket_path)
                )
            except IPCStateError:
                raise WorkerStateError from None
        lease_descriptor = _acquire_worker_lease(root)
        if lease_descriptor is None:
            return worker_busy

        store = SQLiteStore(root / _STATE_DATABASE_NAME)
        store.recover_cold_start()
        if requested_socket_path is not None:
            try:
                health_server = HealthServer(
                    requested_socket_path,
                    health=lambda: _health_from_store(store),
                )
            except IPCStateError:
                raise WorkerStateError from None
        ready_event.set()
        if health_server is None:
            shutdown_event.wait()
        else:
            while not shutdown_event.wait(_IPC_POLL_SECONDS):
                health_server.serve_once()
        return None
    finally:
        try:
            try:
                if health_server is not None:
                    health_server.close()
            finally:
                if store is not None:
                    store.close()
        finally:
            try:
                if lease_descriptor is not None:
                    _release_worker_lease(lease_descriptor)
            finally:
                stopped_event.set()


def main() -> int:
    """Run one lock-respecting cold-start bootstrap without scheduling work."""

    shutdown_event = threading.Event()
    shutdown_event.set()
    outcome = run_worker(
        Path(os.environ["HERMES_DOWNLOADS_STATE_ROOT"]),
        ready_event=threading.Event(),
        shutdown_event=shutdown_event,
        stopped_event=threading.Event(),
    )
    return 1 if outcome == worker_busy else 0
