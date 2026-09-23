"""Safe bootstrap console entry point and durable worker lifecycle."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path
import stat
import threading
from typing import Final, Protocol

from hermes_downloads.ipc import (
    DirectEngineActivateCommand,
    DirectEngineActivateResult,
    HealthServer,
    IPCError,
    IPCStateError,
    JobControlCommand,
    JobControlResult,
    JobsPage,
    PublicJobRecord,
    QueueGateCommand,
    QueueGateResult,
    WorkerHealth,
    validate_available_socket_path,
)
from hermes_downloads.processes import ProcessBirthIdentity
from hermes_downloads.store import (
    DirectEngineActivationFence,
    DirectEngineRecord,
    RequestConflictError,
    SQLiteStore,
)

__all__ = ["WorkerStateError", "main", "run_worker", "worker_busy"]

_STATE_DATABASE_NAME: Final = "state.db"
_LEASE_FILE_NAME: Final = ".worker.lock"
_IPC_SOCKET_FILE_NAME: Final = "worker.sock"
_DIRECT_RUNTIME_DIRECTORY_NAME: Final = "direct-runtime"
_WORKER_STATE_INVALID: Final = "worker_state_invalid"
worker_busy: Final = "worker_busy"
_LEASE_FLAGS: Final = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC
_LEASE_MODE: Final = 0o600
_IPC_POLL_SECONDS: Final = 0.05


class _LifecycleEvent(Protocol):
    def set(self) -> None: ...

    def wait(self, timeout: float | None = None) -> bool: ...


class _DirectController(Protocol):
    def start(self) -> object: ...

    def close(self) -> None: ...

    def discard_absent(self) -> None: ...


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


def _jobs_page_from_store(store: SQLiteStore, cursor: str | None) -> JobsPage:
    jobs = tuple(
        PublicJobRecord(
            job=job.job,
            generation=job.generation,
            revision=job.revision,
            state=job.state,
        )
        for job in store.list_job_page(cursor=cursor)
    )
    return JobsPage(
        jobs=jobs,
        next_cursor=jobs[-1].job if len(jobs) == 100 else None,
    )


def _queue_gate_from_store(
    store: SQLiteStore, command: QueueGateCommand
) -> QueueGateResult:
    result = store.apply_queue_gate(
        gate=command.gate,
        request_id=command.request_id,
        payload_digest=command.payload_digest,
        expected_revision=command.expected_revision,
    )
    return QueueGateResult(
        applied=result.applied,
        queue_gate=result.gate,
        revision=result.revision,
    )


def _job_control_from_store(
    store: SQLiteStore, command: JobControlCommand
) -> JobControlResult:
    """Bridge a closed IPC command to its durable public readback only."""

    try:
        result = store.apply_job_control(
            job_id=command.job,
            action=command.action,
            request_id=command.request_id,
            payload_digest=command.payload_digest,
            expected_revision=command.expected_revision,
        )
    except RequestConflictError:
        raise
    except (TypeError, ValueError):
        raise IPCError("invalid_request") from None
    return JobControlResult(
        status=result.status,
        job=result.job,
        generation=result.generation,
        revision=result.revision,
        state=result.state,
        authorized=result.authorized,
    )


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
    direct_controller: _DirectController | None = None
    direct_fence: DirectEngineActivationFence | None = None
    direct_record: DirectEngineRecord | None = None
    direct_record_persisted = False
    direct_controller_ready = False
    direct_controller_absent_discard_failed = False
    direct_controller_absent_local_cleanup_complete = False
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

        def clear_owned_direct_claim() -> None:
            nonlocal direct_fence, direct_record, direct_record_persisted

            if direct_record_persisted:
                if direct_record is None or not store.clear_direct_engine_record(
                    direct_record
                ):
                    raise IPCStateError("ipc_health_invalid")
                direct_record = None
                direct_record_persisted = False
            elif direct_fence is not None:
                if not store.clear_direct_engine_activation_fence(direct_fence):
                    raise IPCStateError("ipc_health_invalid")
                direct_fence = None

        def clear_owned_direct_controller_state() -> None:
            nonlocal direct_controller, direct_fence, direct_record
            nonlocal direct_record_persisted, direct_controller_ready
            nonlocal direct_controller_absent_discard_failed
            nonlocal direct_controller_absent_local_cleanup_complete

            direct_controller = None
            direct_fence = None
            direct_record = None
            direct_record_persisted = False
            direct_controller_ready = False
            direct_controller_absent_discard_failed = False
            direct_controller_absent_local_cleanup_complete = False

        def close_owned_direct_controller() -> None:
            controller = direct_controller
            if controller is None:
                return
            controller.close()
            clear_owned_direct_claim()
            clear_owned_direct_controller_state()

        def discard_absent_owned_direct_controller() -> None:
            nonlocal direct_controller_absent_discard_failed
            nonlocal direct_controller_absent_local_cleanup_complete

            controller = direct_controller
            if controller is None:
                raise IPCStateError("ipc_health_invalid")
            controller.discard_absent()
            direct_controller_absent_discard_failed = False
            direct_controller_absent_local_cleanup_complete = True
            clear_owned_direct_claim()
            clear_owned_direct_controller_state()

        def cleanup_owned_direct_candidate() -> None:
            if direct_controller is None:
                clear_owned_direct_claim()
            else:
                close_owned_direct_controller()

        def shutdown_owned_direct_controller() -> None:
            nonlocal direct_controller_absent_discard_failed

            if direct_controller_absent_local_cleanup_complete:
                clear_owned_direct_claim()
                clear_owned_direct_controller_state()
                return
            if direct_controller is None:
                return
            if not direct_record_persisted:
                close_owned_direct_controller()
                return
            owned_record = direct_record
            if owned_record is None:
                raise IPCStateError("ipc_health_invalid")
            from hermes_downloads.processes import reconcile_process_birth

            reconciliation = reconcile_process_birth(owned_record.identity)
            if reconciliation == "current":
                close_owned_direct_controller()
                return
            if reconciliation != "absent":
                return
            try:
                discard_absent_owned_direct_controller()
            except BaseException:
                if not direct_controller_absent_local_cleanup_complete:
                    direct_controller_absent_discard_failed = True
                raise

        def direct_engine_activate(
            command: DirectEngineActivateCommand,
        ) -> DirectEngineActivateResult:
            nonlocal direct_controller, direct_fence, direct_record
            nonlocal direct_record_persisted, direct_controller_ready
            nonlocal direct_controller_absent_discard_failed
            nonlocal direct_controller_absent_local_cleanup_complete

            current_epoch = store.worker_epoch()
            if current_epoch is None:
                raise IPCStateError("ipc_health_invalid")
            if command.expected_worker_epoch != current_epoch:
                return DirectEngineActivateResult(
                    worker_epoch=current_epoch, status="stale_epoch"
                )
            if direct_controller is not None:
                if (
                    direct_controller_absent_discard_failed
                    or direct_controller_absent_local_cleanup_complete
                ):
                    return DirectEngineActivateResult(
                        worker_epoch=current_epoch, status="blocked"
                    )
                owned_record = direct_record
                if (
                    not direct_controller_ready
                    or direct_fence is not None
                    or not direct_record_persisted
                    or owned_record is None
                ):
                    return DirectEngineActivateResult(
                        worker_epoch=current_epoch, status="blocked"
                    )
                if owned_record.worker_epoch != current_epoch:
                    return DirectEngineActivateResult(
                        worker_epoch=current_epoch, status="blocked"
                    )
                from hermes_downloads.processes import reconcile_process_birth

                reconciliation = reconcile_process_birth(owned_record.identity)
                if reconciliation == "current":
                    return DirectEngineActivateResult(
                        worker_epoch=current_epoch, status="active"
                    )
                if reconciliation != "absent":
                    return DirectEngineActivateResult(
                        worker_epoch=current_epoch, status="blocked"
                    )
                try:
                    discard_absent_owned_direct_controller()
                except BaseException:
                    if not direct_controller_absent_local_cleanup_complete:
                        direct_controller_absent_discard_failed = True
                    return DirectEngineActivateResult(
                        worker_epoch=current_epoch, status="blocked"
                    )

            if direct_fence is not None:
                return DirectEngineActivateResult(
                    worker_epoch=current_epoch, status="blocked"
                )

            existing_record = store.get_direct_engine_record()
            if existing_record is not None:
                from hermes_downloads.processes import reconcile_process_birth

                if reconcile_process_birth(existing_record.identity) != "absent":
                    return DirectEngineActivateResult(
                        worker_epoch=current_epoch, status="blocked"
                    )
                if not store.clear_direct_engine_record(existing_record):
                    return DirectEngineActivateResult(
                        worker_epoch=current_epoch, status="blocked"
                    )

            existing_fence = store.get_direct_engine_activation_fence()
            if existing_fence is not None:
                return DirectEngineActivateResult(
                    worker_epoch=current_epoch, status="blocked"
                )
            fence = store.reserve_direct_engine_activation(worker_epoch=current_epoch)
            if fence is None:
                return DirectEngineActivateResult(
                    worker_epoch=current_epoch, status="blocked"
                )
            direct_fence = fence

            def on_engine_bound(identity: ProcessBirthIdentity) -> None:
                nonlocal direct_fence, direct_record, direct_record_persisted

                fence = direct_fence
                if fence is None:
                    raise IPCStateError("ipc_health_invalid")
                record = DirectEngineRecord(
                    worker_epoch=current_epoch,
                    identity=identity,
                )
                store.bind_direct_engine_activation_fence(fence, record)
                direct_record = record
                direct_record_persisted = True
                direct_fence = None

            try:
                from hermes_downloads.direct import DirectAria2Controller

                candidate = DirectAria2Controller(
                    runtime_root=root / _DIRECT_RUNTIME_DIRECTORY_NAME,
                    on_engine_bound=on_engine_bound,
                )
                direct_controller = candidate
                direct_record = None
                direct_record_persisted = False
                direct_controller_ready = False
                direct_controller_absent_discard_failed = False
                direct_controller_absent_local_cleanup_complete = False
                candidate.start()
            except BaseException:
                try:
                    cleanup_owned_direct_candidate()
                except BaseException:
                    pass
                raise
            if (
                direct_fence is not None
                or direct_record is None
                or not direct_record_persisted
            ):
                try:
                    cleanup_owned_direct_candidate()
                except BaseException:
                    pass
                raise IPCStateError("ipc_health_invalid")
            direct_controller_ready = True
            return DirectEngineActivateResult(worker_epoch=current_epoch, status="active")

        if requested_socket_path is not None:
            try:
                health_server = HealthServer(
                    requested_socket_path,
                    health=lambda: _health_from_store(store),
                    jobs_page=lambda cursor: _jobs_page_from_store(store, cursor),
                    queue_gate=lambda command: _queue_gate_from_store(store, command),
                    job_control=lambda command: _job_control_from_store(store, command),
                    direct_engine_activate=direct_engine_activate,
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
                if direct_controller is not None:
                    shutdown_owned_direct_controller()
            finally:
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
