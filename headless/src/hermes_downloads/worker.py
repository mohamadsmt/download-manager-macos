"""Safe bootstrap console entry point and durable worker lifecycle."""

from __future__ import annotations

import fcntl
from dataclasses import dataclass, replace
import os
from pathlib import Path
from queue import Queue
import stat
import threading
from typing import TYPE_CHECKING, Callable, Final, Protocol
from datetime import UTC, datetime

from hermes_downloads.ipc import (
    AddBatchCommand,
    AddBatchResult,
    DirectEngineActivateCommand,
    DirectEngineActivateResult,
    DirectJobDispatchCommand,
    DirectJobDispatchResult,
    HealthServer,
    IPCError,
    IPCStateError,
    JobAddCommand,
    JobAddResult,
    JobControlCommand,
    JobControlResult,
    JobsPage,
    PublicJobRecord,
    QueueGateCommand,
    QueueGateResult,
    WorkerHealth,
    validate_available_socket_path,
)
from hermes_downloads.models import Admission, DownloadIntent, MaterializedJob, SourceKind
from hermes_downloads.network import SourceURL, validate_source_url
from hermes_downloads.processes import ProcessBirthIdentity, reconcile_process_birth
from hermes_downloads.store import (
    DirectEngineActivationFence,
    DirectDispatchResult,
    DirectEngineRecord,
    RequestConflictError,
    SQLiteStore,
    _DirectDispatchPlan,
    _DirectEngineRecoveryCapability,
    _DirectPublicationReconciliationPlan,
    _DirectTerminalPlan,
    _DirectStagePlan,
    _DirectPublicationAttempt,
    _DirectAttemptRecoveryPlan,
)

if TYPE_CHECKING:
    from hermes_downloads.direct import DirectTransfer
    from hermes_downloads.paths import DestinationIntent, StagedPartialPayload

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
_OBSERVATION_JOIN_SECONDS: Final = 1.5
# A timed-out observer from an earlier run_worker in this process must drain
# before any later run can construct another controller. Process exit retires
# daemon threads; within a process this single private gate spans worker runs.
_DIRECT_OBSERVATION_LOCK = threading.Lock()
_DIRECT_OBSERVATION_REGISTRY_LOCK = threading.Lock()
_DIRECT_OBSERVATION_THREAD: threading.Thread | None = None


def _retire_exited_direct_observation() -> None:
    """Called with the registry mutex; only a joined thread retires the lane."""
    global _DIRECT_OBSERVATION_THREAD
    thread = _DIRECT_OBSERVATION_THREAD
    if thread is not None and not thread.is_alive():
        thread.join()
        _DIRECT_OBSERVATION_THREAD = None
        _DIRECT_OBSERVATION_LOCK.release()


def _direct_observation_busy() -> bool:
    with _DIRECT_OBSERVATION_REGISTRY_LOCK:
        _retire_exited_direct_observation()
        return _DIRECT_OBSERVATION_LOCK.locked()


def _acquire_direct_observation() -> bool:
    with _DIRECT_OBSERVATION_REGISTRY_LOCK:
        _retire_exited_direct_observation()
        return _DIRECT_OBSERVATION_LOCK.acquire(blocking=False)


def _start_direct_observation(thread: threading.Thread) -> None:
    global _DIRECT_OBSERVATION_THREAD
    with _DIRECT_OBSERVATION_REGISTRY_LOCK:
        if _DIRECT_OBSERVATION_THREAD is not None:
            raise RuntimeError('direct observation is already tracked')
        _DIRECT_OBSERVATION_THREAD = thread
        try:
            thread.start()
        except BaseException:
            _DIRECT_OBSERVATION_THREAD = None
            _DIRECT_OBSERVATION_LOCK.release()
            raise


@dataclass(frozen=True, slots=True, repr=False)
class _DirectObservation:
    """Only immutable evidence crosses from the observer to the SQLite owner."""

    plan: _DirectTerminalPlan
    result: DirectTransfer | None
    failed: bool


@dataclass(frozen=True, slots=True, repr=False)
class _DirectStageObservation:
    """Only attestation evidence crosses back to the live SQLite owner."""

    plan: _DirectStagePlan
    result: StagedPartialPayload | None
    failed: bool


@dataclass(frozen=True, slots=True, repr=False)
class _PublicationObservation:
    plan: object
    result: object
    failed: bool


class _LifecycleEvent(Protocol):
    def set(self) -> None: ...

    def wait(self, timeout: float | None = None) -> bool: ...


class _DirectController(Protocol):
    def start(self) -> object: ...

    def _recovery_capability(self) -> _DirectEngineRecoveryCapability: ...

    def close(self) -> None: ...

    def discard_absent(self) -> None: ...

    def add_paused(
        self,
        *,
        job_id: str,
        generation: int,
        source: SourceURL,
        destination: DestinationIntent,
        expected_sha256: str | None,
        admission: Admission,
    ) -> object: ...

    def resume(
        self, *, job_id: str, generation: int, admission: Admission
    ) -> object: ...

    def observe_terminal(
        self, *, job_id: str, generation: int, gid: str
    ) -> DirectTransfer | None: ...


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


def _require_reconciliation_chain(
    destination: DestinationIntent,
    plan: _DirectPublicationReconciliationPlan,
) -> None:
    """Require exact pre-existing marker, staged, and final inode identities."""

    try:
        marker = os.lstat(destination.incomplete_dir / ".hermes-reservation")
        staged = os.lstat(destination.partial_path)
        final = os.lstat(destination.final_path)
    except OSError as error:
        raise ValueError("published finalization chain is absent") from error
    if (
        not stat.S_ISREG(marker.st_mode)
        or (marker.st_dev, marker.st_ino)
        != (plan.marker.marker_device, plan.marker.marker_inode)
        or not stat.S_ISREG(staged.st_mode)
        or (staged.st_dev, staged.st_ino, staged.st_size)
        != (
            plan.staged.partial_device,
            plan.staged.partial_inode,
            plan.staged.logical_size,
        )
        or not stat.S_ISREG(final.st_mode)
        or (final.st_dev, final.st_ino, final.st_size)
        != (
            plan.staged.partial_device,
            plan.staged.partial_inode,
            plan.staged.logical_size,
        )
    ):
        raise ValueError("published finalization chain does not match durable bindings")


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


def _add_batch_from_store(store: SQLiteStore, command: AddBatchCommand) -> AddBatchResult:
    try:
        return store.apply_add_batch(command)
    except RequestConflictError:
        raise IPCError('command_conflict') from None


def _job_add_from_store(store: SQLiteStore, command: JobAddCommand) -> JobAddResult:
    """Materialize one validated, inactive direct job without activating an engine."""

    try:
        source = validate_source_url(command.source_url)
        intent = DownloadIntent(
            job_id=command.job,
            request_id=command.request_id,
            payload_digest=command.payload_digest,
            source_url=source.raw_url,
            expected_revision=None,
            generation=0,
            revision=0,
        )
        materialized = MaterializedJob(
            job_id=command.job,
            intent=intent,
            source_kind=SourceKind.DIRECT,
            queue_collection_id=None,
            priority=command.priority,
            order_key=command.order_key,
            scheduled_for=None,
            authorized=False,
            manual_hold=False,
            start_now_requested=False,
            category=command.category,
            destination_collection=None,
            partial_filename=command.partial_filename,
            selected_final_filename=command.selected_final_filename,
        )
        result = store.apply_add(intent, materialized=materialized)
    except RequestConflictError:
        raise
    except (TypeError, UnicodeError, ValueError):
        raise IPCError("invalid_request") from None
    return JobAddResult(
        applied=result.applied,
        job=result.job,
        generation=result.generation,
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


def _recover_cold_direct_engine(store: SQLiteStore) -> bool:
    """Retire only a provably owned persisted direct daemon before worker ready.

    ``True`` keeps this worker's future direct activation fail-closed.  The
    normal cold bootstrap stays engine-import-free unless an exact matching
    private capability requires authenticated shutdown.
    """

    if store.get_direct_engine_activation_fence() is not None:
        return True
    record = store.get_direct_engine_record()
    if record is None:
        return False
    try:
        capability = store._get_direct_engine_recovery_capability(record)
    except (TypeError, ValueError):
        return True
    if capability is None:
        return True

    reconciliation = reconcile_process_birth(record.identity)
    if reconciliation == "absent":
        return not store._clear_direct_engine_record_and_recovery_capability(
            record, capability
        )
    if reconciliation != "current":
        return True
    try:
        from hermes_downloads.direct import _recover_owned_direct_engine

        _recover_owned_direct_engine(record.identity, capability)
    except Exception:
        return True
    return not store._clear_direct_engine_record_and_recovery_capability(record, capability)


def run_worker(
    state_root: str | Path,
    *,
    socket_path: str | Path | None = None,
    ready_event: _LifecycleEvent,
    shutdown_event: _LifecycleEvent,
    stopped_event: _LifecycleEvent,
    recover_socket: bool = False,
) -> str | None:
    """Recover one exclusively owned paused worker until shutdown is requested."""

    endpoint_certificate = None
    owned_endpoint_certificate = None
    lease_descriptor: int | None = None
    store: SQLiteStore | None = None
    health_server: HealthServer | None = None
    direct_controller: _DirectController | None = None
    direct_fence: DirectEngineActivationFence | None = None
    direct_record: DirectEngineRecord | None = None
    direct_recovery_capability: _DirectEngineRecoveryCapability | None = None
    direct_record_persisted = False
    direct_recovery_capability_persisted = False
    direct_controller_ready = False
    direct_recovery_blocked = False
    direct_controller_absent_discard_failed = False
    direct_controller_absent_local_cleanup_complete = False
    active_direct_plan: _DirectDispatchPlan | None = None
    active_terminal_plan: _DirectTerminalPlan | None = None
    active_stage_plan: _DirectStagePlan | None = None
    active_preparation: tuple | None = None
    active_publication: _DirectPublicationAttempt | None = None
    publication_stage: _DirectStagePlan | None = None
    publication_permit = None
    active_recovery: _DirectAttemptRecoveryPlan | None = None
    publication_observed = False
    publication_cancel = threading.Event()
    observation_thread: threading.Thread | None = None
    observation_results: Queue = Queue(maxsize=1)
    try:
        root = _validate_state_root(state_root)
        if recover_socket:
            from hermes_downloads import endpoint_ownership
            root = endpoint_ownership.validate_root(state_root)
            if socket_path is None:
                raise WorkerStateError
            endpoint_certificate = endpoint_ownership.preflight(root)
        requested_socket_path: Path | None = None
        if socket_path is not None:
            try:
                requested_socket_path = _validate_ipc_socket_path(root, socket_path)
                if not recover_socket:
                    requested_socket_path = validate_available_socket_path(requested_socket_path)
            except IPCStateError:
                raise WorkerStateError from None
        lease_descriptor = _acquire_worker_lease(root)
        if lease_descriptor is None:
            return worker_busy

        if recover_socket:
            endpoint_ownership.reclaim(root, endpoint_certificate)
        store = SQLiteStore(root / _STATE_DATABASE_NAME)
        store.recover_cold_start()
        direct_recovery_blocked = _recover_cold_direct_engine(store)

        def clear_owned_direct_claim() -> None:
            nonlocal direct_fence, direct_record, direct_recovery_capability
            nonlocal direct_record_persisted, direct_recovery_capability_persisted

            if direct_record_persisted:
                if direct_record is None:
                    raise IPCStateError("ipc_health_invalid")
                if direct_recovery_capability_persisted:
                    if (
                        direct_recovery_capability is None
                        or not store._clear_direct_engine_record_and_recovery_capability(
                            direct_record, direct_recovery_capability
                        )
                    ):
                        raise IPCStateError("ipc_health_invalid")
                    direct_recovery_capability = None
                    direct_recovery_capability_persisted = False
                elif not store.clear_direct_engine_record(direct_record):
                    raise IPCStateError("ipc_health_invalid")
                direct_record = None
                direct_record_persisted = False
            elif direct_fence is not None:
                if not store.clear_direct_engine_activation_fence(direct_fence):
                    raise IPCStateError("ipc_health_invalid")
                direct_fence = None

        def clear_owned_direct_controller_state() -> None:
            nonlocal direct_controller, direct_fence, direct_record
            nonlocal direct_recovery_capability, active_direct_plan
            nonlocal direct_record_persisted, direct_recovery_capability_persisted
            nonlocal direct_controller_ready
            nonlocal direct_controller_absent_discard_failed
            nonlocal direct_controller_absent_local_cleanup_complete
            nonlocal active_terminal_plan, active_stage_plan

            direct_controller = None
            direct_fence = None
            direct_record = None
            direct_recovery_capability = None
            direct_record_persisted = False
            direct_recovery_capability_persisted = False
            direct_controller_ready = False
            direct_controller_absent_discard_failed = False
            direct_controller_absent_local_cleanup_complete = False
            active_direct_plan = None
            active_terminal_plan = None
            active_stage_plan = None

        def close_owned_direct_controller(
            persist_contained: Callable[[], object] | None = None,
        ) -> object:
            nonlocal active_terminal_plan, active_stage_plan, direct_controller_ready, direct_recovery_blocked

            controller = direct_controller
            if controller is None:
                return None if persist_contained is None else persist_contained()
            # Invalidate callbacks before containment. Keep durable authority
            # until both containment and any paused/removed transaction succeed.
            active_terminal_plan = None
            active_stage_plan = None
            direct_controller_ready = False
            try:
                controller.close()
                result = None if persist_contained is None else persist_contained()
                clear_owned_direct_claim()
                clear_owned_direct_controller_state()
                return result
            except BaseException:
                if persist_contained is not None:
                    direct_recovery_blocked = True
                    raise IPCStateError("direct_dispatch_blocked") from None
                raise

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

            if direct_recovery_blocked:
                return
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
            nonlocal direct_recovery_capability
            nonlocal direct_record_persisted, direct_recovery_capability_persisted
            nonlocal direct_controller_ready
            nonlocal direct_controller_absent_discard_failed
            nonlocal direct_controller_absent_local_cleanup_complete

            current_epoch = store.worker_epoch()
            if current_epoch is None:
                raise IPCStateError("ipc_health_invalid")
            if command.expected_worker_epoch != current_epoch:
                return DirectEngineActivateResult(
                    worker_epoch=current_epoch, status="stale_epoch"
                )
            if direct_recovery_blocked:
                return DirectEngineActivateResult(
                    worker_epoch=current_epoch, status="blocked"
                )
            if direct_controller is None and (
                observation_thread is not None or _direct_observation_busy()
            ):
                return DirectEngineActivateResult(
                    worker_epoch=current_epoch, status="blocked"
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
                    or not direct_recovery_capability_persisted
                    or owned_record is None
                    or direct_recovery_capability is None
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
                if observation_thread is not None:
                    # Let the pending readback contain/pause this dispatch;
                    # absence must not replace a controller under its observer.
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
                nonlocal direct_fence, direct_record, direct_recovery_capability
                nonlocal direct_record_persisted, direct_recovery_capability_persisted

                fence = direct_fence
                if fence is None:
                    raise IPCStateError("ipc_health_invalid")
                record = DirectEngineRecord(
                    worker_epoch=current_epoch,
                    identity=identity,
                )
                store.bind_direct_engine_activation_fence(fence, record)
                direct_record = record
                direct_recovery_capability = None
                direct_record_persisted = True
                direct_recovery_capability_persisted = False
                direct_fence = None

            try:
                from hermes_downloads.direct import DirectAria2Controller

                candidate = DirectAria2Controller(
                    runtime_root=root / _DIRECT_RUNTIME_DIRECTORY_NAME,
                    on_engine_bound=on_engine_bound,
                )
                direct_controller = candidate
                direct_record = None
                direct_recovery_capability = None
                direct_record_persisted = False
                direct_recovery_capability_persisted = False
                direct_controller_ready = False
                direct_controller_absent_discard_failed = False
                direct_controller_absent_local_cleanup_complete = False
                candidate.start()
                record = direct_record
                if record is None or not direct_record_persisted:
                    raise IPCStateError("ipc_health_invalid")
                capability = candidate._recovery_capability()
                store._bind_direct_engine_recovery_capability(record, capability)
                direct_recovery_capability = capability
                direct_recovery_capability_persisted = True
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
                or direct_recovery_capability is None
                or not direct_recovery_capability_persisted
            ):
                try:
                    cleanup_owned_direct_candidate()
                except BaseException:
                    pass
                raise IPCStateError("ipc_health_invalid")
            direct_controller_ready = True
            return DirectEngineActivateResult(worker_epoch=current_epoch, status="active")

        def direct_job_dispatch(
            command: DirectJobDispatchCommand,
        ) -> DirectJobDispatchResult:
            """Run one bounded, marker-bound direct admission lifecycle."""

            nonlocal active_direct_plan, active_terminal_plan, active_recovery, active_publication, publication_observed, publication_cancel

            controller = direct_controller
            controller_ready = (
                controller is not None
                and direct_controller_ready
                and not direct_recovery_blocked
                and active_direct_plan is None
                and observation_thread is None
                and not _direct_observation_busy()
            )
            try:
                prepared = store.prepare_direct_dispatch(
                    job_id=command.job,
                    expected_worker_epoch=command.expected_worker_epoch,
                    expected_generation=command.expected_generation,
                    expected_revision=command.expected_revision,
                    request_id=command.request_id,
                    payload_digest=command.payload_digest,
                    controller_ready=controller_ready,
                    now=datetime.now(UTC),
                )
            except RequestConflictError:
                raise
            except (TypeError, ValueError):
                raise IPCError("invalid_request") from None

            if type(prepared) is DirectDispatchResult:
                return DirectJobDispatchResult(
                    status=prepared.status,
                    job=prepared.job,
                    generation=prepared.generation,
                    revision=prepared.revision,
                    state=prepared.state,
                )
            if type(prepared) is _DirectAttemptRecoveryPlan:
                if active_recovery != prepared:
                    if (observation_thread is not None or active_publication is not None
                        or _direct_observation_busy()):
                        result = store.abort_exact_publication_recovery(prepared)
                        return DirectJobDispatchResult(result.status, result.job,
                            result.generation, result.revision, result.state)
                    publication_cancel = threading.Event()
                    active_recovery = prepared
                    active_publication = prepared.attempt
                    publication_observed = False
                attempt = prepared.attempt
                return DirectJobDispatchResult('pending', attempt.job.job_id,
                    attempt.generation, attempt.revision, attempt.state)
            if type(prepared) is _DirectPublicationReconciliationPlan:
                reconciliation = prepared
                try:
                    from hermes_downloads.paths import (
                        StagedPartialPayload,
                        publish_staged_partial_payload,
                        rehydrate_destination,
                    )

                    destination = rehydrate_destination(
                        category=reconciliation.job.category,
                        collection=reconciliation.job.destination_collection,
                        partial_filename=reconciliation.job.partial_filename,
                        selected_final_filename=reconciliation.job.selected_final_filename,
                        job_id=reconciliation.job.job_id,
                    )
                    _require_reconciliation_chain(destination, reconciliation)
                    published = publish_staged_partial_payload(
                        destination,
                        reconciliation.reservation,
                        StagedPartialPayload(
                            path=destination.partial_path,
                            st_dev=reconciliation.staged.partial_device,
                            st_ino=reconciliation.staged.partial_inode,
                            logical_size=reconciliation.staged.logical_size,
                        ),
                        existing_only=True,
                    )
                    _require_reconciliation_chain(destination, reconciliation)
                    result = store.complete_direct_publication_reconciliation(
                        reconciliation,
                        final_device=published.st_dev,
                        final_inode=published.st_ino,
                        logical_size=published.logical_size,
                    )
                except BaseException:
                    try:
                        aborted = store.abort_direct_publication_reconciliation(
                            reconciliation
                        )
                    except BaseException:
                        raise IPCError("direct_dispatch_blocked") from None
                    return DirectJobDispatchResult(
                        status=aborted.status,
                        job=aborted.job,
                        generation=aborted.generation,
                        revision=aborted.revision,
                        state=aborted.state,
                    )
                return DirectJobDispatchResult(
                    status=result.status,
                    job=result.job,
                    generation=result.generation,
                    revision=result.revision,
                    state=result.state,
                )
            if type(prepared) is not _DirectDispatchPlan or controller is None:
                raise IPCError("direct_dispatch_blocked")

            plan = prepared
            try:
                from hermes_downloads.paths import (
                    attest_publication_reservation_marker,
                    prepare_persisted_destination_workspace,
                    rehydrate_destination,
                )

                destination = rehydrate_destination(
                    category=plan.job.category,
                    collection=plan.job.destination_collection,
                    partial_filename=plan.job.partial_filename,
                    selected_final_filename=plan.job.selected_final_filename,
                    job_id=plan.job.job_id,
                )
                destination = prepare_persisted_destination_workspace(destination)
                marker = attest_publication_reservation_marker(
                    destination, plan.reservation
                )
                binding = store.bind_publication_marker(
                    plan.job.job_id,
                    claim_token=plan.reservation.claim_token,
                    marker_device=marker.st_dev,
                    marker_inode=marker.st_ino,
                )
                if (
                    binding.marker_device != marker.st_dev
                    or binding.marker_inode != marker.st_ino
                ):
                    raise ValueError("publication marker binding does not match")

                source = validate_source_url(plan.job.intent.source_url)
                if source.raw_url != plan.job.intent.source_url:
                    raise ValueError("persisted source bytes changed during validation")
                paused_transfer = controller.add_paused(
                    job_id=plan.job.job_id,
                    generation=plan.generation,
                    source=source,
                    destination=destination,
                    expected_sha256=None,
                    admission=plan.admission,
                )
                if getattr(paused_transfer, "status", None) != "paused":
                    raise ValueError("direct engine did not remain paused")
                plan = store.advance_direct_dispatch_to_downloading(plan)
                controller.resume(
                    job_id=plan.job.job_id,
                    generation=plan.generation,
                    admission=plan.admission,
                )
                active_direct_plan = plan
                if direct_record is None:
                    raise ValueError("direct engine ownership is absent")
                active_terminal_plan = _DirectTerminalPlan(
                    dispatch=plan,
                    record=direct_record,
                    marker=binding,
                    gid=paused_transfer.gid,
                    partial_path=destination.partial_path,
                    capability=direct_recovery_capability,
                )
                result = store.finish_direct_dispatch(plan)
            except BaseException:
                # No failed dispatch leaves a live body-capable controller.  The
                # durable pending receipt becomes a fail-closed paused result.
                try:
                    aborted = close_owned_direct_controller(
                        lambda: store.abort_direct_dispatch(plan)
                    )
                except BaseException:
                    raise IPCError("direct_dispatch_blocked") from None
                return DirectJobDispatchResult(
                    status=aborted.status,
                    job=aborted.job,
                    generation=aborted.generation,
                    revision=aborted.revision,
                    state=aborted.state,
                )
            return DirectJobDispatchResult(
                status=result.status,
                job=result.job,
                generation=result.generation,
                revision=result.revision,
                state=result.state,
            )

        def revoke_publication() -> bool:
            """Invalidate completion promptly; a syscall must quiesce before ack."""
            nonlocal active_preparation, publication_stage, active_recovery
            publication_cancel.set()
            recoverable = active_recovery is not None
            if publication_permit is not None:
                snapshot = publication_permit.revoke()
                if snapshot.in_flight:
                    raise IPCError('direct_dispatch_blocked')
                recoverable = snapshot.creation_attempted
            active_preparation = None
            publication_stage = None
            active_recovery = None
            return recoverable

        def job_control(command: JobControlCommand) -> JobControlResult:
            """Contain the one active body before acknowledging pause/removal."""

            nonlocal active_publication
            plan = active_direct_plan
            attempt = active_publication
            tracked_job = plan.job.job_id if plan is not None else (attempt.job.job_id if attempt else None)
            if (
                command.job == tracked_job
                and command.action in {"pause", "remove"}
                and store.publication_control_is_current(job_id=command.job,
                    action=command.action, request_id=command.request_id,
                    payload_digest=command.payload_digest, expected_revision=command.expected_revision)
            ):
                recoverable = revoke_publication()
                try:
                    result = close_owned_direct_controller(lambda: store.apply_job_control(
                        job_id=command.job,
                        action=command.action,
                        request_id=command.request_id,
                        payload_digest=command.payload_digest,
                        expected_revision=command.expected_revision,
                        _contained_direct_transfer=True,
                        _publication_recoverable=recoverable,
                    ))
                except RequestConflictError:
                    raise
                except (TypeError, ValueError):
                    raise IPCError("direct_dispatch_blocked") from None
                active_publication = None
                return JobControlResult(
                    status=result.status,
                    job=result.job,
                    generation=result.generation,
                    revision=result.revision,
                    state=result.state,
                    authorized=result.authorized,
                )
            return _job_control_from_store(store, command)

        def queue_gate(command: QueueGateCommand) -> QueueGateResult:
            """Contain the one active body before a durable queue pause reply."""

            nonlocal active_publication
            plan = active_direct_plan
            attempt = active_publication
            snapshot = store.queue_gate_snapshot()
            if (
                command.gate == "paused"
                and (plan is not None or attempt is not None)
                and snapshot is not None
                and store.publication_queue_control_is_current(request_id=command.request_id,
                    payload_digest=command.payload_digest, expected_revision=command.expected_revision)
            ):
                recoverable = revoke_publication()
                job_id = plan.job.job_id if plan is not None else attempt.job.job_id
                generation = plan.generation if plan is not None else attempt.generation
                revision = plan.revision if plan is not None else attempt.revision
                try:
                    result = close_owned_direct_controller(lambda: store.apply_queue_gate(
                        gate=command.gate, request_id=command.request_id,
                        payload_digest=command.payload_digest, expected_revision=command.expected_revision,
                        _contained_direct_job=(job_id,generation,revision,recoverable),
                    ))
                except (TypeError, ValueError):
                    raise IPCError("direct_dispatch_blocked") from None
                active_publication = None
                return QueueGateResult(applied=result.applied, queue_gate=result.gate, revision=result.revision)
            return _queue_gate_from_store(store, command)

        def poll_direct_terminal() -> None:
            """Reap/launch one terminal or stage observation; persist on this thread."""

            nonlocal observation_thread, active_direct_plan, active_terminal_plan
            nonlocal active_stage_plan
            nonlocal active_preparation, active_publication, publication_permit, publication_stage, active_recovery
            nonlocal publication_observed, publication_cancel

            if observation_thread is not None:
                if observation_thread.is_alive():
                    return
                observation_thread.join()
                observation_thread = None
                _direct_observation_busy()
                observation = observation_results.get_nowait()
                if type(observation) is _PublicationObservation:
                    if observation.plan is active_preparation:
                        preparation = active_preparation
                        active_preparation = None
                        if not observation.failed:
                            try:
                                active_publication = store.reserve_direct_publication(preparation[0], observation.result)
                                from hermes_downloads.paths import PublicationCreationPermit
                                publication_permit = PublicationCreationPermit()
                                publication_stage = preparation[0]
                                publication_observed = False
                            except Exception:
                                observation = _PublicationObservation(preparation, None, True)
                        if observation.failed:
                            tracked = active_direct_plan
                            close_owned_direct_controller(lambda: store.pause_active_direct_job(
                                job_id=tracked.job.job_id, generation=tracked.generation, revision=tracked.revision))
                    elif observation.plan is active_publication:
                        attempt = active_publication
                        recovery = active_recovery
                        valid = recovery is not None or (publication_stage is not None
                            and publication_permit is not None and not publication_permit.snapshot().revoked)
                        publication_observed = True
                        if valid:
                            try:
                                if observation.failed:
                                    raise ValueError('publication observation failed')
                                store.complete_direct_publication(attempt, observation.result,
                                    initial_stage=publication_stage if recovery is None else None)
                            except Exception:
                                if recovery is not None:
                                    store.abort_exact_publication_recovery(recovery)
                                else:
                                    recoverable = publication_permit.snapshot().creation_attempted
                                    close_owned_direct_controller(lambda: store.pause_active_direct_job(
                                        job_id=attempt.job.job_id, generation=attempt.generation,
                                        revision=attempt.revision, _publication_recoverable=recoverable))
                            else:
                                # Completion is durable before retiring corresponding engine claims.
                                close_owned_direct_controller()
                            active_publication = None
                            publication_stage = None
                            publication_permit = None
                            active_recovery = None
                elif type(observation) is _DirectStageObservation:
                    if observation.plan is active_stage_plan:
                        stage = observation.plan
                        failed = observation.failed
                        if not failed and observation.result is not None:
                            try:
                                store.bind_direct_staged_payload(stage, observation.result)
                            except Exception:
                                failed = True
                            else:
                                active_stage_plan = None
                                publication_cancel = threading.Event()
                                active_preparation = (stage, observation.result)
                        if failed:
                            current = store.get_job(stage.job.job_id)
                            if current is not None and (
                                current.generation, current.revision, current.state
                            ) == (stage.terminal.dispatch.generation, stage.revision, "finalizing"):
                                close_owned_direct_controller(lambda: store.pause_active_direct_job(
                                    job_id=stage.job.job_id,
                                    generation=stage.terminal.dispatch.generation,
                                    revision=stage.revision,
                                ))
                            else:
                                close_owned_direct_controller()
                elif observation.plan is active_terminal_plan:
                    plan = observation.plan.dispatch
                    current = store.get_job(plan.job.job_id)
                    if current is None or (
                        current.generation, current.revision, current.state
                    ) != (plan.generation, plan.revision, "downloading"):
                        close_owned_direct_controller()
                    else:
                        failed = observation.failed
                        if not failed and observation.result is not None:
                            try:
                                marker = (observation.plan.partial_path.parent / ".hermes-reservation").lstat()
                                if (
                                    not stat.S_ISREG(marker.st_mode)
                                    or marker.st_nlink != 1
                                    or (marker.st_dev, marker.st_ino) != (
                                        observation.plan.marker.marker_device,
                                        observation.plan.marker.marker_inode,
                                    )
                                ):
                                    raise ValueError("direct terminal marker is stale")
                                finalizing = store.finalize_direct_terminal(
                                    observation.plan, observation.result
                                )
                                # Keep containment tracked at the new revision
                                # even if preparing the stage plan fails.
                                active_direct_plan = replace(
                                    plan, revision=finalizing.revision,
                                    job=replace(plan.job, intent=replace(
                                        plan.job.intent, revision=finalizing.revision)),
                                )
                                active_stage_plan = store.prepare_direct_stage(
                                    observation.plan, observation.result
                                )
                            except Exception:
                                failed = True
                            else:
                                active_terminal_plan = None
                        if failed:
                            tracked = active_direct_plan
                            close_owned_direct_controller(lambda: store.pause_active_direct_job(
                                job_id=plan.job.job_id,
                                generation=plan.generation,
                                revision=tracked.revision if tracked is not None else plan.revision,
                            ))

            if active_preparation is not None or (active_publication is not None and not publication_observed):
                if not _acquire_direct_observation():
                    return
                preparation, attempt, recovery, permit = active_preparation, active_publication, active_recovery, publication_permit
                cancelled = publication_cancel.is_set
                token = preparation if preparation is not None else attempt
                def publication_observe() -> None:
                    try:
                        from hermes_downloads.paths import (prepare_publication_payload,
                            publish_staged_partial_payload, PublicationReservationMarker, rehydrate_destination)
                        if preparation is not None:
                            stage, staged = preparation
                            job = stage.job
                            if job.source_kind is not SourceKind.DIRECT:
                                raise ValueError('publication requires direct source')
                            destination = rehydrate_destination(category=job.category, collection=job.destination_collection,
                                partial_filename=job.partial_filename, selected_final_filename=job.selected_final_filename,
                                job_id=job.job_id)
                            result = prepare_publication_payload(destination, stage.terminal.dispatch.reservation,
                                PublicationReservationMarker(destination.incomplete_dir / '.hermes-reservation',
                                    stage.terminal.marker.marker_device, stage.terminal.marker.marker_inode), staged, cancelled=cancelled)
                        else:
                            if attempt.job.source_kind is not SourceKind.DIRECT:
                                raise ValueError('publication requires direct source')
                            p = attempt.prepared
                            result = publish_staged_partial_payload(p.destination,p.reservation,p.staged_payload,
                                prepared=p,creation_permit=None if recovery is not None else permit,
                                existing_only=recovery is not None, _cancelled=cancelled)
                        outcome = _PublicationObservation(token,result,False)
                    except BaseException:
                        outcome = _PublicationObservation(token,None,True)
                    observation_results.put_nowait(outcome)
                observation_thread = threading.Thread(target=publication_observe,
                    name='direct-publication-observation', daemon=True)
                try:
                    _start_direct_observation(observation_thread)
                except BaseException:
                    observation_thread = None
                    raise
                return

            controller, terminal, stage = direct_controller, active_terminal_plan, active_stage_plan
            if controller is None or (terminal is None and stage is None) or not direct_controller_ready:
                return
            if not _acquire_direct_observation():
                return

            def observe() -> None:
                if stage is not None:
                    try:
                        from hermes_downloads.paths import (
                            StagedPartialPayload, attest_staged_partial_payload,
                            rehydrate_destination,
                        )
                        job = stage.job
                        destination = rehydrate_destination(
                            category=job.category, collection=job.destination_collection,
                            partial_filename=job.partial_filename,
                            selected_final_filename=job.selected_final_filename,
                            job_id=job.job_id,
                        )
                        staged = attest_staged_partial_payload(
                            destination, stage.terminal.dispatch.reservation
                        )
                        identity = stage.observed.verified_identity
                        if type(staged) is not StagedPartialPayload or identity is None or (
                            staged.path, staged.st_dev, staged.st_ino, staged.logical_size, staged.mtime_ns,
                            staged.st_mode, staged.st_nlink, staged.ctime_ns
                        ) != (stage.terminal.partial_path, identity.st_dev, identity.st_ino,
                              identity.logical_size, identity.mtime_ns,
                              identity.st_mode, identity.st_nlink, identity.ctime_ns):
                            raise ValueError("staged payload differs from verified output")
                        outcome = _DirectStageObservation(stage, staged, False)
                    except BaseException:
                        outcome = _DirectStageObservation(stage, None, True)
                else:
                    try:
                        from hermes_downloads.direct import DirectTransfer

                        result = controller.observe_terminal(
                            job_id=terminal.dispatch.job.job_id,
                            generation=terminal.dispatch.generation,
                            gid=terminal.gid,
                        )
                        if result is not None and type(result) is not DirectTransfer:
                            raise TypeError("direct observation result is invalid")
                        outcome = _DirectObservation(terminal, result, False)
                    except BaseException:
                        outcome = _DirectObservation(terminal, None, True)
                observation_results.put_nowait(outcome)

            observation_thread = threading.Thread(
                target=observe, name="direct-stage-observation" if stage is not None else
                "direct-terminal-observation", daemon=True,
            )
            try:
                _start_direct_observation(observation_thread)
            except BaseException:
                observation_thread = None
                tracked = active_direct_plan
                close_owned_direct_controller(lambda: store.pause_active_direct_job(
                    job_id=tracked.job.job_id,
                    generation=tracked.generation,
                    revision=tracked.revision,
                ))

        if requested_socket_path is not None:
            try:
                health_server = HealthServer(
                    requested_socket_path,
                    health=lambda: _health_from_store(store),
                    jobs_page=lambda cursor: _jobs_page_from_store(store, cursor),
                    queue_gate=queue_gate,
                    job_add=lambda command: _job_add_from_store(store, command),
                    add_batch=lambda command: _add_batch_from_store(store, command),
                    job_control=job_control,
                    direct_engine_activate=direct_engine_activate,
                    direct_job_dispatch=direct_job_dispatch,
                )
            except IPCStateError:
                raise WorkerStateError from None
        if recover_socket:
            owned_endpoint_certificate = endpoint_ownership.publish(
                root, endpoint_certificate, store.worker_epoch(), health_server._identity
            )
        if not recover_socket or not shutdown_event.wait(0):
            ready_event.set()
        if health_server is None:
            shutdown_event.wait()
        else:
            while not shutdown_event.wait(_IPC_POLL_SECONDS):
                health_server.serve_once()
                if not shutdown_event.wait(0):
                    poll_direct_terminal()
        return None
    finally:
        try:
            try:
                publication_cancel.set()
                recoverable = active_recovery is not None
                if publication_permit is not None:
                    snapshot = publication_permit.revoke()
                    if snapshot.in_flight:
                        direct_recovery_blocked = True
                        if direct_controller is not None:
                            direct_controller.close()
                        raise IPCStateError('direct_dispatch_blocked')
                    recoverable = snapshot.creation_attempted
                if not direct_recovery_blocked and store is not None:
                    if active_recovery is not None:
                        store.abort_exact_publication_recovery(active_recovery)
                    elif (active_publication is not None and publication_stage is not None
                        and not publication_observed):
                        attempt = active_publication
                        close_owned_direct_controller(lambda: store.pause_active_direct_job(
                            job_id=attempt.job.job_id, generation=attempt.generation,
                            revision=attempt.revision, _publication_recoverable=recoverable))
                    elif active_preparation is not None and active_direct_plan is not None:
                        tracked = active_direct_plan
                        close_owned_direct_controller(lambda: store.pause_active_direct_job(
                            job_id=tracked.job.job_id, generation=tracked.generation,
                            revision=tracked.revision))
                active_preparation = None
                publication_stage = None
                active_recovery = None
                if direct_controller is not None:
                    shutdown_owned_direct_controller()
            finally:
                try:
                    if observation_thread is not None:
                        observation_thread.join(_OBSERVATION_JOIN_SECONDS)
                        _direct_observation_busy()
                    if health_server is not None:
                        health_server.close()
                        if owned_endpoint_certificate is not None:
                            endpoint_ownership.clear(root, owned_endpoint_certificate)
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
