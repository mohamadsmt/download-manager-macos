"""Safe bootstrap console entry point and durable worker lifecycle."""

from __future__ import annotations

import fcntl
from dataclasses import dataclass, replace
import os
import json
from pathlib import Path
from queue import Queue
import stat
import threading
import time
from typing import TYPE_CHECKING, Callable, Final, Protocol
from datetime import UTC, datetime

from hermes_downloads.ipc import (
    AddBatchCommand,
    AddBatchResult,
    TargetAuthorizeCommand,
    TargetAuthorizeResult,
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
    QueryListPage,
    QueryStatus,
    QueryEventsPage,
    PublicJobRecord,
    QueueGateCommand,
    QueueGateResult,
    WorkerHealth,
    WorkerQueueSnapshot,
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
    _SavedResumePlan,
    _DirectEngineRecoveryCapability,
    _DirectPublicationReconciliationPlan,
    _DirectTerminalPlan,
    _DirectStagePlan,
    _DirectPublicationAttempt,
    _DirectAttemptRecoveryPlan,
    _DirectCleanupPlan,
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


def _filesystem_errno(error):
    for _ in range(8):
        if isinstance(error,OSError) and type(error.errno) is int:return error.errno
        error=error.__cause__
        if error is None:return None
    return None


@dataclass(frozen=True, slots=True, repr=False)
class _DirectObservation:
    """Only immutable evidence crosses from the observer to the SQLite owner."""

    plan: _DirectTerminalPlan
    result: DirectTransfer | None
    failed: bool
    failure_state: DirectTransfer | None = None


@dataclass(frozen=True, slots=True, repr=False)
class _DirectStageObservation:
    """Only attestation evidence crosses back to the live SQLite owner."""

    plan: _DirectStagePlan
    result: StagedPartialPayload | None
    failed: bool
    filesystem_errno: int | None = None


@dataclass(frozen=True, slots=True, repr=False)
class _PublicationObservation:
    plan: object
    result: object
    failed: bool
    filesystem_errno: int | None = None


@dataclass(frozen=True, slots=True, repr=False)
class _CleanupObservation:
    plan: _DirectCleanupPlan
    result: object
    failed: bool


@dataclass(frozen=True, slots=True, repr=False)
class _SavedObservation:
    kind: str
    token: object
    result: object
    failed: bool
    source_decision: bool = False


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


def _queue_snapshot_from_store(store: SQLiteStore, deadline: float) -> WorkerQueueSnapshot:
    connection = store._connection
    if connection.in_transaction:
        raise ValueError('queue snapshot cannot borrow a transaction')
    with store._batch_budget(deadline=deadline):
        connection.execute('BEGIN')
        try:
            epoch = store._current_worker_epoch(connection)
            gate, revision = store.queue_gate_snapshot()
            return WorkerQueueSnapshot(epoch, gate, revision)
        finally:
            connection.rollback()


def _query_list_from_store(store: SQLiteStore, cursor: str | None, deadline: float) -> QueryListPage:
    return store.read_query_list_page(cursor=cursor, deadline=deadline)


def _query_status_from_store(store: SQLiteStore, id: str, deadline: float) -> QueryStatus:
    return store.read_query_status(id=id, deadline=deadline)


def _query_events_from_store(store: SQLiteStore, cursor: str | None, deadline: float) -> QueryEventsPage:
    return store.read_query_events_page(cursor=cursor, deadline=deadline)


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


def _target_authorize_from_store(store: SQLiteStore, command: TargetAuthorizeCommand) -> TargetAuthorizeResult:
    try:
        return store.apply_target_authorize(command)
    except RequestConflictError:
        raise IPCError('command_conflict') from None
    except RuntimeError as error:
        raise IPCError(str(error)) from None


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


def _recover_cold_direct_engine(store: SQLiteStore, *, record=None) -> bool:
    """Retire only a provably owned persisted direct daemon before worker ready.

    ``True`` keeps this worker's future direct activation fail-closed.  The
    normal cold bootstrap stays engine-import-free unless an exact matching
    private capability requires authenticated shutdown.
    """

    if store.get_direct_engine_activation_fence() is not None:
        return True
    record = store.get_direct_engine_record() if record is None else record
    if record is None:
        return False
    try:
        capability = store._get_direct_engine_recovery_capability(record)
    except (TypeError, ValueError):
        return True
    if capability is None:
        return True

    reconciliation = reconcile_process_birth(record.identity)
    try:
        owned=store.cold_body_owner(record)
        if reconciliation=='absent':completed=None
        elif reconciliation=='current':
            from hermes_downloads.direct import _recover_owned_direct_engine
            completed=_recover_owned_direct_engine(record.identity,capability,
                **({'gid':owned[0]['gid']} if owned is not None else {}))
        else:return True
        try:os.killpg(record.identity.process_group_id,0)
        except ProcessLookupError:pass
        else:return True
    except Exception:return True
    if owned is not None:
        # A new cold attestation never claims the departed owner's wait or JOIN.
        from hermes_downloads.paths import _seal_stopped_pair
        receipt=owned[1];destination=store._saved_destination(receipt.job)
        reservation=store._effective_reservation(store._connection,receipt.job)
        result=[]
        def seal():
            try:result.append(_seal_stopped_pair(destination,reservation))
            except Exception:pass
        if not _acquire_direct_observation():return True
        thread=threading.Thread(target=seal,name='cold-stopped-observation')
        _start_direct_observation(thread);thread.join();_direct_observation_busy()
        if not result:return True
        try:
            os.killpg(record.identity.process_group_id,0)
        except ProcessLookupError:pass
        else:return True
        try:store.seal_cold_body(record,owned,completed if type(completed) is int and completed>0 else None,result[0])
        except (TypeError,ValueError,OSError):return True
    return not store._clear_direct_engine_record_and_recovery_capability(record,capability)


class _DirectOwner:
    """One dispatch owns its controller, callbacks, cancellation and cleanup."""

    def __init__(self, store, root, *, blocked=False, before_body=None, saved_qualifications=None):
        self.store=store
        self.root=root
        self.engine_plan=None
        self.assigned_job=None
        self.close_after_observer=False
        self.close_deadline=None
        self.direct_controller: _DirectController | None = None
        self.direct_fence: DirectEngineActivationFence | None = None
        self.direct_record: DirectEngineRecord | None = None
        self.direct_recovery_capability: _DirectEngineRecoveryCapability | None = None
        self.direct_record_persisted = False
        self.direct_recovery_capability_persisted = False
        self.direct_controller_ready = False
        self.direct_recovery_blocked = False
        self.direct_controller_absent_discard_failed = False
        self.direct_controller_absent_local_cleanup_complete = False
        self.active_direct_plan: _DirectDispatchPlan | None = None
        self.active_terminal_plan: _DirectTerminalPlan | None = None
        self.active_stage_plan: _DirectStagePlan | None = None
        self.active_preparation: tuple | None = None
        self.active_publication: _DirectPublicationAttempt | None = None
        self.publication_stage: _DirectStagePlan | None = None
        self.publication_permit = None
        self.active_recovery: _DirectAttemptRecoveryPlan | None = None
        self.publication_observed = False
        self.publication_cancel = threading.Event()
        self.active_cleanup: _DirectCleanupPlan | None = None
        self.cleanup_activated = False
        self.cleanup_cancel = threading.Event()
        self.partial_unlink_permit = None
        self.marker_unlink_permit = None
        self.cleanup_producer_request_id: str | None = None
        self.retained_unlink_permits = []
        self.private_queue_operation = None
        self.observation_thread: threading.Thread | None = None
        self.observation_results: Queue = Queue(maxsize=1)
        self.saved_phases: Queue = Queue(maxsize=1)
        self.active_saved = None
        self.saved_cancel = threading.Event()
        self.saved_qualifications = {} if saved_qualifications is None else saved_qualifications
        self.last_body_observer = None
        self.pending_stop=None
        self.before_body=before_body
        self.direct_recovery_blocked=blocked

    def clear_owned_direct_claim(self) -> None:

        if self.direct_record_persisted:
            if self.direct_record is None:
                raise IPCStateError("ipc_health_invalid")
            if self.direct_recovery_capability_persisted:
                if (
                    self.direct_recovery_capability is None
                    or not self.store._clear_direct_engine_record_and_recovery_capability(
                        self.direct_record, self.direct_recovery_capability
                    )
                ):
                    raise IPCStateError("ipc_health_invalid")
                self.direct_recovery_capability = None
                self.direct_recovery_capability_persisted = False
            elif not self.store.clear_direct_engine_record(self.direct_record):
                raise IPCStateError("ipc_health_invalid")
            self.direct_record = None
            self.direct_record_persisted = False
        elif self.direct_fence is not None:
            cleared=(self.store.clear_direct_engine_activation_fence(self.direct_fence) if self.engine_plan is None
                else self.store.clear_job_engine_fence(self.engine_plan,self.direct_fence))
            if not cleared:
                raise IPCStateError("ipc_health_invalid")
            self.direct_fence = None

    def clear_owned_direct_controller_state(self) -> None:

        self.engine_plan=None
        self.close_after_observer=False
        if hasattr(self,'applied_share'):del self.applied_share
        self.direct_controller = None
        self.direct_fence = None
        self.direct_record = None
        self.direct_recovery_capability = None
        self.direct_record_persisted = False
        self.direct_recovery_capability_persisted = False
        self.direct_controller_ready = False
        self.direct_controller_absent_discard_failed = False
        self.direct_controller_absent_local_cleanup_complete = False
        self.active_direct_plan = None
        self.active_terminal_plan = None
        self.active_stage_plan = None

    def close_owned_direct_controller(self,
        persist_contained: Callable[[], object] | None = None,
        *, _join_observer: bool = False,
    ) -> object:

        controller = self.direct_controller
        if controller is None:
            return None if persist_contained is None else persist_contained()
        # Invalidate callbacks before containment. Keep durable authority
        # until both containment and any paused/removed transaction succeed.
        self.active_terminal_plan = None
        self.active_stage_plan = None
        self.direct_controller_ready = False
        try:
            if self.close_deadline is None:controller.close()
            else:controller.close(deadline=self.close_deadline)
            if not _join_observer and self.observation_thread is not None and self.observation_thread.is_alive():
                result=None if persist_contained is None else persist_contained()
                # Legacy receipts may settle; ownership stays fenced until the original mutator joins.
                self.close_after_observer=True
                return result
            self.join_owned_observation()
            result = None if persist_contained is None else persist_contained()
            self.clear_owned_direct_claim()
            self.clear_owned_direct_controller_state()
            return result
        except BaseException:
            if persist_contained is not None:
                self.direct_recovery_blocked = True
                raise IPCStateError("direct_dispatch_blocked") from None
            raise

    def join_owned_observation(self) -> None:
        if self.observation_thread is not None:
            self.observation_thread.join(_OBSERVATION_JOIN_SECONDS if self.close_deadline is None else max(0,self.close_deadline-time.monotonic()))
            if self.observation_thread.is_alive():
                self.direct_recovery_blocked = True
                raise IPCError('direct_dispatch_blocked')
            self.observation_thread = None
            while not self.observation_results.empty():
                self.observation_results.get_nowait()
        _direct_observation_busy()
        self.partial_unlink_permit = None
        self.marker_unlink_permit = None

    def cancel_saved(self, *, pause=True) -> None:
        task = self.active_saved
        if task is not None and task[0] == 'checkpoint':
            # A stop checkpoint has no ongoing BODY to revoke. Drain its exact
            # result before another control can release or reuse this owner.
            self.observation_thread.join(_OBSERVATION_JOIN_SECONDS if self.close_deadline is None else max(0,self.close_deadline-time.monotonic()))
            if self.observation_thread.is_alive():
                raise IPCError('direct_dispatch_blocked')
            self.poll_direct_terminal()
            return
        self.saved_cancel.set()
        self.join_owned_observation()
        while not self.saved_phases.empty():
            _,_,_,ack = self.saved_phases.get_nowait()
            ack.set()
        self.active_saved = None
        if task is not None and task[0] == 'prepare':
            self.saved_qualifications.pop(task[1].job_id,None)
            self.store.fail_saved_preparation(task[1],pause=pause)
        if task is not None and task[0] == 'source':
            self.store.apply_source_replacement(task[1][0].request_id)

    def start_saved(self, kind, token, operation) -> None:
        if not _acquire_direct_observation(): raise IPCError('direct_dispatch_blocked')
        self.active_saved = (kind,token)
        self.saved_cancel = threading.Event()
        cancelled = self.saved_cancel.is_set
        end = time.monotonic() + 60
        def phase(name, facts):
            if cancelled() or time.monotonic()>=end: raise ValueError('saved preparation cancelled')
            ack = threading.Event()
            self.saved_phases.put_nowait((token,name,facts,ack))
            while not ack.wait(.02):
                if cancelled() or time.monotonic()>=end: raise ValueError('saved preparation cancelled')
            if cancelled(): raise ValueError('saved preparation cancelled')
        def observe_saved():
            try:
                result = operation(lambda: cancelled() or time.monotonic()>=end,phase)
                outcome = _SavedObservation(kind,token,result,False)
            except BaseException as error:
                from hermes_downloads.direct import _SavedSourceIdentityError
                outcome = _SavedObservation(kind,token,None,True,isinstance(error,_SavedSourceIdentityError))
            self.observation_results.put_nowait(outcome)
        self.observation_thread = threading.Thread(target=observe_saved,name='direct-saved-observation',daemon=True)
        try: _start_direct_observation(self.observation_thread)
        except BaseException:
            self.observation_thread = None
            self.active_saved = None
            raise

    def saved_body(self, plan, destination=None) -> bool:
        cause = self.store._read_target_dispatch_cause(self.store._connection,plan.request_id,{})
        if cause is None or cause['kind']!='body' or cause['admission_serial']==1: return False
        if self.store._fresh_retry(self.store._connection,plan.job.job_id,cause['activation_request_id']) is not None: return False
        continuation = self.store._require_ready_saved_pair(self.store._connection,plan.job.job_id,cause['activation_request_id'])
        if (continuation['body_request_id'],continuation['worker_epoch'],continuation['generation']) != (
            plan.request_id,self.store.worker_epoch(),plan.generation): raise ValueError('saved dispatch fence changed')
        self.store._require_pending_direct_dispatch_command(self.store._connection,plan,_effect=True)
        return True

    def discard_absent_owned_direct_controller(self) -> None:

        controller = self.direct_controller
        if controller is None:
            raise IPCStateError("ipc_health_invalid")
        controller.discard_absent()
        self.direct_controller_absent_discard_failed = False
        self.direct_controller_absent_local_cleanup_complete = True
        self.clear_owned_direct_claim()
        self.clear_owned_direct_controller_state()

    def cleanup_owned_direct_candidate(self) -> None:
        if self.direct_controller is None:
            self.clear_owned_direct_claim()
        else:
            self.close_owned_direct_controller()

    def shutdown_owned_direct_controller(self) -> None:

        if self.direct_recovery_blocked:
            return
        if self.direct_controller_absent_local_cleanup_complete:
            self.clear_owned_direct_claim()
            self.clear_owned_direct_controller_state()
            return
        if self.direct_controller is None:
            return
        if not self.direct_record_persisted:
            self.close_owned_direct_controller()
            return
        owned_record = self.direct_record
        if owned_record is None:
            raise IPCStateError("ipc_health_invalid")
        from hermes_downloads.processes import reconcile_process_birth

        reconciliation = reconcile_process_birth(owned_record.identity)
        if reconciliation == "current":
            self.close_owned_direct_controller()
            return
        if reconciliation != "absent":
            return
        try:
            self.discard_absent_owned_direct_controller()
        except BaseException:
            if not self.direct_controller_absent_local_cleanup_complete:
                self.direct_controller_absent_discard_failed = True
            raise

    def direct_engine_activate(self,
        command: DirectEngineActivateCommand,
    ) -> DirectEngineActivateResult:

        current_epoch = self.store.worker_epoch()
        if current_epoch is None:
            raise IPCStateError("ipc_health_invalid")
        if command.expected_worker_epoch != current_epoch:
            return DirectEngineActivateResult(
                worker_epoch=current_epoch, status="stale_epoch"
            )
        if self.direct_recovery_blocked:
            return DirectEngineActivateResult(
                worker_epoch=current_epoch, status="blocked"
            )
        if self.direct_controller is None and (
            self.active_cleanup is not None or self.pending_stop is not None or self.observation_thread is not None or _direct_observation_busy()
        ):
            return DirectEngineActivateResult(
                worker_epoch=current_epoch, status="blocked"
            )
        if self.direct_controller is not None:
            if (
                self.direct_controller_absent_discard_failed
                or self.direct_controller_absent_local_cleanup_complete
            ):
                return DirectEngineActivateResult(
                    worker_epoch=current_epoch, status="blocked"
                )
            owned_record = self.direct_record
            if (
                not self.direct_controller_ready
                or self.direct_fence is not None
                or not self.direct_record_persisted
                or not self.direct_recovery_capability_persisted
                or owned_record is None
                or self.direct_recovery_capability is None
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
            if self.observation_thread is not None:
                # Let the pending readback contain/pause this dispatch;
                # absence must not replace a controller under its observer.
                return DirectEngineActivateResult(
                    worker_epoch=current_epoch, status="blocked"
                )
            try:
                self.discard_absent_owned_direct_controller()
            except BaseException:
                if not self.direct_controller_absent_local_cleanup_complete:
                    self.direct_controller_absent_discard_failed = True
                return DirectEngineActivateResult(
                    worker_epoch=current_epoch, status="blocked"
                )

        if self.direct_fence is not None:
            return DirectEngineActivateResult(
                worker_epoch=current_epoch, status="blocked"
            )

        existing_record = self.store.get_direct_engine_record()
        if existing_record is not None:
            return DirectEngineActivateResult(
                worker_epoch=current_epoch, status="blocked"
            )

        existing_fence = self.store.get_direct_engine_activation_fence()
        if existing_fence is not None:
            return DirectEngineActivateResult(
                worker_epoch=current_epoch, status="blocked"
            )
        fence = (self.store.reserve_direct_engine_activation(worker_epoch=current_epoch) if self.engine_plan is None
                else self.store.reserve_job_engine(self.engine_plan))
        if fence is None:
            return DirectEngineActivateResult(
                worker_epoch=current_epoch, status="blocked"
            )
        self.direct_fence = fence

        def on_engine_bound(identity: ProcessBirthIdentity) -> None:

            fence = self.direct_fence
            if fence is None:
                raise IPCStateError("ipc_health_invalid")
            record = DirectEngineRecord(
                worker_epoch=current_epoch,
                identity=identity,
            )
            (self.store.bind_direct_engine_activation_fence(fence, record) if self.engine_plan is None
                    else self.store.bind_job_engine(self.engine_plan,fence,record))
            self.direct_record = record
            self.direct_recovery_capability = None
            self.direct_record_persisted = True
            self.direct_recovery_capability_persisted = False
            self.direct_fence = None

        try:
            from hermes_downloads.direct import DirectAria2Controller

            candidate = DirectAria2Controller(
                runtime_root=self.root / _DIRECT_RUNTIME_DIRECTORY_NAME,
                **({} if self.engine_plan is None else dict(split=int(self.store.configuration_snapshot()['desired']['connections']),max_connection_per_server=int(self.store.configuration_snapshot()['desired']['connections']))),
                on_engine_bound=on_engine_bound,
            )
            self.direct_controller = candidate
            self.direct_record = None
            self.direct_recovery_capability = None
            self.direct_record_persisted = False
            self.direct_recovery_capability_persisted = False
            self.direct_controller_ready = False
            self.direct_controller_absent_discard_failed = False
            self.direct_controller_absent_local_cleanup_complete = False
            candidate.start()
            record = self.direct_record
            if record is None or not self.direct_record_persisted:
                raise IPCStateError("ipc_health_invalid")
            capability = candidate._recovery_capability()
            self.store._bind_direct_engine_recovery_capability(record, capability)
            self.direct_recovery_capability = capability
            self.direct_recovery_capability_persisted = True
        except BaseException:
            try:
                self.cleanup_owned_direct_candidate()
            except BaseException:
                pass
            raise
        if (
            self.direct_fence is not None
            or self.direct_record is None
            or not self.direct_record_persisted
            or self.direct_recovery_capability is None
            or not self.direct_recovery_capability_persisted
        ):
            try:
                self.cleanup_owned_direct_candidate()
            except BaseException:
                pass
            raise IPCStateError("ipc_health_invalid")
        self.direct_controller_ready = True
        return DirectEngineActivateResult(worker_epoch=current_epoch, status="active")

    def direct_job_dispatch(self,
        command: DirectJobDispatchCommand,
    ) -> DirectJobDispatchResult:
        """Run one bounded, marker-bound direct admission lifecycle."""

        controller = self.direct_controller
        controller_ready = (
            controller is not None
            and self.direct_controller_ready
            and not self.direct_recovery_blocked
            and self.active_direct_plan is None
            and self.active_cleanup is None
            and self.observation_thread is None
            and not _direct_observation_busy()
        )
        try:
            prepared = self.store.prepare_direct_dispatch(
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

        return self.execute_prepared_dispatch(prepared)

    def execute_prepared_dispatch(self, prepared) -> DirectJobDispatchResult:
        controller = self.direct_controller

        if type(prepared) is _DirectCleanupPlan:
            result = prepared.download_result
            if (self.direct_controller is None and self.direct_fence is None and self.direct_record is None
                and not self.direct_record_persisted and not self.direct_recovery_capability_persisted
                and not self.direct_recovery_blocked and self.active_direct_plan is None
                and self.active_preparation is None and self.active_publication is None
                and self.active_cleanup is None and self.observation_thread is None
                and not _direct_observation_busy()):
                self.active_cleanup = prepared
                self.cleanup_producer_request_id = self.cleanup_producer(prepared)
                self.cleanup_activated = False
                self.cleanup_cancel = threading.Event()
            return DirectJobDispatchResult(result.status, result.job,
                result.generation, result.revision, result.state)
        if type(prepared) is DirectDispatchResult:
            return DirectJobDispatchResult(
                status=prepared.status,
                job=prepared.job,
                generation=prepared.generation,
                revision=prepared.revision,
                state=prepared.state,
            )
        if type(prepared) is _DirectAttemptRecoveryPlan:
            if self.active_recovery != prepared:
                if (self.active_cleanup is not None or self.observation_thread is not None or self.active_publication is not None
                    or _direct_observation_busy()):
                    result = self.store.abort_exact_publication_recovery(prepared)
                    return DirectJobDispatchResult(result.status, result.job,
                        result.generation, result.revision, result.state)
                self.publication_cancel = threading.Event()
                self.active_recovery = prepared
                self.active_publication = prepared.attempt
                self.publication_observed = False
            attempt = prepared.attempt
            return DirectJobDispatchResult('pending', attempt.job.job_id,
                attempt.generation, attempt.revision, attempt.state)
        if type(prepared) is _DirectPublicationReconciliationPlan:
            reconciliation = prepared
            if (self.active_cleanup is not None or self.observation_thread is not None
                or _direct_observation_busy()):
                result = self.store.abort_direct_publication_reconciliation(reconciliation)
                return DirectJobDispatchResult(result.status, result.job,
                    result.generation, result.revision, result.state)
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
                result = self.store.complete_direct_publication_reconciliation(
                    reconciliation,
                    final_device=published.st_dev,
                    final_inode=published.st_ino,
                    logical_size=published.logical_size,
                )
            except BaseException:
                try:
                    aborted = self.store.abort_direct_publication_reconciliation(
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

            destination = self.store.runtime_destination(plan.job.job_id)
            destination = prepare_persisted_destination_workspace(destination)
            if self.saved_body(plan,destination):
                from hermes_downloads.paths import PublicationReservationMarker
                checkpoint = self.store._saved_checkpoint(self.store._connection,plan.job.job_id,self.store._saved_continuation(self.store._connection,plan.job.job_id).source_request_id)[1]
                marker = PublicationReservationMarker(destination.incomplete_dir/'.hermes-reservation',*checkpoint.marker)
            else:
                marker = attest_publication_reservation_marker(destination,plan.reservation)
            binding = self.store.bind_publication_marker(
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

            source_bytes=self.store.effective_source(plan.job.job_id)
            source = validate_source_url(source_bytes)
            if source.raw_url != source_bytes:
                raise ValueError("persisted source bytes changed during validation")
            self.store._require_pending_direct_dispatch_command(self.store._connection,plan,_effect=True)
            continuing = self.saved_body(plan,destination)
            validator = None
            if continuing and self.store._read_creation_origin(self.store._connection,plan.job.job_id,{},target=False).batch.expected_sha256 is None:
                saved=self.store._saved_continuation(self.store._connection,plan.job.job_id)
                qualification=self.saved_qualifications.pop(saved.job_id,None)
                if qualification is None or qualification[:2]!=(saved,source.raw_url):
                    raise ValueError('saved source qualification changed')
                validator=qualification[2]
            if not continuing: self.require_first_private_body_absent(plan, destination)
            paused_transfer = controller.add_paused(
                job_id=plan.job.job_id,
                generation=plan.generation,
                source=source,
                destination=destination,
                expected_sha256=None,
                admission=plan.admission,
                **({'saved_resume':True,'_resume_validator':validator} if continuing else {}),
            )
            if continuing:
                closure = json.loads(self.store._saved_checkpoint(self.store._connection,plan.job.job_id,self.store._saved_continuation(self.store._connection,plan.job.job_id).source_request_id)[0]['closure'])
                if paused_transfer.gid == closure['gid']: raise ValueError('saved GID reused')
            if getattr(paused_transfer, "status", None) != "paused":
                raise ValueError("direct engine did not remain paused")
            plan = self.store.advance_direct_dispatch_to_downloading(plan)
            if continuing: self.saved_body(plan,destination)
            if self.store._read_target_dispatch_cause(self.store._connection,plan.request_id,{}) is not None:
                self.store._connection.execute('INSERT INTO saved_body_owners VALUES (?,?,?,?)',
                    (plan.request_id,json.dumps(self.direct_record.identity.to_record(),sort_keys=True),paused_transfer.gid,self.store.worker_epoch()))
            if hasattr(controller,'_bind_stock_owner'):
                owner=controller._bind_stock_owner(plan.job.job_id,plan.generation,paused_transfer.gid,self.store.worker_epoch(),plan.request_id,self.direct_record.identity)
                self.store.bind_stock_owner(plan,self.direct_record,paused_transfer.gid,owner)
            if self.before_body is not None and self.engine_plan is not None:self.before_body(self,plan)
            controller.resume(
                job_id=plan.job.job_id,
                generation=plan.generation,
                admission=plan.admission,
            )
            self.active_direct_plan = plan
            if self.direct_record is None:
                raise ValueError("direct engine ownership is absent")
            self.active_terminal_plan = _DirectTerminalPlan(
                dispatch=plan,
                record=self.direct_record,
                marker=binding,
                gid=paused_transfer.gid,
                partial_path=destination.partial_path,
                capability=self.direct_recovery_capability,
            )
            result = self.store.finish_direct_dispatch(plan)
        except BaseException:
            # No failed dispatch leaves a live body-capable controller.  The
            # durable pending receipt becomes a fail-closed paused result.
            try:
                aborted = self.close_owned_direct_controller(
                    lambda: self.store.abort_direct_dispatch(plan)
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

    def private_dispatch(self, request_id) -> bool:
        if self.store._read_direct_dispatch_command(self.store._connection,request_id) is None:
            raise ValueError('owned dispatch receipt missing')
        return self.store._read_target_dispatch_cause(self.store._connection,request_id,{}) is not None

    def require_first_private_body_absent(self, plan, destination=None) -> None:
        if not self.private_dispatch(plan.request_id):
            return
        cause = self.store._read_target_dispatch_cause(self.store._connection,plan.request_id,{})
        if cause['kind'] != 'body': raise ValueError('first private body cause changed')
        if cause['admission_serial'] != 1:
            self.store.require_fresh_retry_absence(plan)
        self.store._require_pending_direct_dispatch_command(self.store._connection,plan,_effect=True)
        if destination is None:
            from hermes_downloads.paths import rehydrate_destination
            destination = rehydrate_destination(category=plan.job.category,
                collection=plan.job.destination_collection,partial_filename=plan.job.partial_filename,
                selected_final_filename=plan.job.selected_final_filename,job_id=plan.job.job_id)
        for path in (destination.partial_path, destination.partial_path.with_name(destination.partial_path.name+'.aria2')):
            try:
                path.lstat()
            except FileNotFoundError:
                continue
            raise ValueError('first private body has unowned partial or control bytes')

    def cleanup_producer(self, plan, captured=None):
        """Route by the exact completion producer, independently of its BODY."""
        claim = self.store._read_direct_cleanup_claim(self.store._connection,plan.claim.job_id,{})
        if claim != plan.claim:
            raise ValueError('cleanup claim changed')
        original = json.loads(claim.attempt.proof)['request']
        completed = []
        for row in self.store._connection.execute('''SELECT request_id FROM target_dispatch_causes
            WHERE kind='publication' AND job_id=? AND attempt_id=? AND original_body_request_id=?''',
            (claim.job_id,claim.attempt_id,original)):
            request = row[0]
            self.store._read_target_dispatch_cause(self.store._connection,request,{})
            receipt = self.store._read_direct_dispatch_command(self.store._connection,request)
            if (receipt.status,receipt.generation,receipt.revision,receipt.state) == (
                'started',claim.completion_generation,claim.completion_revision,'completed'):
                completed.append(request)
        if len(completed) > 1:
            raise ValueError('ambiguous private cleanup completion')
        request = captured or (completed[0] if completed else original)
        receipt = self.store._read_direct_dispatch_command(self.store._connection,request)
        self.private_dispatch(request)
        if request != original:
            if receipt.job != claim.job_id or (receipt.status,receipt.generation,receipt.revision,receipt.state) != (
                'started',claim.completion_generation,claim.completion_revision,'completed'):
                raise ValueError('cleanup completion receipt changed')
            if completed and request != completed[0]:
                raise ValueError('cleanup completion producer changed')
        return request

    def revoke_publication(self) -> bool:
        """Invalidate completion promptly; a syscall must quiesce before ack."""
        self.publication_cancel.set()
        recoverable = self.active_recovery is not None
        if self.publication_permit is not None:
            snapshot = self.publication_permit.revoke()
            if snapshot.in_flight:
                raise IPCError('direct_dispatch_blocked')
            recoverable = snapshot.creation_attempted
        self.active_preparation = None
        self.publication_stage = None
        self.active_recovery = None
        return recoverable

    def revoke_cleanup(self) -> None:
        """Discard late evidence; an entered unlink must settle before ack."""
        self.cleanup_cancel.set()
        snapshots = [permit.revoke() for permit in (self.partial_unlink_permit, self.marker_unlink_permit,*self.retained_unlink_permits)
            if permit is not None]
        if any(snapshot.in_flight for snapshot in snapshots):
            raise IPCError('direct_dispatch_blocked')
        self.active_cleanup = None
        self.cleanup_activated = False

    def capture_stopped_body(self):
        plan=self.active_direct_plan;terminal=self.active_terminal_plan
        if plan is None or terminal is None or self.active_publication is not None: return None
        try:
            cause=self.store._read_target_dispatch_cause(self.store._connection,plan.request_id,{})
            process=self.direct_controller._process;observer=self.observation_thread or self.last_body_observer
            observed=self.direct_controller.readback(job_id=plan.job.job_id,generation=plan.generation,gid=terminal.gid)
            # Stock progress can be zero while a received tail is flushed on stop.
            # The sealer requires an exact owned payload and positive stock control.
            if (cause is not None and cause['kind']=='body' and observer is not None
                and type(observed.completed_length) is int
                and observed.completed_length>=0
                and (observed.total_length==0 or observed.completed_length<=observed.total_length)):
                return plan,terminal,process,observer,observed.completed_length,self.direct_controller
        except Exception: pass
        return None

    def flush_pending_stop(self):
        if self.pending_stop is None or self.active_saved is not None or self.observation_thread is not None:return
        token,destination=self.pending_stop
        def checkpoint_operation(cancelled,phase):
            from hermes_downloads.paths import _seal_stopped_pair
            if cancelled():raise ValueError('checkpoint cancelled')
            return _seal_stopped_pair(destination,token[0].reservation)
        try:self.start_saved('checkpoint',token,checkpoint_operation)
        except IPCError:pass

    def seal_owned_stop(self, context,command,result,stop_kind):
        if context is None:return
        plan,terminal,process,observer,completed,controller=context
        if process.returncode is None or observer.is_alive():raise ValueError('original stop unsettled')
        try:os.killpg(terminal.record.identity.process_group_id,0)
        except ProcessLookupError:pass
        else:raise ValueError('original group still present')
        closure=dict(process=terminal.record.identity.to_record(),returncode=process.returncode,
            group_absent=True,observer_joined=True,gid=terminal.gid,completed_length=completed)
        stock=controller._sealed_stock_result(plan.job.job_id,plan.generation,terminal.gid)
        if stock.owner is not None:
            from dataclasses import asdict
            closure['stock']=asdict(stock)
        token=(plan,command,result,closure,stop_kind)
        if self.pending_stop is not None and self.pending_stop[0]!=token:raise ValueError('pending stop already owned')
        self.pending_stop=(token,self.store._saved_destination(plan.job.job_id))
        self.flush_pending_stop()

    def stop_for_configuration(self, command, *, stop_kind='configure'):
        if self.active_direct_plan is None:return
        context=self.capture_stopped_body();tracked=self.active_direct_plan
        self.publication_cancel.set()
        if self.publication_permit is not None and self.publication_permit.revoke().in_flight:
            raise IPCError('direct_dispatch_blocked')
        self.close_owned_direct_controller(lambda:self.store.pause_active_direct_job(job_id=tracked.job.job_id,
            generation=tracked.generation,revision=tracked.revision),_join_observer=True)
        self.active_preparation=None;self.active_publication=None;self.active_recovery=None;self.publication_stage=None;self.publication_permit=None
        self.seal_owned_stop(context,command,self.store.get_job(tracked.job.job_id),stop_kind)

    def job_control(self, command: JobControlCommand) -> JobControlResult:
        """Contain the one active body before acknowledging pause/removal."""
        if (self.active_saved is not None and command.action in {'pause','remove'}
            and command.job == (self.active_saved[1].job_id if self.active_saved[0]=='prepare' else self.active_saved[1][0].id if self.active_saved[0]=='source' else self.active_saved[1][0].job.job_id)
            and self.store.publication_control_is_current(job_id=command.job,action=command.action,
                request_id=command.request_id,payload_digest=command.payload_digest,expected_revision=command.expected_revision)):
            self.cancel_saved(pause=False)
        cleanup = self.active_cleanup
        if (cleanup is not None and command.job == cleanup.claim.job_id
            and command.action in {'pause', 'remove'}
            and self.store.cleanup_control_is_current(job_id=command.job,
                action=command.action, request_id=command.request_id,
                payload_digest=command.payload_digest, expected_revision=command.expected_revision)):
            self.revoke_cleanup()
        plan = self.active_direct_plan
        attempt = self.active_publication
        tracked_job = plan.job.job_id if plan is not None else (attempt.job.job_id if attempt else None)
        if (
            command.job == tracked_job
            and command.action in {"pause", "remove"}
            and self.store.publication_control_is_current(job_id=command.job,
                action=command.action, request_id=command.request_id,
                payload_digest=command.payload_digest, expected_revision=command.expected_revision)
        ):
            private_request = self.active_recovery.request_id if self.active_recovery is not None else (plan.request_id if plan is not None else None)
            private = private_request is not None and self.store._read_target_dispatch_cause(self.store._connection,private_request,{}) is not None
            stopped_context = self.capture_stopped_body() if command.action=='pause' else None
            recovery = self.active_recovery
            recoverable = self.revoke_publication()
            def contained_control():
                result = self.store.apply_job_control(
                    job_id=command.job,
                    action=command.action,
                    request_id=command.request_id,
                    payload_digest=command.payload_digest,
                    expected_revision=command.expected_revision,
                    _contained_direct_transfer=True,
                    _publication_recoverable=recoverable,
                )
                if recovery is not None:
                    self.store.abort_exact_publication_recovery(recovery)
                return result
            try:
                result = self.close_owned_direct_controller(contained_control,_join_observer=private)
            except RequestConflictError:
                raise
            except (TypeError, ValueError):
                raise IPCError("direct_dispatch_blocked") from None
            self.active_publication = None
            if private:
                self.join_owned_observation()
            if stopped_context is not None and result.status=='applied':
                self.seal_owned_stop(stopped_context,command,result,'pause')
            return JobControlResult(
                status=result.status,
                job=result.job,
                generation=result.generation,
                revision=result.revision,
                state=result.state,
                authorized=result.authorized,
            )
        return _job_control_from_store(self.store, command)

    def queue_gate(self, command: QueueGateCommand, *, _gate_committed=False) -> QueueGateResult:
        """Retain legacy cancellation; private pause commits before containment."""
        current = _gate_committed or self.store.publication_queue_control_is_current(request_id=command.request_id,
            payload_digest=command.payload_digest,expected_revision=command.expected_revision)
        if command.gate != 'paused' or not current:
            return _queue_gate_from_store(self.store,command)
        if self.active_saved is not None:
            result = _queue_gate_from_store(self.store,command)
            self.cancel_saved()
            return result
        operation = self.private_queue_operation or (self.active_direct_plan,self.active_publication,self.active_recovery,
            self.active_cleanup,self.cleanup_producer_request_id if self.active_cleanup is not None else None)
        plan,attempt,recovery,cleanup,producer = operation
        try:
            if cleanup is not None:
                producer = self.cleanup_producer(cleanup,producer)
            elif recovery is not None:
                if self.store._read_publication_attempt(self.store._connection,recovery.attempt.job.job_id) != recovery.attempt:
                    raise ValueError('owned recovery changed')
                producer = recovery.request_id
            elif attempt is not None:
                producer = attempt.pending_request_id or json.loads(attempt.proof)['request']
                self.store._read_publication_attempt(self.store._connection,attempt.job.job_id)
            elif plan is not None:
                producer = plan.request_id
                receipt = self.store._read_direct_dispatch_command(self.store._connection,producer)
                if receipt is None or (receipt.job,receipt.generation,receipt.payload_digest) != (
                    plan.job.job_id,plan.generation,plan.payload_digest):
                    raise ValueError('owned body changed')
            private = producer is not None and self.private_dispatch(producer)
        except (TypeError,ValueError,KeyError):
            raise IPCError('direct_dispatch_blocked') from None
        if not private:
            # Accepted19: entered legacy syscalls refuse before any gate write.
            if self.store._read_command_receipt(self.store._connection,command.request_id) is not None:
                return _queue_gate_from_store(self.store,command)
            if cleanup is not None:
                self.revoke_cleanup()
            if plan is not None or attempt is not None:
                recovery = self.active_recovery
                recoverable = self.revoke_publication()
                job_id = plan.job.job_id if plan is not None else attempt.job.job_id
                generation = plan.generation if plan is not None else attempt.generation
                revision = plan.revision if plan is not None else attempt.revision
                def contained_queue():
                    result = self.store.apply_queue_gate(
                        gate=command.gate,request_id=command.request_id,payload_digest=command.payload_digest,
                        expected_revision=command.expected_revision,
                        _contained_direct_job=(job_id,generation,revision,recoverable))
                    if recovery is not None:
                        self.store.abort_exact_publication_recovery(recovery)
                    return result
                try:
                    result = self.close_owned_direct_controller(contained_queue)
                except (TypeError,ValueError):
                    raise IPCError('direct_dispatch_blocked') from None
                self.active_publication = None
                return QueueGateResult(applied=result.applied,queue_gate=result.gate,revision=result.revision)
            return _queue_gate_from_store(self.store,command)
        stopped_context = self.capture_stopped_body()
        self.private_queue_operation = (plan,attempt,recovery,cleanup,producer)
        result = _queue_gate_from_store(self.store,command)
        try:
            recoverable = self.revoke_publication()
            self.revoke_cleanup()
            def contained():
                if recovery is not None:
                    cause = self.store._read_target_dispatch_cause(self.store._connection,recovery.request_id,{})
                    if cause is not None and cause['kind']=='publication' and recovery.attempt.state=='paused':
                        # Park only unchanged durable work, never its observer/token.
                        if self.store._read_publication_attempt(self.store._connection,recovery.attempt.job.job_id) != recovery.attempt:
                            raise ValueError('private parked recovery changed')
                        self.store._capture_target_head(self.store._connection,recovery.attempt.job.job_id)
                    else:
                        self.store.abort_exact_publication_recovery(recovery)
                elif plan is not None:
                    receipt = self.store._read_direct_dispatch_command(self.store._connection,plan.request_id)
                    if receipt is not None and receipt.status=='pending':
                        self.store.abort_direct_dispatch(plan)
                    else:
                        job = self.store.get_job(plan.job.job_id)
                        if job.state in {'downloading','finalizing'}:
                            self.store.pause_active_direct_job(job_id=job.job,generation=job.generation,
                                revision=job.revision,_publication_recoverable=recoverable)
                elif attempt is not None and attempt.state in {'downloading','finalizing'}:
                    self.store.pause_active_direct_job(job_id=attempt.job.job_id,generation=attempt.generation,
                        revision=attempt.revision,_publication_recoverable=recoverable)
            self.close_owned_direct_controller(contained,_join_observer=True)
            self.active_publication = None
            self.join_owned_observation()
            if (self.direct_controller is None and self.direct_fence is None and self.direct_record is None
                and self.direct_recovery_capability is None and not self.direct_record_persisted
                and not self.direct_recovery_capability_persisted):
                self.direct_recovery_blocked = False
            if self.direct_recovery_blocked:
                raise IPCError('direct_dispatch_blocked')
            self.private_queue_operation = None
            self.cleanup_producer_request_id = None
            if stopped_context is not None:
                current=self.store.get_job(stopped_context[0].job.job_id)
                self.seal_owned_stop(stopped_context,command,current,'queue')
        except BaseException:
            self.direct_recovery_blocked = True
            raise IPCError('direct_dispatch_blocked') from None
        return result

    def poll_target_dispatch(self) -> None:
        ready = (self.direct_controller is None and self.direct_fence is None and self.direct_record is None
            and self.direct_recovery_capability is None and not self.direct_record_persisted
            and not self.direct_recovery_capability_persisted and not self.direct_recovery_blocked
            and self.active_direct_plan is None and self.active_terminal_plan is None and self.active_stage_plan is None
            and self.active_preparation is None and self.active_publication is None and self.active_recovery is None
            and self.active_cleanup is None and self.partial_unlink_permit is None and self.marker_unlink_permit is None
            and self.observation_thread is None and self.pending_stop is None and not _direct_observation_busy())
        if not ready: return
        epoch = self.store.worker_epoch()
        try:
            self.store.prepare_due_retry(now=datetime.now(UTC))
            prepared = self.store._prepare_target_dispatch(expected_worker_epoch=epoch,
                owner_slot_ready=ready,now=datetime.now(UTC),_saved_qualifications=self.saved_qualifications)
        except (TypeError,ValueError,RuntimeError,OverflowError):
            self.direct_recovery_blocked = True
            return
        if prepared is not None and type(prepared) is not DirectDispatchResult:
            self.assigned_job=(prepared.job_id if type(prepared) is _SavedResumePlan else prepared.claim.job_id if type(prepared) is _DirectCleanupPlan else prepared.job.job_id if type(prepared) is _DirectDispatchPlan else prepared.attempt.job.job_id)
        if type(prepared) is _SavedResumePlan:
            from hermes_downloads.paths import _prepare_saved_pair
            destination = self.store._saved_destination(prepared.job_id)
            reservation = self.store._effective_reservation(self.store._connection,prepared.job_id)
            checkpoint = self.store._saved_checkpoint(self.store._connection,prepared.job_id,prepared.source_request_id)[1]
            original=self.store._saved_checkpoint(self.store._connection,prepared.job_id,prepared.source_request_id)[0]
            stock=self.store._checkpoint_stock(self.store._connection,original)
            source=validate_source_url(self.store.effective_source(prepared.job_id))
            expected=self.store._read_creation_origin(self.store._connection,prepared.job_id,{},target=False).batch.expected_sha256
            existing=self.store._continuation_row(self.store._connection,prepared.job_id,prepared.request_id)['phase']=='ready'
            if existing:self.store._require_ready_saved_pair(self.store._connection,prepared.job_id,prepared.request_id)
            def prepare_saved(cancelled,phase):
                validator=None
                if expected is None:
                    from hermes_downloads.direct import _verify_saved_source
                    validator=_verify_saved_source(source,stock,cancelled=cancelled)
                facts=None if existing else _prepare_saved_pair(destination,reservation,checkpoint,cancelled=cancelled,phase=phase,retained_names=prepared.retained_names)
                return facts,source.raw_url,validator,existing
            self.start_saved('prepare',prepared,prepare_saved)
            return
        if type(prepared) is _DirectDispatchPlan:
            self.engine_plan=prepared
            try:
                if not self.saved_body(prepared): self.require_first_private_body_absent(prepared)
                activated = self.direct_engine_activate(DirectEngineActivateCommand(epoch))
                if activated.status != 'active': raise ValueError('private activation blocked')
            except BaseException:
                try:
                    self.close_owned_direct_controller(lambda: self.store.abort_direct_dispatch(prepared))
                except BaseException:
                    self.direct_recovery_blocked = True
                    self.private_queue_operation = (prepared,None,None,None,prepared.request_id)
                if self.direct_controller is not None or self.direct_fence is not None or self.direct_record is not None:
                    self.direct_recovery_blocked = True
                    self.private_queue_operation = (prepared,None,None,None,prepared.request_id)
                return
        if prepared is not None and type(prepared) is not DirectDispatchResult:
            self.execute_prepared_dispatch(prepared)

    def poll_direct_terminal(self) -> None:
        """Reap/launch one terminal or stage observation; persist on this thread."""

        if self.close_after_observer:
            if self.observation_thread is not None and self.observation_thread.is_alive():return
            self.join_owned_observation()
            self.clear_owned_direct_claim()
            self.clear_owned_direct_controller_state()
            return
        self.flush_pending_stop()
        if not self.saved_phases.empty():
            token,phase,facts,ack = self.saved_phases.get_nowait()
            try:
                if self.active_saved != ('prepare',token) or self.saved_cancel.is_set(): raise ValueError('saved phase revoked')
                self.store.record_saved_phase(token,phase,facts)
            except Exception:
                self.saved_cancel.set()
            finally: ack.set()
        if self.observation_thread is not None:
            if self.observation_thread.is_alive():
                return
            self.observation_thread.join()
            self.observation_thread = None
            _direct_observation_busy()
            observation = self.observation_results.get_nowait()
            if type(observation) is _SavedObservation:
                task = self.active_saved
                self.active_saved = None
                # A stopped pair is one observation, including failure or revocation.
                # Retrying cannot create authority and would monopolize this owner.
                if (task == ('checkpoint',observation.token) and self.pending_stop is not None
                    and self.pending_stop[0] == observation.token):
                    self.pending_stop = None
                if task == (observation.kind,observation.token) and not self.saved_cancel.is_set() and not observation.failed:
                    try:
                        if observation.kind=='prepare':
                            self.store.qualify_saved_drain(observation.token,_existing=observation.result[3])
                            self.saved_qualifications[observation.token.job_id]=(observation.token,observation.result[1],observation.result[2])
                        if observation.kind=='source':
                            self.store.apply_source_replacement(observation.token[0].request_id,_verified_stock=observation.result)
                        if observation.kind=='checkpoint':
                            plan,command,result,closure,stop_kind = observation.token
                            self.store.seal_stopped_direct_body(plan,command,result,observation.result,closure,stop_kind=stop_kind)
                    except Exception:
                        if observation.kind=='prepare': self.store.fail_saved_preparation(observation.token)
                elif observation.kind=='prepare':
                    self.store.fail_saved_preparation(observation.token,_identity_decision=observation.source_decision)
                elif observation.kind=='source':
                    self.store.apply_source_replacement(observation.token[0].request_id)
            elif type(observation) is _CleanupObservation:
                cleanup = self.active_cleanup
                if (observation.plan is cleanup and not self.cleanup_cancel.is_set()
                    and not observation.failed):
                    try:
                        if cleanup.claim.phase == 'pending':
                            self.store.acknowledge_retained_cleanup(cleanup)
                            self.active_cleanup = self.store.certify_direct_cleanup(cleanup, observation.result)
                        else:
                            self.store.finish_direct_cleanup(cleanup, observation.result)
                            self.active_cleanup = None
                            self.cleanup_activated = False
                    except Exception:
                        self.active_cleanup = None
                        self.cleanup_activated = False
                elif observation.plan is cleanup:
                    self.active_cleanup = None
                    self.cleanup_activated = False
                if self.active_cleanup is None:
                    self.cleanup_producer_request_id = None
                self.partial_unlink_permit = None
                self.marker_unlink_permit = None
            elif type(observation) is _PublicationObservation:
                if observation.plan is self.active_preparation:
                    preparation = self.active_preparation
                    self.active_preparation = None
                    if not observation.failed:
                        try:
                            self.active_publication = self.store.reserve_direct_publication(preparation[0], observation.result)
                            from hermes_downloads.paths import PublicationCreationPermit
                            self.publication_permit = PublicationCreationPermit()
                            self.publication_stage = preparation[0]
                            self.publication_observed = False
                        except Exception:
                            observation = _PublicationObservation(preparation, None, True)
                    if observation.failed:
                        tracked = self.active_direct_plan
                        self.close_owned_direct_controller(lambda: self.store.pause_active_direct_job(
                            job_id=tracked.job.job_id, generation=tracked.generation, revision=tracked.revision,_filesystem_errno=observation.filesystem_errno))
                elif observation.plan is self.active_publication:
                    attempt = self.active_publication
                    recovery = self.active_recovery
                    valid = recovery is not None or (self.publication_stage is not None
                        and self.publication_permit is not None and not self.publication_permit.snapshot().revoked)
                    self.publication_observed = True
                    if valid:
                        try:
                            if observation.failed:
                                raise ValueError('publication observation failed')
                            self.store.complete_direct_publication(attempt, observation.result,
                                initial_stage=self.publication_stage if recovery is None else None)
                        except Exception:
                            if recovery is not None:
                                self.store.abort_exact_publication_recovery(recovery)
                            else:
                                recoverable = self.publication_permit.snapshot().creation_attempted
                                self.close_owned_direct_controller(lambda: self.store.pause_active_direct_job(
                                    job_id=attempt.job.job_id, generation=attempt.generation,
                                    revision=attempt.revision, _publication_recoverable=recoverable,_filesystem_errno=observation.filesystem_errno))
                        else:
                            # Completion is durable before retiring corresponding engine claims.
                            producer = attempt.pending_request_id or json.loads(attempt.proof)['request']
                            try:
                                self.active_cleanup = self.store.prepare_live_direct_cleanup(attempt, observation.result)
                                self.cleanup_producer_request_id = (self.cleanup_producer(self.active_cleanup,producer)
                                    if self.active_cleanup is not None else None)
                            except Exception:
                                self.active_cleanup = None
                            self.cleanup_activated = False
                            self.cleanup_cancel = threading.Event()
                            self.close_owned_direct_controller()
                        self.active_publication = None
                        self.publication_stage = None
                        self.publication_permit = None
                        self.active_recovery = None
            elif type(observation) is _DirectStageObservation:
                if observation.plan is self.active_stage_plan:
                    stage = observation.plan
                    failed = observation.failed
                    if not failed and observation.result is not None:
                        try:
                            self.store.bind_direct_staged_payload(stage, observation.result)
                        except Exception:
                            failed = True
                        else:
                            self.active_stage_plan = None
                            self.publication_cancel = threading.Event()
                            self.active_preparation = (stage, observation.result)
                    if failed:
                        current = self.store.get_job(stage.job.job_id)
                        if current is not None and (
                            current.generation, current.revision, current.state
                        ) == (stage.terminal.dispatch.generation, stage.revision, "finalizing"):
                            self.close_owned_direct_controller(lambda: self.store.pause_active_direct_job(
                                job_id=stage.job.job_id,
                                generation=stage.terminal.dispatch.generation,
                                revision=stage.revision,_filesystem_errno=observation.filesystem_errno,
                            ))
                        else:
                            self.close_owned_direct_controller()
            elif observation.plan is self.active_terminal_plan:
                plan = observation.plan.dispatch
                current = self.store.get_job(plan.job.job_id)
                if current is None or (
                    current.generation, current.revision, current.state
                ) != (plan.generation, plan.revision, "downloading"):
                    self.close_owned_direct_controller()
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
                            finalizing = self.store.finalize_direct_terminal(
                                observation.plan, observation.result
                            )
                            # Keep containment tracked at the new revision
                            # even if preparing the stage plan fails.
                            self.active_direct_plan = replace(
                                plan, revision=finalizing.revision,
                                job=replace(plan.job, intent=replace(
                                    plan.job.intent, revision=finalizing.revision)),
                            )
                            self.active_stage_plan = self.store.prepare_direct_stage(
                                observation.plan, observation.result
                            )
                        except Exception:
                            failed = True
                        else:
                            self.active_terminal_plan = None
                    if failed:
                        tracked = self.active_direct_plan
                        if observation.failure_state is not None:
                            stopped_context=self.capture_stopped_body()
                            controller=self.direct_controller;original=controller._process
                            terminal=observation.plan
                            def failed_owner():
                                return self.store.fail_direct_terminal(terminal,
                                    error_code=observation.failure_state.error_code,
                                    stock=controller._sealed_stock_result(plan.job.job_id,plan.generation,terminal.gid),
                                    closure=dict(process=terminal.record.identity.to_record(),returncode=original.returncode,
                                        group_absent=True,observer_joined=True,reader_joined=True,eof=True,gid=terminal.gid),now=datetime.now(UTC))
                            failed_result=self.close_owned_direct_controller(failed_owner)
                            self.seal_owned_stop(stopped_context,None,failed_result,'failure')
                        else:
                            self.close_owned_direct_controller(lambda: self.store.pause_active_direct_job(
                                job_id=plan.job.job_id,generation=plan.generation,
                                revision=tracked.revision if tracked is not None else plan.revision))

        if self.active_cleanup is not None:
            if (self.direct_controller is not None or self.direct_fence is not None or self.direct_record is not None
                or self.direct_record_persisted or self.direct_recovery_capability_persisted
                or self.direct_recovery_blocked or self.active_direct_plan is not None
                or self.active_preparation is not None or self.active_publication is not None
                or _direct_observation_busy()):
                return
            try:
                if not self.cleanup_activated:
                    self.active_cleanup = self.store.activate_direct_cleanup(self.active_cleanup)
                    self.cleanup_activated = True
                from hermes_downloads.paths import _PartialUnlinkPermit, _MarkerUnlinkPermit
                if self.active_cleanup.claim.phase == 'pending':
                    retained_records=self.store.retained_cleanup_records(self.active_cleanup,authorize=True)
                    self.retained_unlink_permits=[_PartialUnlinkPermit() for _ in retained_records]
                    self.partial_unlink_permit = _PartialUnlinkPermit()
                elif self.active_cleanup.claim.phase == 'certified':
                    from hermes_downloads.paths import _require_retained_absence
                    retained_records=self.store.retained_cleanup_records(self.active_cleanup)
                    # The certified marker unlink may already have happened before a crash.
                    _require_retained_absence(self.active_cleanup.claim.attempt.prepared,retained_records,marker_present=False)
                    self.marker_unlink_permit = _MarkerUnlinkPermit()
                else:
                    raise ValueError('cleanup phase is not active')
            except Exception:
                self.active_cleanup = None
                self.cleanup_activated = False
                return
            if not _acquire_direct_observation():
                return
            cleanup = self.active_cleanup
            partial_permit, marker_permit = self.partial_unlink_permit, self.marker_unlink_permit
            alias_permits=tuple(self.retained_unlink_permits)
            cancelled = self.cleanup_cancel.is_set
            def cleanup_observe() -> None:
                try:
                    from hermes_downloads.paths import (prepare_direct_cleanup_payload,
                        finish_direct_cleanup_payload)
                    if cleanup.claim.phase == 'pending':
                        from hermes_downloads.paths import _cleanup_retained_aliases
                        _cleanup_retained_aliases(cleanup.claim.attempt.prepared,cleanup.claim.published_stat,
                            retained_records,permits=alias_permits,cancelled=cancelled)
                        result = prepare_direct_cleanup_payload(cleanup.claim.attempt.prepared,
                            cleanup.claim.published_stat, partial_permit=partial_permit, cancelled=cancelled)
                    else:
                        result = finish_direct_cleanup_payload(cleanup.claim.attempt.prepared,
                            cleanup.claim.certified_stat, marker_permit=marker_permit, cancelled=cancelled)
                    outcome = _CleanupObservation(cleanup, result, False)
                except BaseException:
                    outcome = _CleanupObservation(cleanup, None, True)
                self.observation_results.put_nowait(outcome)
            self.observation_thread = threading.Thread(target=cleanup_observe,
                name='direct-cleanup-observation', daemon=True)
            try:
                _start_direct_observation(self.observation_thread)
            except BaseException:
                self.observation_thread = None
                self.revoke_cleanup()
                raise
            return

        if self.active_preparation is not None or (self.active_publication is not None and not self.publication_observed):
            if not _acquire_direct_observation():
                return
            preparation, attempt, recovery, permit = self.active_preparation, self.active_publication, self.active_recovery, self.publication_permit
            prepared_destination=None if preparation is None else self.store.runtime_destination(preparation[0].job.job_id)
            cancelled = self.publication_cancel.is_set
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
                        destination = prepared_destination
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
                except BaseException as error:
                    outcome = _PublicationObservation(token,None,True,_filesystem_errno(error))
                self.observation_results.put_nowait(outcome)
            self.observation_thread = threading.Thread(target=publication_observe,
                name='direct-publication-observation', daemon=True)
            try:
                _start_direct_observation(self.observation_thread)
            except BaseException:
                self.observation_thread = None
                raise
            return

        controller, terminal, stage = self.direct_controller, self.active_terminal_plan, self.active_stage_plan
        if controller is None or (terminal is None and stage is None) or not self.direct_controller_ready:
            return
        if not _acquire_direct_observation():
            return

        stage_destination=None if stage is None else self.store.runtime_destination(stage.job.job_id)
        def observe() -> None:
            if stage is not None:
                try:
                    from hermes_downloads.paths import (
                        StagedPartialPayload, attest_staged_partial_payload,
                        rehydrate_destination,
                    )
                    job = stage.job
                    destination = stage_destination
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
                except BaseException as error:
                    outcome = _DirectStageObservation(stage, None, True,_filesystem_errno(error))
            else:
                try:
                    from hermes_downloads.direct import DirectTransfer, _StockTerminalError

                    result = controller.observe_terminal(
                        job_id=terminal.dispatch.job.job_id,
                        generation=terminal.dispatch.generation,
                        gid=terminal.gid,
                    )
                    if result is not None and type(result) is not DirectTransfer:
                        raise TypeError("direct observation result is invalid")
                    outcome = _DirectObservation(terminal, result, False)
                except _StockTerminalError as error:
                    outcome = _DirectObservation(terminal, None, True, error.state)
                except BaseException:
                    outcome = _DirectObservation(terminal, None, True)
            self.observation_results.put_nowait(outcome)

        self.observation_thread = threading.Thread(
            target=observe, name="direct-stage-observation" if stage is not None else
            "direct-terminal-observation", daemon=True,
        )
        if terminal is not None: self.last_body_observer = self.observation_thread
        try:
            _start_direct_observation(self.observation_thread)
        except BaseException:
            self.observation_thread = None
            tracked = self.active_direct_plan
            self.close_owned_direct_controller(lambda: self.store.pause_active_direct_job(
                job_id=tracked.job.job_id,
                generation=tracked.generation,
                revision=tracked.revision,
            ))


    def owns_job(self, job):
        return (self.assigned_job==job
            or self.active_direct_plan is not None and self.active_direct_plan.job.job_id==job
            or self.active_publication is not None and self.active_publication.job.job_id==job
            or self.active_cleanup is not None and self.active_cleanup.claim.job_id==job)

    def idle(self):
        return (self.direct_controller is None and self.direct_fence is None and self.direct_record is None
            and not self.direct_recovery_blocked and self.active_direct_plan is None and self.active_terminal_plan is None
            and self.active_stage_plan is None and self.active_preparation is None and self.active_publication is None
            and self.active_recovery is None and self.active_cleanup is None and self.active_saved is None
            and self.observation_thread is None and self.pending_stop is None)

    def shutdown(self):
        context=self.capture_stopped_body() if self.active_direct_plan is not None and self.active_terminal_plan is not None and self.active_saved is None and self.active_publication is None else None
        self.publication_cancel.set()
        self.cleanup_cancel.set()
        closed_owned=self.close_after_observer
        try:
            # Orderly shutdown retains ownership until the exact checkpoint
            # observer joins and its result settles through the normal poll path.
            # Public controls keep cancel_saved's bounded drain semantics.
            if self.active_saved is not None:
                if self.active_saved[0]=='checkpoint':
                    self.observation_thread.join()
                    self.poll_direct_terminal()
                else:
                    self.cancel_saved()
            from hermes_downloads.processes import reconcile_process_birth
            # Orderly exit keeps the lease and process claim through original mutator JOIN.
            if self.direct_controller is not None and (self.direct_record is None
                or reconcile_process_birth(self.direct_record.identity)=='current'):
                try:self.direct_controller.close()
                except BaseException:raise IPCStateError('direct_dispatch_blocked') from None
                closed_owned=True
            if self.observation_thread is not None:
                self.observation_thread.join()
                self.join_owned_observation()
            if self.active_cleanup is not None or self.partial_unlink_permit is not None or self.marker_unlink_permit is not None:
                self.revoke_cleanup()
            if (not self.direct_recovery_blocked and self.active_direct_plan is not None and self.active_terminal_plan is not None
                and self.active_saved is None and self.active_publication is None):
                tracked=self.active_direct_plan
                self.close_owned_direct_controller(lambda: self.store.pause_active_direct_job(job_id=tracked.job.job_id,
                    generation=tracked.generation,revision=tracked.revision),_join_observer=True)
                self.seal_owned_stop(context,None,self.store.get_job(tracked.job.job_id),'shutdown')
                if self.active_saved is not None:
                    self.observation_thread.join()
                    self.poll_direct_terminal()
            self.flush_pending_stop()
            if self.active_saved is not None and self.active_saved[0]=='checkpoint':
                self.observation_thread.join();self.poll_direct_terminal()
            self.saved_cancel.set()
            self.publication_cancel.set()
            recoverable = self.active_recovery is not None
            if self.publication_permit is not None:
                snapshot = self.publication_permit.revoke()
                if snapshot.in_flight:
                    self.direct_recovery_blocked = True
                    if self.direct_controller is not None:
                        self.direct_controller.close()
                    raise IPCStateError('direct_dispatch_blocked')
                recoverable = snapshot.creation_attempted
            if not self.direct_recovery_blocked and self.store is not None:
                if self.active_recovery is not None:
                    self.store.abort_exact_publication_recovery(self.active_recovery)
                elif (self.active_publication is not None and self.publication_stage is not None
                    and not self.publication_observed):
                    attempt = self.active_publication
                    self.close_owned_direct_controller(lambda: self.store.pause_active_direct_job(
                        job_id=attempt.job.job_id, generation=attempt.generation,
                        revision=attempt.revision, _publication_recoverable=recoverable))
                elif self.active_preparation is not None and self.active_direct_plan is not None:
                    tracked = self.active_direct_plan
                    self.close_owned_direct_controller(lambda: self.store.pause_active_direct_job(
                        job_id=tracked.job.job_id, generation=tracked.generation,
                        revision=tracked.revision))
            self.active_preparation = None
            self.publication_stage = None
            self.active_recovery = None
            if self.direct_controller is not None:
                if closed_owned and not self.direct_recovery_blocked:
                    self.clear_owned_direct_claim()
                    self.clear_owned_direct_controller_state()
                else:
                    self.shutdown_owned_direct_controller()
        finally:
            if self.observation_thread is not None:
                self.observation_thread.join()
                _direct_observation_busy()


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
    owners=[]
    saved_qualifications={}
    legacy_owner=None
    rotation=0
    file_task=None
    file_cancel=threading.Event()
    file_phases=Queue(maxsize=1)
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
        for job,record,dispatch,token,capability in store._job_engine_owners():
            if record is None:
                direct_recovery_blocked=True
            elif capability is None:
                if reconcile_process_birth(record.identity)=='absent':
                    if not store.clear_direct_engine_record(record):direct_recovery_blocked=True
                else:direct_recovery_blocked=True
            elif _recover_cold_direct_engine(store,record=record):
                direct_recovery_blocked=True

        legacy_owner=_DirectOwner(store,root,blocked=direct_recovery_blocked,saved_qualifications=saved_qualifications)
        owners.append(legacy_owner)

        def direct_engine_activate(command):
            if any(owner is not legacy_owner and not owner.idle() for owner in owners):
                return DirectEngineActivateResult(store.worker_epoch(),'blocked')
            return legacy_owner.direct_engine_activate(command)

        def direct_job_dispatch(command):return legacy_owner.direct_job_dispatch(command)

        def job_control(command):
            matched=[owner for owner in owners if owner.owns_job(command.job) and not owner.idle()]
            if len(matched)>1:raise IPCError('direct_dispatch_blocked')
            return matched[0].job_control(command) if matched else _job_control_from_store(store,command)

        def queue_gate(command):
            if command.gate=='running':
                for owner in owners:
                    if owner.active_saved is not None and owner.active_saved[0]=='checkpoint':
                        owner.observation_thread.join(5)
                        if owner.observation_thread.is_alive():raise IPCError('direct_dispatch_blocked')
                        owner.poll_direct_terminal()
            active=[owner for owner in owners if not owner.idle()]
            if len(active)<=1:return (active[0] if active else legacy_owner).queue_gate(command)
            current=store.publication_queue_control_is_current(request_id=command.request_id,payload_digest=command.payload_digest,expected_revision=command.expected_revision)
            if command.gate!='paused' or not current:return _queue_gate_from_store(store,command)
            result=_queue_gate_from_store(store,command)
            deadline=time.monotonic()+10
            failure=None
            for owner in active:
                owner.close_deadline=deadline
                try:owner.queue_gate(command,_gate_committed=True)
                except BaseException as error:failure=error
                finally:owner.close_deadline=None
            if failure is not None:raise IPCError('direct_dispatch_blocked') from None
            return result

        def apply_allocation(candidate=None, candidate_plan=None):
            snapshot=store.configuration_snapshot();desired=snapshot['desired']
            live=[owner for owner in owners if owner.direct_controller_ready and owner.active_direct_plan is not None]
            if candidate is not None and candidate not in live:live.append(candidate)
            from hermes_downloads.bandwidth import allocate
            shares=allocate(global_limit_bps=None if desired['global_limit_bps'] is None else int(desired['global_limit_bps']),item_caps_bps=[None if (cap:=store._effective_settings(store._connection,(candidate_plan if owner is candidate else owner.active_direct_plan).job)['limit_bps']) is None else int(cap) for owner in live])
            changes=list(zip(live,shares))
            changes.sort(key=lambda pair:pair[1] is None or getattr(pair[0],'applied_share',None) is not None and pair[1]>pair[0].applied_share)
            for owner,share in changes:
                if share==0:raise IPCError('direct_dispatch_blocked')
                plan=candidate_plan if owner is candidate else owner.active_direct_plan
                if getattr(owner,'applied_share',object())!=share:
                    owner.direct_controller.set_global_limit(global_limit_bps=share)
                    owner.direct_controller.set_allocation(job_id=plan.job.job_id,generation=plan.generation,allocation_bps=share)
                    owner.applied_share=share
            return snapshot,live,shares

        control_deadlines={}
        def apply_controls():
            for pending in store._connection.execute("SELECT request_id FROM direct_control_requests WHERE status='pending' ORDER BY rowid").fetchall():
                request=pending[0];intent,command,_=store._control_request(request)
                if command.expected_worker_epoch!=store.worker_epoch():store.apply_captured_cohort(request);continue
                if command.action=='preempt':
                    victim=store._preemption(request)
                    matching=[] if victim is None else [owner for owner in owners if owner.assigned_job==victim['job_id'] and not owner.idle()]
                    if victim is None and len([owner for owner in owners if not owner.idle()])>=int(store.configuration_snapshot()['desired']['concurrency']):
                        eligible=[owner for owner in owners if owner.active_direct_plan is not None and owner.direct_record is not None and owner.active_saved is None and owner.active_direct_plan.job.job_id!=command.job and store.get_job(owner.active_direct_plan.job.job_id).state=='downloading']
                        if not eligible:continue
                        owner=min(eligible,key=lambda owner:store._effective_settings(store._connection,owner.active_direct_plan.job)['priority'])
                        victim=store.reserve_preemption(request,owner.active_direct_plan,owner.direct_record);matching=[owner]
                    deadline=control_deadlines.setdefault(request,time.monotonic()+10)
                    for owner in matching:
                        if owner.active_saved is not None:continue
                        if owner.direct_controller is not None:
                            owner.close_deadline=deadline
                            try:owner.stop_for_configuration(command,stop_kind='preempt')
                            except IPCError:pass
                            finally:owner.close_deadline=None
                    if any(not owner.idle() for owner in matching):continue
                    try:store.finish_preemption(request)
                    except (ValueError,RuntimeError):store._connection.execute("UPDATE direct_control_requests SET status='blocked' WHERE request_id=? AND status='pending'",(request,))
                    continue
                if command.action=='retry':
                    try:store.apply_explicit_retry(request)
                    except (ValueError,RuntimeError):store._connection.execute("UPDATE direct_control_requests SET status='blocked' WHERE request_id=? AND status='pending'",(request,))
                    continue
                if command.action=='start':
                    try:store.apply_captured_cohort(request)
                    except RuntimeError:pass
                    continue
                matching=[owner for owner in owners if not owner.idle() and (owner.assigned_job is not None or owner.active_direct_plan is not None) and store.get_materialized_job(owner.assigned_job if owner.assigned_job is not None else owner.active_direct_plan.job.job_id).queue_collection_id==command.collection]
                if command.action=='pause':
                    deadline=control_deadlines.setdefault(request,time.monotonic()+10)
                    for owner in matching:
                        if owner.active_saved is not None:continue
                        if owner.direct_controller is not None:
                            owner.close_deadline=deadline
                            try:owner.stop_for_configuration(command,stop_kind='collection')
                            except IPCError:pass
                            finally:owner.close_deadline=None
                    if any(not owner.idle() for owner in matching):continue
                else:
                    for member in store._connection.execute('SELECT job_id FROM direct_control_members WHERE request_id=?',(request,)).fetchall():
                        job=store.get_materialized_job(member[0]);current=store._read_job_control_projection(store._connection,member[0]);head=store._capture_target_head(store._connection,member[0]);checkpoint=store._saved_checkpoint(store._connection,member[0])
                        if current.state=='paused' and current.authorized and not current.manual_hold and head is not None and checkpoint is not None and checkpoint[0]['worker_epoch']==store.worker_epoch() and checkpoint[0]['stop_kind']=='collection':
                            resume='collection-resume:'+hashlib.sha256((request+':'+member[0]).encode()).hexdigest()
                            store.apply_target_authorize(TargetAuthorizeCommand(resume,store.worker_epoch(),'resume',dict(kind='jobs',targets=[dict(job=member[0],expected_revision=current.revision)])))
                store._connection.execute("UPDATE direct_control_requests SET status='applied' WHERE request_id=? AND status='pending'",(request,))
                store._connection.execute("UPDATE direct_control_cohorts SET status='applied' WHERE request_id=? AND status='pending'",(request,))

        def poll_files():
            nonlocal file_task
            if file_task is not None:
                request,job,thread,result,permit=file_task
                while not file_phases.empty():
                    index,fact,ack=file_phases.get_nowait()
                    try:store.acknowledge_saved_file_artifact(request,job,index,fact)
                    except Exception:file_cancel.set()
                    finally:ack.set()
                if thread.is_alive():return
                thread.join();_direct_observation_busy()
                store.acknowledge_file_purge(request,job,failed=not result or result[0] is None)
                file_task=None
            if _direct_observation_busy():return
            row=store._connection.execute("SELECT request_id,job_id FROM direct_file_outcomes WHERE status IN ('pending','purge_authorized','saved_purge_authorized') ORDER BY rowid LIMIT 1").fetchone()
            if row is None:return
            request,job=row
            matched=[owner for owner in owners if owner.owns_job(job) and not owner.idle()]
            if matched:
                _,command=store._file_request(store._connection,request)
                if command.action=='purge':
                    deadline=control_deadlines.setdefault('files:'+request,time.monotonic()+10)
                    for owner in matched:
                        if owner.active_saved is not None:continue
                        if owner.direct_controller is not None:
                            owner.close_deadline=deadline
                            try:owner.stop_for_configuration(command,stop_kind='purge')
                            except IPCError:pass
                            finally:owner.close_deadline=None
                return
            plan=store.prepare_file_action(request,job)
            if plan is None:return
            if not _acquire_direct_observation():return
            from hermes_downloads.paths import _UnlinkPermit,_purge_finished_payload
            saved=plan[0]=='saved' if type(plan) is tuple and type(plan[0]) is str else False
            result=[];permit=_UnlinkPermit();end=time.monotonic()+60
            if saved:
                _,pair,records,allow=plan;permits=[_UnlinkPermit() for record in records]
                destination=store._saved_destination(job);reservation=store._effective_reservation(store._connection,job)
            else:claim,allow=plan
            def phase(index,fact):
                ack=threading.Event();file_phases.put_nowait((index,fact,ack))
                while not ack.wait(.02):
                    if file_cancel.is_set() or time.monotonic()>=end:raise ValueError('saved purge cancelled')
            def purge():
                try:
                    if saved:
                        from hermes_downloads.paths import _purge_saved_artifacts
                        result.append(_purge_saved_artifacts(destination,reservation,pair,records,permits=permits,cancelled=lambda:file_cancel.is_set() or time.monotonic()>=end,phase=phase,allow_unlink=allow))
                    else:result.append(_purge_finished_payload(claim.attempt.prepared,claim.certified_stat,permit=permit,cancelled=file_cancel.is_set,allow_unlink=allow))
                except BaseException:result.append(None)
            thread=threading.Thread(target=purge,name='direct-selected-purge',daemon=True)
            file_task=(request,job,thread,result,permit)
            _start_direct_observation(thread)

        def apply_edits():
            for row in store._connection.execute("SELECT request_id,job_id FROM direct_edit_outcomes WHERE status='pending' ORDER BY rowid").fetchall():
                _,command,_=store._edit_request(store._connection,row[0]);job=row[1]
                matched=[owner for owner in owners if owner.owns_job(job) and not owner.idle()]
                if any(owner.active_saved is not None for owner in matched):continue
                live=[owner for owner in matched if owner.direct_controller is not None]
                if live:
                    for owner in live:owner.stop_for_configuration(command,stop_kind='edit')
                    continue
                store.apply_edit(command.request_id,job)

        def apply_sources():
            for row in store._connection.execute("SELECT request_id FROM direct_source_requests WHERE status='pending' ORDER BY rowid").fetchall():
                intent,command=store._source_request(row[0])
                matched=[owner for owner in owners if owner.owns_job(command.id) and not owner.idle()]
                if any(owner.active_saved is not None for owner in matched):continue
                live=[owner for owner in matched if owner.direct_controller is not None]
                if live:
                    for owner in live:owner.stop_for_configuration(command,stop_kind='replace_source')
                    continue
                verified=None
                checkpoint=store._saved_checkpoint(store._connection,command.id)
                origin=store._read_creation_origin(store._connection,command.id,{},target=False)
                if origin.kind=='batch' and origin.batch.expected_sha256 is None and checkpoint is not None:
                    if _direct_observation_busy():continue
                    owner=next((owner for owner in owners if owner.idle()),None)
                    if owner is None:
                        owner=_DirectOwner(store,root,blocked=direct_recovery_blocked,before_body=apply_allocation,saved_qualifications=saved_qualifications)
                        owners.append(owner)
                    stock=store._checkpoint_stock(store._connection,checkpoint[0])
                    source=validate_source_url(command.url)
                    def verify_source(cancelled,phase,source=source,stock=stock):
                        from hermes_downloads.direct import _verify_saved_source
                        _verify_saved_source(source,stock,cancelled=cancelled)
                        return stock
                    owner.assigned_job=command.id
                    owner.start_saved('source',(command,stock),verify_source)
                    continue
                store.apply_source_replacement(command.request_id,_verified_stock=verified)

        def apply_settings():
            snapshot=store.configuration_snapshot();desired=snapshot['desired']
            if snapshot['revision']=='0':return
            active=[owner for owner in owners if owner.direct_controller_ready and owner.active_direct_plan is not None]
            stopping=[owner for index,owner in enumerate(active) if desired['global_limit_bps']=='0'
                or owner.direct_controller._split!=int(desired['connections']) or index>=min(int(desired['concurrency']),int(desired['global_limit_bps']) if desired['global_limit_bps'] is not None else int(desired['concurrency']))]
            if stopping:
                command,epoch=store.configuration_command(revision=int(snapshot['revision']))
                deadline=time.monotonic()+10
                for owner in stopping:
                    owner.close_deadline=deadline
                    try:owner.stop_for_configuration(command)
                    finally:owner.close_deadline=None
                return
            if any(owner.active_saved is not None and owner.active_saved[0]=='checkpoint' for owner in owners):return
            if desired['global_limit_bps']!='0':
                command,epoch=store.configuration_command(revision=int(snapshot['revision'])) if int(snapshot['revision']) else (None,None)
                if command is not None and epoch==store.worker_epoch():
                    for row in store._connection.execute('SELECT job_id,source_request_id FROM stopped_control_bodies WHERE worker_epoch=?',(epoch,)).fetchall():
                        job=store._read_job_control_projection(store._connection,row['job_id']);head=store._capture_target_head(store._connection,row['job_id'])
                        if job.state!='paused' or head.intent_status!='current' or not job.authorized or job.manual_hold:continue
                        if store._latest_body_source(store._connection,row['job_id'])!=row['source_request_id']:continue
                        request='settings-resume:'+__import__('hashlib').sha256((command.request_id+':'+row['source_request_id']).encode()).hexdigest()
                        if store._connection.execute('SELECT 1 FROM target_commands WHERE request_id=?',(request,)).fetchone():continue
                        store.apply_target_authorize(TargetAuthorizeCommand(request,epoch,'resume',dict(kind='jobs',targets=[dict(job=row['job_id'],expected_revision=job.revision)])))
            snapshot,live,shares=apply_allocation()
            if snapshot['status']=='pending' and len(live)<=int(desired['concurrency']):
                if all(owner.direct_controller._split==int(desired['connections']) for owner in live):store.applied_configuration(snapshot)

        legacy_owner.before_body=apply_allocation

        if requested_socket_path is not None:
            try:
                health_server = HealthServer(
                    requested_socket_path,
                    health=lambda: _health_from_store(store),
                    jobs_page=lambda cursor: _jobs_page_from_store(store, cursor),
                    query_list=lambda cursor, deadline: _query_list_from_store(store, cursor, deadline),
                    query_status=lambda id, deadline: _query_status_from_store(store, id, deadline),
                    query_events=lambda cursor, deadline: _query_events_from_store(store, cursor, deadline),
                    notification_pending=lambda command, deadline: store.read_notification_pending(command, deadline=deadline),
                    notification_claim=lambda command, deadline: store.claim_notification(command, deadline=deadline),
                    notification_presented=lambda command, deadline: store.present_notification(command, deadline=deadline),
                    notification_release=lambda command, deadline: store.release_notification(command, deadline=deadline),
                    queue_snapshot=lambda deadline: _queue_snapshot_from_store(store, deadline),
                    queue_gate=queue_gate,
                    job_add=lambda command: _job_add_from_store(store, command),
                    add_batch=lambda command: _add_batch_from_store(store, command),
                    target_authorize=lambda command: _target_authorize_from_store(store, command),
                    configure=lambda command,deadline: store.accept_configuration(command,deadline=deadline),
                    query_settings=lambda deadline: store.configuration_snapshot(deadline=deadline),
                    replace_source=lambda command,deadline:store.accept_source_replacement(command,deadline=deadline),
                    edit=lambda command,deadline:store.accept_edit(command,deadline=deadline),
                    files=lambda command,deadline:store.accept_files(command,deadline=deadline),
                    files_bridge=lambda command,deadline:store.file_bridge(command,deadline=deadline),
                    control_intent=lambda command,deadline:store.accept_control_intent(command,deadline=deadline),
                    query_control=lambda request,deadline:store.control_snapshot(request,deadline=deadline),
                    query_detail=lambda job,deadline:store.source_detail(job,deadline=deadline),
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
                    apply_controls()
                    poll_files()
                    apply_edits()
                    apply_sources()
                    apply_settings()
                    desired=store.configuration_snapshot()['desired']
                    capacity=min(int(desired['concurrency']),int(desired['global_limit_bps']) if desired['global_limit_bps'] is not None else int(desired['concurrency']))
                    spare=next((owner for owner in owners if owner.idle()),None)
                    if sum(not owner.idle() for owner in owners)<capacity:
                        if spare is None:
                            spare=_DirectOwner(store,root,blocked=direct_recovery_blocked,before_body=apply_allocation,saved_qualifications=saved_qualifications);owners.append(spare)
                        spare.poll_target_dispatch()
                    rotation=(rotation+1)%len(owners)
                    for owner in owners[rotation:]+owners[:rotation]:owner.poll_direct_terminal()
        return None
    finally:
        try:
            try:
                failure=None
                file_cancel.set()
                if file_task is not None:
                    file_task[4].revoke();file_task[2].join();poll_files()
                for owner in owners:
                    if owner.active_saved is None or owner.active_saved[0] != 'checkpoint':
                        owner.saved_cancel.set()
                    owner.publication_cancel.set();owner.cleanup_cancel.set()
                for owner in owners:
                    try:owner.shutdown()
                    except BaseException as error:failure=error
                if failure is not None:raise failure
            finally:
                try:
                    if health_server is not None:
                        health_server.close()
                        if owned_endpoint_certificate is not None:endpoint_ownership.clear(root,owned_endpoint_certificate)
                finally:
                    if store is not None:store.close()
        finally:
            try:
                if lease_descriptor is not None:_release_worker_lease(lease_descriptor)
            finally:stopped_event.set()


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
