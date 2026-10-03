"""Real worker-owned AF_UNIX health IPC coverage."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import multiprocessing
import os
import secrets
from pathlib import Path
from queue import Empty
import signal
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import types
from typing import Any, Callable, Iterator, Protocol, cast

import pytest

from hermes_downloads import ipc, worker
from hermes_downloads.ipc import (
    DirectEngineActivateCommand,
    DirectEngineActivateResult,
    MAX_MESSAGE_BYTES,
    JobsPage,
    PublicJobRecord,
    activate_direct_engine,
    request_health,
    request_jobs_page,
    set_queue_gate,
)
from hermes_downloads.models import DownloadIntent, MaterializedJob, SourceKind
from hermes_downloads.processes import ProcessBirthIdentity, reconcile_process_birth
from hermes_downloads.store import (
    DirectEngineActivationFence,
    DirectEngineRecord,
    SQLiteStore,
    _DirectEngineRecoveryCapability,
)


_WATCHDOG_SECONDS = 5.0

_FIXTURE_SPEC = importlib.util.spec_from_file_location(
    "_ipc_http_origin",
    Path(__file__).resolve().parents[1] / "fixtures" / "http_origin.py",
)
assert _FIXTURE_SPEC is not None and _FIXTURE_SPEC.loader is not None
_FIXTURE_MODULE = importlib.util.module_from_spec(_FIXTURE_SPEC)
sys.modules[_FIXTURE_SPEC.name] = _FIXTURE_MODULE
_FIXTURE_SPEC.loader.exec_module(_FIXTURE_MODULE)


def _origin_type() -> type[Any]:
    origin_type = getattr(_FIXTURE_MODULE, "SyntheticHttpOrigin", None)
    assert origin_type is not None, "fixture must expose SyntheticHttpOrigin"
    return origin_type


def _direct_module() -> Any:
    return importlib.import_module("hermes_downloads.direct")


@pytest.fixture
def short_socket_root() -> Iterator[Path]:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        yield Path(temporary_root)


class _JoinedProcess(Protocol):
    @property
    def exitcode(self) -> int | None: ...

    def is_alive(self) -> bool: ...

    def join(self, timeout: float | None = None) -> None: ...

    def terminate(self) -> None: ...


class _LifecycleEvent(Protocol):
    def set(self) -> None: ...

    def wait(self, timeout: float | None = None) -> bool: ...


class _ImportGateEvent(_LifecycleEvent, Protocol):
    def is_set(self) -> bool: ...


def _run_worker_process(
    state_root: str,
    socket_path: str,
    ready_event: object,
    shutdown_event: object,
    stopped_event: object,
    results: object,
) -> None:
    try:
        outcome = worker.run_worker(
            Path(state_root),
            socket_path=Path(socket_path),
            ready_event=ready_event,
            shutdown_event=shutdown_event,
            stopped_event=stopped_event,
        )
    except BaseException as error:
        results.put(("error", type(error).__name__, str(error)))
    else:
        results.put(("result", outcome))


def _run_worker_process_with_engine_imports_gated(
    state_root: str,
    socket_path: str,
    ready_event: object,
    shutdown_event: object,
    stopped_event: object,
    results: object,
    direct_import_allowed: _ImportGateEvent,
    bound_before_ready: _LifecycleEvent | None = None,
    release_rpc_ready: _LifecycleEvent | None = None,
    reap_child_exits: bool = False,
    fence_before_direct_import: _LifecycleEvent | None = None,
    release_direct_import: _LifecycleEvent | None = None,
    forbid_direct_group_signal: _ImportGateEvent | None = None,
    direct_group_signal_attempted: _LifecycleEvent | None = None,
) -> None:
    """Reject cold engine imports and optionally hold direct RPC readiness."""

    import builtins

    original_import = builtins.__import__
    direct_patched = False
    direct_signal_patched = False
    bound_event = bound_before_ready
    release_event = release_rpc_ready
    original_sigchld: Any | None = None

    if reap_child_exits:

        def reap_child_exit(_signal_number: int, _frame: object) -> None:
            while True:
                try:
                    child_pid, _status = os.waitpid(-1, os.WNOHANG)
                except ChildProcessError:
                    return
                if child_pid == 0:
                    return

        original_sigchld = signal.signal(signal.SIGCHLD, reap_child_exit)

    def guarded_import(
        name: str,
        globals: object = None,
        locals: object = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        nonlocal direct_patched, direct_signal_patched
        if name in {"hermes_downloads.direct", "hermes_downloads.video"}:
            if name == "hermes_downloads.video" or not direct_import_allowed.is_set():
                raise AssertionError("worker imported an engine outside direct activation")
            if name == "hermes_downloads.direct" and fence_before_direct_import is not None:
                assert release_direct_import is not None
                observer = SQLiteStore(Path(state_root) / "state.db")
                try:
                    fence = observer.get_direct_engine_activation_fence()
                    assert fence is not None
                    assert fence.worker_epoch == observer.worker_epoch()
                    assert observer.get_direct_engine_record() is None
                finally:
                    observer.close()
                fence_before_direct_import.set()
                if not release_direct_import.wait(_WATCHDOG_SECONDS):
                    raise RuntimeError("test did not release direct engine import")
            imported = cast(Any, original_import)(
                name, globals, locals, fromlist, level
            )
            if (
                name == "hermes_downloads.direct"
                and forbid_direct_group_signal is not None
                and not direct_signal_patched
            ):
                group_signal_guard = cast(
                    _ImportGateEvent, forbid_direct_group_signal
                )
                direct_module = sys.modules["hermes_downloads.direct"]
                original_killpg = direct_module.os.killpg

                def guarded_killpg(
                    process_group_id: int, signal_number: signal.Signals
                ) -> None:
                    if group_signal_guard.is_set():
                        if direct_group_signal_attempted is not None:
                            direct_group_signal_attempted.set()
                        raise AssertionError(
                            "stale absent direct controller called os.killpg"
                        )
                    original_killpg(process_group_id, signal_number)

                direct_module.os.killpg = guarded_killpg
                direct_signal_patched = True
            if (
                name == "hermes_downloads.direct"
                and bound_event is not None
                and not direct_patched
            ):
                assert release_event is not None
                ready_bound_event = cast(_LifecycleEvent, bound_event)
                ready_release_event = cast(_LifecycleEvent, release_event)
                direct_module = sys.modules["hermes_downloads.direct"]
                controller_type = direct_module.DirectAria2Controller
                original_wait_for_rpc_ready = controller_type._wait_for_rpc_ready

                def wait_for_rpc_ready(controller: object) -> None:
                    ready_bound_event.set()
                    if not ready_release_event.wait(_WATCHDOG_SECONDS):
                        raise RuntimeError("test did not release direct RPC readiness")
                    original_wait_for_rpc_ready(controller)

                controller_type._wait_for_rpc_ready = wait_for_rpc_ready
                direct_patched = True
            return imported
        return cast(Any, original_import)(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import
    try:
        _run_worker_process(
            state_root,
            socket_path,
            ready_event,
            shutdown_event,
            stopped_event,
            results,
        )
    finally:
        builtins.__import__ = original_import
        if original_sigchld is not None:
            signal.signal(signal.SIGCHLD, original_sigchld)


def _run_worker_process_with_retrying_absent_private_cleanup(
    state_root: str,
    socket_path: str,
    ready_event: object,
    shutdown_event: object,
    stopped_event: object,
    results: object,
    cleanup_attempts: Any,
    cleanup_failed: _LifecycleEvent,
    cleanup_succeeded: _LifecycleEvent,
    no_signal_or_rpc_guard: _ImportGateEvent,
    normal_close_attempted: _LifecycleEvent,
    rpc_attempted: _LifecycleEvent,
    group_signal_attempted: _LifecycleEvent,
    process_signal_attempted: _LifecycleEvent,
) -> None:
    """Fail one local discard cleanup while guarding stale-owner side effects."""

    direct_module = _direct_module()
    controller_type = direct_module.DirectAria2Controller
    original_remove_private_runtime = direct_module._remove_private_runtime
    original_rpc = controller_type._rpc
    original_close = controller_type.close
    original_killpg = direct_module.os.killpg
    original_kill = direct_module.os.kill

    def fail_private_cleanup_once(path: Path) -> None:
        cleanup_attempts.value += 1
        if cleanup_attempts.value == 1:
            cleanup_failed.set()
            raise direct_module.DirectEngineError(
                "fixture transient absent private cleanup failure"
            )
        original_remove_private_runtime(path)
        cleanup_succeeded.set()

    def guarded_rpc(controller: object, *args: Any, **kwargs: Any) -> Any:
        if no_signal_or_rpc_guard.is_set():
            rpc_attempted.set()
            raise AssertionError("absent owner discard issued aria2 RPC")
        return original_rpc(controller, *args, **kwargs)

    def guarded_close(controller: object) -> None:
        if no_signal_or_rpc_guard.is_set():
            normal_close_attempted.set()
            raise AssertionError("absent owner discard used normal close")
        original_close(controller)

    def guarded_killpg(process_group_id: int, signal_number: signal.Signals) -> None:
        if no_signal_or_rpc_guard.is_set():
            group_signal_attempted.set()
            raise AssertionError("absent owner discard signaled a process group")
        original_killpg(process_group_id, signal_number)

    def guarded_kill(process_id: int, signal_number: int) -> None:
        if no_signal_or_rpc_guard.is_set():
            process_signal_attempted.set()
            raise AssertionError("absent owner discard signaled a process")
        original_kill(process_id, signal_number)

    def reap_child_exits(_signal_number: int, _frame: object) -> None:
        while True:
            try:
                child_pid, _status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return
            if child_pid == 0:
                return

    original_sigchld = signal.signal(signal.SIGCHLD, reap_child_exits)
    direct_module._remove_private_runtime = fail_private_cleanup_once
    controller_type._rpc = guarded_rpc
    controller_type.close = guarded_close
    direct_module.os.killpg = guarded_killpg
    direct_module.os.kill = guarded_kill
    try:
        _run_worker_process(
            state_root,
            socket_path,
            ready_event,
            shutdown_event,
            stopped_event,
            results,
        )
    finally:
        direct_module._remove_private_runtime = original_remove_private_runtime
        controller_type._rpc = original_rpc
        controller_type.close = original_close
        direct_module.os.killpg = original_killpg
        direct_module.os.kill = original_kill
        signal.signal(signal.SIGCHLD, original_sigchld)


def _run_worker_process_with_fake_direct(
    state_root: str,
    socket_path: str,
    ready_event: object,
    shutdown_event: object,
    stopped_event: object,
    results: object,
    fake_identity: ProcessBirthIdentity,
    start_mode: str,
    close_mode: str,
    controller_constructed: _LifecycleEvent,
    start_called: _LifecycleEvent,
    callback_entered: _LifecycleEvent,
    callback_completed: _LifecycleEvent,
    close_called: _LifecycleEvent,
    persistence_attempted: _LifecycleEvent | None = None,
    release_after_callback: _LifecycleEvent | None = None,
    controller_construction_count: Any | None = None,
    fence_before_construction: _LifecycleEvent | None = None,
    release_before_start: _LifecycleEvent | None = None,
    shutdown_reconciliation: str | None = None,
    shutdown_reconciliation_called: _LifecycleEvent | None = None,
    capability_persistence_attempted: _LifecycleEvent | None = None,
) -> None:
    """Install a deterministic direct controller before the worker imports it lazily."""

    if start_mode not in {"succeeds", "fails_after_callback"}:
        raise AssertionError("invalid fake direct start mode")
    if close_mode not in {"succeeds", "fails"}:
        raise AssertionError("invalid fake direct close mode")
    if shutdown_reconciliation not in {None, "absent", "current", "indeterminate"}:
        raise AssertionError("invalid fake shutdown reconciliation")

    original_bind_direct_engine_activation_fence = (
        worker.SQLiteStore.bind_direct_engine_activation_fence
    )
    original_bind_direct_engine_recovery_capability = (
        worker.SQLiteStore._bind_direct_engine_recovery_capability
    )
    processes: Any | None = None
    original_reconcile_process_birth: Any | None = None
    if shutdown_reconciliation is not None:
        processes = importlib.import_module("hermes_downloads.processes")
        original_reconcile_process_birth = processes.reconcile_process_birth

        def reconcile_for_shutdown(identity: object) -> str:
            if identity == fake_identity:
                if shutdown_reconciliation_called is not None:
                    shutdown_reconciliation_called.set()
                return shutdown_reconciliation
            assert original_reconcile_process_birth is not None
            return original_reconcile_process_birth(identity)

        processes.reconcile_process_birth = reconcile_for_shutdown
    if persistence_attempted is not None:

        def fail_direct_record_persistence(
            self: SQLiteStore,
            fence: DirectEngineActivationFence,
            record: DirectEngineRecord,
        ) -> None:
            if (
                type(self) is not SQLiteStore
                or type(fence) is not DirectEngineActivationFence
                or type(record) is not DirectEngineRecord
            ):
                raise AssertionError("worker passed an invalid direct activation binding")
            persistence_attempted.set()
            raise RuntimeError("fake direct-record persistence failure")

        worker.SQLiteStore.bind_direct_engine_activation_fence = (
            fail_direct_record_persistence
        )
    if capability_persistence_attempted is not None:

        def fail_recovery_capability_persistence(
            self: SQLiteStore,
            record: DirectEngineRecord,
            capability: _DirectEngineRecoveryCapability,
        ) -> None:
            if (
                type(self) is not SQLiteStore
                or type(record) is not DirectEngineRecord
                or type(capability) is not _DirectEngineRecoveryCapability
            ):
                raise AssertionError("worker passed an invalid direct recovery capability")
            capability_persistence_attempted.set()
            raise RuntimeError("fake direct recovery-capability persistence failure")

        worker.SQLiteStore._bind_direct_engine_recovery_capability = (
            fail_recovery_capability_persistence
        )

    class FakeDirectAria2Controller:
        def __init__(
            self,
            *,
            runtime_root: Path,
            on_engine_bound: Callable[[ProcessBirthIdentity], None],
        ) -> None:
            if not isinstance(runtime_root, Path) or not callable(on_engine_bound):
                raise AssertionError("worker did not construct the direct controller safely")
            if fence_before_construction is not None:
                observer = SQLiteStore(Path(state_root) / "state.db")
                try:
                    fence = observer.get_direct_engine_activation_fence()
                    assert fence is not None
                    assert fence.worker_epoch == observer.worker_epoch()
                    assert observer.get_direct_engine_record() is None
                finally:
                    observer.close()
                fence_before_construction.set()
            self._on_engine_bound = on_engine_bound
            self._capability = _DirectEngineRecoveryCapability(
                rpc_port=43123,
                rpc_secret=secrets.token_urlsafe(32),
            )
            if controller_construction_count is not None:
                controller_construction_count.value += 1
            controller_constructed.set()
            if release_before_start is not None and not release_before_start.wait(
                _WATCHDOG_SECONDS
            ):
                raise RuntimeError("test did not release fake direct start")

        def start(self) -> object:
            start_called.set()
            callback_entered.set()
            self._on_engine_bound(fake_identity)
            callback_completed.set()
            if start_mode == "fails_after_callback":
                if release_after_callback is not None and not release_after_callback.wait(
                    _WATCHDOG_SECONDS
                ):
                    raise RuntimeError("test did not release fake direct start failure")
                raise RuntimeError("fake direct start failure")
            return object()

        def _recovery_capability(self) -> _DirectEngineRecoveryCapability:
            return self._capability

        def close(self) -> None:
            close_called.set()
            if close_mode == "fails":
                raise RuntimeError("fake direct close failure")

    fake_module = types.ModuleType("hermes_downloads.direct")
    setattr(fake_module, "DirectAria2Controller", FakeDirectAria2Controller)
    sys.modules["hermes_downloads.direct"] = fake_module
    package = importlib.import_module("hermes_downloads")
    setattr(package, "direct", fake_module)
    try:
        _run_worker_process(
            state_root,
            socket_path,
            ready_event,
            shutdown_event,
            stopped_event,
            results,
        )
    finally:
        worker.SQLiteStore.bind_direct_engine_activation_fence = (
            original_bind_direct_engine_activation_fence
        )
        worker.SQLiteStore._bind_direct_engine_recovery_capability = (
            original_bind_direct_engine_recovery_capability
        )
        if processes is not None:
            assert original_reconcile_process_birth is not None
            processes.reconcile_process_birth = original_reconcile_process_birth


def _run_worker_process_with_absent_fake_direct(
    state_root: str,
    socket_path: str,
    ready_event: object,
    shutdown_event: object,
    stopped_event: object,
    results: object,
    fake_identity: ProcessBirthIdentity,
    discard_fails: bool,
    controller_construction_count: Any,
    reconciliation_called: _LifecycleEvent,
    discard_called: _LifecycleEvent,
    stale_close_guard: _ImportGateEvent,
    stale_close_attempted: _LifecycleEvent,
    replacement_claim_cleared: _LifecycleEvent,
    reconciliation: str = "absent",
    replacement_identity: ProcessBirthIdentity | None = None,
    clear_failures: int = 0,
    clear_attempts: Any | None = None,
    discard_attempts: Any | None = None,
) -> None:
    """Install a controlled reconciled-owner controller for direct lifecycle tests."""

    if reconciliation not in {"absent", "current", "indeterminate"}:
        raise AssertionError("invalid fake reconciliation")
    if replacement_identity is not None and (
        type(replacement_identity) is not ProcessBirthIdentity
        or replacement_identity == fake_identity
    ):
        raise AssertionError("invalid fake replacement identity")
    if type(clear_failures) is not int or clear_failures < 0:
        raise AssertionError("invalid fake clear failure count")

    processes: Any = importlib.import_module("hermes_downloads.processes")
    original_reconcile_process_birth = processes.reconcile_process_birth
    original_clear_direct_engine_record_and_recovery_capability = (
        worker.SQLiteStore._clear_direct_engine_record_and_recovery_capability
    )
    expected_record = DirectEngineRecord(worker_epoch=1, identity=fake_identity)
    expected_capability: _DirectEngineRecoveryCapability | None = None
    controller_index = 0
    clear_attempt_count = 0
    clear_patched = clear_failures != 0 or clear_attempts is not None

    class FakeDirectAria2Controller:
        def __init__(
            self,
            *,
            runtime_root: Path,
            on_engine_bound: Callable[[ProcessBirthIdentity], None],
        ) -> None:
            nonlocal controller_index

            if not isinstance(runtime_root, Path) or not callable(on_engine_bound):
                raise AssertionError("worker did not construct the direct controller safely")
            self._index = controller_index
            controller_index += 1
            controller_construction_count.value += 1
            self._on_engine_bound = on_engine_bound
            self._capability = _DirectEngineRecoveryCapability(
                rpc_port=43123 + self._index,
                rpc_secret=secrets.token_urlsafe(32),
            )
            if self._index == 1:
                observer = SQLiteStore(Path(state_root) / "state.db")
                try:
                    assert observer.get_direct_engine_record() is None
                    fence = observer.get_direct_engine_activation_fence()
                    assert fence is not None
                    assert fence.worker_epoch == observer.worker_epoch()
                finally:
                    observer.close()
                replacement_claim_cleared.set()

        def start(self) -> object:
            nonlocal expected_capability

            self._on_engine_bound(
                fake_identity
                if self._index == 0 or replacement_identity is None
                else replacement_identity
            )
            if self._index == 0:
                expected_capability = self._capability
            return object()

        def _recovery_capability(self) -> _DirectEngineRecoveryCapability:
            return self._capability

        def discard_absent(self) -> None:
            if self._index != 0:
                raise AssertionError("worker discarded a fresh direct controller")
            discard_called.set()
            if discard_attempts is not None:
                discard_attempts.value += 1
            if discard_fails:
                raise RuntimeError("fake direct absent discard failure")

        def close(self) -> None:
            if self._index == 0 and stale_close_guard.is_set():
                stale_close_attempted.set()
                raise AssertionError("stale absent direct controller used normal close")

    def reconcile_owned(identity: object) -> str:
        if identity == fake_identity:
            reconciliation_called.set()
            return reconciliation
        if identity == replacement_identity:
            return "current"
        return original_reconcile_process_birth(identity)

    if clear_patched:

        def clear_exact_direct_claim(
            self: SQLiteStore,
            record: DirectEngineRecord,
            capability: _DirectEngineRecoveryCapability,
        ) -> bool:
            nonlocal clear_attempt_count

            if (
                type(self) is not SQLiteStore
                or record != expected_record
                or capability != expected_capability
            ):
                raise AssertionError("worker did not compare-clear its exact direct claim")
            clear_attempt_count += 1
            if clear_attempts is not None:
                clear_attempts.value += 1
            if clear_attempt_count <= clear_failures:
                return False
            return original_clear_direct_engine_record_and_recovery_capability(
                self, record, capability
            )

        worker.SQLiteStore._clear_direct_engine_record_and_recovery_capability = (
            clear_exact_direct_claim
        )

    fake_module = types.ModuleType("hermes_downloads.direct")
    setattr(fake_module, "DirectAria2Controller", FakeDirectAria2Controller)
    sys.modules["hermes_downloads.direct"] = fake_module
    package = importlib.import_module("hermes_downloads")
    setattr(package, "direct", fake_module)
    processes.reconcile_process_birth = reconcile_owned
    try:
        _run_worker_process(
            state_root,
            socket_path,
            ready_event,
            shutdown_event,
            stopped_event,
            results,
        )
    finally:
        processes.reconcile_process_birth = original_reconcile_process_birth
        if clear_patched:
            worker.SQLiteStore._clear_direct_engine_record_and_recovery_capability = (
                original_clear_direct_engine_record_and_recovery_capability
            )


def _synthetic_birth(*, owner_uid: int) -> ProcessBirthIdentity:
    return ProcessBirthIdentity(
        leader_pid=999_991,
        process_group_id=999_991,
        session_id=999_991,
        owner_uid=owner_uid,
        started_unix_us=1,
        argv_sha256="a" * 64,
    )


def _fake_unowned_birth() -> ProcessBirthIdentity:
    """Return a structurally valid identity for a fake that starts no process group."""

    identity = ProcessBirthIdentity(
        leader_pid=2_000_000_000,
        process_group_id=2_000_000_000,
        session_id=2_000_000_000,
        owner_uid=os.geteuid(),
        started_unix_us=1,
        argv_sha256="b" * 64,
    )
    assert ProcessBirthIdentity.from_record(identity.to_record()) == identity
    assert _group_is_gone(identity.process_group_id)
    return identity


def _seed_epoch_one_direct_record(
    state_root: Path, record: DirectEngineRecord
) -> None:
    store = SQLiteStore(state_root / "state.db")
    try:
        assert store.recover_cold_start() == 1
        store.set_direct_engine_record(record)
    finally:
        store.close()


def _direct_engine_state_snapshot(
    state_root: Path,
) -> tuple[int | None, str | None, tuple[tuple[object, ...], ...]]:
    """Read direct-engine record fields without relying on worker-local state."""

    store = SQLiteStore(state_root / "state.db")
    try:
        rows = store._connection.execute(
            """
            SELECT
                engine_kind,
                worker_epoch,
                leader_pid,
                process_group_id,
                session_id,
                owner_uid,
                started_unix_us,
                argv_sha256
            FROM engine_instances
            ORDER BY engine_kind
            """
        ).fetchall()
        return (
            store.worker_epoch(),
            store.queue_gate(),
            tuple(tuple(row) for row in rows),
        )
    finally:
        store.close()


def _direct_engine_claims(
    state_root: Path,
) -> tuple[DirectEngineRecord | None, DirectEngineActivationFence | None]:
    """Read the mutually exclusive durable direct-engine lifecycle claim."""

    store = SQLiteStore(state_root / "state.db")
    try:
        return (
            store.get_direct_engine_record(),
            store.get_direct_engine_activation_fence(),
        )
    finally:
        store.close()


def _recorded_direct_controller(
    state_root: Path,
) -> tuple[Any, DirectEngineRecord]:
    """Start a real foreign blank daemon and persist its callback birth."""

    store = SQLiteStore(state_root / "state.db")
    records: list[DirectEngineRecord] = []
    try:
        assert store.recover_cold_start() == 1

        def on_engine_bound(identity: ProcessBirthIdentity) -> None:
            record = DirectEngineRecord(worker_epoch=1, identity=identity)
            store.set_direct_engine_record(record)
            records.append(record)

        controller = _direct_module().DirectAria2Controller(
            runtime_root=state_root / "foreign-direct-runtime",
            on_engine_bound=on_engine_bound,
        )
        try:
            controller.start()
        except BaseException:
            try:
                controller.close()
            finally:
                raise
        assert len(records) == 1
        return controller, records[0]
    finally:
        store.close()


def test_worker_cold_recovery_shuts_down_current_paired_direct_engine_before_ready() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        controller, record = _recorded_direct_controller(state_root)
        capability = controller._recovery_capability()
        process_group_id = record.identity.process_group_id
        store = SQLiteStore(state_root / "state.db")
        try:
            store._bind_direct_engine_recovery_capability(record, capability)
        finally:
            store.close()

        socket_path = state_root / "worker.sock"
        reaped = threading.Event()

        def reap_foreign_owner_after_shutdown() -> None:
            # The test process remains the aria2 parent, unlike a crashed
            # worker. Reap only after recovery asks aria2 to exit so its group
            # can be proven absent without sending a stale-PGID signal.
            owned_process = controller._process
            assert owned_process is not None
            deadline = time.monotonic() + _WATCHDOG_SECONDS
            while time.monotonic() < deadline:
                if owned_process.poll() is not None:
                    reaped.set()
                    return
                time.sleep(0.01)

        reaper = threading.Thread(target=reap_foreign_owner_after_shutdown)
        reaper.start()
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        process = context.Process(
            target=_run_worker_process,
            args=(str(state_root), str(socket_path), ready, shutdown, stopped, results),
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert reaped.wait(_WATCHDOG_SECONDS)
            _assert_group_gone(process_group_id)
            health_record = request_health(socket_path).to_record()
            assert health_record == {
                "protocol_version": 1,
                "worker_epoch": 2,
                "queue_gate": "paused",
            }
            if capability.rpc_secret in json.dumps(health_record):
                pytest.fail("health response exposed a private recovery capability")

            observer = SQLiteStore(state_root / "state.db")
            try:
                assert observer.get_direct_engine_record() is None
                assert observer._get_direct_engine_recovery_capability(record) is None
            finally:
                observer.close()

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)
            reaper.join(_WATCHDOG_SECONDS)
            assert not reaper.is_alive()
            if controller.engine_identity is not None:
                try:
                    controller.discard_absent()
                except BaseException:
                    _force_stop_group(process_group_id)


def _group_is_gone(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def _assert_group_gone(process_group_id: int) -> None:
    deadline = time.monotonic() + _WATCHDOG_SECONDS
    while time.monotonic() < deadline:
        if _group_is_gone(process_group_id):
            return
        time.sleep(0.01)
    pytest.fail("aria2 process group survived worker cleanup")


def _force_stop_group(process_group_id: int) -> None:
    try:
        os.killpg(process_group_id, signal.SIGKILL)
    except ProcessLookupError:
        return
    except PermissionError:
        return
    _assert_group_gone(process_group_id)


def _result(results: object) -> tuple[object, ...]:
    try:
        return results.get(timeout=_WATCHDOG_SECONDS)
    except Empty:
        pytest.fail("worker process did not report an outcome")


def _join(process: _JoinedProcess) -> None:
    process.join(_WATCHDOG_SECONDS)
    if process.is_alive():
        process.terminate()
        process.join(_WATCHDOG_SECONDS)
        pytest.fail("worker process did not stop after its shutdown handshake")
    assert process.exitcode == 0


def _raw_request(
    socket_path: Path, payload: bytes | tuple[bytes, ...]
) -> dict[str, object]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(_WATCHDOG_SECONDS)
        client.connect(str(socket_path))
        chunks = (payload,) if isinstance(payload, bytes) else payload
        for chunk in chunks:
            try:
                client.sendall(chunk)
            except (BrokenPipeError, ConnectionResetError):
                # Oversized input may be rejected as soon as the worker reads its
                # bounded prefix, before this client finishes writing it.
                break
        try:
            client.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        response = bytearray()
        while not response.endswith(b"\n"):
            chunk = client.recv(4096)
            assert chunk
            response.extend(chunk)
            assert len(response) <= MAX_MESSAGE_BYTES
    return json.loads(response)


def _serve_one(
    server: ipc.HealthServer, request: Callable[[], object]
) -> object:
    """Issue one client request while the test owns one server dispatch."""

    results: list[object] = []
    errors: list[BaseException] = []

    def run() -> None:
        try:
            results.append(request())
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        server.serve_once()
    finally:
        thread.join(_WATCHDOG_SECONDS)
    assert not thread.is_alive()
    if errors:
        raise errors[0]
    assert len(results) == 1
    return results[0]


def _start_response_server(
    socket_path: Path, response: bytes, requests: list[bytes | None]
) -> tuple[threading.Thread, list[BaseException]]:
    """Serve exactly one raw response so client decoding is exercised end-to-end."""

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(socket_path))
    listener.listen(1)
    errors: list[BaseException] = []

    def run() -> None:
        try:
            connection, _address = listener.accept()
            with connection:
                connection.settimeout(_WATCHDOG_SECONDS)
                requests.append(ipc._read_line(connection))
                connection.sendall(response)
        except BaseException as error:
            errors.append(error)
        finally:
            listener.close()

    thread = threading.Thread(target=run)
    thread.start()
    return thread, errors


def test_direct_engine_activate_client_uses_exact_typed_envelope(
    short_socket_root: Path,
) -> None:
    socket_path = short_socket_root / "worker.sock"
    commands: list[DirectEngineActivateCommand] = []

    def direct_engine_activate(
        command: DirectEngineActivateCommand,
    ) -> DirectEngineActivateResult:
        commands.append(command)
        return DirectEngineActivateResult(worker_epoch=7, status="active")

    server = ipc.HealthServer(
        socket_path,
        health=lambda: ipc.WorkerHealth(worker_epoch=7, queue_gate="paused"),
        direct_engine_activate=direct_engine_activate,
    )
    try:
        assert _serve_one(
            server,
            lambda: activate_direct_engine(socket_path, expected_worker_epoch=7),
        ) == DirectEngineActivateResult(worker_epoch=7, status="active")
    finally:
        server.close()

    assert [command.to_record() for command in commands] == [
        {"op": "direct_engine_activate", "expected_worker_epoch": 7}
    ]


@pytest.mark.parametrize("status", ("active", "blocked", "stale_epoch"))
def test_direct_engine_activate_response_is_closed_and_status_limited(
    status: str,
) -> None:
    response = DirectEngineActivateResult(worker_epoch=7, status=status)
    assert response.to_record() == {"worker_epoch": 7, "status": status}
    assert DirectEngineActivateResult.from_record(response.to_record()) == response

    for record in (
        {"worker_epoch": 7},
        {"worker_epoch": 7, "status": "unexpected"},
        {"worker_epoch": 7, "status": status, "pid": 123},
        {"worker_epoch": 0, "status": status},
        {"worker_epoch": True, "status": status},
    ):
        with pytest.raises(ipc.IPCError, match="^ipc_response_invalid$"):
            DirectEngineActivateResult.from_record(record)


def test_direct_engine_activate_server_rejects_invalid_requests_without_invoking_handlers(
    short_socket_root: Path,
) -> None:
    socket_path = short_socket_root / "worker.sock"
    commands: list[DirectEngineActivateCommand] = []

    def direct_engine_activate(
        command: DirectEngineActivateCommand,
    ) -> DirectEngineActivateResult:
        commands.append(command)
        return DirectEngineActivateResult(worker_epoch=7, status="active")

    server = ipc.HealthServer(
        socket_path,
        health=lambda: ipc.WorkerHealth(worker_epoch=7, queue_gate="paused"),
        direct_engine_activate=direct_engine_activate,
    )
    try:
        for payload in (
            b'{"op":"direct_engine_activate"}\n',
            b'{"op":"direct_engine_activate","expected_worker_epoch":0}\n',
            b'{"op":"direct_engine_activate","expected_worker_epoch":true}\n',
            b'{"op":"direct_engine_activate","expected_worker_epoch":1.0}\n',
            b'{"op":"direct_engine_activate","expected_worker_epoch":1,"path":"/private"}\n',
            b'{"op":"direct_engine_activate","expected_worker_epoch":1,"executable":"aria2c"}\n',
            b'{"op":"direct_engine_activate","expected_worker_epoch":1,"runtime":"private"}\n',
            b'{"op":"direct_engine_activate","expected_worker_epoch":1,"secret":"secret"}\n',
            b'{"op":"direct_engine_activate","expected_worker_epoch":1,"url":"https://example.test"}\n',
            b'{"op":"direct_engine_activate","expected_worker_epoch":1,"gid":"1234"}\n',
            b'{"op":"direct_engine_activate","expected_worker_epoch":1,"request_id":"request"}\n',
            b'{"op":"direct_engine_activate","expected_worker_epoch":1,"payload_digest":"a"}\n',
            b'{"op":"direct_engine_activate","expected_worker_epoch":1,"expected_worker_epoch":2}\n',
        ):
            assert _serve_one(
                server, lambda payload=payload: _raw_request(socket_path, payload)
            ) == {"error": "invalid_request"}
    finally:
        server.close()
    assert commands == []

    other_handler_calls: list[object] = []

    def queue_gate(command: ipc.QueueGateCommand) -> ipc.QueueGateResult:
        other_handler_calls.append(command)
        return ipc.QueueGateResult(applied=True, queue_gate="running", revision=1)

    socket_path = short_socket_root / "without-handler.sock"
    server = ipc.HealthServer(
        socket_path,
        health=lambda: ipc.WorkerHealth(worker_epoch=7, queue_gate="paused"),
        queue_gate=queue_gate,
    )
    try:
        assert _serve_one(
            server,
            lambda: _raw_request(
                socket_path,
                b'{"op":"direct_engine_activate","expected_worker_epoch":7}\n',
            ),
        ) == {"error": "invalid_request"}
    finally:
        server.close()
    assert other_handler_calls == []


def test_direct_engine_activate_server_maps_bad_handler_results_to_bounded_conflict(
    short_socket_root: Path,
) -> None:
    handlers: tuple[Callable[[DirectEngineActivateCommand], object], ...] = (
        lambda _command: {"worker_epoch": 7, "status": "active"},
        lambda _command: (_ for _ in ()).throw(RuntimeError("private failure")),
    )
    for index, direct_engine_activate in enumerate(handlers):
        socket_path = short_socket_root / f"worker-{index}.sock"
        server = ipc.HealthServer(
            socket_path,
            health=lambda: ipc.WorkerHealth(worker_epoch=7, queue_gate="paused"),
            direct_engine_activate=cast(
                Callable[[DirectEngineActivateCommand], DirectEngineActivateResult],
                direct_engine_activate,
            ),
        )
        try:
            assert _serve_one(
                server,
                lambda: _raw_request(
                    socket_path,
                    b'{"op":"direct_engine_activate","expected_worker_epoch":7}\n',
                ),
            ) == {"error": "command_conflict"}
        finally:
            server.close()


def test_health_server_requires_a_callable_direct_engine_activate_handler(
    short_socket_root: Path,
) -> None:
    with pytest.raises(TypeError, match="^direct_engine_activate must be callable$"):
        ipc.HealthServer(
            short_socket_root / "worker.sock",
            health=lambda: ipc.WorkerHealth(worker_epoch=7, queue_gate="paused"),
            direct_engine_activate=cast(
                Callable[[DirectEngineActivateCommand], DirectEngineActivateResult],
                object(),
            ),
        )


@pytest.mark.parametrize(
    ("response", "error"),
    (
        pytest.param(b'{"error":"command_conflict"}\n', "command_conflict", id="conflict"),
        pytest.param(b'{"error":"invalid_request"}\n', "invalid_request", id="invalid-request"),
        pytest.param(
            b'{"worker_epoch":7,"status":"active","pid":123}\n',
            "ipc_response_invalid",
            id="extra-field",
        ),
        pytest.param(
            b'{"worker_epoch":7,"status":"active","status":"blocked"}\n',
            "ipc_response_invalid",
            id="duplicate-field",
        ),
        pytest.param(
            b'{"worker_epoch":7,"status":"unexpected"}\n',
            "ipc_response_invalid",
            id="unknown-status",
        ),
    ),
)
def test_direct_engine_activate_client_rejects_error_or_nonclosed_response(
    short_socket_root: Path, response: bytes, error: str
) -> None:
    socket_path = short_socket_root / "worker.sock"
    requests: list[bytes | None] = []
    thread, errors = _start_response_server(socket_path, response, requests)
    try:
        with pytest.raises(ipc.IPCError, match=rf"^{error}$"):
            activate_direct_engine(socket_path, expected_worker_epoch=7)
    finally:
        thread.join(_WATCHDOG_SECONDS)
        socket_path.unlink(missing_ok=True)
    assert not thread.is_alive()
    assert errors == []
    assert requests == [b'{"expected_worker_epoch":7,"op":"direct_engine_activate"}']


def test_worker_direct_activation_reserves_before_import_and_binds_before_rpc_ready(
) -> None:
    """A real lazy activation fences before import and atomically binds before ready."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        direct_import_allowed = context.Event()
        fence_before_direct_import = context.Event()
        release_direct_import = context.Event()
        bound_before_ready = context.Event()
        release_rpc_ready = context.Event()
        process = context.Process(
            target=_run_worker_process_with_engine_imports_gated,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                direct_import_allowed,
                bound_before_ready,
                release_rpc_ready,
                False,
                fence_before_direct_import,
                release_direct_import,
            ),
        )
        activation_thread: threading.Thread | None = None

        with _origin_type()() as origin:
            seeded = SQLiteStore(state_root / "state.db")
            try:
                seeded.apply_add(
                    DownloadIntent(
                        job_id="fixture-job",
                        request_id="fixture-request",
                        payload_digest="a" * 64,
                        source_url=origin.url("/range").encode("utf-8"),
                        generation=7,
                        revision=11,
                    )
                )
            finally:
                seeded.close()

            process.start()
            try:
                assert ready.wait(_WATCHDOG_SECONDS), _result(results)
                assert request_health(socket_path).to_record() == {
                    "protocol_version": 1,
                    "worker_epoch": 1,
                    "queue_gate": "paused",
                }
                assert request_jobs_page(socket_path) == JobsPage(
                    jobs=(
                        PublicJobRecord(
                            job="fixture-job",
                            generation=8,
                            revision=12,
                            state="paused",
                        ),
                    ),
                    next_cursor=None,
                )
                assert set_queue_gate(
                    socket_path,
                    gate="running",
                    request_id="fixture-open",
                    expected_revision=1,
                ) == ipc.QueueGateResult(
                    applied=True, queue_gate="running", revision=2
                )
                assert origin.ledger.response_body_bytes == 0

                responses: list[DirectEngineActivateResult] = []
                errors: list[BaseException] = []

                def activate() -> None:
                    try:
                        responses.append(
                            activate_direct_engine(
                                socket_path, expected_worker_epoch=1
                            )
                        )
                    except BaseException as error:
                        errors.append(error)

                direct_import_allowed.set()
                activation_thread = threading.Thread(target=activate)
                activation_thread.start()
                assert fence_before_direct_import.wait(_WATCHDOG_SECONDS)

                observer = SQLiteStore(state_root / "state.db")
                try:
                    fence = observer.get_direct_engine_activation_fence()
                    assert fence is not None
                    assert fence.worker_epoch == 1
                    assert observer.get_direct_engine_record() is None
                finally:
                    observer.close()
                assert activation_thread.is_alive()

                release_direct_import.set()
                assert bound_before_ready.wait(_WATCHDOG_SECONDS)

                observer = SQLiteStore(state_root / "state.db")
                try:
                    record = observer.get_direct_engine_record()
                    assert record is not None
                    assert record.worker_epoch == 1
                    assert reconcile_process_birth(record.identity) == "current"
                    assert observer._get_direct_engine_recovery_capability(record) is None
                    assert observer.get_direct_engine_activation_fence() is None
                finally:
                    observer.close()
                assert activation_thread.is_alive()
                assert origin.ledger.response_body_bytes == 0
                assert origin.ledger.request_count == 0

                release_rpc_ready.set()
                activation_thread.join(_WATCHDOG_SECONDS)
                assert not activation_thread.is_alive()
                assert errors == []
                assert responses == [
                    DirectEngineActivateResult(worker_epoch=1, status="active")
                ]
                assert activate_direct_engine(
                    socket_path, expected_worker_epoch=1
                ) == DirectEngineActivateResult(worker_epoch=1, status="active")

                observer = SQLiteStore(state_root / "state.db")
                try:
                    assert observer.get_direct_engine_record() == record
                    assert observer._get_direct_engine_recovery_capability(record) is not None
                    assert observer.get_direct_engine_activation_fence() is None
                finally:
                    observer.close()
                assert origin.ledger.response_body_bytes == 0

                shutdown.set()
                assert stopped.wait(_WATCHDOG_SECONDS)
                _join(process)
                assert _result(results) == ("result", None)
                _assert_group_gone(record.identity.process_group_id)

                reopened = SQLiteStore(state_root / "state.db")
                try:
                    assert reopened.get_direct_engine_record() is None
                    assert reopened._get_direct_engine_recovery_capability(record) is None
                    assert reopened.get_direct_engine_activation_fence() is None
                finally:
                    reopened.close()
            finally:
                release_direct_import.set()
                release_rpc_ready.set()
                shutdown.set()
                if activation_thread is not None:
                    activation_thread.join(_WATCHDOG_SECONDS)
                if process.is_alive():
                    _join(process)


def test_worker_direct_activation_replaces_an_absent_owned_controller() -> None:
    """A killed owned daemon cannot make its stale controller report active."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        direct_import_allowed = context.Event()
        stale_group_signal_guard = context.Event()
        stale_group_signal_attempted = context.Event()
        process = context.Process(
            target=_run_worker_process_with_engine_imports_gated,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                direct_import_allowed,
                None,
                None,
                True,
                None,
                None,
                stale_group_signal_guard,
                stale_group_signal_attempted,
            ),
        )
        initial: DirectEngineRecord | None = None
        replacement: DirectEngineRecord | None = None
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            direct_import_allowed.set()
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="active")

            observer = SQLiteStore(state_root / "state.db")
            try:
                initial = observer.get_direct_engine_record()
                assert initial is not None
                assert reconcile_process_birth(initial.identity) == "current"
            finally:
                observer.close()

            _force_stop_group(initial.identity.process_group_id)
            assert reconcile_process_birth(initial.identity) == "absent"
            assert process.is_alive()

            stale_group_signal_guard.set()
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="active")
            assert not stale_group_signal_attempted.is_set()
            observer = SQLiteStore(state_root / "state.db")
            try:
                replacement = observer.get_direct_engine_record()
                assert replacement is not None
                assert replacement != initial
                assert reconcile_process_birth(replacement.identity) == "current"
            finally:
                observer.close()

            stale_group_signal_guard.clear()
            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
            _assert_group_gone(replacement.identity.process_group_id)
        finally:
            stale_group_signal_guard.clear()
            shutdown.set()
            if process.is_alive():
                _join(process)
            if replacement is not None:
                _force_stop_group(replacement.identity.process_group_id)


@pytest.mark.parametrize(
    "discard_fails",
    (False, True),
    ids=("clears-the-exact-claim-before-replacement", "blocks-with-claim-intact"),
)
def test_worker_direct_activation_uses_absent_discard_without_normal_close(
    discard_fails: bool,
) -> None:
    """Only a no-signal discard may retire a reconciled-absent in-memory owner."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        identity = _fake_unowned_birth()
        expected_record = DirectEngineRecord(worker_epoch=1, identity=identity)
        replacement_identity = _synthetic_birth(owner_uid=os.geteuid())
        expected_replacement_record = DirectEngineRecord(
            worker_epoch=1, identity=replacement_identity
        )
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        controller_construction_count = context.Value("i", 0)
        reconciliation_called = context.Event()
        discard_called = context.Event()
        stale_close_guard = context.Event()
        stale_close_attempted = context.Event()
        replacement_claim_cleared = context.Event()
        discard_attempts = context.Value("i", 0)
        process = context.Process(
            target=_run_worker_process_with_absent_fake_direct,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                identity,
                discard_fails,
                controller_construction_count,
                reconciliation_called,
                discard_called,
                stale_close_guard,
                stale_close_attempted,
                replacement_claim_cleared,
            ),
            kwargs={
                "replacement_identity": replacement_identity,
                "discard_attempts": discard_attempts,
            },
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="active")
            assert controller_construction_count.value == 1
            assert _direct_engine_claims(state_root) == (expected_record, None)

            stale_close_guard.set()
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(
                worker_epoch=1,
                status="blocked" if discard_fails else "active",
            )
            assert reconciliation_called.wait(_WATCHDOG_SECONDS)
            assert discard_called.wait(_WATCHDOG_SECONDS)
            assert discard_attempts.value == 1
            assert not stale_close_attempted.is_set()

            if discard_fails:
                assert controller_construction_count.value == 1
                assert not replacement_claim_cleared.is_set()
                assert _direct_engine_claims(state_root) == (expected_record, None)

                # Keep the close guard armed through teardown: a failed absent
                # discard must not later fall back to normal group signaling.
                shutdown.set()
                assert stopped.wait(_WATCHDOG_SECONDS)
                _join(process)
                assert _result(results) == (
                    "error",
                    "RuntimeError",
                    "fake direct absent discard failure",
                )
                assert discard_attempts.value == 2
                assert not stale_close_attempted.is_set()
                assert _direct_engine_claims(state_root) == (expected_record, None)
            else:
                assert controller_construction_count.value == 2
                assert replacement_claim_cleared.wait(_WATCHDOG_SECONDS)
                assert _direct_engine_claims(state_root) == (
                    expected_replacement_record,
                    None,
                )

                stale_close_guard.clear()
                shutdown.set()
                assert stopped.wait(_WATCHDOG_SECONDS)
                _join(process)
                assert _result(results) == ("result", None)
                assert discard_attempts.value == 1
                assert _direct_engine_claims(state_root) == (None, None)
        finally:
            stale_close_guard.clear()
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_shutdown_retries_absent_discard_private_cleanup_without_signals() -> None:
    """Shutdown retries only local cleanup for an already-absent exact owner."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        cleanup_attempts = context.Value("i", 0)
        cleanup_failed = context.Event()
        cleanup_succeeded = context.Event()
        no_signal_or_rpc_guard = context.Event()
        normal_close_attempted = context.Event()
        rpc_attempted = context.Event()
        group_signal_attempted = context.Event()
        process_signal_attempted = context.Event()
        process = context.Process(
            target=_run_worker_process_with_retrying_absent_private_cleanup,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                cleanup_attempts,
                cleanup_failed,
                cleanup_succeeded,
                no_signal_or_rpc_guard,
                normal_close_attempted,
                rpc_attempted,
                group_signal_attempted,
                process_signal_attempted,
            ),
        )
        record: DirectEngineRecord | None = None
        runtime_path: Path | None = None
        config_path: Path | None = None
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="active")

            record, fence = _direct_engine_claims(state_root)
            assert record is not None
            assert fence is None
            runtime_paths = tuple((state_root / "direct-runtime").iterdir())
            assert len(runtime_paths) == 1
            runtime_path = runtime_paths[0]
            config_path = runtime_path / "aria2.conf"
            assert config_path.is_file()
            assert b"rpc-secret=" in config_path.read_bytes()

            _force_stop_group(record.identity.process_group_id)
            assert reconcile_process_birth(record.identity) == "absent"
            no_signal_or_rpc_guard.set()

            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="blocked")
            assert cleanup_failed.wait(_WATCHDOG_SECONDS)
            assert cleanup_attempts.value == 1
            assert _direct_engine_claims(state_root) == (record, None)
            assert runtime_path.exists()
            assert config_path.exists()
            assert not normal_close_attempted.is_set()
            assert not rpc_attempted.is_set()
            assert not group_signal_attempted.is_set()
            assert not process_signal_attempted.is_set()

            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="blocked")
            assert cleanup_attempts.value == 1
            assert _direct_engine_claims(state_root) == (record, None)

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
            assert cleanup_succeeded.wait(_WATCHDOG_SECONDS)
            assert cleanup_attempts.value == 2
            assert _direct_engine_claims(state_root) == (None, None)
            assert not normal_close_attempted.is_set()
            assert not rpc_attempted.is_set()
            assert not group_signal_attempted.is_set()
            assert not process_signal_attempted.is_set()
            assert not runtime_path.exists()
            assert not config_path.exists()
            assert tuple((state_root / "direct-runtime").glob("*/aria2.conf")) == ()
        finally:
            no_signal_or_rpc_guard.clear()
            shutdown.set()
            if process.is_alive():
                _join(process)
            if record is not None:
                _force_stop_group(record.identity.process_group_id)


def test_worker_shutdown_discards_an_absent_owned_controller_without_normal_close() -> None:
    """A stale owned record is discarded locally before its exact claim is cleared."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        identity = _fake_unowned_birth()
        expected_record = DirectEngineRecord(worker_epoch=1, identity=identity)
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        controller_construction_count = context.Value("i", 0)
        reconciliation_called = context.Event()
        discard_called = context.Event()
        stale_close_guard = context.Event()
        stale_close_attempted = context.Event()
        replacement_claim_cleared = context.Event()
        clear_attempts = context.Value("i", 0)
        discard_attempts = context.Value("i", 0)
        process = context.Process(
            target=_run_worker_process_with_absent_fake_direct,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                identity,
                False,
                controller_construction_count,
                reconciliation_called,
                discard_called,
                stale_close_guard,
                stale_close_attempted,
                replacement_claim_cleared,
            ),
            kwargs={
                "clear_attempts": clear_attempts,
                "discard_attempts": discard_attempts,
            },
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="active")
            assert controller_construction_count.value == 1
            assert _direct_engine_claims(state_root) == (expected_record, None)

            stale_close_guard.set()
            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
            assert reconciliation_called.wait(_WATCHDOG_SECONDS)
            assert discard_called.wait(_WATCHDOG_SECONDS)
            assert discard_attempts.value == 1
            assert clear_attempts.value == 1
            assert not stale_close_attempted.is_set()
            assert _direct_engine_claims(state_root) == (None, None)
        finally:
            stale_close_guard.clear()
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_shutdown_retains_an_indeterminate_owned_controller_without_side_effects() -> None:
    """An indeterminate owned record cannot be closed, discarded, or cleared."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        identity = _fake_unowned_birth()
        expected_record = DirectEngineRecord(worker_epoch=1, identity=identity)
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        controller_construction_count = context.Value("i", 0)
        reconciliation_called = context.Event()
        discard_called = context.Event()
        stale_close_guard = context.Event()
        stale_close_attempted = context.Event()
        replacement_claim_cleared = context.Event()
        clear_attempts = context.Value("i", 0)
        discard_attempts = context.Value("i", 0)
        process = context.Process(
            target=_run_worker_process_with_absent_fake_direct,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                identity,
                False,
                controller_construction_count,
                reconciliation_called,
                discard_called,
                stale_close_guard,
                stale_close_attempted,
                replacement_claim_cleared,
            ),
            kwargs={
                "reconciliation": "indeterminate",
                "clear_attempts": clear_attempts,
                "discard_attempts": discard_attempts,
            },
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="active")
            assert controller_construction_count.value == 1
            assert _direct_engine_claims(state_root) == (expected_record, None)

            stale_close_guard.set()
            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
            assert reconciliation_called.wait(_WATCHDOG_SECONDS)
            assert not discard_called.is_set()
            assert discard_attempts.value == 0
            assert clear_attempts.value == 0
            assert not stale_close_attempted.is_set()
            assert _direct_engine_claims(state_root) == (expected_record, None)
        finally:
            stale_close_guard.clear()
            shutdown.set()
            if process.is_alive():
                _join(process)


@pytest.mark.parametrize(
    ("clear_failures", "expected_result", "claim_retained"),
    (
        pytest.param(1, ("result", None), False, id="retry-succeeds"),
        pytest.param(
            2,
            ("error", "IPCStateError", "ipc_health_invalid"),
            True,
            id="persistent-failure-retains-claim",
        ),
    ),
)
def test_worker_shutdown_retries_only_the_exact_claim_clear_after_absent_discard(
    clear_failures: int,
    expected_result: tuple[object, ...],
    claim_retained: bool,
) -> None:
    """A completed no-signal discard leaves only its exact durable CAS to retry."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        identity = _fake_unowned_birth()
        expected_record = DirectEngineRecord(worker_epoch=1, identity=identity)
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        controller_construction_count = context.Value("i", 0)
        reconciliation_called = context.Event()
        discard_called = context.Event()
        stale_close_guard = context.Event()
        stale_close_attempted = context.Event()
        replacement_claim_cleared = context.Event()
        clear_attempts = context.Value("i", 0)
        discard_attempts = context.Value("i", 0)
        process = context.Process(
            target=_run_worker_process_with_absent_fake_direct,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                identity,
                False,
                controller_construction_count,
                reconciliation_called,
                discard_called,
                stale_close_guard,
                stale_close_attempted,
                replacement_claim_cleared,
            ),
            kwargs={
                "clear_failures": clear_failures,
                "clear_attempts": clear_attempts,
                "discard_attempts": discard_attempts,
            },
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="active")
            assert _direct_engine_claims(state_root) == (expected_record, None)

            stale_close_guard.set()
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="blocked")
            assert reconciliation_called.wait(_WATCHDOG_SECONDS)
            assert discard_called.wait(_WATCHDOG_SECONDS)
            assert discard_attempts.value == 1
            assert clear_attempts.value == 1
            assert not stale_close_attempted.is_set()
            assert _direct_engine_claims(state_root) == (expected_record, None)

            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="blocked")
            assert discard_attempts.value == 1
            assert clear_attempts.value == 1

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == expected_result
            assert discard_attempts.value == 1
            assert clear_attempts.value == 2
            assert not stale_close_attempted.is_set()
            assert _direct_engine_claims(state_root) == (
                (expected_record if claim_retained else None),
                None,
            )
        finally:
            stale_close_guard.clear()
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_direct_activation_fences_stale_epoch_before_engine_or_record_touch() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        prior = DirectEngineRecord(
            worker_epoch=1, identity=_synthetic_birth(owner_uid=os.geteuid())
        )
        _seed_epoch_one_direct_record(state_root, prior)

        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        direct_import_allowed = context.Event()
        process = context.Process(
            target=_run_worker_process_with_engine_imports_gated,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                direct_import_allowed,
            ),
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=2, status="stale_epoch")
            observer = SQLiteStore(state_root / "state.db")
            try:
                assert observer.get_direct_engine_record() == prior
            finally:
                observer.close()

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_direct_activation_blocks_an_indeterminate_prior_record_without_importing_an_engine() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        prior = DirectEngineRecord(
            worker_epoch=1, identity=_synthetic_birth(owner_uid=os.geteuid() + 1)
        )
        _seed_epoch_one_direct_record(state_root, prior)
        assert reconcile_process_birth(prior.identity) == "indeterminate"

        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        direct_import_allowed = context.Event()
        process = context.Process(
            target=_run_worker_process_with_engine_imports_gated,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                direct_import_allowed,
            ),
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=2
            ) == DirectEngineActivateResult(worker_epoch=2, status="blocked")
            observer = SQLiteStore(state_root / "state.db")
            try:
                assert observer.get_direct_engine_record() == prior
            finally:
                observer.close()

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_direct_activation_blocks_a_current_prior_record_without_importing_an_engine() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        controller, prior = _recorded_direct_controller(state_root)
        process_group_id = prior.identity.process_group_id
        try:
            assert reconcile_process_birth(prior.identity) == "current"
            socket_path = state_root / "worker.sock"
            context = multiprocessing.get_context("spawn")
            ready = context.Event()
            shutdown = context.Event()
            stopped = context.Event()
            results = context.Queue()
            direct_import_allowed = context.Event()
            process = context.Process(
                target=_run_worker_process_with_engine_imports_gated,
                args=(
                    str(state_root),
                    str(socket_path),
                    ready,
                    shutdown,
                    stopped,
                    results,
                    direct_import_allowed,
                ),
            )
            process.start()
            try:
                assert ready.wait(_WATCHDOG_SECONDS), _result(results)
                assert activate_direct_engine(
                    socket_path, expected_worker_epoch=2
                ) == DirectEngineActivateResult(worker_epoch=2, status="blocked")
                observer = SQLiteStore(state_root / "state.db")
                try:
                    assert observer.get_direct_engine_record() == prior
                finally:
                    observer.close()

                shutdown.set()
                assert stopped.wait(_WATCHDOG_SECONDS)
                _join(process)
                assert _result(results) == ("result", None)
            finally:
                shutdown.set()
                if process.is_alive():
                    _join(process)
        finally:
            try:
                controller.close()
            finally:
                _force_stop_group(process_group_id)
            reopened = SQLiteStore(state_root / "state.db")
            try:
                assert reopened.clear_direct_engine_record(prior) is True
            finally:
                reopened.close()


def test_worker_direct_activation_blocks_an_absent_legacy_record_without_capability() -> None:
    """A pre-migration record cannot authorize replacement even after absence is proven."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        stale_controller, stale = _recorded_direct_controller(state_root)
        stale_group_id = stale.identity.process_group_id
        try:
            stale_controller.close()
        finally:
            _force_stop_group(stale_group_id)
        assert reconcile_process_birth(stale.identity) == "absent"

        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        direct_import_allowed = context.Event()
        process = context.Process(
            target=_run_worker_process_with_engine_imports_gated,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                direct_import_allowed,
            ),
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=2
            ) == DirectEngineActivateResult(worker_epoch=2, status="blocked")
            observer = SQLiteStore(state_root / "state.db")
            try:
                assert observer.get_direct_engine_record() == stale
                assert observer._get_direct_engine_recovery_capability(stale) is None
            finally:
                observer.close()

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_direct_activation_replaces_an_absent_paired_record_after_cold_recovery() -> None:
    """A proved-absent modern claim is compare-cleared before the next activation."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        stale_controller, stale = _recorded_direct_controller(state_root)
        stale_group_id = stale.identity.process_group_id
        capability = stale_controller._recovery_capability()
        store = SQLiteStore(state_root / "state.db")
        try:
            store._bind_direct_engine_recovery_capability(stale, capability)
        finally:
            store.close()
        try:
            stale_controller.close()
        finally:
            _force_stop_group(stale_group_id)
        assert reconcile_process_birth(stale.identity) == "absent"

        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        direct_import_allowed = context.Event()
        process = context.Process(
            target=_run_worker_process_with_engine_imports_gated,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                direct_import_allowed,
            ),
        )
        process.start()
        replacement: DirectEngineRecord | None = None
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            observer = SQLiteStore(state_root / "state.db")
            try:
                assert observer.get_direct_engine_record() is None
                assert observer._get_direct_engine_recovery_capability(stale) is None
            finally:
                observer.close()

            direct_import_allowed.set()
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=2
            ) == DirectEngineActivateResult(worker_epoch=2, status="active")
            observer = SQLiteStore(state_root / "state.db")
            try:
                replacement = observer.get_direct_engine_record()
                assert replacement is not None
                assert replacement.worker_epoch == 2
                assert replacement != stale
                assert observer._get_direct_engine_recovery_capability(replacement) is not None
                assert reconcile_process_birth(replacement.identity) == "current"
            finally:
                observer.close()

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
            _assert_group_gone(replacement.identity.process_group_id)

            reopened = SQLiteStore(state_root / "state.db")
            try:
                assert reopened.get_direct_engine_record() is None
                assert reopened._get_direct_engine_recovery_capability(replacement) is None
            finally:
                reopened.close()
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)
            if replacement is not None:
                _force_stop_group(replacement.identity.process_group_id)


def test_worker_direct_activation_post_bind_start_failure_cleans_persisted_record() -> None:
    """A callback record does not survive a candidate that fails after binding."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        identity = _fake_unowned_birth()
        expected_record = DirectEngineRecord(worker_epoch=1, identity=identity)
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        controller_constructed = context.Event()
        start_called = context.Event()
        callback_entered = context.Event()
        callback_completed = context.Event()
        close_called = context.Event()
        fence_before_construction = context.Event()
        release_before_start = context.Event()
        release_after_callback = context.Event()
        process = context.Process(
            target=_run_worker_process_with_fake_direct,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                identity,
                "fails_after_callback",
                "succeeds",
                controller_constructed,
                start_called,
                callback_entered,
                callback_completed,
                close_called,
                None,
                release_after_callback,
                None,
                fence_before_construction,
                release_before_start,
            ),
        )
        activation_thread: threading.Thread | None = None
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            before = _direct_engine_state_snapshot(state_root)
            assert before == (1, "paused", ())
            assert not controller_constructed.wait(0.05)
            responses: list[DirectEngineActivateResult] = []
            errors: list[BaseException] = []

            def activate() -> None:
                try:
                    responses.append(
                        activate_direct_engine(socket_path, expected_worker_epoch=1)
                    )
                except BaseException as error:
                    errors.append(error)

            activation_thread = threading.Thread(target=activate)
            activation_thread.start()
            assert fence_before_construction.wait(_WATCHDOG_SECONDS)
            assert controller_constructed.wait(_WATCHDOG_SECONDS)
            unbound_record, unbound_fence = _direct_engine_claims(state_root)
            assert unbound_record is None
            assert unbound_fence is not None
            assert unbound_fence.worker_epoch == 1
            release_before_start.set()
            assert start_called.wait(_WATCHDOG_SECONDS)
            assert callback_entered.wait(_WATCHDOG_SECONDS)
            assert callback_completed.wait(_WATCHDOG_SECONDS)
            assert activation_thread.is_alive()

            observer = SQLiteStore(state_root / "state.db")
            try:
                assert observer.get_direct_engine_record() == expected_record
                assert observer.get_direct_engine_activation_fence() is None
            finally:
                observer.close()

            release_after_callback.set()
            activation_thread.join(_WATCHDOG_SECONDS)
            assert not activation_thread.is_alive()
            assert responses == []
            assert len(errors) == 1
            assert isinstance(errors[0], ipc.IPCError)
            assert str(errors[0]) == "command_conflict"
            assert close_called.wait(_WATCHDOG_SECONDS)
            assert _direct_engine_state_snapshot(state_root) == before
            assert _direct_engine_claims(state_root) == (None, None)
            assert _group_is_gone(identity.process_group_id)
            assert not (state_root / "direct-runtime").exists()
            assert request_health(socket_path).to_record() == {
                "protocol_version": 1,
                "worker_epoch": 1,
                "queue_gate": "paused",
            }

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
            assert not socket_path.exists()
        finally:
            release_before_start.set()
            release_after_callback.set()
            shutdown.set()
            if activation_thread is not None:
                activation_thread.join(_WATCHDOG_SECONDS)
            if process.is_alive():
                _join(process)


def test_worker_direct_activation_callback_persistence_failure_cleans_without_mutation() -> None:
    """A failed callback write is cleaned without leaving a direct record behind."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        identity = _fake_unowned_birth()
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        controller_constructed = context.Event()
        start_called = context.Event()
        callback_entered = context.Event()
        callback_completed = context.Event()
        close_called = context.Event()
        persistence_attempted = context.Event()
        process = context.Process(
            target=_run_worker_process_with_fake_direct,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                identity,
                "succeeds",
                "succeeds",
                controller_constructed,
                start_called,
                callback_entered,
                callback_completed,
                close_called,
                persistence_attempted,
            ),
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            before = _direct_engine_state_snapshot(state_root)
            assert before == (1, "paused", ())
            assert not controller_constructed.wait(0.05)

            with pytest.raises(ipc.IPCError, match="^command_conflict$"):
                activate_direct_engine(socket_path, expected_worker_epoch=1)

            assert controller_constructed.wait(_WATCHDOG_SECONDS)
            assert start_called.wait(_WATCHDOG_SECONDS)
            assert callback_entered.wait(_WATCHDOG_SECONDS)
            assert persistence_attempted.wait(_WATCHDOG_SECONDS)
            assert not callback_completed.wait(0.05)
            assert close_called.wait(_WATCHDOG_SECONDS)
            assert _direct_engine_state_snapshot(state_root) == before
            assert _direct_engine_claims(state_root) == (None, None)
            assert _group_is_gone(identity.process_group_id)
            assert not (state_root / "direct-runtime").exists()
            assert request_health(socket_path).to_record() == {
                "protocol_version": 1,
                "worker_epoch": 1,
                "queue_gate": "paused",
            }

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
            assert not socket_path.exists()
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_direct_activation_recovery_capability_persistence_failure_closes_and_clears() -> None:
    """A post-ready capability write failure leaves no active or durable direct owner."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        identity = _fake_unowned_birth()
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        controller_constructed = context.Event()
        start_called = context.Event()
        callback_entered = context.Event()
        callback_completed = context.Event()
        close_called = context.Event()
        capability_persistence_attempted = context.Event()
        process = context.Process(
            target=_run_worker_process_with_fake_direct,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                identity,
                "succeeds",
                "succeeds",
                controller_constructed,
                start_called,
                callback_entered,
                callback_completed,
                close_called,
            ),
            kwargs={
                "capability_persistence_attempted": capability_persistence_attempted,
            },
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            with pytest.raises(ipc.IPCError, match="^command_conflict$"):
                activate_direct_engine(socket_path, expected_worker_epoch=1)
            assert controller_constructed.wait(_WATCHDOG_SECONDS)
            assert start_called.wait(_WATCHDOG_SECONDS)
            assert callback_entered.wait(_WATCHDOG_SECONDS)
            assert callback_completed.wait(_WATCHDOG_SECONDS)
            assert capability_persistence_attempted.wait(_WATCHDOG_SECONDS)
            assert close_called.wait(_WATCHDOG_SECONDS)
            assert _direct_engine_claims(state_root) == (None, None)
            observer = SQLiteStore(state_root / "state.db")
            try:
                assert observer.get_direct_engine_record() is None
            finally:
                observer.close()
            assert request_health(socket_path).to_record() == {
                "protocol_version": 1,
                "worker_epoch": 1,
                "queue_gate": "paused",
            }

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_direct_activation_blocks_retry_after_unpersisted_cleanup_failure() -> None:
    """A failed callback write cannot lose an uncontained direct candidate."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        identity = _fake_unowned_birth()
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        controller_constructed = context.Event()
        start_called = context.Event()
        callback_entered = context.Event()
        callback_completed = context.Event()
        close_called = context.Event()
        persistence_attempted = context.Event()
        controller_construction_count = context.Value("i", 0)
        process = context.Process(
            target=_run_worker_process_with_fake_direct,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                identity,
                "succeeds",
                "fails",
                controller_constructed,
                start_called,
                callback_entered,
                callback_completed,
                close_called,
                persistence_attempted,
                None,
                controller_construction_count,
            ),
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            before = _direct_engine_state_snapshot(state_root)
            assert before == (1, "paused", ())

            with pytest.raises(ipc.IPCError, match="^command_conflict$"):
                activate_direct_engine(socket_path, expected_worker_epoch=1)

            assert controller_constructed.wait(_WATCHDOG_SECONDS)
            assert start_called.wait(_WATCHDOG_SECONDS)
            assert callback_entered.wait(_WATCHDOG_SECONDS)
            assert persistence_attempted.wait(_WATCHDOG_SECONDS)
            assert not callback_completed.wait(0.05)
            assert close_called.wait(_WATCHDOG_SECONDS)
            assert controller_construction_count.value == 1
            assert _direct_engine_state_snapshot(state_root) == before
            retained_record, retained_fence = _direct_engine_claims(state_root)
            assert retained_record is None
            assert retained_fence is not None
            assert retained_fence.worker_epoch == 1

            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="blocked")
            assert controller_construction_count.value == 1
            assert _direct_engine_state_snapshot(state_root) == before
            assert _direct_engine_claims(state_root) == (None, retained_fence)

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == (
                "error",
                "RuntimeError",
                "fake direct close failure",
            )
            assert _direct_engine_claims(state_root) == (None, retained_fence)

            restarted_ready = context.Event()
            restarted_shutdown = context.Event()
            restarted_stopped = context.Event()
            restarted_results = context.Queue()
            restart_direct_import_allowed = context.Event()
            restarted = context.Process(
                target=_run_worker_process_with_engine_imports_gated,
                args=(
                    str(state_root),
                    str(socket_path),
                    restarted_ready,
                    restarted_shutdown,
                    restarted_stopped,
                    restarted_results,
                    restart_direct_import_allowed,
                ),
            )
            restarted.start()
            try:
                assert restarted_ready.wait(_WATCHDOG_SECONDS), _result(restarted_results)
                assert activate_direct_engine(
                    socket_path, expected_worker_epoch=2
                ) == DirectEngineActivateResult(worker_epoch=2, status="blocked")
                assert controller_construction_count.value == 1
                assert _direct_engine_claims(state_root) == (None, retained_fence)

                restarted_shutdown.set()
                assert restarted_stopped.wait(_WATCHDOG_SECONDS)
                _join(restarted)
                assert _result(restarted_results) == ("result", None)
                assert _direct_engine_claims(state_root) == (None, retained_fence)
            finally:
                restarted_shutdown.set()
                if restarted.is_alive():
                    _join(restarted)
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_direct_activation_shutdown_close_failure_retains_exact_record() -> None:
    """Worker teardown must fail closed rather than clear an engine it could not close."""

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        identity = _fake_unowned_birth()
        expected_record = DirectEngineRecord(worker_epoch=1, identity=identity)
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        controller_constructed = context.Event()
        start_called = context.Event()
        callback_entered = context.Event()
        callback_completed = context.Event()
        close_called = context.Event()
        reconciliation_called = context.Event()
        process = context.Process(
            target=_run_worker_process_with_fake_direct,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                identity,
                "succeeds",
                "fails",
                controller_constructed,
                start_called,
                callback_entered,
                callback_completed,
                close_called,
            ),
            kwargs={
                "shutdown_reconciliation": "current",
                "shutdown_reconciliation_called": reconciliation_called,
            },
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="active")
            assert controller_constructed.wait(_WATCHDOG_SECONDS)
            assert start_called.wait(_WATCHDOG_SECONDS)
            assert callback_entered.wait(_WATCHDOG_SECONDS)
            assert callback_completed.wait(_WATCHDOG_SECONDS)
            assert not close_called.wait(0.05)
            assert not reconciliation_called.is_set()

            observer = SQLiteStore(state_root / "state.db")
            try:
                assert observer.get_direct_engine_record() == expected_record
                assert observer.get_direct_engine_activation_fence() is None
            finally:
                observer.close()

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == (
                "error",
                "RuntimeError",
                "fake direct close failure",
            )
            assert reconciliation_called.wait(_WATCHDOG_SECONDS)
            assert close_called.wait(_WATCHDOG_SECONDS)
            assert not socket_path.exists()
            assert _group_is_gone(identity.process_group_id)
            assert not (state_root / "direct-runtime").exists()

            reopened = SQLiteStore(state_root / "state.db")
            try:
                assert reopened.get_direct_engine_record() == expected_record
                assert reopened.get_direct_engine_activation_fence() is None
            finally:
                reopened.close()
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_jobs_page_ipc_is_empty_and_read_only() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        process = context.Process(
            target=_run_worker_process,
            args=(str(state_root), str(socket_path), ready, shutdown, stopped, results),
        )
        process.start()

        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            expected = JobsPage(jobs=(), next_cursor=None)
            assert request_jobs_page(socket_path) == expected
            assert _raw_request(socket_path, b'{"op":"jobs_page"}\n') == {
                "jobs": [],
                "next_cursor": None,
            }
            assert request_health(socket_path).to_record() == {
                "protocol_version": 1,
                "worker_epoch": 1,
                "queue_gate": "paused",
            }

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
            assert not socket_path.exists()

            store = SQLiteStore(state_root / "state.db")
            try:
                assert store.list_jobs() == ()
                assert store.queue_gate() == "paused"
            finally:
                store.close()
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_jobs_page_ipc_pages_the_worker_owned_store_before_startup() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        store = SQLiteStore(state_root / "state.db")
        try:
            for index in range(101):
                store.apply_add(
                    DownloadIntent(
                        job_id=f"job-{index:03d}",
                        request_id=f"request-{index:03d}",
                        payload_digest=f"{index:064x}",
                        source_url=(
                            f"https://example.test/private-{index}?token=secret-{index}"
                        ).encode("utf-8"),
                        generation=index,
                        revision=index,
                    )
                )
        finally:
            store.close()

        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        process = context.Process(
            target=_run_worker_process,
            args=(str(state_root), str(socket_path), ready, shutdown, stopped, results),
        )
        process.start()

        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            first_page = request_jobs_page(socket_path, cursor=None)
            assert first_page == JobsPage(
                jobs=tuple(
                    PublicJobRecord(
                        job=f"job-{index:03d}",
                        generation=index + 1,
                        revision=index + 1,
                        state="paused",
                    )
                    for index in range(100)
                ),
                next_cursor="job-099",
            )
            assert first_page.next_cursor == "job-099"
            assert request_jobs_page(
                socket_path, cursor=first_page.next_cursor
            ) == JobsPage(
                jobs=(
                    PublicJobRecord(
                        job="job-100",
                        generation=101,
                        revision=101,
                        state="paused",
                    ),
                ),
                next_cursor=None,
            )
            assert request_jobs_page(socket_path) == first_page

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_jobs_page_rejects_bad_cursor_without_mutating_or_leaking() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        intent = DownloadIntent(
            job_id="job-secret",
            request_id="request-secret",
            payload_digest="a" * 64,
            source_url=b"https://example.test/private-source?token=source-secret",
            generation=7,
            revision=11,
        )
        store = SQLiteStore(state_root / "state.db")
        try:
            store.apply_add(
                intent,
                materialized=MaterializedJob(
                    job_id=intent.job_id,
                    intent=intent,
                    source_kind=SourceKind.DIRECT,
                    queue_collection_id="queue-secret",
                    priority=0,
                    order_key=0,
                    scheduled_for=None,
                    authorized=True,
                    manual_hold=False,
                    start_now_requested=False,
                    category="Other",
                    destination_collection="private-destination",
                    partial_filename="private.bin",
                    selected_final_filename="private--job-secret.bin",
                ),
            )
        finally:
            store.close()

        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        process = context.Process(
            target=_run_worker_process,
            args=(str(state_root), str(socket_path), ready, shutdown, stopped, results),
        )
        process.start()

        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            expected = JobsPage(
                jobs=(
                    PublicJobRecord(
                        job="job-secret",
                        generation=8,
                        revision=12,
                        state="paused",
                    ),
                ),
                next_cursor=None,
            )
            assert request_jobs_page(socket_path) == expected
            safe_response = json.dumps(expected.to_record())
            for secret in (
                "private-source",
                "source-secret",
                "private-destination",
                "private.bin",
                "request-secret",
                "a" * 64,
            ):
                assert secret not in safe_response

            for payload in (
                b'{"op":"jobs_page","cursor":""}\n',
                b'{"op":"jobs_page","cursor":true}\n',
                b'{"op":"jobs_page","cursor":"not/a-cursor"}\n',
                b'{"op":"jobs_page","cursor":"job-secret","extra":true}\n',
                b'{"op":"jobs_page","cursor":"job-secret","cursor":"other"}\n',
            ):
                assert _raw_request(socket_path, payload) == {
                    "error": "invalid_request"
                }

            assert request_jobs_page(socket_path) == expected

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)

            store = SQLiteStore(state_root / "state.db")
            try:
                assert [
                    (job.job, job.generation, job.revision, job.state)
                    for job in store.list_jobs()
                ] == [("job-secret", 8, 12, "paused")]
                assert store.queue_gate() == "paused"
            finally:
                store.close()
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


@pytest.mark.parametrize(
    ("corrupted_state", "persisted_state", "sqlite_type"),
    (
        pytest.param(sqlite3.Binary(b"paused"), b"paused", "blob", id="blob"),
        pytest.param("unexpected", "unexpected", "text", id="unexpected-text"),
    ),
)
def test_worker_jobs_page_rejects_malformed_persisted_state_without_mutation_or_leak_and_keeps_worker_healthy(
    corrupted_state: object,
    persisted_state: object,
    sqlite_type: str,
) -> None:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        intent = DownloadIntent(
            job_id="job-secret",
            request_id="request-secret",
            payload_digest="a" * 64,
            source_url=b"https://example.test/private-source?token=source-secret",
            generation=7,
            revision=11,
        )
        store = SQLiteStore(state_root / "state.db")
        try:
            store.apply_add(intent)
            store._connection.execute(
                "UPDATE jobs SET state = ? WHERE job_id = ?",
                (corrupted_state, intent.job_id),
            )
        finally:
            store.close()

        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        process = context.Process(
            target=_run_worker_process,
            args=(str(state_root), str(socket_path), ready, shutdown, stopped, results),
        )
        process.start()

        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            observer = SQLiteStore(state_root / "state.db")
            try:
                before = tuple(
                    tuple(row)
                    for row in observer._connection.execute(
                        """
                        SELECT job_id, source_url, generation, revision, state
                        FROM jobs
                        ORDER BY job_id
                        """
                    )
                )
                assert type(before[-1][-1]) is type(persisted_state)
                assert before[-1][-1] == persisted_state
                storage_class = observer._connection.execute(
                    "SELECT typeof(state) FROM jobs WHERE job_id = ?",
                    (intent.job_id,),
                ).fetchone()
                assert storage_class is not None
                assert storage_class[0] == sqlite_type
                queue_gate_before = observer.queue_gate()

                response = _raw_request(socket_path, b'{"op":"jobs_page"}\n')
                assert response == {"error": "invalid_request"}
                response_text = json.dumps(response)
                for secret in (
                    "private-source",
                    "source-secret",
                    "request-secret",
                    "a" * 64,
                ):
                    assert secret not in response_text

                after = tuple(
                    tuple(row)
                    for row in observer._connection.execute(
                        """
                        SELECT job_id, source_url, generation, revision, state
                        FROM jobs
                        ORDER BY job_id
                        """
                    )
                )
                assert after == before
                assert observer.queue_gate() == queue_gate_before
            finally:
                observer.close()

            assert request_health(socket_path).to_record() == {
                "protocol_version": 1,
                "worker_epoch": 1,
                "queue_gate": "paused",
            }

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_jobs_page_client_rejects_non_job_state_identifier() -> None:
    with pytest.raises(ipc.IPCError, match="ipc_response_invalid"):
        JobsPage.from_record(
            {
                "jobs": [
                    {
                        "job": "job-invalid",
                        "generation": 1,
                        "revision": 1,
                        "state": "unexpected",
                    }
                ],
                "next_cursor": None,
            }
        )


def test_request_reader_rejects_fragmented_multiline_payload() -> None:
    class ChunkedConnection:
        def __init__(self) -> None:
            self._chunks = iter(
                (
                    b'{"op":"health"}\n',
                    b'{"op":"health"}\n',
                    b"",
                )
            )

        def recv(self, _size: int) -> bytes:
            return next(self._chunks)

    assert ipc._read_line(cast(socket.socket, ChunkedConnection())) is None


@pytest.mark.parametrize("protocol_version", (True, 1.0, "1"))
def test_health_response_requires_an_exact_integer_protocol_version(
    protocol_version: object,
) -> None:
    with pytest.raises(ipc.IPCError, match="ipc_response_invalid"):
        ipc.WorkerHealth.from_record(
            {
                "protocol_version": protocol_version,
                "worker_epoch": 1,
                "queue_gate": "paused",
            }
        )


def test_unknown_socket_is_never_unlinked_without_recorded_identity() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        socket_path = Path(temporary_root) / "worker.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(socket_path))
            ipc._unlink_owned_socket(socket_path, None)
            assert socket_path.is_socket()
        finally:
            listener.close()
            socket_path.unlink(missing_ok=True)


def test_worker_health_ipc_is_bounded_read_only_and_removed_on_shutdown() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-ipc-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        socket_path = state_root / "worker.sock"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        shutdown = context.Event()
        stopped = context.Event()
        results = context.Queue()
        process = context.Process(
            target=_run_worker_process,
            args=(str(state_root), str(socket_path), ready, shutdown, stopped, results),
        )
        process.start()

        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert socket_path.is_socket()

            assert request_health(socket_path).to_record() == {
                "protocol_version": 1,
                "worker_epoch": 1,
                "queue_gate": "paused",
            }
            opened = set_queue_gate(
                socket_path,
                gate="running",
                request_id="queue-open-request",
                expected_revision=1,
            )
            assert opened.to_record() == {
                "applied": True,
                "queue_gate": "running",
                "revision": 2,
            }
            assert set_queue_gate(
                socket_path,
                gate="running",
                request_id="queue-open-request",
                expected_revision=1,
            ).to_record()["applied"] is False
            assert _raw_request(
                socket_path, b'{"op":"queue_gate","op":"health"}\n'
            ) == {"error": "invalid_request"}
            assert request_health(socket_path).to_record() == {
                "protocol_version": 1,
                "worker_epoch": 1,
                "queue_gate": "running",
            }
            assert _raw_request(
                socket_path,
                b'{"op":"queue_gate","gate":"paused","request_id":"queue-close","expected_revision":1}\n',
            ) == {"error": "command_conflict"}
            assert _raw_request(
                socket_path,
                b'{"op":"queue_gate","gate":"paused","request_id":"queue-close","expected_revision":true}\n',
            ) == {"error": "invalid_request"}
            assert _raw_request(socket_path, b'{"op":"unknown"}\n') == {
                "error": "invalid_request"
            }
            assert _raw_request(socket_path, b'{"op":\n') == {
                "error": "invalid_request"
            }
            assert _raw_request(
                socket_path,
                (b'{"op":"health"}\n', b'{"op":"health"}\n'),
            ) == {"error": "invalid_request"}
            deeply_nested_request = b"[" * 1024 + b"0" + b"]" * 1024 + b"\n"
            assert len(deeply_nested_request) <= MAX_MESSAGE_BYTES
            assert _raw_request(socket_path, deeply_nested_request) == {
                "error": "invalid_request"
            }
            assert _raw_request(socket_path, b"x" * (MAX_MESSAGE_BYTES + 1)) == {
                "error": "invalid_request"
            }
            assert request_health(socket_path).to_record() == {
                "protocol_version": 1,
                "worker_epoch": 1,
                "queue_gate": "running",
            }

            store = SQLiteStore(state_root / "state.db")
            try:
                assert store.worker_epoch() == 1
                assert store.queue_gate() == "running"
                receipts = store._connection.execute(
                    """
                    SELECT request_id, payload_digest, gate, revision
                    FROM queue_commands
                    ORDER BY request_id
                    """
                ).fetchall()
                assert len(receipts) == 1
                assert tuple(receipts[0]) == (
                    "queue-open-request",
                    hashlib.sha256(
                        json.dumps(
                            {
                                "expected_revision": 1,
                                "gate": "running",
                                "request_id": "queue-open-request",
                            },
                            separators=(",", ":"),
                            sort_keys=True,
                        ).encode("utf-8")
                    ).hexdigest(),
                    "running",
                    2,
                )
                assert store.list_jobs() == ()
            finally:
                store.close()

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
            assert not socket_path.exists()
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_job_control_is_durable_typed_and_never_imports_or_transfers(
    short_socket_root: Path,
) -> None:
    state_root = short_socket_root / "state"
    state_root.mkdir(mode=0o700)
    socket_path = state_root / "worker.sock"
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    shutdown = context.Event()
    stopped = context.Event()
    results = context.Queue()
    direct_import_allowed = context.Event()

    with _origin_type()() as origin:
        intent = DownloadIntent(
            job_id="fixture-job",
            request_id="fixture-add-request",
            payload_digest="a" * 64,
            source_url=origin.url("/range").encode("utf-8"),
            generation=7,
            revision=11,
        )
        seeded = SQLiteStore(state_root / "state.db")
        try:
            seeded.apply_add(
                intent,
                materialized=MaterializedJob(
                    job_id=intent.job_id,
                    intent=intent,
                    source_kind=SourceKind.DIRECT,
                    queue_collection_id=None,
                    priority=0,
                    order_key=0,
                    scheduled_for=None,
                    authorized=False,
                    manual_hold=False,
                    start_now_requested=False,
                    category="Other",
                    destination_collection=None,
                    partial_filename="fixture.bin",
                    selected_final_filename="fixture--fixture-job.bin",
                ),
            )
        finally:
            seeded.close()

        process = context.Process(
            target=_run_worker_process_with_engine_imports_gated,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                direct_import_allowed,
            ),
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert request_health(socket_path).to_record() == {
                "protocol_version": 1,
                "worker_epoch": 1,
                "queue_gate": "paused",
            }

            blocked = ipc.control_job(
                socket_path,
                job="fixture-job",
                action="start_now",
                request_id="start-blocked-request",
                expected_revision=12,
            )
            assert blocked == ipc.JobControlResult(
                status="blocked",
                job="fixture-job",
                generation=8,
                revision=12,
                state="paused",
                authorized=False,
            )
            paused = ipc.control_job(
                socket_path,
                job="fixture-job",
                action="pause",
                request_id="pause-request",
                expected_revision=12,
            )
            assert paused == ipc.JobControlResult(
                status="applied",
                job="fixture-job",
                generation=8,
                revision=13,
                state="paused",
                authorized=False,
            )
            assert ipc.control_job(
                socket_path,
                job="fixture-job",
                action="pause",
                request_id="pause-request",
                expected_revision=12,
            ) == paused
            resumed = ipc.control_job(
                socket_path,
                job="fixture-job",
                action="resume",
                request_id="resume-request",
                expected_revision=13,
            )
            assert resumed == ipc.JobControlResult(
                status="applied",
                job="fixture-job",
                generation=8,
                revision=14,
                state="queued",
                authorized=False,
            )
            assert set_queue_gate(
                socket_path,
                gate="running",
                request_id="queue-open-request",
                expected_revision=1,
            ).to_record() == {
                "applied": True,
                "queue_gate": "running",
                "revision": 2,
            }
            started = ipc.control_job(
                socket_path,
                job="fixture-job",
                action="start_now",
                request_id="start-request",
                expected_revision=14,
            )
            assert started == ipc.JobControlResult(
                status="applied",
                job="fixture-job",
                generation=8,
                revision=15,
                state="queued",
                authorized=True,
            )
            assert ipc.control_job(
                socket_path,
                job="fixture-job",
                action="pause",
                request_id="stale-request",
                expected_revision=14,
            ) == ipc.JobControlResult(
                status="stale",
                job="fixture-job",
                generation=8,
                revision=15,
                state="queued",
                authorized=True,
            )
            with pytest.raises(ipc.IPCError, match="^command_conflict$"):
                ipc.control_job(
                    socket_path,
                    job="fixture-job",
                    action="resume",
                    request_id="pause-request",
                    expected_revision=15,
                )

            for payload in (
                b'{"op":"job_control"}\n',
                b'{"op":"job_control","job":"fixture-job","action":"delete","request_id":"bad-request","expected_revision":15}\n',
                b'{"op":"job_control","job":"fixture-job","action":"pause","request_id":"bad-request","expected_revision":true}\n',
                b'{"op":"job_control","job":"fixture-job","action":"pause","request_id":"bad-request","expected_revision":15,"extra":true}\n',
                b'{"op":"job_control","job":"fixture-job","action":"pause","action":"resume","request_id":"bad-request","expected_revision":15}\n',
                b'{"op":"job_control","job":"unknown-job","action":"pause","request_id":"unknown-request","expected_revision":0}\n',
            ):
                assert _raw_request(socket_path, payload) == {"error": "invalid_request"}

            observer = SQLiteStore(state_root / "state.db")
            try:
                store_job = observer.get_job("fixture-job")
                assert store_job is not None
                assert (
                    store_job.generation,
                    store_job.revision,
                    store_job.state,
                ) == (8, 15, "queued")
                materialized = observer.get_materialized_job("fixture-job")
                assert materialized is not None
                assert (
                    materialized.authorized,
                    materialized.manual_hold,
                    materialized.start_now_requested,
                ) == (True, False, True)
                receipt = observer._connection.execute(
                    """
                    SELECT payload_digest, action, status, generation, revision, state, authorized
                    FROM job_control_commands
                    WHERE request_id = 'pause-request'
                    """
                ).fetchone()
                assert receipt is not None
                assert tuple(receipt) == (
                    hashlib.sha256(
                        json.dumps(
                            {
                                "action": "pause",
                                "expected_revision": 12,
                                "job": "fixture-job",
                                "request_id": "pause-request",
                            },
                            separators=(",", ":"),
                            sort_keys=True,
                        ).encode("utf-8")
                    ).hexdigest(),
                    "pause",
                    "applied",
                    8,
                    13,
                    "paused",
                    0,
                )
                assert [event.kind for event in observer.list_events()] == [
                    "job_added",
                    "job_paused",
                    "job_paused",
                    "job_resumed",
                    "job_start_now_requested",
                ]
            finally:
                observer.close()

            assert direct_import_allowed.is_set() is False
            assert origin.ledger.request_count == 0
            assert origin.ledger.response_body_bytes == 0
            assert not (state_root / "direct-runtime").exists()
            assert request_health(socket_path).queue_gate == "running"

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_job_control_client_rejects_nonclosed_or_duplicate_responses(
    short_socket_root: Path,
) -> None:
    control_job = getattr(ipc, "control_job", None)
    assert callable(control_job)
    responses = (
        b'{"status":"applied","job":"job-1","generation":1,"revision":2,"state":"queued","authorized":true,"extra":0}\n',
        b'{"status":"applied","job":"job-1","generation":1,"revision":2,"state":"queued","authorized":true,"status":"stale"}\n',
        b'{"status":"applied","job":"job-1","generation":1,"revision":2,"state":"queued","authorized":1}\n',
        b'{"status":"unexpected","job":"job-1","generation":1,"revision":2,"state":"queued","authorized":true}\n',
    )
    for index, response in enumerate(responses):
        socket_path = short_socket_root / f"worker-{index}.sock"
        requests: list[bytes | None] = []
        thread, errors = _start_response_server(socket_path, response, requests)
        try:
            with pytest.raises(ipc.IPCError, match="^ipc_response_invalid$"):
                control_job(
                    socket_path,
                    job="job-1",
                    action="pause",
                    request_id="pause-request",
                    expected_revision=2,
                )
        finally:
            thread.join(_WATCHDOG_SECONDS)
            socket_path.unlink(missing_ok=True)
        assert not thread.is_alive()
        assert errors == []
        assert requests == [
            b'{"action":"pause","expected_revision":2,"job":"job-1","op":"job_control","request_id":"pause-request"}'
        ]


def _run_worker_process_with_job_add_guards(
    state_root: str,
    socket_path: str,
    ready_event: object,
    shutdown_event: object,
    stopped_event: object,
    results: object,
    resolver_calls: Any,
) -> None:
    """Forbid engine/path work and count any outbound-name resolution."""

    import builtins

    assert "hermes_downloads.direct" not in sys.modules
    assert "hermes_downloads.paths" not in sys.modules
    assert "hermes_downloads.video" not in sys.modules
    original_import = builtins.__import__
    original_getaddrinfo: Any = socket.getaddrinfo

    def guarded_import(
        name: str,
        globals: object = None,
        locals: object = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        if name in {
            "hermes_downloads.direct",
            "hermes_downloads.paths",
            "hermes_downloads.video",
        }:
            raise AssertionError("job add imported an engine or path resolver")
        return cast(Any, original_import)(name, globals, locals, fromlist, level)

    def guarded_getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
        if host == "job-add.example.test":
            resolver_calls.value += 1
            return original_getaddrinfo("127.0.0.1", *args, **kwargs)
        return original_getaddrinfo(host, *args, **kwargs)

    builtins.__import__ = guarded_import
    socket.getaddrinfo = guarded_getaddrinfo
    try:
        _run_worker_process(
            state_root,
            socket_path,
            ready_event,
            shutdown_event,
            stopped_event,
            results,
        )
    finally:
        socket.getaddrinfo = original_getaddrinfo
        builtins.__import__ = original_import


def _run_worker_process_with_direct_dispatch_gates(
    state_root: str,
    socket_path: str,
    ready_event: object,
    shutdown_event: object,
    stopped_event: object,
    results: object,
    origin_url: str,
    marker_attested: _LifecycleEvent,
    release_paused_add: _LifecycleEvent,
    paused_add_complete: _LifecycleEvent,
    release_resume: _LifecycleEvent,
) -> None:
    """Grant only this fixture origin and freeze dispatch at its body cutpoints."""

    from hermes_downloads import direct as direct_module, network

    direct: Any = direct_module
    grant = network.LocalOriginGrant.for_url(origin_url)
    original_validate_source_url = worker.validate_source_url
    original_add_paused = direct.DirectAria2Controller.add_paused

    def validate_fixture_source(value: Any) -> Any:
        return original_validate_source_url(value, local_origin_grant=grant)

    def gated_add_paused(controller: Any, **kwargs: Any) -> Any:
        job_id = kwargs["job_id"]
        assert type(job_id) is str
        observer = SQLiteStore(Path(state_root) / "state.db")
        try:
            assert observer.get_publication_marker_binding(job_id) is not None
        finally:
            observer.close()
        marker_attested.set()
        assert release_paused_add.wait(_WATCHDOG_SECONDS)
        result = original_add_paused(controller, **kwargs)
        assert result.status == "paused"
        assert result.completed_length == 0
        paused_add_complete.set()
        assert release_resume.wait(_WATCHDOG_SECONDS)
        return result

    worker.validate_source_url = validate_fixture_source
    direct.DirectAria2Controller.add_paused = gated_add_paused
    try:
        _run_worker_process(
            state_root,
            socket_path,
            ready_event,
            shutdown_event,
            stopped_event,
            results,
        )
    finally:
        direct.DirectAria2Controller.add_paused = original_add_paused
        worker.validate_source_url = original_validate_source_url


def _seed_local_direct_dispatch_job(state_root: Path, source_url: str) -> None:
    intent = DownloadIntent(
        job_id="dispatch-job",
        request_id="dispatch-add-request",
        payload_digest="d" * 64,
        source_url=source_url.encode("utf-8"),
        generation=0,
        revision=0,
    )
    materialized = MaterializedJob(
        job_id=intent.job_id,
        intent=intent,
        source_kind=SourceKind.DIRECT,
        queue_collection_id=None,
        priority=0,
        order_key=0,
        scheduled_for=None,
        authorized=False,
        manual_hold=False,
        start_now_requested=False,
        category="Other",
        destination_collection=None,
        partial_filename="dispatch.bin",
        selected_final_filename="dispatch.bin",
    )
    store = SQLiteStore(state_root / "state.db")
    try:
        assert store.apply_add(intent, materialized=materialized).applied is True
    finally:
        store.close()


def _job_add_record(
    *,
    job: str = "job-add-1",
    request_id: str = "job-add-request-1",
    source_url: str = "https://downloads.example.test/payload.bin?signature=private-query",
    priority: int = -7,
    order_key: int = 9,
    category: str = "Other",
    partial_filename: str = "payload.bin",
    selected_final_filename: str = "payload--job-add-1.bin",
) -> dict[str, object]:
    return {
        "op": "job_add",
        "job": job,
        "request_id": request_id,
        "source_url": source_url,
        "source_kind": "direct",
        "priority": priority,
        "order_key": order_key,
        "category": category,
        "partial_filename": partial_filename,
        "selected_final_filename": selected_final_filename,
        "start": False,
    }


def _job_add_payload(record: dict[str, object]) -> bytes:
    return (
        json.dumps(record, separators=(",", ":"), sort_keys=True).encode("utf-8")
        + b"\n"
    )


def _job_add_store_snapshot(
    state_root: Path,
) -> tuple[object, ...]:
    """Capture add-visible durable state without exposing stored source URLs."""

    store = SQLiteStore(state_root / "state.db")
    try:
        connection = store._connection
        return (
            tuple(
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT key, value, revision
                    FROM settings
                    ORDER BY key
                    """
                ).fetchall()
            ),
            tuple(
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT job_id, generation, revision, state
                    FROM jobs
                    ORDER BY job_id
                    """
                ).fetchall()
            ),
            tuple(
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT request_id, job_id, generation, revision
                    FROM commands
                    ORDER BY request_id
                    """
                ).fetchall()
            ),
            tuple(
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT request_id, scope, action
                    FROM command_receipts
                    ORDER BY request_id
                    """
                ).fetchall()
            ),
            tuple(
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT request_id, payload_digest, gate, revision
                    FROM queue_commands
                    ORDER BY request_id
                    """
                ).fetchall()
            ),
            tuple(
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT
                        request_id,
                        payload_digest,
                        job_id,
                        action,
                        status,
                        generation,
                        revision,
                        state,
                        authorized
                    FROM job_control_commands
                    ORDER BY request_id
                    """
                ).fetchall()
            ),
            tuple(
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT kind, job_id, generation, revision
                    FROM events
                    ORDER BY event_id
                    """
                ).fetchall()
            ),
            tuple(
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT
                        job_id,
                        source_kind,
                        priority,
                        order_key,
                        authorized,
                        manual_hold,
                        start_now_requested
                    FROM materialized_jobs
                    ORDER BY job_id
                    """
                ).fetchall()
            ),
        )
    finally:
        store.close()


def _add_job(
    socket_path: Path,
    **overrides: object,
) -> Any:
    add_job = getattr(ipc, "add_job", None)
    assert callable(add_job), "job_add client is missing"
    record = _job_add_record(**cast(dict[str, Any], overrides))
    return add_job(
        socket_path,
        job=record["job"],
        request_id=record["request_id"],
        source_url=record["source_url"],
        priority=record["priority"],
        order_key=record["order_key"],
        category=record["category"],
        partial_filename=record["partial_filename"],
        selected_final_filename=record["selected_final_filename"],
    )


def test_job_add_client_uses_closed_envelope_and_full_derived_digest(
    short_socket_root: Path,
) -> None:
    command_type = getattr(ipc, "JobAddCommand", None)
    result_type = getattr(ipc, "JobAddResult", None)
    add_job = getattr(ipc, "add_job", None)
    assert isinstance(command_type, type), "job_add command is missing"
    assert isinstance(result_type, type), "job_add result is missing"
    assert callable(add_job), "job_add client is missing"

    record = _job_add_record()
    command = command_type(
        job=record["job"],
        request_id=record["request_id"],
        source_url=record["source_url"],
        priority=record["priority"],
        order_key=record["order_key"],
        category=record["category"],
        partial_filename=record["partial_filename"],
        selected_final_filename=record["selected_final_filename"],
    )
    assert command.to_record() == record
    assert command.payload_digest == hashlib.sha256(
        json.dumps(
            {
                "job": record["job"],
                "request_id": record["request_id"],
                "source_url": record["source_url"],
                "source_kind": "direct",
                "priority": record["priority"],
                "order_key": record["order_key"],
                "category": record["category"],
                "partial_filename": record["partial_filename"],
                "selected_final_filename": record["selected_final_filename"],
                "start": False,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    assert "payload_digest" not in record

    socket_path = short_socket_root / "worker.sock"
    commands: list[ipc.JobAddCommand] = []

    def job_add(command: ipc.JobAddCommand) -> ipc.JobAddResult:
        commands.append(command)
        return ipc.JobAddResult(
            applied=True, job="job-add-1", generation=0, revision=0
        )

    server = ipc.HealthServer(
        socket_path,
        health=lambda: ipc.WorkerHealth(worker_epoch=1, queue_gate="paused"),
        job_add=job_add,
    )
    try:
        result = cast(
            ipc.JobAddResult,
            _serve_one(
                server,
                lambda: add_job(
                    socket_path,
                    job=record["job"],
                    request_id=record["request_id"],
                    source_url=record["source_url"],
                    priority=record["priority"],
                    order_key=record["order_key"],
                    category=record["category"],
                    partial_filename=record["partial_filename"],
                    selected_final_filename=record["selected_final_filename"],
                ),
            ),
        )
    finally:
        server.close()

    assert result.to_record() == {
        "applied": True,
        "job": "job-add-1",
        "generation": 0,
        "revision": 0,
    }
    assert json.dumps(result.to_record()) == (
        '{"applied": true, "job": "job-add-1", "generation": 0, "revision": 0}'
    )
    assert "private-query" not in json.dumps(result.to_record())
    assert [captured.to_record() for captured in commands] == [record]


def test_job_add_client_rejects_error_or_nonclosed_response(
    short_socket_root: Path,
) -> None:
    add_job = getattr(ipc, "add_job", None)
    assert callable(add_job), "job_add client is missing"
    responses = (
        (b'{"error":"command_conflict"}\n', "command_conflict"),
        (b'{"error":"invalid_request"}\n', "invalid_request"),
        (
            b'{"applied":true,"job":"job-add-1","generation":0,"revision":0,"extra":0}\n',
            "ipc_response_invalid",
        ),
        (
            b'{"applied":true,"job":"job-add-1","generation":0,"revision":0,"revision":1}\n',
            "ipc_response_invalid",
        ),
        (
            b'{"applied":1,"job":"job-add-1","generation":0,"revision":0}\n',
            "ipc_response_invalid",
        ),
    )
    record = _job_add_record()
    expected_request = _job_add_payload(record)[:-1]
    for index, (response, error) in enumerate(responses):
        socket_path = short_socket_root / f"worker-{index}.sock"
        requests: list[bytes | None] = []
        thread, errors = _start_response_server(socket_path, response, requests)
        try:
            with pytest.raises(ipc.IPCError, match=rf"^{error}$"):
                add_job(
                    socket_path,
                    job=record["job"],
                    request_id=record["request_id"],
                    source_url=record["source_url"],
                    priority=record["priority"],
                    order_key=record["order_key"],
                    category=record["category"],
                    partial_filename=record["partial_filename"],
                    selected_final_filename=record["selected_final_filename"],
                )
        finally:
            thread.join(_WATCHDOG_SECONDS)
            socket_path.unlink(missing_ok=True)
        assert not thread.is_alive()
        assert errors == []
        assert requests == [expected_request]


def test_job_add_client_rejects_unbounded_input_before_digest_or_socket(
    short_socket_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import builtins

    add_job = getattr(ipc, "add_job", None)
    command_type = getattr(ipc, "JobAddCommand", None)
    assert callable(add_job), "job_add client is missing"
    assert isinstance(command_type, type), "job_add command is missing"

    socket_path = short_socket_root / "worker.sock"
    handler_commands: list[ipc.JobAddCommand] = []

    def job_add_handler(command: ipc.JobAddCommand) -> ipc.JobAddResult:
        handler_commands.append(command)
        return ipc.JobAddResult(
            applied=True, job=command.job, generation=0, revision=0
        )

    server = ipc.HealthServer(
        socket_path,
        health=lambda: ipc.WorkerHealth(worker_epoch=1, queue_gate="paused"),
        job_add=job_add_handler,
    )
    socket_calls: list[object] = []
    digest_calls: list[object] = []
    engine_imports: list[str] = []
    original_import = builtins.__import__

    def reject_engine_import(
        name: str,
        globals: object = None,
        locals: object = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        if name in {
            "hermes_downloads.direct",
            "hermes_downloads.paths",
            "hermes_downloads.video",
        }:
            engine_imports.append(name)
            raise AssertionError("invalid job add imported an engine")
        return cast(Any, original_import)(name, globals, locals, fromlist, level)

    def reject_socket(*args: object, **kwargs: object) -> object:
        socket_calls.append((args, kwargs))
        raise AssertionError("invalid job add opened a socket")

    def reject_digest(record: object) -> str:
        digest_calls.append(record)
        raise AssertionError("unbounded job add reached canonical digest")

    marker = "must-not-leak"
    url_within_source_policy = (
        "https://downloads.example.test/payload?signature=" + marker + "u" * 5_000
    )
    assert len(url_within_source_policy.encode("utf-8")) <= 8_192
    cases: tuple[tuple[str, dict[str, object], type[Exception]], ...] = (
        (
            "oversized-url-within-url-policy",
            {"source_url": url_within_source_policy},
            ValueError,
        ),
        (
            "oversized-filename",
            {
                "partial_filename": marker + "f" * 5_000 + ".bin",
                "selected_final_filename": marker + "f" * 5_000 + ".bin",
            },
            ValueError,
        ),
        (
            "escape-heavy-url",
            {
                "source_url": "https://downloads.example.test/payload?signature="
                + marker
                + '"' * 2_500,
            },
            ValueError,
        ),
        (
            "escape-heavy-filename",
            {
                "partial_filename": marker + '"' * 2_500 + ".bin",
                "selected_final_filename": marker + '"' * 2_500 + ".bin",
            },
            ValueError,
        ),
        ("invalid-primitive", {"source_url": True}, TypeError),
    )
    monkeypatch.setattr(builtins, "__import__", reject_engine_import)
    monkeypatch.setattr(ipc.socket, "socket", reject_socket)
    monkeypatch.setattr(ipc, "_canonical_payload_digest", reject_digest)
    try:
        for index, (_case, overrides, direct_error) in enumerate(cases):
            values = {
                "job": f"bounded-job-{index}",
                "request_id": f"bounded-request-{index}",
                "selected_final_filename": "payload.bin",
                **overrides,
            }
            record = _job_add_record(**cast(dict[str, Any], values))
            with pytest.raises(direct_error) as direct_failure:
                command_type(
                    job=record["job"],
                    request_id=record["request_id"],
                    source_url=record["source_url"],
                    priority=record["priority"],
                    order_key=record["order_key"],
                    category=record["category"],
                    partial_filename=record["partial_filename"],
                    selected_final_filename=record["selected_final_filename"],
                )
            assert marker not in str(direct_failure.value)
            assert marker not in repr(direct_failure.value)

            with pytest.raises(ipc.IPCError, match="^invalid_request$") as public_failure:
                add_job(
                    socket_path,
                    job=record["job"],
                    request_id=record["request_id"],
                    source_url=record["source_url"],
                    priority=record["priority"],
                    order_key=record["order_key"],
                    category=record["category"],
                    partial_filename=record["partial_filename"],
                    selected_final_filename=record["selected_final_filename"],
                )
            assert marker not in str(public_failure.value)
            assert marker not in repr(public_failure.value)
    finally:
        server.close()

    assert socket_calls == []
    assert handler_commands == []
    assert digest_calls == []
    assert engine_imports == []


def test_worker_job_add_persists_a_cold_direct_job_without_engine_activation(
    short_socket_root: Path,
) -> None:
    state_root = short_socket_root / "state"
    state_root.mkdir(mode=0o700)
    socket_path = state_root / "worker.sock"
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    shutdown = context.Event()
    stopped = context.Event()
    results = context.Queue()
    resolver_calls = context.Value("i", 0)

    with _origin_type()() as origin:
        process = context.Process(
            target=_run_worker_process_with_job_add_guards,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                resolver_calls,
            ),
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert set_queue_gate(
                socket_path,
                gate="running",
                request_id="open-before-add",
                expected_revision=1,
            ).to_record() == {
                "applied": True,
                "queue_gate": "running",
                "revision": 2,
            }
            state_entries_before = tuple(sorted(path.name for path in state_root.iterdir()))
            source_url = (
                f"http://job-add.example.test:{origin.port}/range?"
                "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-SignedHeaders=host&"
                "X-Amz-Signature=fixture-exact-signed-query"
            )
            assert len(_job_add_payload(_job_add_record(source_url=source_url))) <= MAX_MESSAGE_BYTES
            result = _add_job(
                socket_path,
                job="cold-direct-job",
                request_id="cold-direct-request",
                source_url=source_url,
                priority=-17,
                order_key=23,
                category="Documents",
                partial_filename="cold.bin",
                selected_final_filename="cold--cold-direct-job.bin",
            )
            assert result.to_record() == {
                "applied": True,
                "job": "cold-direct-job",
                "generation": 0,
                "revision": 0,
            }
            assert "source-private" not in json.dumps(result.to_record())
            assert tuple(sorted(path.name for path in state_root.iterdir())) == state_entries_before
            assert resolver_calls.value == 0
            assert origin.ledger.request_count == 0
            assert origin.ledger.response_body_bytes == 0
            assert not (state_root / "direct-runtime").exists()

            observer = SQLiteStore(state_root / "state.db")
            try:
                job = observer.get_job("cold-direct-job")
                materialized = observer.get_materialized_job("cold-direct-job")
                assert job is not None
                assert materialized is not None
                assert (job.generation, job.revision, job.state) == (0, 0, "queued")
                assert job.source_url == source_url.encode("utf-8")
                assert materialized.source_kind is SourceKind.DIRECT
                assert materialized.queue_collection_id is None
                assert materialized.scheduled_for is None
                assert materialized.destination_collection is None
                assert (
                    materialized.priority,
                    materialized.order_key,
                    materialized.category,
                    materialized.partial_filename,
                    materialized.selected_final_filename,
                ) == (
                    -17,
                    23,
                    "Documents",
                    "cold.bin",
                    "cold--cold-direct-job.bin",
                )
                assert (
                    materialized.authorized,
                    materialized.manual_hold,
                    materialized.start_now_requested,
                    materialized.intent.expected_revision,
                ) == (False, False, False, None)
                assert [event.kind for event in observer.list_events()] == ["job_added"]
            finally:
                observer.close()

            started = ipc.control_job(
                socket_path,
                job="cold-direct-job",
                action="start_now",
                request_id="cold-direct-start",
                expected_revision=0,
            )
            assert started == ipc.JobControlResult(
                status="applied",
                job="cold-direct-job",
                generation=0,
                revision=1,
                state="queued",
                authorized=True,
            )
            assert resolver_calls.value == 0
            assert origin.ledger.request_count == 0
            assert origin.ledger.response_body_bytes == 0
            assert not (state_root / "direct-runtime").exists()

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_worker_job_add_replays_conflicts_globally_and_has_no_partial_write(
    short_socket_root: Path,
) -> None:
    state_root = short_socket_root / "state"
    state_root.mkdir(mode=0o700)
    socket_path = state_root / "worker.sock"
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    shutdown = context.Event()
    stopped = context.Event()
    results = context.Queue()
    process = context.Process(
        target=_run_worker_process,
        args=(str(state_root), str(socket_path), ready, shutdown, stopped, results),
    )
    process.start()
    try:
        assert ready.wait(_WATCHDOG_SECONDS), _result(results)
        base = {
            "job": "replay-job",
            "request_id": "replay-request",
            "source_url": "https://downloads.example.test/replay.bin?signature=first",
            "priority": -3,
            "order_key": 5,
            "category": "Other",
            "partial_filename": "replay.bin",
            "selected_final_filename": "replay.bin",
        }
        first = _add_job(socket_path, **base)
        assert first.to_record() == {
            "applied": True,
            "job": "replay-job",
            "generation": 0,
            "revision": 0,
        }
        after_first = _job_add_store_snapshot(state_root)
        assert _add_job(socket_path, **base).to_record() == {
            "applied": False,
            "job": "replay-job",
            "generation": 0,
            "revision": 0,
        }
        assert _job_add_store_snapshot(state_root) == after_first

        for changed in (
            {"job": "different-job"},
            {"source_url": "https://downloads.example.test/replay.bin?signature=changed"},
            {"priority": 4},
            {"order_key": 6},
            {"category": "Documents"},
            {
                "partial_filename": "changed.bin",
                "selected_final_filename": "changed.bin",
            },
            {"selected_final_filename": "replay--replay-job.bin"},
        ):
            with pytest.raises(ipc.IPCError, match="^command_conflict$"):
                _add_job(socket_path, **{**base, **changed})
            assert _job_add_store_snapshot(state_root) == after_first

        with pytest.raises(ipc.IPCError, match="^command_conflict$"):
            ipc.control_job(
                socket_path,
                job="replay-job",
                action="pause",
                request_id="replay-request",
                expected_revision=0,
            )
        assert _job_add_store_snapshot(state_root) == after_first

        _add_job(
            socket_path,
            job="occupied-job",
            request_id="occupied-request",
            source_url="https://downloads.example.test/occupied.bin",
            priority=0,
            order_key=6,
            category="Other",
            partial_filename="occupied.bin",
            selected_final_filename="occupied.bin",
        )
        before_occupied_conflict = _job_add_store_snapshot(state_root)
        with pytest.raises(ipc.IPCError, match="^command_conflict$"):
            _add_job(
                socket_path,
                job="occupied-job",
                request_id="new-request-for-occupied-job",
                source_url="https://downloads.example.test/new.bin",
                priority=0,
                order_key=7,
                category="Other",
                partial_filename="new.bin",
                selected_final_filename="new.bin",
            )
        assert _job_add_store_snapshot(state_root) == before_occupied_conflict

        assert set_queue_gate(
            socket_path,
            gate="running",
            request_id="queue-gate-collision",
            expected_revision=1,
        ).applied is True
        before_queue_collision = _job_add_store_snapshot(state_root)
        with pytest.raises(ipc.IPCError, match="^command_conflict$"):
            _add_job(
                socket_path,
                job="queue-gate-collision-job",
                request_id="queue-gate-collision",
                source_url="https://downloads.example.test/gate.bin",
                priority=0,
                order_key=8,
                category="Other",
                partial_filename="gate.bin",
                selected_final_filename="gate.bin",
            )
        assert _job_add_store_snapshot(state_root) == before_queue_collision

        _add_job(
            socket_path,
            job="controlled-job",
            request_id="controlled-add-request",
            source_url="https://downloads.example.test/controlled.bin",
            priority=0,
            order_key=9,
            category="Other",
            partial_filename="controlled.bin",
            selected_final_filename="controlled.bin",
        )
        assert ipc.control_job(
            socket_path,
            job="controlled-job",
            action="pause",
            request_id="job-control-collision",
            expected_revision=0,
        ).status == "applied"
        before_control_collision = _job_add_store_snapshot(state_root)
        with pytest.raises(ipc.IPCError, match="^command_conflict$"):
            _add_job(
                socket_path,
                job="job-control-collision-job",
                request_id="job-control-collision",
                source_url="https://downloads.example.test/control.bin",
                priority=0,
                order_key=10,
                category="Other",
                partial_filename="control.bin",
                selected_final_filename="control.bin",
            )
        assert _job_add_store_snapshot(state_root) == before_control_collision

        shutdown.set()
        assert stopped.wait(_WATCHDOG_SECONDS)
        _join(process)
        assert _result(results) == ("result", None)
    finally:
        shutdown.set()
        if process.is_alive():
            _join(process)


def test_worker_job_add_replays_original_receipt_after_lifecycle_controls(
    short_socket_root: Path,
) -> None:
    state_root = short_socket_root / "state"
    state_root.mkdir(mode=0o700)
    socket_path = state_root / "worker.sock"
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    shutdown = context.Event()
    stopped = context.Event()
    results = context.Queue()
    process = context.Process(
        target=_run_worker_process,
        args=(str(state_root), str(socket_path), ready, shutdown, stopped, results),
    )
    process.start()
    try:
        assert ready.wait(_WATCHDOG_SECONDS), _result(results)
        paused_add = {
            "job": "pause-replay-job",
            "request_id": "pause-replay-add-request",
            "source_url": "https://downloads.example.test/pause-replay.bin?sig=one",
            "priority": -1,
            "order_key": 1,
            "category": "Other",
            "partial_filename": "pause-replay.bin",
            "selected_final_filename": "pause-replay.bin",
        }
        assert _add_job(socket_path, **paused_add).to_record() == {
            "applied": True,
            "job": "pause-replay-job",
            "generation": 0,
            "revision": 0,
        }
        assert ipc.control_job(
            socket_path,
            job="pause-replay-job",
            action="pause",
            request_id="pause-replay-control-request",
            expected_revision=0,
        ).to_record() == {
            "status": "applied",
            "job": "pause-replay-job",
            "generation": 0,
            "revision": 1,
            "state": "paused",
            "authorized": False,
        }
        after_pause = _job_add_store_snapshot(state_root)
        assert _add_job(socket_path, **paused_add).to_record() == {
            "applied": False,
            "job": "pause-replay-job",
            "generation": 0,
            "revision": 0,
        }
        assert _job_add_store_snapshot(state_root) == after_pause
        with pytest.raises(ipc.IPCError, match="^command_conflict$"):
            _add_job(socket_path, **{**paused_add, "priority": 1})
        assert _job_add_store_snapshot(state_root) == after_pause

        assert set_queue_gate(
            socket_path,
            gate="running",
            request_id="open-queue-for-start-now",
            expected_revision=1,
        ).to_record() == {
            "applied": True,
            "queue_gate": "running",
            "revision": 2,
        }
        started_add = {
            "job": "start-replay-job",
            "request_id": "start-replay-add-request",
            "source_url": "https://downloads.example.test/start-replay.bin?sig=two",
            "priority": -2,
            "order_key": 2,
            "category": "Other",
            "partial_filename": "start-replay.bin",
            "selected_final_filename": "start-replay.bin",
        }
        assert _add_job(socket_path, **started_add).to_record() == {
            "applied": True,
            "job": "start-replay-job",
            "generation": 0,
            "revision": 0,
        }
        assert ipc.control_job(
            socket_path,
            job="start-replay-job",
            action="start_now",
            request_id="start-replay-control-request",
            expected_revision=0,
        ).to_record() == {
            "status": "applied",
            "job": "start-replay-job",
            "generation": 0,
            "revision": 1,
            "state": "queued",
            "authorized": True,
        }
        after_start_now = _job_add_store_snapshot(state_root)
        assert _add_job(socket_path, **started_add).to_record() == {
            "applied": False,
            "job": "start-replay-job",
            "generation": 0,
            "revision": 0,
        }
        assert _job_add_store_snapshot(state_root) == after_start_now
        with pytest.raises(ipc.IPCError, match="^command_conflict$"):
            _add_job(socket_path, **{**started_add, "order_key": 3})
        assert _job_add_store_snapshot(state_root) == after_start_now

        shutdown.set()
        assert stopped.wait(_WATCHDOG_SECONDS)
        _join(process)
        assert _result(results) == ("result", None)
    finally:
        shutdown.set()
        if process.is_alive():
            _join(process)


def test_worker_job_add_rejects_closed_invalid_source_and_oversized_requests(
    short_socket_root: Path,
) -> None:
    state_root = short_socket_root / "state"
    state_root.mkdir(mode=0o700)
    socket_path = state_root / "worker.sock"
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    shutdown = context.Event()
    stopped = context.Event()
    results = context.Queue()
    process = context.Process(
        target=_run_worker_process,
        args=(str(state_root), str(socket_path), ready, shutdown, stopped, results),
    )
    process.start()
    try:
        assert ready.wait(_WATCHDOG_SECONDS), _result(results)
        valid = _job_add_record()
        baseline = _job_add_store_snapshot(state_root)
        malformed_records = (
            {key: value for key, value in valid.items() if key != "source_url"},
            {**valid, "unexpected": True},
            {**valid, "payload_digest": "a" * 64},
            {**valid, "job": "bad/job"},
            {**valid, "source_kind": "video"},
            {**valid, "start": True},
            {**valid, "priority": True},
            {**valid, "priority": 1 << 31},
            {**valid, "priority": -(1 << 31) - 1},
            {**valid, "order_key": True},
            {**valid, "order_key": -1},
            {**valid, "order_key": 1 << 63},
            {**valid, "category": "Unexpected"},
            {**valid, "partial_filename": "../unsafe.bin"},
            {**valid, "selected_final_filename": "another.bin"},
            {**valid, "source_url": True},
            {**valid, "source_url": "not a URL"},
            {**valid, "source_url": "https://user:pass@example.test/private.bin"},
            {**valid, "source_url": "http://127.0.0.1:18080/range"},
            {**valid, "source_url": "https://downloads.example.test/" + "x" * 8193},
            {**valid, "request_id": "x" * 129},
        )
        for record in malformed_records:
            assert _raw_request(socket_path, _job_add_payload(record)) == {
                "error": "invalid_request"
            }
            assert _job_add_store_snapshot(state_root) == baseline

        duplicate = _job_add_payload(valid).replace(
            b'"op":"job_add",', b'"op":"job_add","op":"health",', 1
        )
        assert _raw_request(socket_path, duplicate) == {"error": "invalid_request"}
        assert _raw_request(socket_path, b"x" * (MAX_MESSAGE_BYTES + 1)) == {
            "error": "invalid_request"
        }
        assert _job_add_store_snapshot(state_root) == baseline
        assert request_health(socket_path).to_record() == {
            "protocol_version": 1,
            "worker_epoch": 1,
            "queue_gate": "paused",
        }

        shutdown.set()
        assert stopped.wait(_WATCHDOG_SECONDS)
        _join(process)
        assert _result(results) == ("result", None)
    finally:
        shutdown.set()
        if process.is_alive():
            _join(process)


def test_worker_job_remove_tombstones_cold_direct_job_without_engine_or_file_side_effects(
    short_socket_root: Path,
) -> None:
    state_root = short_socket_root / "state"
    state_root.mkdir(mode=0o700)
    socket_path = state_root / "worker.sock"
    payload_path = short_socket_root / "user-owned-payload.bin"
    payload_path.write_bytes(b"preserve this payload")
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    shutdown = context.Event()
    stopped = context.Event()
    results = context.Queue()
    resolver_calls = context.Value("i", 0)

    with _origin_type()() as origin:
        process = context.Process(
            target=_run_worker_process_with_job_add_guards,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                resolver_calls,
            ),
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            state_entries_before = tuple(sorted(path.name for path in state_root.iterdir()))
            source_url = (
                f"http://job-add.example.test:{origin.port}/range?"
                "X-Amz-Signature=remove-preserves-owned-data"
            )
            assert _add_job(
                socket_path,
                job="removed-direct-job",
                request_id="removed-direct-add-request",
                source_url=source_url,
                priority=-4,
                order_key=17,
                category="Documents",
                partial_filename="remove.bin",
                selected_final_filename="remove--removed-direct-job.bin",
            ).to_record() == {
                "applied": True,
                "job": "removed-direct-job",
                "generation": 0,
                "revision": 0,
            }

            removed = ipc.control_job(
                socket_path,
                job="removed-direct-job",
                action="remove",
                request_id="removed-direct-request",
                expected_revision=0,
            )
            assert removed.to_record() == {
                "status": "applied",
                "job": "removed-direct-job",
                "generation": 0,
                "revision": 1,
                "state": "removed",
                "authorized": False,
            }
            assert ipc.control_job(
                socket_path,
                job="removed-direct-job",
                action="remove",
                request_id="removed-direct-request",
                expected_revision=0,
            ) == removed
            assert source_url not in json.dumps(removed.to_record())
            assert payload_path.read_bytes() == b"preserve this payload"
            assert tuple(sorted(path.name for path in state_root.iterdir())) == state_entries_before
            assert resolver_calls.value == 0
            assert origin.ledger.request_count == 0
            assert origin.ledger.response_body_bytes == 0
            assert not (state_root / "direct-runtime").exists()

            observer = SQLiteStore(state_root / "state.db")
            try:
                job = observer.get_job("removed-direct-job")
                materialized = observer.get_materialized_job("removed-direct-job")
                assert job is not None
                assert materialized is not None
                assert (
                    job.source_url,
                    job.generation,
                    job.revision,
                    job.state,
                ) == (source_url.encode("utf-8"), 0, 1, "removed")
                assert materialized.source_kind is SourceKind.DIRECT
                assert (
                    materialized.authorized,
                    materialized.manual_hold,
                    materialized.start_now_requested,
                ) == (False, True, False)
                assert [event.kind for event in observer.list_events()] == [
                    "job_added",
                    "job_removed",
                ]
                receipt = observer._connection.execute(
                    """
                    SELECT action, status, generation, revision, state, authorized
                    FROM job_control_commands
                    WHERE request_id = 'removed-direct-request'
                    """
                ).fetchone()
                assert receipt is not None
                assert tuple(receipt) == ("remove", "applied", 0, 1, "removed", 0)
            finally:
                observer.close()

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
        finally:
            shutdown.set()
            if process.is_alive():
                _join(process)


def test_direct_job_dispatch_client_uses_a_closed_fenced_envelope(
    short_socket_root: Path,
) -> None:
    command_type = getattr(ipc, "DirectJobDispatchCommand", None)
    result_type = getattr(ipc, "DirectJobDispatchResult", None)
    dispatch = getattr(ipc, "dispatch_direct_job", None)
    assert isinstance(command_type, type), "direct dispatch command is missing"
    assert isinstance(result_type, type), "direct dispatch result is missing"
    assert callable(dispatch), "direct dispatch client is missing"

    command = command_type(
        job="dispatch-job",
        expected_worker_epoch=2,
        expected_generation=4,
        expected_revision=7,
        request_id="dispatch-request",
    )
    assert command.to_record() == {
        "op": "direct_job_dispatch",
        "job": "dispatch-job",
        "expected_worker_epoch": 2,
        "expected_generation": 4,
        "expected_revision": 7,
        "request_id": "dispatch-request",
    }

    socket_path = short_socket_root / "worker.sock"
    commands: list[object] = []

    def direct_job_dispatch(command: object) -> object:
        commands.append(command)
        return result_type(
            status="started",
            job="dispatch-job",
            generation=4,
            revision=9,
            state="downloading",
        )

    server = ipc.HealthServer(
        socket_path,
        health=lambda: ipc.WorkerHealth(worker_epoch=2, queue_gate="running"),
        direct_job_dispatch=direct_job_dispatch,
    )
    try:
        result = _serve_one(
            server,
            lambda: dispatch(
                socket_path,
                job="dispatch-job",
                expected_worker_epoch=2,
                expected_generation=4,
                expected_revision=7,
                request_id="dispatch-request",
            ),
        )
    finally:
        server.close()

    assert result.to_record() == {
        "status": "started",
        "job": "dispatch-job",
        "generation": 4,
        "revision": 9,
        "state": "downloading",
    }
    assert [captured.to_record() for captured in commands] == [command.to_record()]


def test_direct_job_dispatch_rejects_closed_malformed_envelopes(
    short_socket_root: Path,
) -> None:
    socket_path = short_socket_root / "worker.sock"
    commands: list[object] = []

    def direct_job_dispatch(command: object) -> ipc.DirectJobDispatchResult:
        commands.append(command)
        return ipc.DirectJobDispatchResult(
            status="blocked",
            job="dispatch-job",
            generation=1,
            revision=2,
            state="queued",
        )

    server = ipc.HealthServer(
        socket_path,
        health=lambda: ipc.WorkerHealth(worker_epoch=1, queue_gate="paused"),
        direct_job_dispatch=direct_job_dispatch,
    )
    valid = {
        "op": "direct_job_dispatch",
        "job": "dispatch-job",
        "expected_worker_epoch": 1,
        "expected_generation": 1,
        "expected_revision": 2,
        "request_id": "dispatch-request",
    }
    malformed = (
        {key: value for key, value in valid.items() if key != "expected_generation"},
        {**valid, "unexpected": True},
        {**valid, "expected_worker_epoch": 0},
        {**valid, "expected_generation": True},
        {**valid, "expected_revision": -1},
        {**valid, "job": "../dispatch-job"},
    )
    try:
        for record in malformed:
            payload = json.dumps(record, separators=(",", ":")).encode("utf-8") + b"\n"
            assert _serve_one(server, lambda payload=payload: _raw_request(socket_path, payload)) == {
                "error": "invalid_request"
            }
        duplicate = (
            b'{"op":"direct_job_dispatch","op":"health","job":"dispatch-job",'
            b'"expected_worker_epoch":1,"expected_generation":1,'
            b'"expected_revision":2,"request_id":"dispatch-request"}\n'
        )
        assert _serve_one(server, lambda: _raw_request(socket_path, duplicate)) == {
            "error": "invalid_request"
        }
    finally:
        server.close()
    assert commands == []


def test_worker_direct_dispatch_attests_before_body_and_pause_stops_running_bytes(
    short_socket_root: Path,
) -> None:
    state_root = short_socket_root / "state"
    state_root.mkdir(mode=0o700)
    socket_path = state_root / "worker.sock"
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    shutdown = context.Event()
    stopped = context.Event()
    results = context.Queue()
    marker_attested = context.Event()
    release_paused_add = context.Event()
    paused_add_complete = context.Event()
    release_resume = context.Event()
    destination_root = Path.home() / "Downloads" / "Hermes"
    destination_root.mkdir(parents=True, mode=0o700)
    destination_root.chmod(0o700)

    with _origin_type()(payload_size=16 * 1024 * 1024) as origin:
        _seed_local_direct_dispatch_job(state_root, origin.url("/range"))
        process = context.Process(
            target=_run_worker_process_with_direct_dispatch_gates,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                origin.url(),
                marker_attested,
                release_paused_add,
                paused_add_complete,
                release_resume,
            ),
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert request_health(socket_path) == ipc.WorkerHealth(
                worker_epoch=1, queue_gate="paused"
            )
            assert set_queue_gate(
                socket_path,
                gate="running",
                request_id="dispatch-open-queue",
                expected_revision=1,
            ).to_record() == {
                "applied": True,
                "queue_gate": "running",
                "revision": 2,
            }
            assert ipc.control_job(
                socket_path,
                job="dispatch-job",
                action="start_now",
                request_id="dispatch-start-now",
                expected_revision=1,
            ).to_record() == {
                "status": "applied",
                "job": "dispatch-job",
                "generation": 1,
                "revision": 2,
                "state": "queued",
                "authorized": True,
            }
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="active")

            responses: list[object] = []
            failures: list[BaseException] = []

            def dispatch() -> None:
                try:
                    responses.append(
                        ipc.dispatch_direct_job(
                            socket_path,
                            job="dispatch-job",
                            expected_worker_epoch=1,
                            expected_generation=1,
                            expected_revision=2,
                            request_id="dispatch-request",
                        )
                    )
                except BaseException as error:
                    failures.append(error)

            thread = threading.Thread(target=dispatch)
            thread.start()
            assert marker_attested.wait(_WATCHDOG_SECONDS), failures

            incomplete_dir = Path.home() / "Downloads" / "Hermes" / ".incomplete" / "dispatch-job"
            partial_path = incomplete_dir / "dispatch.bin"
            marker_path = incomplete_dir / ".hermes-reservation"
            final_path = Path.home() / "Downloads" / "Hermes" / "Other" / "dispatch.bin"
            observer = SQLiteStore(state_root / "state.db")
            try:
                binding = observer.get_publication_marker_binding("dispatch-job")
                assert binding is not None
                marker_details = marker_path.stat()
                assert (marker_details.st_dev, marker_details.st_ino) == (
                    binding.marker_device,
                    binding.marker_inode,
                )
            finally:
                observer.close()
            assert origin.ledger.response_body_bytes == 0
            assert not partial_path.exists()
            assert not final_path.exists()

            release_paused_add.set()
            assert paused_add_complete.wait(_WATCHDOG_SECONDS)
            assert origin.ledger.response_body_bytes == 0
            assert not partial_path.exists() or partial_path.stat().st_size == 0
            release_resume.set()
            thread.join(_WATCHDOG_SECONDS)
            assert not thread.is_alive()
            assert failures == []
            assert responses == [
                ipc.DirectJobDispatchResult(
                    status="started",
                    job="dispatch-job",
                    generation=1,
                    revision=4,
                    state="downloading",
                )
            ]

            deadline = time.monotonic() + _WATCHDOG_SECONDS
            while (
                (not partial_path.exists() or partial_path.stat().st_size == 0)
                and time.monotonic() < deadline
            ):
                time.sleep(0.005)
            assert partial_path.stat().st_size > 0
            assert not final_path.exists()

            paused = ipc.control_job(
                socket_path,
                job="dispatch-job",
                action="pause",
                request_id="dispatch-pause",
                expected_revision=4,
            )
            assert paused == ipc.JobControlResult(
                status="applied",
                job="dispatch-job",
                generation=1,
                revision=5,
                state="paused",
                authorized=True,
            )
            paused_bytes = partial_path.stat().st_size
            time.sleep(0.1)
            assert partial_path.stat().st_size == paused_bytes
            assert marker_path.exists()
            assert not final_path.exists()

            resumed = ipc.control_job(
                socket_path,
                job="dispatch-job",
                action="resume",
                request_id="dispatch-resume-only",
                expected_revision=5,
            )
            assert resumed == ipc.JobControlResult(
                status="applied",
                job="dispatch-job",
                generation=1,
                revision=6,
                state="queued",
                authorized=True,
            )
            time.sleep(0.1)
            assert partial_path.stat().st_size == paused_bytes
            assert not final_path.exists()

            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="active")
            redispatched = ipc.dispatch_direct_job(
                socket_path,
                job="dispatch-job",
                expected_worker_epoch=1,
                expected_generation=1,
                expected_revision=6,
                request_id="dispatch-queue-pause",
            )
            assert redispatched == ipc.DirectJobDispatchResult(
                status="started",
                job="dispatch-job",
                generation=1,
                revision=8,
                state="downloading",
            )
            deadline = time.monotonic() + _WATCHDOG_SECONDS
            while partial_path.stat().st_size == paused_bytes and time.monotonic() < deadline:
                time.sleep(0.005)
            assert partial_path.stat().st_size > paused_bytes

            queue_paused = set_queue_gate(
                socket_path,
                gate="paused",
                request_id="dispatch-queue-pause-gate",
                expected_revision=2,
            )
            assert queue_paused == ipc.QueueGateResult(
                applied=True, queue_gate="paused", revision=3
            )
            queue_paused_bytes = partial_path.stat().st_size
            time.sleep(0.1)
            assert partial_path.stat().st_size == queue_paused_bytes
            assert request_jobs_page(socket_path).jobs == (
                ipc.PublicJobRecord(
                    job="dispatch-job",
                    generation=1,
                    revision=9,
                    state="paused",
                ),
            )
            assert marker_path.exists()
            assert not final_path.exists()

            assert set_queue_gate(
                socket_path,
                gate="running",
                request_id="dispatch-reopen-queue",
                expected_revision=3,
            ) == ipc.QueueGateResult(applied=True, queue_gate="running", revision=4)
            assert ipc.control_job(
                socket_path,
                job="dispatch-job",
                action="resume",
                request_id="dispatch-resume-for-remove",
                expected_revision=9,
            ) == ipc.JobControlResult(
                status="applied",
                job="dispatch-job",
                generation=1,
                revision=10,
                state="queued",
                authorized=True,
            )
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="active")
            removing = ipc.dispatch_direct_job(
                socket_path,
                job="dispatch-job",
                expected_worker_epoch=1,
                expected_generation=1,
                expected_revision=10,
                request_id="dispatch-remove",
            )
            assert removing == ipc.DirectJobDispatchResult(
                status="started",
                job="dispatch-job",
                generation=1,
                revision=12,
                state="downloading",
            )
            deadline = time.monotonic() + _WATCHDOG_SECONDS
            while (
                partial_path.stat().st_size == queue_paused_bytes
                and time.monotonic() < deadline
            ):
                time.sleep(0.005)
            assert partial_path.stat().st_size > queue_paused_bytes
            removed = ipc.control_job(
                socket_path,
                job="dispatch-job",
                action="remove",
                request_id="dispatch-remove-control",
                expected_revision=12,
            )
            assert removed == ipc.JobControlResult(
                status="applied",
                job="dispatch-job",
                generation=1,
                revision=13,
                state="removed",
                authorized=False,
            )
            removed_bytes = partial_path.stat().st_size
            time.sleep(0.1)
            assert partial_path.stat().st_size == removed_bytes
            assert marker_path.exists()
            assert not final_path.exists()

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
        finally:
            release_paused_add.set()
            release_resume.set()
            shutdown.set()
            if process.is_alive():
                _join(process)

def test_worker_direct_dispatch_stage_producer_retains_finalizing(
    short_socket_root: Path,
) -> None:
    state_root = short_socket_root / "state"
    state_root.mkdir(mode=0o700)
    socket_path = state_root / "worker.sock"
    destination_root = Path.home() / "Downloads" / "Hermes"
    destination_root.mkdir(parents=True, mode=0o700)
    destination_root.chmod(0o700)
    context = multiprocessing.get_context("spawn")
    ready, shutdown, stopped = context.Event(), context.Event(), context.Event()
    results = context.Queue()
    marker_attested = context.Event()
    release_paused_add = context.Event()
    paused_add_complete = context.Event()
    release_resume = context.Event()

    with _origin_type()(payload_size=1024) as origin:
        _seed_local_direct_dispatch_job(state_root, origin.url("/range"))
        process = context.Process(
            target=_run_worker_process_with_direct_dispatch_gates,
            args=(
                str(state_root),
                str(socket_path),
                ready,
                shutdown,
                stopped,
                results,
                origin.url(),
                marker_attested,
                release_paused_add,
                paused_add_complete,
                release_resume,
            ),
        )
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert set_queue_gate(
                socket_path,
                gate="running",
                request_id="completed-open-queue",
                expected_revision=1,
            ) == ipc.QueueGateResult(applied=True, queue_gate="running", revision=2)
            assert ipc.control_job(
                socket_path,
                job="dispatch-job",
                action="start_now",
                request_id="completed-start-now",
                expected_revision=1,
            ) == ipc.JobControlResult(
                status="applied",
                job="dispatch-job",
                generation=1,
                revision=2,
                state="queued",
                authorized=True,
            )
            assert activate_direct_engine(
                socket_path, expected_worker_epoch=1
            ) == DirectEngineActivateResult(worker_epoch=1, status="active")
            release_paused_add.set()
            release_resume.set()
            dispatched = ipc.dispatch_direct_job(
                socket_path,
                job="dispatch-job",
                expected_worker_epoch=1,
                expected_generation=1,
                expected_revision=2,
                request_id="completed-dispatch",
            )
            assert dispatched == ipc.DirectJobDispatchResult(
                status="started",
                job="dispatch-job",
                generation=1,
                revision=4,
                state="downloading",
            )
            assert marker_attested.wait(_WATCHDOG_SECONDS)
            assert paused_add_complete.wait(_WATCHDOG_SECONDS)
            deadline = time.monotonic() + _WATCHDOG_SECONDS
            while origin.ledger.response_body_bytes < 1024 and time.monotonic() < deadline:
                time.sleep(0.005)
            assert origin.ledger.response_body_bytes >= 1024

            incomplete_dir = Path.home() / "Downloads" / "Hermes" / ".incomplete" / "dispatch-job"
            partial_path = incomplete_dir / "dispatch.bin"
            marker_path = incomplete_dir / ".hermes-reservation"
            final_path = Path.home() / "Downloads" / "Hermes" / "Other" / "dispatch.bin"
            deadline = time.monotonic() + _WATCHDOG_SECONDS
            while (
                (not partial_path.exists() or partial_path.stat().st_size < 1024)
                and time.monotonic() < deadline
            ):
                time.sleep(0.005)
            assert partial_path.exists()
            assert partial_path.stat().st_size == 1024
            assert marker_path.exists()
            assert not final_path.exists()
            deadline = time.monotonic() + _WATCHDOG_SECONDS
            while request_jobs_page(socket_path).jobs[0].state != "finalizing" and time.monotonic() < deadline:
                time.sleep(0.005)
            assert request_jobs_page(socket_path).jobs == (
                ipc.PublicJobRecord(
                    job="dispatch-job",
                    generation=1,
                    revision=5,
                    state="finalizing",
                ),
            )
            observer = SQLiteStore(state_root / "state.db")
            try:
                event_kinds = [event.kind for event in observer.list_events()]
                assert event_kinds == [
                    "job_added",
                    "job_paused",
                    "job_start_now_requested",
                    "job_resolving",
                    "job_downloading",
                    "job_finalizing",
                ]
                deadline = time.monotonic() + _WATCHDOG_SECONDS
                while observer._get_staged_payload_binding("dispatch-job") is None and time.monotonic() < deadline:
                    request_health(socket_path)
                    time.sleep(0.005)
                staged = observer._get_staged_payload_binding("dispatch-job")
                details = partial_path.stat()
                assert staged is not None, "actual completed controller did not produce staged binding"
                assert (staged.partial_device, staged.partial_inode, staged.logical_size) == (details.st_dev, details.st_ino, 1024)
                assert observer._get_final_publication_binding("dispatch-job") is None
                assert ipc.dispatch_direct_job(
                    socket_path, job="dispatch-job", expected_worker_epoch=1,
                    expected_generation=1, expected_revision=2,
                    request_id="completed-dispatch",
                ) == dispatched
                assert [e.kind for e in observer.list_events()].count("job_finalizing") == 1
            finally:
                observer.close()

            shutdown.set()
            assert stopped.wait(_WATCHDOG_SECONDS)
            _join(process)
            assert _result(results) == ("result", None)
        finally:
            release_paused_add.set()
            release_resume.set()
            shutdown.set()
            if process.is_alive():
                _join(process)



def _run_terminal_gated_worker(state_root, socket_path, ready, shutdown, stopped, results,
        origin_url, mode, entered, release, returned):
    from hermes_downloads import direct, network
    original_validate = worker.validate_source_url
    grant = network.LocalOriginGrant.for_url(origin_url)
    worker.validate_source_url = lambda value: original_validate(value, local_origin_grant=grant)
    # This is an event handshake around the real controller, never a synthetic success.
    assert hasattr(direct.DirectAria2Controller, "observe_terminal"), "one-shot terminal observation is missing"
    original_observe = direct.DirectAria2Controller.observe_terminal
    original_verify = direct.DirectAria2Controller._verify_completed_output
    original_rpc = direct.DirectAria2Controller._rpc
    original_close = direct.DirectAria2Controller.close
    original_pause = worker.SQLiteStore.pause_active_direct_job
    original_finalize = worker.SQLiteStore.finalize_direct_terminal
    owner_thread = threading.get_ident()
    controllers = []
    def pause(store, **kwargs):
        assert threading.get_ident() == owner_thread, "SQLite used by observer"
        if mode == "rpc-persist-failure":
            raise RuntimeError("synthetic secret diagnostics")
        return original_pause(store, **kwargs)
    def finalize(store, *args):
        assert threading.get_ident() == owner_thread, "SQLite used by observer"
        return original_finalize(store, *args)
    def close(controller):
        if mode == "rpc-containment-failure":
            raise RuntimeError("synthetic secret diagnostics")
        return original_close(controller)
    def verify(controller, *args):
        if mode in {"verify", "marker"}:
            entered.set()
            assert release.wait(_WATCHDOG_SECONDS)
        return original_verify(controller, *args)
    def observe(controller, **kwargs):
        if controller not in controllers:
            controllers.append(controller)
        if mode in {"observe", "absent", "rpc", "rpc-persist-failure", "rpc-containment-failure"}:
            entered.set()
            if mode == "absent":
                deadline = time.monotonic() + _WATCHDOG_SECONDS
                while controller._process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.005)
                assert controller._process.poll() is not None
                returned.set()
            assert release.wait(_WATCHDOG_SECONDS)
        if mode.startswith("rpc"):
            # Genuine RPC failure: the owned endpoint cannot accept the readback.
            controller._port = 0
        result = original_observe(controller, **kwargs)
        if result is not None and mode in {"late-success", "late-failure"}:
            entered.set()
            assert release.wait(_WATCHDOG_SECONDS)
            returned.set()
            if mode == "late-failure":
                raise direct.DirectTransferError("synthetic private diagnostics https://secret.test/?token=hidden")
        return result
    direct.DirectAria2Controller.observe_terminal = observe
    direct.DirectAria2Controller._verify_completed_output = verify
    direct.DirectAria2Controller.close = close
    worker.SQLiteStore.pause_active_direct_job = pause
    worker.SQLiteStore.finalize_direct_terminal = finalize
    try:
        _run_worker_process(state_root, socket_path, ready, shutdown, stopped, results)
    finally:
        direct.DirectAria2Controller.observe_terminal = original_observe
        direct.DirectAria2Controller._verify_completed_output = original_verify
        direct.DirectAria2Controller._rpc = original_rpc
        direct.DirectAria2Controller.close = original_close
        worker.SQLiteStore.pause_active_direct_job = original_pause
        worker.SQLiteStore.finalize_direct_terminal = original_finalize
        for controller in controllers:
            original_close(controller)
        worker.validate_source_url = original_validate


@pytest.mark.parametrize("mode,action", (
    ("observe", None), ("absent", None), ("verify", None), ("marker", None), ("late-success", "pause"),
    ("late-success", "remove"), ("late-failure", "pause"),
    ("late-success", "shutdown"), ("rpc", None), ("error", None),
    ("rpc-persist-failure", None), ("rpc-containment-failure", None),
))
def test_worker_terminal_observation_is_responsive_and_fenced(short_socket_root, mode, action):
    state_root = short_socket_root / "state"
    state_root.mkdir(mode=0o700)
    socket_path = state_root / "worker.sock"
    root = Path.home() / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    context = multiprocessing.get_context("spawn")
    ready, shutdown, stopped = context.Event(), context.Event(), context.Event()
    entered, release, returned = context.Event(), context.Event(), context.Event()
    results = context.Queue()
    with _origin_type()(payload_size=1024) as origin:
        _seed_local_direct_dispatch_job(state_root, origin.url("/missing" if mode == "error" else "/range"))
        process = context.Process(target=_run_terminal_gated_worker, args=(str(state_root), str(socket_path),
            ready, shutdown, stopped, results, origin.url(), mode, entered, release, returned))
        process.start()
        try:
            assert ready.wait(_WATCHDOG_SECONDS)
            assert set_queue_gate(socket_path, gate="running", request_id="terminal-open", expected_revision=1).applied
            assert ipc.control_job(socket_path, job="dispatch-job", action="start_now", request_id="terminal-authorize", expected_revision=1).status == "applied"
            assert activate_direct_engine(socket_path, expected_worker_epoch=1).status == "active"
            started = ipc.dispatch_direct_job(socket_path, job="dispatch-job", expected_worker_epoch=1,
                expected_generation=1, expected_revision=2, request_id="terminal-dispatch")
            assert (started.status, started.state, started.revision) == ("started", "downloading", 4)
            owned_group = None
            if mode != "error":
                assert entered.wait(_WATCHDOG_SECONDS)
                assert request_health(socket_path).worker_epoch == 1
                assert request_jobs_page(socket_path).jobs[0].state == "downloading"
                assert not release.is_set()
                observer = SQLiteStore(state_root / "state.db")
                try:
                    owned = observer.get_direct_engine_record()
                    assert owned is not None
                    owned_group = owned.identity.process_group_id
                finally:
                    observer.close()
            if mode == "absent":
                observer = SQLiteStore(state_root / "state.db")
                try:
                    record = observer.get_direct_engine_record()
                    assert record is not None
                finally:
                    observer.close()
                os.killpg(record.identity.process_group_id, signal.SIGKILL)
                assert returned.wait(_WATCHDOG_SECONDS)
                assert reconcile_process_birth(record.identity) == "absent"
                assert activate_direct_engine(socket_path, expected_worker_epoch=1).status == "blocked"
                assert not release.is_set()
            if mode == "marker":
                marker = root / ".incomplete" / "dispatch-job" / ".hermes-reservation"
                # Keep the original inode allocated, guaranteeing a distinct replacement.
                marker.rename(marker.with_name("retained-test-marker"))
                marker.write_bytes(b"changed fixture marker")
            if action in {"pause", "remove"}:
                controlled = ipc.control_job(socket_path, job="dispatch-job", action=action,
                    request_id="terminal-control", expected_revision=4)
                assert (controlled.status, controlled.state, controlled.revision) == ("applied", "paused" if action == "pause" else "removed", 5)
                assert not release.is_set()  # containment and ack cannot await delayed verification
                assert owned_group is not None and _group_is_gone(owned_group)
                assert activate_direct_engine(socket_path, expected_worker_epoch=1).status == "blocked"
                observer = SQLiteStore(state_root / "state.db")
                try:
                    assert observer.get_direct_engine_record() is None
                finally:
                    observer.close()
                release.set()
                assert returned.wait(_WATCHDOG_SECONDS)
            elif action == "shutdown":
                shutdown.set()
                release.set()
                assert stopped.wait(_WATCHDOG_SECONDS)
                _join(process)
                assert _result(results) == ("result", None)
            else:
                release.set()
            if mode in {"rpc-persist-failure", "rpc-containment-failure"}:
                assert stopped.wait(_WATCHDOG_SECONDS)
                _join(process)
                assert _result(results) == ("error", "IPCStateError", "direct_dispatch_blocked")
                observer = SQLiteStore(state_root / "state.db")
                try:
                    retained = observer.get_direct_engine_record()
                    assert retained is not None
                    assert observer._get_direct_engine_recovery_capability(retained) is not None
                    assert (observer.get_job("dispatch-job").state, observer.get_job("dispatch-job").revision) == ("downloading", 4)
                    assert [e.kind for e in observer.list_events()].count("job_paused") == 1  # cold bootstrap only
                    assert "job_finalizing" not in [e.kind for e in observer.list_events()]
                finally:
                    observer.close()
                return
            if action != "shutdown":
                expected = "paused" if mode in {"rpc", "error", "marker", "absent"} or action == "pause" else "removed" if action == "remove" else "finalizing"
                deadline = time.monotonic() + _WATCHDOG_SECONDS
                while request_jobs_page(socket_path).jobs[0].state != expected and time.monotonic() < deadline:
                    time.sleep(0.005)
                assert request_jobs_page(socket_path).jobs[0].state == expected
                if expected in {"paused", "removed"} and owned_group is not None:
                    assert _group_is_gone(owned_group)
                assert ipc.dispatch_direct_job(socket_path, job="dispatch-job", expected_worker_epoch=1,
                    expected_generation=1, expected_revision=2, request_id="terminal-dispatch") == started
                if action in {"pause", "remove"}:
                    deadline = time.monotonic() + _WATCHDOG_SECONDS
                    while activate_direct_engine(socket_path, expected_worker_epoch=1).status != "active" and time.monotonic() < deadline:
                        time.sleep(0.005)
                    assert request_jobs_page(socket_path).jobs[0].state == expected
                observer = SQLiteStore(state_root / "state.db")
                try:
                    kinds = [e.kind for e in observer.list_events()]
                    assert kinds.count("job_finalizing") == (1 if expected == "finalizing" else 0)
                    assert "job_completed" not in kinds
                    assert observer._get_final_publication_binding("dispatch-job") is None
                    if expected == "finalizing":
                        deadline = time.monotonic() + _WATCHDOG_SECONDS
                        while observer._get_staged_payload_binding("dispatch-job") is None and time.monotonic() < deadline:
                            request_health(socket_path)
                            time.sleep(0.005)
                        assert observer._get_staged_payload_binding("dispatch-job") is not None
                    else:
                        assert observer._get_staged_payload_binding("dispatch-job") is None
                    if mode in {"rpc", "error"}:
                        assert observer.get_direct_engine_record() is None
                finally:
                    observer.close()
                partial = root / ".incomplete" / "dispatch-job" / "dispatch.bin"
                if mode != "error":
                    assert partial.read_bytes() == origin.payload
                assert (partial.parent / ".hermes-reservation").exists()
                assert not (root / "Other" / "dispatch.bin").exists()
                shutdown.set()
                assert stopped.wait(_WATCHDOG_SECONDS)
                _join(process)
                assert _result(results) == ("result", None)
        finally:
            release.set()
            shutdown.set()
            if process.is_alive():
                _join(process)


def _run_stage_gated_worker(state_root, socket_path, ready, shutdown, stopped, results,
        origin_url, mode, entered, release, returned):
    from hermes_downloads import direct, paths, network
    owner = threading.get_ident()
    grant = network.LocalOriginGrant.for_url(origin_url)
    original_validate = worker.validate_source_url
    worker.validate_source_url = lambda value: original_validate(value, local_origin_grant=grant)
    original_sync = paths._fsync_staged_partial_payload
    original_attest = paths.attest_staged_partial_payload
    original_pause = worker.SQLiteStore.pause_active_direct_job
    original_close = direct.DirectAria2Controller.close
    original_bind = getattr(worker.SQLiteStore, "bind_direct_staged_payload", None)
    assert original_bind is not None, "missing actual stage producer bind"
    def held():
        entered.set()
        assert release.wait(_WATCHDOG_SECONDS)
    def sync(fd):
        assert threading.get_ident() != owner, "stage fsync blocked IPC owner"
        if mode not in {"post-inode", "post-mtime", "pre-rewrite", "post-rewrite", "crash-after"}:
            held()
            if mode in {"error", "late-error", "persist-uncertain", "contain-uncertain"}:
                returned.set()
                raise OSError("private fixture diagnostics")
        return original_sync(fd)
    def attest(*args, **kwargs):
        if mode == "pre-rewrite":
            held()
        result = original_attest(*args, **kwargs)
        if mode in {"post-inode", "post-mtime", "post-rewrite"}:
            held()
        returned.set()
        return result
    def bind(store, *args):
        assert threading.get_ident() == owner, "stage SQLite left owner thread"
        if mode == "bind-error":
            raise sqlite3.OperationalError("private fixture failure")
        result = original_bind(store, *args)
        if mode == "bind-after-error":
            raise sqlite3.OperationalError("fixture failure after durable bind")
        if mode == "crash-after":
            held()
        return result
    def pause(store, **kwargs):
        assert threading.get_ident() == owner
        if mode == "persist-uncertain":
            raise sqlite3.OperationalError("fixture pause failure")
        return original_pause(store, **kwargs)
    def close(controller):
        if mode == "contain-uncertain":
            raise RuntimeError("fixture containment failure")
        return original_close(controller)
    worker.SQLiteStore.pause_active_direct_job = pause
    direct.DirectAria2Controller.close = close
    paths._fsync_staged_partial_payload = sync
    paths.attest_staged_partial_payload = attest
    worker.SQLiteStore.bind_direct_staged_payload = bind
    try:
        _run_worker_process(state_root, socket_path, ready, shutdown, stopped, results)
    finally:
        paths._fsync_staged_partial_payload = original_sync
        paths.attest_staged_partial_payload = original_attest
        worker.SQLiteStore.bind_direct_staged_payload = original_bind
        worker.SQLiteStore.pause_active_direct_job = original_pause
        direct.DirectAria2Controller.close = original_close
        worker.validate_source_url = original_validate


@pytest.mark.parametrize("mode,action", (
    ("success", None), ("error", None), ("bind-error", None), ("bind-after-error", None),
    ("persist-uncertain", None), ("contain-uncertain", None),
    ("success", "pause"), ("success", "remove"), ("success", "queue"), ("success", "close"),
    ("late-error", "pause"), ("late-error", "remove"), ("late-error", "queue"), ("late-error", "close"),
    ("post-inode", None), ("post-mtime", None), ("success", "marker"),
    ("pre-rewrite", None), ("post-rewrite", None),
    ("success", "crash-before"), ("crash-after", "crash-after"),
))
def test_stage_producer_real_completion_races_and_crash_retention(short_socket_root, mode, action):
    state_root = short_socket_root / "state"
    state_root.mkdir(mode=0o700)
    socket_path = state_root / "worker.sock"
    root = Path.home() / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    context = multiprocessing.get_context("spawn")
    ready, shutdown, stopped = context.Event(), context.Event(), context.Event()
    entered, release, returned = context.Event(), context.Event(), context.Event()
    results = context.Queue()
    with _origin_type()(payload_size=1024) as origin:
        _seed_local_direct_dispatch_job(state_root, origin.url("/range"))
        process = context.Process(target=_run_stage_gated_worker, args=(str(state_root), str(socket_path),
            ready, shutdown, stopped, results, origin.url(), mode, entered, release, returned))
        process.start()
        owned = None
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
            assert set_queue_gate(socket_path, gate="running", request_id="stage-open", expected_revision=1).applied
            assert ipc.control_job(socket_path, job="dispatch-job", action="start_now", request_id="stage-authorize", expected_revision=1).status == "applied"
            assert activate_direct_engine(socket_path, expected_worker_epoch=1).status == "active"
            started = ipc.dispatch_direct_job(socket_path, job="dispatch-job", expected_worker_epoch=1,
                expected_generation=1, expected_revision=2, request_id="stage-start")
            assert entered.wait(_WATCHDOG_SECONDS), "real finalizing never reached stage operation"
            partial = root / ".incomplete" / "dispatch-job" / "dispatch.bin"
            marker = partial.parent / ".hermes-reservation"
            observer = SQLiteStore(state_root / "state.db")
            try:
                owned = observer.get_direct_engine_record()
                assert owned is not None
                assert observer.get_job("dispatch-job").state == "finalizing"
                assert [e.kind for e in observer.list_events()].count("job_finalizing") == 1
                assert (observer._get_staged_payload_binding("dispatch-job") is not None) == (action == "crash-after")
            finally:
                observer.close()
            if action != "crash-after":
                before = time.monotonic()
                assert request_health(socket_path).worker_epoch == 1
                assert request_jobs_page(socket_path).jobs[0].state == "finalizing"
                assert time.monotonic() - before < 2
                assert ipc.dispatch_direct_job(socket_path, job="dispatch-job", expected_worker_epoch=1,
                    expected_generation=1, expected_revision=2, request_id="stage-start") == started
            if action in {"pause", "remove"}:
                controlled = ipc.control_job(socket_path, job="dispatch-job", action=action,
                    request_id="stage-control", expected_revision=5)
                assert (controlled.status, controlled.state, controlled.revision) == ("applied", "paused" if action == "pause" else "removed", 6)
                assert _group_is_gone(owned.identity.process_group_id)
                assert activate_direct_engine(socket_path, expected_worker_epoch=1).status == "blocked"
            elif action == "queue":
                assert set_queue_gate(socket_path, gate="paused", request_id="stage-queue", expected_revision=2).applied
                assert _group_is_gone(owned.identity.process_group_id)
            elif action == "marker":
                marker.rename(marker.with_name("retained-marker"))
                marker.write_bytes(b"changed fixture marker")
            elif action in {"crash-before", "crash-after"}:
                process.kill()
                process.join(_WATCHDOG_SECONDS)
                assert not process.is_alive()
                assert process.exitcode < 0
            elif action == "close":
                shutdown.set()
                assert stopped.wait(_WATCHDOG_SECONDS)
                _join(process)
                assert _result(results) == ("result", None)
                assert _group_is_gone(owned.identity.process_group_id)
            if mode == "post-inode":
                partial.rename(partial.with_name("retained-payload"))
                partial.write_bytes(origin.payload)  # Same size, different allocated inode.
            elif mode == "post-mtime":
                details = partial.stat()
                os.utime(partial, ns=(details.st_atime_ns, details.st_mtime_ns + 1000000))
            elif mode in {"pre-rewrite", "post-rewrite"}:
                details = partial.stat()
                with partial.open("r+b") as payload:
                    payload.write(bytes([origin.payload[0] ^ 1]))
                    payload.flush()
                    os.fsync(payload.fileno())
                os.utime(partial, ns=(details.st_atime_ns, details.st_mtime_ns))
                current = partial.stat()
                assert (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) == (
                    details.st_dev, details.st_ino, details.st_size, details.st_mtime_ns)
                assert current.st_ctime_ns != details.st_ctime_ns
            if process.is_alive():
                release.set()
            if mode in {"persist-uncertain", "contain-uncertain"}:
                assert stopped.wait(_WATCHDOG_SECONDS)
                _join(process)
                assert _result(results) == ("error", "IPCStateError", "direct_dispatch_blocked")
                observer = SQLiteStore(state_root / "state.db")
                try:
                    retained = observer.get_direct_engine_record()
                    assert retained == owned
                    assert observer._get_direct_engine_recovery_capability(retained) is not None
                    assert (observer.get_job("dispatch-job").state, observer.get_job("dispatch-job").revision) == ("finalizing", 5)
                    assert [e.kind for e in observer.list_events()].count("job_paused") == 1
                    assert observer._get_staged_payload_binding("dispatch-job") is None
                finally:
                    observer.close()
                assert partial.read_bytes() == origin.payload
                assert marker.exists()
                assert not (root / "Other" / "dispatch.bin").exists()
                return
            if action not in {"close", "crash-before", "crash-after"}:
                deadline = time.monotonic() + _WATCHDOG_SECONDS
                expected = "removed" if action == "remove" else "paused" if (action in {"pause", "queue", "marker"} or mode != "success") else "finalizing"
                while time.monotonic() < deadline:
                    observer = SQLiteStore(state_root / "state.db")
                    try:
                        settled = observer.get_job("dispatch-job").state == expected and (expected != "finalizing" or observer._get_staged_payload_binding("dispatch-job") is not None)
                    finally:
                        observer.close()
                    if settled:
                        break
                    request_health(socket_path)
                    time.sleep(0.005)
                assert settled
                shutdown.set()
                assert stopped.wait(_WATCHDOG_SECONDS)
                _join(process)
                assert _result(results) == ("result", None)
            observer = SQLiteStore(state_root / "state.db")
            try:
                assert (observer._get_staged_payload_binding("dispatch-job") is not None) == (action == "crash-after" or (mode in {"success", "bind-after-error"} and action is None))
                assert observer._get_final_publication_binding("dispatch-job") is None
                assert [e.kind for e in observer.list_events()].count("job_finalizing") == 1
                assert "job_completed" not in [e.kind for e in observer.list_events()]
                if action in {"crash-before", "crash-after"}:
                    observer.recover_cold_start()
                    assert observer.get_job("dispatch-job").state == "paused"
                    assert (observer._get_staged_payload_binding("dispatch-job") is not None) == (action == "crash-after")
            finally:
                observer.close()
            expected_payload = (bytes([origin.payload[0] ^ 1]) + origin.payload[1:]
                if mode in {"pre-rewrite", "post-rewrite"} else origin.payload)
            assert partial.read_bytes() == expected_payload
            assert marker.exists()
            assert not (root / "Other" / "dispatch.bin").exists()
        finally:
            # A killed process may hold a multiprocessing Event's semaphore.
            # Never acquire shared event locks after a crash or process exit.
            if process.is_alive():
                release.set()
                shutdown.set()
                _join(process)
            if owned is not None and not _group_is_gone(owned.identity.process_group_id):
                os.killpg(owned.identity.process_group_id, signal.SIGKILL)
