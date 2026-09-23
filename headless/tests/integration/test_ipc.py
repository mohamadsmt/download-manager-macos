"""Real worker-owned AF_UNIX health IPC coverage."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import multiprocessing
import os
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
    original_clear_direct_engine_record = worker.SQLiteStore.clear_direct_engine_record
    expected_record = DirectEngineRecord(worker_epoch=1, identity=fake_identity)
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
            self._on_engine_bound(
                fake_identity
                if self._index == 0 or replacement_identity is None
                else replacement_identity
            )
            return object()

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
            self: SQLiteStore, record: DirectEngineRecord
        ) -> bool:
            nonlocal clear_attempt_count

            if type(self) is not SQLiteStore or record != expected_record:
                raise AssertionError("worker did not compare-clear its exact direct record")
            clear_attempt_count += 1
            if clear_attempts is not None:
                clear_attempts.value += 1
            if clear_attempt_count <= clear_failures:
                return False
            return original_clear_direct_engine_record(self, record)

        worker.SQLiteStore.clear_direct_engine_record = clear_exact_direct_claim

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
            worker.SQLiteStore.clear_direct_engine_record = (
                original_clear_direct_engine_record
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
            client.sendall(chunk)
        client.shutdown(socket.SHUT_WR)
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


def test_worker_direct_activation_replaces_only_a_proven_absent_record() -> None:
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
        replacement: DirectEngineRecord | None = None
        try:
            assert ready.wait(_WATCHDOG_SECONDS), _result(results)
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
