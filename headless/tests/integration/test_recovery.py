"""Process-handshake coverage for bounded worker recovery."""

from __future__ import annotations

import builtins
import multiprocessing
import os
from pathlib import Path
from queue import Empty
import secrets
import sqlite3
import subprocess
import sys
import textwrap
from typing import Any, Protocol

import pytest

from hermes_downloads import worker
import hermes_downloads.store as store_module
from hermes_downloads.processes import ProcessBirthIdentity
from hermes_downloads.store import DirectEngineRecord, SQLiteStore


_WATCHDOG_SECONDS = 5.0

_COLD_WORKER_PROGRAM = textwrap.dedent(
    """
    import builtins
    import os
    from pathlib import Path
    import stat
    import subprocess
    import sys
    import threading

    state_root = Path(sys.argv[1])
    root_stat = state_root.lstat()
    assert state_root.is_absolute()
    assert stat.S_ISDIR(root_stat.st_mode)
    assert root_stat.st_uid == os.geteuid()
    assert stat.S_IMODE(root_stat.st_mode) == 0o700
    assert not {
        "hermes_downloads.worker",
        "hermes_downloads.direct",
        "hermes_downloads.video",
    } & sys.modules.keys()

    blocked_imports = []
    launches = []
    original_import = builtins.__import__
    original_popen = subprocess.Popen

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {"hermes_downloads.direct", "hermes_downloads.video"}:
            blocked_imports.append(name)
            raise AssertionError("cold recovery imported an engine module")
        return original_import(name, globals, locals, fromlist, level)

    def forbidden_popen(*args, **kwargs):
        launches.append((args, kwargs))
        raise AssertionError("cold recovery launched an engine process")

    builtins.__import__ = guarded_import
    subprocess.Popen = forbidden_popen
    try:
        from hermes_downloads import worker

        assert Path(worker.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
        shutdown = threading.Event()
        shutdown.set()
        ready = threading.Event()
        stopped = threading.Event()
        assert worker.run_worker(
            state_root,
            ready_event=ready,
            shutdown_event=shutdown,
            stopped_event=stopped,
        ) is None
        assert ready.is_set()
        assert stopped.is_set()
    finally:
        subprocess.Popen = original_popen
        builtins.__import__ = original_import

    assert not {"hermes_downloads.direct", "hermes_downloads.video"} & sys.modules.keys()
    assert blocked_imports == []
    assert launches == []
    assert sorted(path.name for path in state_root.iterdir()) == [".worker.lock", "state.db"]
    """
)


def _run_worker_process(
    state_root: str,
    ready_event: object,
    shutdown_event: object,
    stopped_event: object,
    results: object,
) -> None:
    try:
        outcome = worker.run_worker(
            Path(state_root),
            ready_event=ready_event,
            shutdown_event=shutdown_event,
            stopped_event=stopped_event,
        )
    except BaseException as error:
        results.put(("error", type(error).__name__, str(error)))
    else:
        results.put(("result", outcome))


def _result(results: object) -> tuple[object, ...]:
    try:
        return results.get(timeout=_WATCHDOG_SECONDS)
    except Empty:
        pytest.fail("worker process did not report an outcome")


class _JoinedProcess(Protocol):
    @property
    def exitcode(self) -> int | None: ...

    def is_alive(self) -> bool: ...

    def join(self, timeout: float | None = None) -> None: ...

    def terminate(self) -> None: ...


def _join(process: _JoinedProcess) -> None:
    process.join(_WATCHDOG_SECONDS)
    if process.is_alive():
        process.terminate()
        process.join(_WATCHDOG_SECONDS)
        pytest.fail("worker process did not stop after its shutdown handshake")
    assert process.exitcode == 0


def test_cold_worker_import_is_guarded_before_lazy_engine_activation(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "cold-worker-state"
    scratch = tmp_path / "cold-worker-scratch"
    for path in (state_root, scratch):
        path.mkdir(mode=0o700)
        path.chmod(0o700)

    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    environment.pop("PYTHONHOME", None)
    environment["HERMES_DOWNLOADS_DISABLE_NETWORK"] = "1"
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONNOUSERSITE"] = "1"
    assert "PYTHONPATH" not in environment
    assert "PYTHONHOME" not in environment

    try:
        completed = subprocess.run(
            [sys.executable, "-c", _COLD_WORKER_PROGRAM, str(state_root)],
            cwd=scratch,
            env=environment,
            stdin=subprocess.DEVNULL,
            text=True,
            capture_output=True,
            check=False,
            timeout=_WATCHDOG_SECONDS,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("cold guarded worker subprocess did not stop")
    assert completed.returncode == 0, (
        "cold guarded worker subprocess failed:\n"
        f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
    )


def test_worker_lifecycle_recovers_before_ready_and_stops_on_shutdown_event(
    private_roots: dict[str, Path],
) -> None:
    state_root = private_roots["state"]
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    shutdown = context.Event()
    stopped = context.Event()
    results = context.Queue()
    process = context.Process(
        target=_run_worker_process,
        args=(str(state_root), ready, shutdown, stopped, results),
    )
    process.start()

    try:
        if not ready.wait(_WATCHDOG_SECONDS):
            _join(process)
            pytest.fail(f"worker did not become ready: {_result(results)!r}")
        assert not stopped.is_set()
        assert (state_root / "state.db").is_file()

        shutdown.set()
        assert stopped.wait(_WATCHDOG_SECONDS)
        _join(process)
        assert _result(results) == ("result", None)

        store = SQLiteStore(state_root / "state.db")
        try:
            assert store.queue_gate() == "paused"
            assert store.worker_epoch() == 1
        finally:
            store.close()
    finally:
        shutdown.set()
        if process.is_alive():
            _join(process)


def test_second_worker_returns_busy_without_advancing_the_recovery_epoch(
    private_roots: dict[str, Path],
) -> None:
    state_root = private_roots["state"]
    context = multiprocessing.get_context("spawn")
    owner_ready = context.Event()
    owner_shutdown = context.Event()
    owner_stopped = context.Event()
    owner_results = context.Queue()
    owner = context.Process(
        target=_run_worker_process,
        args=(str(state_root), owner_ready, owner_shutdown, owner_stopped, owner_results),
    )
    contender_ready = context.Event()
    contender_shutdown = context.Event()
    contender_stopped = context.Event()
    contender_results = context.Queue()
    contender = context.Process(
        target=_run_worker_process,
        args=(
            str(state_root),
            contender_ready,
            contender_shutdown,
            contender_stopped,
            contender_results,
        ),
    )

    owner.start()
    try:
        assert owner_ready.wait(_WATCHDOG_SECONDS), _result(owner_results)
        contender.start()
        _join(contender)

        assert _result(contender_results) == ("result", worker.worker_busy)
        assert not owner_stopped.is_set()

        owner_shutdown.set()
        assert owner_stopped.wait(_WATCHDOG_SECONDS)
        _join(owner)
        assert _result(owner_results) == ("result", None)
    finally:
        owner_shutdown.set()
        if owner.is_alive():
            _join(owner)
        contender_shutdown.set()
        if contender.is_alive():
            _join(contender)

    store = SQLiteStore(state_root / "state.db")
    try:
        assert store.worker_epoch() == 1
    finally:
        store.close()


def test_restarting_worker_refences_an_already_paused_job(
    private_roots: dict[str, Path],
) -> None:
    from hermes_downloads.models import DownloadIntent

    state_root = private_roots["state"]
    seeded = SQLiteStore(state_root / "state.db")
    try:
        seeded.apply_add(
            DownloadIntent(
                job_id="paused-job",
                request_id="paused-request",
                payload_digest="a" * 64,
                source_url=b"https://example.test/files/paused.bin",
                generation=7,
                revision=11,
            )
        )
        seeded._connection.execute(
            "UPDATE jobs SET state = 'paused' WHERE job_id = 'paused-job'"
        )
    finally:
        seeded.close()

    context = multiprocessing.get_context("spawn")
    first_ready = context.Event()
    first_shutdown = context.Event()
    first_stopped = context.Event()
    first_results = context.Queue()
    first = context.Process(
        target=_run_worker_process,
        args=(str(state_root), first_ready, first_shutdown, first_stopped, first_results),
    )
    second_ready = context.Event()
    second_shutdown = context.Event()
    second_stopped = context.Event()
    second_results = context.Queue()
    second = context.Process(
        target=_run_worker_process,
        args=(str(state_root), second_ready, second_shutdown, second_stopped, second_results),
    )

    first.start()
    try:
        assert first_ready.wait(_WATCHDOG_SECONDS), _result(first_results)
        first_shutdown.set()
        assert first_stopped.wait(_WATCHDOG_SECONDS)
        _join(first)
        assert _result(first_results) == ("result", None)

        second.start()
        assert second_ready.wait(_WATCHDOG_SECONDS), _result(second_results)
        second_shutdown.set()
        assert second_stopped.wait(_WATCHDOG_SECONDS)
        _join(second)
        assert _result(second_results) == ("result", None)
    finally:
        first_shutdown.set()
        if first.is_alive():
            _join(first)
        second_shutdown.set()
        if second.is_alive():
            _join(second)

    recovered = SQLiteStore(state_root / "state.db")
    try:
        assert recovered.worker_epoch() == 2
        job = recovered.get_job("paused-job")
        assert job is not None
        assert (job.state, job.generation, job.revision) == ("paused", 9, 13)
        assert [
            (event.kind, event.job, event.generation, event.revision)
            for event in recovered.list_events()
        ] == [
            ("job_added", "paused-job", 7, 11),
            ("job_paused", "paused-job", 8, 12),
            ("job_paused", "paused-job", 9, 13),
        ]
    finally:
        recovered.close()


def _run_worker_process_with_engine_imports_forbidden(
    state_root: str,
    ready_event: object,
    shutdown_event: object,
    stopped_event: object,
    results: object,
) -> None:
    import builtins

    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals: object = None,
        locals: object = None,
        fromlist: object = (),
        level: int = 0,
    ) -> object:
        if name in {"hermes_downloads.direct", "hermes_downloads.video"}:
            raise AssertionError("cold recovery imported an engine module")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import
    try:
        _run_worker_process(
            state_root, ready_event, shutdown_event, stopped_event, results
        )
    finally:
        builtins.__import__ = original_import


def test_restarted_worker_does_not_import_or_start_engines(
    private_roots: dict[str, Path],
) -> None:
    state_root = private_roots["state"]
    context = multiprocessing.get_context("spawn")
    first_ready = context.Event()
    first_shutdown = context.Event()
    first_stopped = context.Event()
    first_results = context.Queue()
    first = context.Process(
        target=_run_worker_process,
        args=(str(state_root), first_ready, first_shutdown, first_stopped, first_results),
    )
    restarted_ready = context.Event()
    restarted_shutdown = context.Event()
    restarted_stopped = context.Event()
    restarted_results = context.Queue()
    restarted = context.Process(
        target=_run_worker_process_with_engine_imports_forbidden,
        args=(
            str(state_root),
            restarted_ready,
            restarted_shutdown,
            restarted_stopped,
            restarted_results,
        ),
    )

    first.start()
    try:
        assert first_ready.wait(_WATCHDOG_SECONDS), _result(first_results)
        first_shutdown.set()
        assert first_stopped.wait(_WATCHDOG_SECONDS)
        _join(first)
        assert _result(first_results) == ("result", None)

        restarted.start()
        assert restarted_ready.wait(_WATCHDOG_SECONDS), _result(restarted_results)
        restarted_shutdown.set()
        assert restarted_stopped.wait(_WATCHDOG_SECONDS)
        _join(restarted)
        assert _result(restarted_results) == ("result", None)
    finally:
        first_shutdown.set()
        if first.is_alive():
            _join(first)
        restarted_shutdown.set()
        if restarted.is_alive():
            _join(restarted)

    store = SQLiteStore(state_root / "state.db")
    try:
        assert store.worker_epoch() == 2
    finally:
        store.close()


def test_restarted_worker_preserves_a_prior_direct_record_without_importing_engines(
    private_roots: dict[str, Path],
) -> None:
    state_root = private_roots["state"]
    prior = DirectEngineRecord(
        worker_epoch=1,
        identity=ProcessBirthIdentity(
            leader_pid=999_991,
            process_group_id=999_991,
            session_id=999_991,
            owner_uid=os.geteuid(),
            started_unix_us=1,
            argv_sha256="a" * 64,
        ),
    )
    seeded = SQLiteStore(state_root / "state.db")
    try:
        assert seeded.recover_cold_start() == 1
        seeded.set_direct_engine_record(prior)
    finally:
        seeded.close()

    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    shutdown = context.Event()
    stopped = context.Event()
    results = context.Queue()
    restarted = context.Process(
        target=_run_worker_process_with_engine_imports_forbidden,
        args=(str(state_root), ready, shutdown, stopped, results),
    )
    restarted.start()
    try:
        assert ready.wait(_WATCHDOG_SECONDS), _result(results)
        shutdown.set()
        assert stopped.wait(_WATCHDOG_SECONDS)
        _join(restarted)
        assert _result(results) == ("result", None)
    finally:
        shutdown.set()
        if restarted.is_alive():
            _join(restarted)

    recovered = SQLiteStore(state_root / "state.db")
    try:
        assert recovered.worker_epoch() == 2
        assert recovered.get_direct_engine_record() == prior
    finally:
        recovered.close()


def test_restarted_worker_clears_an_absent_paired_direct_capability_without_importing_engines(
    private_roots: dict[str, Path],
) -> None:
    state_root = private_roots["state"]
    prior = DirectEngineRecord(
        worker_epoch=1,
        identity=ProcessBirthIdentity(
            leader_pid=999_991,
            process_group_id=999_991,
            session_id=999_991,
            owner_uid=os.geteuid(),
            started_unix_us=1,
            argv_sha256="a" * 64,
        ),
    )
    capability = store_module._DirectEngineRecoveryCapability(
        rpc_port=43123,
        rpc_secret=secrets.token_urlsafe(32),
    )
    seeded = SQLiteStore(state_root / "state.db")
    try:
        assert seeded.recover_cold_start() == 1
        seeded.set_direct_engine_record(prior)
        seeded._bind_direct_engine_recovery_capability(prior, capability)
    finally:
        seeded.close()

    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    shutdown = context.Event()
    stopped = context.Event()
    results = context.Queue()
    restarted = context.Process(
        target=_run_worker_process_with_engine_imports_forbidden,
        args=(str(state_root), ready, shutdown, stopped, results),
    )
    restarted.start()
    try:
        assert ready.wait(_WATCHDOG_SECONDS), _result(results)
        shutdown.set()
        assert stopped.wait(_WATCHDOG_SECONDS)
        _join(restarted)
        assert _result(results) == ("result", None)
    finally:
        shutdown.set()
        if restarted.is_alive():
            _join(restarted)

    recovered = SQLiteStore(state_root / "state.db")
    try:
        assert recovered.worker_epoch() == 2
        assert recovered.get_direct_engine_record() is None
        assert recovered._get_direct_engine_recovery_capability(prior) is None
    finally:
        recovered.close()


@pytest.mark.parametrize(
    "mode",
    ("missing", "raw_blob", "mismatch", "indeterminate", "unbound_fence"),
)
def test_cold_direct_recovery_blocks_unsafe_claims_without_importing_an_engine(
    private_roots: dict[str, Path],
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Unsafe durable claims stay intact and cannot trigger a replacement engine."""

    state_root = private_roots["state"]
    store = SQLiteStore(state_root / "state.db")
    record = DirectEngineRecord(
        worker_epoch=1,
        identity=ProcessBirthIdentity(
            leader_pid=999_991,
            process_group_id=999_991,
            session_id=999_991,
            owner_uid=os.geteuid(),
            started_unix_us=1,
            argv_sha256="a" * 64,
        ),
    )
    capability = store_module._DirectEngineRecoveryCapability(
        rpc_port=43123,
        rpc_secret=secrets.token_urlsafe(32),
    )
    try:
        assert store.recover_cold_start() == 1
        if mode == "unbound_fence":
            fence = store.reserve_direct_engine_activation(worker_epoch=1)
            assert fence is not None
        else:
            fence = None
            store.set_direct_engine_record(record)
            if mode != "missing":
                store._bind_direct_engine_recovery_capability(record, capability)
            if mode == "raw_blob":
                store._connection.execute("PRAGMA ignore_check_constraints = ON")
                try:
                    store._connection.execute(
                        "UPDATE direct_engine_recovery_capabilities SET rpc_secret = ?",
                        (sqlite3.Binary(b"not-a-text-secret"),),
                    )
                finally:
                    store._connection.execute("PRAGMA ignore_check_constraints = OFF")
            elif mode == "mismatch":
                store._connection.execute(
                    """
                    UPDATE direct_engine_recovery_capabilities
                    SET leader_pid = ?, process_group_id = ?, session_id = ?
                    """,
                    (999_992, 999_992, 999_992),
                )
        assert store.recover_cold_start() == 2

        original_import = builtins.__import__

        def guarded_import(
            name: str,
            globals: Any = None,
            locals: Any = None,
            fromlist: Any = (),
            level: int = 0,
        ) -> object:
            if name == "hermes_downloads.direct":
                pytest.fail("unsafe cold recovery imported the direct engine")
            return original_import(name, globals, locals, fromlist, level)

        monkeypatch.setattr(builtins, "__import__", guarded_import)
        if mode == "indeterminate":
            monkeypatch.setattr(
                worker,
                "reconcile_process_birth",
                lambda _identity: "indeterminate",
            )
        assert worker._recover_cold_direct_engine(store) is True
        assert store.worker_epoch() == 2
        if fence is not None:
            assert store.get_direct_engine_activation_fence() == fence
            assert store.get_direct_engine_record() is None
        else:
            assert store.get_direct_engine_record() == record
            if mode in {"raw_blob", "mismatch"}:
                with pytest.raises(ValueError, match="recovery capability"):
                    store._get_direct_engine_recovery_capability(record)
            elif mode == "missing":
                assert store._get_direct_engine_recovery_capability(record) is None
            else:
                assert store._get_direct_engine_recovery_capability(record) == capability
    finally:
        store.close()


def test_cold_direct_recovery_retains_current_paired_claim_after_shutdown_failure(
    private_roots: dict[str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An authenticated shutdown failure cannot clear or replace a current owner."""

    from hermes_downloads import direct

    state_root = private_roots["state"]
    store = SQLiteStore(state_root / "state.db")
    record = DirectEngineRecord(
        worker_epoch=1,
        identity=ProcessBirthIdentity(
            leader_pid=999_991,
            process_group_id=999_991,
            session_id=999_991,
            owner_uid=os.geteuid(),
            started_unix_us=1,
            argv_sha256="a" * 64,
        ),
    )
    capability = store_module._DirectEngineRecoveryCapability(
        rpc_port=43123,
        rpc_secret=secrets.token_urlsafe(32),
    )
    calls: list[tuple[object, object]] = []
    try:
        assert store.recover_cold_start() == 1
        store.set_direct_engine_record(record)
        store._bind_direct_engine_recovery_capability(record, capability)
        assert store.recover_cold_start() == 2

        def fail_shutdown(identity: object, recovered_capability: object) -> None:
            calls.append((identity, recovered_capability))
            raise direct.DirectEngineError("aria2 recovery failed")

        monkeypatch.setattr(worker, "reconcile_process_birth", lambda _identity: "current")
        monkeypatch.setattr(direct, "_recover_owned_direct_engine", fail_shutdown)
        assert worker._recover_cold_direct_engine(store) is True
        assert calls == [(record.identity, capability)]
        assert store.get_direct_engine_record() == record
        assert store._get_direct_engine_recovery_capability(record) == capability
    finally:
        store.close()


def test_restarted_worker_preserves_an_unbound_direct_activation_fence_without_importing_engines(
    private_roots: dict[str, Path],
) -> None:
    state_root = private_roots["state"]
    seeded = SQLiteStore(state_root / "state.db")
    try:
        assert seeded.recover_cold_start() == 1
        fence = seeded.reserve_direct_engine_activation(worker_epoch=1)
        assert fence is not None
        assert seeded.get_direct_engine_record() is None
        assert seeded.get_direct_engine_activation_fence() == fence
    finally:
        seeded.close()

    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    shutdown = context.Event()
    stopped = context.Event()
    results = context.Queue()
    restarted = context.Process(
        target=_run_worker_process_with_engine_imports_forbidden,
        args=(str(state_root), ready, shutdown, stopped, results),
    )
    restarted.start()
    try:
        assert ready.wait(_WATCHDOG_SECONDS), _result(results)
        shutdown.set()
        assert stopped.wait(_WATCHDOG_SECONDS)
        _join(restarted)
        assert _result(results) == ("result", None)
    finally:
        shutdown.set()
        if restarted.is_alive():
            _join(restarted)

    recovered = SQLiteStore(state_root / "state.db")
    try:
        assert recovered.worker_epoch() == 2
        assert recovered.get_direct_engine_record() is None
        assert recovered.get_direct_engine_activation_fence() == fence
    finally:
        recovered.close()


def _exit_unrelated_client() -> None:
    return None


def test_unrelated_client_exit_leaves_the_worker_alive(
    private_roots: dict[str, Path],
) -> None:
    state_root = private_roots["state"]
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    shutdown = context.Event()
    stopped = context.Event()
    results = context.Queue()
    worker_process = context.Process(
        target=_run_worker_process,
        args=(str(state_root), ready, shutdown, stopped, results),
    )
    client_process = context.Process(target=_exit_unrelated_client)

    worker_process.start()
    try:
        assert ready.wait(_WATCHDOG_SECONDS), _result(results)

        client_process.start()
        _join(client_process)

        assert worker_process.is_alive()
        assert not stopped.is_set()

        shutdown.set()
        assert stopped.wait(_WATCHDOG_SECONDS)
        _join(worker_process)
        assert _result(results) == ("result", None)
    finally:
        shutdown.set()
        if worker_process.is_alive():
            _join(worker_process)
        if client_process.is_alive():
            _join(client_process)

