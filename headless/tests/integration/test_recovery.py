"""Process-handshake coverage for bounded worker recovery."""

from __future__ import annotations

import multiprocessing
from pathlib import Path
from queue import Empty

import pytest

from hermes_downloads import worker
from hermes_downloads.store import SQLiteStore


_WATCHDOG_SECONDS = 5.0


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


def _join(process: multiprocessing.Process) -> None:
    process.join(_WATCHDOG_SECONDS)
    if process.is_alive():
        process.terminate()
        process.join(_WATCHDOG_SECONDS)
        pytest.fail("worker process did not stop after its shutdown handshake")
    assert process.exitcode == 0


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

