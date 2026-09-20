"""Real worker-owned AF_UNIX health IPC coverage."""

from __future__ import annotations

import hashlib
import json
import multiprocessing
from pathlib import Path
from queue import Empty
import socket
import sqlite3
import tempfile
from typing import Protocol, cast

import pytest

from hermes_downloads import ipc, worker
from hermes_downloads.ipc import (
    MAX_MESSAGE_BYTES,
    JobsPage,
    PublicJobRecord,
    request_health,
    request_jobs_page,
    set_queue_gate,
)
from hermes_downloads.models import DownloadIntent, MaterializedJob, SourceKind
from hermes_downloads.store import SQLiteStore


_WATCHDOG_SECONDS = 5.0


class _JoinedProcess(Protocol):
    @property
    def exitcode(self) -> int | None: ...

    def is_alive(self) -> bool: ...

    def join(self, timeout: float | None = None) -> None: ...

    def terminate(self) -> None: ...


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
