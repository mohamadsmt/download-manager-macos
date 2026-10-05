"""Worker bootstrap persistence tests."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
import stat
import tempfile
import threading

import pytest

from hermes_downloads import direct, ipc, paths, worker
from hermes_downloads.models import DownloadIntent, MaterializedJob, SourceKind
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


@pytest.fixture
def short_state_root() -> Iterator[Path]:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="hd-t15g-") as temporary_root:
        state_root = Path(temporary_root) / "state"
        state_root.mkdir(mode=0o700)
        yield state_root


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


def test_run_worker_rejects_existing_ipc_socket_path_before_bootstrap(
    private_roots: dict[str, Path],
) -> None:
    state_root = private_roots["state"]
    socket_path = state_root / "worker.sock"
    socket_path.write_text("unsafe pre-existing object", encoding="utf-8")
    ready = threading.Event()
    shutdown = threading.Event()
    stopped = threading.Event()

    with pytest.raises(worker.WorkerStateError) as failure:
        worker.run_worker(
            state_root,
            socket_path=socket_path,
            ready_event=ready,
            shutdown_event=shutdown,
            stopped_event=stopped,
        )

    assert str(failure.value) == "worker_state_invalid"
    assert not ready.is_set()
    assert stopped.is_set()
    assert list(state_root.iterdir()) == [socket_path]


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


def _seed_published_finalization(
    state_root: Path,
    *,
    final_bound: bool,
    mismatched_final: bool,
    missing_final: bool,
) -> tuple[paths.DestinationIntent, str]:
    intent = DownloadIntent(
        job_id="reconcile-job",
        request_id="reconcile-add",
        payload_digest="a" * 64,
        source_url=b"https://example.test/reconciliation-must-not-transfer",
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
        partial_filename="payload.bin",
        selected_final_filename="payload.bin",
    )
    store = SQLiteStore(state_root / "state.db")
    try:
        assert store.apply_add(intent, materialized=materialized).applied
        reservation = store.get_publication_reservation(intent.job_id)
        assert reservation is not None
        root = Path.home() / "Downloads" / "Hermes"
        root.mkdir(parents=True, mode=0o700)
        root.chmod(0o700)
        destination = paths.rehydrate_destination(
            category=materialized.category,
            collection=materialized.destination_collection,
            partial_filename=materialized.partial_filename,
            selected_final_filename=materialized.selected_final_filename,
            job_id=intent.job_id,
        )
        paths.prepare_persisted_destination_workspace(destination)
        marker = paths.attest_publication_reservation_marker(destination, reservation)
        store.bind_publication_marker(
            intent.job_id,
            claim_token=reservation.claim_token,
            marker_device=marker.st_dev,
            marker_inode=marker.st_ino,
        )
        destination.partial_path.write_bytes(b"durably published payload")
        staged = paths.attest_staged_partial_payload(destination, reservation)
        store._bind_staged_payload(
            intent.job_id,
            claim_token=reservation.claim_token,
            partial_device=staged.st_dev,
            partial_inode=staged.st_ino,
            logical_size=staged.logical_size,
        )
        published = paths.publish_staged_partial_payload(destination, reservation, staged)
        if final_bound:
            store._bind_final_publication(
                intent.job_id,
                claim_token=reservation.claim_token,
                final_device=published.st_dev,
                final_inode=published.st_ino,
                logical_size=published.logical_size,
            )
        if mismatched_final:
            destination.final_path.unlink()
            destination.final_path.write_bytes(b"different payload")
        if missing_final:
            destination.final_path.unlink()
        store._connection.execute(
            "UPDATE jobs SET revision = 1, state = 'finalizing' WHERE job_id = ?",
            (intent.job_id,),
        )
        store._connection.execute(
            """
            INSERT INTO events (kind, job_id, generation, revision)
            VALUES ('job_finalizing', ?, 0, 1)
            """,
            (intent.job_id,),
        )
        return destination, reservation.claim_token
    finally:
        store.close()


@pytest.mark.parametrize(
    ("final_bound", "mismatched_final", "missing_final"),
    (
        (False, False, False),
        (True, False, False),
        (False, True, False),
        (True, True, False),
        (False, False, True),
        (True, False, True),
    ),
    ids=(
        "pre-bind",
        "final-bound",
        "mismatch",
        "final-bound-mismatch",
        "missing-final",
        "final-bound-missing-final",
    ),
)
@pytest.mark.parametrize("late_change", (None, "remove", "replace"))
def test_explicit_dispatch_reconciles_a_published_final_without_transfer(
    short_state_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    final_bound: bool,
    mismatched_final: bool,
    missing_final: bool,
    late_change: str | None,
) -> None:
    state_root = short_state_root
    destination, claim_token = _seed_published_finalization(
        state_root,
        final_bound=final_bound,
        mismatched_final=mismatched_final,
        missing_final=missing_final,
    )
    seam_calls: list[str] = []
    original_publish = paths.publish_staged_partial_payload
    retained_entries = {p.name: p.read_bytes() for p in destination.incomplete_dir.iterdir() if p.is_file()}

    def change_final_then_publish(*args: object, **kwargs: object) -> object:
        seam_calls.append("called")
        destination.final_path.unlink()
        if late_change == "replace":
            destination.final_path.write_bytes(b"different payload")
        return original_publish(*args, **kwargs)

    if late_change is not None:
        monkeypatch.setattr(paths, "publish_staged_partial_payload", change_final_then_publish)
    transfer_calls: list[str] = []
    results: list[ipc.DirectJobDispatchResult] = []
    shutdown = threading.Event()
    stopped = threading.Event()
    errors: list[BaseException] = []

    def forbidden_transfer(*_args: object, **_kwargs: object) -> object:
        transfer_calls.append("attempted")
        raise AssertionError("published-final reconciliation must not transfer")

    monkeypatch.setattr(worker, "validate_source_url", forbidden_transfer)
    monkeypatch.setattr(direct.DirectAria2Controller, "add_paused", forbidden_transfer)
    monkeypatch.setattr(direct.DirectAria2Controller, "resume", forbidden_transfer)

    class DispatchServer:
        def __init__(self, _path: Path, **handlers: object) -> None:
            self._queue = handlers["queue_gate"]
            self._job = handlers["job_control"]
            self._dispatch = handlers["direct_job_dispatch"]
            self._served = False

        def serve_once(self) -> None:
            if self._served:
                return
            self._served = True
            startup = SQLiteStore(state_root / "state.db")
            try:
                job = startup.get_job("reconcile-job")
                assert job is not None
                assert (job.state, job.generation, job.revision) == ("paused", 1, 2)
                assert startup.queue_gate() == "paused"
                assert "job_completed" not in [e.kind for e in startup.list_events()]
                assert destination.final_path.exists() is (not missing_final)
                assert transfer_calls == []
            finally:
                startup.close()
            command = ipc.DirectJobDispatchCommand(
                job="reconcile-job",
                expected_worker_epoch=1,
                expected_generation=1,
                expected_revision=2,
                request_id="reconcile-dispatch",
            )
            first = self._dispatch(command)
            replay = self._dispatch(command)
            assert isinstance(first, ipc.DirectJobDispatchResult)
            assert isinstance(replay, ipc.DirectJobDispatchResult)
            results.extend((first, replay))
            shutdown.set()

        def close(self) -> None:
            return None

    monkeypatch.setattr(worker, "HealthServer", DispatchServer)

    def run() -> None:
        try:
            worker.run_worker(
                state_root,
                socket_path=state_root / "worker.sock",
                ready_event=threading.Event(),
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
    assert errors == []
    assert transfer_calls == []
    store = SQLiteStore(state_root / "state.db")
    try:
        job = store.get_job("reconcile-job")
        assert job is not None
        if mismatched_final or missing_final or late_change is not None:
            expected = ipc.DirectJobDispatchResult("blocked", "reconcile-job", 1, 2, "paused")
            assert results == [expected, expected]
            assert job.state == "paused"
            if final_bound:
                assert store._get_final_publication_binding("reconcile-job") is not None
            else:
                assert store._get_final_publication_binding("reconcile-job") is None
            if mismatched_final or (late_change == "replace" and not missing_final):
                assert destination.final_path.read_bytes() == b"different payload"
            else:
                assert not destination.final_path.exists()
        else:
            expected = ipc.DirectJobDispatchResult("started", "reconcile-job", 1, 3, "completed")
            assert results == [expected, expected]
            assert job.state == "completed"
            final = os.lstat(destination.final_path)
            partial = os.lstat(destination.partial_path)
            assert (final.st_dev, final.st_ino, final.st_size) == (
                partial.st_dev,
                partial.st_ino,
                partial.st_size,
            )
            assert store._get_final_publication_binding("reconcile-job") is not None
            assert [event.kind for event in store.list_events()].count("job_completed") == 1
        if late_change is not None and not mismatched_final and not missing_final:
            assert seam_calls == ["called"]
            assert {p.name: p.read_bytes() for p in destination.incomplete_dir.iterdir() if p.is_file()} == retained_entries
            assert "job_completed" not in [e.kind for e in store.list_events()]
        assert claim_token not in repr(results)
        assert claim_token not in repr(store.list_job_page())
        assert claim_token not in repr(store.list_events())
    finally:
        store.close()



def test_worker_without_dispatch_never_observes_or_verifies_terminal(private_roots, monkeypatch):
    calls = []
    def forbidden(*args, **kwargs):
        calls.append("called")
        raise AssertionError("bootstrap cannot observe a transfer")
    assert hasattr(direct.DirectAria2Controller, "observe_terminal"), "missing one-shot API"
    monkeypatch.setattr(direct.DirectAria2Controller, "observe_terminal", forbidden)
    monkeypatch.setattr(paths, "attest_staged_partial_payload", forbidden)
    monkeypatch.setattr(direct.DirectAria2Controller, "_verify_completed_output", forbidden)
    assert worker.main() == 0
    assert calls == []



@pytest.mark.parametrize("operation", ("terminal", "stage", "prepare", "post-link", "publication-exit"))
def test_held_observation_cannot_overlap_controller_after_same_process_worker_restart(short_state_root, monkeypatch, operation):
    from types import SimpleNamespace
    from hermes_downloads.processes import ProcessBirthIdentity
    from hermes_downloads.store import _DirectEngineRecoveryCapability
    state_root = short_state_root
    intent = DownloadIntent(job_id="observer-job", request_id="observer-add", payload_digest="a" * 64,
        source_url=b"https://downloads.example.test/observer.bin", generation=0, revision=0)
    job = MaterializedJob(job_id=intent.job_id, intent=intent, source_kind=SourceKind.DIRECT,
        queue_collection_id=None, priority=0, order_key=0, scheduled_for=None, authorized=False,
        manual_hold=False, start_now_requested=False, category="Other", destination_collection=None,
        partial_filename="observer.bin", selected_final_filename="observer.bin")
    seed = SQLiteStore(state_root / "state.db")
    seed.apply_add(intent, materialized=job)
    seed.close()
    root = Path.home() / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    entered, release, returned = threading.Event(), threading.Event(), threading.Event()
    ready, shutdown, stopped = threading.Event(), threading.Event(), threading.Event()
    errors = []
    constructed = []
    identity = ProcessBirthIdentity(leader_pid=4321, process_group_id=4321, session_id=4321,
        owner_uid=os.geteuid(), started_unix_us=123456789, argv_sha256="a" * 64)
    class Controller:
        def __init__(self, **kwargs):
            constructed.append(self)
            self.bound = kwargs["on_engine_bound"]
        def start(self):
            self.bound(identity)
        def _recovery_capability(self):
            return _DirectEngineRecoveryCapability(rpc_port=43123, rpc_secret="a" * 43)
        def add_paused(self, **kwargs):
            self.destination = kwargs["destination"]
            if operation != "terminal":
                self.destination.partial_path.write_bytes(b"body")
            return SimpleNamespace(status="paused", gid="0123456789abcdef")
        def resume(self, **kwargs):
            return None
        def observe_terminal(self, **kwargs):
            if operation != "terminal":
                details = self.destination.partial_path.stat()
                return direct.DirectTransfer(job_id="observer-job", generation=1,
                    gid="0123456789abcdef", status="complete", total_length=4,
                    completed_length=4, partial_path=self.destination.partial_path,
                    hash_verified=False, verification=direct.CompletionVerification.TRANSPORT_VERIFIED,
                    verified_identity=direct._VerifiedPayloadIdentity(
                        details.st_dev, details.st_ino, 4, details.st_mtime_ns,
                        details.st_mode, details.st_nlink, details.st_ctime_ns))
            entered.set()
            assert release.wait(10)
            returned.set()
            return None
        def close(self):
            return None
    if operation == "stage":
        original_sync = paths._fsync_staged_partial_payload
        def held_sync(fd):
            entered.set()
            assert release.wait(10)
            original_sync(fd)
            returned.set()
        monkeypatch.setattr(paths, "_fsync_staged_partial_payload", held_sync)
    elif operation in {'prepare', 'post-link'}:
        name = '_hash_publication_payload' if operation == 'prepare' else '_fsync_published_final_directory'
        original = getattr(paths, name)
        def held_publication(*args, **kwargs):
            entered.set()
            assert release.wait(10)
            try:
                return original(*args, **kwargs)
            finally:
                returned.set()
        monkeypatch.setattr(paths, name, held_publication)
    elif operation == 'publication-exit':
        original_thread = threading.Thread
        class HeldExitThread(original_thread):
            def run(self):
                super().run()
                if self.name == 'direct-publication-observation':
                    entered.set()
                    assert release.wait(10)
                    returned.set()
        monkeypatch.setattr(worker.threading, 'Thread', HeldExitThread)
    monkeypatch.setattr(direct, "DirectAria2Controller", Controller)
    monkeypatch.setattr("hermes_downloads.processes.reconcile_process_birth", lambda _: "current")
    monkeypatch.setattr(worker, "reconcile_process_birth", lambda _: "current")
    phase = [1]
    responses = []
    class Server:
        def __init__(self, path, **handlers):
            self.handlers = handlers
            self.served = False
        def serve_once(self):
            if self.served:
                return
            self.served = True
            if phase[0] == 1:
                self.handlers["queue_gate"](ipc.QueueGateCommand(gate="running", request_id="observer-open", expected_revision=1))
                self.handlers["job_control"](ipc.JobControlCommand(job="observer-job", action="start_now", request_id="observer-start", expected_revision=1))
                assert self.handlers["direct_engine_activate"](ipc.DirectEngineActivateCommand(expected_worker_epoch=1)).status == "active"
                assert self.handlers["direct_job_dispatch"](ipc.DirectJobDispatchCommand(job="observer-job", expected_worker_epoch=1, expected_generation=1, expected_revision=2, request_id="observer-dispatch")).status == "started"
            else:
                responses.append(self.handlers["direct_engine_activate"](ipc.DirectEngineActivateCommand(expected_worker_epoch=2)))
                shutdown.set()
        def close(self):
            return None
    monkeypatch.setattr(worker, "HealthServer", Server)
    def run():
        try:
            worker.run_worker(state_root, socket_path=state_root / "worker.sock", ready_event=ready,
                shutdown_event=shutdown, stopped_event=stopped)
        except BaseException as error:
            errors.append(error)
    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert entered.wait(_WATCHDOG_SECONDS)
        shutdown.set()
        assert stopped.wait(_WATCHDOG_SECONDS)
        thread.join(_WATCHDOG_SECONDS)
        assert errors == []
        assert not returned.is_set()
        phase[0] = 2
        shutdown.clear()
        stopped.clear()
        run()
        assert responses == [ipc.DirectEngineActivateResult(worker_epoch=2, status="blocked")]
        assert len(constructed) == 1
    finally:
        release.set()
        shutdown.set()
        thread.join(_WATCHDOG_SECONDS)
        assert returned.wait(_WATCHDOG_SECONDS)
def test_exact_publication_pending_is_redacted_and_bounded():
    from hermes_downloads.ipc import DirectJobDispatchResult
    result = DirectJobDispatchResult(status='pending', job='job-1', generation=1,
        revision=5, state='finalizing')
    assert DirectJobDispatchResult.from_record(result.to_record()) == result


def test_certified_serving_reads_complete_schema16_without_history_effects(short_state_root):
    import json
    import sqlite3
    from hermes_downloads import endpoint_ownership as ownership
    root = Path('/private') / short_state_root.relative_to('/') if str(short_state_root).startswith('/tmp/') else short_state_root
    ready, shutdown, stopped = threading.Event(), threading.Event(), threading.Event()
    errors = []
    def run():
        try:
            worker.run_worker(root, socket_path=root / 'worker.sock', recover_socket=True,
                ready_event=ready, shutdown_event=shutdown, stopped_event=stopped)
        except BaseException as error:
            errors.append(error)
    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert ready.wait(_WATCHDOG_SECONDS)
        record = (root / ownership.RECORD).read_bytes()
        assert json.loads(record)['schema'] == 1
        with sqlite3.connect((root / 'state.db').as_uri() + '?mode=ro', uri=True) as connection:
            before = tuple(connection.iterdump())
            assert connection.execute('PRAGMA user_version').fetchone()[0] == 16
            assert connection.execute('SELECT COUNT(*) FROM direct_publication_attempts').fetchone()[0] == 0
            assert connection.execute('SELECT COUNT(*) FROM closed_direct_publication_attempts').fetchone()[0] == 0
        certificate = ownership.preflight(root)
        assert certificate.record[1] == record
        assert json.loads(record)['worker_epoch'] == ipc.request_health(root / 'worker.sock').worker_epoch
        with sqlite3.connect((root / 'state.db').as_uri() + '?mode=ro', uri=True) as connection:
            assert tuple(connection.iterdump()) == before
        assert (root / ownership.RECORD).read_bytes() == record
    finally:
        shutdown.set()
        thread.join(_WATCHDOG_SECONDS)
        assert stopped.is_set() and not thread.is_alive() and errors == []
    assert not (root / 'worker.sock').exists() and not (root / ownership.RECORD).exists()


@pytest.mark.parametrize('fault', ('incomplete', 'malformed', 'newer', 'missing-archive', 'malformed-archive'))
def test_schema16_certificate_refuses_before_lease_bootstrap_and_reclaim(short_state_root, monkeypatch, fault):
    import sqlite3
    from hermes_downloads import endpoint_ownership as ownership
    root = Path('/private') / short_state_root.relative_to('/') if str(short_state_root).startswith('/tmp/') else short_state_root
    certificate = ownership.preflight(root)
    store = SQLiteStore(root / 'state.db')
    store.recover_cold_start()
    epoch = store.worker_epoch()
    store.close()
    server = ipc.HealthServer(root / 'worker.sock', health=lambda: ipc.WorkerHealth(epoch, 'paused'))
    owned = ownership.publish(root, certificate, epoch, server._identity)
    try:
        readback = ownership.preflight(root)
        assert (readback.root_identity, readback.record, readback.socket) == (
            owned.root_identity, owned.record, owned.socket)
        database_details = (root / 'state.db').stat()
        assert readback.database_identity == (database_details.st_dev, database_details.st_ino)
        with sqlite3.connect(root / 'state.db') as connection:
            if fault == 'newer':
                connection.execute('PRAGMA user_version=17')
            elif fault.endswith('archive'):
                connection.execute('DROP TABLE closed_direct_publication_attempts')
                if fault == 'malformed-archive':
                    connection.execute('CREATE TABLE closed_direct_publication_attempts (job_id TEXT)')
            else:
                connection.execute('DROP TABLE direct_publication_attempts')
                if fault == 'malformed':
                    connection.execute('CREATE TABLE direct_publication_attempts (job_id TEXT)')
        before = {p.name: (p.lstat().st_dev, p.lstat().st_ino, p.lstat().st_mode,
            p.read_bytes() if p.is_file() else None) for p in root.iterdir()}
        def forbidden(*args, **kwargs):
            pytest.fail('uncertified catalogue reached writable/lease/reclaim effects')
        monkeypatch.setattr(worker, '_acquire_worker_lease', forbidden)
        monkeypatch.setattr(worker, 'SQLiteStore', forbidden)
        monkeypatch.setattr(ownership, 'reclaim', forbidden)
        stopped = threading.Event()
        expected_error = 'newer than supported' if fault == 'newer' else 'incomplete'
        with pytest.raises(RuntimeError, match='database schema version is ' + expected_error):
            worker.run_worker(root, socket_path=root / 'worker.sock', recover_socket=True,
                ready_event=threading.Event(), shutdown_event=threading.Event(), stopped_event=stopped)
        assert stopped.is_set()
        assert {p.name: (p.lstat().st_dev, p.lstat().st_ino, p.lstat().st_mode,
            p.read_bytes() if p.is_file() else None) for p in root.iterdir()} == before
        assert not (root / '.worker.lock').exists()
    finally:
        server.close()
        ownership.clear(root, owned)
    assert not (root / ownership.RECORD).exists()
