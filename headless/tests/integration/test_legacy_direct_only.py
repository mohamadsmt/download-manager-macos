"""Direct-only retirement: synthetic legacy state stays readable and inert."""
from __future__ import annotations

import builtins
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime
import hashlib
import importlib.metadata
import importlib.util
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import sys
import threading

import pytest

from hermes_downloads.models import DownloadIntent, MaterializedJob, SourceKind
from hermes_downloads.retry import RetryAuthority, RetryPolicy
from hermes_downloads.store import DirectDispatchResult, SQLiteStore

NOW = datetime(2031, 1, 1, tzinfo=UTC)
_SPEC = importlib.util.spec_from_file_location(
    "_direct_only_origin", Path(__file__).resolve().parents[1] / "fixtures/http_origin.py"
)
assert _SPEC is not None and _SPEC.loader is not None
_ORIGIN = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _ORIGIN
_SPEC.loader.exec_module(_ORIGIN)


def _job(job_id="legacy", source=b"https://example.test/synthetic.mp4?token=fixture"):
    intent = DownloadIntent(job_id, f"add-{job_id}", "a" * 64, source, generation=4, revision=7)
    return MaterializedJob(job_id, intent, SourceKind.DIRECT, "collection", 9, 17,
                           datetime(2032, 1, 1, tzinfo=UTC), True, False, True, "Videos", None,
                           f"{job_id}.mp4", f"{job_id}.mp4")


def _snapshot(store, job_id="legacy"):
    """Capture every target table and the associated immutable receipt registry."""
    connection = store._connection
    result = {}
    requests = []
    for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        name = row[0]
        columns = {column[1] for column in connection.execute(f'PRAGMA table_info("{name}")')}
        if "job_id" in columns:
            rows = connection.execute(f'SELECT * FROM "{name}" WHERE job_id=? ORDER BY rowid', (job_id,)).fetchall()
            result[name] = tuple(tuple(item) for item in rows)
            if "request_id" in columns:
                requests.extend(item["request_id"] for item in rows)
    result["command_receipts"] = tuple(
        tuple(connection.execute("SELECT * FROM command_receipts WHERE request_id=?", (request,)).fetchone() or ())
        for request in sorted(requests)
    )
    return result


def _seed(store, *, state="queued", bindings=3, dispatch=False):
    """Create a private direct fixture, then restore literal historical video data."""
    job = _job()
    store.apply_add(job.intent, materialized=job)
    store.initialize_cold_start()
    store.apply_job_control(job_id=job.job_id, action="resume", request_id="historical-control",
                            payload_digest="b" * 64, expected_revision=7)
    store.set_retry_budget(RetryAuthority.open(policy=RetryPolicy(), job_id=job.job_id, generation=4).budget)
    reservation = store.get_publication_reservation(job.job_id)
    if bindings >= 1:
        store.bind_publication_marker(job.job_id, claim_token=reservation.claim_token, marker_device=11, marker_inode=12)
    if bindings >= 2:
        store._bind_staged_payload(job.job_id, claim_token=reservation.claim_token, partial_device=13, partial_inode=14, logical_size=15)
    if bindings >= 3:
        store._bind_final_publication(job.job_id, claim_token=reservation.claim_token, final_device=13, final_inode=14, logical_size=15)
    connection = store._connection
    if dispatch:
        connection.execute("INSERT INTO direct_dispatch_commands (request_id,payload_digest,job_id,status,generation,revision,state) VALUES (?, ?, ?, 'pending', 4, 7, ?)",
                           ("historical-dispatch", "c" * 64, job.job_id, state))
    if state == "finalizing":
        connection.execute("INSERT INTO events (kind, job_id, generation, revision) VALUES ('job_finalizing', ?, 4, 7)", (job.job_id,))
    connection.execute("UPDATE jobs SET state=? WHERE job_id=?", (state, job.job_id))
    connection.execute("UPDATE materialized_jobs SET source_kind='video' WHERE job_id=?", (job.job_id,))
    connection.commit()
    return job, reservation


def _guards(monkeypatch):
    original = builtins.__import__
    def guarded(name, *args, **kwargs):
        if name in {"hermes_downloads.video", "hermes_downloads.direct", "yt_dlp"}:
            pytest.fail(f"legacy work imported an engine: {name}")
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", guarded)
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: pytest.fail("legacy work launched a process"))
    monkeypatch.setattr(os, "fsync", lambda *a: pytest.fail("legacy work fsynced payloads"))
    monkeypatch.setattr(os, "link", lambda *a, **k: pytest.fail("legacy work published payloads"))


def test_installed_product_has_no_web_video_capability():
    assert importlib.util.find_spec("hermes_downloads.video") is None
    requirements = importlib.metadata.requires("hermes-downloads")
    assert all(not item.lower().startswith("yt-dlp") for item in requirements)
    assert SourceKind("video").name == "LEGACY_VIDEO"
    assert not hasattr(SourceKind, "VIDEO")


@pytest.mark.parametrize("state", ["queued", "resolving", "downloading", "pausing", "retry_wait", "finalizing", "paused", "completed", "failed"])
def test_mixed_cold_restart_read_and_resume_preserve_complete_legacy_state(private_roots, monkeypatch, state):
    database = private_roots["state"] / "state.db"
    with closing(SQLiteStore(database)) as store:
        _seed(store, state=state, dispatch=True)
        direct = _job("direct")
        store.apply_add(direct.intent, materialized=direct)
        before = _snapshot(store)
    owned = private_roots["output"] / "legacy-owned"
    owned.mkdir()
    files = [owned / name for name in ("partial.mp4", "marker", "final.mp4")]
    for file in files:
        file.write_bytes(b"unchanged synthetic historical bytes")
    file_snapshot = [(p.read_bytes(), p.stat()) for p in files]
    _guards(monkeypatch)
    from hermes_downloads import worker
    for iteration in range(2):
        ready, shutdown, stopped = (threading.Event() for _ in range(3))
        shutdown.set()
        assert worker.run_worker(private_roots["state"], ready_event=ready,
                                 shutdown_event=shutdown, stopped_event=stopped) is None
        assert ready.is_set() and stopped.is_set()
        with closing(SQLiteStore(database)) as store:
            assert _snapshot(store) == before
            assert store.get_materialized_job("legacy").source_kind.value == "video"
            assert store.get_job("legacy").state == state
            assert {job.job for job in store.list_job_page()} == {"direct", "legacy"}
            assert store.list_jobs() and store.list_events()
            assert store.get_command("add-legacy") is not None
            assert store.get_retry_budget("legacy").generation == 4
            assert store.get_job("direct").state == "paused"
            assert store.get_job("direct").generation == 5 + iteration
            gate, revision = store.queue_gate_snapshot()
            assert gate == "paused"
            store.apply_queue_gate(gate="running", request_id=f"resume-all-{iteration}",
                                   payload_digest="d" * 64, expected_revision=revision)
            assert _snapshot(store) == before
    assert [(p.read_bytes(), p.stat()) for p in files] == file_snapshot


@pytest.mark.parametrize("action", ["pause", "resume", "start_now", "remove"])
@pytest.mark.parametrize("stale", [False, True])
def test_legacy_controls_are_blocked_without_fresh_receipts(tmp_path, monkeypatch, action, stale):
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        _seed(store)
        before = _snapshot(store)
        _guards(monkeypatch)
        result = store.apply_job_control(job_id="legacy", action=action, request_id="new-control",
                                         payload_digest="e" * 64, expected_revision=6 if stale else 7,
                                         _contained_direct_transfer=True)
        assert result.status == "blocked"
        assert (result.generation, result.revision, result.state, result.authorized) == (4, 7, "queued", True)
        assert _snapshot(store) == before
        assert store._connection.execute("SELECT 1 FROM command_receipts WHERE request_id='new-control'").fetchone() is None


@pytest.mark.parametrize("dispatch_request", ["new-dispatch", "historical-dispatch"])
@pytest.mark.parametrize("state", ["queued", "finalizing", "paused", "downloading"])
def test_legacy_dispatch_never_writes_or_reconciles(tmp_path, monkeypatch, dispatch_request, state):
    with _ORIGIN.SyntheticHttpOrigin() as origin, closing(SQLiteStore(tmp_path / "state.db")) as store:
        _seed(store, state=state, dispatch=True)
        store.recover_cold_start()
        gate, revision = store.queue_gate_snapshot()
        store.apply_queue_gate(gate="running", request_id="global-resume", payload_digest="d" * 64, expected_revision=revision)
        before = _snapshot(store)
        _guards(monkeypatch)
        for expected_generation in (3, 4):
            result = store.prepare_direct_dispatch(job_id="legacy", expected_worker_epoch=store.worker_epoch(),
                                                  expected_generation=expected_generation, expected_revision=7,
                                                  request_id=dispatch_request, payload_digest="c" * 64,
                                                  controller_ready=True, now=NOW)
            assert type(result) is DirectDispatchResult and result.status == "blocked"
            assert _snapshot(store) == before
        assert origin.ledger.response_body_bytes == 0


def test_new_legacy_add_rejects_before_any_persistence(tmp_path):
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        job = replace(_job(), source_kind=SourceKind("video"))
        before = tuple(store._connection.iterdump())
        with pytest.raises(ValueError, match="unsupported source kind"):
            store.apply_add(job.intent, materialized=job)
        assert tuple(store._connection.iterdump()) == before


def test_legacy_retry_setter_rejects_even_exact_snapshot(tmp_path):
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        _seed(store)
        before = _snapshot(store)
        with pytest.raises(ValueError, match="unsupported source kind"):
            store.set_retry_budget(store.get_retry_budget("legacy"))
        assert _snapshot(store) == before


@pytest.mark.parametrize("bindings", [0, 1, 2])
def test_legacy_binding_helpers_reject_fresh_bindings(tmp_path, bindings):
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        _, reservation = _seed(store, bindings=bindings)
        before = _snapshot(store)
        calls = [lambda: store.bind_publication_marker("legacy", claim_token=reservation.claim_token, marker_device=11, marker_inode=12),
                 lambda: store._bind_staged_payload("legacy", claim_token=reservation.claim_token, partial_device=13, partial_inode=14, logical_size=15),
                 lambda: store._bind_final_publication("legacy", claim_token=reservation.claim_token, final_device=13, final_inode=14, logical_size=15)]
        with pytest.raises(ValueError, match="unsupported source kind"):
            calls[bindings]()
        assert _snapshot(store) == before


def test_legacy_exact_receipts_and_bindings_replay_read_only(tmp_path):
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        job, reservation = _seed(store)
        before = _snapshot(store)
        assert not store.apply_add(job.intent, materialized=replace(job, source_kind=SourceKind("video"))).applied
        assert store.apply_job_control(job_id="legacy", action="resume", request_id="historical-control",
                                       payload_digest="b" * 64, expected_revision=7).status == "applied"
        assert store.bind_publication_marker("legacy", claim_token=reservation.claim_token, marker_device=11, marker_inode=12)
        assert store._bind_staged_payload("legacy", claim_token=reservation.claim_token, partial_device=13, partial_inode=14, logical_size=15)
        assert store._bind_final_publication("legacy", claim_token=reservation.claim_token, final_device=13, final_inode=14, logical_size=15)
        assert _snapshot(store) == before


@pytest.mark.parametrize("kind", ["unknown", "Video", "", 42])
def test_unknown_source_kind_cannot_control_retry_bind_or_dispatch(tmp_path, kind):
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        _, reservation = _seed(store, bindings=0)
        store._connection.execute("UPDATE materialized_jobs SET source_kind=?", (kind,))
        store._connection.commit()
        before = _snapshot(store)
        calls = [lambda: store.get_materialized_job("legacy"),
                 lambda: store.apply_job_control(job_id="legacy", action="pause", request_id="bad-control", payload_digest="f" * 64, expected_revision=7),
                 lambda: store.set_retry_budget(store.get_retry_budget("legacy")),
                 lambda: store.bind_publication_marker("legacy", claim_token=reservation.claim_token, marker_device=11, marker_inode=12),
                 lambda: store.prepare_direct_dispatch(job_id="legacy", expected_worker_epoch=1, expected_generation=4, expected_revision=7, request_id="bad-dispatch", payload_digest="f" * 64, controller_ready=True, now=NOW)]
        for call in calls:
            with pytest.raises((TypeError, ValueError)):
                call()
            assert _snapshot(store) == before


def test_direct_mp4_is_generic_hash_verified_bytes_with_collision_preserved(tmp_path, private_roots, monkeypatch):
    from hermes_downloads import direct, network, paths
    from hermes_downloads.models import Admission, PublicationReservation
    original_import, original_popen = builtins.__import__, subprocess.Popen
    launches = []
    def guarded_import(name, *args, **kwargs):
        if name == "yt_dlp" or name.startswith("yt_dlp.") or name == "hermes_downloads.video":
            pytest.fail("direct media bytes reached web-video importer")
        return original_import(name, *args, **kwargs)
    def guarded_popen(argv, *args, **kwargs):
        assert Path(argv[0]).name not in {"yt-dlp", "ffmpeg", "ffprobe"}
        process = original_popen(argv, *args, **kwargs)
        launches.append(process)
        return process
    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(subprocess, "Popen", guarded_popen)
    root = Path.home() / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    (root / "Videos").mkdir()
    canary = root / "Videos" / "ordinary.mp4"
    canary.write_bytes(b"existing direct media file")
    destination = paths.resolve_destination(root, category="Videos", filename=canary.name, job_id="mp4-direct")
    assert destination.final_path.name == "ordinary--mp4-direct.mp4"
    reservation = PublicationReservation("mp4-direct", "Videos", destination.final_path.name, "a" * 64)
    paths.attest_publication_reservation_marker(destination, reservation)
    controller = direct.DirectAria2Controller(executable=Path("/opt/homebrew/bin/aria2c"), runtime_root=tmp_path / "aria2-private")
    identity = None
    try:
        with _ORIGIN.SyntheticHttpOrigin() as origin:
            source = network.validate_source_url(origin.url("/range"), local_origin_grant=network.LocalOriginGrant.for_url(origin.url()))
            digest = hashlib.sha256(origin.payload).hexdigest()
            admission = Admission(True, False, True, False, True)
            controller.start()
            identity = controller.engine_identity
            controller.add_paused(job_id="mp4-direct", generation=1, source=source, destination=destination, expected_sha256=digest, admission=admission)
            assert origin.ledger.response_body_bytes == 0
            controller.resume(job_id="mp4-direct", generation=1, admission=admission)
            complete = controller.wait_for_terminal(job_id="mp4-direct", generation=1, timeout=5)
            assert complete.hash_verified and complete.partial_path.read_bytes() == origin.payload
            staged = paths.attest_staged_partial_payload(destination, reservation)
            published = paths.publish_staged_partial_payload(destination, reservation, staged)
            assert published.path.read_bytes() == origin.payload
            assert hashlib.sha256(published.path.read_bytes()).hexdigest() == digest
            assert canary.read_bytes() == b"existing direct media file"
    finally:
        controller.close()
    assert identity is not None
    with pytest.raises(ProcessLookupError):
        os.killpg(identity.process_group_id, 0)
    assert launches and all(process.poll() is not None for process in launches)


@pytest.mark.parametrize("operation", ["admission", "reconciliation"])
def test_private_legacy_admission_and_reconciliation_cannot_create_authority(tmp_path, operation):
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        _seed(store, state="finalizing")
        gate, revision = store.queue_gate_snapshot()
        store.apply_queue_gate(gate="running", request_id="run", payload_digest="d" * 64, expected_revision=revision)
        before = _snapshot(store)
        job = store.get_materialized_job("legacy")
        with pytest.raises(ValueError, match="unsupported source kind"):
            if operation == "admission":
                store._direct_dispatch_admission(store._connection, materialized=job, now=NOW)
            else:
                store._prepare_direct_publication_reconciliation(
                    store._connection, current=store._read_job_control_projection(store._connection, "legacy"),
                    materialized=job, request_id="reconcile", payload_digest="e" * 64)
        assert _snapshot(store) == before


@pytest.mark.parametrize("operation", ["advance", "finish", "abort", "active_pause"])
def test_legacy_target_cannot_inherit_a_preexisting_direct_plan(tmp_path, operation):
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        store.recover_cold_start()
        store.apply_queue_gate(gate="running", request_id="run", payload_digest="d" * 64, expected_revision=1)
        job = _job()
        store.apply_add(job.intent, materialized=job)
        plan = store.prepare_direct_dispatch(job_id="legacy", expected_worker_epoch=1,
                                              expected_generation=4, expected_revision=7,
                                              request_id="dispatch", payload_digest="c" * 64,
                                              controller_ready=True, now=NOW)
        if operation in {"finish", "active_pause"}:
            plan = store.advance_direct_dispatch_to_downloading(plan)
        store._connection.execute("UPDATE materialized_jobs SET source_kind='video' WHERE job_id='legacy'")
        store._connection.commit()
        before = _snapshot(store)
        with pytest.raises(ValueError, match="unsupported source kind"):
            if operation == "advance":
                store.advance_direct_dispatch_to_downloading(plan)
            elif operation == "finish":
                store.finish_direct_dispatch(plan)
            elif operation == "abort":
                store.abort_direct_dispatch(plan)
            else:
                store.pause_active_direct_job(job_id="legacy", generation=plan.generation, revision=plan.revision)
        assert _snapshot(store) == before


def test_actual_worker_ipc_resume_controls_and_dispatch_leave_legacy_inert(monkeypatch):
    from hermes_downloads import ipc, worker
    with tempfile.TemporaryDirectory(prefix="direct-only-", dir="/private/tmp") as directory, _ORIGIN.SyntheticHttpOrigin() as origin:
        root = Path(directory)
        root.chmod(0o700)
        database = root / "state.db"
        with closing(SQLiteStore(database)) as store:
            _seed(store, state="finalizing", dispatch=True)
            store._connection.execute("UPDATE jobs SET source_url=? WHERE job_id='legacy'", (origin.url("/range").encode(),))
            store._connection.commit()
            before = _snapshot(store)
        _guards(monkeypatch)
        ready, shutdown, stopped = (threading.Event() for _ in range(3))
        outcomes = []
        def run():
            try:
                outcomes.append(worker.run_worker(root, socket_path=root / "worker.sock", ready_event=ready,
                                                   shutdown_event=shutdown, stopped_event=stopped))
            except BaseException as error:
                outcomes.append(error)
        thread = threading.Thread(target=run, name="direct-only-private-worker")
        thread.start()
        try:
            assert ready.wait(5)
            socket = root / "worker.sock"
            health = ipc.request_health(socket)
            assert health.queue_gate == "paused"
            assert ipc.request_jobs_page(socket).jobs[0].state == "finalizing"
            assert ipc.set_queue_gate(socket, gate="running", request_id="resume-all", expected_revision=2).queue_gate == "running"
            for action in ("pause", "resume", "start_now", "remove"):
                assert ipc.control_job(socket, job="legacy", action=action, request_id=f"ipc-{action}", expected_revision=7).status == "blocked"
            assert ipc.dispatch_direct_job(socket, job="legacy", expected_worker_epoch=health.worker_epoch,
                                           expected_generation=4, expected_revision=7, request_id="ipc-dispatch").status == "blocked"
            assert origin.ledger.response_body_bytes == 0
        finally:
            shutdown.set()
            thread.join(5)
        assert not thread.is_alive() and stopped.is_set() and outcomes == [None]
        with closing(SQLiteStore(database)) as store:
            assert _snapshot(store) == before
