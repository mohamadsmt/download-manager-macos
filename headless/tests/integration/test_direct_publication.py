"""Real direct producer, publication controls and explicit retained-final recovery."""
from __future__ import annotations
import importlib.util
import json
import hashlib
from dataclasses import asdict
import multiprocessing
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
import ctypes
import signal
import uuid
import pytest
from hermes_downloads import ipc, worker
from hermes_downloads.store import SQLiteStore

_spec = importlib.util.spec_from_file_location('_publication_ipc_helpers', Path(__file__).with_name('test_ipc.py'))
_helpers = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _helpers
_spec.loader.exec_module(_helpers)


@pytest.fixture
def publication_evidence(tmp_path):
    parent = Path(os.environ.get('T15G4B_EVIDENCE', str(tmp_path)))
    evidence = parent / ('publication-' + uuid.uuid4().hex)
    evidence.mkdir(mode=0o700)
    return evidence


def _os_argv(pid):
    """Read the actual Darwin argv vector, not the multiprocessing sys.argv."""
    library = ctypes.CDLL(None, use_errno=True)
    mib = (ctypes.c_int * 3)(1, 49, pid)  # CTL_KERN, KERN_PROCARGS2
    data = ctypes.create_string_buffer(262144)
    size = ctypes.c_size_t(len(data))
    call = library.sysctl
    call.argtypes = (ctypes.POINTER(ctypes.c_int), ctypes.c_uint, ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t), ctypes.c_void_p, ctypes.c_size_t)
    call.restype = ctypes.c_int
    assert call(mib, 3, data, ctypes.byref(size), None, 0) == 0
    raw = data.raw[:size.value]
    argc = int.from_bytes(raw[:4], sys.byteorder)
    assert 0 < argc < 1024
    offset = raw.index(b'\0', 4) + 1
    while raw[offset] == 0:
        offset += 1
    argv = []
    for _ in range(argc):
        end = raw.index(b'\0', offset)
        argv.append(os.fsdecode(raw[offset:end]))
        offset = end + 1
    return argv


def _append_record(path, kind, **values):
    with path.open('a', encoding='utf-8') as output:
        path.chmod(0o600)
        output.write(json.dumps(dict(kind=kind, **values), sort_keys=True) + '\n')
        output.flush()
        os.fsync(output.fileno())


def _close_fixture_process(process, evidence, *, crash=False):
    """Signal only this newly recorded matching birth; reap waitable workers."""
    from hermes_downloads import processes
    ledger = evidence / f'fixture-{process.pid}.jsonl'
    try:
        if crash and process.is_alive():
            rows = [json.loads(line) for line in ledger.read_text().splitlines()]
            identity = processes.ProcessBirthIdentity.from_record(next(
                row['identity'] for row in rows if row['kind'] == 'worker-birth'))
            assert processes.is_current_process_birth(identity)
            assert os.getpgid(process.pid) == identity.process_group_id
            os.killpg(identity.process_group_id, signal.SIGKILL)
    finally:
        process.join(8)
        alive = process.is_alive()
        _append_record(evidence / 'parent-closure.jsonl', 'worker-join-result',
            pid=process.pid, exitcode=process.exitcode, alive=alive, waitable_child=True)
        assert not alive, 'owned fixture child survived join8; closure uncertain'
    rows = [json.loads(line) for line in ledger.read_text().splitlines()]
    identity = processes.ProcessBirthIdentity.from_record(next(
        row['identity'] for row in rows if row['kind'] == 'worker-birth'))
    assert processes.reconcile_process_birth(identity) == 'absent'
    _append_record(evidence / 'parent-closure.jsonl', 'worker-reaped',
        identity=identity.to_record(), exitcode=process.exitcode, waitable_child=True,
        reconciliation='absent')
    for row in rows:
        if row['kind'] != 'engine-birth':
            continue
        engine = processes.ProcessBirthIdentity.from_record(row['identity'])
        for sig in (signal.SIGTERM, signal.SIGKILL):
            status = processes.reconcile_process_birth(engine)
            # A just-exited adopted leader may be unreapable while launchd
            # retires it. Wait for fresh authority/positive absence; uncertainty
            # never authorizes a signal or another payload.
            deadline = time.monotonic() + 2
            while status == 'indeterminate' and time.monotonic() < deadline:
                time.sleep(.01)
                status = processes.reconcile_process_birth(engine)
            if status == 'absent':
                break
            assert status == 'current' and processes.is_current_process_birth(engine)
            assert os.getpgid(engine.leader_pid) == engine.process_group_id
            os.killpg(engine.process_group_id, sig)
            deadline = time.monotonic() + 2
            while processes.reconcile_process_birth(engine) == 'current' and time.monotonic() < deadline:
                time.sleep(.01)
        assert processes.reconcile_process_birth(engine) == 'absent'
        _append_record(evidence / 'parent-closure.jsonl', 'engine-positive-absence',
            identity=engine.to_record(), reconciliation='absent',
            waitable_child=False, adopted_after_worker_exit=crash)


def _publication_worker(state, sock, ready, shutdown, stopped, results, origin, barrier, entered, release, evidence, restart_ready=None, recover_socket=False):
    from hermes_downloads import network, paths, direct, processes
    os.setsid()
    ledger = Path(evidence) / f'fixture-{os.getpid()}.jsonl'
    def record(kind, **values):
        with ledger.open('a', encoding='utf-8') as output:
            os.chmod(ledger, 0o600)
            output.write(json.dumps(dict(kind=kind, **values), sort_keys=True) + '\n')
            output.flush()
            os.fsync(output.fileno())
    argv = _os_argv(os.getpid())
    identity = processes.capture_process_birth(processes.EngineIdentity(os.getpid(), os.getpid(), time.monotonic_ns(), hashlib.sha256(json.dumps(argv).encode()).hexdigest()))
    assert identity is not None
    record('worker-birth', identity=identity.to_record(), os_argv=argv, argv_source='KERN_PROCARGS2')
    engine_births = []
    original_init = direct.DirectAria2Controller.__init__
    def init(self, *args, **kwargs):
        callback = kwargs.get('on_engine_bound')
        def bound(identity):
            engine_births.append(identity)
            assert processes.is_current_process_birth(identity)
            actual_argv = _os_argv(identity.leader_pid)
            configured_digest = hashlib.sha256(b'\0'.join(os.fsencode(arg) for arg in actual_argv)).hexdigest()
            assert configured_digest == identity.argv_sha256
            record('engine-birth', identity=identity.to_record(), ownership='new fixture callback',
                os_argv=actual_argv, os_argv_sha256=configured_digest, argv_source='KERN_PROCARGS2',
                configured_argv_matches_os=True)
            if callback is not None:
                callback(identity)
        kwargs['on_engine_bound'] = bound
        original_init(self, *args, **kwargs)
    direct.DirectAria2Controller.__init__ = init
    original_close = direct.DirectAria2Controller.close
    def close(self):
        child = self._process
        original_close(self)
        for identity in engine_births:
            absence = processes.reconcile_process_birth(identity)
            record('engine-closure', identity=identity.to_record(), reconciliation=absence,
                leader_reaped=self._process is None)
            assert absence == 'absent'
            if child is not None and child.pid == identity.leader_pid:
                assert type(child.returncode) is int
                with pytest.raises(ChildProcessError):
                    os.waitpid(child.pid, os.WNOHANG)
                record('engine-waitable-child-reaped', identity=identity.to_record(),
                    returncode=child.returncode, waitpid='ECHILD', reconciliation=absence)
    direct.DirectAria2Controller.close = close
    original_gate = SQLiteStore.apply_queue_gate
    def gate(self, **kwargs):
        try:
            return original_gate(self, **kwargs)
        except BaseException as error:
            record('queue-fault', type=type(error).__name__, detail=str(error))
            raise
    SQLiteStore.apply_queue_gate = gate
    owner = threading.get_ident()
    grant = network.LocalOriginGrant.for_url(origin)
    validate = worker.validate_source_url
    worker.validate_source_url = lambda value: validate(value, local_origin_grant=grant)
    def hold(*, owner_allowed=False):
        assert owner_allowed or threading.get_ident() != owner
        record('phase-entered', phase=barrier, owner_thread=threading.get_ident() == owner)
        entered.set()
        if barrier.startswith('cut-'):
            limit = time.monotonic() + 12
            while not (Path(evidence) / 'release-cut').exists() and time.monotonic() < limit:
                time.sleep(.01)
            assert (Path(evidence) / 'release-cut').exists(), 'finite crash barrier expired'
        elif barrier == 'recovery-chunk':
            release_file = Path(evidence) / f'release-hash-{os.getpid()}'
            limit = time.monotonic() + 12
            while not release_file.exists() and time.monotonic() < limit:
                time.sleep(.01)
            assert release_file.exists(), 'finite recovery hash barrier expired'
        elif barrier == 'post-link' and os.environ.get('T15G4B_CRASH_FILE'):
            limit = time.monotonic()+8
            while not Path(os.environ['T15G4B_CRASH_FILE']).exists() and time.monotonic()<limit:
                time.sleep(.01)
        else:
            assert release.wait(8)
    if barrier in {'prepare', 'cut-prepared'}:
        original = paths.prepare_publication_payload
        def prepare(*args, **kwargs):
            if barrier == 'cut-prepared':
                value = original(*args, **kwargs)
                hold()
                return value
            hold()
            return original(*args, **kwargs)
        paths.prepare_publication_payload = prepare
    elif barrier == 'pre-create':
        original = paths._publish_prepared_payload
        def before_creation(*args, **kwargs):
            hold()
            return original(*args, **kwargs)
        paths._publish_prepared_payload = before_creation
    elif barrier in {'link', 'cut-link'}:
        original = paths.os.link
        def link(*args, **kwargs):
            if barrier == 'cut-link':
                value = original(*args, **kwargs)
                hold()
                return value
            hold()
            return original(*args, **kwargs)
        paths.os.link = link
    elif barrier in {'recovery', 'recovery-chunk'}:
        original = paths._hash_publication_payload
        original_pread = os.pread
        reads = []
        hashing = threading.local()
        def pread(fd, count, offset):
            value = original_pread(fd, count, offset)
            if getattr(hashing, 'active', False):
                reads.append((count, offset))
                if offset == 0:
                    hold()
            return value
        def hashed(*args, **kwargs):
            if barrier == 'recovery':
                hold()
            hashing.active = True
            try:
                return original(*args, **kwargs)
            finally:
                hashing.active = False
                record('recovery-hash-exit', reads=reads,
                    cancelled=bool(kwargs.get('cancelled') and kwargs['cancelled']()))
        paths._hash_publication_payload = hashed
        if barrier == 'recovery-chunk':
            paths.os.pread = pread
        paths.os.link = lambda *a, **k: (_ for _ in ()).throw(AssertionError('recovery link prohibited'))
    elif barrier in {'post-link', 'cut-before-fsync', 'cut-after-fsync'}:
        original = paths._fsync_published_final_directory
        def sync(fd):
            if barrier == 'cut-after-fsync':
                original(fd)
                hold()
                return
            hold()
            return original(fd)
        paths._fsync_published_final_directory = sync
    elif barrier == 'cut-attempt':
        original = SQLiteStore.reserve_direct_publication
        def reserve(self, *args, **kwargs):
            value = original(self, *args, **kwargs)
            hold(owner_allowed=True)
            return value
        SQLiteStore.reserve_direct_publication = reserve
    elif barrier == 'cut-permit':
        original = paths.PublicationCreationPermit.__init__
        def permit(self):
            original(self)
            hold(owner_allowed=True)
        paths.PublicationCreationPermit.__init__ = permit
    elif barrier in {'cut-before-commit', 'cut-after-commit'}:
        original = SQLiteStore.complete_direct_publication
        class CompletionConnection:
            def __init__(self, connection):
                self.connection = connection
            def __getattr__(self, name):
                return getattr(self.connection, name)
            def commit(self):
                if barrier == 'cut-before-commit':
                    hold(owner_allowed=True)
                self.connection.commit()
                if barrier == 'cut-after-commit':
                    hold(owner_allowed=True)
        def complete(self, *args, **kwargs):
            connection = self._connection
            self._connection = CompletionConnection(connection)
            try:
                return original(self, *args, **kwargs)
            finally:
                self._connection = connection
        SQLiteStore.complete_direct_publication = complete
    if barrier in {'cold', 'recovery', 'recovery-chunk'}:
        def forbidden_engine(*args, **kwargs):
            record('forbidden-engine-effect')
            raise AssertionError('cold/recovery cannot activate or admit an engine')
        direct.DirectAria2Controller.start = forbidden_engine
        direct.DirectAria2Controller.add_paused = forbidden_engine
        direct.DirectAria2Controller.resume = forbidden_engine
        if barrier == 'cold':
            def forbidden_io(*args, **kwargs):
                record('forbidden-cold-io')
                raise AssertionError('cold must not hash/link/complete')
            paths._hash_publication_payload = forbidden_io
            paths.os.link = forbidden_io
            SQLiteStore.complete_direct_publication = forbidden_io
    _helpers._run_worker_process(state, sock, ready, shutdown, stopped, results, recover_socket)
    if restart_ready is not None:
        record('shutdown-drain', gate_locked=worker._DIRECT_OBSERVATION_LOCK.locked(),
            surviving_threads=[t.name for t in threading.enumerate() if t.name == 'direct-publication-observation'],
            socket_absent=not Path(sock).exists(),
            certificate_absent=not (Path(state) / '.worker-endpoint.json').exists())
        shutdown.clear()
        stopped.clear()
        class RestartReady:
            def set(self):
                restart_ready.set()
        _helpers._run_worker_process(state, sock, RestartReady(), shutdown, stopped, results, recover_socket)
    record('worker-return', engines=[dict(identity=i.to_record(), reconciliation=processes.reconcile_process_birth(i)) for i in engine_births])


def _wait_state(sock, desired):
    deadline = time.monotonic() + 7
    while time.monotonic() < deadline:
        current = ipc.request_jobs_page(sock).jobs[0]
        if current.state == desired:
            return current
        time.sleep(.01)
    pytest.fail(f'worker never reached {desired}; last={current}')


def _receipt(db):
    with sqlite3.connect(db) as connection:
        return tuple(connection.execute('SELECT * FROM direct_dispatch_commands WHERE request_id = ?', ('publication-start',)).fetchone())


@pytest.mark.parametrize('barrier,action', [(None,None), ('prepare','pause'), ('prepare','remove'),
    ('prepare','queue'), ('link','pause'), ('link','remove'), ('link','queue'),
    ('post-link','pause'), ('post-link','queue'), ('post-link','remove'), ('post-link','cold')])
def test_real_direct_initial_publication_and_exact_attempt_recovery(barrier, action, publication_evidence):
    with tempfile.TemporaryDirectory(dir='/tmp', prefix='hd-pub-') as temp:
        state = Path(temp) / 'state'
        state.mkdir(mode=0o700)
        sock = state / 'worker.sock'
        root = Path.home() / 'Downloads/Hermes'
        root.mkdir(parents=True, mode=0o700)
        partial = root / '.incomplete/dispatch-job/dispatch.bin'
        final = root / 'Other/dispatch.bin'
        context = multiprocessing.get_context('spawn')
        ready, shutdown, stopped, entered, release = [context.Event() for _ in range(5)]
        results = context.Queue()
        with _helpers._origin_type()(payload_size=1024) as origin:
            _helpers._seed_local_direct_dispatch_job(state, origin.url('/range'))
            if action == 'cold':
                os.environ['T15G4B_CRASH_FILE'] = str(state / 'release')
            else:
                os.environ.pop('T15G4B_CRASH_FILE', None)
            process = context.Process(target=_publication_worker, args=(str(state), str(sock), ready,
                shutdown, stopped, results, origin.url(), barrier, entered, release, str(publication_evidence)))
            process.start()
            try:
                assert ready.wait(5)
                assert ipc.set_queue_gate(sock, gate='running', request_id='publication-open', expected_revision=1).applied
                assert ipc.control_job(sock, job='dispatch-job', action='start_now', request_id='publication-authorize', expected_revision=1).status == 'applied'
                assert ipc.activate_direct_engine(sock, expected_worker_epoch=1).status == 'active'
                started = ipc.dispatch_direct_job(sock, job='dispatch-job', expected_worker_epoch=1,
                    expected_generation=1, expected_revision=2, request_id='publication-start')
                assert (started.status, started.state, started.revision) == ('started','downloading',4)
                original = _receipt(state / 'state.db')
                if barrier:
                    assert entered.wait(5), 'real producer did not enter publication barrier'
                    before = time.monotonic()
                    assert ipc.request_health(sock).worker_epoch == 1
                    assert ipc.request_jobs_page(sock).jobs[0].state == 'finalizing'
                    assert time.monotonic() - before < 2
                    assert ipc.dispatch_direct_job(sock, job='dispatch-job', expected_worker_epoch=1,
                        expected_generation=1, expected_revision=2, request_id='publication-start') == started
                    if action == 'cold':
                        _close_fixture_process(process, publication_evidence, crash=True)
                        # This BASE rejects abandoned sockets; explicitly preserve the fixture endpoint.
                        sock.rename(state / 'retained-abandoned.sock')
                    else:
                        def control():
                            if action == 'queue':
                                return ipc.set_queue_gate(sock, gate='paused', request_id='publication-control', expected_revision=2)
                            return ipc.control_job(sock, job='dispatch-job', action=action,
                                request_id='publication-control', expected_revision=5)
                        if barrier == 'link':
                            before = time.monotonic()
                            with pytest.raises(ipc.IPCError, match='command_conflict'):
                                control()
                            assert time.monotonic() - before < 2
                            assert ipc.request_jobs_page(sock).jobs[0].state == 'finalizing'
                            with sqlite3.connect(state / 'state.db') as conn:
                                assert conn.execute('SELECT COUNT(*) FROM command_receipts WHERE request_id = ?', ('publication-control',)).fetchone()[0] == 0
                            release.set()
                        retry_deadline = time.monotonic()+2
                        while True:
                            try:
                                result = control()
                                break
                            except ipc.IPCError:
                                if barrier != 'link' or time.monotonic() >= retry_deadline:
                                    raise
                                time.sleep(.01)
                        assert getattr(result, 'status', 'applied') == 'applied'
                        release.set()
                        _wait_state(sock, 'removed' if action == 'remove' else 'paused')
                    if barrier == 'prepare':
                        assert not final.exists()
                    elif action != 'remove':
                        assert final.exists()
                    if action != 'cold':
                        shutdown.set()
                        _helpers._join(process)
                    if barrier == 'post-link' and action in {'pause','queue','cold'}:
                        # Genuine cold epochs update only the exact attempt pointer.
                        observer = SQLiteStore(state / 'state.db')
                        observer.recover_cold_start()
                        observer.close()
                        ready, shutdown, stopped, entered, release = [context.Event() for _ in range(5)]
                        before_cold_ledger = origin.ledger
                        os.environ.pop('T15G4B_CRASH_FILE', None)
                        process = context.Process(target=_publication_worker, args=(str(state),str(sock),ready,
                            shutdown,stopped,results,origin.url(),'recovery',entered,release,str(publication_evidence)))
                        process.start()
                        assert ready.wait(5)
                        current = ipc.request_jobs_page(sock).jobs[0]
                        epoch = ipc.request_health(sock).worker_epoch
                        assert current.state == 'paused'
                        assert ipc.request_health(sock).queue_gate == 'paused'
                        assert origin.ledger == before_cold_ledger
                        assert not entered.is_set()
                        pending = ipc.dispatch_direct_job(sock, job='dispatch-job', expected_worker_epoch=epoch,
                            expected_generation=current.generation, expected_revision=current.revision, request_id='publication-recover')
                        assert pending.status == 'pending'
                        assert entered.wait(5)
                        assert ipc.request_health(sock).queue_gate == 'paused'
                        assert ipc.dispatch_direct_job(sock, job='dispatch-job', expected_worker_epoch=epoch,
                            expected_generation=current.generation, expected_revision=current.revision, request_id='publication-recover') == pending
                        with pytest.raises(ipc.IPCError, match='command_conflict'):
                            ipc.dispatch_direct_job(sock, job='dispatch-job', expected_worker_epoch=epoch,
                                expected_generation=current.generation, expected_revision=current.revision+1, request_id='publication-recover')
                        release.set()
                        _wait_state(sock,'completed')
                        finished = ipc.dispatch_direct_job(sock, job='dispatch-job', expected_worker_epoch=epoch,
                            expected_generation=current.generation, expected_revision=current.revision, request_id='publication-recover')
                        assert (finished.status, finished.state) == ('started','completed')
                else:
                    _wait_state(sock,'completed')
                expected = int(action is None or (barrier == 'post-link' and action in {'pause','queue','cold'}))
                cleanup = None
                if expected:
                    cleanup = _helpers._assert_finished_direct_cleanup(state / 'state.db', partial, final, origin.payload)
                else:
                    assert partial.exists() and partial.with_name('.hermes-reservation').exists()
                evidence = publication_evidence / f'outcome-{barrier}-{action}-{os.getpid()}.json'
                evidence.write_text(json.dumps(dict(barrier=barrier, action=action, origin_ledger=asdict(origin.ledger), original_receipt=original, final_exists=final.exists(), partial_exists=partial.exists(), partial_sha256=hashlib.sha256(partial.read_bytes()).hexdigest() if partial.exists() else None, final_sha256=hashlib.sha256(final.read_bytes()).hexdigest() if final.exists() else None, cleanup_claim=cleanup), sort_keys=True))
                evidence.chmod(0o600)
                assert _receipt(state / 'state.db') == original
                observer = SQLiteStore(state / 'state.db')
                try:
                    count = sum(e.kind == 'job_completed' for e in observer.list_events())
                    assert count == expected
                    with sqlite3.connect(state / 'state.db') as connection:
                        details = dict(attempts=[tuple(r) for r in connection.execute('SELECT * FROM direct_publication_attempts')],
                            events=[tuple(r) for r in connection.execute('SELECT * FROM events')],
                            final_bindings=[tuple(r) for r in connection.execute('SELECT * FROM final_publication_bindings')])
                    proof_file = evidence.with_name('binding-' + evidence.name)
                    proof_file.write_text(json.dumps(details,sort_keys=True))
                    proof_file.chmod(0o600)
                    if expected:
                        assert observer._get_final_publication_binding('dispatch-job') is not None
                finally:
                    observer.close()
            finally:
                release.set()
                shutdown.set()
                try:
                    _close_fixture_process(process, publication_evidence)
                finally:
                    try:
                        results.close()
                    finally:
                        results.join_thread()


def _spawn_publication_worker(state, origin, barrier, evidence, *, restart=False, recover_socket=False):
    context = multiprocessing.get_context('spawn')
    ready, shutdown, stopped, entered, release, restarted = [context.Event() for _ in range(6)]
    results = context.Queue()
    process = context.Process(target=_publication_worker, args=(str(state), str(state / 'worker.sock'),
        ready, shutdown, stopped, results, origin.url(), barrier, entered, release, str(evidence),
        restarted if restart else None, recover_socket))
    process.start()
    try:
        assert ready.wait(6)
    except BaseException:
        release.set()
        shutdown.set()
        try:
            _close_fixture_process(process, evidence)
        finally:
            try:
                results.close()
            finally:
                results.join_thread()
        raise
    return dict(process=process, shutdown=shutdown, stopped=stopped, entered=entered,
        release=release, restarted=restarted, results=results)


def _finish_publication_worker(child, evidence):
    if child['process'].is_alive():
        release_file = evidence / f"release-hash-{child['process'].pid}"
        release_file.touch(mode=0o600)
        child['release'].set()
        child['shutdown'].set()
    try:
        _close_fixture_process(child['process'], evidence)
    finally:
        try:
            child['results'].close()
        finally:
            child['results'].join_thread()


def _start_real_publication(state, origin, barrier, evidence, *, restart=False, recover_socket=False):
    _helpers._seed_local_direct_dispatch_job(state, origin.url('/range'))
    child = _spawn_publication_worker(state, origin, barrier, evidence, restart=restart,
        recover_socket=recover_socket)
    try:
        sock = state / 'worker.sock'
        assert ipc.set_queue_gate(sock, gate='running', request_id='publication-open', expected_revision=1).applied
        assert ipc.control_job(sock, job='dispatch-job', action='start_now', request_id='publication-authorize', expected_revision=1).status == 'applied'
        assert ipc.activate_direct_engine(sock, expected_worker_epoch=1).status == 'active'
        started = ipc.dispatch_direct_job(sock, job='dispatch-job', expected_worker_epoch=1,
            expected_generation=1, expected_revision=2, request_id='publication-start')
        assert (started.status, started.state, started.revision) == ('started', 'downloading', 4)
        assert child['entered'].wait(6)
        return child, started, _receipt(state / 'state.db')
    except BaseException:
        _finish_publication_worker(child, evidence)
        raise


def test_real_same_job_closed_attempt_new_dispatch_transfers_and_publishes(publication_evidence):
    with tempfile.TemporaryDirectory(dir='/private/tmp', prefix='hd-pub-') as temp:
        state = Path(temp) / 'state'; state.mkdir(mode=0o700)
        sock = state / 'worker.sock'
        root = Path.home() / 'Downloads/Hermes'; root.mkdir(parents=True,mode=0o700)
        partial = root / '.incomplete/dispatch-job/dispatch.bin'
        final = root / 'Other/dispatch.bin'
        with _helpers._origin_type()(payload_size=1024) as origin:
            child, started, original = _start_real_publication(state,origin,'pre-create',publication_evidence)
            try:
                assert not final.exists()
                assert ipc.control_job(sock,job='dispatch-job',action='pause',
                    request_id='closed-before-create',expected_revision=5).status == 'applied'
                current = _wait_state(sock,'paused')
                with sqlite3.connect(state / 'state.db') as connection:
                    closed = tuple(connection.execute('SELECT * FROM direct_publication_attempts').fetchone())
                    assert closed[4] == 'closed'
                child['release'].set()
                assert ipc.control_job(sock,job='dispatch-job',action='start_now',
                    request_id='new-attempt-authorize',expected_revision=current.revision).state == 'queued'
                deadline = time.monotonic()+5
                while ipc.activate_direct_engine(sock,expected_worker_epoch=1).status != 'active':
                    assert time.monotonic()<deadline
                    time.sleep(.01)
                current = ipc.request_jobs_page(sock).jobs[0]
                second = ipc.dispatch_direct_job(sock,job='dispatch-job',expected_worker_epoch=1,
                    expected_generation=current.generation,expected_revision=current.revision,
                    request_id='new-attempt-start')
                assert (second.status,second.state)==('started','downloading')
                _wait_state(sock,'completed')
                cleanup = _helpers._assert_finished_direct_cleanup(state / 'state.db', partial, final, origin.payload)
                assert final.stat().st_size == 1024
                assert _receipt(state / 'state.db') == original
                assert ipc.dispatch_direct_job(sock,job='dispatch-job',expected_worker_epoch=1,
                    expected_generation=1,expected_revision=2,request_id='publication-start') == started
                with sqlite3.connect(state / 'state.db') as connection:
                    assert tuple(connection.execute('SELECT * FROM closed_direct_publication_attempts').fetchone()) == closed
                    active = tuple(connection.execute('SELECT * FROM direct_publication_attempts').fetchone())
                    assert active[1] != closed[1] and active[4] == 'finished'
                    assert connection.execute("SELECT COUNT(*) FROM events WHERE kind='job_completed'").fetchone()[0] == 1
                rows = [json.loads(line) for line in (publication_evidence / f"fixture-{child['process'].pid}.jsonl").read_text().splitlines()]
                assert len([row for row in rows if row['kind']=='engine-birth']) == 2
                outcome = dict(original_receipt=original,closed_attempt=closed,current_attempt=active,
                    origin_ledger=asdict(origin.ledger),partial_exists=False,partial_sha256=None,cleanup_claim=cleanup,
                    final_sha256=hashlib.sha256(final.read_bytes()).hexdigest(),engine_births=2,payload_bytes=1024)
                path = publication_evidence / 'closed-continuation-outcome.json'
                path.write_text(json.dumps(outcome,sort_keys=True));path.chmod(0o600)
            finally:
                _finish_publication_worker(child,publication_evidence)


@pytest.mark.parametrize('action', ('pause', 'remove', 'queue', 'shutdown'))
def test_controller_absent_held_recovery_hash_controls_cancel_without_late_completion(action, publication_evidence):
    with tempfile.TemporaryDirectory(dir='/tmp', prefix='hd-pub-') as temp:
        state = Path(temp) / 'state'
        state.mkdir(mode=0o700)
        sock = state / 'worker.sock'
        root = Path.home() / 'Downloads/Hermes'
        root.mkdir(parents=True, mode=0o700)
        children = []
        with _helpers._origin_type()(payload_size=2*1024*1024+17) as origin:
            try:
                first, started, original = _start_real_publication(state, origin, 'post-link', publication_evidence)
                children.append(first)
                assert ipc.control_job(sock, job='dispatch-job', action='pause', request_id='retain-final', expected_revision=5).status == 'applied'
                _finish_publication_worker(first, publication_evidence)
                before_body = origin.ledger
                recovery = _spawn_publication_worker(state, origin, 'recovery-chunk', publication_evidence)
                children.append(recovery)
                current = ipc.request_jobs_page(sock).jobs[0]
                epoch = ipc.request_health(sock).worker_epoch
                arguments = dict(job='dispatch-job', expected_worker_epoch=epoch,
                    expected_generation=current.generation, expected_revision=current.revision,
                    request_id='held-new-recovery')
                pending = ipc.dispatch_direct_job(sock, **arguments)
                assert pending.status == 'pending'
                assert recovery['entered'].wait(6)
                assert ipc.dispatch_direct_job(sock, **arguments) == pending
                assert ipc.control_job(sock, job='dispatch-job', action='remove', request_id='stale-remove', expected_revision=current.revision-1).status == 'stale'
                assert ipc.request_jobs_page(sock).jobs[0].state == 'paused'
                with sqlite3.connect(state / 'state.db') as connection:
                    assert connection.execute('SELECT COUNT(*) FROM engine_instances').fetchone()[0] == 0
                    audits_before = tuple(connection.execute('SELECT * FROM events'))
                    gate_revision = int(connection.execute("SELECT revision FROM settings WHERE key='queue_gate'").fetchone()[0])
                before = time.monotonic()
                if action == 'shutdown':
                    recovery['shutdown'].set()
                    assert recovery['stopped'].wait(3)
                elif action == 'queue':
                    assert ipc.set_queue_gate(sock, gate='paused', request_id='cancel-held', expected_revision=gate_revision).applied
                else:
                    assert ipc.control_job(sock, job='dispatch-job', action=action, request_id='cancel-held', expected_revision=current.revision).status == 'applied'
                assert time.monotonic() - before < 3
                assert not recovery['release'].is_set()
                if action != 'shutdown':
                    assert ipc.activate_direct_engine(sock, expected_worker_epoch=epoch).status == 'blocked'
                    result = ipc.dispatch_direct_job(sock, **arguments)
                    assert result.status == 'blocked'
                    (publication_evidence / f"release-hash-{recovery['process'].pid}").touch(mode=0o600)
                    recovery['release'].set()
                    _wait_state(sock, 'removed' if action == 'remove' else 'paused')
                _finish_publication_worker(recovery, publication_evidence)
                rows = [json.loads(line) for line in (publication_evidence / f"fixture-{recovery['process'].pid}.jsonl").read_text().splitlines()]
                if action != 'shutdown':
                    hashed = next(r for r in rows if r['kind'] == 'recovery-hash-exit')
                    assert hashed['reads'] == [[1024*1024, 0]] and hashed['cancelled']
                assert origin.ledger == before_body
                assert _receipt(state / 'state.db') == original
                with sqlite3.connect(state / 'state.db') as connection:
                    assert connection.execute("SELECT COUNT(*) FROM events WHERE kind='job_completed'").fetchone()[0] == 0
                    assert connection.execute('SELECT COUNT(*) FROM final_publication_bindings').fetchone()[0] == 0
                    assert connection.execute("SELECT status FROM direct_dispatch_commands WHERE request_id='held-new-recovery'").fetchone()[0] == 'blocked'
                    if action in {'pause', 'queue'}:
                        assert tuple(connection.execute('SELECT * FROM events')) == audits_before
                final = root / 'Other/dispatch.bin'
                partial = root / '.incomplete/dispatch-job/dispatch.bin'
                assert final.stat().st_ino == partial.stat().st_ino
                assert final.read_bytes() == origin.payload
            finally:
                for child in children:
                    _finish_publication_worker(child, publication_evidence)


@pytest.mark.parametrize('barrier', ('link', 'post-link'))
@pytest.mark.parametrize('recover_socket', (False, True))
def test_real_shutdown_same_process_restart_activation_waits_for_publisher_exit(barrier, recover_socket, publication_evidence):
    with tempfile.TemporaryDirectory(dir='/private/tmp' if recover_socket else '/tmp', prefix='hd-pub-') as temp:
        state = Path(temp) / 'state'
        state.mkdir(mode=0o700)
        sock = state / 'worker.sock'
        (Path.home() / 'Downloads/Hermes').mkdir(parents=True, mode=0o700)
        with _helpers._origin_type()(payload_size=1024) as origin:
            child, started, original = _start_real_publication(state, origin, barrier, publication_evidence,
                restart=True, recover_socket=recover_socket)
            try:
                body_before = origin.ledger
                child['shutdown'].set()
                assert child['restarted'].wait(6)
                assert not child['release'].is_set()
                assert ipc.request_health(sock).worker_epoch == 2
                assert ipc.request_jobs_page(sock).jobs[0].state == 'paused'
                assert ipc.activate_direct_engine(sock, expected_worker_epoch=2).status == 'blocked'
                assert origin.ledger == body_before
                rows = [json.loads(line) for line in (publication_evidence / f"fixture-{child['process'].pid}.jsonl").read_text().splitlines()]
                drain = next(row for row in rows if row['kind'] == 'shutdown-drain')
                assert drain['gate_locked'] and drain['surviving_threads'] == ['direct-publication-observation']
                if recover_socket:
                    assert drain['socket_absent'] and drain['certificate_absent']
                    assert json.loads((state / '.worker-endpoint.json').read_bytes())['worker_epoch'] == 2
                first_result = child['results'].get(timeout=2)
                assert first_result == (('error', 'IPCStateError', 'direct_dispatch_blocked') if barrier == 'link' else ('result', None))
                child['release'].set()
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if ipc.activate_direct_engine(sock, expected_worker_epoch=2).status == 'active':
                        break
                    time.sleep(.01)
                else:
                    pytest.fail('drained publisher still blocks activation')
                assert _receipt(state / 'state.db') == original
                assert ipc.dispatch_direct_job(sock, job='dispatch-job', expected_worker_epoch=1,
                    expected_generation=1, expected_revision=2, request_id='publication-start') == started
                assert origin.ledger == body_before
                with sqlite3.connect(state / 'state.db') as connection:
                    assert connection.execute("SELECT COUNT(*) FROM events WHERE kind='job_completed'").fetchone()[0] == 0
            finally:
                _finish_publication_worker(child, publication_evidence)


@pytest.mark.parametrize('cut', ('prepared', 'attempt', 'permit', 'link', 'before-fsync', 'after-fsync', 'before-commit', 'after-commit'))
def test_real_direct_finite_crash_matrix_cold_inert_and_new_existing_only(cut, publication_evidence):
    with tempfile.TemporaryDirectory(dir='/tmp', prefix='hd-pub-') as temp:
        state = Path(temp) / 'state'
        state.mkdir(mode=0o700)
        sock = state / 'worker.sock'
        root = Path.home() / 'Downloads/Hermes'
        root.mkdir(parents=True, mode=0o700)
        children = []
        with _helpers._origin_type()(payload_size=1024) as origin:
            try:
                child, started, original = _start_real_publication(state, origin, 'cut-' + cut, publication_evidence)
                children.append(child)
                _close_fixture_process(child['process'], publication_evidence, crash=True)
                sock.rename(state / 'retained-crash-endpoint.sock')
                before_body = origin.ledger
                cold = _spawn_publication_worker(state, origin, 'cold', publication_evidence)
                children.append(cold)
                current = ipc.request_jobs_page(sock).jobs[0]
                completed_before_crash = cut == 'after-commit'
                assert current.state == ('completed' if completed_before_crash else 'paused')
                assert ipc.request_health(sock).queue_gate == 'paused'
                assert not cold['entered'].is_set()
                assert ipc.dispatch_direct_job(sock, job='dispatch-job', expected_worker_epoch=1,
                    expected_generation=1, expected_revision=2, request_id='publication-start') == started
                assert origin.ledger == before_body
                with sqlite3.connect(state / 'state.db') as connection:
                    assert connection.execute("SELECT COUNT(*) FROM events WHERE kind='job_completed'").fetchone()[0] == int(completed_before_crash)
                _finish_publication_worker(cold, publication_evidence)
                cold_rows = [json.loads(line) for line in (publication_evidence / f"fixture-{cold['process'].pid}.jsonl").read_text().splitlines()]
                assert not any(r['kind'].startswith('forbidden-') for r in cold_rows)
                recovery = _spawn_publication_worker(state, origin, 'recovery', publication_evidence)
                children.append(recovery)
                current = ipc.request_jobs_page(sock).jobs[0]
                epoch = ipc.request_health(sock).worker_epoch
                args = dict(job='dispatch-job', expected_worker_epoch=epoch,
                    expected_generation=current.generation, expected_revision=current.revision,
                    request_id='new-crash-recovery')
                result = ipc.dispatch_direct_job(sock, **args)
                final_present = cut in {'link', 'before-fsync', 'after-fsync', 'before-commit', 'after-commit'}
                if final_present:
                    assert result.status == ('blocked' if completed_before_crash else 'pending')
                    assert recovery['entered'].wait(6)
                    assert ipc.dispatch_direct_job(sock, **args) == result
                    recovery['release'].set()
                    _wait_state(sock, 'completed')
                    assert ipc.dispatch_direct_job(sock, **args).state == 'completed'
                else:
                    deadline = time.monotonic() + 5
                    while result.status == 'pending' and time.monotonic() < deadline:
                        result = ipc.dispatch_direct_job(sock, **args)
                        time.sleep(.01)
                    assert result.status == 'blocked'
                    assert not recovery['entered'].is_set()
                assert _receipt(state / 'state.db') == original
                assert origin.ledger == before_body
                partial = root / '.incomplete/dispatch-job/dispatch.bin'
                final = root / 'Other/dispatch.bin'
                cleanup = (_helpers._assert_finished_direct_cleanup(state / 'state.db', partial, final, origin.payload)
                    if final_present else None)
                assert origin.ledger == before_body
                with sqlite3.connect(state / 'state.db') as connection:
                    details = dict(cut=cut, final_present=final_present, original_receipt=original,
                        cleanup_claim=cleanup, partial_exists=partial.exists(),
                        partial_sha256=hashlib.sha256(partial.read_bytes()).hexdigest() if partial.exists() else None,
                        final_sha256=hashlib.sha256(final.read_bytes()).hexdigest() if final.exists() else None,
                        origin_ledger=asdict(origin.ledger), attempts=[tuple(r) for r in connection.execute('SELECT * FROM direct_publication_attempts')],
                        events=[tuple(r) for r in connection.execute('SELECT * FROM events')],
                        final_bindings=[tuple(r) for r in connection.execute('SELECT * FROM final_publication_bindings')])
                    assert sum(r[1] == 'job_completed' for r in details['events']) == int(final_present)
                record = publication_evidence / 'crash-outcome.json'
                record.write_text(json.dumps(details, sort_keys=True))
                record.chmod(0o600)
                if not final_present:
                    assert partial.read_bytes() == origin.payload
                    assert partial.with_name('.hermes-reservation').exists()
                assert final.exists() == final_present
            finally:
                for child in children:
                    _finish_publication_worker(child, publication_evidence)


@pytest.mark.parametrize('retained_final', (True, False))
def test_certified_restart_recovers_only_new_exact_direct_attempt_preserving_legacy(retained_final, publication_evidence):
    from contextlib import closing
    from types import SimpleNamespace
    spec = importlib.util.spec_from_file_location('_publication_legacy_history',
        Path(__file__).with_name('test_legacy_direct_only.py'))
    legacy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(legacy)
    with tempfile.TemporaryDirectory(dir='/private/tmp', prefix='hd-pub-') as temp:
        state = Path(temp) / 'state'
        state.mkdir(mode=0o700)
        sock = state / 'worker.sock'
        database = state / 'state.db'
        root = Path.home() / 'Downloads/Hermes'
        root.mkdir(parents=True, mode=0o700)
        children = []
        def legacy_snapshot():
            with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True) as connection:
                connection.row_factory = sqlite3.Row
                return legacy._snapshot(SimpleNamespace(_connection=connection))
        def direct_job():
            return next(job for job in ipc.request_jobs_page(sock).jobs if job.job == 'dispatch-job')
        with _helpers._origin_type()(payload_size=1024) as origin:
            try:
                barrier = 'post-link' if retained_final else 'cut-permit'
                first, started, original = _start_real_publication(state, origin, barrier,
                    publication_evidence, recover_socket=True)
                children.append(first)
                old_record = (state / '.worker-endpoint.json').read_bytes()
                assert json.loads(old_record)['schema'] == 1
                if retained_final:
                    assert ipc.control_job(sock, job='dispatch-job', action='pause',
                        request_id='certified-manual-hold', expected_revision=5).status == 'applied'
                _close_fixture_process(first['process'], publication_evidence, crash=True)
                assert sock.exists() and (state / '.worker-endpoint.json').read_bytes() == old_record
                with closing(SQLiteStore(database)) as store:
                    legacy._seed(store, state='finalizing', dispatch=True)
                history = legacy_snapshot()
                before_body = origin.ledger
                cold = _spawn_publication_worker(state, origin, 'cold', publication_evidence,
                    recover_socket=True)
                children.append(cold)
                current = direct_job()
                assert current.state == 'paused'
                assert ipc.request_health(sock).queue_gate == 'paused'
                assert ipc.request_health(sock).worker_epoch == 2
                assert json.loads((state / '.worker-endpoint.json').read_bytes())['worker_epoch'] == 2
                assert not cold['entered'].is_set()
                assert legacy_snapshot() == history
                assert _receipt(database) == original and origin.ledger == before_body
                with sqlite3.connect(database) as connection:
                    assert connection.execute('PRAGMA user_version').fetchone()[0] == 20
                    assert connection.execute("SELECT manual_hold FROM materialized_jobs WHERE job_id='dispatch-job'").fetchone()[0] == int(retained_final)
                    assert connection.execute('SELECT COUNT(*) FROM final_publication_bindings WHERE job_id=\'dispatch-job\'').fetchone()[0] == 0
                    assert connection.execute("SELECT COUNT(*) FROM events WHERE kind='job_completed'").fetchone()[0] == 0
                    pointer = connection.execute("SELECT audit_id, generation, revision, state, worker_epoch FROM direct_publication_attempts WHERE job_id='dispatch-job'").fetchone()
                    latest = connection.execute("SELECT event_id FROM events WHERE job_id='dispatch-job' ORDER BY event_id DESC LIMIT 1").fetchone()[0]
                    assert pointer == (latest, current.generation, current.revision, 'paused', 2)
                _finish_publication_worker(cold, publication_evidence)
                assert not sock.exists() and not (state / '.worker-endpoint.json').exists()
                rows = [json.loads(line) for line in (publication_evidence / f"fixture-{cold['process'].pid}.jsonl").read_text().splitlines()]
                assert not any(row['kind'].startswith('forbidden-') for row in rows)
                recovery = _spawn_publication_worker(state, origin, 'recovery', publication_evidence,
                    recover_socket=True)
                children.append(recovery)
                current = direct_job()
                epoch = ipc.request_health(sock).worker_epoch
                assert epoch == 3 and current.state == 'paused'
                # Old fences cannot initiate verification. The original receipt remains replayable.
                assert ipc.dispatch_direct_job(sock, job='dispatch-job', expected_worker_epoch=1,
                    expected_generation=1, expected_revision=2, request_id='publication-start') == started
                assert not recovery['entered'].is_set()
                assert ipc.dispatch_direct_job(sock, job='dispatch-job', expected_worker_epoch=epoch-1,
                    expected_generation=current.generation, expected_revision=current.revision,
                    request_id='certified-stale-recovery').status == 'stale'
                assert not recovery['entered'].is_set()
                arguments = dict(job='dispatch-job', expected_worker_epoch=epoch,
                    expected_generation=current.generation, expected_revision=current.revision,
                    request_id='certified-new-recovery')
                result = ipc.dispatch_direct_job(sock, **arguments)
                if retained_final:
                    assert result.status == 'pending' and recovery['entered'].wait(6)
                    assert ipc.dispatch_direct_job(sock, **arguments) == result
                    recovery['release'].set()
                deadline = time.monotonic() + 6
                while result.status == 'pending' and time.monotonic() < deadline:
                    result = ipc.dispatch_direct_job(sock, **arguments)
                    time.sleep(.01)
                assert (result.status, result.state) == (('started', 'completed') if retained_final else ('blocked', 'paused'))
                assert _receipt(database) == original and origin.ledger == before_body
                assert legacy_snapshot() == history
                assert ipc.request_health(sock).queue_gate == 'paused'
                partial = root / '.incomplete/dispatch-job/dispatch.bin'
                final = root / 'Other/dispatch.bin'
                cleanup = (_helpers._assert_finished_direct_cleanup(database, partial, final, origin.payload)
                    if retained_final else None)
                assert _receipt(database) == original and origin.ledger == before_body
                assert legacy_snapshot() == history
                with sqlite3.connect(database) as connection:
                    assert connection.execute("SELECT manual_hold FROM materialized_jobs WHERE job_id='dispatch-job'").fetchone()[0] == int(retained_final)
                    assert connection.execute("SELECT COUNT(*) FROM events WHERE kind='job_completed'").fetchone()[0] == int(retained_final)
                    assert connection.execute("SELECT COUNT(*) FROM final_publication_bindings WHERE job_id='dispatch-job'").fetchone()[0] == int(retained_final)
                    assert connection.execute('SELECT COUNT(*) FROM engine_instances').fetchone()[0] == 0
                    details = dict(retained_final=retained_final, original_receipt=original,
                        cleanup_claim=cleanup, partial_exists=partial.exists(),
                        partial_sha256=hashlib.sha256(partial.read_bytes()).hexdigest() if partial.exists() else None,
                        final_sha256=hashlib.sha256(final.read_bytes()).hexdigest() if final.exists() else None,
                        recovery_result=result.to_record(), origin_ledger=asdict(origin.ledger),
                        legacy_history_unchanged=True,
                        attempts=[tuple(row) for row in connection.execute('SELECT * FROM direct_publication_attempts')])
                (publication_evidence / 'certified-restart-outcome.json').write_text(json.dumps(details))
                (publication_evidence / 'certified-restart-outcome.json').chmod(0o600)
                if not retained_final:
                    assert partial.read_bytes() == origin.payload
                    assert partial.with_name('.hermes-reservation').exists()
                assert final.exists() == retained_final
            finally:
                for child in children:
                    _finish_publication_worker(child, publication_evidence)
            assert not sock.exists() and not (state / '.worker-endpoint.json').exists()
