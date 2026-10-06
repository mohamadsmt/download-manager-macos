"""One real current producer and existing-command success cleanup, privately."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import uuid

import pytest
from hermes_downloads import ipc
from test_add_batch import entry
import test_direct_publication as publication


def _cleanup_worker(state, sock, ready, shutdown, stopped, results, origin,
                    evidence, mode, entered, release):
    from hermes_downloads import direct, network, paths, store
    from hermes_downloads.retry import CompletionVerification
    owner = threading.get_ident()
    evidence = Path(evidence)
    ledger = evidence / 'cleanup-effects.jsonl'
    def record(kind, **values):
        publication._append_record(ledger, kind, **values)
    original_join = threading.Thread.join
    def joined(thread, *args, **kwargs):
        result = original_join(thread, *args, **kwargs)
        if thread.name.startswith('direct-') and thread.name.endswith('-observation'):
            assert threading.get_ident() == owner
            assert not thread.is_alive()
            record('actual-direct-observer-joined', name=thread.name, ident=thread.ident,
                native_id=thread.native_id, returned=True, surviving=False)
        return result
    threading.Thread.join = joined
    def hold(kind):
        record(kind, owner_thread=threading.get_ident() == owner)
        entered.set()
        if mode and mode.startswith('cut-'):
            release_file = evidence / 'release-cut'
            deadline = time.monotonic() + 12
            while not release_file.exists() and time.monotonic() < deadline:
                time.sleep(.01)
            assert release_file.exists(), 'bounded cleanup crash barrier expired'
        else:
            assert release.wait(12), 'bounded cleanup barrier expired'
    grant = network.LocalOriginGrant.for_url(origin)
    validate = store.validate_source_url
    store.validate_source_url = lambda value: validate(value, local_origin_grant=grant)
    original_prepare = paths.prepare_publication_payload
    def prepare(*args, **kwargs):
        prepared = original_prepare(*args, **kwargs)
        sidecar = prepared.destination.incomplete_dir / 'unknown.sidecar'
        sidecar.write_bytes(b'unknown artifact is preserved')
        sidecar.chmod(0o600)
        record('prepared-current-producer', sha256=prepared.sha256,
            source='actual stable descriptor', unknown_sidecar=str(sidecar))
        if mode == 'corrupt-intent':
            with sqlite3.connect(Path(state) / 'state.db') as connection:
                connection.execute('UPDATE add_batch_entries SET creation_intent_blob=CAST("{}" AS BLOB)')
        return prepared
    paths.prepare_publication_payload = prepare
    original_add = direct.DirectAria2Controller.add_paused
    def add(self, *args, **kwargs):
        assert kwargs['expected_sha256'] is None
        record('engine-add-transport-only', expected_sha256=None)
        return original_add(self, *args, **kwargs)
    direct.DirectAria2Controller.add_paused = add
    original_unlink = paths.os.unlink
    def unlink(name, *args, **kwargs):
        directory = kwargs.get('dir_fd')
        job_dir = Path.home() / 'Downloads/Hermes/.incomplete/cleanup-job'
        tracked = False
        if directory is not None and job_dir.exists():
            actual, expected_dir = os.fstat(directory), job_dir.stat()
            tracked = (actual.st_dev, actual.st_ino) == (expected_dir.st_dev, expected_dir.st_ino)
        kind = 'partial' if tracked and name == 'payload.bin' else (
            'marker' if tracked and name == '.hermes-reservation' else None)
        if kind:
            if mode == 'cold-recovery':
                assert release.is_set(), 'cold startup attempted cleanup unlink'
            phase = _rows(Path(state) / 'state.db', 'SELECT phase FROM direct_cleanup_claims')[0][0]
            assert phase == ('pending' if kind == 'partial' else 'certified')
            record('cleanup-' + kind + '-unlink-enter')
            if mode == 'held-unlink' and kind == 'partial':
                hold('held-partial-unlink')
        result = original_unlink(name, *args, **kwargs)
        if kind:
            record('cleanup-' + kind + '-unlink-exit')
            if mode == 'cut-' + kind:
                hold('cut-after-' + kind + '-unlink')
        return result
    paths.os.unlink = unlink
    original_hash = paths._hash_publication_payload
    def hashed(descriptor, expected, **kwargs):
        root = Path.home() / 'Downloads/Hermes'
        cleanup = expected[4] == 1 and (root / 'Other/payload.bin').exists() and not (
            root / '.incomplete/cleanup-job/payload.bin').exists()
        if cleanup:
            if mode == 'cold-recovery':
                assert release.is_set(), 'cold startup attempted cleanup hash'
            record('cleanup-fresh-hash-enter', stat=list(expected))
            if mode == 'held-hash':
                hold('held-post-unlink-hash')
        try:
            result = original_hash(descriptor, expected, **kwargs)
        finally:
            if cleanup:
                record('cleanup-fresh-hash-exit',
                    cancelled=bool(kwargs.get('cancelled') and kwargs['cancelled']()))
        return result
    paths._hash_publication_payload = hashed
    original_sync = paths._fsync_staged_partial_directory
    def sync(descriptor):
        final = Path.home() / 'Downloads/Hermes/Other/payload.bin'
        cleanup = final.exists() and final.stat().st_nlink == 1
        if cleanup:
            phase = _rows(Path(state) / 'state.db', 'SELECT phase FROM direct_cleanup_claims')[0][0]
            record('cleanup-directory-fsync-enter', phase=phase)
            if mode == 'held-fsync' and phase == 'pending':
                hold('held-post-unlink-fsync')
        result = original_sync(descriptor)
        if cleanup:
            record('cleanup-directory-fsync-exit', phase=phase)
        return result
    paths._fsync_staged_partial_directory = sync
    if hasattr(store.SQLiteStore, '_mint_direct_cleanup_claim'):
        original_mint = store.SQLiteStore._mint_direct_cleanup_claim
        def mint(self, connection, finished, published, verification, cache):
            assert threading.get_ident() == owner
            assert type(verification) is CompletionVerification
            record('actual-owner-derived-verification', verification=verification.value,
                actual_sha256=published.sha256)
            result = original_mint(self, connection, finished, published, verification, cache)
            if mode == 'mint-fault':
                raise sqlite3.OperationalError('finite injected claim-readback fault')
            return result
        store.SQLiteStore._mint_direct_cleanup_claim = mint
    if hasattr(store.SQLiteStore, 'activate_direct_cleanup'):
        original_activate = store.SQLiteStore.activate_direct_cleanup
        def activate(self, plan):
            assert threading.get_ident() == owner
            assert self.get_direct_engine_record() is None
            assert self.get_direct_engine_activation_fence() is None
            assert not any(t.name.startswith('direct-') and t.name.endswith('-observation')
                for t in threading.enumerate())
            observations = [json.loads(line) for line in ledger.read_text().splitlines()]
            joined_names = {row['name'] for row in observations
                if row['kind'] == 'actual-direct-observer-joined'}
            assert {'direct-stage-observation', 'direct-publication-observation'} <= joined_names or mode == 'cold-recovery'
            closures = [json.loads(line) for line in (evidence / f'fixture-{os.getpid()}.jsonl').read_text().splitlines()]
            assert any(row['kind'] == 'engine-waitable-child-reaped' for row in closures) or mode == 'cold-recovery'
            if mode == 'cold-recovery':
                assert release.is_set(), 'cold startup attempted cleanup activation'
            namespace = plan.claim.attempt.prepared.destination
            names = (namespace.partial_path, namespace.final_path,
                namespace.incomplete_dir / '.hermes-reservation', namespace.incomplete_dir / 'unknown.sidecar')
            snapshots = [dict(path=str(path), dev=(item := path.stat()).st_dev, ino=item.st_ino,
                nlink=item.st_nlink, logical=item.st_size, blocks=item.st_blocks) for path in names if path.exists()]
            record('activation-after-owned-reap-claim-retirement-observer-join', namespace=snapshots,
                unique_inode_blocks=sum({(row['dev'], row['ino']): row['blocks'] for row in snapshots}.values()))
            return original_activate(self, plan)
        store.SQLiteStore.activate_direct_cleanup = activate
    for name in ('certify_direct_cleanup', 'finish_direct_cleanup'):
        if hasattr(store.SQLiteStore, name):
            original = getattr(store.SQLiteStore, name)
            def phase(self, *args, _original=original, _name=name):
                assert threading.get_ident() == owner
                result = _original(self, *args)
                record('durable-' + _name, phase=_rows(Path(state) / 'state.db',
                    'SELECT phase FROM direct_cleanup_claims')[0][0])
                return result
            setattr(store.SQLiteStore, name, phase)
    if mode in {'certify-fault', 'finish-fault'}:
        name = 'certify_direct_cleanup' if mode == 'certify-fault' else 'finish_direct_cleanup'
        def fail(self, *args):
            record('injected-' + mode)
            entered.set()
            raise sqlite3.OperationalError('finite injected cleanup phase fault')
        setattr(store.SQLiteStore, name, fail)
    if mode == 'cold-recovery':
        def forbidden(*args, **kwargs):
            record('forbidden-cold-effect')
            raise AssertionError('cold/list/add cannot perform cleanup or engine effects')
        direct.DirectAria2Controller.start = forbidden
        direct.DirectAria2Controller.add_paused = forbidden
        direct.DirectAria2Controller.resume = forbidden
    publication._publication_worker(state, sock, ready, shutdown, stopped, results,
        origin, None, entered, release, str(evidence), recover_socket=True)


def _rows(database, query, parameters=()):
    with sqlite3.connect(database) as connection:
        return [tuple(row) for row in connection.execute(query, parameters)]


def _wait(predicate, *, seconds=6):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.01)
    pytest.fail('bounded real producer did not reach its expected effect')


@contextmanager
def real_cleanup_producer(*, expected='match', mode=None, payload_size=1024):
    """Typed add_batch, private unmanaged start, and the actual stock engine."""
    parent = Path(os.environ.get('T19_CLEANUP_RUN', tempfile.gettempdir()))
    evidence = parent / ('cleanup-' + uuid.uuid4().hex)
    evidence.mkdir(mode=0o700)
    with tempfile.TemporaryDirectory(dir='/private/tmp', prefix='hd-clean-') as temporary:
        state = Path(temporary)
        state.chmod(0o700)
        root = Path.home() / 'Downloads/Hermes'
        root.mkdir(parents=True, mode=0o700)
        context = multiprocessing.get_context('spawn')
        ready, shutdown, stopped, entered, release = [context.Event() for _ in range(5)]
        results = context.Queue()
        with publication._helpers._origin_type()(payload_size=payload_size) as origin:
            origin_thread = origin._thread
            assert origin_thread is not None and origin_thread.is_alive()
            publication._append_record(evidence / 'origin.jsonl', 'local-origin-thread-birth',
                owner_pid=os.getpid(), thread_name=origin_thread.name,
                thread_ident=origin_thread.ident, native_id=origin_thread.native_id,
                address=origin.origin, ownership='new finite synthetic fixture')
            digest = hashlib.sha256(origin.payload).hexdigest()
            expected_digest = digest if expected == 'match' else (
                hashlib.sha256(origin.changed_payload).hexdigest() if expected == 'wrong' else None)
            process = context.Process(target=_cleanup_worker,
                args=(str(state), str(state / 'worker.sock'), ready, shutdown, stopped,
                    results, origin.url(), str(evidence), mode, entered, release))
            process.start()
            fixture = dict(state=state, database=state / 'state.db', sock=state / 'worker.sock',
                root=root, partial=root / '.incomplete/cleanup-job/payload.bin',
                final=root / 'Other/payload.bin', marker=root / '.incomplete/cleanup-job/.hermes-reservation',
                process=process, shutdown=shutdown, stopped=stopped, entered=entered,
                release=release, results=results, origin=origin, evidence=evidence, digest=digest)
            fixture['children'] = [(process, shutdown, release, results)]
            try:
                assert ready.wait(6), 'owned worker did not become ready'
                assert ipc.request_health(fixture['sock']).queue_gate == 'paused'
                result = ipc.add_batch(fixture['sock'], request_id='cleanup-create', collection=None,
                    entries=[entry(job='cleanup-job', source_url=origin.url('/no-range'),
                        partial_filename='payload.bin', selected_final_filename='payload.bin',
                        expected_sha256=expected_digest)])
                assert result.results[0].status == 'applied'
                original_blob = _rows(fixture['database'], 'SELECT creation_intent_blob FROM add_batch_entries')[0][0]
                publication._append_record(evidence / 'producer.jsonl', 'actual-typed-batch-creation',
                    immutable_blob_sha256=hashlib.sha256(original_blob).hexdigest(),
                    expected_sha256=expected_digest, actual_payload_sha256=digest,
                    reply=result.to_record())
                assert origin.ledger.request_count == 0
                assert _rows(fixture['database'], 'SELECT count(*) FROM job_authorization_heads') == [(0,)]
                assert ipc.set_queue_gate(fixture['sock'], gate='running',
                    request_id='cleanup-open', expected_revision=1).applied
                current = ipc.request_jobs_page(fixture['sock']).jobs[0]
                authorized = ipc.control_job(fixture['sock'], job='cleanup-job', action='start_now',
                    request_id='cleanup-authorize', expected_revision=current.revision)
                assert authorized.status == 'applied'
                assert ipc.activate_direct_engine(fixture['sock'], expected_worker_epoch=1).status == 'active'
                fixture['started'] = ipc.dispatch_direct_job(fixture['sock'], job='cleanup-job',
                    expected_worker_epoch=1, expected_generation=authorized.generation,
                    expected_revision=authorized.revision, request_id='cleanup-start')
                assert fixture['started'].status == 'started'
                fixture['original_receipt'] = _rows(fixture['database'],
                    'SELECT * FROM direct_dispatch_commands WHERE request_id=?', ('cleanup-start',))[0]
                publication._append_record(evidence / 'producer.jsonl', 'actual-started-receipt',
                    receipt=fixture['original_receipt'], dispatch=fixture['started'].to_record())
                yield fixture
            finally:
                for child, child_shutdown, child_release, child_results in fixture['children']:
                    if child.is_alive():
                        (evidence / 'release-cut').touch(mode=0o600)
                        child_release.set()
                        child_shutdown.set()
                    publication._close_fixture_process(child, evidence)
                    if child.exitcode == 0:
                        result = child_results.get(timeout=1)
                        assert result[0] == 'result', result
                publication._append_record(evidence / 'origin.jsonl', 'local-origin-final',
                    ledger=asdict(origin.ledger), sha256=digest,
                    synthetic=True, public_live_acceptance=False)
                origin.close()
                assert not origin_thread.is_alive()
                publication._append_record(evidence / 'origin.jsonl', 'local-origin-thread-joined',
                    owner_pid=os.getpid(), thread_ident=origin_thread.ident,
                    native_id=origin_thread.native_id, joined=True, surviving=False)


def test_actual_new_completion_removes_only_owned_partial_and_marker():
    with real_cleanup_producer() as fixture:
        current = _wait(lambda: (job if (job := ipc.request_jobs_page(fixture['sock']).jobs[0]).state
            == 'completed' else None))
        final = fixture['final']
        assert final.read_bytes() == fixture['origin'].payload
        assert hashlib.sha256(final.read_bytes()).hexdigest() == fixture['digest']
        _wait(lambda: not fixture['partial'].exists(), seconds=2)
        _wait(lambda: not fixture['marker'].exists(), seconds=2)
        _wait(lambda: _rows(fixture['database'], 'SELECT phase FROM direct_cleanup_claims') == [('finished',)])
        _wait(lambda: any(row['kind'] == 'durable-finish_direct_cleanup' for row in _effect_rows(fixture)))
        assert final.stat().st_nlink == 1
        assert _rows(fixture['database'], "SELECT count(*) FROM events WHERE kind='job_completed'") == [(1,)]
        assert _rows(fixture['database'], 'SELECT * FROM direct_dispatch_commands WHERE request_id=?',
            ('cleanup-start',))[0] == fixture['original_receipt']
        assert _rows(fixture['database'], 'SELECT phase FROM direct_cleanup_claims') == [('finished',)]
        assert _rows(fixture['database'], "SELECT status FROM direct_publication_attempts") == [('finished',)]
        publication._append_record(fixture['evidence'] / 'producer.jsonl', 'actual-current-completion',
            claim=_rows(fixture['database'], 'SELECT * FROM direct_cleanup_claims')[0],
            attempt=_rows(fixture['database'], 'SELECT * FROM direct_publication_attempts')[0],
            events=_rows(fixture['database'], 'SELECT * FROM events WHERE job_id="cleanup-job"'),
            started_receipt=fixture['original_receipt'])
        assert current.state == 'completed'
        assert (fixture['partial'].parent / 'unknown.sidecar').read_bytes() == b'unknown artifact is preserved'
        assert fixture['origin'].ledger.request_count == 1
        assert fixture['origin'].ledger.response_body_bytes == len(fixture['origin'].payload)
        rows = _effect_rows(fixture)
        activation = next(row for row in rows if row['kind'].startswith('activation-after'))
        payload_names = [row for row in activation['namespace'] if row['path'].endswith('payload.bin')]
        assert len(payload_names) == 2
        assert len({(row['dev'], row['ino']) for row in payload_names}) == 1
        assert all(row['nlink'] == 2 for row in payload_names)
        assert final.stat().st_blocks == payload_names[0]['blocks']
        retained = [final, fixture['partial'].parent / 'unknown.sidecar', fixture['partial'].parent]
        physical = [dict(path=str(path), dev=(item := path.stat()).st_dev, ino=item.st_ino,
            logical=item.st_size, blocks=item.st_blocks, nlink=item.st_nlink) for path in retained]
        publication._append_record(fixture['evidence'] / 'cleanup-effects.jsonl', 'physical-after-cleanup',
            namespace=physical,
            unique_inode_blocks=sum({(row['dev'], row['ino']): row['blocks'] for row in physical}.values()),
            payload_blocks_before=payload_names[0]['blocks'], payload_blocks_after=final.stat().st_blocks,
            payload_allocation_reclaimed=False, block_unit_bytes=512)
        rows = _effect_rows(fixture)
        kinds = [row['kind'] for row in rows]
        assert kinds.index('cleanup-partial-unlink-exit') < kinds.index('cleanup-fresh-hash-enter')
        assert kinds.index('cleanup-directory-fsync-exit') < kinds.index('cleanup-fresh-hash-enter')
        assert kinds.index('durable-certify_direct_cleanup') < kinds.index('cleanup-marker-unlink-enter')
        assert kinds.index('cleanup-marker-unlink-exit') < kinds.index('durable-finish_direct_cleanup')
        before = final.stat()
        replay = ipc.dispatch_direct_job(fixture['sock'], job='cleanup-job', expected_worker_epoch=1,
            expected_generation=fixture['started'].generation, expected_revision=1,
            request_id='cleanup-start')
        assert replay == fixture['started']
        assert final.stat() == before
        assert _effect_rows(fixture) == rows


def _effect_rows(fixture):
    path = fixture['evidence'] / 'cleanup-effects.jsonl'
    return [json.loads(line) for line in path.read_text().splitlines()]


def _restart_cold(fixture):
    context = multiprocessing.get_context('spawn')
    ready, shutdown, stopped, entered, release = [context.Event() for _ in range(5)]
    results = context.Queue()
    process = context.Process(target=_cleanup_worker,
        args=(str(fixture['state']), str(fixture['sock']), ready, shutdown, stopped, results,
            fixture['origin'].url(), str(fixture['evidence']), 'cold-recovery', entered, release))
    process.start()
    fixture['children'].append((process, shutdown, release, results))
    assert ready.wait(6)
    return dict(process=process, entered=entered, release=release)


@pytest.mark.parametrize('mode,phase,marker_present', [
    ('cut-partial', 'pending', True), ('cut-marker', 'certified', False)])
def test_crash_cut_recovers_only_new_request_after_inert_cold_restart(mode, phase, marker_present):
    with real_cleanup_producer(mode=mode) as fixture:
        assert fixture['entered'].wait(6)
        assert not fixture['partial'].exists()
        assert fixture['marker'].exists() == marker_present
        assert fixture['final'].stat().st_nlink == 1
        assert _rows(fixture['database'], 'SELECT phase FROM direct_cleanup_claims') == [(phase,)]
        protected_queries = ['SELECT * FROM direct_publication_attempts',
            'SELECT * FROM direct_dispatch_commands WHERE request_id="cleanup-start"',
            'SELECT * FROM closed_direct_publication_attempts',
            'SELECT * FROM publication_reservations WHERE job_id="cleanup-job"',
            'SELECT * FROM publication_marker_bindings WHERE job_id="cleanup-job"',
            'SELECT * FROM staged_payload_bindings WHERE job_id="cleanup-job"',
            'SELECT * FROM final_publication_bindings WHERE job_id="cleanup-job"',
            'SELECT * FROM events WHERE job_id="cleanup-job"']
        before = [_rows(fixture['database'], query) for query in protected_queries]
        namespace = {path: path.stat() for path in (fixture['final'], fixture['marker']) if path.exists()}
        body = fixture['origin'].ledger
        publication._close_fixture_process(fixture['process'], fixture['evidence'], crash=True)
        cold = _restart_cold(fixture)
        assert ipc.request_health(fixture['sock']).queue_gate == 'paused'
        assert ipc.request_health(fixture['sock']).worker_epoch == 2
        current = ipc.request_jobs_page(fixture['sock']).jobs[0]
        assert current.state == 'completed'
        added = ipc.add_batch(fixture['sock'], request_id='cold-add-only', collection=None,
            entries=[entry(1, source_url=fixture['origin'].url('/no-range'))])
        assert added.results[0].status == 'applied'
        replay = ipc.dispatch_direct_job(fixture['sock'], job='cleanup-job', expected_worker_epoch=1,
            expected_generation=fixture['started'].generation, expected_revision=1,
            request_id='cleanup-start')
        assert replay == fixture['started']
        assert namespace == {path: path.stat() for path in namespace}
        assert _rows(fixture['database'], 'SELECT phase FROM direct_cleanup_claims') == [(phase,)]
        assert fixture['origin'].ledger == body
        cold['release'].set()
        result = ipc.dispatch_direct_job(fixture['sock'], job='cleanup-job', expected_worker_epoch=2,
            expected_generation=current.generation, expected_revision=current.revision,
            request_id='cleanup-cold-new')
        assert (result.status, result.state) == ('blocked', 'completed')
        _wait(lambda: _rows(fixture['database'], 'SELECT phase FROM direct_cleanup_claims') == [('finished',)])
        _wait(lambda: any(row['kind'] == 'durable-finish_direct_cleanup' for row in _effect_rows(fixture)))
        assert not fixture['partial'].exists() and not fixture['marker'].exists()
        assert fixture['final'].read_bytes() == fixture['origin'].payload
        assert fixture['origin'].ledger == body
        assert [_rows(fixture['database'], query) for query in protected_queries] == before
        rows = _effect_rows(fixture)
        replay = ipc.dispatch_direct_job(fixture['sock'], job='cleanup-job', expected_worker_epoch=2,
            expected_generation=current.generation, expected_revision=current.revision,
            request_id='cleanup-cold-new')
        assert replay == result
        assert _effect_rows(fixture) == rows


@pytest.mark.parametrize('mode,phase,partial_present,marker_present', [
    ('mint-fault', None, True, True), ('certify-fault', 'pending', False, True),
    ('finish-fault', 'certified', False, False)])
def test_actual_completion_and_cleanup_faults_keep_truthful_atomic_phase(mode, phase, partial_present, marker_present):
    with real_cleanup_producer(mode=mode) as fixture:
        if mode == 'mint-fault':
            _wait(lambda: ipc.request_jobs_page(fixture['sock']).jobs[0].state == 'paused')
        else:
            assert fixture['entered'].wait(6)
        assert fixture['partial'].exists() == partial_present
        assert fixture['marker'].exists() == marker_present
        assert fixture['final'].read_bytes() == fixture['origin'].payload
        assert _rows(fixture['database'], 'SELECT phase FROM direct_cleanup_claims') == ([] if phase is None else [(phase,)])
        if phase is None:
            assert _rows(fixture['database'], 'SELECT count(*) FROM final_publication_bindings') == [(0,)]
        assert _rows(fixture['database'], "SELECT count(*) FROM events WHERE kind='job_completed'") == [(0 if phase is None else 1,)]
        assert _rows(fixture['database'], 'SELECT * FROM direct_dispatch_commands WHERE request_id=?',
            ('cleanup-start',))[0] == fixture['original_receipt']
        assert (fixture['partial'].parent / 'unknown.sidecar').exists()


@pytest.mark.parametrize('mode', ['held-unlink', 'held-hash', 'held-fsync'])
def test_control_waits_for_held_unlink_and_discards_cancelled_late_hash(mode):
    with real_cleanup_producer(mode=mode) as fixture:
        assert fixture['entered'].wait(6)
        started = time.monotonic()
        assert ipc.request_health(fixture['sock']).worker_epoch == 1
        assert ipc.request_jobs_page(fixture['sock']).jobs[0].state == 'completed'
        if mode == 'held-unlink':
            with pytest.raises(ipc.IPCError):
                ipc.set_queue_gate(fixture['sock'], gate='paused', request_id='cleanup-cancel', expected_revision=2)
            assert time.monotonic() - started < 2
            assert ipc.request_health(fixture['sock']).queue_gate == 'running'
            assert fixture['partial'].exists()
            fixture['release'].set()
            _wait(lambda: not fixture['partial'].exists())
            result = ipc.set_queue_gate(fixture['sock'], gate='paused',
                request_id='cleanup-cancel', expected_revision=2)
            assert result.applied
        else:
            assert not fixture['partial'].exists() and fixture['marker'].exists()
            result = ipc.set_queue_gate(fixture['sock'], gate='paused',
                request_id='cleanup-cancel', expected_revision=2)
            assert result.applied and time.monotonic() - started < 2
            fixture['release'].set()
            cut = 'cleanup-fresh-hash-exit' if mode == 'held-hash' else 'cleanup-directory-fsync-exit'
            _wait(lambda: any(row['kind'] == cut for row in _effect_rows(fixture)))
            assert _rows(fixture['database'], 'SELECT phase FROM direct_cleanup_claims') == [('pending',)]
            assert fixture['marker'].exists()
            assert not any(row['kind'] == 'durable-certify_direct_cleanup' for row in _effect_rows(fixture))
            assert not any(row['kind'] == 'cleanup-marker-unlink-enter' for row in _effect_rows(fixture))
        assert fixture['final'].read_bytes() == fixture['origin'].payload


def test_restored_mtime_same_size_rewrite_never_certifies_or_removes_unknown_sidecar():
    with real_cleanup_producer(mode='held-hash') as fixture:
        assert fixture['entered'].wait(6)
        before = fixture['final'].stat()
        fixture['final'].write_bytes(fixture['origin'].changed_payload)
        os.utime(fixture['final'], ns=(before.st_atime_ns, before.st_mtime_ns))
        assert fixture['final'].stat().st_size == before.st_size
        assert fixture['final'].stat().st_mtime_ns == before.st_mtime_ns
        fixture['release'].set()
        _wait(lambda: any(row['kind'] == 'cleanup-fresh-hash-exit' for row in _effect_rows(fixture)))
        assert _rows(fixture['database'], 'SELECT phase FROM direct_cleanup_claims') == [('pending',)]
        assert fixture['marker'].exists()
        assert (fixture['partial'].parent / 'unknown.sidecar').read_bytes() == b'unknown artifact is preserved'
        assert ipc.request_jobs_page(fixture['sock']).jobs[0].state == 'completed'
