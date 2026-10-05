"""Real SQLite and installed AF_UNIX batch creation, on private fixtures only."""
import hashlib
from contextlib import closing
import json
import os
from pathlib import Path
import signal
import sqlite3
import socket
import subprocess
import sys
import tempfile
import threading
import time

import pytest
from hermes_downloads import ipc
from hermes_downloads.store import SQLiteStore
from hermes_downloads.models import DownloadIntent


def entry(index=0, **changes):
    value = dict(job=f'batch-job-{index}', source_kind='direct',
        source_url=f'https://example.test/file-{index}?signed=unchanged', priority=0,
        category='Other', partial_filename=f'file-{index}.bin',
        selected_final_filename=f'file-{index}.bin', expected_sha256=None)
    value.update(changes)
    return value


def envelope(entries=None, **changes):
    value = dict(op='add_batch', protocol_version=2, request_id='batch-parent',
        start=False, collection=None, entries=[entry()] if entries is None else entries)
    value.update(changes)
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
        ensure_ascii=True, allow_nan=False).encode('ascii')


def exchange(root, payload, *, framed=True):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(5)
        connection.connect(str(root / 'worker.sock'))
        connection.sendall(b'HDM2\n' + len(payload).to_bytes(4, 'big') + payload
            if framed else payload + b'\n')
        connection.shutdown(socket.SHUT_WR)
        output = bytearray()
        while True:
            chunk = connection.recv(8192)
            if not chunk:
                break
            output.extend(chunk)
            assert len(output) <= 256 * 1024 + 9
    if output.startswith(b'HDM2\n'):
        assert len(output) == 9 + int.from_bytes(output[5:9], 'big')
        return json.loads(output[9:])
    return json.loads(output)


def ledger(record):
    path = os.environ.get('T17_PROCESS_LOG')
    if path:
        with open(path, 'a') as stream:
            stream.write(json.dumps(record) + '\n')


@pytest.fixture
def service():
    # Actual installed entry point and certificate. Never a user state directory.
    with tempfile.TemporaryDirectory(prefix='t17-', dir='/private/tmp') as directory:
        root = Path(directory); root.chmod(0o700)
        process = subprocess.Popen([str(Path(sys.executable).parent / 'hermes-downloads-worker'),
            '--serve', '--state-root', str(root)], cwd=root, env=os.environ.copy(),
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True)
        birth = subprocess.check_output(['/bin/ps', '-p', str(process.pid),
            '-o', 'pid=,ppid=,pgid=,lstart=,command='], text=True).strip()
        ledger(dict(kind='batch-service-birth', pid=process.pid, pgid=process.pid, birth=birth))
        try:
            deadline = time.monotonic() + 4
            while True:
                assert process.poll() is None, process.communicate()[1]
                try:
                    health = ipc.request_health(root / 'worker.sock')
                    break
                except ipc.IPCError:
                    assert time.monotonic() < deadline
                    threading.Event().wait(0.02)
            assert health.queue_gate == 'paused'
            assert (root / '.worker-endpoint.json').is_file()
            yield root, process
        finally:
            if process.poll() is None:
                assert os.getpgid(process.pid) == process.pid
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=3)
            absent = False
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                absent = True
            ledger(dict(kind='batch-service-closure', pid=process.pid, pgid=process.pid,
                returncode=process.returncode, reaped=True, group_absent=absent))
            assert absent


def test_real_v2_add_creates_one_inactive_job(service):
    root, _ = service
    reply = exchange(root, canonical(envelope()))
    assert reply.get('status') == 'applied', reply
    assert reply['readback_kind'] == 'creation_receipt'
    assert reply['replayed'] is False
    assert reply['results'][0]['job'] == 'batch-job-0'
    with closing(SQLiteStore(root / 'state.db')) as store:
        job = store.get_materialized_job('batch-job-0')
        assert job.intent.generation == job.intent.revision == 0
        assert not job.authorized and not job.manual_hold and not job.start_now_requested
        assert job.scheduled_for is None
        creation = store.get_batch_creation_intent(job.job_id)
        assert creation.original_job == job and creation.expected_sha256 is None


def evidence(name, value):
    root = os.environ.get('T17_IMPLEMENTATION_RUN')
    if root:
        path = Path(root) / name
        with path.open('a') as stream:
            stream.write(json.dumps(value, sort_keys=True) + '\n')
        path.chmod(0o600)


def worst_entries():
    values = []
    for index in range(500):
        job = 'J' + 'x' * 123 + f'{index:04d}'
        filename = '\u00a1' * 125 + '"' + f'{index:04d}'
        prefix = 'https://example.test/'
        suffix = f'?signature={index:04d}'
        remaining = 8192 - len((prefix + suffix).encode('utf-8'))
        url = prefix + '\u00a1' * (remaining // 2) + 'a' * (remaining % 2) + suffix
        assert len(url.encode('utf-8')) == 8192
        values.append(entry(index, job=job, source_url=url, priority=-(1 << 31),
            category='Documents', partial_filename=filename, selected_final_filename=filename,
            expected_sha256=f'{index:064x}'))
    return values


def test_real_500_worst_encoded_batch_fit_timing_and_replay(service, tmp_path):
    root, _ = service
    values = worst_entries(); collection = '\u00a1' * 127 + '"'
    parent = 'P' + 'x' * 127
    assert len(parent) == len(parent.encode('utf-8')) == 128
    started = time.monotonic()
    command = ipc.AddBatchCommand(parent, collection, values)
    encoded_time = time.monotonic() - started
    entry_sizes = [len(canonical(item)) for item in values]
    assert max(entry_sizes) < 32768
    assert len(command._wire_request) < 16 * 1024 * 1024
    started = time.monotonic()
    result = ipc.add_batch(root / 'worker.sock', request_id=parent,
        collection=collection, entries=values)
    client_time = time.monotonic() - started
    assert client_time < 5
    assert all(item.status == 'applied' for item in result.results)
    assert [item.job for item in result.results] == [item['job'] for item in values]
    assert [item.order_key for item in result.results] == list(range(500))
    response_size = len(canonical(result.to_record()))
    assert response_size <= 256 * 1024
    started = time.monotonic(); health = ipc.request_health(root / 'worker.sock')
    health_time = time.monotonic() - started
    assert health.queue_gate == 'paused' and health_time < 1
    with sqlite3.connect(root / 'state.db') as connection:
        for table in ('jobs','commands','events','publication_reservations','add_batch_entries'):
            assert connection.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] == 500
        assert connection.execute('SELECT payload_digest FROM add_batch_commands').fetchone()[0] == command.payload_digest
        receipt = connection.execute('SELECT request_id,entry_count,receipt FROM add_batch_commands').fetchone()
        assert receipt[:2] == (parent, 500) and json.loads(receipt[2]) == result.to_record()
        assert connection.execute('SELECT COUNT(*) FROM command_receipts').fetchone()[0] == 501
        assert connection.execute('SELECT payload_digest,scope,action FROM command_receipts WHERE request_id=?',
            (parent,)).fetchone() == (command.payload_digest, 'add_batch', 'add_batch')
        blobs = connection.execute('SELECT creation_intent_blob,creation_intent_digest FROM add_batch_entries ORDER BY entry_index').fetchall()
        assert len(blobs) == 500
        for index, (blob, seal) in enumerate(blobs):
            assert len(blob) < 40960
            record = json.loads(blob)
            assert record['parent_request_id'] == parent and record['parent_payload_digest'] == command.payload_digest
            assert record['index'] == record['creation']['order_key'] == index
            assert record['entry'] == values[index]
            assert record['child_request_id'] == result.results[index].child_request_id
            assert connection.execute('SELECT payload_digest,job_id FROM commands WHERE request_id=?',
                (record['child_request_id'],)).fetchone() == (record['child_payload_digest'], values[index]['job'])
            assert connection.execute('SELECT job_id,kind FROM events WHERE event_id=?',
                (record['creation']['audit_id'],)).fetchone() == (values[index]['job'], 'job_added')
            assert hashlib.sha256(b'hermes-downloads:batch-creation-intent:v1\0' + blob).hexdigest() == seal
    started = time.monotonic()
    replay = ipc.add_batch(root / 'worker.sock', request_id=parent, collection=collection, entries=values)
    replay_time = time.monotonic() - started
    assert replay.replayed and replay.results == result.results and replay_time < 5
    with closing(SQLiteStore(tmp_path / 'worst.db')) as store:
        started = time.monotonic(); direct = store.apply_add_batch(command)
        db_time = time.monotonic() - started
        assert db_time < 2 and direct.results == result.results
        assert len(store._connection.execute('SELECT * FROM jobs').fetchall()) == 500
    evidence('worst-500.jsonl', dict(parent_characters=len(parent),parent_utf8_bytes=len(parent.encode('utf-8')),
        raw_url_bytes=8192,entry_min=min(entry_sizes),entry_max=max(entry_sizes),
        body_bytes=len(command._wire_request),frame_bytes=len(command._wire_request)+9,
        reply_bytes=response_size,reply_frame_bytes=response_size+9,
        parent_digest=command.payload_digest,hashes=500,rows=500,events=500,
        ordered_results=500,ordered_creation_links=500,global_receipts=501,receipt_bytes=len(receipt[2]),
        encoding_seconds=encoded_time,client_seconds=client_time,db_seconds=db_time,
        replay_seconds=replay_time,health_seconds=health_time,
        maximum_creation_blob=max(len(blob) for blob,_ in blobs)))


@pytest.mark.parametrize('change', [dict(entries=[]),dict(entries=[entry()] * 501),dict(start=True),
    dict(start=0),dict(protocol_version=True),dict(collection='..'),dict(collection='x'*256),
    dict(collection='ا'*128),dict(collection=2),dict(extra='refused'),dict(payload_digest='0'*64)])
def test_closed_batch_envelope_refuses_without_writes(service, change):
    root, _ = service
    assert exchange(root, canonical(envelope(**change))) == {'error':'invalid_request'}
    assert ipc.request_jobs_page(root / 'worker.sock').jobs == ()


@pytest.mark.parametrize('bad', ['A'*64,'a'*63,'a'*65,'g'*64,' '+'a'*64,0,True,[],{},1.5])
def test_invalid_hash_retains_parent_identity_but_has_no_authority(tmp_path, bad):
    with closing(SQLiteStore(tmp_path / 'hash.db')) as store:
        command = ipc.AddBatchCommand('hash-parent',None,[entry(expected_sha256=bad),entry(1)])
        result = store.apply_add_batch(command)
        assert result.results[0].reason == 'invalid_entry' and result.results[1].status == 'applied'
        assert store.get_batch_creation_intent('batch-job-1').expected_sha256 is None
        assert len(store._connection.execute('SELECT * FROM jobs').fetchall()) == 1
        changed = ipc.AddBatchCommand('hash-parent',None,[entry(expected_sha256='f'*64),entry(1)])
        from hermes_downloads.store import RequestConflictError
        with pytest.raises(RequestConflictError): store.apply_add_batch(changed)


def test_hash_absence_null_snapshot_lifecycle_and_reopen(tmp_path):
    path = tmp_path / 'intent.db'; value = entry(expected_sha256='f'*64)
    command = ipc.AddBatchCommand('parent', 'مجموعه', [value])
    value['expected_sha256'] = None; value['source_url'] = 'changed'
    with closing(SQLiteStore(path)) as store:
        result = store.apply_add_batch(command)
        original = store.get_batch_creation_intent('batch-job-0')
        assert original.expected_sha256 == 'f'*64
        store.initialize_cold_start(); store.recover_cold_start(); store.recover_cold_start()
        current = store.get_job('batch-job-0')
        pause = ipc.JobControlCommand('batch-job-0','pause','pause-command',current.revision)
        store.apply_job_control(job_id=pause.job,action=pause.action,request_id=pause.request_id,
            payload_digest=pause.payload_digest,expected_revision=pause.expected_revision)
        assert store.get_batch_creation_intent('batch-job-0') == original
    with closing(SQLiteStore(path)) as store:
        before = store._connection.total_changes
        assert store.apply_add_batch(command).replayed
        assert store._connection.total_changes == before
        assert store.get_batch_creation_intent('batch-job-0') == original
        assert store.get_batch_creation_intent('unknown') is None
    absent = entry(); absent.pop('expected_sha256')
    assert ipc.AddBatchCommand('a',None,[absent]).payload_digest == ipc.AddBatchCommand('a',None,[entry()]).payload_digest


@pytest.mark.parametrize('change,reason', [(dict(source_kind='video'),'invalid_entry'),
    (dict(priority=True),'invalid_entry'),(dict(priority=1<<31),'invalid_entry'),
    (dict(source_url='file:///private/file'),'invalid_source'),
    (dict(source_url='http://user:password@example.test/file'),'invalid_source'),
    (dict(source_url='https://127.0.0.1/file'),'invalid_source'),
    (dict(partial_filename='../escape'),'invalid_entry'),
    (dict(selected_final_filename='unmanaged'),'invalid_entry'),
    (dict(category='Unknown'),'invalid_entry'),(dict(unknown=True),'invalid_entry')])
def test_partial_invalid_positions_gap_and_all_invalid(tmp_path, change, reason):
    with closing(SQLiteStore(tmp_path / 'partial.db')) as store:
        result = store.apply_add_batch(ipc.AddBatchCommand('partial',None,[entry(**change),entry(1)]))
        assert result.results[0].reason == reason
        assert result.results[1].order_key == 1
        rejected = store.apply_add_batch(ipc.AddBatchCommand('all-invalid',None,[entry(**change)]))
        assert rejected.results[0].reason == reason
        assert store.apply_add_batch(ipc.AddBatchCommand('all-invalid',None,[entry(**change)])).replayed


def test_within_batch_jobs_and_destinations_are_distinct_conflicts(tmp_path):
    with closing(SQLiteStore(tmp_path / 'duplicates.db')) as store:
        values = [entry(),entry(),entry(2,partial_filename='file-0.bin',selected_final_filename='file-0.bin')]
        result = store.apply_add_batch(ipc.AddBatchCommand('duplicates',None,values))
        assert [r.reason for r in result.results] == [None,'job_conflict','destination_conflict']
        assert len(store._connection.execute('SELECT * FROM events').fetchall()) == 1


@pytest.mark.parametrize('sql', ["DELETE FROM command_receipts WHERE scope='add_batch_entry'",
    "DELETE FROM command_receipts WHERE scope='add_batch'", "DELETE FROM add_batch_entries",
    "DELETE FROM add_batch_commands", "UPDATE commands SET payload_digest='"+'b'*64+"'",
    "UPDATE jobs SET source_url=X'00'", "UPDATE materialized_jobs SET priority=1",
    "UPDATE publication_reservations SET claim_token='"+'c'*64+"'",
    "UPDATE events SET revision=1", "UPDATE add_batch_entries SET creation_intent_digest='"+'d'*64+"'"])
def test_missing_and_corrupt_creation_links_fail_closed_without_repair(tmp_path, sql):
    with closing(SQLiteStore(tmp_path / 'corrupt.db')) as store:
        command = ipc.AddBatchCommand('corrupt',None,[entry(expected_sha256='a'*64)])
        store.apply_add_batch(command)
        store._connection.execute('PRAGMA foreign_keys=OFF')
        store._connection.execute(sql)
        before = store._connection.total_changes
        with pytest.raises(RuntimeError,match='^batch_state_invalid$'): store.get_batch_creation_intent('batch-job-0')
        with pytest.raises(RuntimeError,match='^batch_state_invalid$'): store.apply_add_batch(command)
        assert store._connection.total_changes == before


@pytest.mark.parametrize('mutation', ['missing_hash','null_hash','different_hash','missing_entry','wrong_index','extra_key'])
def test_blob_corruption_never_becomes_hashless(tmp_path, mutation):
    with closing(SQLiteStore(tmp_path / 'blob.db')) as store:
        command = ipc.AddBatchCommand('blob',None,[entry(expected_sha256='a'*64)])
        store.apply_add_batch(command)
        blob = json.loads(store._connection.execute('SELECT creation_intent_blob FROM add_batch_entries').fetchone()[0])
        if mutation == 'missing_hash': blob['entry'].pop('expected_sha256')
        elif mutation == 'null_hash': blob['entry']['expected_sha256'] = None
        elif mutation == 'different_hash': blob['entry']['expected_sha256'] = 'b'*64
        elif mutation == 'missing_entry': blob.pop('entry')
        elif mutation == 'wrong_index': blob['index'] = 1
        else: blob['unknown'] = True
        raw = canonical(blob)
        # Even a recomputed outer seal cannot hide broken child/global/original links.
        seal = hashlib.sha256(b'hermes-downloads:batch-creation-intent:v1\0'+raw).hexdigest()
        store._connection.execute('UPDATE add_batch_entries SET creation_intent_blob=?,creation_intent_digest=?',(raw,seal))
        with pytest.raises(RuntimeError,match='^batch_state_invalid$'): store.get_batch_creation_intent('batch-job-0')


@pytest.mark.parametrize('table', ['jobs','commands','command_receipts','materialized_jobs',
    'publication_reservations','events','add_batch_entries','add_batch_commands'])
def test_every_real_sql_insert_fault_aborts_whole_batch(tmp_path, table):
    with closing(SQLiteStore(tmp_path / 'fault.db')) as store:
        # Real SQLite write faults, no mocked transaction or digest.
        store._connection.execute(f"CREATE TEMP TRIGGER batch_fault BEFORE INSERT ON main.{table} BEGIN SELECT RAISE(ABORT, 'owned SQL fault'); END")
        with pytest.raises(RuntimeError,match='^batch_state_invalid$'):
            store.apply_add_batch(ipc.AddBatchCommand('fault',None,[entry(),entry(1)]))
        for name in ('jobs','commands','command_receipts','events','add_batch_entries','add_batch_commands'):
            assert store._connection.execute(f'SELECT count(*) FROM {name}').fetchone()[0] == 0


def test_real_deferred_commit_failure_rolls_back_every_row(tmp_path):
    with closing(SQLiteStore(tmp_path / 'commit.db')) as store:
        store._connection.execute("""CREATE TEMP TRIGGER commit_fault AFTER INSERT ON main.add_batch_commands
            BEGIN INSERT INTO add_batch_entries (parent_request_id,entry_index,status,reason)
                VALUES ('nonexistent-parent',0,'blocked','invalid_entry'); END""")
        with pytest.raises(RuntimeError,match='^batch_state_invalid$'):
            store.apply_add_batch(ipc.AddBatchCommand('commit',None,[entry(),entry(1)]))
        for table in ('jobs','commands','events','command_receipts','publication_reservations','add_batch_entries','add_batch_commands'):
            assert store._connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0] == 0


@pytest.mark.parametrize('surface', ['single','queue','control','dispatch'])
def test_parent_and_child_ids_conflict_with_every_existing_surface(tmp_path, surface):
    from hermes_downloads.store import RequestConflictError
    with closing(SQLiteStore(tmp_path / 'cross.db')) as store:
        store.initialize_cold_start(); store.recover_cold_start()
        command = ipc.AddBatchCommand('cross-parent',None,[entry()])
        child = 'batch-entry:' + hashlib.sha256(b'cross-parent\x000').hexdigest()
        if surface == 'single':
            store.apply_add(DownloadIntent('prior-job','cross-parent','a'*64,b'https://example.test/one'))
        elif surface == 'queue':
            store.apply_queue_gate(gate='paused',request_id='cross-parent',payload_digest='a'*64,expected_revision=store.queue_gate_snapshot()[1])
        else:
            store.apply_add_batch(ipc.AddBatchCommand('other-parent',None,[entry(2)]))
            if surface == 'control':
                store.apply_job_control(job_id='batch-job-2',action='pause',request_id='cross-parent',payload_digest='a'*64,expected_revision=0)
            else:
                store.prepare_direct_dispatch(job_id='batch-job-2',request_id='cross-parent',payload_digest='a'*64,
                    expected_generation=0,expected_revision=0,expected_worker_epoch=store.worker_epoch(),now=__import__('datetime').datetime.now(__import__('datetime').UTC),controller_ready=False)
        before = tuple(store._connection.iterdump())
        with pytest.raises(RequestConflictError): store.apply_add_batch(command)
        assert tuple(store._connection.iterdump()) == before
    with closing(SQLiteStore(tmp_path / 'child.db')) as store:
        store.initialize_cold_start(); store.recover_cold_start()
        if surface == 'single':
            store.apply_add(DownloadIntent('prior-job',child,'a'*64,b'https://example.test/one'))
        elif surface == 'queue':
            store.apply_queue_gate(gate='paused',request_id=child,payload_digest='a'*64,expected_revision=store.queue_gate_snapshot()[1])
        else:
            store.apply_add_batch(ipc.AddBatchCommand('other-parent',None,[entry(2)]))
            if surface == 'control':
                store.apply_job_control(job_id='batch-job-2',action='pause',request_id=child,payload_digest='a'*64,expected_revision=0)
            else:
                store.prepare_direct_dispatch(job_id='batch-job-2',request_id=child,payload_digest='a'*64,
                    expected_generation=0,expected_revision=0,expected_worker_epoch=store.worker_epoch(),now=__import__('datetime').datetime.now(__import__('datetime').UTC),controller_ready=False)
        result = store.apply_add_batch(command)
        assert result.results[0].reason == 'request_conflict'


@pytest.mark.parametrize('surface', ['single','queue','control','dispatch','batch'])
@pytest.mark.parametrize('child', [False, True])
def test_created_parent_and_child_ids_refuse_other_surfaces(tmp_path,surface,child):
    from hermes_downloads.store import RequestConflictError
    from datetime import UTC, datetime
    with closing(SQLiteStore(tmp_path / 'reverse.db')) as store:
        store.initialize_cold_start(); store.recover_cold_start()
        result = store.apply_add_batch(ipc.AddBatchCommand('reverse',None,[entry()]))
        request = result.results[0].child_request_id if child else 'reverse'
        before = tuple(store._connection.iterdump())
        with pytest.raises(RequestConflictError):
            if surface == 'single': store.apply_add(DownloadIntent('new-job',request,'a'*64,b'https://example.test/one'))
            elif surface == 'queue': store.apply_queue_gate(gate='paused',request_id=request,payload_digest='a'*64,expected_revision=0)
            elif surface == 'control': store.apply_job_control(job_id='batch-job-0',action='pause',request_id=request,payload_digest='a'*64,expected_revision=0)
            elif surface == 'batch': store.apply_add_batch(ipc.AddBatchCommand(request,None,[entry(1)]))
            else: store.prepare_direct_dispatch(job_id='batch-job-0',request_id=request,payload_digest='a'*64,
                expected_generation=0,expected_revision=0,expected_worker_epoch=store.worker_epoch(),now=datetime.now(UTC),controller_ready=False)
        assert tuple(store._connection.iterdump()) == before


@pytest.mark.parametrize('order', [(1<<63)-1,-1,'malformed'])
def test_order_overflow_or_malformed_is_whole_transaction_failure(tmp_path,order):
    with closing(SQLiteStore(tmp_path / 'order.db')) as store:
        store.apply_add_batch(ipc.AddBatchCommand('first',None,[entry()]))
        store._connection.execute('UPDATE materialized_jobs SET order_key=?',(order,))
        before=tuple(store._connection.iterdump())
        with pytest.raises(RuntimeError,match='^batch_state_invalid$'): store.apply_add_batch(ipc.AddBatchCommand('next',None,[entry(1)]))
        assert tuple(store._connection.iterdump()) == before


def test_batch_collection_mapping_has_distinct_creation_cohorts(tmp_path):
    with closing(SQLiteStore(tmp_path / 'collections.db')) as store:
        name='مجموعه درس'; expected='collection:'+hashlib.sha256(b'hermes-downloads:collection:v1\0'+name.encode()).hexdigest()
        first=store.apply_add_batch(ipc.AddBatchCommand('first',name,[entry()]))
        second=store.apply_add_batch(ipc.AddBatchCommand('second',name,[entry(1)]))
        a=store.get_batch_creation_intent('batch-job-0'); b=store.get_batch_creation_intent('batch-job-1')
        assert a.parent_request_id != b.parent_request_id
        assert a.original_job.queue_collection_id == b.original_job.queue_collection_id == expected
        assert b.original_job.order_key == 1
        store._connection.execute("UPDATE materialized_jobs SET destination_collection='different' WHERE job_id='batch-job-0'")
        with pytest.raises(RuntimeError,match='^batch_state_invalid$'): store.apply_add_batch(ipc.AddBatchCommand('third',name,[entry(2)]))


def test_absolute_slow_frame_and_health_recovers(service):
    root,_=service
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as connection:
        connection.settimeout(3); connection.connect(str(root/'worker.sock'))
        started=time.monotonic(); connection.sendall(b'H')
        for byte in b'DM2\n':
            threading.Event().wait(0.48)
            connection.sendall(bytes([byte]))
        # Length/body never arrive. Progress cannot renew the absolute deadline.
        output=connection.recv(1024)
        assert output.startswith(b'HDM2\n') and time.monotonic()-started < 2.8
    assert ipc.request_health(root/'worker.sock').queue_gate == 'paused'
    assert ipc.request_jobs_page(root/'worker.sock').jobs == ()


@pytest.mark.parametrize('payload', [b'{"op":"add_batch","op":"add_batch"}',
    b'{"x":NaN}',b'{"x":Infinity}',b'{"x":'+b'['*9+b'0'+b']'*9+b'}',
    b'\xff',b'{} {}',b'{',b'{}'])
def test_malformed_framed_json_is_zero_effect(service,payload):
    root,_=service
    assert exchange(root,payload) == {'error':'invalid_request'}
    assert ipc.request_jobs_page(root/'worker.sock').jobs == ()


def test_json_only_v2_and_framed_v1_ops_are_refused(service):
    root,_=service
    assert exchange(root,canonical(envelope()),framed=False)=={'error':'invalid_request'}
    assert exchange(root,b'{"op":"health"}')=={'error':'invalid_request'}


@pytest.mark.parametrize('frame', [b'HDM2\n\x01\x00\x00\x01',b'HDM2\n\0\0\0\0',
    b'HDM2\n\0\0\0\x05{}',b'HDM2\n\0\0\0\x02{}trailing',b'HDM2\n\0\0'])
def test_truncated_trailing_oversize_frames_refuse_without_allocation_or_write(service,frame):
    root,_=service
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as connection:
        connection.settimeout(3); connection.connect(str(root/'worker.sock'))
        connection.sendall(frame); connection.shutdown(socket.SHUT_WR)
        output=connection.recv(1024)
        assert output.startswith(b'HDM2\n') and json.loads(output[9:])=={'error':'invalid_request'}
    assert ipc.request_jobs_page(root/'worker.sock').jobs==()


def test_fragmented_frame_is_accepted_and_lost_reply_replays(service):
    root,_=service; payload=canonical(envelope(request_id='fragmented'))
    frame=b'HDM2\n'+len(payload).to_bytes(4,'big')+payload
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as connection:
        connection.connect(str(root/'worker.sock'))
        for byte in frame: connection.sendall(bytes([byte]))
        connection.shutdown(socket.SHUT_WR)
        # Deliberately lose a real reply, then reconcile the original request.
    deadline=time.monotonic()+3
    while True:
        with sqlite3.connect(root/'state.db') as connection:
            committed=connection.execute("SELECT count(*) FROM add_batch_commands WHERE request_id='fragmented'").fetchone()[0]
        if committed: break
        assert time.monotonic()<deadline; threading.Event().wait(0.02)
    replay=ipc.add_batch(root/'worker.sock',request_id='fragmented',collection=None,entries=[entry()])
    assert replay.replayed
    with sqlite3.connect(root/'state.db') as connection:
        assert connection.execute('SELECT count(*) FROM events').fetchone()[0]==1


@pytest.mark.parametrize('barrier',['before','after'])
def test_owned_sigkill_at_real_commit_barriers_zero_or_whole_500(barrier):
    with tempfile.TemporaryDirectory(prefix='t17-crash-',dir='/private/tmp') as directory:
        root=Path(directory); root.chmod(0o700)
        command=ipc.AddBatchCommand('crash-parent',None,worst_entries())
        request=root/'request.json'; request.write_bytes(command._wire_request); request.chmod(0o600)
        marker=root/'commit-barrier'; child=None; serving=None
        code='''
import json,sys,threading
from pathlib import Path
from hermes_downloads.ipc import AddBatchCommand
from hermes_downloads.store import SQLiteStore
root=Path(sys.argv[1]); phase=sys.argv[2]
command=AddBatchCommand.from_record(json.loads((root/'request.json').read_bytes()))
store=SQLiteStore(root/'state.db')
def trace(sql):
    if phase=='before' and sql=='COMMIT':
        (root/'commit-barrier').write_text('BEFORE_REAL_COMMIT')
        threading.Event().wait(8)
store._connection.set_trace_callback(trace)
store.apply_add_batch(command)
if phase=='after':
    (root/'commit-barrier').write_text('AFTER_REAL_COMMIT')
    threading.Event().wait(8)
store.close()
'''
        def close_owned(process,signum):
            if process.poll() is None:
                assert os.getpgid(process.pid)==process.pid
                os.killpg(process.pid,signum)
            process.wait(timeout=5)
            absent=False
            try: os.killpg(process.pid,0)
            except ProcessLookupError: absent=True
            ledger(dict(kind='batch-crash-closure',barrier=barrier,pid=process.pid,pgid=process.pid,
                returncode=process.returncode,reaped=True,group_absent=absent))
            assert absent
        try:
            child=subprocess.Popen([sys.executable,'-I','-B','-c',code,str(root),barrier],
                cwd=root,env=os.environ.copy(),stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,start_new_session=True)
            birth=subprocess.check_output(['/bin/ps','-p',str(child.pid),'-o','pid=,ppid=,pgid=,lstart=,command='],text=True).strip()
            ledger(dict(kind='batch-crash-birth',barrier=barrier,pid=child.pid,pgid=child.pid,birth=birth))
            deadline=time.monotonic()+5
            while not marker.exists():
                assert child.poll() is None,child.communicate()[1]
                assert time.monotonic()<deadline
                threading.Event().wait(0.01)
            assert marker.read_text()==('BEFORE_REAL_COMMIT' if barrier=='before' else 'AFTER_REAL_COMMIT')
            close_owned(child,signal.SIGKILL)
            with sqlite3.connect(root/'state.db') as connection:
                counts={table:connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
                    for table in ('jobs','commands','events','publication_reservations','add_batch_entries','add_batch_commands')}
            wanted=0 if barrier=='before' else 500
            assert counts['jobs']==counts['commands']==counts['events']==counts['publication_reservations']==counts['add_batch_entries']==wanted
            assert counts['add_batch_commands']==(0 if barrier=='before' else 1)
            serving=subprocess.Popen([str(Path(sys.executable).parent/'hermes-downloads-worker'),
                '--serve','--state-root',str(root)],cwd=root,env=os.environ.copy(),
                stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
            birth=subprocess.check_output(['/bin/ps','-p',str(serving.pid),'-o','pid=,ppid=,pgid=,lstart=,command='],text=True).strip()
            ledger(dict(kind='batch-crash-cold-birth',barrier=barrier,pid=serving.pid,pgid=serving.pid,birth=birth))
            deadline=time.monotonic()+4
            while True:
                assert serving.poll() is None,serving.communicate()[1]
                try:
                    health=ipc.request_health(root/'worker.sock');break
                except ipc.IPCError:
                    assert time.monotonic()<deadline;threading.Event().wait(0.02)
            assert health.queue_gate=='paused'
            result=ipc.add_batch(root/'worker.sock',request_id='crash-parent',collection=None,entries=worst_entries())
            assert result.replayed is (barrier=='after') and len(result.results)==500
            assert ipc.add_batch(root/'worker.sock',request_id='crash-parent',collection=None,entries=worst_entries()).replayed
            with sqlite3.connect(root/'state.db') as connection:
                assert connection.execute('SELECT count(*) FROM jobs').fetchone()[0]==500
                assert connection.execute('SELECT count(*) FROM events WHERE kind=\'job_added\'').fetchone()[0]==500
            evidence('crash-500.jsonl',dict(barrier=barrier,counts_at_crash=counts,
                same_parent_replayed=result.replayed,after_reconciliation_rows=500,creation_events=500))
        finally:
            if child is not None and child.poll() is None: close_owned(child,signal.SIGKILL)
            if serving is not None: close_owned(serving,signal.SIGTERM)


def test_add_list_and_cold_preserve_output_and_real_body_zero(service):
    from http.server import HTTPServer,BaseHTTPRequestHandler
    root,process=service; seen=[]
    class Origin(BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append('GET');self.send_response(200);self.end_headers();self.wfile.write(b'owned body')
        def do_HEAD(self): seen.append('HEAD');self.send_response(200);self.end_headers()
        def log_message(self,*_): pass
    origin=HTTPServer(('127.0.0.1',0),Origin)
    thread=threading.Thread(target=origin.serve_forever,kwargs={'poll_interval':0.02});thread.start()
    output=Path(os.environ['HERMES_DOWNLOADS_OUTPUT_ROOT']);output.mkdir(mode=0o700,exist_ok=True)
    sentinel=output/'t17-sentinel';sentinel.write_bytes(b'private existing output');sentinel.chmod(0o600)
    before=(sentinel.stat().st_ino,sentinel.read_bytes(),sentinel.stat().st_mtime_ns)
    try:
        local=entry(1,source_url=f'http://127.0.0.1:{origin.server_port}/never-requested')
        result=ipc.add_batch(root/'worker.sock',request_id='zero-body',collection=None,entries=[entry(),local])
        assert result.results[0].status=='applied' and result.results[1].reason=='invalid_source'
        ipc.request_jobs_page(root/'worker.sock');ipc.request_health(root/'worker.sock')
        assert (sentinel.stat().st_ino,sentinel.read_bytes(),sentinel.stat().st_mtime_ns)==before
        assert not (root/'direct-runtime').exists()
        assert seen==[]
        evidence('body-zero.jsonl',dict(body_requests=0,head_probes=0,engine_runtime_absent=True,
            sentinel_unchanged=True,accepted_inactive=1,rejected_literal_local=1))
    finally:
        origin.shutdown();origin.server_close();thread.join(timeout=2);assert not thread.is_alive()


def _queued_batch_attempt(store, action):
    """Real captured 0/0 intent, stage, pre-link cold pause and typed control."""
    from dataclasses import replace
    from datetime import UTC, datetime
    from hermes_downloads import direct, paths, processes, retry, store as storage
    body = b'body'
    command = ipc.AddBatchCommand('queued-parent',None,[entry(expected_sha256=hashlib.sha256(body).hexdigest())])
    receipt = store.apply_add_batch(command)
    captured = store.get_batch_creation_intent('batch-job-0')
    store.recover_cold_start()
    def running(request):
        assert store.apply_queue_gate(gate='running',request_id=request,payload_digest='6'*64,
            expected_revision=store.queue_gate_snapshot()[1]).applied
    def control(action, request):
        command = ipc.JobControlCommand('batch-job-0',action,request,store.get_job('batch-job-0').revision)
        result = store.apply_job_control(job_id=command.job,action=command.action,request_id=command.request_id,
            payload_digest=command.payload_digest,expected_revision=command.expected_revision)
        assert result.status == 'applied' and result.state == 'queued'
        return command
    running('initial-running'); control('start_now','initial-start')
    job = store.get_materialized_job('batch-job-0')
    destination = paths.rehydrate_destination(category=job.category,collection=job.destination_collection,
        partial_filename=job.partial_filename,selected_final_filename=job.selected_final_filename,job_id=job.job_id)
    destination.root.mkdir(parents=True,mode=0o700,exist_ok=True)
    paths.prepare_persisted_destination_workspace(destination)
    reservation = store.get_publication_reservation(job.job_id)
    marker = paths.attest_publication_reservation_marker(destination,reservation)
    binding = store.bind_publication_marker(job.job_id,claim_token=reservation.claim_token,
        marker_device=marker.st_dev,marker_inode=marker.st_ino)
    identity = processes.ProcessBirthIdentity.from_record(dict(leader_pid=4242,process_group_id=4242,
        session_id=4242,owner_uid=os.getuid(),started_unix_us=1700000000000001,argv_sha256='a'*64))
    def stage(request):
        current = store.get_job(job.job_id); epoch = store.worker_epoch()
        dispatch = store.prepare_direct_dispatch(job_id=job.job_id,expected_worker_epoch=epoch,
            expected_generation=current.generation,expected_revision=current.revision,
            request_id=request,payload_digest='7'*64,controller_ready=True,now=datetime(2032,1,2,tzinfo=UTC))
        dispatch = store.advance_direct_dispatch_to_downloading(dispatch)
        assert store.finish_direct_dispatch(dispatch).status == 'started'
        record = storage.DirectEngineRecord(epoch,identity)
        store.set_direct_engine_record(record)
        capability = storage._DirectEngineRecoveryCapability(43123,'a'*43)
        store._bind_direct_engine_recovery_capability(record,capability)
        destination.partial_path.write_bytes(body)
        details = destination.partial_path.stat()
        terminal = storage._DirectTerminalPlan(dispatch,record,binding,'0123456789abcdef',destination.partial_path,capability)
        observed = direct.DirectTransfer(job_id=job.job_id,generation=dispatch.generation,gid=terminal.gid,status='complete',
            total_length=4,completed_length=4,partial_path=destination.partial_path,hash_verified=False,
            verification=retry.CompletionVerification.TRANSPORT_VERIFIED,
            verified_identity=direct._VerifiedPayloadIdentity(details.st_dev,details.st_ino,details.st_size,details.st_mtime_ns,
                details.st_mode,details.st_nlink,details.st_ctime_ns))
        store.finalize_direct_terminal(terminal,observed)
        plan = store.prepare_direct_stage(terminal,observed)
        staged = paths.attest_staged_partial_payload(destination,reservation)
        store.bind_direct_staged_payload(plan,staged)
        prepared = paths.prepare_publication_payload(destination,reservation,marker,staged)
        return plan,prepared
    original_stage,prepared = stage('original-dispatch')
    old = store.reserve_direct_publication(original_stage,prepared)
    original_receipt = tuple(store._connection.execute("SELECT * FROM direct_dispatch_commands WHERE request_id='original-dispatch'").fetchone())
    assert not destination.final_path.exists()
    assert store.recover_cold_start() == 2
    assert tuple(store._connection.execute('SELECT status,state FROM direct_publication_attempts').fetchone()) == ('eligible','paused')
    running('successor-running'); closure = control(action,'closure-'+action)
    closed = tuple(store._connection.execute('SELECT * FROM direct_publication_attempts').fetchone())
    assert closed[4] == 'closed' and closed[8] == 'queued' and closed[3] == old.proof
    assert store._clear_direct_engine_record_and_recovery_capability(original_stage.terminal.record,original_stage.capability)
    return command,receipt,captured,old,original_receipt,destination,stage,closure,closed


@pytest.mark.parametrize('action', ('resume','start_now'))
@pytest.mark.parametrize('damage', (None,'unknown-audit','audit-generation','missing-control',
    'blocked-control','noop-digest','missing-registry','wrong-registry'))
def test_eligible_paused_batch_queued_closure_original_replay_and_retirement(tmp_path, action, damage):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / 'queued-batch.db')) as store:
        command,receipt,captured,old,original_receipt,destination,stage,closure,closed = _queued_batch_attempt(store,action)
        original_blob = tuple(store._connection.execute('SELECT creation_intent_blob,creation_intent_digest FROM add_batch_entries').fetchone())
        source = store.get_materialized_job('batch-job-0').intent.source_url
        reservation = store.get_publication_reservation('batch-job-0')
        if damage:
            sql = {
                'unknown-audit': "UPDATE events SET kind='job_unknown' WHERE event_id=?",
                'audit-generation': 'UPDATE events SET generation=generation+1 WHERE event_id=?',
                'missing-control': 'DELETE FROM job_control_commands WHERE request_id=?',
                'blocked-control': "UPDATE job_control_commands SET status='blocked' WHERE request_id=?",
                'noop-digest': 'UPDATE job_control_commands SET payload_digest=? WHERE request_id=?',
                'missing-registry': 'DELETE FROM command_receipts WHERE request_id=?',
                'wrong-registry': "UPDATE command_receipts SET action='pause' WHERE request_id=?",
            }[damage]
            if damage == 'noop-digest':
                noop = ipc.JobControlCommand(closure.job,closure.action,closure.request_id,closure.expected_revision+1)
                store._connection.execute(sql,(noop.payload_digest,closure.request_id))
                store._connection.execute('UPDATE command_receipts SET payload_digest=? WHERE request_id=?',(noop.payload_digest,closure.request_id))
            else:
                store._connection.execute(sql,(closed[5] if 'audit' in damage else closure.request_id,))
        before = tuple(store._connection.iterdump())
        if damage:
            with pytest.raises(RuntimeError,match='^batch_state_invalid$'): store.get_batch_creation_intent('batch-job-0')
            with pytest.raises(RuntimeError,match='^batch_state_invalid$'): store.apply_add_batch(command)
        else:
            assert store.get_batch_creation_intent('batch-job-0') == captured
            replay = store.apply_add_batch(command)
            assert replay.replayed and replay.results == receipt.results
        assert tuple(store._connection.iterdump()) == before
        fresh,prepared = stage('fresh-dispatch')
        before = tuple(store._connection.iterdump())
        if damage:
            with pytest.raises(ValueError): store.reserve_direct_publication(fresh,prepared)
            assert tuple(store._connection.iterdump()) == before
            assert not destination.final_path.exists()
            return
        attempt = store.reserve_direct_publication(fresh,prepared)
        assert attempt.attempt_id != old.attempt_id and attempt.prepared.staged_payload != old.prepared.staged_payload
        assert tuple(store._connection.execute('SELECT * FROM closed_direct_publication_attempts').fetchone()) == closed
        assert store.get_batch_creation_intent('batch-job-0') == captured
        published = paths.publish_staged_partial_payload(destination,prepared.reservation,prepared.staged_payload,
            prepared=prepared,creation_permit=paths.PublicationCreationPermit())
        assert store.complete_direct_publication(attempt,published,initial_stage=fresh).state == 'completed'
        assert destination.final_path.read_bytes() == destination.partial_path.read_bytes() == b'body'
        assert store.get_batch_creation_intent('batch-job-0') == captured
        assert store.apply_add_batch(command).results == receipt.results
        assert tuple(store._connection.execute('SELECT creation_intent_blob,creation_intent_digest FROM add_batch_entries').fetchone()) == original_blob
        assert store.get_materialized_job('batch-job-0').intent.source_url == source
        assert store.get_publication_reservation('batch-job-0') == reservation == captured.reservation
        assert tuple(store._connection.execute("SELECT * FROM direct_dispatch_commands WHERE request_id='original-dispatch'").fetchone()) == original_receipt
        assert tuple(store._connection.execute('SELECT * FROM closed_direct_publication_attempts').fetchone()) == closed
        before = tuple(store._connection.iterdump())
        with pytest.raises(ValueError): store.complete_direct_publication(old,None)
        with pytest.raises(ValueError): store.complete_direct_publication(attempt,published,initial_stage=fresh)
        assert tuple(store._connection.iterdump()) == before
        assert sum(event.kind == 'job_completed' for event in store.list_events()) == 1
        evidence('queued-closure-regression.jsonl',dict(action=action,original_attempt=old.attempt_id,
            successor_attempt=attempt.attempt_id,original_hash=captured.expected_sha256,archived_tuple_preserved=True,
            blob_seal_preserved=True,source_claim_preserved=True,original_started_preserved=True,completed_once=True))
