"""Durable completion delivery through the existing owner, without downloads on read."""
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import UTC, datetime
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import threading
import uuid

import pytest
from hermes_downloads import ipc, paths, store as storage, direct, processes, retry
from hermes_downloads.store import SQLiteStore
from test_add_batch import entry
from test_success_cleanup import real_cleanup_producer, _wait, _rows
from test_target_authority import target, authorize, control

_spec = importlib.util.spec_from_file_location('_event_store_fixtures', Path(__file__).parents[1] / 'unit/test_store.py')
fixtures = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = fixtures
_spec.loader.exec_module(fixtures)

C1, C2 = str(uuid.uuid4()), str(uuid.uuid4())


def pending(store):
    return store.read_notification_pending(ipc.NotificationPendingCommand()).to_record()


def claim(store, consumer=C1):
    return store.claim_notification(ipc.NotificationClaimCommand(consumer)).to_record()


def release_command(value, consumer=C1):
    return ipc.NotificationReleaseCommand(consumer, value['event']['event_id'], value['claim_token'])


def completed(store):
    stage, attempt, destination, _ = fixtures._exact_attempt_fixture(store)
    published = paths.publish_staged_partial_payload(destination, attempt.prepared.reservation,
        attempt.prepared.staged_payload, prepared=attempt.prepared, creation_permit=paths.PublicationCreationPermit())
    store.complete_direct_publication(attempt, published, initial_stage=stage)
    return destination


def test_completion_outbox_exists_without_backfill(tmp_path):
    store = SQLiteStore(tmp_path / 'state.sqlite3')
    try:
        tables = {row[0] for row in store._connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert 'notification_outbox' in tables
        assert store._connection.execute('SELECT count(*) FROM notification_outbox').fetchone()[0] == 0
        before = tuple(store._connection.iterdump())
        assert pending(store)['events'] == []
        assert tuple(store._connection.iterdump()) == before
        assert store._connection.execute("SELECT value FROM settings WHERE key='notification_store_uuid'").fetchone() is None
    finally:
        store.close()


def test_standalone_real_completion_claim_release_replay_across_new_claim(tmp_path):
    with closing(SQLiteStore(tmp_path / 'claims.db')) as store:
        completed(store)
        before = tuple(store._connection.iterdump())
        event = pending(store)['events'][0]
        assert event['type'] == 'completion' and event['count'] == 1 and event['reveal_path'] is None
        assert event['generation'] == store.get_job('job-1').generation
        assert tuple(store._connection.iterdump()) == before
        first = claim(store)['claims'][0]
        replay = claim(store)['claims'][0]
        assert first == replay
        assert claim(store, C2)['claims'] == []
        released = release_command(first)
        assert store.release_notification(released).record == {'status':'released'}
        second = claim(store, C2)['claims'][0]
        assert second['claim_token'] != first['claim_token']
        assert store.release_notification(released).record == {'status':'already_released'}
        assert claim(store, C2)['claims'][0] == second
        presented = ipc.NotificationPresentedCommand(C2, second['event']['event_id'], second['claim_token'], 'host-insertion')
        assert store.present_notification(presented).record == {'status':'presented'}
        assert store.present_notification(presented).record == {'status':'already_presented'}
        assert store.present_notification(replace(presented, notification_id='different')).record == {'status':'stale_claim'}
        assert store.release_notification(release_command(second, C2)).record == {'status':'stale_claim'}
        assert pending(store)['events'] == []
        assert store._connection.execute('SELECT count(*) FROM notification_claims').fetchone()[0] == 2
        assert store._connection.execute("SELECT count(*) FROM events WHERE kind='job_completed'").fetchone()[0] == 1


def test_expired_and_uncommitted_tokens_never_settle_new_lease(tmp_path, monkeypatch):
    with closing(SQLiteStore(tmp_path / 'expiry.db')) as store:
        completed(store)
        first = claim(store)['claims'][0]
        now = first['lease_expires_at_ms'] + 1
        monkeypatch.setattr(storage.time, 'time_ns', lambda: now * 1_000_000)
        assert store.release_notification(release_command(first)).record == {'status':'stale_claim'}
        second = claim(store, C2)['claims'][0]
        assert second['claim_token'] != first['claim_token']
        assert store.release_notification(release_command(first)).record == {'status':'stale_claim'}
        assert store.release_notification(replace(release_command(second, C2), claim_token='a'*64)).record == {'status':'stale_claim'}
        assert claim(store, C2)['claims'][0] == second


@pytest.mark.parametrize('field,value', [('sequence',1 << 53),('generation',1 << 53),('created_at_ms',1 << 53)])
def test_unrepresentable_native_rows_are_durable_and_not_claimed(tmp_path, field, value):
    with closing(SQLiteStore(tmp_path / 'native.db')) as store:
        completed(store)
        store._connection.execute(f'UPDATE notification_outbox SET {field}=?', (value,))
        before = tuple(store._connection.iterdump())
        for call in (lambda: pending(store), lambda: claim(store)):
            with pytest.raises(ValueError): call()
            assert tuple(store._connection.iterdump()) == before
        assert store.get_job('job-1').state == 'completed'


def test_legitimate_native_completion_audit_over_safe_limit_does_not_roll_back(tmp_path):
    with closing(SQLiteStore(tmp_path / 'native-audit.db')) as store:
        fixtures._seed_finalizing_direct_publication(store, final_bound=False)
        # SQLite's native audit sequence may exceed the renderer's numeric range.
        store._connection.execute("UPDATE events SET event_id=? WHERE event_id=(SELECT MAX(event_id) FROM events)", ((1 << 53),))
        plan = store.prepare_direct_dispatch(job_id='job-1', expected_worker_epoch=1,
            expected_generation=1, expected_revision=3, request_id='native-reconcile', payload_digest='6'*64,
            controller_ready=False, now=datetime(2032,1,2,tzinfo=UTC))
        assert store.complete_direct_publication_reconciliation(plan, final_device=903, final_inode=904, logical_size=905).state == 'completed'
        assert store._connection.execute('SELECT count(*) FROM notification_outbox').fetchone()[0] == 1


def test_real_reconciliation_produces_once_and_outbox_insert_fault_rolls_back(tmp_path):
    with closing(SQLiteStore(tmp_path / 'reconcile.db')) as store:
        fixtures._seed_finalizing_direct_publication(store, final_bound=False)
        plan = store.prepare_direct_dispatch(job_id='job-1', expected_worker_epoch=1,
            expected_generation=1, expected_revision=3, request_id='reconcile', payload_digest='6'*64,
            controller_ready=False, now=datetime(2032,1,2,tzinfo=UTC))
        store._connection.execute("CREATE TEMP TRIGGER fail_outbox BEFORE INSERT ON notification_outbox BEGIN SELECT RAISE(ABORT,'outbox failure'); END")
        before = tuple(store._connection.iterdump())
        with pytest.raises(sqlite3.IntegrityError):
            store.complete_direct_publication_reconciliation(plan, final_device=903, final_inode=904, logical_size=905)
        assert tuple(store._connection.iterdump()) == before
        store._connection.execute('DROP TRIGGER fail_outbox')
        store.complete_direct_publication_reconciliation(plan, final_device=903, final_inode=904, logical_size=905)
        assert pending(store)['events'][0]['reveal_path'] is None
        assert store._connection.execute('SELECT count(*) FROM notification_outbox').fetchone()[0] == 1


def test_real_worker_completion_delivery_is_pure_and_certified_without_hash():
    with real_cleanup_producer() as fixture:
        _wait(lambda: _rows(fixture['database'], 'SELECT phase FROM direct_cleanup_claims') == [('finished',)])
        baseline = fixture['origin'].ledger
        event = ipc.request_notification(fixture['sock'], ipc.NotificationPendingCommand()).record['events'][0]
        # The unchanged renderer rejects /private fixture homes, even with a valid certificate.
        assert event['reveal_path'] is None
        with closing(SQLiteStore(fixture['database'])) as store:
            certificate = store._read_direct_cleanup_claim(store._connection, 'cleanup-job', {})
            assert certificate.phase == 'finished'
            payload = paths._direct_cleanup_payload(certificate.attempt.prepared, certificate.certified_stat)
            paths.require_current_direct_cleanup_payload(certificate.attempt.prepared, payload, marker_present=False)
        first = ipc.request_notification(fixture['sock'], ipc.NotificationClaimCommand(C1)).record['claims'][0]
        assert first['event']['event_id'] == event['event_id']
        assert ipc.request_notification(fixture['sock'], ipc.NotificationClaimCommand(C1)).record['claims'][0] == first
        assert fixture['origin'].ledger == baseline
        assert _rows(fixture['database'], 'SELECT * FROM direct_dispatch_commands WHERE request_id=?', ('cleanup-start',))[0] == fixture['original_receipt']
        fixture['final'].write_bytes(b'x' * len(fixture['origin'].payload))
        assert ipc.request_notification(fixture['sock'], ipc.NotificationPendingCommand()).record['events'][0]['reveal_path'] is None
        assert fixture['origin'].ledger == baseline


def test_read_refuses_foreign_transaction_and_restores_original_db_budget(tmp_path):
    with closing(SQLiteStore(tmp_path / 'budget.db')) as store:
        store._connection.execute('PRAGMA busy_timeout=37')
        store._connection.execute('BEGIN')
        with pytest.raises(ValueError): pending(store)
        assert store._connection.in_transaction
        store._connection.rollback()
        with pytest.raises(TimeoutError):
            store.read_notification_pending(ipc.NotificationPendingCommand(), deadline=time.monotonic()-1)
        assert store._connection.execute('PRAGMA busy_timeout').fetchone()[0] == 37
        assert not store._connection.in_transaction


@pytest.mark.parametrize('operation', ['claim', 'release', 'presented'])
def test_notification_reader_blocked_commit_spends_only_remaining_deadline(tmp_path, monkeypatch, operation):
    database = tmp_path / 'commit-budget.db'
    with closing(SQLiteStore(database)) as store:
        completed(store)
        first = claim(store)['claims'][0] if operation != 'claim' else None
        call = (lambda end: store.claim_notification(ipc.NotificationClaimCommand(C1), deadline=end))
        if operation == 'release':
            call = lambda end: store.release_notification(release_command(first), deadline=end)
        elif operation == 'presented':
            command = ipc.NotificationPresentedCommand(C1, first['event']['event_id'], first['claim_token'], 'held-commit')
            call = lambda end: store.present_notification(command, deadline=end)
        before = tuple(store._connection.iterdump())
        store._connection.execute('PRAGMA busy_timeout=37')
        shared, release = threading.Event(), threading.Event()
        errors = []
        def reader():
            try:
                with closing(sqlite3.connect(database)) as connection:
                    connection.execute('BEGIN')
                    connection.execute('SELECT * FROM notification_outbox').fetchall()
                    shared.set()
                    assert release.wait(3)
                    connection.rollback()
            except BaseException as error:
                errors.append(error)
        thread = threading.Thread(target=reader)
        thread.start()
        original = store._notification_transaction
        @contextmanager
        def delayed(*, deadline, write):
            with original(deadline=deadline, write=write) as connection:
                yield connection
                # Actual claim/settlement writes already succeeded under the SHARED reader.
                assert connection.in_transaction
                time.sleep(.30)
        monkeypatch.setattr(store, '_notification_transaction', delayed)
        try:
            assert shared.wait(2)
            started = time.monotonic()
            with pytest.raises((sqlite3.OperationalError, TimeoutError)):
                call(started + .60)
            elapsed = time.monotonic() - started
            assert elapsed < .78, elapsed
            assert tuple(store._connection.iterdump()) == before
            assert not store._connection.in_transaction
            assert store._connection.execute('PRAGMA busy_timeout').fetchone()[0] == 37
            # The expired temporary progress callback must also have been removed.
            assert store._connection.execute('WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<3000) SELECT max(x) FROM n').fetchone()[0] == 3000
        finally:
            release.set()
            thread.join(3)
            assert not thread.is_alive() and not errors


def round_completion(store, job_id):
    """Reuse the existing batch/target/publication owner sequence for two members."""
    job = store.get_materialized_job(job_id)
    destination = paths.rehydrate_destination(category=job.category, collection=job.destination_collection,
        partial_filename=job.partial_filename, selected_final_filename=job.selected_final_filename, job_id=job_id)
    destination.root.mkdir(parents=True, mode=0o700, exist_ok=True)
    paths.prepare_persisted_destination_workspace(destination)
    reservation = store.get_publication_reservation(job_id)
    marker = paths.attest_publication_reservation_marker(destination, reservation)
    binding = store.bind_publication_marker(job_id, claim_token=reservation.claim_token,
        marker_device=marker.st_dev, marker_inode=marker.st_ino)
    current = store.get_job(job_id)
    dispatch = store._prepare_target_dispatch(expected_worker_epoch=1, owner_slot_ready=True, now=datetime(2032,1,2,tzinfo=UTC))
    assert dispatch.job.job_id == job_id
    dispatch = store.advance_direct_dispatch_to_downloading(dispatch)
    store.finish_direct_dispatch(dispatch)
    identity = processes.ProcessBirthIdentity.from_record(dict(leader_pid=4242, process_group_id=4242,
        session_id=4242, owner_uid=os.getuid(), started_unix_us=1700000000000001, argv_sha256='a'*64))
    record = storage.DirectEngineRecord(1, identity)
    store.set_direct_engine_record(record)
    capability = storage._DirectEngineRecoveryCapability(43123, 'a'*43)
    store._bind_direct_engine_recovery_capability(record, capability)
    destination.partial_path.write_bytes(b'body')
    details = destination.partial_path.stat()
    terminal = storage._DirectTerminalPlan(dispatch, record, binding, '0123456789abcdef', destination.partial_path, capability)
    observed = direct.DirectTransfer(job_id=job_id, generation=dispatch.generation, gid=terminal.gid, status='complete',
        total_length=4, completed_length=4, partial_path=destination.partial_path, hash_verified=False,
        verification=retry.CompletionVerification.TRANSPORT_VERIFIED,
        verified_identity=direct._VerifiedPayloadIdentity(details.st_dev, details.st_ino, details.st_size,
            details.st_mtime_ns, details.st_mode, details.st_nlink, details.st_ctime_ns))
    store.finalize_direct_terminal(terminal, observed)
    plan = store.prepare_direct_stage(terminal, observed)
    staged = paths.attest_staged_partial_payload(destination, reservation)
    store.bind_direct_staged_payload(plan, staged)
    prepared = paths.prepare_publication_payload(destination, reservation, marker, staged)
    attempt = store.reserve_direct_publication(plan, prepared)
    published = paths.publish_staged_partial_payload(destination, reservation, staged,
        prepared=prepared, creation_permit=paths.PublicationCreationPermit())
    store.complete_direct_publication(attempt, published, initial_stage=plan)
    assert store._clear_direct_engine_record_and_recovery_capability(record, capability)
    finished = store._read_publication_attempt(store._connection, job_id, require_current=False)
    cleanup = store.activate_direct_cleanup(store.prepare_live_direct_cleanup(finished, published))
    payload = paths.prepare_direct_cleanup_payload(cleanup.claim.attempt.prepared, cleanup.claim.published_stat,
        partial_permit=paths._PartialUnlinkPermit())
    cleanup = store.certify_direct_cleanup(cleanup, payload)
    payload = paths.finish_direct_cleanup_payload(cleanup.claim.attempt.prepared, cleanup.claim.certified_stat,
        marker_permit=paths._MarkerUnlinkPermit())
    store.finish_direct_cleanup(cleanup, payload)


@pytest.mark.parametrize('last', ['complete','remove','zero'])
def test_original_round_coalesces_only_corroborated_completions_and_last_remove(tmp_path, last):
    with closing(SQLiteStore(tmp_path / 'round.db')) as store:
        store.recover_cold_start()
        store.apply_add_batch(ipc.AddBatchCommand('batch-parent', 'named', [entry(0),entry(1)]))
        authorize(store)
        store.apply_queue_gate(gate='running', request_id='open', payload_digest='6'*64, expected_revision=1)
        if last == 'zero':
            control(store, 'batch-job-0', 'remove', 'remove-0')
        else:
            round_completion(store, 'batch-job-0')
        assert pending(store)['events'] == []
        if last == 'complete':
            round_completion(store, 'batch-job-1')
        else:
            control(store, 'batch-job-1', 'remove', 'remove-1')
        assert store._connection.execute('SELECT status FROM authorization_rounds').fetchone()[0] == 'terminal'
        if last == 'zero':
            assert pending(store)['events'] == []
            assert store._notification_store_uuid(store._connection) is None
        else:
            event = pending(store)['events'][0]
            assert event['count'] == (2 if last == 'complete' else 1)
            assert event['job_id'] == (None if last == 'complete' else 'batch-job-0')
            assert event['collection_id'] == store.get_materialized_job('batch-job-0').queue_collection_id
            assert event['reveal_path'] is None
            immutable = tuple(store._connection.execute('SELECT * FROM notification_outbox').fetchone())
            control(store, 'batch-job-0', 'remove', 'remove-completed')
            assert tuple(store._connection.execute('SELECT * FROM notification_outbox').fetchone()) == immutable
            assert store._connection.execute('SELECT count(*) FROM notification_outbox').fetchone()[0] == 1


def test_duplicate_immutable_payload_is_checked_not_ignored(tmp_path):
    with closing(SQLiteStore(tmp_path / 'dedup.db')) as store:
        completed(store)
        row = store._connection.execute('SELECT * FROM notification_outbox').fetchone()
        args = dict(semantic_key=row['semantic_key'], job_id=row['job_id'], collection_id=row['collection_id'],
            generation=row['generation'], count=row['count'], provenance=json.loads(row['provenance']))
        before = tuple(store._connection.iterdump())
        store._connection.execute('BEGIN IMMEDIATE')
        store._enqueue_completion(store._connection, **args)
        store._connection.commit()
        assert tuple(store._connection.iterdump()) == before
        store._connection.execute('BEGIN IMMEDIATE')
        try:
            with pytest.raises(ValueError): store._enqueue_completion(store._connection, **{**args, 'count':2})
        finally: store._connection.rollback()
        assert tuple(store._connection.iterdump()) == before


def test_delivery_fixture_needs_action_priority_and_existing_claim_replay(tmp_path):
    """Delivery ordering fixture only; this is not an actionable producer."""
    with closing(SQLiteStore(tmp_path / 'priority.db')) as store:
        completed(store)
        first = claim(store)['claims'][0]
        store._connection.execute('''INSERT INTO notification_outbox
            (store_uuid,semantic_key,type,job_id,collection_id,generation,created_at_ms,count,reason_code,provenance)
            SELECT store_uuid,'fixture-only-action','needs_space',job_id,collection_id,generation,created_at_ms,count,
                'insufficient_space',provenance FROM notification_outbox WHERE sequence=1''')
        assert pending(store)['events'][0]['type'] == 'needs_space'
        assert pending(store)['has_more'] is True
        assert claim(store)['claims'][0] == first
        assert claim(store, C2)['claims'][0]['event']['type'] == 'needs_space'

from test_mcp_control import sdk_lifecycle

@pytest.mark.parametrize('status,state,event',[(401,'needs_auth','needs_login'),(404,'needs_link','needs_link'),(403,'blocked',None),(503,'retry_wait',None)])
def test_stock_failure_reaches_atomic_owner_and_actionable_outbox(tmp_path,monkeypatch,sdk_lifecycle,status,state,event):
    import asyncio
    import tempfile
    from mcp import ClientSession
    from mcp.client.stdio import stdio_client
    from test_mcp_control import body_worker,parameters,result_record,targets
    from test_direct_publication import _helpers
    evidence_root=Path(os.environ.get('T26_RESUME_RUN',str(tmp_path)))
    evidence=Path(tempfile.mkdtemp(prefix=f'failure-{status}-',dir=evidence_root));evidence.chmod(0o700)
    home=evidence/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home))
    (home/'Downloads/Hermes').mkdir(parents=True,mode=0o700)
    with tempfile.TemporaryDirectory(prefix='fail15-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()() as origin:
            origin._response_for=lambda *_:(status,{'Retry-After':'999'} if status==503 else {},b'')
            with body_worker(root,origin.url(),evidence) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='failure-add',items=[entry(source_url=origin.url())]))
                            receipt=await call('downloads_control',targets(['batch-job-0']))
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='go',expected_revision='1'))
                            deadline=time.monotonic()+8
                            while True:
                                current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                if current['state'] not in {'queued','resolving','downloading'}:break
                                assert process.is_alive() and time.monotonic()<deadline
                                await asyncio.sleep(.02)
                            assert current['state']==state,current
                            with closing(SQLiteStore(root/'state.db')) as store:
                                failure=dict(store._connection.execute('SELECT * FROM direct_failures').fetchone())
                                audit=store._connection.execute('SELECT kind,generation,revision FROM events WHERE event_id=?',(failure['audit_id'],)).fetchone()
                                assert tuple(audit)==('job_failure',current['generation'],current['revision'])
                                assert failure['http_status']==status and failure['state']==state
                                assert failure['retry_due_ms'] is None or failure['retry_due_ms']>=failure['observed_at_ms']+999000
                                budget=store.get_retry_budget('batch-job-0')
                                assert budget.ordinary_attempts==(1 if status==503 else 0)
                                events=store.read_notification_pending(ipc.NotificationPendingCommand()).to_record()['events']
                                assert [e['type'] for e in events]==([] if event is None else [event])
                                assert all(e['reveal_path'] is None for e in events)
                                before=tuple(store._connection.iterdump())
                            assert await call('downloads_control',targets(['batch-job-0']))==receipt|dict(replayed=True)
                            await call('downloads_query',dict(scope='list'))
                            with closing(SQLiteStore(root/'state.db')) as store:assert tuple(store._connection.iterdump())==before
                            assert origin.ledger.response_body_bytes==0
                            (evidence/'failure.json').write_text(json.dumps(dict(failure=failure,events=events,synthetic=True),indent=2))
                asyncio.run(exercise())


def test_real_stock_due_retry_same_epoch_uses_fresh_absence_authority(tmp_path,monkeypatch,sdk_lifecycle):
    import asyncio,hashlib,tempfile
    from mcp import ClientSession
    from mcp.client.stdio import stdio_client
    from test_mcp_control import body_worker,parameters,result_record,targets
    from test_direct_publication import _helpers
    evidence=Path(tempfile.mkdtemp(prefix='auto-retry-',dir=Path(os.environ.get('T26_RESUME_RUN',str(tmp_path)))));evidence.chmod(0o700)
    home=evidence/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));(home/'Downloads/Hermes').mkdir(parents=True,mode=0o700)
    with tempfile.TemporaryDirectory(prefix='retry15-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()() as origin:
            original=origin._response_for;calls=[]
            def response(*args):
                calls.append(args[0])
                return (503,{'Retry-After':'0'},b'') if len(calls)==1 else original(*args)
            origin._response_for=response
            with body_worker(root,origin.url(),evidence) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='retry-add',items=[entry(source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest())]))
                            await call('downloads_control',targets(['batch-job-0']))
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='retry-go',expected_revision='1'))
                            deadline=time.monotonic()+12
                            while True:
                                current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                if current['state']=='completed':break
                                assert process.is_alive() and time.monotonic()<deadline,current
                                await asyncio.sleep(.03)
                            assert (home/'Downloads/Hermes/Other/file-0.bin').read_bytes()==origin.payload
                            with closing(SQLiteStore(root/'state.db')) as store:
                                bodies=[tuple(r) for r in store._connection.execute("SELECT admission_serial,request_id FROM target_dispatch_causes WHERE kind='body' ORDER BY admission_serial")]
                                assert [r[0] for r in bodies]==[1,2]
                                assert store.get_retry_budget('batch-job-0').ordinary_attempts==1
                                assert store._connection.execute('SELECT count(*) FROM direct_failures').fetchone()[0]==1
                                assert store._connection.execute('SELECT count(*) FROM direct_retry_fresh WHERE body_request_id=?',(bodies[1][1],)).fetchone()[0]==1
                            (evidence/'retry.json').write_text(json.dumps(dict(bodies=bodies,calls=len(calls),synthetic=True)))
                asyncio.run(exercise())


def test_actual_renderer_explicit_reveal_contract(tmp_path):
    import subprocess,signal,hashlib
    root=Path(__file__).resolve().parents[3]
    output=Path(os.environ.get('T26_RESUME_RUN',str(tmp_path)))
    log=output/'renderer-explicit.log';start=time.monotonic()
    with log.open('xb') as stream:
        child=subprocess.Popen(['node','--test',str(root/'integrations/hermes-downloads/desktop/plugin.test.mjs')],stdout=stream,stderr=subprocess.STDOUT,start_new_session=True,env={k:v for k,v in os.environ.items() if k not in {'PYTHONPATH','PYTHONHOME'}})
        birth=subprocess.check_output(['/bin/ps','-p',str(child.pid),'-o','pid=,ppid=,pgid=,lstart=,comm='],text=True,timeout=5)
        try:code=child.wait(timeout=60)
        finally:
            if child.poll() is None:os.killpg(child.pid,signal.SIGTERM);child.wait(timeout=5)
    try:os.killpg(child.pid,0);absent=False
    except ProcessLookupError:absent=True
    (output/'renderer-explicit-result.json').write_text(json.dumps(dict(pid=child.pid,birth=birth,waited=True,reaped=child.returncode is not None,group_absent=absent,exit_code=child.returncode,elapsed=time.monotonic()-start,log_sha256=hashlib.sha256(log.read_bytes()).hexdigest())))
    assert absent and code==0,log.read_text()[-4000:]


def test_actual_backend_explicit_reveal_contract(tmp_path):
    import subprocess,signal,hashlib,shutil
    root=Path(__file__).resolve().parents[3];output=Path(os.environ.get('T26_RESUME_RUN',str(tmp_path)));copy=output/'backend-physical';copy.mkdir(mode=0o700)
    official=Path('/Users/mohamadsmt/.hermes/hermes-agent/hermes_constants.py')
    python=Path('/Users/mohamadsmt/.hermes/handoffs/download-manager/full-delivery-2026-10-03/backend-test-env-prerequisite-3/environment/bin/python')
    sources={'hermes_constants.py':official,**{name:root/'integrations/hermes-downloads/dashboard'/name for name in ('plugin_api.py','test_plugin_api.py')}}
    proof={}
    for name,path in sources.items():
        target=copy/name;shutil.copyfile(path,target);target.chmod(0o600)
        assert target.read_bytes()==path.read_bytes() and target.stat().st_ino!=path.stat().st_ino
        proof[name]=dict(source=str(path),copy=str(target),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),distinct_inode=True)
    (output/'backend-physical.json').write_text(json.dumps(proof))
    env={k:v for k,v in os.environ.items() if k not in {'PYTHONPATH','PYTHONHOME'}};env.update(PYTHONDONTWRITEBYTECODE='1',PYTEST_DISABLE_PLUGIN_AUTOLOAD='1',T19_OFFICIAL_CONSTANTS=str(official),T19_BACKEND_EVIDENCE=str(output/'backend-threads.jsonl'))
    log=output/'backend-explicit.log';start=time.monotonic()
    with log.open('xb') as stream:
        child=subprocess.Popen([str(python),'-I','-B','-m','pytest','-p','no:cacheprovider','-q',str(copy/'test_plugin_api.py')],cwd=copy,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True,env=env)
        birth=subprocess.check_output(['/bin/ps','-p',str(child.pid),'-o','pid=,ppid=,pgid=,lstart=,comm='],text=True,timeout=5)
        try:code=child.wait(timeout=120)
        finally:
            if child.poll() is None:os.killpg(child.pid,signal.SIGTERM);child.wait(timeout=5)
    try:os.killpg(child.pid,0);absent=False
    except ProcessLookupError:absent=True
    unchanged=all(hashlib.sha256(path.read_bytes()).hexdigest()==proof[name]['sha256'] for name,path in sources.items())
    (output/'backend-explicit-result.json').write_text(json.dumps(dict(pid=child.pid,birth=birth,waited=True,reaped=child.returncode is not None,group_absent=absent,exit_code=child.returncode,elapsed=time.monotonic()-start,log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),sources_unchanged=unchanged)))
    assert absent and unchanged and code==0,log.read_text()[-7000:]


def test_captured_501_parent_waits_for_late_cohort_and_keeps_closed_event_bytes(tmp_path):
    """Real target/publication transactions; synthetic four-byte staged payloads."""
    import hashlib
    with closing(SQLiteStore(tmp_path/'captured.db')) as store:
        store.recover_cold_start()
        store.apply_add_batch(ipc.AddBatchCommand('capture-add-0','named',[entry(i) for i in range(500)]))
        store.apply_add_batch(ipc.AddBatchCommand('capture-add-1','named',[entry(500)]))
        command=ipc.ControlIntentCommand('queue','start','capture-parent',1)
        receipt=store.accept_control_intent(command,deadline=time.monotonic()+2)
        assert receipt['member_count']=='501' and receipt['cohort_count']=='2'
        store.apply_captured_cohort(command.request_id);store.apply_captured_cohort(command.request_id)
        cohorts=store._connection.execute('SELECT members FROM direct_control_cohorts ORDER BY cohort_index').fetchall()
        early=json.loads(cohorts[0][0])[0]['job_id'];late=json.loads(cohorts[1][0])[0]['job_id']
        current=store.get_job(late)
        store.apply_target_authorize(ipc.TargetAuthorizeCommand('late-first',1,'start_now',dict(kind='jobs',targets=[dict(job=late,expected_revision=current.revision)])))
        store.apply_queue_gate(gate='running',request_id='captured-open',payload_digest='6'*64,expected_revision=1)
        round_completion(store,late)
        assert pending(store)['events']==[]
        assert not store.control_snapshot(command.request_id)['completion_closed']
        remaining=[member['job_id'] for member in json.loads(cohorts[0][0])]
        for job in remaining[:-1]:
            current=store.get_job(job)
            command_remove=ipc.JobControlCommand(job,'remove','captured-remove-'+job,current.revision)
            store.apply_job_control(job_id=job,action='remove',request_id=command_remove.request_id,payload_digest=command_remove.payload_digest,expected_revision=current.revision)
        assert pending(store)['events']==[]
        current=store.get_job(remaining[-1])
        store.apply_target_authorize(ipc.TargetAuthorizeCommand('early-last',1,'start_now',dict(kind='jobs',targets=[dict(job=remaining[-1],expected_revision=current.revision)])))
        round_completion(store,remaining[-1])
        assert store.control_snapshot(command.request_id)['completion_closed']
        durable=[tuple(row) for row in store._connection.execute('SELECT semantic_key,count,provenance FROM notification_outbox ORDER BY sequence')]
        assert len(durable)==2 and sum(row[1] for row in durable)==2
        assert pending(store)['has_more'] and len(pending(store)['events'])==1
        delivered=[]
        for index in range(2):
            claimed=claim(store)['claims'][0]
            delivered.append(claimed['event'])
            presented=ipc.NotificationPresentedCommand(C1,claimed['event']['event_id'],claimed['claim_token'],'captured-host-'+str(index))
            assert store.present_notification(presented).record=={'status':'presented'}
            assert store.present_notification(presented).record=={'status':'already_presented'}
        assert len({event['event_id'] for event in delivered})==2
        assert sum(event['count'] for event in delivered)==2 and pending(store)['events']==[]
        frozen=[tuple(row) for row in store._connection.execute('SELECT semantic_key,count,provenance FROM notification_outbox ORDER BY sequence')]
        assert frozen==durable
        assert store.accept_control_intent(command,deadline=time.monotonic()+2)==receipt|dict(replayed=True)
        store.apply_captured_cohort(command.request_id)
        assert [tuple(row) for row in store._connection.execute('SELECT semantic_key,count,provenance FROM notification_outbox ORDER BY sequence')]==frozen


@pytest.mark.parametrize('operation',['space-stage','space-publication'])
def test_real_worker_filesystem_errno_reaches_actionable_transition(tmp_path,monkeypatch,sdk_lifecycle,operation):
    import asyncio,tempfile,hashlib
    from mcp import ClientSession
    from mcp.client.stdio import stdio_client
    from test_mcp_control import body_worker,parameters,result_record,targets
    from test_direct_publication import _helpers
    evidence=Path(tempfile.mkdtemp(prefix='fs-errno-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));evidence.chmod(0o700)
    home=evidence/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));(home/'Downloads/Hermes').mkdir(parents=True,mode=0o700)
    with tempfile.TemporaryDirectory(prefix='fserr15-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=1024) as origin, body_worker(root,origin.url(),evidence,barrier=operation) as process:
            async def exercise():
                async with stdio_client(parameters(root)) as (read,write):
                    async with ClientSession(read,write) as session:
                        await session.initialize()
                        async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                        await call('downloads_add',dict(request_id='fs-add',items=[entry(source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest())]))
                        await call('downloads_control',targets(['batch-job-0']));await call('downloads_control',dict(scope='queue',action='resume',request_id='fs-go',expected_revision='1'))
                        end=time.monotonic()+8
                        while True:
                            current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                            if current['state']=='paused':break
                            assert process.is_alive() and time.monotonic()<end;await asyncio.sleep(.02)
                        with closing(SQLiteStore(root/'state.db')) as store:
                            events=store.read_notification_pending(ipc.NotificationPendingCommand()).to_record()['events']
                            assert [event['type'] for event in events]==['needs_space']
                            assert events[0]['reveal_path'] is None
                            assert store.get_retry_budget('batch-job-0') is None
                        partial=home/'Downloads/Hermes/.incomplete/batch-job-0/file-0.bin'
                        assert partial.read_bytes()==origin.payload and not (home/'Downloads/Hermes/Other/file-0.bin').exists()
            asyncio.run(exercise())


def test_actual_five_stock_failures_hold_and_cold_exhaustion_require_fresh_retry(tmp_path,monkeypatch,sdk_lifecycle):
    import asyncio,hashlib,tempfile
    from mcp import ClientSession
    from mcp.client.stdio import stdio_client
    from test_mcp_control import body_worker,parameters,result_record,targets
    from test_direct_publication import _helpers
    evidence=Path(tempfile.mkdtemp(prefix='five-cold-retry-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));evidence.chmod(0o700)
    home=evidence/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    with tempfile.TemporaryDirectory(prefix='five-retry-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=1024) as origin:
            response=origin._response_for;origin._response_for=lambda *_:(503,{},b'')
            authorization=targets(['batch-job-0'],request='five-authorize')
            with body_worker(root,origin.url(),evidence) as original:
                async def exhaust():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='five-add',items=[entry(source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest())]))
                            receipt=await call('downloads_control',authorization)
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='five-go',expected_revision='1'))
                            end=time.monotonic()+12
                            while True:
                                current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                if current['state']=='retry_wait':break
                                assert original.is_alive() and time.monotonic()<end;await asyncio.sleep(.05)
                            await call('downloads_control',dict(scope='queue',action='pause',request_id='five-hold',expected_revision='2'))
                            with closing(SQLiteStore(root/'state.db')) as store:
                                held=store.get_retry_budget('batch-job-0');assert held.ordinary_attempts==1
                                due=store._connection.execute('SELECT retry_due_ms FROM direct_failures').fetchone()[0]
                            await asyncio.sleep(max(.1,due/1000-time.time()+.4))
                            with closing(SQLiteStore(root/'state.db')) as store:
                                assert store.get_retry_budget('batch-job-0')==held
                                assert store._connection.execute("SELECT count(*) FROM target_dispatch_causes WHERE kind='body'").fetchone()[0]==1
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='five-unhold',expected_revision='3'))
                            end=time.monotonic()+180
                            while True:
                                current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                if current['state']=='blocked':break
                                assert original.is_alive() and time.monotonic()<end;await asyncio.sleep(.15)
                            with closing(SQLiteStore(root/'state.db')) as store:
                                budget=store.get_retry_budget('batch-job-0')
                                assert budget.ordinary_attempts==5 and budget.exhausted
                                bodies=[tuple(row) for row in store._connection.execute("SELECT admission_serial,request_id FROM target_dispatch_causes WHERE kind='body' ORDER BY admission_serial")]
                                assert [row[0] for row in bodies]==[1,2,3,4,5]
                                assert store._connection.execute('SELECT count(*) FROM direct_failures').fetchone()[0]==5
                                assert [tuple(row) for row in store._connection.execute('SELECT type,count(*) FROM notification_outbox GROUP BY type')]==[('persistent_error',1)]
                            return receipt,budget,bodies
                receipt,budget,bodies=asyncio.run(exhaust())
            assert original.exitcode==0
            with body_worker(root,origin.url(),evidence) as cold:
                async def fresh():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            assert (await call('downloads_query',dict(scope='queue')))['queue_gate']=='paused'
                            assert await call('downloads_control',authorization)==receipt|dict(replayed=True)
                            await call('downloads_query',dict(scope='list'))
                            with closing(SQLiteStore(root/'state.db')) as store:
                                restored=store.get_retry_budget('batch-job-0')
                                assert restored.ordinary_attempts==budget.ordinary_attempts and restored.exhausted and restored.budget_number==budget.budget_number and restored.generation==budget.generation+1 and restored.paused
                                assert restored.audit[:len(budget.audit)]==budget.audit
                                assert store._connection.execute("SELECT count(*) FROM target_dispatch_causes WHERE kind='body'").fetchone()[0]==5
                            origin._response_for=response
                            current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                            request=dict(scope='job',action='retry',job='batch-job-0',request_id='five-fresh',expected_revision=str(current['revision']),expected_worker_epoch='2')
                            accepted=await call('downloads_control',request)
                            end=time.monotonic()+8
                            while (await call('downloads_query',dict(scope='control',id='five-fresh')))['status']=='pending':
                                assert time.monotonic()<end;await asyncio.sleep(.03)
                            with closing(SQLiteStore(root/'state.db')) as store:
                                after=store.get_retry_budget('batch-job-0');assert after.ordinary_attempts==0 and not after.exhausted and after.budget_number==budget.budget_number+1
                                assert store._connection.execute("SELECT count(*) FROM target_dispatch_causes WHERE kind='body'").fetchone()[0]==5
                            queue=await call('downloads_query',dict(scope='queue'))
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='five-cold-go',expected_revision=queue['revision']))
                            end=time.monotonic()+12
                            while not (output/'Other/file-0.bin').exists():
                                assert cold.is_alive() and time.monotonic()<end;await asyncio.sleep(.03)
                            assert (output/'Other/file-0.bin').read_bytes()==origin.payload
                            assert await call('downloads_control',request)==accepted|dict(replayed=True)
                            (evidence/'five-retry-result.json').write_text(json.dumps(dict(bodies=bodies,ordinary_attempts=5,cold_preserved=True,hold_preserved=True,fresh_budget=True,synthetic=True)))
                asyncio.run(fresh())
