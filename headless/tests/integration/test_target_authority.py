"""Internal target receipts on real private SQLite and AF_UNIX owners."""
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import socket
import time

import pytest
from hermes_downloads import ipc
from hermes_downloads.store import SQLiteStore, RequestConflictError
from hermes_downloads import store as store_module, worker
from test_add_batch import canonical, entry, envelope, exchange, service, worst_entries


def target(request='target', epoch=1, action='start', selector=None):
    return dict(op='target_authorize', protocol_version=2, request_id=request,
        expected_worker_epoch=epoch, action=action,
        selector=selector or dict(kind='creation_cohort',
            creation=dict(kind='batch', request_id='batch-parent'), indices=None))


def test_real_batch_target_captures_authority_without_execution(tmp_path):
    with closing(SQLiteStore(tmp_path / 'target.db')) as store:
        store.recover_cold_start()
        store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
        method = getattr(store, 'apply_target_authorize', None)
        assert callable(method), 'missing internal target-authorize owner transaction'
        command_type = getattr(ipc, 'TargetAuthorizeCommand', None)
        assert command_type is not None, 'missing closed target command'
        result = method(command_type.from_record(target())).to_record()
        assert result['execution_effect'] == 'none'
        assert result['results'][0]['outcome'] == 'new_authority'
        assert result['results'][0]['round_generation'] == 1
        assert result['results'][0]['held_by'] == ['global']
        assert store.get_job('batch-job-0').state == 'queued'
        assert store.get_batch_creation_intent('batch-job-0').original_job.authorized is False
        assert store._connection.execute('SELECT count(*) FROM job_authorization_heads').fetchone()[0] == 1
        before = store._connection.total_changes
        replay = method(command_type.from_record(target())).to_record()
        assert replay == {**result, 'replayed': True}
        assert store._connection.total_changes == before


def test_target_wire_reaches_real_owner(service):
    root, _ = service
    exchange(root, canonical(envelope()))
    result = ipc.target_authorize(root / 'worker.sock', ipc.TargetAuthorizeCommand.from_record(target()))
    assert result.to_record()['results'][0]['outcome'] == 'new_authority'


def test_cold_head_and_safe_control_keep_real_lineage(tmp_path):
    with closing(SQLiteStore(tmp_path / 'target.db')) as store:
        store.recover_cold_start()
        store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
        original = store.apply_target_authorize(ipc.TargetAuthorizeCommand.from_record(target())).to_record()
        assert store.get_batch_creation_intent('batch-job-0') is not None
        store.recover_cold_start()
        head = store._connection.execute('SELECT * FROM job_authorization_heads').fetchone()
        assert head['current_worker_epoch'] == 2 and head['intent_status'] == 'inactive'
        assert store.apply_target_authorize(ipc.TargetAuthorizeCommand.from_record(target())).to_record() == {**original, 'replayed': True}
        resumed = store.apply_target_authorize(ipc.TargetAuthorizeCommand.from_record(target('resume', epoch=2, action='resume')))
        assert resumed.to_record()['results'][0]['outcome'] == 'existing_authority'
        current = store.get_job('batch-job-0')
        command = ipc.JobControlCommand('batch-job-0','pause','pause',current.revision)
        result = store.apply_job_control(job_id=command.job,action=command.action,request_id=command.request_id,
            payload_digest=command.payload_digest,expected_revision=command.expected_revision)
        assert result.status == 'applied'
        assert store.get_batch_creation_intent(command.job) is not None
        head = store._connection.execute('SELECT * FROM job_authorization_heads').fetchone()
        assert head['current_revision'] == result.revision


def test_upgraded_v1_resume_is_blocked_before_flags(tmp_path):
    with closing(SQLiteStore(tmp_path / 'target.db')) as store:
        store.recover_cold_start()
        store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
        store.apply_target_authorize(ipc.TargetAuthorizeCommand.from_record(target()))
        before = store.get_materialized_job('batch-job-0')
        command = ipc.JobControlCommand('batch-job-0','start_now','v1-start',before.intent.revision)
        result = store.apply_job_control(job_id=command.job,action=command.action,request_id=command.request_id,
            payload_digest=command.payload_digest,expected_revision=command.expected_revision)
        assert result.status == 'blocked'
        assert store.get_materialized_job('batch-job-0') == before


def select_jobs(jobs, revision=None):
    return dict(kind='jobs',targets=[dict(job=j,expected_revision=revision) for j in jobs])


def authorize(store, request='target', action='start', selector=None):
    return store.apply_target_authorize(ipc.TargetAuthorizeCommand.from_record(
        target(request,store.worker_epoch(),action,selector))).to_record()


def test_unknown_history_stays_negative_across_requests_cold_and_real_creation(tmp_path):
    database=tmp_path/'unknown-history.db';selector=select_jobs(['missing'])
    with closing(SQLiteStore(database)) as store:
        store.recover_cold_start()
        command=ipc.TargetAuthorizeCommand.from_record(target('missing-first',selector=selector))
        first=store.apply_target_authorize(command).to_record()
        assert first['results']==[dict(index=0,job='missing',outcome='blocked',reason='unknown_job',
            round_generation=None,captured_generation=None,captured_revision=None,held_by=None)]
        before=tuple(store._connection.iterdump())
        assert store.apply_target_authorize(command).to_record()=={**first,'replayed':True}
        assert tuple(store._connection.iterdump())==before
        second=authorize(store,'missing-second',selector=selector)
        assert second['results']==first['results'] and second['replayed'] is False
        assert store.get_batch_creation_intent('missing') is None
        assert not store._target_managed(store._connection,'missing',{})
        assert store._connection.execute('SELECT count(*) FROM authorization_rounds').fetchone()[0]==0
        assert store._connection.execute('SELECT count(*) FROM job_authorization_heads').fetchone()[0]==0
    with closing(SQLiteStore(database)) as store:
        store.recover_cold_start();before=tuple(store._connection.iterdump())
        assert store.apply_target_authorize(command).to_record()=={**first,'replayed':True}
        assert tuple(store._connection.iterdump())==before
        assert authorize(store,'missing-third',selector=selector)['results']==first['results']
        worker._job_add_from_store(store,ipc.JobAddCommand('missing','real-add',
            'https://example.test/created',0,2,'Other','created.bin','created.bin'))
        assert authorize(store,'no-inherited-grant','resume',selector)['results'][0]['reason']=='incompatible_authority'
        assert not store.get_materialized_job('missing').authorized
        real=authorize(store,'real-grant',selector=selector)['results'][0]
        assert real['outcome']=='new_authority' and real['round_generation']==1
        head=store._connection.execute('SELECT * FROM job_authorization_heads').fetchone()
        assert head['owner_request_id']=='real-grant' and head['cohort_request_id']=='real-add'
        before=tuple(store._connection.iterdump())
        assert store.apply_target_authorize(command).to_record()=={**first,'replayed':True}
        assert tuple(store._connection.iterdump())==before


def test_unknown_history_wire_mixed_request_keeps_independent_valid_target(service):
    root,_=service;selector=select_jobs(['missing'])
    command=ipc.TargetAuthorizeCommand.from_record(target('missing-first',selector=selector))
    first=ipc.target_authorize(root/'worker.sock',command).to_record()
    with sqlite3.connect(root/'state.db') as connection:before=tuple(connection.iterdump())
    assert ipc.target_authorize(root/'worker.sock',command).to_record()=={**first,'replayed':True}
    with sqlite3.connect(root/'state.db') as connection:assert tuple(connection.iterdump())==before
    assert exchange(root,canonical(envelope()))['results'][0]['status']=='applied'
    mixed=ipc.target_authorize(root/'worker.sock',ipc.TargetAuthorizeCommand.from_record(
        target('mixed-missing',selector=select_jobs(['missing','batch-job-0'])))).to_record()
    assert [(r['index'],r['job'],r['outcome'],r['reason']) for r in mixed['results']]==[
        (0,'missing','blocked','unknown_job'),(1,'batch-job-0','new_authority',None)]
    again=ipc.target_authorize(root/'worker.sock',ipc.TargetAuthorizeCommand.from_record(
        target('missing-again',selector=selector))).to_record()
    assert again['results']==first['results']
    with sqlite3.connect(root/'state.db') as connection:
        assert connection.execute('SELECT count(*) FROM authorization_rounds').fetchone()[0]==1
        assert connection.execute('SELECT count(*) FROM job_authorization_heads WHERE job_id="missing"').fetchone()[0]==0
    assert ipc.request_health(root/'worker.sock').worker_epoch==1
    evidence('unknown-repair.jsonl',dict(real_owner_wire=True,repeated_unknown=True,mixed_independent=True,
        original_replay=True,unknown_rounds=0,unknown_heads=0,execution_effect=mixed['execution_effect']))


@pytest.mark.parametrize('damage',['vector','receipt','registry','orphan','reason','observed'])
def test_unknown_history_corruption_still_refuses_without_writes(tmp_path,damage):
    with closing(SQLiteStore(tmp_path/'unknown-corrupt.db')) as store:
        store.recover_cold_start();selector=select_jobs(['missing'])
        authorize(store,'negative',selector=selector)
        connection=store._connection;connection.execute('PRAGMA foreign_keys=OFF')
        if damage=='vector':connection.execute('UPDATE target_commands SET vector_digest=printf("%064d",0)')
        elif damage=='receipt':connection.execute('UPDATE target_commands SET receipt_blob=CAST("{}" AS BLOB)')
        elif damage=='registry':connection.execute('DELETE FROM command_receipts WHERE request_id="negative"')
        elif damage=='orphan':connection.execute('DELETE FROM target_commands')
        else:
            # A recomputed seal cannot turn a non-unknown or observed member into absence.
            connection.execute('UPDATE target_members SET '+('reason="terminal"' if damage=='reason' else 'held_mask=0'))
            members=[dict(row) for row in connection.execute('SELECT * FROM target_members ORDER BY target_index')]
            receipt=json.loads(connection.execute('SELECT receipt_blob FROM target_commands').fetchone()[0])
            receipt['results']=[store_module._target_result(m) for m in members]
            connection.execute('UPDATE target_commands SET vector_digest=?,receipt_blob=?',
                (store_module._target_vector_digest(members),canonical(receipt)))
        connection.execute('PRAGMA foreign_keys=ON');before=tuple(connection.iterdump())
        with pytest.raises(RuntimeError,match='target_authority_corrupt'):
            authorize(store,'new-negative',selector=selector)
        assert tuple(connection.iterdump())==before
        with pytest.raises(RuntimeError,match='batch_state_invalid'):store.get_batch_creation_intent('missing')
        assert tuple(connection.iterdump())==before


@pytest.mark.parametrize('upgraded',[False,True])
def test_missing_real_creation_links_never_become_unknown(tmp_path,upgraded):
    with closing(SQLiteStore(tmp_path/'missing-positive.db')) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
        if upgraded:authorize(store,'positive')
        connection=store._connection;connection.execute('PRAGMA foreign_keys=OFF')
        connection.execute('DELETE FROM add_batch_entries');connection.execute('DELETE FROM commands')
        if upgraded:
            for table in ('job_authorization_heads','publication_reservations','materialized_jobs','jobs','events'):
                connection.execute(f'DELETE FROM {table}')
        connection.execute('PRAGMA foreign_keys=ON');before=tuple(connection.iterdump())
        with pytest.raises(RuntimeError,match='target_authority_corrupt'):
            authorize(store,'missing-proof',selector=select_jobs(['batch-job-0']))
        assert tuple(connection.iterdump())==before


def control(store, job, action, request, **contained):
    command = ipc.JobControlCommand(job,action,request,store.get_job(job).revision)
    return store.apply_job_control(job_id=job,action=action,request_id=request,
        payload_digest=command.payload_digest,expected_revision=command.expected_revision,**contained)


def evidence(name, record):
    root = os.environ.get('T18_IMPLEMENTATION_RUN')
    if root:
        path = Path(root) / name
        with path.open('a') as stream:
            path.chmod(0o600)
            stream.write(json.dumps(record,sort_keys=True)+'\n')


def target_exchange(root, payload, *, preamble=b'HDT2\n', trailing=b'', eof=True, declared=None):
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as connection:
        connection.settimeout(5);connection.connect(str(root/'worker.sock'))
        connection.sendall(preamble+(len(payload) if declared is None else declared).to_bytes(4,'big')+payload+trailing)
        if eof: connection.shutdown(socket.SHUT_WR)
        output = bytearray()
        while chunk := connection.recv(8192):
            output.extend(chunk);assert len(output)<=256*1024+9
    assert len(output)==9+int.from_bytes(output[5:9],'big')
    return json.loads(output[9:])


def test_real_maximal_500_bounds_replay_conflicts_and_progress(service,tmp_path):
    root,_ = service;values = worst_entries()
    assert ipc.add_batch(root/'worker.sock',request_id='batch-parent',collection=None,entries=values).results[-1].status=='applied'
    jobs = [e['job'] for e in values];request='T'+'x'*127
    command = ipc.TargetAuthorizeCommand.from_record(target(request,selector=select_jobs(jobs)))
    maximal = ipc.TargetAuthorizeCommand.from_record(target('M'+'x'*127,
        epoch=(1<<63)-1,selector=select_jobs(jobs,(1<<63)-1)))
    assert len(maximal._wire_request)<=128*1024
    started=time.monotonic();result=ipc.target_authorize(root/'worker.sock',command);client_seconds=time.monotonic()-started
    record=result.to_record();assert [r['job'] for r in record['results']]==jobs
    assert all(r['outcome']=='new_authority' for r in record['results'])
    assert len(result._wire_reply)<=256*1024 and client_seconds<5
    started=time.monotonic();replay=ipc.target_authorize(root/'worker.sock',command);replay_seconds=time.monotonic()-started
    assert replay.to_record()=={**record,'replayed':True}
    for changed in [target(request,epoch=2,selector=select_jobs(jobs)),
            target(request,action='start_now',selector=select_jobs(jobs)),
            target(request,selector=select_jobs(list(reversed(jobs)))),
            target(request,selector=select_jobs(jobs[:-1]+['changed-last'])),
            target(request,selector=select_jobs(jobs,1))]:
        with pytest.raises(ipc.IPCError,match='command_conflict'):
            ipc.target_authorize(root/'worker.sock',ipc.TargetAuthorizeCommand.from_record(changed))
    started=time.monotonic();assert ipc.request_health(root/'worker.sock').worker_epoch==1
    health_seconds=time.monotonic()-started;assert health_seconds<1
    with sqlite3.connect(root/'state.db') as connection:
        counts={name:connection.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
            for name in ('target_commands','target_members','authorization_rounds','job_authorization_heads')}
        assert counts==dict(target_commands=1,target_members=500,authorization_rounds=1,job_authorization_heads=500)
        rows=[tuple(row) for row in connection.execute('SELECT * FROM target_members ORDER BY target_index')]
    with closing(SQLiteStore(tmp_path/'500.db')) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope(values)))
        busy=store._connection.execute('PRAGMA busy_timeout').fetchone()[0]
        started=time.monotonic();database_result=store.apply_target_authorize(command);database_seconds=time.monotonic()-started
        assert database_result.to_record()==record and database_seconds<2
        assert store._connection.execute('PRAGMA busy_timeout').fetchone()[0]==busy
        store.recover_cold_start()
        assert store.apply_target_authorize(command).to_record()=={**record,'replayed':True}
        assert store._connection.execute('SELECT count(*) FROM job_authorization_heads WHERE intent_status="inactive"').fetchone()[0]==500
    evidence('measured-500.jsonl',dict(request_bytes=len(command._wire_request),max_scalar_request_bytes=len(maximal._wire_request),
        reply_bytes=len(result._wire_reply),member_scalar_bytes=len(canonical(rows)),rows=counts,
        database_seconds=database_seconds,client_seconds=client_seconds,replay_seconds=replay_seconds,
        health_seconds=health_seconds,body_effects=0,cold_replay_original=True,busy_restored=True))


@pytest.mark.parametrize('changes',[
    dict(expected_worker_epoch=True),dict(expected_worker_epoch=0),dict(expected_worker_epoch=1.0),
    dict(action='continue'),dict(extra=True),dict(protocol_version=True),
    dict(selector=select_jobs(['same','same'])),dict(selector=select_jobs(['bad/id'])),
    dict(selector=select_jobs(['j'],True)),dict(selector=select_jobs(['j'],-1)),
    dict(selector=dict(kind='creation_cohort',creation=dict(kind='single',request_id='p'),indices=[1])),
    dict(selector=dict(kind='creation_cohort',creation=dict(kind='batch',request_id='p'),indices=[False])),
])
def test_closed_target_validation_has_no_mutation(changes):
    with pytest.raises((ValueError,TypeError)):
        ipc.TargetAuthorizeCommand.from_record({**target(),**changes})


@pytest.mark.parametrize('fault',['duplicate','utf8','nan','deep','trailing','oversize','no-eof','wrong-op','batch-tag','ndjson','501'])
def test_target_wire_refuses_before_owner_effects(service,fault):
    root,_=service;payload=canonical(target());tag=b'HDT2\n';trailing=b'';eof=True
    if fault=='duplicate':payload=payload[:-1]+b',"action":"start"}'
    elif fault=='utf8':payload=b'\xff'
    elif fault=='nan':payload=payload.replace(b'"expected_worker_epoch":1',b'"expected_worker_epoch":NaN')
    elif fault=='deep':payload=b'['*9+b'0'+b']'*9
    elif fault=='trailing':trailing=b'X'
    elif fault=='oversize':payload=b''
    elif fault=='no-eof':eof=False
    elif fault=='wrong-op':payload=canonical(envelope())
    elif fault=='batch-tag':tag=b'HDM2\n'
    elif fault=='501':payload=canonical(target(selector=select_jobs([f'j-{i}' for i in range(501)])))
    with sqlite3.connect(root/'state.db') as connection:before=tuple(connection.iterdump())
    if fault=='ndjson':reply=exchange(root,payload,framed=False)
    else:reply=target_exchange(root,payload,preamble=tag,trailing=trailing,eof=eof,
        declared=128*1024+1 if fault=='oversize' else None)
    assert set(reply)=={'error'}
    if fault=='501':assert reply=={'error':'unsupported_selection_size'}
    with sqlite3.connect(root/'state.db') as connection:assert tuple(connection.iterdump())==before
    assert ipc.request_health(root/'worker.sock').worker_epoch==1


def test_cohort_positions_rounds_and_noop_activation_are_real(tmp_path):
    with closing(SQLiteStore(tmp_path/'cohort.db')) as store:
        store.recover_cold_start()
        created=store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope([entry(0),{'invalid':True},entry(2)])))
        assert [r.status for r in created.results]==['applied','blocked','applied']
        before=tuple(store._connection.iterdump())
        with pytest.raises(RuntimeError,match='target_request_invalid'):
            authorize(store,selector=dict(kind='creation_cohort',creation=dict(kind='batch',request_id='batch-parent'),indices=[0,1]))
        assert tuple(store._connection.iterdump())==before
        first=authorize(store,'first',selector=select_jobs(['batch-job-0']))
        mixed=authorize(store,'mixed')
        assert [r['outcome'] for r in mixed['results']]==['existing_authority','new_authority']
        assert [r['round_generation'] for r in mixed['results']]==[1,2]
        assert [tuple(r) for r in store._connection.execute('SELECT round_generation,member_count FROM authorization_rounds ORDER BY round_generation')]==[(1,1),(2,1)]
        audit_count=len(store.list_events());head=tuple(store._connection.execute('SELECT * FROM job_authorization_heads WHERE job_id="batch-job-0"').fetchone())
        authorize(store,'noop',selector=select_jobs(['batch-job-0']))
        assert len(store.list_events())==audit_count
        new=store._connection.execute('SELECT * FROM job_authorization_heads WHERE job_id="batch-job-0"').fetchone()
        assert new['activation_request_id']=='noop' and new['owner_request_id']==head[1] and new['last_admission_serial']==0
        assert store._connection.execute('SELECT intent_audit_id FROM target_members WHERE request_id="noop"').fetchone()[0] is None
        store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope([entry(3)],request_id='other-parent')))
        other=authorize(store,'other',selector=select_jobs(['batch-job-3']))
        assert other['results'][0]['round_generation']==1
        assert store._connection.execute('SELECT count(*) FROM settings WHERE key LIKE "authorization_round:%"').fetchone()[0]==2
        assert first['results'][0]['round_generation']==1


@pytest.mark.parametrize('action',['start','resume','start_now'])
@pytest.mark.parametrize('global_hold',[False,True])
@pytest.mark.parametrize('collection_hold',[False,True])
@pytest.mark.parametrize('manual_hold',[False,True])
def test_target_preserves_each_supported_gate(tmp_path,action,global_hold,collection_hold,manual_hold):
    with closing(SQLiteStore(tmp_path/'holds.db')) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope(collection='collection')))
        authorize(store,'grant')
        job=store.get_materialized_job('batch-job-0')
        if not global_hold:store.apply_queue_gate(gate='running',request_id='open',payload_digest='a'*64,expected_revision=1)
        if collection_hold:store.set_collection_hold(job.queue_collection_id,held=True)
        if manual_hold:control(store,job.job_id,'pause','pause')
        result=authorize(store,'intent',action=action)['results'][0]
        expected=(['global'] if global_hold else [])+(['collection'] if collection_hold else [])+(['manual'] if manual_hold and action=='start' else [])
        assert result['held_by']==expected
        assert store.queue_gate()==('paused' if global_hold else 'running')
        assert store.collection_hold(job.queue_collection_id)==collection_hold
        assert store.get_batch_creation_intent(job.job_id).original_job.authorized is False


def test_remove_closes_only_all_original_round_members_and_cold_terminal_epoch(tmp_path):
    with closing(SQLiteStore(tmp_path/'terminal.db')) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope([entry(0),entry(1)])))
        original=authorize(store)
        assert control(store,'batch-job-0','remove','remove-0').state=='removed'
        assert store._connection.execute('SELECT status FROM authorization_rounds').fetchone()[0]=='open'
        store.recover_cold_start()
        assert control(store,'batch-job-1','remove','remove-1').state=='removed'
        assert store._connection.execute('SELECT status FROM authorization_rounds').fetchone()[0]=='terminal'
        assert authorize(store,'blocked')['results'][0]['reason']=='terminal'
        assert store.apply_target_authorize(ipc.TargetAuthorizeCommand.from_record(target())).to_record()=={**original,'replayed':True}


@pytest.mark.parametrize('table',['target_commands','target_members','authorization_rounds','job_authorization_heads','events','settings'])
def test_real_insert_fault_rolls_back_every_target_role(tmp_path,table):
    with closing(SQLiteStore(tmp_path/'rollback.db')) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
        store._connection.execute(f"CREATE TRIGGER fail_target BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'target failure'); END")
        before=tuple(store._connection.iterdump())
        with pytest.raises(RuntimeError,match='target_authority_corrupt'):authorize(store)
        assert tuple(store._connection.iterdump())==before
        store._connection.execute('DROP TRIGGER fail_target')
        assert authorize(store)['results'][0]['outcome']=='new_authority'


@pytest.mark.parametrize('damage',[
    'DELETE FROM job_authorization_heads',
    'DELETE FROM settings WHERE key LIKE "authorization_round:%"',
    'UPDATE settings SET value="01" WHERE key LIKE "authorization_round:%"',
    'UPDATE job_authorization_heads SET current_worker_epoch=2',
    'UPDATE job_authorization_heads SET activation_request_id="missing"',
    'UPDATE target_commands SET vector_digest=printf("%064d",0)',
    'UPDATE add_batch_entries SET creation_intent_blob=CAST("{}" AS BLOB)',
    'DELETE FROM command_receipts WHERE scope="add_batch"',
    'UPDATE jobs SET source_url=CAST("https://example.test/changed" AS BLOB)',
    'UPDATE materialized_jobs SET manual_hold=1',
])
def test_target_corruption_refuses_without_repair(tmp_path,damage):
    with closing(SQLiteStore(tmp_path/'corrupt.db')) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()));authorize(store)
        store._connection.execute('PRAGMA foreign_keys=OFF');store._connection.execute(damage);store._connection.execute('PRAGMA foreign_keys=ON')
        before=tuple(store._connection.iterdump())
        with pytest.raises(RuntimeError,match='target_authority_corrupt'):authorize(store,'new')
        assert tuple(store._connection.iterdump())==before


def test_supported_single_uses_actual_typed_creation_and_v1_barrier_without_head(tmp_path,monkeypatch):
    from hermes_downloads import direct, paths
    with closing(SQLiteStore(tmp_path/'single.db')) as store:
        store.recover_cold_start()
        add=ipc.JobAddCommand('single','add-single','https://example.test/single',0,4,'Other','single.bin','single.bin')
        worker._job_add_from_store(store,add)
        assert store.get_batch_creation_intent('single') is None
        assert authorize(store,'resume','resume',select_jobs(['single']))['results'][0]['reason']=='incompatible_authority'
        def forbidden(*a,**k):pytest.fail('foundation reached execution/filesystem entry point')
        for name in ('start','add_paused','resume','resolve_source','observe'):
            if hasattr(direct.DirectAria2Controller,name):monkeypatch.setattr(direct.DirectAria2Controller,name,forbidden)
        for name in ('resolve_destination','attest_staged_partial_payload','prepare_publication_payload','publish_staged_partial_payload','_hash_publication_payload','_link_staged_partial_payload'):
            monkeypatch.setattr(paths,name,forbidden)
        result=authorize(store,'grant',selector=select_jobs(['single']))
        assert result['results'][0]['outcome']=='new_authority'
        store._connection.execute('DELETE FROM job_authorization_heads')
        before=store.get_materialized_job('single')
        assert control(store,'single','start_now','v1').status=='blocked'
        dispatch=store.prepare_direct_dispatch(job_id='single',expected_worker_epoch=1,expected_generation=0,
            expected_revision=before.intent.revision,request_id='dispatch',payload_digest='b'*64,controller_ready=True,
            now=store_module.datetime.now(store_module.UTC))
        assert dispatch.status=='blocked' and store.get_materialized_job('single')==before
        assert store._connection.execute('SELECT count(*) FROM authorization_rounds').fetchone()[0]==1
        evidence('foundation-effects.jsonl',dict(body=0,engine=0,source_probe=0,destination=0,hash=0,link=0))


def v17_fixture(database):
    # Actual original batch rows, no target grants; only the fixture catalog is v17.
    with closing(SQLiteStore(database)) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
    with sqlite3.connect(database,isolation_level=None) as connection:
        assert connection.execute('SELECT count(*) FROM target_dispatch_causes').fetchone()[0]==0
        assert connection.execute("SELECT count(*) FROM direct_dispatch_commands WHERE producer_kind!='legacy'").fetchone()[0]==0
        connection.execute('DROP TABLE target_dispatch_causes')
        connection.execute('ALTER TABLE direct_dispatch_commands DROP COLUMN producer_kind')
        for name in ('direct_cleanup_claims','job_authorization_heads','target_members','authorization_rounds','target_commands'):
            assert connection.execute(f'SELECT count(*) FROM {name}').fetchone()[0]==0
            connection.execute(f'DROP TABLE {name}')
        registry=connection.execute('SELECT * FROM command_receipts').fetchall()
        connection.execute('DROP TABLE command_receipts');connection.execute(store_module._V17_COMMAND_RECEIPTS_SCHEMA)
        connection.executemany('INSERT INTO command_receipts VALUES (?,?,?,?)',registry)
        connection.execute('PRAGMA user_version=17')
        connection.row_factory=sqlite3.Row
        assert SQLiteStore._has_table_schemas(connection,store_module._V17_TABLE_SCHEMAS)
        assert connection.execute('PRAGMA foreign_key_check').fetchall()==[]
        return {name:[tuple(r) for r in connection.execute(f'SELECT * FROM {name}')]
            for name in store_module._V17_TABLE_SCHEMAS}


def test_populated_v17_migration_preserves_original_bytes_and_restores_fks(tmp_path):
    database=tmp_path/'v17.db';before=v17_fixture(database)
    with closing(SQLiteStore(database)) as store:
        assert store._connection.execute('PRAGMA user_version').fetchone()[0]==20
        assert store._connection.execute('PRAGMA foreign_keys').fetchone()[0]==1
        assert store._connection.execute('PRAGMA foreign_key_check').fetchall()==[]
        for table,rows in before.items():
            columns='request_id,payload_digest,job_id,status,generation,revision,state' if table=='direct_dispatch_commands' else '*'
            assert [tuple(r) for r in store._connection.execute(f'SELECT {columns} FROM {table}')]==rows
        assert store._connection.execute("SELECT count(*) FROM direct_dispatch_commands WHERE producer_kind!='legacy'").fetchone()[0]==0
        for name in ('target_commands','target_members','authorization_rounds','job_authorization_heads'):
            assert store._connection.execute(f'SELECT count(*) FROM {name}').fetchone()[0]==0
        assert store.get_batch_creation_intent('batch-job-0') is not None
        assert authorize(store)['results'][0]['outcome']=='new_authority'


class FaultConnection:
    """Inject one fault around a real connection; no transaction/digest doubles."""
    def __init__(self,connection,prefix):self.connection=connection;self.prefix=prefix
    def __getattr__(self,name):return getattr(self.connection,name)
    def execute(self,sql,*args):
        if sql.lstrip().startswith(self.prefix):raise sqlite3.OperationalError('injected target fault')
        return self.connection.execute(sql,*args)
    def commit(self):
        if self.prefix=='COMMIT':raise sqlite3.OperationalError('injected target commit fault')
        return self.connection.commit()


@pytest.mark.parametrize('prefix',['CREATE TABLE target_registry_v18','INSERT INTO target_registry_v18',
    'DROP TABLE command_receipts','ALTER TABLE target_registry_v18','CREATE TABLE target_commands',
    'CREATE TABLE target_members','CREATE TABLE authorization_rounds','CREATE TABLE job_authorization_heads',
    'PRAGMA foreign_key_check','PRAGMA user_version = 18','COMMIT'])
def test_v18_ddl_copy_check_version_commit_fault_is_one_rollback(tmp_path,prefix):
    database=tmp_path/'migration.db';v17_fixture(database)
    with sqlite3.connect(database,isolation_level=None) as connection:
        connection.row_factory=sqlite3.Row;connection.execute('PRAGMA foreign_keys=ON')
        before=tuple(connection.iterdump())
        with pytest.raises(sqlite3.OperationalError):SQLiteStore._migrate_schema_v15(FaultConnection(connection,prefix))
        assert connection.execute('PRAGMA foreign_keys').fetchone()[0]==1
        assert connection.execute('PRAGMA user_version').fetchone()[0]==17
        assert tuple(connection.iterdump())==before


def test_target_commit_fault_and_epoch_stale_are_atomic(tmp_path):
    with closing(SQLiteStore(tmp_path/'commit.db')) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
        connection=store._connection;before=tuple(connection.iterdump())
        store._connection=FaultConnection(connection,'COMMIT')
        try:
            with pytest.raises(RuntimeError,match='target_authority_corrupt'):authorize(store)
        finally:store._connection=connection
        assert tuple(connection.iterdump())==before
        with pytest.raises(RuntimeError,match='target_epoch_stale'):
            store.apply_target_authorize(ipc.TargetAuthorizeCommand.from_record(target(epoch=2)))
        assert tuple(connection.iterdump())==before


@pytest.mark.parametrize('completion',[False,True])
def test_exact_paused_publication_target_audit_cold_and_terminal_hooks(tmp_path,completion,monkeypatch):
    from test_add_batch import _queued_batch_attempt
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path/'publication.db')) as store:
        command,_,captured,old,old_receipt,destination,stage,_,closed=_queued_batch_attempt(store,'resume')
        fresh,prepared=stage('fresh-producer');attempt=store.reserve_direct_publication(fresh,prepared)
        published=None
        if completion:
            published=paths.publish_staged_partial_payload(destination,prepared.reservation,prepared.staged_payload,
                prepared=prepared,creation_permit=paths.PublicationCreationPermit())
        assert store.recover_cold_start()==3
        selector=select_jobs(['batch-job-0'])
        grant=authorize(store,'grant',selector=selector)
        assert grant['results'][0]['outcome']=='new_authority'
        assert control(store,'batch-job-0','pause','manual-pause',
            _contained_direct_transfer=True,_publication_recoverable=True).status=='applied'
        before=store._connection.execute('SELECT * FROM direct_publication_attempts').fetchone()
        resumed=authorize(store,'resume',action='resume',selector=selector)
        assert resumed['results'][0]['outcome']=='existing_authority'
        current=store._read_publication_attempt(store._connection,'batch-job-0')
        assert current.state=='paused' and current.proof==attempt.proof and current.attempt_id==attempt.attempt_id
        assert current.revision==before['revision']+1
        assert store.get_batch_creation_intent('batch-job-0')==captured
        assert tuple(store._connection.execute('SELECT * FROM closed_direct_publication_attempts').fetchone())==closed
        assert store.recover_cold_start()==4
        authorize(store,'reactivate',action='start_now',selector=selector)
        current=store._read_publication_attempt(store._connection,'batch-job-0')
        if completion:
            assert store.complete_direct_publication(current,published).state=='completed'
            assert destination.final_path.read_bytes()==b'body'
        else:
            assert control(store,'batch-job-0','remove','remove').state=='removed'
        assert store._connection.execute('SELECT intent_status FROM job_authorization_heads').fetchone()[0]=='terminal'
        assert store._connection.execute('SELECT status FROM authorization_rounds').fetchone()[0]=='terminal'
        assert store.get_batch_creation_intent('batch-job-0')==captured
        assert tuple(store._connection.execute('SELECT * FROM direct_dispatch_commands WHERE request_id="original-dispatch"').fetchone())==old_receipt
        evidence('target-publication-lineage.jsonl',dict(completed=completion,original_attempt=old.attempt_id,
            current_attempt=attempt.attempt_id,original_receipt_preserved=True,archive_preserved=True,terminal_round=True))


@pytest.mark.parametrize('first',['add','batch','queue','control','dispatch','target'])
def test_request_ids_conflict_across_every_real_surface(tmp_path,first):
    with closing(SQLiteStore(tmp_path/'conflict.db')) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
        if first=='add':worker._job_add_from_store(store,ipc.JobAddCommand('s','collision','https://example.test/s',0,2,'Other','s.bin','s.bin'))
        elif first=='batch':store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope([entry(2)],request_id='collision')))
        elif first=='queue':store.apply_queue_gate(gate='running',request_id='collision',payload_digest='a'*64,expected_revision=1)
        elif first=='control':control(store,'batch-job-0','pause','collision')
        elif first=='dispatch':store.prepare_direct_dispatch(job_id='batch-job-0',expected_worker_epoch=1,
            expected_generation=0,expected_revision=0,request_id='collision',payload_digest='a'*64,
            controller_ready=False,now=store_module.datetime.now(store_module.UTC))
        else:authorize(store,'collision')
        before=tuple(store._connection.iterdump())
        if first!='target':
            with pytest.raises(RequestConflictError):authorize(store,'collision')
        else:
            operations=[lambda:worker._job_add_from_store(store,ipc.JobAddCommand('s','collision','https://example.test/s',0,2,'Other','s.bin','s.bin')),
                lambda:store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope([entry(2)],request_id='collision'))),
                lambda:store.apply_queue_gate(gate='running',request_id='collision',payload_digest='a'*64,expected_revision=1),
                lambda:control(store,'batch-job-0','pause','collision'),
                lambda:store.prepare_direct_dispatch(job_id='batch-job-0',expected_worker_epoch=1,expected_generation=0,
                    expected_revision=1,request_id='collision',payload_digest='a'*64,controller_ready=True,
                    now=store_module.datetime.now(store_module.UTC))]
            for operation in operations:
                with pytest.raises(RequestConflictError):operation()
        assert tuple(store._connection.iterdump())==before


def test_unregistered_target_receipt_never_opens_id_reuse(tmp_path):
    with closing(SQLiteStore(tmp_path/'registry.db')) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()));authorize(store)
        store._connection.execute('PRAGMA foreign_keys=OFF')
        store._connection.execute('DELETE FROM command_receipts WHERE request_id="target"')
        store._connection.execute('PRAGMA foreign_keys=ON');before=tuple(store._connection.iterdump())
        with pytest.raises(RuntimeError):worker._job_add_from_store(store,
            ipc.JobAddCommand('s','target','https://example.test/s',0,2,'Other','s.bin','s.bin'))
        assert tuple(store._connection.iterdump())==before


@pytest.mark.parametrize('fault',['lock','progress'])
def test_target_real_database_deadline_rolls_back_and_restores_handlers(tmp_path,fault):
    database=tmp_path/'deadline.db'
    with closing(SQLiteStore(database)) as store, sqlite3.connect(database,isolation_level=None) as blocker:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
        connection=store._connection;busy=connection.execute('PRAGMA busy_timeout').fetchone()[0]
        if fault=='progress':connection.execute('''CREATE TRIGGER slow_target BEFORE INSERT ON settings
            BEGIN SELECT sum(n) FROM (WITH RECURSIVE values_to_sum(n) AS
            (VALUES(1) UNION ALL SELECT n+1 FROM values_to_sum WHERE n<100000000) SELECT n FROM values_to_sum); END''')
        else:blocker.execute('BEGIN IMMEDIATE')
        before=tuple(connection.iterdump());started=time.monotonic()
        with pytest.raises(RuntimeError,match='target_deadline'):authorize(store)
        seconds=time.monotonic()-started;blocker.rollback()
        assert tuple(connection.iterdump())==before
        assert connection.execute('PRAGMA busy_timeout').fetchone()[0]==busy
        assert connection.execute('WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<50000) SELECT max(x) FROM n').fetchone()[0]==50000
        if fault=='progress':connection.execute('DROP TRIGGER slow_target')
        assert authorize(store)['results'][0]['outcome']=='new_authority'
        evidence('database-deadlines.jsonl',dict(fault=fault,seconds=seconds,rollback=True,busy_restored=True,progress_restored=True,
            syscall_or_commit_preemption_claim=False))


def test_response_loss_reopen_and_uncertain_committed_reply_replay_original(service,tmp_path):
    root,_=service;exchange(root,canonical(envelope()))
    command=ipc.TargetAuthorizeCommand.from_record(target())
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as connection:
        connection.connect(str(root/'worker.sock'))
        connection.sendall(b'HDT2\n'+len(command._wire_request).to_bytes(4,'big')+command._wire_request)
        connection.shutdown(socket.SHUT_WR)
    assert ipc.request_health(root/'worker.sock').worker_epoch==1
    replay=ipc.target_authorize(root/'worker.sock',command).to_record();assert replay['replayed'] is True
    database=tmp_path/'uncertain.db'
    with closing(SQLiteStore(database)) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
        original_connection=store._connection
        class UncertainCommit(FaultConnection):
            def commit(self):
                self.connection.commit();raise OSError('response uncertain after real commit')
        store._connection=UncertainCommit(original_connection,'NONE')
        try:
            with pytest.raises(RuntimeError,match='target_authority_corrupt'):store.apply_target_authorize(command)
        finally:store._connection=original_connection
        assert store.apply_target_authorize(command).to_record()==replay
    with closing(SQLiteStore(database)) as store:
        store.recover_cold_start();before=tuple(store._connection.iterdump())
        assert store.apply_target_authorize(command).to_record()==replay
        assert tuple(store._connection.iterdump())==before


def test_real_supported_single_creation_counters_are_not_assumed_zero(tmp_path):
    from dataclasses import replace
    from hermes_downloads.models import DownloadIntent,MaterializedJob,SourceKind
    with closing(SQLiteStore(tmp_path/'nonzero.db')) as store:
        store.recover_cold_start()
        command=ipc.JobAddCommand('s','original','https://example.test/s',0,3,'Other','s.bin','s.bin')
        intent=DownloadIntent('s','original',command.payload_digest,command.source_url.encode(),None,17,29)
        job=MaterializedJob('s',intent,SourceKind.DIRECT,None,0,3,None,False,False,False,'Other',None,'s.bin','s.bin')
        store.apply_add(intent,materialized=job)
        result=authorize(store,selector=select_jobs(['s']))['results'][0]
        assert (result['captured_generation'],result['captured_revision'])==(17,29)
        assert store.get_job('s').generation==17 and store.get_job('s').revision==30
        assert tuple(store._connection.execute('SELECT creation_generation,creation_revision FROM target_members').fetchone())==(17,29)


@pytest.mark.parametrize('held_mask',range(16))
def test_unsupported_scheduled_single_reports_all_observed_gates_without_grant(tmp_path,held_mask):
    from hermes_downloads.models import DownloadIntent,MaterializedJob,SourceKind
    with closing(SQLiteStore(tmp_path/'unsupported.db')) as store:
        store.recover_cold_start()
        intent=DownloadIntent('s','original','a'*64,b'https://example.test/s',None,4,7)
        due=store_module.datetime(2032 if held_mask&8 else 2000,1,1,tzinfo=store_module.UTC)
        job=MaterializedJob('s',intent,SourceKind.DIRECT,'cohort',0,3,due,False,bool(held_mask&4),False,'Other',None,'s.bin','s.bin')
        store.apply_add(intent,materialized=job)
        if not held_mask&1:store.apply_queue_gate(gate='running',request_id='open',payload_digest='b'*64,expected_revision=1)
        if held_mask&2:store.set_collection_hold('cohort',held=True)
        selector=dict(kind='creation_cohort',creation=dict(kind='single',request_id='original'),indices=[0])
        result=authorize(store,selector=selector)['results'][0]
        assert result['reason']=='incompatible_authority'
        assert result['held_by']==[name for bit,name in enumerate(['global','collection','manual','not_due']) if held_mask&(1<<bit)]
        assert store._connection.execute('SELECT count(*) FROM authorization_rounds').fetchone()[0]==0
        assert store.get_materialized_job('s')==job


def test_cold_refuses_positive_member_with_missing_head_without_epoch_write(tmp_path):
    with closing(SQLiteStore(tmp_path/'cold-missing-head.db')) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()));authorize(store)
        store._connection.execute('DELETE FROM job_authorization_heads')
        before=tuple(store._connection.iterdump())
        with pytest.raises(ValueError,match='head missing'):store.recover_cold_start()
        assert tuple(store._connection.iterdump())==before


def test_counter_last_round_requires_real_causal_members_without_allocation(tmp_path):
    with closing(SQLiteStore(tmp_path/'counter-last.db')) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()));authorize(store)
        store._connection.execute('INSERT INTO authorization_rounds SELECT cohort_kind,cohort_request_id,2,origin_request_id,member_count,vector_digest,status FROM authorization_rounds')
        store._connection.execute('UPDATE settings SET value="2",revision=2 WHERE key LIKE "authorization_round:%"')
        before=tuple(store._connection.iterdump())
        with pytest.raises(RuntimeError,match='target_authority_corrupt'):authorize(store,'last-round')
        assert tuple(store._connection.iterdump())==before


@pytest.mark.parametrize('error',[[],{}])
def test_target_client_refuses_malformed_error_reply(tmp_path,error):
    from threading import Thread
    from tempfile import TemporaryDirectory
    with TemporaryDirectory(prefix='ht-',dir='/tmp') as directory, socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as server:
        path=Path(directory)/'reply.sock'
        server.bind(str(path));server.listen(1)
        def reply():
            with server.accept()[0] as client:
                while client.recv(4096):pass
                body=canonical(dict(error=error))
                client.sendall(b'HDT2\n'+len(body).to_bytes(4,'big')+body)
        thread=Thread(target=reply);thread.start()
        try:
            with pytest.raises(ipc.IPCError,match='ipc_response_invalid'):
                ipc.target_authorize(path,ipc.TargetAuthorizeCommand.from_record(target()))
        finally:
            thread.join(5)
            assert not thread.is_alive()


def test_real_500_single_cohorts_maximum_captured_counters_and_overflow(tmp_path):
    from hermes_downloads.models import DownloadIntent,MaterializedJob,SourceKind
    maximum=(1<<63)-1
    with closing(SQLiteStore(tmp_path/'500-single.db')) as store:
        store.recover_cold_start();jobs=[]
        for index in range(500):
            job_id='J'+format(index,'03d')+'x'*124;jobs.append(job_id)
            command=ipc.JobAddCommand(job_id,'original-'+str(index),'https://example.test/'+str(index),0,index,'Other',str(index)+'.bin',str(index)+'.bin')
            intent=DownloadIntent(job_id,command.request_id,command.payload_digest,command.source_url.encode(),None,maximum,maximum-1)
            job=MaterializedJob(job_id,intent,SourceKind.DIRECT,None,0,index,None,False,False,False,'Other',None,command.partial_filename,command.selected_final_filename)
            store.apply_add(intent,materialized=job)
        command=ipc.TargetAuthorizeCommand.from_record(target('R'+'x'*127,selector=select_jobs(jobs,maximum-1)))
        started=time.monotonic();result=store.apply_target_authorize(command);seconds=time.monotonic()-started
        assert seconds<2 and len(result._wire_reply)<=256*1024
        assert all((r['outcome'],r['round_generation'],r['captured_generation'],r['captured_revision'])==('new_authority',1,maximum,maximum-1) for r in result.to_record()['results'])
        assert store._connection.execute('SELECT count(*) FROM authorization_rounds').fetchone()[0]==500
        assert store._connection.execute('SELECT count(*) FROM settings WHERE key LIKE "authorization_round:%" AND value="1" AND revision=1').fetchone()[0]==500
        existing=authorize(store,'existing','start',select_jobs(jobs,maximum))
        assert all(r['outcome']=='existing_authority' and r['captured_revision']==maximum for r in existing['results'])
        before=tuple(store._connection.iterdump())
        with pytest.raises(RuntimeError,match='target_authority_corrupt'):authorize(store,'overflow','start_now',select_jobs([jobs[0]],maximum))
        assert tuple(store._connection.iterdump())==before
        evidence('measured-500-single.jsonl',dict(real_creation='typed single add owner',positions=500,cohorts=500,rounds=500,captured_generation=maximum,captured_revision=maximum-1,request_bytes=len(command._wire_request),reply_bytes=len(result._wire_reply),database_seconds=seconds,overflow_rollback=True))
