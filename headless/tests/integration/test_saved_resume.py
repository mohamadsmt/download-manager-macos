"""Current-process immutable-SHA saved resume, with private synthetic evidence."""
import pytest
from hermes_downloads import ipc, mcp_server
from test_direct_publication import _helpers


def test_saved_resume_public_envelope():
    command = mcp_server._parse_control_arguments(dict(scope='job', action='resume',
        request_id='saved-resume', job='batch-job-0', expected_revision='5', expected_worker_epoch='1'))
    assert type(command) is ipc.TargetAuthorizeCommand
    assert command.action == 'resume'
    assert command.to_record()['selector'] == dict(kind='jobs', targets=[dict(job='batch-job-0', expected_revision=5)])


def test_origin_accounts_only_completed_body_writes():
    origin = _helpers._origin_type()()
    class Writer:
        def __init__(self): self.calls = 0
        def write(self, body):
            self.calls += 1
            if self.calls == 2: raise BrokenPipeError
            return len(body)
        def flush(self): pass
    class Handler:
        wfile = Writer()
        def send_response(self, status): pass
        def send_header(self, *args): pass
        def end_headers(self): pass
    body = b'x' * (2 * _helpers._FIXTURE_MODULE._RANGE_PACED_CHUNK_BYTES)
    assert origin._send_response(Handler(), 200, {}, body, pace_large_range=True) == len(body)//2

import asyncio
from contextlib import closing
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import socket
import tempfile
import time
from mcp import ClientSession
from mcp.client.stdio import stdio_client
from hermes_downloads import paths, processes
from hermes_downloads.store import SQLiteStore
from test_mcp_control import body_worker, sdk_lifecycle, parameters, result_record, entry, targets


def rows(database, sql, arguments=()):
    with closing(sqlite3.connect(database)) as connection:
        return connection.execute(sql,arguments).fetchall()


@pytest.mark.parametrize('outcome', ['success','no-range','wrong-bytes','repeated'])
def test_real_saved_resume_default_stock_and_immutable_publication(tmp_path, monkeypatch, sdk_lifecycle, outcome):
    evidence_root = Path(os.environ.get('T26_RESUME_RUN', str(tmp_path))).resolve()
    evidence = Path(tempfile.mkdtemp(prefix='saved-'+outcome+'-',dir=evidence_root)); evidence.chmod(0o700)
    home = evidence/'home'; home.mkdir(mode=0o700)
    monkeypatch.setenv('HOME',str(home))
    output = home/'Downloads/Hermes'; output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.01)
    with tempfile.TemporaryDirectory(prefix='sr12-',dir='/private/tmp') as temporary:
        state = Path(temporary); state.chmod(0o700)
        with _helpers._origin_type()(payload_size=8*1024*1024) as origin:
            origin_thread = origin._thread
            expected = hashlib.sha256(origin.payload).hexdigest()
            with body_worker(state,origin.url(),evidence) as process:
                async def exercise():
                    async with stdio_client(parameters(state)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            discovered = await session.list_tools()
                            assert [tool.name for tool in discovered.tools] == ['downloads_add','downloads_control','downloads_edit','downloads_replace_source','downloads_files','downloads_configure','downloads_query']
                            async def call(name,arguments):
                                return result_record(await session.call_tool(name,arguments=arguments))
                            value = entry(source_url=origin.url(),partial_filename='saved.bin',selected_final_filename='saved.bin',expected_sha256=expected)
                            await call('downloads_add',dict(request_id='saved-add',items=[value]))
                            original_authorization = await call('downloads_control',targets(['batch-job-0']))
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='saved-run',expected_revision='1'))
                            partial = output/'.incomplete/batch-job-0/saved.bin'
                            control = partial.with_name('saved.bin.aria2')
                            deadline = time.monotonic()+15
                            while not partial.exists() or partial.stat().st_blocks*512 < 3*1024*1024:
                                assert process.is_alive() and time.monotonic()<deadline
                                await asyncio.sleep(.02)
                            with closing(SQLiteStore(state/'state.db')) as store:
                                engine = next((owner[1] for owner in store._job_engine_owners() if owner[0]=='batch-job-0'),None)
                            assert engine is not None
                            original = rows(state/'state.db',"SELECT * FROM direct_dispatch_commands WHERE producer_kind='target_body'")[0]
                            original_cause = rows(state/'state.db',"SELECT * FROM target_dispatch_causes WHERE kind='body'")[0]
                            current = await call('downloads_query',dict(scope='status',id='batch-job-0'))
                            pause = await call('downloads_control',dict(scope='job',action='pause',request_id='saved-pause',job='batch-job-0',expected_revision=str(current['record']['revision'])))
                            acknowledged = time.monotonic()
                            assert pause['status']=='applied' and pause['state']=='paused'
                            assert processes.reconcile_process_birth(engine.identity)=='absent'
                            with pytest.raises(ProcessLookupError): os.killpg(engine.identity.process_group_id,0)
                            deadline = time.monotonic()+5
                            while not rows(state/'state.db','SELECT * FROM stopped_direct_bodies'):
                                assert process.is_alive() and time.monotonic()<deadline, 'stopped checkpoint missing'
                                await asyncio.sleep(.02)
                            checkpoint = rows(state/'state.db','SELECT * FROM stopped_direct_bodies')[0]
                            assert control.exists() and control.stat().st_size>0
                            originals = [(file.stat().st_ino,hashlib.sha256(file.read_bytes()).hexdigest()) for file in (partial,control)]
                            marker = partial.parent/'.hermes-reservation'
                            original_marker = (marker.stat().st_ino,marker.read_bytes())
                            deadline = time.monotonic()+3
                            while not origin.ledger.entries:
                                assert time.monotonic()<deadline
                                await asyncio.sleep(.02)
                            body1 = origin.ledger
                            assert 0<body1.response_body_bytes< len(origin.payload)
                            if outcome=='no-range':
                                response = origin._response_for
                                origin._response_for = lambda endpoint,range_header: (200,{},origin.payload)
                            elif outcome=='wrong-bytes':
                                origin.payload = origin.changed_payload
                            # A global hold prevents preparation; the explicit job intent is still replayable.
                            await call('downloads_control',dict(scope='queue',action='pause',request_id='saved-hold',expected_revision='2'))
                            stale = await call('downloads_control',dict(scope='job',action='resume',request_id='stale-resume',job='batch-job-0',expected_revision=str(int(pause['revision'])-1),expected_worker_epoch='1'))
                            assert stale['results'][0]['outcome']=='stale'
                            epoch = await session.call_tool('downloads_control',arguments=dict(scope='job',action='resume',request_id='stale-epoch',job='batch-job-0',expected_revision=pause['revision'],expected_worker_epoch='2'))
                            assert epoch.isError
                            assert rows(state/'state.db','SELECT * FROM saved_direct_continuations')==[]
                            resume = dict(scope='job',action='resume',request_id='saved-resume',job='batch-job-0',expected_revision=pause['revision'],expected_worker_epoch='1')
                            receipt = await call('downloads_control',resume)
                            assert receipt['results'][0]['outcome']=='existing_authority' and receipt['execution_effect']=='none'
                            assert receipt['results'][0]['held_by']==['global']
                            # Drop an actual owner replay reply, then recover through the public SDK.
                            native=mcp_server._parse_control_arguments(resume)
                            with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as lost:
                                lost.settimeout(2);lost.connect(str(state/'worker.sock'))
                                wire=json.dumps(native.to_record(),separators=(',',':')).encode()
                                lost.sendall(b'HDM2\n'+len(wire).to_bytes(4,'big')+wire)
                                lost.shutdown(socket.SHUT_WR)
                            assert await call('downloads_control',resume)==receipt|dict(replayed=True)
                            repeated=await call('downloads_control',resume|dict(request_id='unsupported-repeat',expected_revision=str(int(pause['revision'])+1)))
                            assert repeated['results'][0]['reason']=='incompatible_authority'
                            assert partial.stat().st_ino==originals[0][0]
                            assert rows(state/'state.db','SELECT phase FROM saved_direct_continuations')==[('intent',)]
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='saved-go',expected_revision='3'))
                            retained=[partial.parent/name for name in paths._SAVED_NAMES]
                            end=time.monotonic()+5
                            while not all(file.exists() and file.stat().st_nlink==1 for file in retained):
                                assert time.monotonic()<end
                                await asyncio.sleep(.02)
                            for file,(inode,digest) in zip(retained,originals):
                                assert file.stat().st_ino==inode and hashlib.sha256(file.read_bytes()).hexdigest()==digest
                            if outcome=='repeated':
                                end=time.monotonic()+12
                                while len(rows(state/'state.db',"SELECT * FROM target_dispatch_causes WHERE kind='body'"))<2 or not partial.exists() or partial.stat().st_blocks*512<5*1024*1024:
                                    assert time.monotonic()<end
                                    await asyncio.sleep(.02)
                                current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                pause2=await call('downloads_control',dict(scope='job',action='pause',request_id='pause2',job='batch-job-0',expected_revision=str(current['revision'])))
                                end=time.monotonic()+4
                                while len(rows(state/'state.db','SELECT * FROM stopped_direct_bodies UNION ALL SELECT '+','.join(['job_id','source_request_id','pause_request_id','pause_audit_id','worker_epoch','generation','revision','checkpoint_digest','facts','closure'])+' FROM stopped_direct_cycles'))!=2:
                                    assert time.monotonic()<end, 'second stopped checkpoint missing'
                                    await asyncio.sleep(.02)
                                resume2=await call('downloads_control',dict(scope='job',action='resume',request_id='resume2',job='batch-job-0',expected_revision=pause2['revision'],expected_worker_epoch='1'))
                                assert resume2['results'][0]['outcome']=='existing_authority'
                            deadline = time.monotonic()+25
                            while True:
                                status = (await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                bodies = rows(state/'state.db',"SELECT request_id,admission_serial FROM target_dispatch_causes WHERE kind='body' ORDER BY admission_serial")
                                if status['state']=='completed' or (len(bodies)==2 and status['state']=='paused'): break
                                assert process.is_alive() and time.monotonic()<deadline, (status,bodies,rows(state/'state.db','SELECT phase,facts FROM saved_direct_continuations'))
                                await asyncio.sleep(.05)
                            assert len(bodies)==(3 if outcome=='repeated' else 2) and [serial for _,serial in bodies]==list(range(1,len(bodies)+1))
                            assert await call('downloads_control',resume)==receipt|dict(replayed=True)
                            assert await call('downloads_control',targets(['batch-job-0']))==original_authorization|dict(replayed=True)
                            assert rows(state/'state.db','SELECT * FROM stopped_direct_bodies')[0]==checkpoint
                            assert rows(state/'state.db','SELECT * FROM direct_dispatch_commands WHERE request_id=?',(original[0],))[0]==original
                            assert rows(state/'state.db','SELECT * FROM target_dispatch_causes WHERE request_id=?',(original_cause[0],))[0]==original_cause
                            retained = [partial.parent/name for name in paths._SAVED_NAMES]
                            if outcome not in {'success','repeated'}:
                                for file,(inode,digest) in zip(retained,originals):
                                    assert file.stat().st_ino==inode and file.stat().st_nlink==1
                                    assert hashlib.sha256(file.read_bytes()).hexdigest()==digest
                            final = output/'Other/saved.bin'
                            if outcome in {'success','repeated'}:
                                assert status['state']=='completed' and hashlib.sha256(final.read_bytes()).hexdigest()==expected
                                deadline = time.monotonic()+4
                                while not rows(state/'state.db',"SELECT phase FROM direct_cleanup_claims WHERE phase='finished'"):
                                    assert time.monotonic()<deadline
                                    await asyncio.sleep(.02)
                                assert not marker.exists()
                                names=rows(state/'state.db','SELECT retained_names FROM saved_resume_bindings')
                                assert names and all(not (partial.parent/name).exists() for value, in names for name in json.loads(value)), 'retained originals escaped successful cleanup'
                            else:
                                assert status['state']=='paused' and not final.exists()
                                assert (marker.stat().st_ino,marker.read_bytes())==original_marker
                            ledger = origin.ledger
                            body2 = ledger.response_body_bytes-body1.response_body_bytes
                            assert ledger.dropped_entries==0 and ledger.response_body_bytes<=2*len(origin.payload) and ledger.request_count<=24
                            if outcome in {'success','repeated'}:
                                assert 0<body2<len(origin.payload), (body1,ledger)
                                assert any(item.range_header is not None and item.body_bytes>0 for item in ledger.entries[len(body1.entries):])
                            closures = [json.loads(line) for line in (evidence/f'fixture-{process.pid}.jsonl').read_text().splitlines()]
                            assert any(row['kind']=='engine-waitable-child-reaped' and row['identity']==engine.identity.to_record() and row['waitpid']=='ECHILD' for row in closures)
                            effects = [json.loads(line) for line in (evidence/'scheduler-effects.jsonl').read_text().splitlines()]
                            assert any(row['kind']=='observer-join' and not row['alive'] and row['monotonic']<=acknowledged for row in effects)
                            starts = [row['cause'] for row in effects if row['kind']=='causal-engine-start']
                            assert len(starts)==len(bodies)
                            stock = [row for row in effects if row['kind']=='saved-stock-paused']
                            assert len(stock)==len(bodies)-1 and all(row['expected_sha256'] is None for row in stock)
                            assert stock[0]['gid'] != json.loads(checkpoint[-1])['gid']
                            assert stock[0]['options']=={'split':'4','max-connection-per-server':'4','continue':'true',
                                'always-resume':'true','allow-overwrite':'false','auto-file-renaming':'false'}
                            record = dict(outcome=outcome,expected_sha256=expected,state=status['state'],body1=asdict(body1),body2_successful_bytes=body2,
                                full_payload_bytes=len(origin.payload),ledger=asdict(ledger),bodies=bodies,original_receipt=list(original),original_cause=list(original_cause),
                                lost_owner_reply_recovered_through_sdk=True,stock=stock,original_engine=engine.identity.to_record(),original_wait_reaped=True,original_observer_join_before_ack=True,
                                retained=[dict(inode=inode,sha256=digest) for inode,digest in originals],retained_cleanup='OPEN',synthetic=True)
                            (evidence/'result.json').write_text(json.dumps(record,indent=2)); (evidence/'result.json').chmod(0o600)
                asyncio.run(exercise())
            assert not process.is_alive()
        assert not origin_thread.is_alive()
        (evidence/'origin-joined.json').write_text(json.dumps(dict(joined=True,ident=origin_thread.ident)))


@pytest.mark.parametrize('cooperative',[False,True])
@pytest.mark.parametrize('action',['pause','remove','queue'])
def test_public_cancel_joins_saved_lane_and_late_clone_never_readies(tmp_path,monkeypatch,sdk_lifecycle,action,cooperative):
    import multiprocessing
    evidence=Path(tempfile.mkdtemp(prefix='saved-cancel-'+action+'-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path)))).resolve()
    home=evidence/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home))
    output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    context=multiprocessing.get_context('spawn')
    entered,release=context.Event(),context.Event()
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.01)
    with tempfile.TemporaryDirectory(prefix='srcancel-',dir='/private/tmp') as directory:
        state=Path(directory);state.chmod(0o700)
        with _helpers._origin_type()(payload_size=8*1024*1024) as origin:
            origin_thread=origin._thread
            with body_worker(state,origin.url(),evidence,barrier='saved-clone-cancellable' if cooperative else 'saved-clone',entered=entered,release=release) as process:
                async def exercise():
                    async with stdio_client(parameters(state)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args): return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='add',items=[entry(source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest(),partial_filename='cancel.bin',selected_final_filename='cancel.bin')]))
                            await call('downloads_control',targets(['batch-job-0']))
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='run',expected_revision='1'))
                            partial=output/'.incomplete/batch-job-0/cancel.bin'
                            end=time.monotonic()+12
                            while not partial.exists() or partial.stat().st_blocks*512 < 2*1024*1024:
                                assert time.monotonic()<end and process.is_alive()
                                await asyncio.sleep(.02)
                            current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                            pause=await call('downloads_control',dict(scope='job',action='pause',job='batch-job-0',request_id='pause',expected_revision=str(current['revision'])))
                            end=time.monotonic()+4
                            while not rows(state/'state.db','SELECT * FROM stopped_direct_bodies'):
                                assert time.monotonic()<end
                                await asyncio.sleep(.02)
                            resume=dict(scope='job',action='resume',job='batch-job-0',request_id='resume',expected_revision=pause['revision'],expected_worker_epoch='1')
                            receipt=await call('downloads_control',resume)
                            assert receipt['results'][0]['outcome']=='existing_authority'
                            end=time.monotonic()+4
                            while not entered.is_set():
                                assert time.monotonic()<end
                                await asyncio.sleep(.02)
                            assert rows(state/'state.db','SELECT phase FROM saved_direct_continuations')==[('retired',)]
                            current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                            if action=='queue': control=dict(scope='queue',action='pause',request_id='cancel',expected_revision='2')
                            else: control=dict(scope='job',action=action,job='batch-job-0',request_id='cancel',expected_revision=str(current['revision']))
                            # The entered operation is still owned: timeout cannot acknowledge a JOIN.
                            blocked=await session.call_tool('downloads_control',arguments=control)
                            if cooperative:
                                assert not blocked.isError
                                value=result_record(blocked)
                                assert value.get('applied',value.get('status')=='applied')
                                assert rows(state/'state.db','SELECT phase FROM saved_direct_continuations')==[('failed',)]
                            else:
                                assert blocked.isError
                                assert rows(state/'state.db','SELECT phase FROM saved_direct_continuations')==[('retired',)]
                            assert len(rows(state/'state.db',"SELECT * FROM target_dispatch_causes WHERE kind='body'"))==1
                            release.set()
                            end=time.monotonic()+4
                            while rows(state/'state.db','SELECT phase FROM saved_direct_continuations')!=[('failed',)]:
                                assert time.monotonic()<end
                                await asyncio.sleep(.02)
                            assert await call('downloads_control',resume)==receipt|dict(replayed=True)
                            assert len(rows(state/'state.db',"SELECT * FROM target_dispatch_causes WHERE kind='body'"))==1
                            assert not (output/'Other/cancel.bin').exists()
                            assert all((partial.parent/name).stat().st_nlink==1 for name in paths._SAVED_NAMES)
                            effects=[json.loads(line) for line in (evidence/'scheduler-effects.jsonl').read_text().splitlines()]
                            if not cooperative: assert any(row['kind']=='observer-join' and row['name']=='direct-saved-observation' and row['alive'] for row in effects)
                            assert any(row['kind']=='observer-join' and row['name']=='direct-saved-observation' and not row['alive'] for row in effects)
                            (evidence/'result.json').write_text(json.dumps(dict(action=action,cooperative=cooperative,timeout_kept_lane_busy=not cooperative,late_return_no_ready=True,bodies=1,retained=True)))
                try: asyncio.run(exercise())
                finally: release.set()
        assert not origin_thread.is_alive()


@pytest.mark.parametrize('stop',['pause','queue','shutdown','crash','prepared','absent','prepared-purge'])
def test_public_cold_fresh_consent_and_exact_lineage(tmp_path,monkeypatch,sdk_lifecycle,stop):
    import multiprocessing,signal
    from test_direct_publication import _append_record
    evidence=Path(tempfile.mkdtemp(prefix='cold-'+stop+'-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path)))).resolve()
    home=evidence/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home))
    output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.1 if stop in {'crash','absent'} else .025)
    context=multiprocessing.get_context('spawn');entered,release=context.Event(),context.Event()
    async def call(state,name,arguments):
        async with stdio_client(parameters(state)) as (read,write):
            async with ClientSession(read,write) as session:
                await session.initialize()
                return result_record(await session.call_tool(name,arguments=arguments))
    def invoke(state,name,arguments): return asyncio.run(call(state,name,arguments))
    def wait(predicate,seconds=12):
        end=time.monotonic()+seconds
        while not predicate():
            assert time.monotonic()<end
            time.sleep(.02)
    def checkpoint_rows(state):
        result=rows(state/'state.db','SELECT source_request_id,checkpoint_digest,facts,closure FROM stopped_direct_bodies UNION ALL SELECT source_request_id,checkpoint_digest,facts,closure FROM stopped_direct_cycles')
        if rows(state/'state.db',"SELECT 1 FROM sqlite_master WHERE name='stopped_absent_bodies'"):
            result+=rows(state/'state.db','SELECT source_request_id,checkpoint_digest,facts,closure FROM stopped_absent_bodies')
        return result
    with tempfile.TemporaryDirectory(prefix='cold13-',dir='/private/tmp') as directory:
        state=Path(directory);state.chmod(0o700)
        with _helpers._origin_type()(payload_size=(32 if stop=='absent' else 16)*1024*1024) as origin:
            expected=hashlib.sha256(origin.payload).hexdigest();origin_thread=origin._thread
            with body_worker(state,origin.url(),evidence,barrier='saved-drained' if stop.startswith('prepared') else None,
                entered=entered,release=release,crash=stop in {'crash','prepared','absent','prepared-purge'},
                recover_socket=stop in {'crash','prepared','absent','prepared-purge'}) as original_worker:
                values=[entry(source_url=origin.url(),expected_sha256=expected,partial_filename='cold.bin',selected_final_filename='cold.bin')]
                if stop=='queue': values.append(entry(1,source_url=origin.url(),expected_sha256=expected,partial_filename='next.bin',selected_final_filename='next.bin'))
                invoke(state,'downloads_add',dict(request_id='cold-add',items=values))
                authorized=targets(['batch-job-0'],request='cold-authorize')
                original_receipt=invoke(state,'downloads_control',authorized)
                invoke(state,'downloads_control',dict(scope='queue',action='resume',request_id='cold-run',expected_revision='1'))
                partial=output/'.incomplete/batch-job-0/cold.bin'
                wait(lambda: partial.exists() and partial.stat().st_blocks*512>3*1024*1024)
                with closing(SQLiteStore(state/'state.db')) as store: engine=next((owner[1] for owner in store._job_engine_owners() if owner[0]=='batch-job-0'),None)
                assert engine is not None
                bodies_before=rows(state/'state.db',"SELECT * FROM target_dispatch_causes WHERE kind='body'")
                receipts_before=rows(state/'state.db','SELECT * FROM direct_dispatch_commands')
                if stop in {'pause','prepared','absent','prepared-purge'}:
                    current=invoke(state,'downloads_query',dict(scope='status',id='batch-job-0'))['record']
                    paused=invoke(state,'downloads_control',dict(scope='job',action='pause',request_id='cold-pause',job='batch-job-0',expected_revision=str(current['revision'])))
                    wait(lambda: bool(checkpoint_rows(state)))
                    if stop.startswith('prepared'):
                        resume=dict(scope='job',action='resume',request_id='prepare-before-crash',job='batch-job-0',expected_revision=paused['revision'],expected_worker_epoch='1')
                        invoke(state,'downloads_control',resume)
                        assert entered.wait(8)
                        assert rows(state/'state.db','SELECT drained FROM saved_resume_bindings')==[(1,)]
                        assert len(rows(state/'state.db',"SELECT * FROM target_dispatch_causes WHERE kind='body'"))==1
                    if stop=='absent':
                        pause_bytes=origin.ledger.response_body_bytes
                        pause_blocks=partial.stat().st_blocks
                        invoke(state,'downloads_control',dict(scope='job',action='resume',request_id='absent-body2',job='batch-job-0',expected_revision=paused['revision'],expected_worker_epoch='1'))
                        wait(lambda:len(rows(state/'state.db',"SELECT * FROM target_dispatch_causes WHERE kind='body'"))==2 and partial.with_name('cold.bin.aria2').exists() and partial.stat().st_blocks>pause_blocks+2048)
                        def observed_engine():
                            with closing(SQLiteStore(state/'state.db')) as store:
                                owners=store._job_engine_owners()
                                return owners[0][1] if owners and store.get_job('batch-job-0').state=='downloading' else None
                        wait(lambda:observed_engine() is not None)
                        engine=observed_engine()
                        assert engine is not None and processes.is_current_process_birth(engine.identity)
                        bodies_before=rows(state/'state.db',"SELECT * FROM target_dispatch_causes WHERE kind='body'")
                        receipts_before=rows(state/'state.db','SELECT * FROM direct_dispatch_commands')
                elif stop=='queue':
                    invoke(state,'downloads_control',dict(scope='queue',action='pause',request_id='cold-queue-stop',expected_revision='2'))
                    wait(lambda: bool(checkpoint_rows(state)))
                    assert rows(state/'state.db',"SELECT stop_kind FROM stopped_direct_cycles")==[('queue',)]
                    assert rows(state/'state.db',"SELECT count(*) FROM job_control_commands WHERE action='pause'")==[(0,)]
                saved_before=checkpoint_rows(state)
                if stop in {'crash','prepared','absent','prepared-purge'}:
                    if stop=='crash':
                        assert processes.is_current_process_birth(engine.identity)
                        assert origin.ledger.response_body_bytes<len(origin.payload)
                    ledger=[json.loads(line) for line in (evidence/f'fixture-{original_worker.pid}.jsonl').read_text().splitlines()]
                    identity=processes.ProcessBirthIdentity.from_record(next(row['identity'] for row in ledger if row['kind']=='worker-birth'))
                    assert processes.is_current_process_birth(identity)
                    os.killpg(identity.process_group_id,signal.SIGKILL)
                    original_worker.join(8)
                    assert original_worker.exitcode==-signal.SIGKILL and not original_worker.is_alive()
                    with pytest.raises(ChildProcessError): os.waitpid(original_worker.pid,os.WNOHANG)
                    _append_record(evidence/'original-crash-wait.jsonl','original-worker-wait',identity=identity.to_record(),exitcode=original_worker.exitcode,waitpid='ECHILD')
                    if stop=='absent':
                        assert processes.is_current_process_birth(engine.identity)
                        os.killpg(engine.identity.process_group_id,signal.SIGKILL)
                        wait(lambda:processes.reconcile_process_birth(engine.identity)=='absent')
                        with pytest.raises(ProcessLookupError):os.killpg(engine.identity.process_group_id,0)
                        wait(lambda:origin.ledger.response_body_bytes>pause_bytes and any(e.range_header is not None and e.body_bytes>0 for e in origin.ledger.entries))
                        _append_record(evidence/'former-engine-absence.jsonl','qualified-absence-after-parent-wait',identity=engine.identity.to_record(),original_engine_wait_claimed=False,original_observer_join_claimed=False)
                    with body_worker(state,origin.url(),evidence,recover_socket=True) as cold:
                        try:
                            _exercise_cold_consent(state,origin,evidence,output,expected,authorized,original_receipt,saved_before,bodies_before,receipts_before,stop,invoke,wait,checkpoint_rows)
                        except BaseException:
                            with closing(sqlite3.connect(state/'state.db')) as source, closing(sqlite3.connect(evidence/'synthetic-failure.db')) as target:
                                source.backup(target)
                            raise
                else:
                    # Leave this original context naturally; it owns its actual shutdown and wait.
                    pass
            if stop not in {'crash','prepared','absent','prepared-purge'}:
                assert original_worker.exitcode==0
                saved_before=checkpoint_rows(state)
                assert saved_before
                with body_worker(state,origin.url(),evidence) as cold:
                    _exercise_cold_consent(state,origin,evidence,output,expected,authorized,original_receipt,saved_before,bodies_before,receipts_before,stop,invoke,wait,checkpoint_rows)
            assert processes.reconcile_process_birth(engine.identity)=='absent'
            with pytest.raises(ProcessLookupError): os.killpg(engine.identity.process_group_id,0)
        assert not origin_thread.is_alive()
        (evidence/'origin-joined.json').write_text(json.dumps(dict(joined=True,ident=origin_thread.ident)))


def _exercise_cold_consent(state,origin,evidence,output,expected,authorized,original_receipt,saved_before,bodies_before,receipts_before,stop,invoke,wait,checkpoint_rows):
    health=invoke(state,'downloads_query',dict(scope='queue'))
    assert health['worker_epoch']=='2' and health['queue_gate']=='paused'
    wait(lambda:len(checkpoint_rows(state))>len(saved_before) if stop=='absent' else bool(checkpoint_rows(state)))
    if stop=='absent':
        closure=json.loads(checkpoint_rows(state)[-1][-1])
        assert closure['observer_joined'] is False and closure['returncode'] is None and closure['completed_length'] is None
    elif stop=='crash':
        assert rows(state/'state.db','SELECT stop_kind FROM stopped_direct_cycles')==[('cold',)]
        closure=json.loads(checkpoint_rows(state)[0][-1])
        assert closure['observer_joined'] is False and closure['returncode'] is None
    else: assert checkpoint_rows(state)==saved_before
    settled=origin.ledger
    assert invoke(state,'downloads_control',authorized)==original_receipt|dict(replayed=True)
    invoke(state,'downloads_query',dict(scope='list'))
    invoke(state,'downloads_add',dict(request_id='cold-add-only',items=[entry(7,source_url=origin.url(),partial_filename='inert.bin',selected_final_filename='inert.bin')]))
    assert rows(state/'state.db',"SELECT * FROM target_dispatch_causes WHERE kind='body'")==bodies_before
    assert origin.ledger.response_body_bytes==settled.response_body_bytes
    current=invoke(state,'downloads_query',dict(scope='status',id='batch-job-0'))['record']
    before_resume=checkpoint_rows(state)
    if stop=='prepared-purge':
        directory=output/'.incomplete/batch-job-0';sentinel=directory/'unknown-sentinel';sentinel.write_bytes(b'preserve')
        lineage=rows(state/'state.db','SELECT * FROM saved_direct_continuations')+rows(state/'state.db','SELECT * FROM saved_direct_cycles')
        bindings=rows(state/'state.db','SELECT * FROM saved_resume_bindings')
        request=dict(action='purge',ids=['batch-job-0'],confirmed=True,request_id='prepared-cold-purge')
        receipt=invoke(state,'downloads_files',request)
        wait(lambda:(invoke(state,'downloads_query',dict(scope='detail',id='batch-job-0'))['files'] or {}).get('status') in {'purged','blocked'})
        detail=invoke(state,'downloads_query',dict(scope='detail',id='batch-job-0'))
        assert detail['files']['status']=='purged',detail
        assert list(directory.iterdir())==[sentinel] and sentinel.read_bytes()==b'preserve'
        assert not (output/'Other/cold.bin').exists()
        assert checkpoint_rows(state)==before_resume
        assert rows(state/'state.db','SELECT * FROM saved_direct_continuations')+rows(state/'state.db','SELECT * FROM saved_direct_cycles')==lineage
        assert rows(state/'state.db','SELECT * FROM saved_resume_bindings')==bindings
        assert rows(state/'state.db',"SELECT * FROM target_dispatch_causes WHERE kind='body'")==bodies_before
        assert origin.ledger.response_body_bytes==settled.response_body_bytes
        assert invoke(state,'downloads_files',request)==receipt|dict(replayed=True)
        (evidence/'result.json').write_text(json.dumps(dict(stop=stop,body_absent=True,checkpoint_unchanged=True,prepared_lineage_unchanged=True,unknown_sentinel_preserved=True,synthetic=True)))
        return
    resume=dict(scope='job',action='resume',request_id='cold-fresh',job='batch-job-0',expected_revision=str(current['revision']),expected_worker_epoch='2')
    receipt=invoke(state,'downloads_control',resume)
    assert receipt['results'][0]['outcome']=='existing_authority',receipt
    assert rows(state/'state.db',"SELECT * FROM target_dispatch_causes WHERE kind='body'")==bodies_before
    queue=invoke(state,'downloads_query',dict(scope='queue'))
    invoke(state,'downloads_control',dict(scope='queue',action='resume',request_id='cold-go',expected_revision=queue['revision']))
    wait(lambda: bool(rows(state/'state.db',"SELECT 1 FROM direct_cleanup_claims WHERE phase='finished'")),50 if stop=='absent' else 25)
    assert (output/'Other/cold.bin').read_bytes()==origin.payload
    assert hashlib.sha256((output/'Other/cold.bin').read_bytes()).hexdigest()==expected
    assert checkpoint_rows(state)==before_resume
    for original in bodies_before:
        assert rows(state/'state.db','SELECT * FROM target_dispatch_causes WHERE request_id=?',(original[0],))==[original]
    for original in receipts_before:
        assert rows(state/'state.db','SELECT * FROM direct_dispatch_commands WHERE request_id=?',(original[0],))==[original]
    assert rows(state/'state.db',"SELECT admission_serial FROM target_dispatch_causes WHERE kind='body' ORDER BY admission_serial")==([(1,),(2,),(3,)] if stop=='absent' else [(1,),(2,)])
    assert invoke(state,'downloads_control',resume)==receipt|dict(replayed=True)
    names=rows(state/'state.db','SELECT retained_names FROM saved_resume_bindings')
    job=output/'.incomplete/batch-job-0'
    assert all(not (job/name).exists() for value, in names for name in json.loads(value))
    assert not (job/'.hermes-reservation').exists()
    ledger=origin.ledger
    body2=ledger.response_body_bytes-settled.response_body_bytes
    assert 0<body2<len(origin.payload) and ledger.dropped_entries==0 and ledger.request_count<=24
    (evidence/'result.json').write_text(json.dumps(dict(stop=stop,body2_successful_bytes=body2,full_payload_bytes=len(origin.payload),
        checkpoint_rows=before_resume,ledger=asdict(ledger),synthetic=True),indent=2))



def test_actual_disconnect_keeps_originals_and_requires_fresh_checksum_consent(tmp_path,monkeypatch,sdk_lifecycle):
    evidence=Path(tempfile.mkdtemp(prefix='disconnect-consent-',dir=Path(os.environ.get('T26_RESUME_RUN',str(tmp_path)))));evidence.chmod(0o700)
    home=evidence/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    with tempfile.TemporaryDirectory(prefix='dc15-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=8*1024*1024) as origin:
            broken=[True];serve=origin._serve
            def interrupted(handler):
                if not broken[0]:return serve(handler)
                span=handler.headers.get('Range');status,headers,body=origin._range_response(span)
                handler.close_connection=True
                sent=origin._send_response(handler,status,headers,body[:1024*1024],content_length=len(body))
                origin._record_request('range',sent,1,span)
            origin._serve=interrupted
            with body_worker(root,origin.url(),evidence) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='disconnect-add',items=[entry(source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest())]))
                            authorization=await call('downloads_control',targets(['batch-job-0']))
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='disconnect-go',expected_revision='1'))
                            end=time.monotonic()+10
                            while True:
                                current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                if current['state'] in {'blocked','retry_wait'}:break
                                assert process.is_alive() and time.monotonic()<end,current
                                await asyncio.sleep(.02)
                            assert current['state']=='blocked',current
                            end=time.monotonic()+5
                            while not rows(root/'state.db','SELECT 1 FROM stopped_failed_bodies'):
                                assert time.monotonic()<end,'failed original checkpoint missing'
                                await asyncio.sleep(.02)
                            checkpoint=rows(root/'state.db','SELECT * FROM stopped_failed_bodies')
                            partial=output/'.incomplete/batch-job-0/file-0.bin';control=partial.with_name(partial.name+'.aria2')
                            originals=[(file.stat().st_ino,hashlib.sha256(file.read_bytes()).hexdigest()) for file in (partial,control)]
                            before=origin.ledger.response_body_bytes
                            assert await call('downloads_control',targets(['batch-job-0']))==authorization|dict(replayed=True)
                            await call('downloads_query',dict(scope='list'))
                            await asyncio.sleep(.1)
                            assert origin.ledger.response_body_bytes==before
                            broken[0]=False
                            consent=await call('downloads_control',dict(scope='job',action='resume',request_id='disconnect-consent',job='batch-job-0',expected_revision=str(current['revision']),expected_worker_epoch='1'))
                            assert consent['results'][0]['outcome']=='existing_authority',consent
                            end=time.monotonic()+20
                            while not rows(root/'state.db',"SELECT 1 FROM direct_cleanup_claims WHERE phase='finished'"):
                                assert time.monotonic()<end
                                await asyncio.sleep(.03)
                            assert (output/'Other/file-0.bin').read_bytes()==origin.payload
                            assert rows(root/'state.db','SELECT * FROM stopped_failed_bodies')==checkpoint
                            assert rows(root/'state.db',"SELECT admission_serial FROM target_dispatch_causes WHERE kind='body' ORDER BY admission_serial")==[(1,),(2,)]
                            assert any(e.range_header is not None and e.body_bytes>0 for e in origin.ledger.entries)
                            (evidence/'result.json').write_text(json.dumps(dict(checkpoint=checkpoint,originals=originals,synthetic=True)))
                asyncio.run(exercise())


@pytest.mark.parametrize('cut',['cut-retained','cut-canonical','cut-cleanup-marker'])
def test_actual_cold_completed_retained_cleanup_requires_fresh_public_consent(tmp_path,monkeypatch,sdk_lifecycle,cut):
    import multiprocessing,signal
    from test_direct_publication import _append_record
    evidence=Path(tempfile.mkdtemp(prefix=cut+'-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path)))).resolve()
    home=evidence/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.06)
    context=multiprocessing.get_context('spawn');entered,release=context.Event(),context.Event()
    async def call(state,name,args):
        async with stdio_client(parameters(state)) as (read,write):
            async with ClientSession(read,write) as session:
                await session.initialize();return result_record(await session.call_tool(name,arguments=args))
    def invoke(state,name,args):return asyncio.run(call(state,name,args))
    def wait(predicate,seconds=12):
        end=time.monotonic()+seconds
        while not predicate():assert time.monotonic()<end;time.sleep(.02)
    with tempfile.TemporaryDirectory(prefix='retaincut-',dir='/private/tmp') as temporary:
        state=Path(temporary);state.chmod(0o700)
        with _helpers._origin_type()(payload_size=16*1024*1024) as origin:
            expected=hashlib.sha256(origin.payload).hexdigest()
            with body_worker(state,origin.url(),evidence,barrier=cut,entered=entered,release=release,crash=True,recover_socket=True) as original:
                invoke(state,'downloads_add',dict(request_id='retained-add',items=[entry(source_url=origin.url(),expected_sha256=expected,partial_filename='retained.bin',selected_final_filename='retained.bin')]))
                authorized=targets(['batch-job-0'],request='retained-authorize');receipt=invoke(state,'downloads_control',authorized)
                invoke(state,'downloads_control',dict(scope='queue',action='resume',request_id='retained-go',expected_revision='1'))
                partial=output/'.incomplete/batch-job-0/retained.bin';wait(lambda:partial.exists() and partial.stat().st_blocks*512>2*1024*1024)
                current=invoke(state,'downloads_query',dict(scope='status',id='batch-job-0'))['record']
                paused=invoke(state,'downloads_control',dict(scope='job',action='pause',job='batch-job-0',request_id='retained-pause',expected_revision=str(current['revision'])))
                wait(lambda:bool(rows(state/'state.db','SELECT source_request_id FROM stopped_direct_bodies')))
                original_checkpoint=rows(state/'state.db','SELECT * FROM stopped_direct_bodies')
                invoke(state,'downloads_control',dict(scope='job',action='resume',job='batch-job-0',request_id='retained-resume',expected_revision=paused['revision'],expected_worker_epoch='1'))
                assert entered.wait(15)
                final=output/'Other/retained.bin';assert hashlib.sha256(final.read_bytes()).hexdigest()==expected
                sentinel=partial.parent/'unknown.sidecar';sentinel.write_bytes(b'unknown-preserved')
                claim_before=rows(state/'state.db','SELECT completion_audit_id,completion_generation,completion_revision,proof_sha256 FROM direct_cleanup_claims')
                immutable=rows(state/'state.db','SELECT * FROM direct_dispatch_commands')
                ledger=[json.loads(line) for line in (evidence/f'fixture-{original.pid}.jsonl').read_text().splitlines()]
                identity=processes.ProcessBirthIdentity.from_record(next(row['identity'] for row in ledger if row['kind']=='worker-birth'))
                os.killpg(identity.process_group_id,signal.SIGKILL);original.join(8)
                assert original.exitcode==-signal.SIGKILL
                with pytest.raises(ChildProcessError):os.waitpid(original.pid,os.WNOHANG)
                _append_record(evidence/'original-cut-wait.jsonl','original-worker-wait',identity=identity.to_record(),exitcode=original.exitcode,waitpid='ECHILD')
            baseline=origin.ledger;names=list(partial.parent.iterdir())
            with body_worker(state,origin.url(),evidence,recover_socket=True) as cold:
                assert invoke(state,'downloads_query',dict(scope='queue'))['queue_gate']=='paused'
                invoke(state,'downloads_query',dict(scope='list'))
                assert invoke(state,'downloads_control',authorized)==receipt|dict(replayed=True)
                assert origin.ledger.response_body_bytes==baseline.response_body_bytes
                assert list(partial.parent.iterdir())==names
                current=invoke(state,'downloads_query',dict(scope='status',id='batch-job-0'))['record'];assert current['state']=='completed'
                fresh=dict(scope='job',action='resume',job='batch-job-0',request_id='retained-cold-cleanup',expected_revision=str(current['revision']),expected_worker_epoch='2')
                accepted=invoke(state,'downloads_control',fresh);assert accepted['results'][0]['outcome']=='existing_authority'
                wait(lambda:rows(state/'state.db','SELECT phase FROM direct_cleanup_claims')==[('finished',)])
                assert list(partial.parent.iterdir())==[sentinel] and sentinel.read_bytes()==b'unknown-preserved'
                assert hashlib.sha256(final.read_bytes()).hexdigest()==expected
                assert invoke(state,'downloads_control',fresh)==accepted|dict(replayed=True)
                assert rows(state/'state.db','SELECT * FROM stopped_direct_bodies')==original_checkpoint
                assert rows(state/'state.db','SELECT completion_audit_id,completion_generation,completion_revision,proof_sha256 FROM direct_cleanup_claims')==claim_before
                assert rows(state/'state.db','SELECT * FROM direct_dispatch_commands')==immutable
                assert origin.ledger.response_body_bytes==baseline.response_body_bytes
                (evidence/'retained-cut-result.json').write_text(json.dumps(dict(cut=cut,original_wait=True,fresh_consent=True,body_inert=True,unknown_preserved=True,checkpoint_immutable=True,claim_immutable=True)))
