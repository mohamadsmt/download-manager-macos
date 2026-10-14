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


@pytest.mark.parametrize('stop',['pause','queue','shutdown','crash','prepared','absent','prepared-purge','hashless','prepared-hashless'])
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
        origin=_helpers._origin_type()(payload_size=(32 if stop=='absent' else 16)*1024*1024)
        cold_requests=[];cold_heads=[]
        if stop in {'hashless','prepared-hashless'}:
            response=origin._response_for
            def respond(endpoint,span):
                status,headers,body=response(endpoint,span)
                return status,headers|{'ETag':'"cold-strong"'},body
            origin._response_for=respond
            handler_type=origin._handler_type
            def handlers():
                Handler=handler_type();get=Handler.do_GET
                def head(handler):
                    cold_heads.append(True);handler.send_response(200);handler.send_header('ETag','"cold-strong"')
                    handler.send_header('Content-Length',str(len(origin.payload)));handler.end_headers()
                def request(handler):
                    cold_requests.append((handler.headers.get('Range'),handler.headers.get('If-Match')));get(handler)
                Handler.do_HEAD=head;Handler.do_GET=request;return Handler
            origin._handler_type=handlers
        with origin:
            expected=hashlib.sha256(origin.payload).hexdigest();origin_thread=origin._thread
            with body_worker(state,origin.url(),evidence,barrier='saved-drained' if stop.startswith('prepared') else None,
                entered=entered,release=release,crash=stop in {'crash','prepared','absent','prepared-purge','prepared-hashless'},
                recover_socket=stop in {'crash','prepared','absent','prepared-purge','prepared-hashless'}) as original_worker:
                values=[entry(source_url=origin.url(),expected_sha256=None if stop in {'hashless','prepared-hashless'} else expected,partial_filename='cold.bin',selected_final_filename='cold.bin')]
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
                if stop in {'pause','prepared','absent','prepared-purge','prepared-hashless'}:
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
                if stop in {'crash','prepared','absent','prepared-purge','prepared-hashless'}:
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
            if stop not in {'crash','prepared','absent','prepared-purge','prepared-hashless'}:
                assert original_worker.exitcode==0
                saved_before=checkpoint_rows(state)
                assert saved_before
                with body_worker(state,origin.url(),evidence) as cold:
                    _exercise_cold_consent(state,origin,evidence,output,expected,authorized,original_receipt,saved_before,bodies_before,receipts_before,stop,invoke,wait,checkpoint_rows)
            assert processes.reconcile_process_birth(engine.identity)=='absent'
            with pytest.raises(ProcessLookupError): os.killpg(engine.identity.process_group_id,0)
            if stop in {'hashless','prepared-hashless'}:
                assert cold_heads==([True,True] if stop=='prepared-hashless' else [True])
                resumed=[row for row in cold_requests if row[1] is not None]
                assert resumed and all(row[1]=='"cold-strong"' for row in resumed)
                assert any(row[0] and int(row[0].split('=')[1].split('-')[0])>0 for row in resumed)
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
    queue=invoke(state,'downloads_query',dict(scope='queue'))
    if stop in {'queue','shutdown','crash','absent','hashless'}:
        resume=dict(scope='queue',action='resume',request_id='cold-go',expected_revision=queue['revision'])
        receipt=invoke(state,'downloads_control',resume)
    else:
        resume=dict(scope='job',action='resume',request_id='cold-fresh',job='batch-job-0',expected_revision=str(current['revision']),expected_worker_epoch='2')
        receipt=invoke(state,'downloads_control',resume)
        assert receipt['results'][0]['outcome']=='existing_authority',receipt
        assert rows(state/'state.db',"SELECT * FROM target_dispatch_causes WHERE kind='body'")==bodies_before
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
    assert invoke(state,'downloads_control',resume)==receipt|(dict(applied=False) if resume['scope']=='queue' else dict(replayed=True))
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


@pytest.mark.parametrize('mode', ['hashless', 'changed', 'sha', 'queue', 'source-same', 'source-changed', 'source-new-url', 'queue-unknown', 'slow-head', 'stalled-head', 'source-slow-head', 'source-stalled-head', 'deadline-head', 'body-changed'])
def test_sdk_partial_resume_identity_and_global_membership(tmp_path, monkeypatch, sdk_lifecycle, mode):
    """A fresh consent resumes the original bytes; replay never admits twice."""
    evidence = Path(tempfile.mkdtemp(prefix='identity-'+mode+'-', dir=os.environ.get('T26_RESUME_RUN', str(tmp_path))))
    home=evidence/'home'; home.mkdir(mode=0o700); monkeypatch.setenv('HOME',str(home))
    output=home/'Downloads/Hermes'; output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.02)
    with tempfile.TemporaryDirectory(prefix='identity17-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        origin=_helpers._origin_type()(payload_size=8*1024*1024)
        version=['"original-strong"']; heads=[]; requests=[]
        import threading
        head_entered=threading.Event(); head_release=threading.Event()
        response=origin._response_for
        def respond(endpoint,span):
            status,headers,body=response(endpoint,span)
            return status,headers|({'ETag':version[0]} if mode!='queue-unknown' else {}),body
        origin._response_for=respond
        handler_type=origin._handler_type
        def handlers():
            Handler=handler_type()
            def head(handler):
                heads.append(version[0])
                head_entered.set()
                if mode.endswith('stalled-head'):
                    head_release.wait(5)
                if mode.endswith('slow-head') or mode=='deadline-head':
                    try:
                        handler.connection.sendall(b'HTTP/1.1 200 OK\r\nX-Slow: ')
                        end=time.monotonic()+5
                        while not head_release.wait(.1) and time.monotonic()<end:
                            handler.connection.sendall(b'x')
                    except OSError:pass
                    handler.close_connection=True
                    return
                handler.send_response(200)
                handler.send_header('ETag',version[0]);handler.send_header('Content-Length',str(len(origin.payload)))
                handler.end_headers()
                if mode=='body-changed':version[0]='"late-changed"'
            Handler.do_HEAD=head
            get=Handler.do_GET
            def request(handler):
                requests.append(dict(range=handler.headers.get('Range'),match=handler.headers.get('If-Match')))
                if handler.headers.get('If-Match') not in (None,version[0]):
                    handler.send_response(412);handler.send_header('Content-Length','0');handler.end_headers();return
                get(handler)
            Handler.do_GET=request
            return Handler
        origin._handler_type=handlers
        with origin:
            origin_thread=origin._thread
            with body_worker(root,origin.url(),evidence) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            digest=hashlib.sha256(origin.payload).hexdigest() if mode in {'sha','queue'} else None
                            items=[entry(index=i,source_url=origin.url(),expected_sha256=digest) for i in range(4)]
                            await call('downloads_add',dict(request_id='identity-add',items=items))
                            await call('downloads_control',targets(['batch-job-0','batch-job-1','batch-job-2'] if mode=='queue' else ['batch-job-0','batch-job-1']))
                            held=(await call('downloads_query',dict(scope='status',id='batch-job-1')))['record']
                            await call('downloads_control',dict(scope='job',action='pause',job='batch-job-1',request_id='manual-hold',expected_revision=str(held['revision'])))
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='identity-run',expected_revision='1'))
                            partial=output/'.incomplete/batch-job-0/file-0.bin';control=partial.with_name(partial.name+'.aria2')
                            end=time.monotonic()+15
                            while not partial.exists() or partial.stat().st_blocks*512<2*1024*1024:
                                assert process.is_alive() and time.monotonic()<end
                                await asyncio.sleep(.02)
                            current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                            if mode in {'queue','queue-unknown'}:
                                pause=await call('downloads_control',dict(scope='queue',action='pause',request_id='identity-pause',expected_revision='2'))
                            else:
                                pause=await call('downloads_control',dict(scope='job',action='pause',request_id='identity-pause',job='batch-job-0',expected_revision=str(current['revision'])))
                                await call('downloads_control',dict(scope='queue',action='pause',request_id='global-hold',expected_revision='2'))
                            end=time.monotonic()+5
                            while not rows(root/'state.db','SELECT 1 FROM stopped_direct_bodies UNION ALL SELECT 1 FROM stopped_direct_cycles'):
                                assert time.monotonic()<end,'actual stopped pair must be sealed even without supplied SHA'
                                await asyncio.sleep(.02)
                            (evidence/'checkpoint.json').write_text(json.dumps(rows(root/'state.db','SELECT * FROM stopped_direct_bodies')+rows(root/'state.db','SELECT * FROM stopped_direct_cycles')))
                            originals=[(p.stat().st_ino,hashlib.sha256(p.read_bytes()).hexdigest()) for p in (partial,control)]
                            before=origin.ledger.response_body_bytes
                            request_boundary=len(requests)
                            async def cancel_probe():
                                end=time.monotonic()+4
                                while not head_entered.is_set():
                                    assert time.monotonic()<end
                                    await asyncio.sleep(.01)
                                start=time.monotonic()
                                await call('downloads_query',dict(scope='status',id='batch-job-0'))
                                assert time.monotonic()-start<1,'HEAD must not block query'
                                start=time.monotonic()
                                await call('downloads_control',dict(scope='queue',action='pause',request_id='probe-cancel',expected_revision='3' if mode.startswith('source-') else '4'))
                                elapsed=time.monotonic()-start
                                head_release.set()
                                assert elapsed<1.5,'pause must cancel and JOIN original identity observer'
                                assert origin.ledger.response_body_bytes==before
                                for p,(inode,sha) in zip((partial,control),originals):
                                    assert p.stat().st_ino==inode and hashlib.sha256(p.read_bytes()).hexdigest()==sha
                                effects=[json.loads(line) for line in (evidence/'scheduler-effects.jsonl').read_text().splitlines()]
                                births=[row for row in effects if row['kind']=='identity-child-birth']
                                assert births and all(row['birth'] is not None for row in births)
                                assert all(any(wait['kind']=='identity-child-original-wait' and wait['pid']==row['pid'] and wait['echild'] and wait['group_absent'] for wait in effects) for row in births)
                                assert any(row['kind']=='observer-join' and row['name']=='direct-saved-observation' and not row['alive'] for row in effects)
                            if mode=='changed':version[0]='"replacement-strong"';origin.payload=origin.changed_payload
                            if mode.startswith('source-'):
                                if mode=='source-changed':version[0]='"replacement-strong"';origin.payload=origin.changed_payload
                                replacement=dict(id='batch-job-0',url=origin.url() if mode!='source-new-url' else origin.url('/range?other'),request_id='identity-source',expected_revision=pause['revision'])
                                accepted=await call('downloads_replace_source',replacement)
                                if mode in {'source-slow-head','source-stalled-head'}:
                                    await cancel_probe();return
                                end=time.monotonic()+8
                                while True:
                                    detail=await call('downloads_query',dict(scope='detail',id='batch-job-0'))
                                    if detail['source_replacement']['status']!='pending':break
                                    assert time.monotonic()<end
                                    await asyncio.sleep(.02)
                                assert detail['source_replacement']['status']==('applied' if mode=='source-same' else 'blocked'),detail
                                assert await call('downloads_replace_source',replacement)==accepted|dict(replayed=True)
                                assert origin.ledger.response_body_bytes==before
                                if mode!='source-same':
                                    assert detail['source_replacement']['alternative']=='separate_download'
                                    for p,(inode,sha) in zip((partial,control),originals):assert p.stat().st_ino==inode and hashlib.sha256(p.read_bytes()).hexdigest()==sha
                                    return
                            if mode not in {'queue','queue-unknown'}:
                                resume=dict(scope='job',action='resume',request_id='identity-resume',job='batch-job-0',expected_revision=pause['revision'],expected_worker_epoch='1')
                                receipt=await call('downloads_control',resume)
                                assert receipt['results'][0]['outcome']=='existing_authority',receipt
                                assert await call('downloads_control',resume)==receipt|dict(replayed=True)
                            assert origin.ledger.response_body_bytes==before
                            go=dict(scope='queue',action='resume',request_id='identity-go',expected_revision='3')
                            receipt=await call('downloads_control',go)
                            assert await call('downloads_control',go)==receipt|dict(applied=False)
                            if mode in {'slow-head','stalled-head'}:
                                await cancel_probe();return
                            probe_started=time.monotonic()
                            end=probe_started+(4.5 if mode=='deadline-head' else 15)
                            while True:
                                status=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                if status['state'] in {'completed','blocked','needs_link','needs_auth'}:break
                                assert time.monotonic()<end,('partial did not continue',mode,status)
                                await asyncio.sleep(.03)
                            if mode in {'changed','queue-unknown','deadline-head','body-changed'}:
                                assert status['state']=='blocked',status
                                detail=await call('downloads_query',dict(scope='detail',id='batch-job-0'))
                                if mode!='body-changed':assert detail['resume_decision']['alternative']=='separate_download'
                                if mode=='deadline-head':
                                    assert time.monotonic()-probe_started<4.5
                                    head_release.set()
                                if mode=='body-changed':
                                    assert requests[request_boundary:] and all(row['match']=='"original-strong"' for row in requests[request_boundary:])
                                assert not (output/'Other/file-0.bin').exists()
                                pair=tuple(partial.parent/name for name in paths._SAVED_NAMES) if mode=='body-changed' else (partial,control)
                                for p,(inode,sha) in zip(pair,originals):
                                    assert p.stat().st_ino==inode and hashlib.sha256(p.read_bytes()).hexdigest()==sha
                                assert origin.ledger.response_body_bytes==before,'changed identity must be refused before BODY'
                            else:
                                assert status['state']=='completed',status
                                assert (output/'Other/file-0.bin').read_bytes()==origin.payload
                                resumed=requests[request_boundary:]
                                assert any(r['range'] and int(r['range'].split('=')[1].split('-')[0])>0 for r in resumed),resumed
                                if digest is None:
                                    assert heads==['"original-strong"']*(2 if mode=='source-same' else 1),heads
                                    assert resumed and all(r['match']=='"original-strong"' for r in resumed),resumed
                                bodies=rows(root/'state.db',"SELECT job_id,admission_serial FROM target_dispatch_causes WHERE kind='body' ORDER BY rowid")
                                assert bodies[:2]==[('batch-job-0',1),('batch-job-0',2)],bodies
                            for job in ('batch-job-1','batch-job-3'):
                                assert not rows(root/'state.db',"SELECT 1 FROM target_dispatch_causes WHERE kind='body' AND job_id=?",(job,))
                            (evidence/'identity-result.json').write_text(json.dumps(dict(mode=mode,originals=originals,heads=heads,synthetic=True)))
                asyncio.run(exercise())
            assert not process.is_alive() and process.exitcode==0
        assert not origin_thread.is_alive()


@pytest.mark.parametrize('strong', [True, False])
def test_sdk_hashless_partial_transient_retry_or_actionable_decision(tmp_path,monkeypatch,sdk_lifecycle,strong):
    evidence=Path(tempfile.mkdtemp(prefix='hashless-retry-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))))
    home=evidence/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home))
    output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.02)
    with tempfile.TemporaryDirectory(prefix='retry17-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        origin=_helpers._origin_type()(payload_size=8*1024*1024);broken=[True];first=[True]
        handler_type=origin._handler_type
        def handlers():
            Handler=handler_type()
            def head(handler):
                handler.send_response(200)
                if strong:handler.send_header('ETag','"retry-original"')
                handler.send_header('Content-Length',str(len(origin.payload)));handler.end_headers()
            Handler.do_HEAD=head;return Handler
        origin._handler_type=handlers
        response=origin._response_for
        def respond(endpoint,span):
            status,headers,body=response(endpoint,span)
            return status,headers|({'ETag':'"retry-original"'} if strong else {}),body
        origin._response_for=respond
        serve=origin._serve
        def interrupted(handler):
            if not broken[0]:return serve(handler)
            with origin._lock:
                initial=first[0];first[0]=False
            span=handler.headers.get('Range')
            if initial:
                status,headers,body=respond('range',span)
                handler.close_connection=True
                sent=origin._send_response(handler,status,headers,body[:2*1024*1024],content_length=len(body),pace_large_range=True)
            else:
                time.sleep(.3)
                sent=origin._send_response(handler,503,{'Retry-After':'2'},b'')
            origin._record_request('range',sent,1,span)
        origin._serve=interrupted
        with origin:
            thread=origin._thread
            with body_worker(root,origin.url(),evidence) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='retry-add',items=[entry(source_url=origin.url())]))
                            await call('downloads_control',targets(['batch-job-0']))
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='retry-run',expected_revision='1'))
                            end=time.monotonic()+15
                            while not rows(root/'state.db','SELECT 1 FROM direct_failures'):
                                assert time.monotonic()<end
                                await asyncio.sleep(.02)
                            failure=rows(root/'state.db','SELECT http_status,state,observation FROM direct_failures')[0]
                            (evidence/'failure.json').write_text(json.dumps(failure))
                            assert failure[:2]==(503,'retry_wait'),failure
                            partial=output/'.incomplete/batch-job-0/file-0.bin';control=partial.with_name(partial.name+'.aria2')
                            assert partial.exists() and partial.stat().st_blocks*512>0
                            end=time.monotonic()+5
                            while not rows(root/'state.db','SELECT 1 FROM stopped_failed_bodies'):
                                assert time.monotonic()<end,'hashless failure must seal original ranges'
                                await asyncio.sleep(.02)
                            originals=[(p.stat().st_ino,hashlib.sha256(p.read_bytes()).hexdigest()) for p in (partial,control)]
                            broken[0]=False
                            end=time.monotonic()+20
                            while True:
                                status=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                if status['state'] in {'blocked','completed'}:break
                                assert time.monotonic()<end,('hashless retry stranded',status)
                                await asyncio.sleep(.03)
                            if strong:
                                assert status['state']=='completed',status
                                assert (output/'Other/file-0.bin').read_bytes()==origin.payload
                                assert rows(root/'state.db',"SELECT admission_serial FROM target_dispatch_causes WHERE kind='body' ORDER BY rowid")==[(1,),(2,)]
                            else:
                                assert status['state']=='blocked',status
                                detail=await call('downloads_query',dict(scope='detail',id='batch-job-0'))
                                assert detail['resume_decision']['alternative']=='separate_download'
                                for p,(inode,sha) in zip((partial,control),originals):assert p.stat().st_ino==inode and hashlib.sha256(p.read_bytes()).hexdigest()==sha
                                assert rows(root/'state.db',"SELECT count(*) FROM target_dispatch_causes WHERE kind='body'")==[(1,)]
                            (evidence/'result.json').write_text(json.dumps(dict(strong=strong,status=status,originals=originals,synthetic=True)))
                asyncio.run(exercise())
            assert process.exitcode==0 and not process.is_alive()
        assert not thread.is_alive()


def test_sdk_cold_global_resume_authorized_unstarted_only(tmp_path,monkeypatch,sdk_lifecycle):
    evidence=Path(tempfile.mkdtemp(prefix='cold-authorized-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))))
    home=evidence/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home))
    output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    async def invoke(root,name,args):
        async with stdio_client(parameters(root)) as (read,write):
            async with ClientSession(read,write) as session:
                await session.initialize();return result_record(await session.call_tool(name,arguments=args))
    def call(root,name,args):return asyncio.run(invoke(root,name,args))
    with tempfile.TemporaryDirectory(prefix='unstarted17-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()() as origin:
            thread=origin._thread
            with body_worker(root,origin.url(),evidence):
                call(root,'downloads_add',dict(request_id='cold-add',items=[entry(i,source_url=origin.url()) for i in range(3)]))
                start=targets(['batch-job-0','batch-job-1'])
                authorized=call(root,'downloads_control',start)
                held=call(root,'downloads_query',dict(scope='status',id='batch-job-1'))['record']
                call(root,'downloads_control',dict(scope='job',action='pause',job='batch-job-1',request_id='cold-manual',expected_revision=str(held['revision'])))
                assert origin.ledger.request_count==0
            with body_worker(root,origin.url(),evidence):
                queue=call(root,'downloads_query',dict(scope='queue'))
                assert queue['queue_gate']=='paused' and queue['worker_epoch']=='2'
                assert call(root,'downloads_control',start)==authorized|dict(replayed=True)
                call(root,'downloads_query',dict(scope='list'))
                assert origin.ledger.request_count==0
                resume=dict(scope='queue',action='resume',request_id='cold-global',expected_revision=queue['revision'])
                receipt=call(root,'downloads_control',resume)
                end=time.monotonic()+10
                while not (output/'Other/file-0.bin').exists():
                    assert time.monotonic()<end,'cold authorized work requires only global resume'
                    time.sleep(.03)
                assert (output/'Other/file-0.bin').read_bytes()==origin.payload
                assert call(root,'downloads_control',resume)==receipt|dict(applied=False)
                bodies=rows(root/'state.db',"SELECT job_id,admission_serial FROM target_dispatch_causes WHERE kind='body'")
                assert bodies==[('batch-job-0',1)]
                assert not (output/'Other/file-1.bin').exists() and not (output/'Other/file-2.bin').exists()
        assert not thread.is_alive()


@pytest.mark.parametrize("pair", ["intact", "empty", "missing", "changed"])
def test_early_piece_zero_rpc_pause_preserves_qualified_resume(tmp_path, monkeypatch, sdk_lifecycle, pair):
    """Stock aria2 has received bytes before its first complete piece is counted."""
    import threading
    from hermes_downloads.direct import _request_recovery_force_shutdown
    evidence = Path(tempfile.mkdtemp(prefix='early-piece-', dir=os.environ.get('T26_RESUME_RUN', str(tmp_path))))
    evidence.chmod(0o700)
    home = evidence/'home'; home.mkdir(mode=0o700)
    monkeypatch.setenv('HOME', str(home))
    output = home/'Downloads/Hermes'; output.mkdir(parents=True, mode=0o700)
    release = threading.Event()
    received = threading.Event()
    with tempfile.TemporaryDirectory(prefix='early27-', dir='/private/tmp') as temporary:
        root = Path(temporary); root.chmod(0o700)
        with _helpers._origin_type()(payload_size=538364) as origin:
            send = origin._send_response
            def gated(handler, status, headers, body, **kwargs):
                if len(body)>3601 and not release.is_set():
                    sent = send(handler, status, headers, body[:3601], content_length=len(body))
                    received.set()
                    assert release.wait(15), 'bounded early BODY gate expired'
                    return sent  # The stopped original never receives the remainder.
                return send(handler, status, headers, body, **kwargs)
            origin._send_response = gated
            expected = hashlib.sha256(origin.payload).hexdigest()
            try:
                with body_worker(root, origin.url(), evidence) as process:
                    async def exercise():
                        async with stdio_client(parameters(root)) as (read, write):
                            async with ClientSession(read, write) as session:
                                await session.initialize()
                                async def call(name, args):
                                    return result_record(await session.call_tool(name, arguments=args))
                                await call('downloads_add', dict(request_id='early-add', items=[entry(source_url=origin.url(), expected_sha256=expected)]))
                                await call('downloads_control', targets(['batch-job-0']))
                                await call('downloads_control', dict(scope='queue', action='resume', request_id='early-run', expected_revision='1'))
                                partial = output/'.incomplete/batch-job-0/file-0.bin'
                                control = partial.with_name(partial.name+'.aria2')
                                marker = partial.parent/'.hermes-reservation'
                                end = time.monotonic()+8
                                while not received.is_set() or not marker.exists():
                                    assert process.is_alive() and time.monotonic()<end
                                    await asyncio.sleep(.02)
                                with closing(SQLiteStore(root/'state.db')) as store:
                                    engine = next(owner[1] for owner in store._job_engine_owners() if owner[0]=='batch-job-0')
                                    capability = store._get_direct_engine_recovery_capability(engine)
                                    gid = store._connection.execute('SELECT gid FROM saved_body_owners').fetchone()[0]
                                # The server write precedes aria2 consuming its socket buffer.
                                # Keep the tail gated while allowing the stock receiver to run.
                                await asyncio.sleep(.3)
                                assert processes.is_current_process_birth(engine.identity)
                                completed = _request_recovery_force_shutdown(capability, timeout=1, gid=gid)
                                assert type(completed) is int and completed==0
                                proof = dict(completed_length=completed, origin_prefix_sent=3601, original_engine_current=True)
                                (evidence/'pre-pause.json').write_text(json.dumps(proof))
                                current = (await call('downloads_query', dict(scope='status', id='batch-job-0')))['record']
                                pause = await call('downloads_control', dict(scope='job', action='pause', request_id='early-pause', job='batch-job-0', expected_revision=str(current['revision'])))
                                acknowledged = time.monotonic()
                                assert pause['status']=='applied' and pause['state']=='paused'
                                release.set()
                                assert processes.reconcile_process_birth(engine.identity)=='absent'
                                with pytest.raises(ProcessLookupError): os.killpg(engine.identity.process_group_id, 0)
                                assert 0<partial.stat().st_size<len(origin.payload)
                                assert partial.read_bytes()==origin.payload[:partial.stat().st_size]
                                assert control.stat().st_size>0 and control.stat().st_mode & 0o777==0o600
                                proof['stopped_pair_sizes'] = [p.stat().st_size for p in (partial, control)]
                                (evidence/'pre-pause.json').write_text(json.dumps(proof))
                                end = time.monotonic()+3
                                while not rows(root/'state.db', 'SELECT 1 FROM stopped_direct_bodies'):
                                    assert process.is_alive() and time.monotonic()<end, 'early positive pair checkpoint missing'
                                    await asyncio.sleep(.02)
                                with closing(SQLiteStore(root/'state.db')) as store:
                                    checkpoint, facts = store._saved_checkpoint(store._connection, 'batch-job-0')
                                    closure = json.loads(checkpoint['closure'])
                                    assert closure['completed_length']==0 and closure['observer_joined'] is True and closure['group_absent'] is True
                                    assert closure['process']==engine.identity.to_record()
                                    assert min(facts.payload[2], facts.control[2])>0
                                    # Actual original Popen wait and observer join qualify closure.
                                    births = [json.loads(line) for line in (evidence/f'fixture-{process.pid}.jsonl').read_text().splitlines()]
                                    assert any(row['kind']=='engine-waitable-child-reaped' and row['identity']==engine.identity.to_record() and row['waitpid']=='ECHILD' for row in births)
                                    effects = [json.loads(line) for line in (evidence/'scheduler-effects.jsonl').read_text().splitlines()]
                                    assert any(row['kind']=='observer-join' and not row['alive'] and row['monotonic']<=acknowledged for row in effects)
                                    destination = store._saved_destination('batch-job-0')
                                    reservation = store._effective_reservation(store._connection, 'batch-job-0')
                                    paths._require_saved_pair(destination, reservation, facts)
                                if pair!='intact':
                                    if pair=='empty': control.write_bytes(b'')
                                    elif pair=='missing': control.unlink()
                                    else: control.write_bytes(control.read_bytes()+b'changed')
                                    with pytest.raises((ValueError, OSError)):
                                        paths._require_saved_pair(destination, reservation, facts)
                                    if pair in {'empty','missing'}:
                                        with pytest.raises((ValueError, OSError)):
                                            paths._seal_stopped_pair(destination, reservation)
                                    rejected = await call('downloads_control', dict(scope='job', action='resume', request_id='early-rejected', job='batch-job-0', expected_revision=pause['revision'], expected_worker_epoch='1'))
                                    assert rejected['results'][0]['reason']=='incompatible_authority'
                                    assert rows(root/'state.db', 'SELECT 1 FROM saved_direct_continuations')==[]
                                    return
                                resume = await call('downloads_control', dict(scope='job', action='resume', request_id='early-resume', job='batch-job-0', expected_revision=pause['revision'], expected_worker_epoch='1'))
                                assert resume['results'][0]['outcome']=='existing_authority'
                                end = time.monotonic()+15
                                final = output/'Other/file-0.bin'
                                while not final.exists():
                                    assert process.is_alive() and time.monotonic()<end
                                    await asyncio.sleep(.03)
                                assert hashlib.sha256(final.read_bytes()).hexdigest()==expected
                                assert final.read_bytes()==origin.payload
                                (evidence/'result.json').write_text(json.dumps(dict(pair=pair, immutable_sha256=expected, closure=closure)))
                    asyncio.run(exercise())
            finally:
                release.set()


@pytest.mark.parametrize('completed', [None, False, True, -1, '0', 1.5, 11])
def test_stop_capture_rejects_unknown_malformed_or_out_of_range_counter(completed):
    from types import SimpleNamespace
    from hermes_downloads.worker import _DirectOwner
    controller = SimpleNamespace(_process=object(), readback=lambda **kw: SimpleNamespace(completed_length=completed, total_length=10))
    store = SimpleNamespace(_connection=None, _read_target_dispatch_cause=lambda *args: {'kind':'body'})
    owner = SimpleNamespace(active_direct_plan=SimpleNamespace(request_id='body', job=SimpleNamespace(job_id='job'), generation=0),
        active_terminal_plan=SimpleNamespace(gid='0'*16), active_publication=None,
        store=store, direct_controller=controller, observation_thread=object(), last_body_observer=None)
    assert _DirectOwner.capture_stopped_body(owner) is None


def test_stop_capture_preserves_positive_progress_with_unknown_total():
    from types import SimpleNamespace
    from hermes_downloads.worker import _DirectOwner
    controller = SimpleNamespace(_process=object(), readback=lambda **kw: SimpleNamespace(completed_length=3601, total_length=0))
    owner = SimpleNamespace(active_direct_plan=SimpleNamespace(request_id='body', job=SimpleNamespace(job_id='job'), generation=0),
        active_terminal_plan=SimpleNamespace(gid='0'*16), active_publication=None,
        store=SimpleNamespace(_connection=None, _read_target_dispatch_cause=lambda *args: {'kind':'body'}),
        direct_controller=controller, observation_thread=object(), last_body_observer=None)
    captured = _DirectOwner.capture_stopped_body(owner)
    assert captured is not None and captured[4]==3601
