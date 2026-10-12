"""Installed SDK controls through the existing owner, with private evidence."""
import asyncio
from contextlib import contextmanager, closing
from datetime import datetime, UTC
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import tempfile
import time

import pytest
from mcp import ClientSession
from mcp.client.stdio import stdio_client
from hermes_downloads import ipc, processes
from hermes_downloads.models import DownloadIntent, MaterializedJob, SourceKind
from hermes_downloads.store import SQLiteStore

from test_mcp_add_query import (sdk_lifecycle, private_worker, parameters,
    session_calls, result_record, snapshot, origin, evidence, owned_worker)
from test_add_batch import entry
from test_target_scheduler import _scheduler_worker
from test_direct_publication import _helpers, _close_fixture_process


def test_real_discovery_requires_downloads_control(private_worker, sdk_lifecycle):
    root, process = private_worker
    with origin() as requests:
        before = snapshot(root)
        initialized, tools, results = asyncio.run(session_calls(root,
            [('downloads_query', {'scope': 'health'})]))
        names = [tool.name for tool in tools.tools]
        assert result_record(results[0]) == {'worker_epoch': 1, 'queue_gate': 'paused'}
        assert process.poll() is None
        assert requests == [] and snapshot(root) == before
        assert list((root / 'output').iterdir()) == [root / 'output' / 'sentinel']
        assert (root / 'output' / 'sentinel').read_bytes() == b'untouched'
        assert not (root / 'effects.jsonl').exists()
        evidence('controls-discovery', tools=names, no_sql_effects=True,
            origin_requests=requests, output_unchanged=True)
        assert 'downloads_control' in names, 'DOWNLOADS_CONTROL_ABSENT: ' + repr(names)
        assert names == ['downloads_add', 'downloads_control', 'downloads_edit', 'downloads_replace_source', 'downloads_files', 'downloads_configure', 'downloads_query']
        schema = tools.tools[1].inputSchema
        assert schema['type']=='object' and schema['additionalProperties'] is False
        assert set(schema['required']) == {'scope','action','request_id'}
        assert set(schema['required']) <= set(schema['properties'])
        assert len(schema['oneOf']) == 9
        assert set(schema['properties']['scope']['enum']) == {'queue','job','targets','collection'}
        assert set(schema['properties']['action']['enum']) == {'pause','resume','remove','start','start_now','retry','preempt'}
        for branch in schema['oneOf']:
            assert branch['type']=='object' and branch['additionalProperties'] is False
            assert set(branch['required']) <= set(branch['properties'])


def targets(jobs, request='authorize', epoch='1', revision='0'):
    return dict(scope='targets', action='start', request_id=request,
        expected_worker_epoch=epoch, targets=[dict(job=job, expected_revision=revision) for job in jobs])


def test_control_decimal_and_closed_shapes_refuse_before_endpoint(tmp_path, sdk_lifecycle):
    root = tmp_path / 'absent'; root.mkdir()
    malformed = [0, 1, True, False, None, 1.0, '', '+1', '-1', ' 1', '1 ', '01', '00',
        '١', '１', '1\n', '9223372036854775808', '9'*1000]
    queue = dict(scope='queue', action='pause', request_id='q', expected_revision='0')
    job = dict(scope='job', action='pause', job='job', request_id='j', expected_revision='0')
    target = targets(['job'])
    invalid = []
    for value in malformed:
        invalid += [queue | dict(expected_revision=value), job | dict(expected_revision=value),
            target | dict(expected_worker_epoch=value),
            target | dict(targets=[dict(job='job', expected_revision=value)])]
    invalid += [target | dict(expected_worker_epoch='0'), target | dict(targets=[]),
        targets(['job']*2), targets([f'J{i}' for i in range(501)]),
        target | dict(targets=[dict(job='job', expected_revision=None)]),
        target | dict(targets=[dict(job='job', expected_revision='0', extra=1)]),
        queue | dict(action='remove'), job | dict(action='resume'), target | dict(action='resume'),
        queue | dict(job='job'), job | dict(expected_worker_epoch='1'), target | dict(expected_revision='0'),
        queue | dict(request_id=True), job | dict(job='../job'), queue | dict(extra='private')]
    invalid += [{k:v for k,v in value.items() if k != 'request_id'} for value in [queue, job, target]]
    invalid += [dict(scope='queue'), {'scope': None}, {'scope': 'unknown'}]
    calls = [('downloads_control', value) for value in invalid]
    calls += [('downloads_query', dict(scope='queue', cursor=None))]
    calls += [('downloads_control', queue), ('downloads_control', targets(['job'], epoch='9223372036854775807', revision='9223372036854775807'))]
    results = asyncio.run(session_calls(root, calls))[2]
    for result in results[:-3]:
        assert result.isError and result.content[0].text == 'downloads_control_invalid_input'
    assert results[-3].isError and results[-3].content[0].text == 'downloads_query_invalid_input'
    assert all(result.isError and result.content[0].text == 'downloads_control_unavailable' for result in results[-2:])
    assert list(root.iterdir()) == []


def test_real_queue_job_receipts_replay_conflict_and_numeric_query_compatibility(private_worker, sdk_lifecycle):
    root, process = private_worker
    async def exercise():
        async with stdio_client(parameters(root)) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                async def call(name, args):
                    return result_record(await session.call_tool(name, arguments=args))
                before = snapshot(root)
                queue = await call('downloads_query', {'scope':'queue'})
                assert queue == dict(worker_epoch='1', queue_gate='paused', revision='1')
                assert snapshot(root) == before
                await call('downloads_add', dict(request_id='add', items=[entry()]))
                paused = dict(scope='job', action='pause', job='batch-job-0', request_id='pause', expected_revision='0')
                receipt = await call('downloads_control', paused)
                assert receipt == dict(status='applied', job='batch-job-0', generation='0', revision='1', state='paused', authorized=False)
                assert await call('downloads_control', paused) == receipt
                stale = await call('downloads_control', paused | dict(request_id='stale', expected_revision='0'))
                assert stale['status'] == 'stale' and stale['revision'] == '1'
                changed = await session.call_tool('downloads_control', arguments=paused | dict(action='remove'))
                assert changed.isError and changed.content[0].text == 'downloads_control_command_conflict'
                resume = dict(scope='queue', action='resume', request_id='run', expected_revision='1')
                running = await call('downloads_control', resume)
                assert running == dict(applied=True, queue_gate='running', revision='2')
                held = await call('downloads_control', dict(scope='queue', action='pause', request_id='hold', expected_revision='2'))
                assert held == dict(applied=True, queue_gate='paused', revision='3')
                assert await call('downloads_control', resume) == running | dict(applied=False)
                stale_queue = await session.call_tool('downloads_control', arguments=dict(scope='queue', action='resume', request_id='stale-queue', expected_revision='1'))
                assert stale_queue.isError and stale_queue.content[0].text == 'downloads_control_command_conflict'
                removed = await call('downloads_control', paused | dict(action='remove', request_id='remove', expected_revision='1'))
                assert removed == dict(status='applied', job='batch-job-0', generation='0', revision='2', state='removed', authorized=False)
                assert await call('downloads_control', paused) == receipt
                status = await call('downloads_query', dict(scope='status', id='batch-job-0'))
                assert status['record']['revision'] == 2 and type(status['record']['revision']) is int
                assert await call('downloads_query', dict(scope='health')) == dict(worker_epoch=1, queue_gate='paused')
                assert await call('downloads_query', dict(scope='queue')) == dict(worker_epoch='1', queue_gate='paused', revision='3')
                assert not (root/'effects.jsonl').exists() and process.poll() is None
    asyncio.run(exercise())


def test_real_500_ordered_large_counter_target_receipts_mixed_and_replay(private_worker, sdk_lifecycle):
    root, process = private_worker
    maximum = (1 << 63) - 1
    jobs = ['J'+format(i, '03d')+'x'*124 for i in range(500)]
    with closing(SQLiteStore(root/'state.db')) as store:
        for i, job in enumerate(jobs):
            command = ipc.JobAddCommand(job, 'original-'+str(i), 'https://example.test/'+str(i), 0, i, 'Other', str(i)+'.bin', str(i)+'.bin')
            intent = DownloadIntent(job, command.request_id, command.payload_digest, command.source_url.encode(), None, maximum, maximum-1)
            materialized = MaterializedJob(job, intent, SourceKind.DIRECT, None, 0, i, None, False, False, False,
                'Other', None, command.partial_filename, command.selected_final_filename)
            assert store.apply_add(intent, materialized=materialized).applied
    async def exercise():
        async with stdio_client(parameters(root)) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                command = targets(jobs, request='T'+'x'*127, revision=str(maximum-1))
                started = time.monotonic()
                receipt = result_record(await session.call_tool('downloads_control', arguments=command))
                assert time.monotonic()-started < 5
                assert set(receipt) == {'op','protocol_version','request_id','status','readback_kind','replayed','worker_epoch','execution_effect','results'}
                assert receipt['protocol_version'] == 2 and type(receipt['protocol_version']) is int
                assert receipt['worker_epoch'] == '1' and receipt['execution_effect'] == 'none' and receipt['replayed'] is False
                assert [r['job'] for r in receipt['results']] == jobs
                for i, row in enumerate(receipt['results']):
                    assert row == dict(index=i, job=jobs[i], outcome='new_authority', reason=None,
                        round_generation='1', captured_generation=str(maximum), captured_revision=str(maximum-1), held_by=['global'])
                    assert type(row['index']) is int
                before = snapshot(root)
                replay = result_record(await session.call_tool('downloads_control', arguments=command))
                assert replay == receipt | dict(replayed=True) and snapshot(root) == before
                for changed in [command | dict(expected_worker_epoch='2'), command | dict(targets=list(reversed(command['targets']))),
                        command | dict(targets=command['targets'][:-1]+[dict(job=jobs[-1], expected_revision='0')])]:
                    conflict = await session.call_tool('downloads_control', arguments=changed)
                    assert conflict.isError and conflict.content[0].text == 'downloads_control_command_conflict'
                mixed = targets(jobs[:-1]+['unknown'], request='mixed', revision=str(maximum))
                mixed['targets'][0]['expected_revision'] = '0'
                record = result_record(await session.call_tool('downloads_control', arguments=mixed))
                assert len(record['results']) == 500 and [r['index'] for r in record['results']] == list(range(500))
                assert record['results'][0]['outcome'] == 'stale'
                assert all(r['outcome']=='existing_authority' for r in record['results'][1:-1])
                assert record['results'][-1] == dict(index=499, job='unknown', outcome='blocked', reason='unknown_job',
                    round_generation=None, captured_generation=None, captured_revision=None, held_by=None)
                assert result_record(await session.call_tool('downloads_control', arguments=command)) == replay
                evidence('controls-500', ordered=500, mixed=True, replay_original=True, conflict=True,
                    signed64_counters=True, projected_bytes=len(json.dumps(receipt).encode()), execution_effect='none')
    asyncio.run(exercise())
    assert process.poll() is None and not (root/'effects.jsonl').exists()


def test_real_queue_signed64_counters_and_legacy_health_remain_distinct(private_worker, sdk_lifecycle):
    root, process = private_worker
    maximum = (1 << 63)-1
    with sqlite3.connect(root/'state.db') as connection:
        connection.execute("UPDATE settings SET value=? WHERE key='worker_epoch'", (str(maximum),))
        connection.execute("UPDATE settings SET revision=? WHERE key='queue_gate'", (maximum-1,))
    before = snapshot(root)
    _, _, results = asyncio.run(session_calls(root, [('downloads_query', dict(scope='queue')),
        ('downloads_query', dict(scope='health'))]))
    assert result_record(results[0]) == dict(worker_epoch=str(maximum), queue_gate='paused', revision=str(maximum-1))
    assert result_record(results[1]) == dict(worker_epoch=maximum, queue_gate='paused')
    assert snapshot(root) == before
    _, _, results = asyncio.run(session_calls(root, [('downloads_control', dict(scope='queue', action='pause',
        request_id='max-queue', expected_revision=str(maximum-1))), ('downloads_query', dict(scope='queue'))]))
    assert result_record(results[0]) == dict(applied=True, queue_gate='paused', revision=str(maximum))
    assert result_record(results[1]) == dict(worker_epoch=str(maximum), queue_gate='paused', revision=str(maximum))
    assert process.poll() is None and not (root/'effects.jsonl').exists()


def test_target_cold_epoch_and_original_receipt_replay(sdk_lifecycle):
    with tempfile.TemporaryDirectory(prefix='mcp10-', dir='/private/tmp') as directory:
        root = Path(directory); root.chmod(0o700)
        (root/'output').mkdir(mode=0o700)
        command = targets(['batch-job-0'])
        with owned_worker(root):
            result_record(asyncio.run(session_calls(root, [('downloads_add', dict(request_id='add', items=[entry()]))]))[2][0])
            captured = result_record(asyncio.run(session_calls(root, [('downloads_control', command)]))[2][0])
        with owned_worker(root):
            _, _, results = asyncio.run(session_calls(root, [('downloads_query', dict(scope='queue')),
                ('downloads_control', command), ('downloads_control', command | dict(request_id='stale-epoch'))]))
            assert result_record(results[0]) == dict(worker_epoch='2', queue_gate='paused', revision='2')
            assert result_record(results[1]) == captured | dict(replayed=True)
            assert results[2].isError and results[2].content[0].text == 'downloads_control_unavailable'
            with closing(SQLiteStore(root/'state.db')) as store:
                head = store._connection.execute('SELECT * FROM job_authorization_heads').fetchone()
                assert head['current_worker_epoch'] == 2 and head['intent_status'] == 'inactive'
                assert store.get_direct_engine_record() is None


def test_real_target_reports_manual_collection_and_not_due_holds(private_worker, sdk_lifecycle):
    root, _ = private_worker
    with closing(SQLiteStore(root/'state.db')) as store:
        store.apply_add_batch(ipc.AddBatchCommand('group', 'collection', [entry()]))
        job = store.get_materialized_job('batch-job-0')
        store.set_collection_hold(job.queue_collection_id, held=True)
        manual = ipc.JobControlCommand(job.job_id, 'pause', 'manual', 0)
        store.apply_job_control(job_id=job.job_id, action=manual.action, request_id=manual.request_id,
            payload_digest=manual.payload_digest, expected_revision=manual.expected_revision)
        intent = DownloadIntent('scheduled', 'original-schedule', 'b'*64, b'https://example.test/s', None, 4, 7)
        scheduled = MaterializedJob('scheduled', intent, SourceKind.DIRECT, 'cohort', 0, 3,
            datetime(2032,1,1,tzinfo=UTC), False, True, False, 'Other', None, 's.bin', 's.bin')
        store.apply_add(intent, materialized=scheduled)
        store.set_collection_hold('cohort', held=True)
    _, _, results = asyncio.run(session_calls(root, [('downloads_control', targets(['batch-job-0'], revision='1')),
        ('downloads_control', targets(['scheduled'], request='scheduled-target', revision='7'))]))
    assert result_record(results[0])['results'][0]['held_by'] == ['global','collection','manual']
    scheduled_result = result_record(results[1])['results'][0]
    assert scheduled_result['reason'] == 'incompatible_authority'
    assert scheduled_result['held_by'] == ['global','collection','manual','not_due']


@contextmanager
def body_worker(root, origin_url, ledger, *, barrier=None, entered=None, release=None, crash=False, recover_socket=False):
    context = multiprocessing.get_context('spawn')
    ready, shutdown, stopped = [context.Event() for _ in range(3)]
    results = context.Queue()
    process = context.Process(target=_scheduler_worker,
        args=(str(root), ready, shutdown, stopped, results, origin_url, str(ledger)),
        kwargs=dict(barrier=barrier,entered=entered,release=release,recover_socket=recover_socket))
    process.start()
    try:
        assert ready.wait(6), 'private scheduler did not become ready'
        yield process
    finally:
        if not crash:shutdown.set()
        try:
            _close_fixture_process(process, ledger,crash=crash)
            if crash:
                import signal
                assert process.exitcode == -signal.SIGKILL
            else:
                assert process.exitcode == 0
                assert results.get(timeout=3) == ('result', None)
        finally:
            try:
                results.close()
            finally:
                results.join_thread()


@pytest.mark.parametrize('action', ['pause', 'remove', 'queue'])
def test_real_body_start_containment_retains_bytes_and_global_blocks_next(tmp_path, monkeypatch, sdk_lifecycle, action):
    directory = os.environ.get('T18_IMPLEMENTATION_RUN')
    ledger = Path(tempfile.mkdtemp(prefix='body-'+action+'-', dir=directory or tmp_path))
    home = tmp_path/'home'; home.mkdir(mode=0o700)
    monkeypatch.setenv('HOME', str(home))
    output = home/'Downloads/Hermes'; output.mkdir(parents=True, mode=0o700)
    # Keep the real origin serving while MCP observes positive growth and contains it.
    monkeypatch.setattr(_helpers._FIXTURE_MODULE, '_RANGE_PACED_CHUNK_DELAY_SECONDS', .04)
    with tempfile.TemporaryDirectory(prefix='mcp10-body-', dir='/private/tmp') as temporary:
        root = Path(temporary); root.chmod(0o700)
        with _helpers._origin_type()(payload_size=8*1024*1024) as origin_server:
            response=origin_server._response_for
            origin_server._response_for=lambda endpoint,span: (lambda status,headers,body:(status,headers|{'ETag':'"control-original"'},body))(*response(endpoint,span))
            origin_thread = origin_server._thread
            with body_worker(root, origin_server.url(), ledger) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read, write):
                        async with ClientSession(read, write) as session:
                            await session.initialize()
                            async def call(name, arguments):
                                return result_record(await session.call_tool(name, arguments=arguments))
                            values = [entry(i, source_url=origin_server.url(),
                                partial_filename=f'body-{i}.bin', selected_final_filename=f'body-{i}.bin')
                                for i in range(2 if action=='queue' else 1)]
                            await call('downloads_add', dict(request_id='body-add', items=values))
                            request = targets([value['job'] for value in values])
                            authorization = await call('downloads_control', request)
                            assert authorization['execution_effect'] == 'none'
                            assert all(row['held_by']==['global'] for row in authorization['results'])
                            assert origin_server.ledger.connection_count == 0
                            assert not (output/'.incomplete').exists()
                            assert await call('downloads_control', dict(scope='queue', action='resume',
                                request_id='body-run', expected_revision='1')) == dict(applied=True, queue_gate='running', revision='2')
                            partial = output/'.incomplete/batch-job-0/body-0.bin'
                            deadline = time.monotonic()+12
                            while not partial.exists() or partial.stat().st_size <= 0:
                                assert process.is_alive() and time.monotonic() < deadline
                                await asyncio.sleep(.02)
                            positive_bytes = partial.stat().st_size
                            assert 0 < positive_bytes < len(origin_server.payload)
                            with closing(SQLiteStore(root/'state.db')) as store:
                                engine = next((owner[1] for owner in store._job_engine_owners() if owner[0]=='batch-job-0'),None)
                            assert engine is not None
                            identity = engine.identity
                            assert processes.is_current_process_birth(identity)
                            if action=='queue':
                                command = dict(scope='queue', action='pause', request_id='body-hold', expected_revision='2')
                            else:
                                current = await call('downloads_query', dict(scope='status', id='batch-job-0'))
                                command = dict(scope='job', action=action, job='batch-job-0', request_id='body-'+action,
                                    expected_revision=str(current['record']['revision']))
                            contained = await call('downloads_control', command)
                            received = time.monotonic()
                            assert contained.get('applied', contained.get('status')=='applied')
                            assert processes.reconcile_process_birth(identity) == 'absent'
                            with pytest.raises(ProcessLookupError):
                                os.killpg(identity.process_group_id, 0)
                            retained = partial.read_bytes()
                            assert len(retained) >= positive_bytes
                            await asyncio.sleep(.35)
                            assert partial.read_bytes() == retained
                            assert not (output/'Other/body-0.bin').exists()
                            closures = [json.loads(line) for line in (ledger/f'fixture-{process.pid}.jsonl').read_text().splitlines()]
                            assert any(row['kind']=='engine-birth' and row['identity']==identity.to_record()
                                and row['os_argv'][0]=='/opt/homebrew/bin/aria2c' for row in closures)
                            assert any(row['kind']=='engine-waitable-child-reaped' and row['identity']==identity.to_record()
                                and row['waitpid']=='ECHILD' and type(row['returncode']) is int for row in closures)
                            effects = [json.loads(line) for line in (ledger/'scheduler-effects.jsonl').read_text().splitlines()]
                            assert any(row['kind']=='observer-join' and not row['alive'] and row['monotonic']<=received for row in effects)
                            if action=='pause':
                                await call('downloads_control',dict(scope='queue',action='pause',request_id='hashless-global-hold',expected_revision='2'))
                                supported = await call('downloads_control',dict(scope='job',action='resume',job='batch-job-0',
                                    request_id='hashless-resume',expected_revision=contained['revision'],expected_worker_epoch='1'))
                                assert supported['results'][0]['outcome']=='existing_authority'
                                assert supported['results'][0]['held_by']==['global']
                                assert partial.read_bytes()==retained
                            replay = await call('downloads_control', request)
                            assert replay == authorization | dict(replayed=True)
                            assert replay['execution_effect'] == 'none'
                            if action=='queue':
                                with closing(SQLiteStore(root/'state.db')) as store:
                                    next_job = store.get_materialized_job('batch-job-1')
                                    assert next_job.authorized and store.get_job('batch-job-1').state=='queued'
                                    assert store._connection.execute("SELECT count(*) FROM target_dispatch_causes WHERE job_id='batch-job-1'").fetchone()[0] == 0
                                assert not (output/'.incomplete/batch-job-1/body-1.bin').exists()
                                assert sum(row['kind']=='causal-engine-start' for row in effects) == 1
                            evidence('controls-body-contained', action=action, positive_bytes=positive_bytes,
                                retained_bytes=len(retained), stable=True, original_engine_absent=True,
                                original_wait_reaped=True, observer_joined_before_receipt=True,
                                next_job_blocked=action=='queue', authorization_replay_original=True)
                asyncio.run(exercise())
            assert not process.is_alive() and process.exitcode == 0
        assert origin_thread is not None and not origin_thread.is_alive()
        evidence('controls-origin-joined', action=action, joined=True,
            requests=origin_server.ledger.request_count, response_body_bytes=origin_server.ledger.response_body_bytes)


def test_public_configure_applies_owned_concurrent_slots_and_independent_pause(tmp_path,monkeypatch,sdk_lifecycle):
    import hashlib
    directory=Path(tempfile.mkdtemp(prefix='slots15-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));directory.chmod(0o700)
    home=directory/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.06)
    with tempfile.TemporaryDirectory(prefix='slots-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=64*1024*1024) as long_origin, _helpers._origin_type()(payload_size=16*1024*1024) as short_origin:
            with body_worker(root,(long_origin.url(),short_origin.url()),directory) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            configure=dict(request_id='configure-slots',concurrency='2',connections='4',global_limit_bps=None)
                            accepted=await call('downloads_configure',configure)
                            assert accepted['status']=='pending' and accepted['revision']=='1'
                            assert await call('downloads_configure',configure)==accepted|dict(replayed=True)
                            assert long_origin.ledger.response_body_bytes==short_origin.ledger.response_body_bytes==0
                            await call('downloads_add',dict(request_id='slots-add',items=[entry(0,source_url=long_origin.url(),expected_sha256=hashlib.sha256(long_origin.payload).hexdigest()),entry(1,source_url=short_origin.url(),expected_sha256=hashlib.sha256(short_origin.payload).hexdigest())]))
                            await call('downloads_control',targets(['batch-job-0','batch-job-1']))
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='slots-go',expected_revision='1'))
                            end=time.monotonic()+12
                            while True:
                                a=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                b=(await call('downloads_query',dict(scope='status',id='batch-job-1')))['record']
                                partial=output/'.incomplete/batch-job-1/file-1.bin'
                                if a['state']==b['state']=='downloading' and partial.exists() and partial.stat().st_blocks*512>=2*1024*1024:break
                                assert time.monotonic()<end and process.is_alive(),(a,b)
                                await asyncio.sleep(.03)
                            with closing(SQLiteStore(root/'state.db')) as store:
                                owners=store._job_engine_owners()
                                assert len(owners)==2 and len({row[1].identity.leader_pid for row in owners})==2
                                original={row[0]:row[1] for row in owners}
                                assert store.get_direct_engine_record() is None
                            pause=await call('downloads_control',dict(scope='job',action='pause',request_id='pause-sibling',job='batch-job-1',expected_revision=str(b['revision'])))
                            assert pause['state']=='paused' and processes.reconcile_process_birth(original['batch-job-1'].identity)=='absent'
                            assert processes.reconcile_process_birth(original['batch-job-0'].identity)=='current'
                            held_a=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                            assert held_a['state']=='downloading'
                            end=time.monotonic()+8
                            while not (output/'.incomplete/batch-job-1/file-1.bin.aria2').exists():
                                assert time.monotonic()<end
                                await asyncio.sleep(.03)
                            resume=await call('downloads_control',dict(scope='job',action='resume',request_id='resume-sibling',job='batch-job-1',expected_revision=pause['revision'],expected_worker_epoch='1'))
                            assert resume['results'][0]['outcome']=='existing_authority'
                            end=time.monotonic()+20
                            while not (output/'Other/file-1.bin').exists():
                                assert time.monotonic()<end
                                await asyncio.sleep(.04)
                            assert (output/'Other/file-1.bin').read_bytes()==short_origin.payload
                            assert processes.reconcile_process_birth(original['batch-job-0'].identity)=='current'
                            a=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                            await call('downloads_control',dict(scope='job',action='pause',request_id='pause-long',job='batch-job-0',expected_revision=str(a['revision'])))
                            assert processes.reconcile_process_birth(original['batch-job-0'].identity)=='absent'
                            settings=await call('downloads_query',dict(scope='settings'))
                            assert settings['desired']==settings['applied'] and settings['applied']['concurrency']=='2' and settings['speed_units']=='bytes per second'
                            (directory/'slots-result.json').write_text(json.dumps(dict(originals={j:r.identity.to_record() for j,r in original.items()},independent_pause=True,short_finished_while_long_owned=True,synthetic=True)))
                asyncio.run(exercise())


def test_public_zero_configuration_seals_truthful_stop_and_positive_resumes(tmp_path,monkeypatch,sdk_lifecycle):
    import hashlib
    directory=Path(tempfile.mkdtemp(prefix='zero15-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));directory.chmod(0o700)
    home=directory/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.05)
    with tempfile.TemporaryDirectory(prefix='zero-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=16*1024*1024) as origin:
            with body_worker(root,origin.url(),directory) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='zero-add',items=[entry(source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest())]))
                            authorization=await call('downloads_control',targets(['batch-job-0']))
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='zero-go',expected_revision='1'))
                            partial=output/'.incomplete/batch-job-0/file-0.bin'
                            end=time.monotonic()+10
                            while not partial.exists() or partial.stat().st_blocks*512<2*1024*1024:
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            stopped=await call('downloads_configure',dict(request_id='zero-wait',global_limit_bps='0'))
                            assert stopped['status']=='pending'
                            end=time.monotonic()+8
                            while (await call('downloads_query',dict(scope='settings')))['status']!='applied':
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                            status=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                            assert status['state']=='paused'
                            with closing(SQLiteStore(root/'state.db')) as store:
                                assert store._job_engine_owners()==[]
                                assert store._connection.execute("SELECT count(*) FROM job_control_commands WHERE action='pause'").fetchone()[0]==0
                                assert store._connection.execute("SELECT stop_kind FROM stopped_control_bodies").fetchall()[0][0]=='configure'
                            before=origin.ledger.response_body_bytes
                            await call('downloads_query',dict(scope='list'));await asyncio.sleep(.1)
                            assert origin.ledger.response_body_bytes==before
                            await call('downloads_configure',dict(request_id='zero-clear',global_limit_bps=None))
                            end=time.monotonic()+20
                            while not (output/'Other/file-0.bin').exists():
                                assert time.monotonic()<end;await asyncio.sleep(.03)
                            assert (output/'Other/file-0.bin').read_bytes()==origin.payload
                            assert await call('downloads_control',targets(['batch-job-0']))==authorization|dict(replayed=True)
                asyncio.run(exercise())

@pytest.mark.parametrize('known',[True,False,'mismatch'])
def test_public_replace_source_quiesces_preserves_creation_and_uses_fresh_authority(tmp_path,monkeypatch,sdk_lifecycle,known):
    import hashlib
    directory=Path(tempfile.mkdtemp(prefix='replace15-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));directory.chmod(0o700)
    home=directory/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.05)
    with tempfile.TemporaryDirectory(prefix='replace-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=16*1024*1024) as old, _helpers._origin_type()(payload_size=16*1024*1024) as fresh:
            if known=='mismatch':fresh.payload=fresh.changed_payload
            with body_worker(root,(old.url(),fresh.url()),directory) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='replace-add',items=[entry(source_url=old.url(),expected_sha256=hashlib.sha256(old.payload).hexdigest() if known else None)]))
                            await call('downloads_control',targets(['batch-job-0']));await call('downloads_control',dict(scope='queue',action='resume',request_id='replace-go',expected_revision='1'))
                            partial=output/'.incomplete/batch-job-0/file-0.bin';end=time.monotonic()+10
                            while not partial.exists() or partial.stat().st_blocks*512<2*1024*1024:
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            with sqlite3.connect(root/'state.db') as db:
                                creation=db.execute('SELECT source_url FROM jobs').fetchone()[0]
                                receipt=db.execute('SELECT receipt FROM add_batch_commands').fetchone()[0]
                            replacement=dict(id='batch-job-0',url=fresh.url(),request_id='replace-source')
                            accepted=await call('downloads_replace_source',replacement)
                            assert accepted['status']=='pending'
                            end=time.monotonic()+12
                            while True:
                                detail=await call('downloads_query',dict(scope='detail',id='batch-job-0'))
                                if detail['source_replacement']['status']!='pending':break
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            assert detail['source_replacement']['status']==('applied' if known else 'blocked')
                            assert await call('downloads_replace_source',replacement)==accepted|dict(replayed=True)
                            if known=='mismatch':
                                end=time.monotonic()+22
                                while True:
                                    current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                    with closing(SQLiteStore(root/'state.db')) as store:
                                        bodies=store._connection.execute("SELECT count(*) FROM target_dispatch_causes WHERE kind='body'").fetchone()[0]
                                        owners=store._job_engine_owners()
                                    if bodies==2 and current['state']=='paused' and not owners:break
                                    assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.03)
                                assert not (output/'Other/file-0.bin').exists()
                                with closing(SQLiteStore(root/'state.db')) as store:retained_name=store._saved_continuation(store._connection,'batch-job-0').retained_names[0]
                                retained=partial.parent/retained_name
                                assert retained.exists() and fresh.ledger.response_body_bytes>0
                                digest=hashlib.sha256(retained.read_bytes()).hexdigest();await asyncio.sleep(.1)
                                assert hashlib.sha256(retained.read_bytes()).hexdigest()==digest
                            elif known:
                                end=time.monotonic()+22
                                while not (output/'Other/file-0.bin').exists():
                                    assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.03)
                                assert (output/'Other/file-0.bin').read_bytes()==old.payload
                                assert fresh.ledger.response_body_bytes>0
                                assert any(row.range_header is not None for row in fresh.ledger.entries)
                            else:
                                before=partial.read_bytes();await asyncio.sleep(.1)
                                assert partial.read_bytes()==before and fresh.ledger.response_body_bytes==0
                                assert detail['source_replacement']['alternative']=='separate_download'
                            with sqlite3.connect(root/'state.db') as db:
                                assert db.execute('SELECT source_url FROM jobs').fetchone()[0]==creation
                                assert db.execute('SELECT receipt FROM add_batch_commands').fetchone()[0]==receipt
                asyncio.run(exercise())

def test_public_edits_reach_order_schedule_cap_and_destination(tmp_path,monkeypatch,sdk_lifecycle):
    import hashlib
    directory=Path(tempfile.mkdtemp(prefix='edit15-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));directory.chmod(0o700)
    home=directory/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    with tempfile.TemporaryDirectory(prefix='edit-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=1024) as origin:
            with body_worker(root,origin.url(),directory) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='edit-add',items=[entry(i,source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest()) for i in range(2)]))
                            first=dict(ids=['batch-job-0'],patch=dict(priority=10,order_key='9',scheduled_for='2032-01-01T12:00:00+03:30',limit_bps='102400'),request_id='edit-first')
                            receipt=await call('downloads_edit',first)
                            second=dict(ids=['batch-job-1'],patch=dict(destination=dict(category='Documents',collection=None)),request_id='edit-destination')
                            await call('downloads_edit',second)
                            end=time.monotonic()+5
                            while (await call('downloads_query',dict(scope='detail',id='batch-job-1')))['edit']['status']!='applied':
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                            await call('downloads_control',targets(['batch-job-0','batch-job-1'],revision='1'))
                            await call('downloads_control',dict(scope='queue',action='resume',request_id='edit-go',expected_revision='1'))
                            end=time.monotonic()+10
                            while not (output/'Documents/file-1.bin').exists():
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.03)
                            assert not (output/'Other/file-0.bin').exists()
                            detail=await call('downloads_query',dict(scope='detail',id='batch-job-0'))
                            assert detail['effective']['scheduled_for']=='2032-01-01T12:00:00+03:30' and detail['effective']['limit_bps']=='102400'
                            current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                            await call('downloads_control',dict(scope='job',action='start_now',job='batch-job-0',request_id='edit-start-now',expected_revision=str(current['revision']),expected_worker_epoch='1'))
                            end=time.monotonic()+10
                            while not (output/'Other/file-0.bin').exists():
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.03)
                            assert await call('downloads_edit',first)==receipt|dict(replayed=True)
                            assert (output/'Documents/file-1.bin').read_bytes()==origin.payload
                            with closing(SQLiteStore(root/'state.db')) as store:
                                captured=store.get_batch_creation_intent('batch-job-1')
                                assert captured.original_job.category=='Other' and captured.reservation.target_component=='Other'
                asyncio.run(exercise())

@pytest.mark.parametrize('destination_change',[False,True])
def test_public_active_edit_retains_checkpoint_and_attempt_budget(tmp_path,monkeypatch,sdk_lifecycle,destination_change):
    import hashlib
    directory=Path(tempfile.mkdtemp(prefix='activeedit15-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));directory.chmod(0o700)
    home=directory/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.05)
    with tempfile.TemporaryDirectory(prefix='activeedit-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=16*1024*1024) as origin:
            if destination_change:
                response=origin._response_for;requests=[]
                def first_failure(*args):
                    requests.append(True)
                    return (503,{},b'') if len(requests)==1 else response(*args)
                origin._response_for=first_failure
            with body_worker(root,origin.url(),directory) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='activeedit-add',items=[entry(source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest())]))
                            await call('downloads_control',targets(['batch-job-0']));await call('downloads_control',dict(scope='queue',action='resume',request_id='activeedit-go',expected_revision='1'))
                            partial=output/'.incomplete/batch-job-0/file-0.bin';end=time.monotonic()+20
                            while not partial.exists() or partial.stat().st_blocks*512<2*1024*1024:
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            with closing(SQLiteStore(root/'state.db')) as store:
                                before=store.get_retry_budget('batch-job-0')
                            edit=dict(ids=['batch-job-0'],patch=dict(priority=8,limit_bps=None),request_id='activeedit-change')
                            if destination_change:edit['patch']['destination']=dict(category='Documents',collection=None)
                            marker=partial.parent/'.hermes-reservation';original_marker=(marker.stat().st_ino,marker.read_bytes())
                            with closing(SQLiteStore(root/'state.db')) as store:original_reservation=store.get_publication_reservation('batch-job-0')
                            receipt=await call('downloads_edit',edit)
                            end=time.monotonic()+25
                            final=output/('Documents' if destination_change else 'Other')/'file-0.bin'
                            while not final.exists():
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.03)
                            assert final.read_bytes()==origin.payload
                            assert not destination_change or not (output/'Other/file-0.bin').exists()
                            assert any(row.range_header is not None for row in origin.ledger.entries)
                            assert await call('downloads_edit',edit)==receipt|dict(replayed=True)
                            with closing(SQLiteStore(root/'state.db')) as store:
                                after=store.get_retry_budget('batch-job-0')
                                if destination_change:
                                    assert before is not None and after is not None and before.ordinary_attempts==1
                                    assert (before.generation,before.budget_number,before.ordinary_attempts,before.exhausted)==(after.generation,after.budget_number,after.ordinary_attempts,after.exhausted)
                                else:assert before is None and after is None
                                assert store.get_publication_reservation('batch-job-0')==original_reservation
                            end=time.monotonic()+5
                            while marker.exists():
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                            assert not list(partial.parent.glob('.hermes-resume-*'))
                            with closing(SQLiteStore(root/'state.db')) as store:
                                assert store._connection.execute("SELECT stop_kind FROM stopped_control_bodies").fetchone()[0]=='edit'
                                assert store._connection.execute("SELECT count(*) FROM job_control_commands WHERE action='pause'").fetchone()[0]==0
                asyncio.run(exercise())


def test_public_files_reveal_and_confirmed_exact_final_purge(tmp_path,monkeypatch,sdk_lifecycle):
    import hashlib
    directory=Path(tempfile.mkdtemp(prefix='files15-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));directory.chmod(0o700)
    home=directory/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    with tempfile.TemporaryDirectory(prefix='files-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=1024) as origin:
            with body_worker(root,origin.url(),directory) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='files-add',items=[entry(source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest())]))
                            await call('downloads_control',targets(['batch-job-0']));await call('downloads_control',dict(scope='queue',action='resume',request_id='files-go',expected_revision='1'))
                            end=time.monotonic()+10
                            while True:
                                with closing(SQLiteStore(root/'state.db')) as store:
                                    claim=store._read_direct_cleanup_claim(store._connection,'batch-job-0',{})
                                    finished=claim is not None and claim.phase=='finished'
                                if finished:break
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            final=output/'Other/file-0.bin';sentinel=final.parent/'unknown.sidecar';sentinel.write_bytes(b'unknown')
                            reveal=dict(action='reveal',ids=['batch-job-0'],request_id='files-reveal')
                            accepted=await call('downloads_files',reveal)
                            assert accepted['status']=='pending'
                            end=time.monotonic()+5
                            while True:
                                detail=await call('downloads_query',dict(scope='detail',id='batch-job-0'))
                                if detail['files']['status']!='pending':break
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                            assert detail['files']['status']=='reveal_pending' and detail['files']['path']==str(final)
                            assert await call('downloads_files',reveal)==accepted|dict(replayed=True)
                            refused=await session.call_tool('downloads_files',arguments=dict(action='purge',ids=['batch-job-0'],request_id='files-unconfirmed'))
                            assert refused.isError and final.exists()
                            purge=dict(action='purge',ids=['batch-job-0'],request_id='files-purge',confirmed=True)
                            receipt=await call('downloads_files',purge)
                            end=time.monotonic()+5
                            while final.exists():
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            end=time.monotonic()+5
                            while (await call('downloads_query',dict(scope='detail',id='batch-job-0')))['files']['status']!='purged':
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                            assert sentinel.read_bytes()==b'unknown'
                            assert await call('downloads_files',purge)==receipt|dict(replayed=True)
                            assert (await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']['state']=='completed'
                asyncio.run(exercise())


@pytest.mark.parametrize('removed',[False,True])
def test_public_active_purge_contains_and_deletes_only_recorded_stopped_artifacts(tmp_path,monkeypatch,sdk_lifecycle,removed):
    import hashlib
    directory=Path(tempfile.mkdtemp(prefix='activepurge15-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));directory.chmod(0o700)
    home=directory/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.08)
    with tempfile.TemporaryDirectory(prefix='activepurge-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=32*1024*1024) as origin:
            with body_worker(root,origin.url(),directory) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='activepurge-add',items=[entry(source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest())]))
                            await call('downloads_control',targets(['batch-job-0']));await call('downloads_control',dict(scope='queue',action='resume',request_id='activepurge-go',expected_revision='1'))
                            partial=output/'.incomplete/batch-job-0/file-0.bin';end=time.monotonic()+10
                            while not partial.exists() or partial.stat().st_blocks*512<2*1024*1024:
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            status=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                            paused=await call('downloads_control',dict(scope='job',action='pause',request_id='activepurge-pause',job='batch-job-0',expected_revision=str(status['revision'])))
                            end=time.monotonic()+6
                            while True:
                                with closing(SQLiteStore(root/'state.db')) as store:checkpoint=store._saved_checkpoint(store._connection,'batch-job-0')
                                if checkpoint is not None:break
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                            await call('downloads_control',dict(scope='job',action='resume',request_id='activepurge-resume',job='batch-job-0',expected_revision=paused['revision'],expected_worker_epoch='1'))
                            end=time.monotonic()+10
                            while True:
                                with closing(SQLiteStore(root/'state.db')) as store:
                                    bodies=store._connection.execute("SELECT count(*) FROM target_dispatch_causes WHERE kind='body'").fetchone()[0];owners=store._job_engine_owners()
                                if bodies==2 and owners and owners[0][1] is not None and partial.stat().st_blocks*512>3*1024*1024:break
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                            identity=owners[0][1].identity;sentinel=partial.parent/'unknown.sidecar';sentinel.write_bytes(b'unknown')
                            if removed:
                                current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                stopped=await call('downloads_control',dict(scope='job',action='pause',request_id='remove-pause',job='batch-job-0',expected_revision=str(current['revision'])))
                                end=time.monotonic()+6
                                while True:
                                    with closing(SQLiteStore(root/'state.db')) as store:sealed=store._saved_checkpoint(store._connection,'batch-job-0')
                                    if sealed is not None and sealed[0]['source_request_id']!=checkpoint[0]['source_request_id']:break
                                    assert time.monotonic()<end;await asyncio.sleep(.02)
                                deleted=await call('downloads_control',dict(scope='job',action='remove',request_id='remove-before-purge',job='batch-job-0',expected_revision=stopped['revision']))
                                assert deleted['status']=='applied' and deleted['state']=='removed'
                                assert partial.exists() and sentinel.exists()
                            request=dict(action='purge',ids=['batch-job-0'],request_id='activepurge-files',confirmed=True)
                            receipt=await call('downloads_files',request);end=time.monotonic()+8
                            while True:
                                detail=await call('downloads_query',dict(scope='detail',id='batch-job-0'))
                                if detail['files']['status'] not in {'pending','purge_authorized','saved_purge_authorized'}:break
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            assert detail['files']['status']=='purged'
                            assert processes.reconcile_process_birth(identity)=='absent'
                            assert list(partial.parent.iterdir())==[sentinel] and sentinel.read_bytes()==b'unknown'
                            assert not (output/'Other/file-0.bin').exists()
                            before=origin.ledger.response_body_bytes;await asyncio.sleep(.15)
                            assert origin.ledger.response_body_bytes==before
                            assert await call('downloads_files',request)==receipt|dict(replayed=True)
                asyncio.run(exercise())


def test_public_queue_start_freezes_more_than_500_and_replay_never_expands(private_worker,sdk_lifecycle):
    root,process=private_worker
    with origin() as requests:
        async def exercise():
            async with stdio_client(parameters(root)) as (read,write):
                async with ClientSession(read,write) as session:
                    await session.initialize()
                    async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                    for offset,count in ((0,500),(500,1)):
                        await call('downloads_add',dict(request_id='capture-add-'+str(offset),items=[entry(i,source_url='https://downloads.example.test/capture.bin') for i in range(offset,offset+count)]))
                    await call('downloads_control',dict(scope='job',action='pause',request_id='capture-manual',job='batch-job-500',expected_revision='0'))
                    request=dict(scope='queue',action='start',request_id='capture-queue',expected_worker_epoch='1')
                    receipt=await call('downloads_control',request)
                    assert receipt['status']=='pending' and receipt['member_count']=='501' and receipt['cohort_count']=='2'
                    await call('downloads_add',dict(request_id='capture-later',items=[entry(501,source_url='https://downloads.example.test/later.bin')]))
                    end=time.monotonic()+15
                    while True:
                        status=await call('downloads_query',dict(scope='control',id='capture-queue'))
                        if status['status']!='pending':break
                        assert time.monotonic()<end;await asyncio.sleep(.02)
                    assert status['status']=='applied' and status['member_count']=='501'
                    assert await call('downloads_control',request)==receipt|dict(replayed=True)
                    with closing(SQLiteStore(root/'state.db')) as store:
                        assert store._connection.execute('SELECT count(*) FROM job_authorization_heads').fetchone()[0]==501
                        assert store.get_materialized_job('batch-job-500').manual_hold
                        assert not store.get_materialized_job('batch-job-501').authorized
                        assert store._connection.execute("SELECT count(*) FROM target_dispatch_causes WHERE kind='body'").fetchone()[0]==0
                    assert requests==[] and process.poll() is None
        asyncio.run(exercise())


def test_public_collection_gate_contains_only_matching_stock_slot(tmp_path,monkeypatch,sdk_lifecycle):
    import hashlib
    directory=Path(tempfile.mkdtemp(prefix='collection15-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));directory.chmod(0o700)
    home=directory/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.2)
    with tempfile.TemporaryDirectory(prefix='collection-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=32*1024*1024) as origin:
            with body_worker(root,origin.url(),directory) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_configure',dict(request_id='collection-two',concurrency='2'))
                            for i,name in enumerate(('collection-a','collection-b')):
                                await call('downloads_add',dict(request_id='collection-add-'+str(i),collection=name,items=[entry(i,source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest())]))
                            await call('downloads_control',targets(['batch-job-0','batch-job-1']));await call('downloads_control',dict(scope='queue',action='resume',request_id='collection-go',expected_revision='1'))
                            end=time.monotonic()+15
                            while True:
                                with closing(SQLiteStore(root/'state.db')) as store:owners={row[0]:row[1] for row in store._job_engine_owners()}
                                if len(owners)==2 and all(value is not None for value in owners.values()) and all((output/'.incomplete'/job/f'file-{i}.bin').exists() and (output/'.incomplete'/job/f'file-{i}.bin').stat().st_blocks*512>1024*1024 for i,job in enumerate(('batch-job-0','batch-job-1'))):break
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            identities={job:record.identity for job,record in owners.items()}
                            collections=[(await call('downloads_query',dict(scope='detail',id='batch-job-'+str(i))))['collection'] for i in range(2)]
                            assert all(value['revision']=='0' and value['held'] is False for value in collections)
                            collection_ids=[value['id'] for value in collections]
                            command=dict(scope='collection',action='pause',collection=collection_ids[0],request_id='collection-pause',expected_worker_epoch='1',expected_revision='0')
                            receipt=await call('downloads_control',command)
                            end=time.monotonic()+8
                            while (await call('downloads_query',dict(scope='control',id='collection-pause')))['status']=='pending':
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                            assert processes.reconcile_process_birth(identities['batch-job-0'])=='absent'
                            assert processes.is_current_process_birth(identities['batch-job-1'])
                            with closing(SQLiteStore(root/'state.db')) as store:
                                assert store.collection_hold(collection_ids[0]) and not store.collection_hold(collection_ids[1])
                                assert not store.get_materialized_job('batch-job-0').manual_hold
                                assert store._connection.execute("SELECT stop_kind FROM stopped_control_bodies WHERE job_id='batch-job-0'").fetchone()[0]=='collection'
                            assert await call('downloads_control',command)==receipt|dict(replayed=True)
                            await call('downloads_control',dict(scope='collection',action='resume',collection=collection_ids[0],request_id='collection-resume',expected_worker_epoch='1',expected_revision='1'))
                            end=time.monotonic()+8
                            while True:
                                with closing(SQLiteStore(root/'state.db')) as store:
                                    count=store._connection.execute("SELECT count(*) FROM target_dispatch_causes WHERE kind='body' AND job_id='batch-job-0'").fetchone()[0]
                                if count==2:break
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                asyncio.run(exercise())



def test_public_explicit_retry_opens_new_budget_only_once_after_actual_exhausted_failure(tmp_path,monkeypatch,sdk_lifecycle):
    import hashlib
    from hermes_downloads import retry
    directory=Path(tempfile.mkdtemp(prefix='retry-open-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));directory.chmod(0o700)
    home=directory/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    with tempfile.TemporaryDirectory(prefix='retryopen-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=1024) as origin:
            response=origin._response_for;origin._response_for=lambda *_:(503,{},b'')
            with body_worker(root,origin.url(),directory) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='retry-open-add',items=[entry(source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest())]))
                            with closing(SQLiteStore(root/'state.db')) as store:
                                authority=retry.RetryAuthority.open(policy=retry.RetryPolicy(),job_id='batch-job-0',generation=0)
                                for _ in range(4):authority.decide(retry.RetryFailure(retry.FailureKind.TRANSIENT_HOST),generation=0)
                                store.set_retry_budget(authority.budget)
                            await call('downloads_control',targets(['batch-job-0']));await call('downloads_control',dict(scope='queue',action='resume',request_id='retry-open-go',expected_revision='1'))
                            end=time.monotonic()+8
                            while True:
                                current=(await call('downloads_query',dict(scope='status',id='batch-job-0')))['record']
                                if current['state']=='blocked':break
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            with closing(SQLiteStore(root/'state.db')) as store:
                                before=store.get_retry_budget('batch-job-0');assert before.exhausted and before.ordinary_attempts==5
                            origin._response_for=response
                            request=dict(scope='job',action='retry',job='batch-job-0',request_id='retry-open-fresh',expected_revision=str(current['revision']),expected_worker_epoch='1')
                            receipt=await call('downloads_control',request);assert receipt['status']=='pending'
                            end=time.monotonic()+10
                            while not (output/'Other/file-0.bin').exists():
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            assert (output/'Other/file-0.bin').read_bytes()==origin.payload
                            assert await call('downloads_control',request)==receipt|dict(replayed=True)
                            audit=await call('downloads_query',dict(scope='events'))
                            kinds=[event['kind'] for event in audit['events']]
                            assert kinds.count('job_retry_opened')==1 and kinds.count('job_failure')==1
                            with closing(SQLiteStore(root/'state.db')) as store:
                                after=store.get_retry_budget('batch-job-0')
                                assert (after.generation,after.budget_number,after.ordinary_attempts,after.exhausted)==(1,before.budget_number+1,0,False)
                                assert after.audit[:len(before.audit)]==before.audit
                                assert sum(event.kind is retry.RetryAuditKind.EXPLICIT_RESUME for event in after.audit)==1
                                assert store._connection.execute("SELECT count(*) FROM target_dispatch_causes WHERE kind='body'").fetchone()[0]==2
                asyncio.run(exercise())


def test_public_preemption_seals_victim_and_short_selected_job_publishes_first(tmp_path,monkeypatch,sdk_lifecycle):
    import hashlib
    directory=Path(tempfile.mkdtemp(prefix='preempt15-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));directory.chmod(0o700)
    home=directory/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.12)
    with tempfile.TemporaryDirectory(prefix='preempt-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=32*1024*1024) as long, _helpers._origin_type()(payload_size=1024) as short:
            with body_worker(root,(long.url(),short.url()),directory) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_add',dict(request_id='preempt-add',items=[entry(0,source_url=long.url(),expected_sha256=hashlib.sha256(long.payload).hexdigest()),entry(1,source_url=short.url(),expected_sha256=hashlib.sha256(short.payload).hexdigest())]))
                            await call('downloads_control',targets(['batch-job-0']));await call('downloads_control',dict(scope='queue',action='resume',request_id='preempt-go',expected_revision='1'))
                            partial=output/'.incomplete/batch-job-0/file-0.bin';end=time.monotonic()+10
                            while not partial.exists() or partial.stat().st_blocks*512<2*1024*1024:
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            with closing(SQLiteStore(root/'state.db')) as store:original=store._job_engine_owners()[0][1].identity
                            request=dict(scope='job',action='preempt',job='batch-job-1',request_id='preempt-short',expected_revision='0',expected_worker_epoch='1')
                            receipt=await call('downloads_control',request);assert receipt['status']=='pending'
                            end=time.monotonic()+10
                            while not (output/'Other/file-1.bin').exists():
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            assert (output/'Other/file-1.bin').read_bytes()==short.payload
                            assert not (output/'Other/file-0.bin').exists()
                            assert processes.reconcile_process_birth(original)=='absent'
                            end=time.monotonic()+10
                            while True:
                                with closing(SQLiteStore(root/'state.db')) as store:
                                    bodies=store._connection.execute("SELECT count(*) FROM target_dispatch_causes WHERE job_id='batch-job-0' AND kind='body'").fetchone()[0]
                                if bodies==2:break
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                            with closing(SQLiteStore(root/'state.db')) as store:
                                assert not store.get_materialized_job('batch-job-0').manual_hold
                                assert store._connection.execute("SELECT stop_kind FROM stopped_control_bodies WHERE job_id='batch-job-0'").fetchone()[0]=='preempt'
                            assert await call('downloads_control',request)==receipt|dict(replayed=True)
                asyncio.run(exercise())



def test_public_tiny_positive_global_share_waits_without_admitting_zero_share_body(tmp_path,monkeypatch,sdk_lifecycle):
    import hashlib
    directory=Path(tempfile.mkdtemp(prefix='tinyshare15-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));directory.chmod(0o700)
    home=directory/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.06)
    with tempfile.TemporaryDirectory(prefix='tinyshare-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=32*1024*1024) as origin:
            with body_worker(root,origin.url(),directory) as process:
                async def exercise():
                    async with stdio_client(parameters(root)) as (read,write):
                        async with ClientSession(read,write) as session:
                            await session.initialize()
                            async def call(name,args):return result_record(await session.call_tool(name,arguments=args))
                            await call('downloads_configure',dict(request_id='tinyshare-limit',concurrency='2',global_limit_bps='1'))
                            await call('downloads_add',dict(request_id='tinyshare-add',items=[entry(i,source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest()) for i in range(2)]))
                            await call('downloads_control',targets(['batch-job-0','batch-job-1']));await call('downloads_control',dict(scope='queue',action='resume',request_id='tinyshare-go',expected_revision='1'))
                            end=time.monotonic()+10
                            while not (output/'.incomplete/batch-job-0/file-0.bin').exists():
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            await asyncio.sleep(.5)
                            with closing(SQLiteStore(root/'state.db')) as store:
                                assert store.get_job('batch-job-1').state=='queued'
                                assert store._capture_target_head(store._connection,'batch-job-1').last_admission_serial==0
                                assert not store._connection.execute("SELECT 1 FROM target_dispatch_causes WHERE kind='body' AND job_id='batch-job-1'").fetchone()
                            await call('downloads_configure',dict(request_id='tinyshare-positive',global_limit_bps='2000000'))
                            end=time.monotonic()+8
                            while True:
                                with closing(SQLiteStore(root/'state.db')) as store:owners=store._job_engine_owners()
                                if len(owners)==2 and all(owner[1] is not None for owner in owners):break
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                            while True:
                                records=[json.loads(line) for line in (directory/'scheduler-effects.jsonl').read_text().splitlines()]
                                shares=[record for record in records if record['kind']=='actual-stock-allocation' and record['allocation_bps']==1000000]
                                if {record['job'] for record in shares}=={'batch-job-0','batch-job-1'}:break
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                            assert all(record['item_option']==record['global_option']=='1000000' and record['connections']==record['split']=='4' for record in shares)
                            original={owner[0]:owner[1].identity for owner in owners}
                            await call('downloads_configure',dict(request_id='tinyshare-connections',connections='2'))
                            end=time.monotonic()+15
                            while True:
                                records=[json.loads(line) for line in (directory/'scheduler-effects.jsonl').read_text().splitlines()]
                                changed=[record for record in records if record['kind']=='actual-stock-allocation' and record['connections']==record['split']=='2']
                                if {record['job'] for record in changed}=={'batch-job-0','batch-job-1'}:break
                                assert time.monotonic()<end and process.is_alive();await asyncio.sleep(.02)
                            assert all(processes.reconcile_process_birth(identity)=='absent' for identity in original.values())
                            with closing(SQLiteStore(root/'state.db')) as store:
                                assert store.get_retry_budget('batch-job-0') is store.get_retry_budget('batch-job-1') is None
                                assert [row[0] for row in store._connection.execute("SELECT count(*) FROM target_dispatch_causes WHERE kind='body' GROUP BY job_id ORDER BY job_id")]==[2,2]
                            await call('downloads_configure',dict(request_id='tinyshare-unlimited',global_limit_bps=None))
                            end=time.monotonic()+6
                            while True:
                                records=[json.loads(line) for line in (directory/'scheduler-effects.jsonl').read_text().splitlines()]
                                unlimited=[record for record in records if record['kind']=='actual-stock-allocation' and record['allocation_bps'] is None]
                                if {record['job'] for record in unlimited}=={'batch-job-0','batch-job-1'}:break
                                assert time.monotonic()<end;await asyncio.sleep(.02)
                            assert all(record['item_option']==record['global_option']=='0' for record in unlimited)
                asyncio.run(exercise())


@pytest.mark.parametrize('cut',['cut-saved-purge','cut-saved-purge-marker'])
def test_actual_cold_selected_purge_uses_fresh_consent_and_prior_unlink_authority(tmp_path,monkeypatch,sdk_lifecycle,cut):
    import hashlib,signal
    from test_direct_publication import _append_record
    directory=Path(tempfile.mkdtemp(prefix=cut+'-',dir=os.environ.get('T26_RESUME_RUN',str(tmp_path))));directory.chmod(0o700)
    home=directory/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home));output=home/'Downloads/Hermes';output.mkdir(parents=True,mode=0o700)
    monkeypatch.setattr(_helpers._FIXTURE_MODULE,'_RANGE_PACED_CHUNK_DELAY_SECONDS',.1)
    context=multiprocessing.get_context('spawn');entered,release=context.Event(),context.Event()
    async def call(root,name,args):
        async with stdio_client(parameters(root)) as (read,write):
            async with ClientSession(read,write) as session:
                await session.initialize();return result_record(await session.call_tool(name,arguments=args))
    def invoke(root,name,args):return asyncio.run(call(root,name,args))
    def wait(predicate,seconds=12):
        end=time.monotonic()+seconds
        while not predicate():assert time.monotonic()<end;time.sleep(.02)
    with tempfile.TemporaryDirectory(prefix='purgecut-',dir='/private/tmp') as temporary:
        root=Path(temporary);root.chmod(0o700)
        with _helpers._origin_type()(payload_size=32*1024*1024) as origin:
            with body_worker(root,origin.url(),directory,barrier=cut,entered=entered,release=release,crash=True,recover_socket=True) as original:
                invoke(root,'downloads_add',dict(request_id='purgecut-add',items=[entry(source_url=origin.url(),expected_sha256=hashlib.sha256(origin.payload).hexdigest())]))
                invoke(root,'downloads_control',targets(['batch-job-0']))
                invoke(root,'downloads_control',dict(scope='queue',action='resume',request_id='purgecut-go',expected_revision='1'))
                partial=output/'.incomplete/batch-job-0/file-0.bin';wait(lambda:partial.exists() and partial.stat().st_blocks*512>2*1024*1024)
                sentinel=partial.parent/'unknown.sidecar';sentinel.write_bytes(b'preserved')
                request=dict(action='purge',ids=['batch-job-0'],request_id='purgecut-old',confirmed=True)
                receipt=invoke(root,'downloads_files',request);assert entered.wait(12)
                with closing(SQLiteStore(root/'state.db')) as store:
                    original_checkpoint=tuple(store._connection.execute('SELECT * FROM stopped_control_bodies').fetchone())
                    preauthorized=[tuple(row) for row in store._connection.execute('SELECT * FROM direct_saved_purge')]
                    creation=store.get_batch_creation_intent('batch-job-0')
                records=[json.loads(line) for line in (directory/f'fixture-{original.pid}.jsonl').read_text().splitlines()]
                identity=processes.ProcessBirthIdentity.from_record(next(row['identity'] for row in records if row['kind']=='worker-birth'))
                os.killpg(identity.process_group_id,signal.SIGKILL);original.join(8);assert original.exitcode==-signal.SIGKILL
                with pytest.raises(ChildProcessError):os.waitpid(original.pid,os.WNOHANG)
                _append_record(directory/'original-purge-wait.jsonl','original-worker-wait',identity=identity.to_record(),exitcode=original.exitcode,waitpid='ECHILD')
            before=origin.ledger.response_body_bytes;names=sorted(path.name for path in partial.parent.iterdir())
            with body_worker(root,origin.url(),directory,recover_socket=True) as cold:
                invoke(root,'downloads_query',dict(scope='list'))
                assert (invoke(root,'downloads_query',dict(scope='queue')))['queue_gate']=='paused'
                assert invoke(root,'downloads_files',request)==receipt|dict(replayed=True)
                assert sorted(path.name for path in partial.parent.iterdir())==names and origin.ledger.response_body_bytes==before
                fresh=request|dict(request_id='purgecut-fresh');accepted=invoke(root,'downloads_files',fresh)
                wait(lambda:(invoke(root,'downloads_query',dict(scope='detail',id='batch-job-0')))['files']['status']=='purged')
                assert list(partial.parent.iterdir())==[sentinel] and sentinel.read_bytes()==b'preserved'
                assert not (output/'Other/file-0.bin').exists() and origin.ledger.response_body_bytes==before
                assert invoke(root,'downloads_files',fresh)==accepted|dict(replayed=True)
                with closing(SQLiteStore(root/'state.db')) as store:
                    assert tuple(store._connection.execute('SELECT * FROM stopped_control_bodies').fetchone())==original_checkpoint
                    assert [tuple(row) for row in store._connection.execute('SELECT * FROM direct_saved_purge WHERE request_id=?',(request['request_id'],))]==preauthorized
                    assert store.get_batch_creation_intent('batch-job-0')==creation
                (directory/'purge-cut-result.json').write_text(json.dumps(dict(cut=cut,original_wait=True,fresh_consent=True,prior_unlink_authority=True,body_inert=True,unknown_preserved=True,synthetic=True)))
