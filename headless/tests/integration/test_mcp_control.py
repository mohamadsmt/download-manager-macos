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
        assert names == ['downloads_add', 'downloads_control', 'downloads_query']
        schema = tools.tools[1].inputSchema
        assert schema['type']=='object' and schema['additionalProperties'] is False
        assert set(schema['required']) == {'scope','action','request_id'}
        assert set(schema['required']) <= set(schema['properties'])
        assert len(schema['oneOf']) == 3
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
def body_worker(root, origin_url, ledger):
    context = multiprocessing.get_context('spawn')
    ready, shutdown, stopped = [context.Event() for _ in range(3)]
    results = context.Queue()
    process = context.Process(target=_scheduler_worker,
        args=(str(root), ready, shutdown, stopped, results, origin_url, str(ledger)))
    process.start()
    try:
        assert ready.wait(6), 'private scheduler did not become ready'
        yield process
    finally:
        shutdown.set()
        try:
            _close_fixture_process(process, ledger)
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
                                engine = store.get_direct_engine_record()
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
