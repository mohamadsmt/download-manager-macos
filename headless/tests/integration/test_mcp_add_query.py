"""Installed official SDK and newly owned private worker add/query evidence."""
import asyncio
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

import pytest
from mcp import ClientSession
import mcp.client.stdio as sdk_stdio
from mcp.client.stdio import StdioServerParameters, stdio_client
from hermes_downloads import ipc
from test_add_batch import entry, worst_entries, exchange, canonical, envelope


def evidence(kind, **record):
    directory = os.environ.get('T18_IMPLEMENTATION_RUN')
    if directory:
        with (Path(directory) / 'mcp-fixtures.jsonl').open('a') as stream:
            stream.write(json.dumps(dict(kind=kind, **record), sort_keys=True) + '\n')


def birth(pid):
    value = subprocess.check_output(['/bin/ps', '-p', str(pid), '-o',
        'pid=,ppid=,pgid=,lstart=,command='], text=True).strip()
    assert value and os.getpgid(pid) == pid
    return value


def group_absent(pid):
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return True
    return False


def snapshot(root):
    with sqlite3.connect(f'file:{root / "state.db"}?mode=ro', uri=True) as connection:
        tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
        return [(name, connection.execute('SELECT * FROM "' + name + '"').fetchall()) for (name,) in tables]


@contextmanager
def owned_worker(root):
    output = root / "output"
    effects = root / 'effects.jsonl'
    code = '''import builtins,hashlib,json,os,sys
from pathlib import Path
root=Path(sys.argv[1]); original=builtins.__import__
def denied(kind):
 with (root/'effects.jsonl').open('a') as stream: stream.write(json.dumps(kind)+'\\n')
 raise AssertionError(kind)
def guarded(name,*args,**kwargs):
 if name in {'hermes_downloads.direct','hermes_downloads.video'}: denied('engine')
 return original(name,*args,**kwargs)
builtins.__import__=guarded
hashlib.file_digest=lambda *a,**k: denied('body_hash')
os.link=lambda *a,**k: denied('link')
original_unlink=os.unlink
def unlink(path,*args,**kwargs):
 if Path(path).is_absolute() and (root/'output') in Path(path).parents: denied('unlink')
 return original_unlink(path,*args,**kwargs)
os.unlink=unlink
from hermes_downloads.service import worker_main
raise SystemExit(worker_main(argv=['--serve','--state-root',str(root)]))
'''
    environment = dict(os.environ, HERMES_DOWNLOADS_OUTPUT_ROOT=str(output),
        PYTHONDONTWRITEBYTECODE='1', PYTHONNOUSERSITE='1')
    environment.pop('PYTHONPATH', None); environment.pop('PYTHONHOME', None)
    process = subprocess.Popen([sys.executable, '-I', '-B', '-c', code, str(root)],
        cwd=root, env=environment, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    owned_birth = None
    try:
        owned_birth = birth(process.pid)
        evidence('worker-birth', pid=process.pid, pgid=process.pid, birth=owned_birth)
        deadline = time.monotonic() + 4
        while True:
            assert process.poll() is None, process.communicate()[1]
            try:
                health = ipc.request_health(root / 'worker.sock'); break
            except ipc.IPCError:
                assert time.monotonic() < deadline
                threading.Event().wait(0.02)
        assert health.queue_gate == 'paused'
        yield process
    finally:
        if process.poll() is None:
            assert os.getpgid(process.pid) == process.pid
            os.killpg(process.pid, signal.SIGTERM)
        try:
            code = process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL); code = process.wait(timeout=3)
        out, err = process.communicate()
        absent = group_absent(process.pid)
        evidence('worker-closure', pid=process.pid, birth=owned_birth,
            wait_returncode=code, reaped=True, group_absent=absent,
            stderr=err.decode(errors='replace'))
        assert absent
        assert not effects.exists(), effects.read_text() if effects.exists() else ''


@pytest.fixture
def private_worker():
    with tempfile.TemporaryDirectory(prefix="mcp7-", dir="/private/tmp") as temporary:
        root = Path(temporary); root.chmod(0o700)
        output = root / "output"; output.mkdir(mode=0o700)
        (output / "sentinel").write_bytes(b"untouched")
        with owned_worker(root) as process:
            yield root, process

@pytest.fixture
def sdk_lifecycle(monkeypatch):
    original = sdk_stdio._create_platform_compatible_process
    processes = []
    async def create(**kwargs):
        process = await original(**kwargs)
        waits = []
        original_wait = process.wait
        async def wait():
            code = await original_wait()
            waits.append(code)
            evidence('sdk-original-wait', pid=process.pid, wait_returncode=code)
            return code
        process.wait = wait
        index = len(processes)
        processes.append((process, None, waits))
        try:
            owned_birth = birth(process.pid)
        except BaseException:
            if process.returncode is None:
                assert os.getpgid(process.pid) == process.pid
                os.killpg(process.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except asyncio.TimeoutError:
                process.kill()
                await asyncio.wait_for(process.wait(), timeout=3)
            await process.aclose()
            raise
        processes[index] = (process, owned_birth, waits)
        evidence('sdk-birth', pid=process.pid, birth=owned_birth)
        return process
    monkeypatch.setattr(sdk_stdio, '_create_platform_compatible_process', create)
    yield processes
    for process, owned_birth, waits in processes:
        assert process.returncode is not None
        assert waits and all(code == (0 if owned_birth is not None else process.returncode) for code in waits)
        absent = group_absent(process.pid)
        evidence('sdk-closure', pid=process.pid, birth=owned_birth,
            wait_returncode=process.returncode, original_sdk_wait=True, original_waits=waits,
            group_absent=absent)
        assert absent


def test_owned_worker_birth_failure_reaps_original_child(tmp_path, monkeypatch):
    children = []; waits = []
    original = subprocess.Popen
    def capture(*args, **kwargs):
        process = original(*args, **kwargs)
        children.append(process)
        original_wait = process.wait
        def wait(*args, **kwargs):
            code = original_wait(*args, **kwargs); waits.append(code)
            return code
        process.wait = wait
        return process
    def failed_birth(pid):
        assert pid == children[0].pid
        raise OSError('injected birth validation failure')
    monkeypatch.setattr(subprocess, 'Popen', capture)
    monkeypatch.setattr(sys.modules[__name__], 'birth', failed_birth)
    try:
        with pytest.raises(OSError, match='injected birth validation failure'):
            with owned_worker(tmp_path):
                pytest.fail('birth failure must prevent fixture admission')
        process, = children
        closed = bool(waits) and process.returncode is not None and group_absent(process.pid)
        evidence('worker-birth-failure-regression', pid=process.pid, observed_birth=None,
            original_waits=list(waits), reaped=process.returncode is not None,
            group_absent=group_absent(process.pid), fixture_closed=closed)
        assert closed, 'worker escaped fixture cleanup after birth validation failed'
    finally:
        for process in children:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=3); process.communicate()
            evidence('worker-birth-failure-bounded-cleanup', pid=process.pid,
                original_waits=list(waits), reaped=True, group_absent=group_absent(process.pid))
            assert group_absent(process.pid)


def test_sdk_birth_failure_reaps_original_child(tmp_path, monkeypatch):
    children = []; waits = []
    original = sdk_stdio._create_platform_compatible_process
    async def capture(**kwargs):
        process = await original(**kwargs)
        children.append(process)
        original_wait = process.wait
        async def wait():
            code = await original_wait(); waits.append(code)
            return code
        process.wait = wait
        return process
    def failed_birth(pid):
        assert pid == children[0].pid
        raise OSError('injected birth validation failure')
    monkeypatch.setattr(sdk_stdio, '_create_platform_compatible_process', capture)
    monkeypatch.setattr(sys.modules[__name__], 'birth', failed_birth)
    lifecycle = sdk_lifecycle.__wrapped__(monkeypatch)
    next(lifecycle)
    async def exercise():
        try:
            with pytest.raises(OSError, match='injected birth validation failure'):
                async with stdio_client(parameters(tmp_path)):
                    pytest.fail('birth failure must prevent fixture admission')
            process, = children
            closed = bool(waits) and process.returncode is not None and group_absent(process.pid)
            evidence('sdk-birth-failure-regression', pid=process.pid, observed_birth=None,
                original_waits=list(waits), reaped=process.returncode is not None,
                group_absent=group_absent(process.pid), fixture_closed=closed)
            assert closed, 'SDK child escaped fixture cleanup after birth validation failed'
        finally:
            for process in children:
                if process.returncode is None:
                    process.terminate()
                await asyncio.wait_for(process.wait(), timeout=3)
                await process.aclose()
                evidence('sdk-birth-failure-bounded-cleanup', pid=process.pid,
                    original_waits=list(waits), reaped=True, group_absent=group_absent(process.pid))
                assert group_absent(process.pid)
    try:
        asyncio.run(exercise())
    finally:
        with pytest.raises(StopIteration):
            next(lifecycle)


@contextmanager
def origin():
    counts = []
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            counts.append('GET'); self.send_response(500); self.end_headers()
        do_HEAD = do_GET
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield counts
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=5)
        evidence('origin-observer-join', joined=not thread.is_alive(), requests=counts)
        assert not thread.is_alive() and counts == []


def parameters(root):
    return StdioServerParameters(command=str(Path(sys.executable).parent / 'hermes-downloads-mcp'),
        cwd=root, env=dict(HERMES_DOWNLOADS_SOCKET=str(root / 'worker.sock'),
            PYTHONDONTWRITEBYTECODE='1', PYTHONNOUSERSITE='1'))


async def session_calls(root, calls=()):
    async with stdio_client(parameters(root)) as (read, write):
        async with ClientSession(read, write, sampling_capabilities=None) as session:
            initialized = await session.initialize()
            tools = await session.list_tools()
            results = [await session.call_tool(name, arguments=args) for name, args in calls]
    return initialized, tools, results


def test_root_bound_real_discovery_requires_downloads_add(private_worker, sdk_lifecycle):
    root, process = private_worker
    with origin() as requests:
        before = snapshot(root)
        initialized, tools, results = asyncio.run(session_calls(root,
            [('downloads_query', {'scope': 'health'})]))
        names = [tool.name for tool in tools.tools]
        assert results[0].isError is False
        assert results[0].structuredContent == {'worker_epoch': 1, 'queue_gate': 'paused'}
        assert process.poll() is None
        observed = []
        observer = threading.Thread(target=lambda: observed.append(snapshot(root)))
        observer.start(); observer.join(timeout=5)
        assert not observer.is_alive() and observed == [before]
        evidence('readonly-observer-join', joined=True, health=results[0].structuredContent,
            tools=names, no_sql_effects=observed == [before], origin_requests=requests,
            output_unchanged=(root / 'output' / 'sentinel').read_bytes() == b'untouched')
        assert requests == [] and snapshot(root) == before
        assert list((root / 'output').iterdir()) == [root / 'output' / 'sentinel']
        assert not (root / 'effects.jsonl').exists()
        assert 'downloads_add' in names, 'ROOT_BOUND_DOWNLOADS_ADD_ABSENT: ' + repr(names)


def result_record(result):
    assert result.isError is False, result
    assert json.loads(result.content[0].text) == result.structuredContent
    return result.structuredContent


def query(root, scope, **arguments):
    return result_record(asyncio.run(session_calls(root,
        [('downloads_query', dict(scope=scope, **arguments))]))[2][0])


def add(root, items, request_id='sdk-parent', **arguments):
    return asyncio.run(session_calls(root,
        [('downloads_add', dict(items=items, request_id=request_id, **arguments))]))[2][0]


def assert_inert_output(root, process):
    assert process.poll() is None
    assert ipc.request_health(root / 'worker.sock').queue_gate == 'paused'
    assert not (root / 'effects.jsonl').exists()
    assert list((root / 'output').iterdir()) == [root / 'output' / 'sentinel']
    assert (root / 'output' / 'sentinel').read_bytes() == b'untouched'


def test_real_sdk_500_escaped_add_replay_conflicts_and_no_transfer(private_worker, sdk_lifecycle):
    root, process = private_worker
    values = worst_entries()
    with origin() as requests:
        created = result_record(add(root, values))
        assert created['protocol_version'] == 2 and created['readback_kind'] == 'creation_receipt'
        assert created['replayed'] is False
        assert [row['job'] for row in created['results']] == [item['job'] for item in values]
        assert [row['order_key'] for row in created['results']] == [str(index) for index in range(500)]
        assert all(row['generation'] == row['revision'] == 0 for row in created['results'])
        replay = result_record(add(root, values))
        assert replay == dict(created, replayed=True)
        before = snapshot(root)
        tail = [dict(item) for item in values]; tail[-1]['source_url'] += 'changed'
        hashes = [dict(item) for item in values]; hashes[-1]['expected_sha256'] = 'f'*64
        for changed in [tail, hashes, list(reversed(values))]:
            rejected = add(root, changed)
            assert rejected.isError and rejected.structuredContent is None
        assert snapshot(root) == before
        with sqlite3.connect(f'file:{root / "state.db"}?mode=ro', uri=True) as connection:
            assert connection.execute('SELECT COUNT(*) FROM jobs').fetchone()[0] == 500
            assert connection.execute('SELECT COUNT(*) FROM events').fetchone()[0] == 500
            blobs = connection.execute('SELECT creation_intent_blob FROM add_batch_entries ORDER BY entry_index').fetchall()
            assert [json.loads(row[0])['entry']['expected_sha256'] for row in blobs] == [item['expected_sha256'] for item in values]
        page = query(root, 'list'); assert len(page['jobs']) == 100 and page['has_more'] is True
        assert query(root, 'status', id=values[0]['job'])['record']['state'] == 'queued'
        assert len(query(root, 'events')['events']) == 100
        assert snapshot(root) == before
        assert requests == []
        assert_inert_output(root, process)
        evidence('sdk-500-add-only', ordered_results=500, hashes=500, replay=True,
            changed_tail_order_hash_refused=True, no_query_writes=True, no_transfer=True)


def test_real_sdk_ordered_semantic_refusals_lost_response_and_large_order(private_worker, sdk_lifecycle):
    root, process = private_worker
    seed = result_record(add(root, [entry(99)], request_id='seed'))
    with sqlite3.connect(root / 'state.db') as connection:
        connection.execute('UPDATE materialized_jobs SET order_key=? WHERE job_id=?', (9007199254740991, seed['results'][0]['job']))
    created = result_record(add(root, [entry(0)]))
    assert created['results'][0]['order_key'] == '9007199254740992'
    assert result_record(add(root, [entry(0)])) == dict(created, replayed=True)
    refused = [entry(1, job='../bad'), entry(2, source_kind='video'), entry(3, source_url='file:///private'),
        entry(4, category='invalid'), entry(5, partial_filename='../bad'), entry(6, expected_sha256='A'*64), entry(7)]
    receipt = result_record(add(root, refused, request_id='semantics'))
    assert [row['index'] for row in receipt['results']] == list(range(7))
    assert [row['status'] for row in receipt['results']] == ['blocked']*6 + ['applied']
    # Lost HDM2 response is reconciled by the same immutable command through real MCP.
    command = ipc.AddBatchCommand('lost', None, [entry(8)])
    import socket
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(str(root / 'worker.sock'))
        client.sendall(b'HDM2\n' + len(command._wire_request).to_bytes(4, 'big') + command._wire_request)
        client.shutdown(socket.SHUT_WR)
    ipc.request_health(root / 'worker.sock')
    recovered = result_record(add(root, [entry(8)], request_id='lost'))
    assert recovered['replayed'] is True and recovered['results'][0]['generation'] == 0
    collision = add(root, [entry(10)], request_id=created['results'][0]['child_request_id'])
    assert collision.isError
    with sqlite3.connect(f'file:{root / "state.db"}?mode=ro', uri=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM events WHERE job_id='batch-job-0'").fetchone()[0] == 1
    assert_inert_output(root, process)


@pytest.mark.parametrize('change,code', [
    ({'start': True}, 'downloads_add_start_unsupported'),
    ({'start': 0}, 'downloads_add_invalid_input'),
    ({'extra': 'secret'}, 'downloads_add_invalid_input'),
    ({'items': [entry(priority=True)]}, 'downloads_add_invalid_input'),
    ({'items': [dict(entry(), extra='secret')]}, 'downloads_add_invalid_input'),
    ({'items': []}, 'downloads_add_invalid_input'),
    ({'items': [entry()] * 501}, 'downloads_add_invalid_input'),
    ({'items': [entry(source_url='¡'*10000)]}, 'downloads_add_invalid_input'),
])
def test_sdk_add_shape_and_start_refuse_before_endpoint(tmp_path, sdk_lifecycle, change, code):
    root = tmp_path / 'absent'; root.mkdir()
    arguments = dict(items=[entry()], request_id='parent'); arguments.update(change)
    result = asyncio.run(session_calls(root, [('downloads_add', arguments)]))[2][0]
    assert result.isError and result.content[0].text == code
    assert list(root.iterdir()) == []


@pytest.mark.parametrize('count', [0, 99, 100, 101, 200, 201])
def test_sdk_exact_page_endings_status_audit_and_no_writes(private_worker, sdk_lifecycle, count):
    root, process = private_worker
    if count:
        result_record(add(root, [entry(index, job=f'J{index:03d}') for index in range(count)]))
    before = snapshot(root)
    async def traverse():
        async with stdio_client(parameters(root)) as (read, write):
            async with ClientSession(read, write, sampling_capabilities=None) as session:
                initialized = await session.initialize(); tools = await session.list_tools()
                assert sorted(tool.name for tool in tools.tools) == ['downloads_add', 'downloads_control', 'downloads_query']
                assert initialized.capabilities.model_dump(by_alias=True, exclude_none=True) == {'experimental': {}, 'tools': {'listChanged': False}}
                for scope, field in [('list', 'jobs'), ('events', 'events')]:
                    cursor = None; seen = []
                    while True:
                        page = result_record(await session.call_tool('downloads_query', arguments=dict(scope=scope, cursor=cursor)))
                        seen += page[field]
                        assert page['has_more'] is (len(seen) < count)
                        assert (page['next_cursor'] is not None) is page['has_more']
                        if not page['has_more']: break
                        cursor = page['next_cursor']
                    assert len(seen) == count
                assert result_record(await session.call_tool('downloads_query', arguments={'scope': 'status', 'id': 'unknown'})) == {'job': 'unknown', 'record': None}
    asyncio.run(traverse())
    assert snapshot(root) == before
    assert_inert_output(root, process)


def test_sdk_status_removed_legacy_and_poison_redaction(private_worker, sdk_lifecycle):
    root, process = private_worker
    result_record(add(root, [entry(0), entry(1)]))
    with sqlite3.connect(root / 'state.db') as connection:
        connection.execute("UPDATE jobs SET state='removed' WHERE job_id='batch-job-0'")
        connection.execute("UPDATE materialized_jobs SET source_kind='video' WHERE job_id='batch-job-1'")
    assert query(root, 'status', id='batch-job-0')['record']['state'] == 'removed'
    assert query(root, 'status', id='batch-job-1')['record']['job'] == 'batch-job-1'
    with sqlite3.connect(root / 'state.db') as connection:
        connection.execute("UPDATE events SET kind='https://raw-secret.invalid/?credential=hidden' WHERE event_id=1")
    result = asyncio.run(session_calls(root, [('downloads_query', {'scope': 'events'})]))[2][0]
    assert result.isError and result.content[0].text == 'downloads_query_unavailable'
    assert result.structuredContent is None
    assert 'hidden' not in str(result)
    assert query(root, 'health')['queue_gate'] == 'paused'
    with sqlite3.connect(root / 'state.db') as connection:
        connection.execute("UPDATE jobs SET revision=9007199254740992 WHERE job_id='batch-job-0'")
    result = asyncio.run(session_calls(root, [('downloads_query', {'scope': 'status', 'id': 'batch-job-0'})]))[2][0]
    assert result.isError and result.content[0].text == 'downloads_query_unavailable'
    assert_inert_output(root, process)


def test_real_sdk_cold_replay_preserves_creation_receipt_and_rejects_cursor(tmp_path, sdk_lifecycle):
    root = Path(tempfile.mkdtemp(prefix='mcp7-cold-', dir='/private/tmp')); root.chmod(0o700)
    try:
        (root / 'output').mkdir(mode=0o700); (root / 'output' / 'sentinel').write_bytes(b'untouched')
        values = [entry(index) for index in range(101)]
        with owned_worker(root) as process:
            created = result_record(add(root, values))
            cursor = query(root, 'list')['next_cursor']
            assert_inert_output(root, process)
        with owned_worker(root) as process:
            assert query(root, 'health')['worker_epoch'] == 2
            replay = result_record(add(root, values))
            assert replay == dict(created, replayed=True)
            result = asyncio.run(session_calls(root, [('downloads_query', {'scope': 'list', 'cursor': cursor})]))[2][0]
            assert result.isError and result.content[0].text == 'downloads_query_unavailable'
            assert_inert_output(root, process)
    finally:
        import shutil
        shutil.rmtree(root)


def test_sdk_closed_query_branches_cross_view_and_no_sqlite_in_adapter(private_worker, sdk_lifecycle):
    root, process = private_worker
    result_record(add(root, [entry(index) for index in range(101)]))
    list_cursor = query(root, 'list')['next_cursor']
    before = snapshot(root)
    invalid = [dict(scope='status'), dict(scope='status', id='batch-job-0', cursor=None),
        dict(scope='health', id='secret'), dict(scope='events', cursor='legacy'),
        dict(scope='events', cursor=list_cursor), dict(scope='list', cursor=list_cursor+'='),
        dict(scope='list', cursor='¡'*1025)]
    results = asyncio.run(session_calls(root, [('downloads_query', value) for value in invalid]))[2]
    assert all(result.isError and result.content[0].text == 'downloads_query_invalid_input' for result in results)
    code = '''import builtins,sqlite3
original=builtins.__import__
def guarded(name,*args,**kwargs):
 if name in {'hermes_downloads.store','hermes_downloads.worker','hermes_downloads.direct'}: raise AssertionError('adapter_owner_import')
 return original(name,*args,**kwargs)
def denied(*args,**kwargs): raise AssertionError('adapter_sqlite_open')
builtins.__import__=guarded; sqlite3.connect=denied
from hermes_downloads.mcp_server import main
raise SystemExit(main())
'''
    async def guarded():
        params = parameters(root)
        params.command = sys.executable; params.args = ['-I', '-B', '-c', code]
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write, sampling_capabilities=None) as session:
                await session.initialize(); await session.list_tools()
                for arguments in [dict(scope='health'), dict(scope='list'), dict(scope='status', id='unknown'), dict(scope='events')]:
                    result_record(await session.call_tool('downloads_query', arguments=arguments))
    asyncio.run(guarded())
    assert snapshot(root) == before
    assert_inert_output(root, process)
