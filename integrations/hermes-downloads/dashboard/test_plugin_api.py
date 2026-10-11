"""Run on a private physical copy with the complete current official constants."""
import asyncio
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time

import httpx
import pytest
from fastapi import FastAPI


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    assert Path(module.__file__).resolve() == path.resolve() == Path(module.__spec__.origin).resolve()
    return module


HERE = Path(__file__).resolve().parent
assert 'hermes_constants' not in sys.modules, 'stale constants must not supply guard authority'
constants = load('hermes_constants', HERE / 'hermes_constants.py')
api = load('candidate_download_notification_api', HERE / 'plugin_api.py')
PREFIX = '/api/plugins/hermes-downloads'
CONSUMER = '12345678-1234-1234-1234-123456789abc'
EVENT_ID = CONSUMER + ':1'


@pytest.fixture
def home(monkeypatch):
    with tempfile.TemporaryDirectory(prefix='n', dir='/private/tmp') as name:
        root = Path(name); root.chmod(0o700)
        default = root / '.hermes'; default.mkdir(mode=0o700)
        monkeypatch.setenv('HOME', str(root)); monkeypatch.setenv('HERMES_HOME', str(default))
        pin = constants._PINNED_PROCESS_HERMES_HOME
        constants.pin_process_hermes_home(None)
        token = constants.set_hermes_home_override(None)
        try: yield root
        finally:
            constants.reset_hermes_home_override(token)
            constants.pin_process_hermes_home(pin)


async def request(method='GET', path='/events?view=pending&limit=1', **kwargs):
    app = FastAPI(); app.include_router(api.router, prefix=PREFIX)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://fixture') as client:
        return await client.request(method, PREFIX + path, **kwargs)


def empty():
    return dict(schema_version=1, profile='default', source='local', events=[], next_cursor=None, has_more=False)


def evidence(record):
    if name := os.environ.get('T19_BACKEND_EVIDENCE'):
        with open(name, 'a') as stream:
            stream.write(json.dumps(record) + '\n')


def test_current_complete_official_source_and_first_real_router_probe(home, monkeypatch):
    official = Path(os.environ['T19_OFFICIAL_CONSTANTS'])
    copy = HERE / 'hermes_constants.py'
    assert official.read_bytes() == copy.read_bytes()
    assert not copy.is_symlink() and (official.stat().st_dev, official.stat().st_ino) != (copy.stat().st_dev, copy.stat().st_ino)
    assert constants.get_routing_process_hermes_home() == constants.get_hermes_home() == home / '.hermes'
    assert {(r.path, tuple(sorted(r.methods))) for r in api.router.routes} == {
        ('/events', ('GET',)), ('/events/claim', ('POST',)), ('/events/presented', ('POST',)), ('/events/release', ('POST',)), ('/files/reveals',('GET',)), ('/files/reveals/offer',('POST',)), ('/files/reveals/presented',('POST',)), ('/files/reveals/click',('POST',)), ('/files/reveals/settle',('POST',))}
    seen = []
    def exchange(endpoint, op, body):
        seen.append((endpoint, op, body)); return empty()
    monkeypatch.setattr(api, '_exchange', exchange)
    response = asyncio.run(request(path='/events?view=pending&limit=1&profile=default'))
    assert response.status_code == 200 and response.json() == empty()
    assert seen == [(home / 'Library/Application Support/HermesDownloadManager/default/worker.sock',
        'notification_pending', {'view':'pending','limit':1})]
    evidence(dict(kind='canonical-complete-official-loader', file=constants.__file__,
        spec_origin=constants.__spec__.origin, resolved=str(copy.resolve()),
        sha256=hashlib.sha256(copy.read_bytes()).hexdigest(), official=str(official),
        distinct_inode=True, actual_router_probe_passed=True))


@pytest.mark.parametrize('mode', ['process','request','both','custom','pinned-mirror','missing-api','suffix'])
def test_actual_home_guards_refuse_before_ipc(home, monkeypatch, mode):
    other = home / '.hermes/profiles/other'
    called = []
    monkeypatch.setattr(api, '_exchange', lambda *args: called.append(args))
    token = None
    if mode in {'process','both','custom'}:
        monkeypatch.setenv('HERMES_HOME', str(other if mode != 'custom' else home / 'custom'))
    if mode in {'request','both'}:
        token = constants.set_hermes_home_override(str(other))
    if mode == 'pinned-mirror':
        constants.pin_process_hermes_home(other)
        assert constants.get_hermes_home() == home / '.hermes'
    if mode == 'missing-api': monkeypatch.delattr(constants, 'get_routing_process_hermes_home')
    if mode == 'suffix':
        monkeypatch.delenv('HERMES_HOME'); monkeypatch.setenv('HERMES_DATA_DIR_SUFFIX', '-other')
    try:
        response = asyncio.run(request())
        assert response.status_code == 403 and response.json() == api._ERROR and called == []
    finally:
        if token is not None: constants.reset_hermes_home_override(token)


def test_actual_contextvar_concurrent_isolation_and_restore(home, monkeypatch):
    called = []
    monkeypatch.setattr(api, '_exchange', lambda *args: (called.append(args), empty())[1])
    async def scoped(path):
        token = constants.set_hermes_home_override(str(path))
        try:
            await asyncio.sleep(0)
            response = await request()
            assert constants.get_hermes_home() == path
            return response.status_code
        finally: constants.reset_hermes_home_override(token)
    async def both():
        return await asyncio.gather(scoped(home / '.hermes'), scoped(home / '.hermes/profiles/other'))
    assert asyncio.run(both()) == [200,403]
    assert len(called) == 1 and constants.get_hermes_home() == home / '.hermes'


@pytest.mark.parametrize('method,path,body', [
    ('GET','/events?view=pending&limit=2',None),
    ('GET','/events?view=pending&limit=1&profile=other',None),
    ('GET','/events?view=pending&limit=1&limit=1',None),
    ('POST','/events/claim',{'consumer_id':CONSUMER,'limit':True}),
    ('POST','/events/claim',{'consumer_id':CONSUMER.upper(),'limit':1}),
    ('POST','/events/claim',{'consumer_id':CONSUMER,'limit':1,'socket':'private'}),
    ('POST','/events/release',{'consumer_id':CONSUMER,'event_id':EVENT_ID,'claim_token':'a'*64,'notification_id':'extra'}),
    ('POST','/events/presented',{'consumer_id':CONSUMER,'event_id':EVENT_ID,'claim_token':'a'*64,'notification_id':'\nsecret'}),
    ('POST','/events/release',{'consumer_id':CONSUMER,'event_id':CONSUMER+':9007199254740992','claim_token':'a'*64}),
])
def test_closed_requests_refuse_zero_ipc(home, monkeypatch, method, path, body):
    called = []; monkeypatch.setattr(api, '_exchange', lambda *args: called.append(args))
    response = asyncio.run(request(method, path, **({} if body is None else {'json':body})))
    assert response.status_code == 400 and response.json() == api._ERROR and called == []


@contextmanager
def owner(home, response):
    endpoint = home / 'Library/Application Support/HermesDownloadManager/default/worker.sock'
    endpoint.parent.mkdir(parents=True, mode=0o700)
    captured = []; errors = []; stop = threading.Event()
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(endpoint)); endpoint.chmod(0o600); listener.listen(1); listener.settimeout(.2)
    def serve():
        try:
            while not stop.is_set():
                try: connection, _ = listener.accept()
                except TimeoutError: continue
                with connection:
                    connection.settimeout(6); raw = bytearray()
                    while chunk := connection.recv(4096):
                        raw.extend(chunk); assert len(raw) <= 4096
                    captured.append(bytes(raw))
                    if callable(response): response(connection, stop)
                    else: connection.sendall(response)
                break
        except BaseException as error: errors.append(error)
    thread = threading.Thread(target=serve, name='owned-notification-socket')
    thread.start()
    evidence(dict(kind='socket-thread-start', owner_pid=os.getpid(), ident=thread.ident,
        native_id=thread.native_id, endpoint=str(endpoint)))
    try: yield captured
    finally:
        stop.set(); thread.join(7); listener.close(); endpoint.unlink()
        evidence(dict(kind='socket-thread-original-join', owner_pid=os.getpid(), ident=thread.ident,
            native_id=thread.native_id, alive=thread.is_alive(), listener_closed=True, endpoint_removed=not endpoint.exists()))
        assert not thread.is_alive() and not errors


def test_actual_socket_framing_default_profile_and_cleanup(home):
    raw = json.dumps(empty()).encode() + b'\n'
    def fragmented(connection, stop):
        for part in (raw[:4],raw[4:]): connection.sendall(part)
    with owner(home, fragmented) as captured:
        response = asyncio.run(request())
        assert response.status_code == 200 and response.json() == empty()
    assert json.loads(captured[0]) == dict(op='notification_pending', protocol_version=1, view='pending', limit=1)


@pytest.mark.parametrize('raw', [b'{}', b'{}\n{}\n', b'x'*36865, b'{"error":"private secret"}\n',
    b'{"status":"released","status":"released"}\n', b'\xff\n'])
def test_actual_socket_bad_replies_are_fixed_redacted_and_closed(home, raw):
    with owner(home, raw):
        response = asyncio.run(request())
        assert response.status_code == 503 and response.json() == api._ERROR


def test_actual_socket_original_deadline_includes_eof(home):
    raw = json.dumps(empty()).encode() + b'\n'
    def no_eof(connection, stop):
        connection.sendall(raw); stop.wait(6)
    with owner(home, no_eof):
        started = time.monotonic(); response = asyncio.run(request())
        assert response.status_code == 503 and 4.5 <= time.monotonic()-started < 5.8


@pytest.mark.parametrize('route,body,result', [
    ('claim',{'consumer_id':CONSUMER,'limit':1},dict(schema_version=1,profile='default',source='local',claims=[],server_now_ms=1)),
    ('presented',{'consumer_id':CONSUMER,'event_id':EVENT_ID,'claim_token':'a'*64,'notification_id':'host-receipt'}, {'status':'already_presented'}),
    ('release',{'consumer_id':CONSUMER,'event_id':EVENT_ID,'claim_token':'a'*64}, {'status':'already_released'}),
])
def test_actual_four_routes_forward_exact_owner_shapes(home, route, body, result):
    with owner(home, json.dumps(result).encode()+b'\n') as captured:
        response = asyncio.run(request('POST','/events/'+route+'?profile=default',json=body))
        assert response.status_code == 200 and response.json() == result
    assert json.loads(captured[0]) == dict(op='notification_'+route, protocol_version=1, **body)


@pytest.mark.parametrize('change', [dict(count=True), dict(count=501), dict(generation=1 << 53),
    dict(event_id=CONSUMER+':9007199254740992'), dict(type='private-engine-error'),
    dict(reason_code='raw private text'), dict(presentation_state='read'), dict(job_id='../secret'),
    dict(reveal_path='/Users/u/Downloads/Hermes/../private'), dict(extra='private')])
def test_actual_router_refuses_poisoned_event_dtos_without_exposing_text(home, change):
    event = dict(event_id=EVENT_ID, type='completion', job_id='job', collection_id=None,
        generation=0, created_at_ms=1, count=1, reason_code=None, reveal_path=None, presentation_state='pending')
    event.update(change); value = empty(); value['events'] = [event]
    with owner(home, json.dumps(value).encode()+b'\n'):
        response = asyncio.run(request())
        assert response.status_code == 503 and response.json() == api._ERROR


@pytest.mark.parametrize('raw', [b'{"consumer_id":"x","consumer_id":"y","limit":1}', b'x'*4097])
def test_duplicate_or_oversized_body_never_enters_transport(home, monkeypatch, raw):
    called = []; monkeypatch.setattr(api, '_exchange', lambda *args: called.append(args))
    response = asyncio.run(request('POST','/events/claim',content=raw))
    assert response.status_code == 400 and called == []

@pytest.mark.parametrize('route,op,body,response',[
    ('/files/reveals','reveal_pending',None,{'request':None}),
    ('/files/reveals/offer','reveal_offer',dict(request_id='request',job_id='job'),dict(status='offered',path='/Users/u/Downloads/Hermes/Other/f',token='a'*64)),
    ('/files/reveals/click','reveal_click',dict(request_id='request',job_id='job',token='a'*64),dict(status='issued',path='/Users/u/Downloads/Hermes/Other/f',token='a'*64)),
    ('/files/reveals/settle','reveal_settle',dict(request_id='request',job_id='job',token='a'*64,success=True),dict(status='revealed',path=None,token=None)),
])
def test_explicit_reveal_routes_are_closed_and_guarded(home,monkeypatch,route,op,body,response):
    seen=[]
    def exchange(endpoint,actual,payload):seen.append((actual,payload));return response
    monkeypatch.setattr(api,'_exchange',exchange)
    reply=asyncio.run(request('GET' if body is None else 'POST',route,**({} if body is None else {'json':body})))
    assert reply.status_code==200 and reply.json()==response
    assert seen==[(op,{} if body is None else body)]
    constants.pin_process_hermes_home(home/'.hermes/profiles/work')
    refused=asyncio.run(request('GET' if body is None else 'POST',route,**({} if body is None else {'json':body})))
    assert refused.status_code==403 and len(seen)==1


def test_explicit_reveal_refuses_unknown_fields_and_unsafe_response(home,monkeypatch):
    seen=[]
    def exchange(endpoint,op,body):seen.append(op);return dict(status='issued',path='/Users/u/Downloads/Hermes/../private',token='a'*64)
    monkeypatch.setattr(api,'_exchange',exchange)
    body=dict(request_id='request',job_id='job',token='a'*64)
    reply=asyncio.run(request('POST','/files/reveals/click',json=body|dict(extra='private')))
    assert reply.status_code==400 and seen==[]
    reply=asyncio.run(request('POST','/files/reveals/click',json=body))
    assert reply.status_code==503 and 'private' not in reply.text
