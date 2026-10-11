"""Default-profile notification relay; the worker alone owns durable state."""
import json
import os
from pathlib import Path
import re
import socket
import stat
import time

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

router = APIRouter()
_UUID = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
_FIELDS = set('event_id type job_id collection_id generation created_at_ms count reason_code reveal_path presentation_state'.split())
_SAFE = (1 << 53) - 1
_ERROR = {'error': 'downloads_notifications_unavailable'}


def _exact(value, fields):
    if type(value) is not dict or set(value) != set(fields):
        raise ValueError('invalid shape')


def _counter(value, positive=False):
    if type(value) is not int or not int(positive) <= value <= _SAFE:
        raise ValueError('invalid counter')


def _text(value, pattern):
    if type(value) is not str or re.fullmatch(pattern, value) is None:
        raise ValueError('invalid identifier')


def _event_id(value):
    _text(value, _UUID + r':[1-9][0-9]{0,15}')
    _counter(int(value.rsplit(':', 1)[1]), positive=True)


def _receipt(value):
    if (type(value) is not str or not value.strip() or len(value) > 256
        or any(ord(c) < 32 or 127 <= ord(c) <= 159 or 0xD800 <= ord(c) <= 0xDFFF for c in value)):
        raise ValueError('invalid receipt')


def _event(event):
    _exact(event, _FIELDS)
    _event_id(event['event_id'])
    if event['type'] not in {'completion','needs_link','needs_login','needs_space','persistent_error'}:
        raise ValueError('invalid type')
    for key in ('job_id', 'collection_id'):
        if event[key] is not None:
            _text(event[key], r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}')
    _counter(event['generation']); _counter(event['created_at_ms'])
    if type(event['count']) is not int or not 1 <= event['count'] <= 500:
        raise ValueError('invalid count')
    if event['reason_code'] is not None and event['reason_code'] not in {'link_expired','login_required','insufficient_space','retry_exhausted','download_failed'}:
        raise ValueError('invalid reason')
    if event['presentation_state'] not in {'pending','claimed','presented','resolved'}:
        raise ValueError('invalid state')
    path = event['reveal_path']
    if path is not None:
        if (type(path) is not str or event['type'] != 'completion' or len(path) > 4096 or not path.startswith('/')
            or any(ord(c) < 32 or 127 <= ord(c) <= 159 or c == '\\' or 0xD800 <= ord(c) <= 0xDFFF for c in path)):
            raise ValueError('invalid path')
        parts = path[1:].split('/')
        if (len(parts) < 6 or parts[0] not in {'Users','home'} or parts[2:4] != ['Downloads','Hermes']
            or any(not p.strip() or p in {'.','..'} or len(p) > 255 for p in parts)):
            raise ValueError('invalid path')


def _response(op, value):
    if op.startswith('reveal_'):return _reveal_response(op,value)
    if op in {'notification_presented','notification_release'}:
        _exact(value, {'status'})
        allowed = {'presented','already_presented','stale_claim'} if op == 'notification_presented' else {'released','already_released','stale_claim'}
        if value['status'] not in allowed:
            raise ValueError('invalid status')
        return value
    pending = op == 'notification_pending'
    _exact(value, {'schema_version','profile','source'} | ({'events','next_cursor','has_more'} if pending else {'claims','server_now_ms'}))
    if type(value['schema_version']) is not int or value['schema_version'] != 1 or value['profile'] != 'default' or value['source'] != 'local':
        raise ValueError('invalid envelope')
    if pending:
        if type(value['has_more']) is not bool:
            raise ValueError('invalid page')
        if value['next_cursor'] is not None:
            _text(value['next_cursor'], r'[A-Za-z0-9._:-]{1,256}')
        rows = value['events']
    else:
        _counter(value['server_now_ms'])
        rows = value['claims']
    if type(rows) is not list or len(rows) > 1:
        raise ValueError('invalid page')
    for row in rows:
        if pending:
            _event(row)
        else:
            _exact(row, {'event','claim_token','lease_expires_at_ms'})
            _event(row['event']); _text(row['claim_token'], r'[0-9a-f]{64}')
            _counter(row['lease_expires_at_ms'])
            if row['lease_expires_at_ms'] <= value['server_now_ms'] or row['event']['presentation_state'] not in {'pending','claimed'}:
                raise ValueError('invalid lease')
    return value


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate key')
        result[key] = value
    return result


def _decode(raw):
    return json.loads(raw.decode('utf-8'), object_pairs_hook=_pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError('invalid constant')))


def _endpoint():
    # Routing launch identity cannot be replaced by a mirrored request environment.
    from hermes_constants import get_hermes_home, get_routing_process_hermes_home
    home = Path.home()
    default = home / '.hermes'
    if not home.is_absolute() or home.resolve() != home or default.resolve() != default:
        raise ValueError('ambiguous default home')
    for value in (get_routing_process_hermes_home(), get_hermes_home()):
        if type(value) is not type(default) or value != default or value.resolve() != default:
            raise ValueError('wrong profile home')
    return home / 'Library/Application Support/HermesDownloadManager/default/worker.sock'


def _exchange(endpoint, op, body):
    deadline = time.monotonic() + 5.0
    details = endpoint.lstat()
    if (endpoint.resolve() != endpoint or not stat.S_ISSOCK(details.st_mode)
        or details.st_uid != os.geteuid() or stat.S_IMODE(details.st_mode) != 0o600):
        raise ValueError('invalid owner endpoint')
    payload = json.dumps(dict(op=op, protocol_version=1, **body), allow_nan=False,
        separators=(',', ':')).encode('utf-8') + b'\n'
    if len(payload) > 4096:
        raise ValueError('request too large')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        for operation in (lambda: client.connect(str(endpoint)), lambda: client.sendall(payload)):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            client.settimeout(remaining); operation()
        client.shutdown(socket.SHUT_WR)
        received = bytearray()
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            client.settimeout(remaining)
            chunk = client.recv(36865 - len(received))
            if not chunk:
                break
            received.extend(chunk)
            if len(received) > 36864:
                raise ValueError('response too large')
        if not received.endswith(b'\n') or received.count(b'\n') != 1:
            raise ValueError('invalid framing')
        return _response(op, _decode(received[:-1]))


async def _handle(request, op):
    try:
        endpoint = _endpoint()
    except Exception:
        return JSONResponse(_ERROR, status_code=403)
    try:
        query = list(request.query_params.multi_items())
        keys = [key for key, _ in query]
        if len(set(keys)) != len(keys):
            raise ValueError('duplicate query')
        params = dict(query)
        if 'profile' in params and params.pop('profile') != 'default':
            raise ValueError('wrong query profile')
        if op == 'notification_pending':
            if params != {'view':'pending','limit':'1'}:
                raise ValueError('invalid pending query')
            body = dict(view='pending', limit=1)
        else:
            if params:
                raise ValueError('unknown query')
            raw = bytearray()
            async for chunk in request.stream():
                if len(raw) + len(chunk) > 4096:
                    raise ValueError('body too large')
                raw.extend(chunk)
            body = _decode(raw)
            if op == 'notification_claim':
                _exact(body, {'consumer_id','limit'})
                if type(body['limit']) is not int or body['limit'] != 1:
                    raise ValueError('invalid limit')
            else:
                _exact(body, {'consumer_id','event_id','claim_token'} | ({'notification_id'} if op == 'notification_presented' else set()))
                _event_id(body['event_id']); _text(body['claim_token'], r'[0-9a-f]{64}')
                if op == 'notification_presented':
                    _receipt(body['notification_id'])
            _text(body['consumer_id'], _UUID)
    except Exception:
        return JSONResponse(_ERROR, status_code=400)
    try:
        return await run_in_threadpool(_exchange, endpoint, op, body)
    except Exception:
        return JSONResponse(_ERROR, status_code=503)


@router.get('/events')
async def pending(request: Request):
    return await _handle(request, 'notification_pending')


@router.post('/events/claim')
async def claim(request: Request):
    return await _handle(request, 'notification_claim')


@router.post('/events/presented')
async def presented(request: Request):
    return await _handle(request, 'notification_presented')


@router.post('/events/release')
async def release(request: Request):
    return await _handle(request, 'notification_release')


def _reveal_response(op,value):
    if op=='reveal_pending':
        _exact(value,{'request'})
        if value['request'] is not None:
            _exact(value['request'],{'request_id','job_id'})
            for field in ('request_id','job_id'):_text(value['request'][field],r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}')
        return value
    _exact(value,{'status','path','token'})
    if value['status'] not in {'offered','issued','presented','revealed','reveal_failed','unknown','stale'}:raise ValueError('invalid reveal status')
    if value['status'] in {'offered','issued'}:
        _text(value['token'],r'[0-9a-f]{64}')
        path=value['path']
        if type(path) is not str or len(path)>4096 or not path.startswith('/') or any(ord(c)<32 or 127<=ord(c)<=159 or c=='\\' or 0xD800<=ord(c)<=0xDFFF for c in path):raise ValueError('invalid reveal path')
        parts=path[1:].split('/')
        if len(parts)<6 or parts[0] not in {'Users','home'} or parts[2:4]!=['Downloads','Hermes'] or any(not p.strip() or p in {'.','..'} or len(p)>255 for p in parts):raise ValueError('invalid reveal path')
    elif value['path'] is not None or value['token'] is not None:raise ValueError('unexpected reveal authority')
    return value


async def _reveal_handle(request,op):
    try:endpoint=_endpoint()
    except Exception:return JSONResponse(_ERROR,status_code=403)
    try:
        params=list(request.query_params.multi_items())
        if params not in ([],[('profile','default')]):raise ValueError('invalid reveal query')
        if op=='reveal_pending':body={}
        else:
            raw=bytearray()
            async for chunk in request.stream():
                if len(raw)+len(chunk)>4096:raise ValueError('reveal body too large')
                raw.extend(chunk)
            body=_decode(raw)
            fields={'request_id','job_id'}|({'token'} if op in {'reveal_click','reveal_presented','reveal_settle'} else set())|({'notification_id'} if op=='reveal_presented' else set())|({'success'} if op=='reveal_settle' else set())
            _exact(body,fields)
            for field in ('request_id','job_id'):_text(body[field],r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}')
            if 'token' in body:_text(body['token'],r'[0-9a-f]{64}')
            if 'notification_id' in body:_receipt(body['notification_id'])
            if 'success' in body and type(body['success']) is not bool:raise ValueError('invalid reveal outcome')
    except Exception:return JSONResponse(_ERROR,status_code=400)
    try:return _reveal_response(op,await run_in_threadpool(_exchange,endpoint,op,body))
    except Exception:return JSONResponse(_ERROR,status_code=503)


@router.get('/files/reveals')
async def reveal_pending(request:Request):return await _reveal_handle(request,'reveal_pending')

@router.post('/files/reveals/offer')
async def reveal_offer(request:Request):return await _reveal_handle(request,'reveal_offer')

@router.post('/files/reveals/presented')
async def reveal_presented(request:Request):return await _reveal_handle(request,'reveal_presented')

@router.post('/files/reveals/click')
async def reveal_click(request:Request):return await _reveal_handle(request,'reveal_click')

@router.post('/files/reveals/settle')
async def reveal_settle(request:Request):return await _reveal_handle(request,'reveal_settle')
