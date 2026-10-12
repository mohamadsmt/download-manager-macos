#!/usr/bin/env python3
"""Opt-in installed SDK/worker/stock acceptance; all evidence and input are private.

The allowance is a conservative HTTP-payload reservation for a trusted, fixed,
sub-1MiB representation on pinned aria2 1.37.0. It is not a WAN byte counter or
an adversarial wire cap. Only one fresh admission is allowed in this trial.
Tagged Request.h MAX_REDIRECT=20 gives 21 possible response heads; reserve six
such chains plus 16KiB receive-buffer slack per response. Success additionally
requires an exact single-request, finite-200 ledger from the original reader.
"""
import argparse
import asyncio
from contextlib import contextmanager, closing
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import runpy
import signal
import stat
import subprocess
import sys
import threading
import time
import tomllib

LIMIT=256*1024*1024
TOOLS=['downloads_add','downloads_control','downloads_edit','downloads_replace_source','downloads_files','downloads_configure','downloads_query']
OPTIONS={'split':'1','max-connection-per-server':'1','min-split-size':'1048576','max-tries':'1',
    'reuse-uri':'false','always-resume':'true','enable-http-pipelining':'false','enable-http-keep-alive':'false',
    'http-auth-challenge':'false','http-accept-gzip':'false','use-head':'false','conditional-get':'false','retry-wait':'0'}


class Refused(RuntimeError): pass


class Parser(argparse.ArgumentParser):
    def error(self, message):raise Refused('arguments_invalid')


class Deadline:
    def __init__(self, record=None):
        self.record=record or dict(wall_start=time.time(),monotonic_start=time.monotonic(),seconds=900,reserve=60)
        if self.record['seconds']!=900 or self.record['reserve']!=60:raise Refused('deadline_invalid')
    def remaining(self, *, cleanup=False):
        value=min(self.record['wall_start']+900-time.time(),self.record['monotonic_start']+900-time.monotonic())-(0 if cleanup else 60)
        if value<=0:raise Refused('aggregate_deadline')
        return value


def save(path,value):
    with Path(path).open('x',encoding='utf-8') as stream:
        os.chmod(path,0o600);json.dump(value,stream,sort_keys=True,indent=2)


def digest(path):
    with Path(path).open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def packet_read(path):
    path=Path(path).absolute()
    if path.resolve()!=path or path.parent.stat().st_uid!=os.geteuid() or stat.S_IMODE(path.parent.stat().st_mode)!=0o700:raise Refused('private_input_parent')
    with os.fdopen(os.open(path,os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW|os.O_CLOEXEC),'rb') as stream:
        info=os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o600 or info.st_nlink!=1 or info.st_size>16384:raise Refused('private_input')
        value=json.load(stream)
    if type(value) is not dict or set(value)!={'url','expected_sha256','length','handle','qualification'}:raise Refused('input_shape')
    if type(value['url']) is not str or len(value['url'].encode())>8192:raise Refused('input_source')
    from urllib.parse import urlsplit
    parts=urlsplit(value['url'])
    if parts.scheme!='https' or not parts.hostname or parts.username is not None or parts.password is not None or parts.fragment or any(c.isspace() or ord(c)<32 for c in value['url']):raise Refused('input_source')
    if type(value['expected_sha256']) is not str or not re.fullmatch('[0-9a-f]{64}',value['expected_sha256']):raise Refused('input_digest')
    if type(value['handle']) is not str or not re.fullmatch('[A-Za-z0-9_-]{1,64}',value['handle']):raise Refused('input_handle')
    allowance(value)
    return value


def allowance(packet):
    length=packet['length'];qualification=packet['qualification']
    if type(length) is not int or not 0<length<1024*1024:raise Refused('tiny_fresh_geometry_required')
    if type(qualification) is not dict or set(qualification)!={'aria2_sha256','trusted_fixed_representation','max_response_bytes'}:raise Refused('budget_unavailable')
    if qualification['trusted_fixed_representation'] is not True or type(qualification['max_response_bytes']) is not int or qualification['max_response_bytes']!=length:raise Refused('budget_unavailable')
    if type(qualification['aria2_sha256']) is not str or not re.fullmatch('[0-9a-f]{64}',qualification['aria2_sha256']):raise Refused('binary_unqualified')
    reserved=6*21*(length+16384)
    if reserved>LIMIT:raise Refused('budget_unavailable')
    return reserved


def birth(pid,argv=()):
    from hermes_downloads.processes import capture_process_birth,EngineIdentity
    argv_sha=hashlib.sha256(b'\0'.join(os.fsencode(arg) for arg in argv)).hexdigest()
    identity=capture_process_birth(EngineIdentity(pid,pid,time.monotonic_ns(),argv_sha))
    if identity is None:raise Refused('child_birth_unavailable')
    return identity.to_record()


def absent(pid):
    try:os.killpg(pid,0)
    except ProcessLookupError:return True
    return False


def echild(pid):
    try:os.waitpid(pid,os.WNOHANG)
    except ChildProcessError:return True
    return False


@contextmanager
def owned_worker(command,env,evidence,deadline):
    """Retain the original child before birth/receipt failures can occur."""
    with (evidence/'worker.log').open('xb') as log:
        os.fchmod(log.fileno(),0o600)
        child=subprocess.Popen(command,env=env,cwd=evidence,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        identity=None;forced=False
        try:
            identity=birth(child.pid,command)
            save(evidence/'worker-birth.json',dict(pid=child.pid,birth=identity,command=command,carrier='original child; live worker uses runpy of byte-verified installed console'))
            yield child
        finally:
            if child.poll() is None:
                # This original, unreaped child owns its session even if birth logging failed.
                if os.getpgid(child.pid)!=child.pid:raise Refused('worker_group_changed')
                os.killpg(child.pid,signal.SIGTERM)
                try:child.wait(timeout=min(20,deadline.remaining(cleanup=True)))
                except subprocess.TimeoutExpired:
                    forced=True;os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=min(5,deadline.remaining(cleanup=True)))
            code=child.wait(timeout=min(5,deadline.remaining(cleanup=True)))
            receipt=dict(pid=child.pid,birth=identity,returncode=code,original_wait=True,echild=echild(child.pid),group_absent=absent(child.pid),forced=forced)
            save(evidence/'worker-closure.json',receipt)
            if forced or not receipt['echild'] or not receipt['group_absent']:raise Refused('worker_closure_failed')


def command_read(argv,deadline,receipts):
    # Hold even a very short metadata command until its original child birth is recorded.
    command=[sys.executable,'-I','-B','-c',"import os,sys;sys.stdin.buffer.read(1);os.execvp(sys.argv[1],sys.argv[1:])",*argv]
    child=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,start_new_session=True)
    identity=None
    try:
        identity=birth(child.pid,command)
        output,_=child.communicate(input=b'1',timeout=min(10,deadline.remaining()))
        if child.wait()!=0:raise Refused('metadata_command_failed')
        return output.decode('utf-8')
    finally:
        if child.poll() is None:
            os.killpg(child.pid,signal.SIGTERM)
            try:child.wait(timeout=min(2,deadline.remaining(cleanup=True)))
            except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=min(2,deadline.remaining(cleanup=True)))
        receipts.append(dict(argv=argv,carrier=command,birth=identity,code=child.wait(),original_wait=True,echild=echild(child.pid),group_absent=absent(child.pid)))
        if not receipts[-1]['echild'] or not receipts[-1]['group_absent']:raise Refused('metadata_child_closure')


def preflight(expected,packet,deadline):
    deadline.remaining()
    if sys.version_info[:2]!=(3,12) or sys.prefix==sys.base_prefix or not sys.flags.isolated or not sys.dont_write_bytecode or os.environ.get('PYTHONPATH') or os.environ.get('PYTHONHOME'):raise Refused('isolated_312_required')
    root=Path(__file__).resolve().parents[2];source=root/'headless/src/hermes_downloads'
    distribution=importlib.metadata.distribution('hermes-downloads');site=Path(distribution.locate_file('hermes_downloads'))
    direct=json.loads(distribution.read_text('direct_url.json'))
    if direct.get('dir_info',{}).get('editable') is not False:raise Refused('noneditable_required')
    modules={}
    for path in source.glob('*.py'):
        installed=site/path.name;a=path.stat();b=installed.stat()
        modules[path.name]=dict(source=digest(path),installed=digest(installed),source_devino=[a.st_dev,a.st_ino],installed_devino=[b.st_dev,b.st_ino])
    if len(modules)!=16 or any(v['source']!=v['installed'] or v['source_devino']==v['installed_devino'] for v in modules.values()):raise Refused('physical_parity')
    normalize=lambda name:re.sub('[-_.]+','-',name).lower()
    versions={normalize(d.metadata['Name']):d.version for d in importlib.metadata.distributions()}
    locked={normalize(p['name']):p['version'] for p in tomllib.loads((root/'headless/uv.lock').read_text())['package']}
    if len(versions)!=43 or any(locked.get(n)!=v for n,v in versions.items()):raise Refused('locked_distributions')
    entries={e.name:e.value for e in distribution.entry_points if e.group=='console_scripts'}
    if entries!={'hermes-downloads':'hermes_downloads.cli:main','hermes-downloads-mcp':'hermes_downloads.mcp_server:main','hermes-downloads-worker':'hermes_downloads.service:worker_main'}:raise Refused('entrypoints')
    consoles={name:str(Path(sys.executable).parent/name) for name in entries}
    for name,target in entries.items():
        module,function=target.split(':');text=Path(consoles[name]).read_text()
        tail=('# -*- coding: utf-8 -*-\nimport sys\nfrom '+module+' import '+function+'\n'
            'if __name__ == "__main__":\n'
            '    if sys.argv[0].endswith("-script.pyw"):\n'
            '        sys.argv[0] = sys.argv[0][:-11]\n'
            '    elif sys.argv[0].endswith(".exe"):\n'
            '        sys.argv[0] = sys.argv[0][:-4]\n'
            '    sys.exit('+function+'())\n')
        interpreter=str(Path(sys.executable).parent/'python3')
        shim="#!/bin/sh\n'''exec' '"+interpreter+"' \"$0\" \"$@\"\n' '''\n"
        if text not in (shim+tail,'#!'+interpreter+'\n'+tail,'#!'+sys.executable+'\n'+tail):raise Refused('actual_console_template')
    receipts=[]
    if type(expected) is not str or not re.fullmatch('[0-9a-f]{40}',expected):raise Refused('frozen_commit_required')
    if command_read(['git','-C',str(root),'rev-parse','HEAD'],deadline,receipts).strip()!=expected or command_read(['git','-C',str(root),'status','--porcelain'],deadline,receipts).strip():raise Refused('clean_frozen_commit_required')
    vendor=Path('/opt/homebrew/bin/aria2c')
    vendor_sha=digest(vendor)
    if vendor_sha!=packet['qualification']['aria2_sha256']:raise Refused('binary_binding')
    version=command_read([str(vendor),'--version'],deadline,receipts)
    if version.splitlines()[0]!='aria2 version 1.37.0':raise Refused('stock_version')
    return dict(commit=expected,modules=modules,versions=versions,entries=entries,direct=direct,consoles=consoles,
        console_hashes={n:digest(p) for n,p in consoles.items()},vendor=str(vendor),vendor_sha256=vendor_sha,version=version,metadata_children=receipts)


class AdmissionGuard:
    def __init__(self,packet,emit,deadline):
        self.packet,self.emit,self.deadline=packet,emit,deadline
        self.reserved=allowance(packet);self.admissions=0
        emit('budget-reservation',bytes=self.reserved,limit=LIMIT,chains=6,heads_per_chain=21,slack_per_head=16384,kind_of_bytes='conservative trusted HTTP payload allowance, not measured WAN')
    def add(self,controller,original,**kwargs):
        self.deadline.remaining()
        if self.reserved!=allowance(self.packet) or self.admissions!=0 or kwargs.get('saved_resume',False) or kwargs['job_id']!='live-job':raise Refused('unqualified_continuation_or_admission')
        destination=kwargs.get('destination')
        if destination is not None and (destination.partial_path.exists() or destination.partial_path.with_name(destination.partial_path.name+'.aria2').exists()):raise Refused('fresh_pair_required')
        self.admissions+=1
        self.emit('budget-admission',number=self.admissions,reserved=self.reserved)
        rpc=controller._rpc
        def qualified_rpc(name,args):
            if name=='aria2.addUri':
                if len(args)!=2 or args[0]!=[self.packet['url']] or args[1].get('pause')!='true':raise Refused('single_paused_uri_required')
                args=[args[0],args[1]|OPTIONS|{'piece-length':'1048576'}]
                self.emit('paused-request-qualified',options=OPTIONS,piece_length='1048576',uris=1)
            return rpc(name,args)
        controller._rpc=qualified_rpc
        try:result=original(**kwargs)
        finally:controller._rpc=rpc
        if result.status!='paused':raise Refused('paused_admission_required')
        if controller._rpc('aria2.changeOption',[result.gid,{'split':'1','max-connection-per-server':'1'}])!='OK':raise Refused('stock_options')
        actual=controller._rpc('aria2.getOption',[result.gid]);global_options=controller._rpc('aria2.getGlobalOption',[])
        self.emit('paused-options-readback',options={k:actual.get(k) for k in (*OPTIONS,'allow-overwrite','auto-file-renaming','continue','check-certificate')},piece_length=global_options.get('piece-length'),tls=global_options.get('check-certificate'),no_netrc=global_options.get('no-netrc'))
        if type(actual) is not dict or any(actual.get(k)!=v for k,v in OPTIONS.items()) or global_options.get('piece-length')!='1048576':raise Refused('stock_options')
        for name,value in {'allow-overwrite':'false','auto-file-renaming':'false','continue':'true'}.items():
            if actual.get(name)!=value:raise Refused('stock_retained_protections')
        if global_options.get('check-certificate')!='true' or global_options.get('no-netrc')!='true':raise Refused('stock_global_protections')
        self.emit('paused-options',gid=result.gid,options={k:actual[k] for k in (*OPTIONS,'allow-overwrite','auto-file-renaming','continue')},piece_length=global_options['piece-length'],tls=global_options['check-certificate'],no_netrc=global_options['no-netrc'])
        return result


def worker_carrier(config_path):
    """Private pass-through instrumentation, then the actual installed console."""
    config=json.loads(Path(config_path).read_text());evidence=Path(config['evidence']);packet=config['packet'];metadata=config['metadata'];deadline=Deadline(config['deadline'])
    lock=threading.Lock();engine_children=[];threads=[];frames=0
    def emit(kind,**values):
        with lock:
            with (evidence/'carrier.jsonl').open('a') as log:
                os.fchmod(log.fileno(),0o600)
                log.write(json.dumps(dict(kind=kind,monotonic=time.monotonic(),**values))+'\n')
    from hermes_downloads import direct
    guard=AdmissionGuard(packet,emit,deadline)
    original_add=direct.DirectAria2Controller.add_paused
    def add(controller,**kwargs):
        try:return guard.add(controller,lambda **kw:original_add(controller,**kw),**kwargs)
        except BaseException as error:
            emit('qualification-refused',reason=str(error) if isinstance(error,Refused) else 'stock_qualification_failed')
            raise
    direct.DirectAria2Controller.add_paused=add
    original_popen=subprocess.Popen
    def popen(argv,*args,**kwargs):
        child=original_popen(argv,*args,**kwargs)
        if str(argv[0])==metadata['vendor']:
            record=dict(child=child,birth=None,waits=[]);engine_children.append(record)
            original_wait=child.wait
            def wait(*a,**kw):
                code=original_wait(*a,**kw);record['waits'].append(code);emit('engine-original-wait',pid=child.pid,code=code);return code
            child.wait=wait
            try:
                record['birth']=birth(child.pid,argv);emit('engine-birth',pid=child.pid,birth=record['birth'],argv=list(argv))
            except BaseException:
                stopped,error=direct._stop_process(child,direct._bind_process_group(child),deadline=time.monotonic()+min(2,deadline.remaining(cleanup=True)))
                emit('engine-birth-failed',pid=child.pid,contained=stopped,error=error is not None)
                if not stopped or error is not None:raise Refused('engine_birth_closure_failed')
                raise
        return child
    subprocess.Popen=popen
    original_join=threading.Thread.join
    def join(thread,*args,**kwargs):
        result=original_join(thread,*args,**kwargs)
        if thread.name.startswith('direct-'):
            if thread not in threads:threads.append(thread)
            emit('original-thread-join',name=thread.name,ident=thread.ident,alive=thread.is_alive())
        return result
    threading.Thread.join=join
    original_head=direct._StockCapture._head
    def head(capture,cuid,kind,lines):
        nonlocal frames
        frames+=1
        if frames>64:raise Refused('head_ledger_bound')
        emit('http-head',cuid=cuid.decode('ascii'),frame=kind.decode('ascii'),lines=[line.decode('ascii') for line in lines])
        return original_head(capture,cuid,kind,lines)
    direct._StockCapture._head=head
    original_finish=direct._StockCapture.finish
    def finish(capture):
        result=original_finish(capture)
        emit('original-reader-eof',eof=capture.eof,stock=[dict(gid=gid,known=capture.result(gid).known,http_status=capture.result(gid).http_status,contributors=capture.result(gid).contributors,total_length=capture.result(gid).total_length) for gid in capture._heads])
        return result
    direct._StockCapture.finish=finish
    console=metadata['consoles']['hermes-downloads-worker']
    if digest(console)!=metadata['console_hashes']['hermes-downloads-worker']:raise Refused('console_changed')
    sys.argv=[console,'--serve','--state-root',config['state']]
    try:runpy.run_path(console,run_name='__main__')
    finally:
        for record in engine_children:
            child=record['child']
            # Observe only; product shutdown must have waited its original engine.
            receipt=dict(pid=child.pid,birth=record['birth'],waits=record['waits'],code=child.returncode,
                echild=echild(child.pid) if child.returncode is not None else False,group_absent=absent(child.pid))
            emit('engine-closure',**receipt)
        emit('carrier-closed',admissions=guard.admissions,threads=[dict(name=t.name,alive=t.is_alive()) for t in threads])


def worker_command(config):
    # The private bootstrap is an observational carrier, not a worker-loop copy.
    code="import runpy,sys;runpy.run_path(sys.argv[1])['worker_carrier'](sys.argv[2])"
    return [sys.executable,'-I','-B','-c',code,str(Path(__file__).resolve()),str(config)]


async def sdk_flow(state,home,packet,evidence,metadata,deadline,worker):
    from mcp import ClientSession,StdioServerParameters
    from mcp.client.stdio import stdio_client
    from mcp.client import stdio as sdk_stdio
    sdk_children=[];original_create=sdk_stdio._create_platform_compatible_process
    async def create(**kwargs):
        child=await original_create(**kwargs)
        record=dict(child=child,birth=None,waits=[]);sdk_children.append(record)
        original_wait=child.wait
        async def wait():
            code=await original_wait();record['waits'].append(code);return code
        child.wait=wait
        try:record['birth']=birth(child.pid,[kwargs['command'],*kwargs.get('args',[])])
        except BaseException:
            if child.returncode is None:child.terminate()
            try:await asyncio.wait_for(child.wait(),timeout=min(3,deadline.remaining(cleanup=True)))
            except asyncio.TimeoutError:
                record['forced']=True;child.kill()
                await asyncio.wait_for(child.wait(),timeout=min(3,deadline.remaining(cleanup=True)))
            await child.aclose();raise
        return child
    sdk_stdio._create_platform_compatible_process=create
    environment={k:v for k,v in os.environ.items() if k in {'PATH','LANG','LC_ALL','TMPDIR'}}
    environment.update(HOME=str(home),HERMES_DOWNLOADS_SOCKET=str(state/'worker.sock'),PYTHONDONTWRITEBYTECODE='1',PYTHONNOUSERSITE='1')
    receipts=[]
    try:
        async with stdio_client(StdioServerParameters(command=metadata['consoles']['hermes-downloads-mcp'],cwd=state,env=environment)) as (read,write):
            async with ClientSession(read,write) as session:
                async def call(name,arguments):
                    result=await asyncio.wait_for(session.call_tool(name,arguments=arguments),timeout=min(10,deadline.remaining()))
                    if result.isError or result.structuredContent!=json.loads(result.content[0].text):raise Refused('sdk_operation')
                    value=result.structuredContent;receipts.append(dict(tool=name,arguments=arguments,result=value));return value
                await asyncio.wait_for(session.initialize(),timeout=min(10,deadline.remaining()))
                tools=await session.list_tools()
                if [tool.name for tool in tools.tools]!=TOOLS:raise Refused('seven_tools_required')
                queue=await call('downloads_query',dict(scope='queue'))
                if queue['queue_gate']!='paused':raise Refused('fresh_paused_queue')
                item=dict(job='live-job',source_kind='direct',source_url=packet['url'],priority=0,category='Other',partial_filename='authorized.bin',selected_final_filename='authorized.bin',expected_sha256=packet['expected_sha256'])
                add=dict(request_id='live-add',items=[item],start=False)
                created=await call('downloads_add',add)
                replay=await call('downloads_add',add)
                if replay!=created|dict(replayed=True):raise Refused('creation_replay')
                await call('downloads_query',dict(scope='list'))
                status=await call('downloads_query',dict(scope='status',id='live-job'))
                if (evidence/'carrier.jsonl').exists() and any(json.loads(line)['kind'] in {'budget-admission','engine-birth','http-head'} for line in (evidence/'carrier.jsonl').read_text().splitlines()):raise Refused('implicit_body')
                await call('downloads_configure',dict(request_id='live-one',concurrency='1',connections='1'))
                while True:
                    settings=await call('downloads_query',dict(scope='settings'))
                    if settings['status']=='applied':break
                    await asyncio.sleep(.02)
                if settings['applied']['concurrency']!='1' or settings['applied']['connections']!='1':raise Refused('configuration_readback')
                await call('downloads_control',dict(scope='targets',action='start',request_id='live-start',expected_worker_epoch=queue['worker_epoch'],targets=[dict(job='live-job',expected_revision=str(status['record']['revision']))]))
                await call('downloads_control',dict(scope='queue',action='resume',request_id='live-resume',expected_revision=queue['revision']))
                while True:
                    if worker.poll() is not None:raise Refused('worker_exited')
                    if any(json.loads(line)['kind'] in {'qualification-refused','engine-birth-failed'} for line in (evidence/'carrier.jsonl').read_text().splitlines()):raise Refused('qualification_refused_before_body')
                    status=await call('downloads_query',dict(scope='status',id='live-job'))
                    state_name=status['record']['state']
                    if state_name=='completed':break
                    if state_name in {'failed','blocked','needs_link','needs_auth','retry_wait','offline_wait','cancelled','removed'}:raise Refused('first_attempt_failed')
                    await asyncio.sleep(.05)
                detail=await call('downloads_query',dict(scope='detail',id='live-job'))
                if await call('downloads_add',add)!=replay:raise Refused('immutable_creation_receipt')
                save(evidence/'sdk-receipts.json',dict(tools=TOOLS,receipts=receipts,terminal=status,detail=detail,creation=created))
    finally:
        sdk_stdio._create_platform_compatible_process=original_create
        closed=[]
        for record in sdk_children:
            child=record['child']
            if child.returncode is None:
                child.terminate()
                try:await asyncio.wait_for(child.wait(),timeout=min(3,deadline.remaining(cleanup=True)))
                except asyncio.TimeoutError:
                    record['forced']=True;child.kill()
                    await asyncio.wait_for(child.wait(),timeout=min(3,deadline.remaining(cleanup=True)))
            if not record['waits']:await asyncio.wait_for(child.wait(),timeout=min(5,deadline.remaining(cleanup=True)))
            closed.append(dict(pid=child.pid,birth=record['birth'],waits=record['waits'],code=child.returncode,forced=record.get('forced',False),echild=echild(child.pid),group_absent=absent(child.pid)))
        if not (evidence/'sdk-receipts.json').exists():save(evidence/'sdk-receipts.json',dict(outcome='FAILED',receipts=receipts))
        save(evidence/'sdk-closure.json',closed)
        if not closed or any(not row['waits'] or row['code']!=0 or row['forced'] or not row['echild'] or not row['group_absent'] for row in closed):raise Refused('sdk_closure')


def qualify_heads(records,length,source=None):
    heads=[row for row in records if row['kind']=='http-head']
    if len(heads)!=2 or [row['frame'] for row in heads]!=['Requesting','Response received'] or heads[0]['cuid']!=heads[1]['cuid']:raise Refused('response_chain_unqualified')
    request,response=heads
    if not request['lines'][0].startswith('GET ') or not re.fullmatch(r'HTTP/1\.[01] 200(?: .*)?',response['lines'][0]):raise Refused('response_status_unqualified')
    if source is not None:
        from urllib.parse import urlsplit
        parts=urlsplit(source);target=(parts.path or '/')+('?' + parts.query if parts.query else '')
        hosts=[line.partition(':')[2].strip() for line in request['lines'][1:] if line.lower().startswith('host:')]
        if request['lines'][0]!='GET '+target+' HTTP/1.1' or hosts!=[parts.netloc]:raise Refused('actual_resource_changed')
    headers={}
    for line in response['lines'][1:]:
        name,separator,value=line.partition(':');name=name.lower()
        if not separator or name in headers:raise Refused('response_headers_unqualified')
        headers[name]=value.strip()
    if headers.get('content-length')!=str(length) or 'transfer-encoding' in headers or headers.get('content-encoding','identity')!='identity':raise Refused('response_length_unqualified')
    if any(line.lower().startswith(('range:','authorization:','cookie:')) for line in request['lines'][1:]):raise Refused('fresh_request_unqualified')
    if sum(line.lower().startswith('x-hermes-attempt: ') for line in request['lines'][1:])!=1:raise Refused('attempt_identity_unqualified')
    return dict(gets=1,responses=1,status=200,content_length=length,qualified_received_http_payload=length,wire_counter=False)


def final_verify(home,packet,deadline):
    root=home/'Downloads/Hermes';fds=[]
    try:
        fds.append(os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW))
        fds.append(os.open('Other',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fds[-1]))
        fds.append(os.open('authorized.bin',os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fds[-1]))
        before=os.fstat(fds[-1]);hasher=hashlib.sha256();count=0
        if not stat.S_ISREG(before.st_mode) or before.st_uid!=os.geteuid() or before.st_nlink!=1 or before.st_size!=packet['length']:raise Refused('final_descriptor')
        while block:=os.read(fds[-1],65536):deadline.remaining();hasher.update(block);count+=len(block)
        after=os.fstat(fds[-1])
        identity=lambda info:(info.st_dev,info.st_ino,info.st_size,info.st_mode,info.st_nlink,info.st_mtime_ns,info.st_ctime_ns)
        visible=os.stat('authorized.bin',dir_fd=fds[-2],follow_symlinks=False)
        if identity(before)!=identity(after) or identity(visible)!=identity(after) or count!=packet['length'] or hasher.hexdigest()!=packet['expected_sha256']:raise Refused('final_digest_or_identity')
        return dict(path=str(root/'Other/authorized.bin'),descriptor=list(identity(after)),sha256=hasher.hexdigest(),bytes=count)
    finally:
        for fd in reversed(fds):os.close(fd)


def run_live(packet,evidence,metadata,deadline):
    evidence=Path(evidence).absolute()
    if evidence.resolve()!=evidence or stat.S_IMODE(evidence.parent.stat().st_mode)!=0o700 or evidence.parent.stat().st_uid!=os.geteuid():raise Refused('private_evidence_parent')
    evidence.mkdir(mode=0o700)
    home=evidence/'home';state=evidence/'state';home.mkdir(mode=0o700);state.mkdir(mode=0o700)
    if len(os.fsencode(state/'worker.sock'))>103:raise Refused('socket_path_too_long')
    output=home/'Downloads/Hermes';output.mkdir(mode=0o700,parents=True)
    save(evidence/'preflight.json',metadata)
    config=evidence/'carrier-input.json';save(config,dict(packet=packet,evidence=str(evidence),state=str(state),metadata=metadata,deadline=deadline.record))
    env={k:v for k,v in os.environ.items() if k in {'PATH','LANG','LC_ALL','TMPDIR'}}
    env.update(HOME=str(home),HERMES_DOWNLOADS_OUTPUT_ROOT=str(output),PYTHONDONTWRITEBYTECODE='1',PYTHONNOUSERSITE='1')
    final=None
    with owned_worker(worker_command(config),env,evidence,deadline) as worker:
        while not (state/'worker.sock').exists():
            deadline.remaining()
            if worker.poll() is not None:raise Refused('worker_start_failed')
            time.sleep(.02)
        asyncio.run(asyncio.wait_for(sdk_flow(state,home,packet,evidence,metadata,deadline,worker),timeout=deadline.remaining()))
        final=final_verify(home,packet,deadline)
    worker_receipt=json.loads((evidence/'worker-closure.json').read_text())
    if worker_receipt['returncode']!=0:raise Refused('worker_exit_failed')
    records=[json.loads(line) for line in (evidence/'carrier.jsonl').read_text().splitlines()]
    measured=qualify_heads(records,packet['length'],packet['url'])
    engines=[row for row in records if row['kind']=='engine-closure']
    joins=[row for row in records if row['kind']=='original-thread-join']
    if len(engines)!=1 or any(not row['waits'] or row['code']!=0 or not row['echild'] or not row['group_absent'] for row in engines):raise Refused('engine_original_closure')
    if not any(row['name']=='direct-stock-reader' and not row['alive'] for row in joins) or not any(row['name']!='direct-stock-reader' and not row['alive'] for row in joins) or not any(row['kind']=='original-reader-eof' and row['eof'] for row in records):raise Refused('original_reader_observer_join')
    stock=[value for row in records if row['kind']=='original-reader-eof' for value in row.get('stock',[])]
    if len(stock)!=1 or not stock[0]['known'] or (stock[0]['http_status'],stock[0]['contributors'],stock[0]['total_length'])!=(200,1,packet['length']):raise Refused('stock_capture_unqualified')
    from hermes_downloads.store import SQLiteStore
    with closing(SQLiteStore(state/'state.db')) as store:
        causes=store._connection.execute("SELECT request_id,job_id,kind,admission_serial FROM target_dispatch_causes WHERE kind='body'").fetchall()
        binding=store._get_final_publication_binding('live-job')
        if len(causes)!=1 or tuple(causes[0])[1:]!=('live-job','body',1) or store.get_job('live-job').state!='completed' or binding is None or store._job_engine_owners():raise Refused('terminal_owner_binding')
        if (binding.final_device,binding.final_inode,binding.logical_size)!=tuple(final['descriptor'][:3]):raise Refused('final_owner_descriptor_mismatch')
        stock_owner=store._connection.execute('SELECT owner FROM direct_stock_bindings WHERE body_request_id=?',(causes[0][0],)).fetchone()
        if stock_owner is None or json.loads(stock_owner[0])[:3]!=['live-job',store.get_job('live-job').generation,stock[0]['gid']]:raise Refused('source_owner_binding')
        source_owner=json.loads(stock_owner[0])
        request=next(row for row in records if row['kind']=='http-head' and row['frame']=='Requesting')
        tag=next(line.partition(':')[2].strip() for line in request['lines'][1:] if line.lower().startswith('x-hermes-attempt:'))
        actual_birth=engines[0]['birth']
        argv_sha=hashlib.sha256(b'\0'.join(arg.encode() for arg in next(row['argv'] for row in records if row['kind']=='engine-birth'))).hexdigest()
        if actual_birth['argv_sha256']!=argv_sha:raise Refused('source_original_argv')
        if source_owner[5]!=hashlib.sha256(json.dumps(actual_birth,sort_keys=True,separators=(',',':')).encode()).hexdigest() or source_owner[6]!=hashlib.sha256(tag.encode()).hexdigest():raise Refused('source_original_birth_or_attempt')
        owner=dict(cause=list(causes[0]),final_binding=repr(binding),source_binding=source_owner)
    receipt=dict(outcome='QUALIFIED_DIRECT_PASS',handle=packet['handle'],commit=metadata['commit'],deadline=deadline.record,
        reservation=allowance(packet),http_payload=measured,final=final,owner=owner,A13='OPEN',A14='OPEN',whole_delivery='OPEN')
    deadline.remaining(cleanup=True);save(evidence/'result.json',receipt)
    return dict(outcome=receipt['outcome'],handle=packet['handle'],path=final['path'],A13='OPEN',A14='OPEN')


def main(argv=None):
    deadline=Deadline()  # Before input, preflight, metadata or any child birth.
    argv=sys.argv[1:] if argv is None else argv
    if '--live' not in argv or os.environ.get('DISABLE_NETWORK')=='1' or os.environ.get('HERMES_DOWNLOADS_DISABLE_NETWORK')=='1':
        print(json.dumps(dict(outcome='SKIP',reason='explicit opt-in required; network disable wins; not A11 PASS')));return 0
    parser=Parser(add_help=False,allow_abbrev=False)
    parser.add_argument('--live',action='store_true');parser.add_argument('--input-file',type=Path)
    parser.add_argument('--evidence',type=Path);parser.add_argument('--expected-commit')
    try:
        args=parser.parse_args(argv)
        if args.input_file is None or args.evidence is None:raise Refused('input_required')
        packet=packet_read(args.input_file);deadline.remaining()
    except (Exception,SystemExit):
        print(json.dumps(dict(outcome='REFUSED_INPUT_OR_BUDGET')));return 1
    previous=signal.getsignal(signal.SIGTERM)
    def interrupted(*args):raise Refused('owned_runner_interrupted')
    signal.signal(signal.SIGTERM,interrupted)
    try:
        try:
            metadata=preflight(args.expected_commit,packet,deadline)
            result=run_live(packet,args.evidence,metadata,deadline)
        except BaseException:
            print(json.dumps(dict(outcome='DIRECT_ACCEPTANCE_FAILED',handle=packet['handle'],A11='OPEN')));return 1
        print(json.dumps(result));return 0
    finally:signal.signal(signal.SIGTERM,previous)


if __name__=='__main__':raise SystemExit(main())
