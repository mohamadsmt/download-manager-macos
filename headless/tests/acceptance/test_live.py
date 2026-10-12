"""Offline runner contracts; the separate real-source case skips by default."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import pytest

SCRIPT=Path(__file__).resolve().parents[2]/'scripts/live-acceptance.py'

@pytest.fixture
def live():
    spec=importlib.util.spec_from_file_location('live_acceptance_contract',SCRIPT)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def packet():
    return dict(url='https://example.test/authorized',expected_sha256='a'*64,length=538364,handle='synthetic-case',
        qualification=dict(aria2_sha256='b'*64,trusted_fixed_representation=True,max_response_bytes=538364))


@pytest.mark.parametrize('disabled',[None,'DISABLE_NETWORK','HERMES_DOWNLOADS_DISABLE_NETWORK'])
def test_default_and_disabled_skip_before_input_or_effects(live,monkeypatch,tmp_path,capsys,disabled):
    def denied(*args,**kwargs):pytest.fail('skip crossed input/child/network boundary')
    monkeypatch.setattr(Path,'open',denied)
    monkeypatch.setattr(subprocess,'Popen',denied)
    monkeypatch.delenv('DISABLE_NETWORK',raising=False)
    monkeypatch.delenv('HERMES_DOWNLOADS_DISABLE_NETWORK',raising=False)
    if disabled:monkeypatch.setenv(disabled,'1')
    monkeypatch.setattr(live,'packet_read',denied)
    monkeypatch.setattr(live,'preflight',denied)
    monkeypatch.setattr(live,'run_live',denied)
    args=['--live','--input-file',str(tmp_path/'missing')] if disabled else ['--input-file',str(tmp_path/'missing')]
    assert live.main(args)==0
    assert 'SKIP' in capsys.readouterr().out


@pytest.mark.parametrize('damage',['missing','public','symlink','malformed','length','budget','unknown'])
def test_private_input_and_budget_refuse_before_effects(live,monkeypatch,tmp_path,capsys,damage):
    monkeypatch.delenv('HERMES_DOWNLOADS_DISABLE_NETWORK',raising=False)
    monkeypatch.delenv('DISABLE_NETWORK',raising=False)
    root=tmp_path/'packet-dir';root.mkdir(mode=0o700)
    path=root/'input.json';value=packet()
    if damage=='length':value['length']=True
    if damage=='budget':value['length']=8*1024*1024;value['qualification']['max_response_bytes']=value['length']
    if damage=='unknown':value['qualification']['trusted_fixed_representation']=False
    if damage!='missing':path.write_text('{bad' if damage=='malformed' else json.dumps(value));path.chmod(0o644 if damage=='public' else 0o600)
    if damage=='symlink':target=root/'target';path.rename(target);path.symlink_to(target)
    def denied(*args,**kwargs):pytest.fail('invalid input spawned a child')
    monkeypatch.setattr(subprocess,'Popen',denied)
    evidence=root/'evidence'
    assert live.main(['--live','--input-file',str(path),'--evidence',str(evidence),'--expected-commit','c'*40])==1
    assert 'REFUSED_INPUT_OR_BUDGET' in capsys.readouterr().out
    assert not evidence.exists()


@pytest.mark.parametrize('bad',['none','options','continuation','second','seventh'])
def test_reservation_precedes_original_paused_admission(live,bad):
    events=[];options={}
    class Controller:
        def _rpc(self,name,args):
            events.append(name)
            if name=='aria2.addUri':options.update(args[1]);return '0'*16
            if name=='aria2.changeOption':options.update(args[1]);return 'OK'
            if name=='aria2.getOption':return options|({'split':'4'} if bad=='options' else {})
            if name=='aria2.getGlobalOption':return {'piece-length':'1048576'}
        def original(self,**kwargs):
            events.append('original-paused');self._rpc('aria2.addUri',[[packet()['url']],{'pause':'true'}]);return type('Paused',(),dict(gid='0'*16,status='paused'))()
    controller=Controller()
    guard=live.AdmissionGuard(packet(),lambda kind,**values:events.append(kind),live.Deadline())
    kwargs=dict(job_id='live-job',saved_resume=bad=='continuation')
    if bad=='none':guard.reserved=0
    if bad=='second':guard.admissions=1
    if bad=='seventh':guard.admissions=6
    with pytest.raises(live.Refused):guard.add(controller,controller.original,**kwargs)
    if bad=='options':
        assert events.index('budget-admission')<events.index('original-paused')<events.index('aria2.changeOption')
    else:assert 'original-paused' not in events


@pytest.mark.parametrize('phase',['birth','late'])
def test_original_owned_worker_closes_on_birth_or_late_failure(live,monkeypatch,tmp_path,phase):
    children=[];original=subprocess.Popen
    def create(*args,**kwargs):
        child=original(*args,**kwargs);children.append(child);return child
    monkeypatch.setattr(subprocess,'Popen',create)
    if phase=='birth':
        def fail(*args):raise RuntimeError('synthetic birth logging failure')
        monkeypatch.setattr(live,'birth',fail)
    evidence=tmp_path/'evidence';evidence.mkdir(mode=0o700)
    code='import signal,time,sys;signal.signal(signal.SIGTERM,lambda *a:sys.exit(0));time.sleep(30)'
    try:
        with pytest.raises(RuntimeError):
            with live.owned_worker([sys.executable,'-I','-B','-c',code],dict(os.environ),evidence,live.Deadline()):
                time.sleep(.1);raise RuntimeError('synthetic late failure')
        assert len(children)==1
        child=children[0]
        assert child.returncode is not None
        with pytest.raises(ChildProcessError):os.waitpid(child.pid,os.WNOHANG)
        with pytest.raises(ProcessLookupError):os.killpg(child.pid,0)
        receipt=json.loads((evidence/'worker-closure.json').read_text())
        assert receipt['original_wait'] and receipt['echild'] and receipt['group_absent']
    finally:
        # RED implementations must not leak the test's originally retained child.
        for child in children:
            if child.poll() is None:
                os.killpg(child.pid,signal.SIGTERM);child.wait(timeout=5)
                (evidence/'independent-failed-phase-cleanup.json').write_text(json.dumps(dict(pid=child.pid,original_wait=child.returncode,echild=live.echild(child.pid),group_absent=live.absent(child.pid))))



def test_real_authorized_direct_source(live,tmp_path):
    if os.environ.get('DISABLE_NETWORK')=='1' or os.environ.get('HERMES_DOWNLOADS_DISABLE_NETWORK')=='1' or os.environ.get('HERMES_DOWNLOADS_LIVE')!='1':
        pytest.skip('T23 real source requires explicit opt-in; disabled/default SKIP is not A11 PASS')
    required=['HERMES_DOWNLOADS_LIVE_INPUT','HERMES_DOWNLOADS_LIVE_EVIDENCE','HERMES_DOWNLOADS_LIVE_COMMIT']
    assert all(os.environ.get(name) for name in required),'private input, fresh evidence and frozen commit required'
    env=dict(os.environ);env.pop('PYTHONPATH',None);env.pop('PYTHONHOME',None)
    caller=tmp_path/'live-caller';caller.mkdir(mode=0o700)
    clock=live.Deadline()
    argv=[sys.executable,'-I','-B',str(SCRIPT),'--live','--input-file',env[required[0]],
        '--evidence',env[required[1]],'--expected-commit',env[required[2]]]
    with live.owned_worker(argv,env,caller,clock) as child:
        assert child.wait(timeout=clock.remaining(cleanup=True))==0
    assert 'QUALIFIED_DIRECT_PASS' in (caller/'worker.log').read_text()



@pytest.mark.parametrize('bad',['redirect','chunked','length','extra','truncated'])
def test_response_evidence_never_promotes_unknown_chain(live,bad):
    heads=[dict(kind='http-head',cuid='1',frame='Requesting',lines=['GET /fixture HTTP/1.1','Host: example.test','X-Hermes-Attempt: '+'a'*64]),
        dict(kind='http-head',cuid='1',frame='Response received',lines=['HTTP/1.1 200 OK','Content-Length: 538364'])]
    if bad=='redirect':heads[1]['lines'][0]='HTTP/1.1 302 Found'
    if bad=='chunked':heads[1]['lines'].append('Transfer-Encoding: chunked')
    if bad=='length':heads[1]['lines'][1]='Content-Length: 999999'
    if bad=='extra':heads.append(heads[1])
    if bad=='truncated':heads.pop()
    with pytest.raises(live.Refused):live.qualify_heads(heads,538364)


def test_aggregate_deadline_is_not_renewed(live,monkeypatch):
    clock=live.Deadline();record=dict(clock.record)
    monkeypatch.setattr(time,'monotonic',lambda:record['monotonic_start']+841)
    with pytest.raises(live.Refused):clock.remaining()
    assert clock.remaining(cleanup=True)<=59
    inherited=live.Deadline(record)
    with pytest.raises(live.Refused):inherited.remaining()


@pytest.mark.parametrize('failure',[False,True,'engine-birth'])
def test_actual_offline_sdk_stock_carrier_and_original_closures(live,monkeypatch,failure):
    """Actual installed pipeline with a synthetic origin; never a live verdict."""
    import hashlib
    from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
    import tempfile
    import threading
    payload=b'x'*538364;requests=[]
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(dict(range=self.headers.get('Range'),attempt=self.headers.get('X-Hermes-Attempt')))
            self.send_response(200);self.send_header('Content-Length',str(len(payload)));self.end_headers()
            self.wfile.write(payload)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever);thread.start()
    try:
        url=f'http://127.0.0.1:{server.server_port}/fixture'
        value=packet();value['url']=url;value['expected_sha256']=hashlib.sha256(payload).hexdigest()
        consoles={name:str(Path(sys.executable).parent/name) for name in ('hermes-downloads','hermes-downloads-mcp','hermes-downloads-worker')}
        metadata=dict(commit='synthetic-offline-only',vendor='/opt/homebrew/bin/aria2c',consoles=consoles,console_hashes={k:live.digest(v) for k,v in consoles.items()})
        def command(config):
            code="""import runpy,sys,json
from pathlib import Path
from hermes_downloads import network,store,worker
p=json.loads(Path(sys.argv[2]).read_text())
grant=network.LocalOriginGrant.for_url(p['packet']['url'])
validate=network.validate_source_url
def fixture(value):return validate(value,local_origin_grant=grant)
store.validate_source_url=worker.validate_source_url=fixture
ns=runpy.run_path(sys.argv[1])
if sys.argv[3]=='engine-birth':
 def denied_birth(pid,*args):raise RuntimeError('synthetic engine birth receipt failure')
 ns['worker_carrier'].__globals__['birth']=denied_birth
ns['worker_carrier'](sys.argv[2])
"""
            return [sys.executable,'-I','-B','-c',code,str(SCRIPT),str(config),str(failure)]
        monkeypatch.setattr(live,'worker_command',command)
        if failure is True:
            def failed_final(*args):raise RuntimeError('synthetic late descriptor failure')
            monkeypatch.setattr(live,'final_verify',failed_final)
        with tempfile.TemporaryDirectory(prefix='t23-',dir='/private/tmp') as directory:
            parent=Path(directory);parent.chmod(0o700);evidence=parent/'e'
            try:
                if failure=='engine-birth':
                    with pytest.raises(BaseExceptionGroup):live.run_live(value,evidence,metadata,live.Deadline())
                elif failure is True:
                    with pytest.raises(RuntimeError,match='synthetic late descriptor failure'):live.run_live(value,evidence,metadata,live.Deadline())
                else:
                    result=live.run_live(value,evidence,metadata,live.Deadline())
                    assert result['outcome']=='QUALIFIED_DIRECT_PASS'
                    assert result['A13']==result['A14']=='OPEN'
                    receipt=json.loads((evidence/'result.json').read_text())
                    assert receipt['http_payload']['qualified_received_http_payload']==len(payload)
                    assert receipt['reservation']==6*21*(len(payload)+16384)
                    assert Path(result['path']).read_bytes()==payload
                closed=json.loads((evidence/'worker-closure.json').read_text())
                assert closed['returncode']==0 and closed['original_wait'] and closed['echild'] and closed['group_absent']
                effects=[json.loads(line) for line in (evidence/'carrier.jsonl').read_text().splitlines()]
                kinds=[row['kind'] for row in effects]
                if failure=='engine-birth':
                    assert 'budget-admission' not in kinds and 'http-head' not in kinds and requests==[]
                    assert any(row['kind']=='engine-birth-failed' and row['contained'] for row in effects)
                else:assert kinds.index('budget-admission')<kinds.index('paused-options')<kinds.index('http-head')
                engines=[row for row in effects if row['kind']=='engine-closure']
                assert len(engines)==1 and engines[0]['waits'] and engines[0]['echild'] and engines[0]['group_absent']
                if failure!='engine-birth':
                    assert any(row['kind']=='original-thread-join' and row['name']=='direct-stock-reader' and not row['alive'] for row in effects)
                    assert len(requests)==1 and requests[0]['range'] is None
            finally:
                # Keep synthetic receipts under the author-provided private evidence root.
                destination=os.environ.get('T26_RESUME_RUN')
                if destination and evidence.exists():
                    import shutil
                    shutil.copytree(evidence,Path(destination)/('t23-offline-birth' if failure=='engine-birth' else 't23-offline-late' if failure else 't23-offline-success'))
    finally:
        server.shutdown();server.server_close();thread.join(5)
        assert not thread.is_alive()


def test_isolated_physical_metadata_and_frozen_git_gate(live,tmp_path):
    """No network or worker: exercise the real isolated preflight before consent."""
    expected=os.environ.get('T23_EXPECTED_FROZEN_COMMIT','0'*40)
    code="""import json,runpy,sys
ns=runpy.run_path(sys.argv[1]);value=dict(url='https://example.test/synthetic',expected_sha256='a'*64,length=538364,handle='synthetic-metadata',qualification=dict(aria2_sha256=ns['digest']('/opt/homebrew/bin/aria2c'),trusted_fixed_representation=True,max_response_bytes=538364))
try:
 record=ns['preflight'](sys.argv[2],value,ns['Deadline']())
except ns['Refused'] as error:
 assert sys.argv[2]=='0'*40 and str(error)=='clean_frozen_commit_required',str(error)
 print('FROZEN_METADATA_PENDING')
else:
 assert len(record['modules'])==16 and len(record['versions'])==43 and len(record['entries'])==3
 assert all(v['source']==v['installed'] and v['source_devino']!=v['installed_devino'] for v in record['modules'].values())
 assert all(c['original_wait'] and c['echild'] and c['group_absent'] and c['code']==0 for c in record['metadata_children'])
 print('FROZEN_PHYSICAL_METADATA_VERIFIED')
"""
    evidence=tmp_path/'metadata';evidence.mkdir(mode=0o700)
    env=dict(os.environ);env.pop('PYTHONPATH',None);env.pop('PYTHONHOME',None)
    with live.owned_worker([sys.executable,'-I','-B','-c',code,str(SCRIPT),expected],env,evidence,live.Deadline()) as child:
        assert child.wait(timeout=20)==0,(evidence/'worker.log').read_text()
    marker='FROZEN_METADATA_PENDING' if expected=='0'*40 else 'FROZEN_PHYSICAL_METADATA_VERIFIED'
    assert marker in (evidence/'worker.log').read_text()


def test_owned_evidence_is_private_under_permissive_umask(live,tmp_path):
    evidence=tmp_path/'private-evidence';evidence.mkdir(mode=0o700)
    previous=os.umask(0o022)
    try:
        with live.owned_worker([sys.executable,'-I','-B','-c','pass'],dict(os.environ),evidence,live.Deadline()) as child:
            assert child.wait(timeout=5)==0
    finally:os.umask(previous)
    assert {path.name:path.stat().st_mode&0o777 for path in evidence.iterdir()}=={
        'worker.log':0o600,'worker-birth.json':0o600,'worker-closure.json':0o600}



def test_private_fifo_refused_promptly_by_original_child(live,tmp_path):
    import shutil
    root=tmp_path/'fifo';root.mkdir(mode=0o700)
    fifo=root/'input';os.mkfifo(fifo,mode=0o600)
    evidence=root/'e';evidence.mkdir(mode=0o700)
    code="""import runpy,sys
ns=runpy.run_path(sys.argv[1])
try:ns['packet_read'](sys.argv[2])
except ns['Refused'] as error:
 assert str(error)=='private_input'
 print('FIFO_REFUSED',flush=True)
else:raise AssertionError('FIFO accepted')
"""
    try:
        with live.owned_worker([sys.executable,'-I','-B','-c',code,str(SCRIPT),str(fifo)],dict(os.environ),evidence,live.Deadline()) as child:
            assert child.wait(timeout=1)==0
        assert 'FIFO_REFUSED' in (evidence/'worker.log').read_text()
    finally:
        receipt=json.loads((evidence/'worker-closure.json').read_text())
        assert receipt['original_wait'] and receipt['echild'] and receipt['group_absent'] and not receipt['forced']
        destination=os.environ.get('T26_RESUME_RUN')
        if destination:shutil.copytree(evidence,Path(destination)/'fifo-closure')


def test_stalled_sdk_flow_times_out_and_closes_owned_contexts(live,monkeypatch):
    import asyncio
    from contextlib import asynccontextmanager
    import shutil
    import tempfile
    async def stalled(state,home,packet,evidence,metadata,deadline,worker):
        @asynccontextmanager
        async def owned_sdk_context():
            try:yield
            finally:live.save(evidence/'sdk-stall-closure.json',dict(closed=True))
        async with owned_sdk_context():
            await asyncio.sleep(2)
    def command(config):
        code="""import json,signal,sys,time
from pathlib import Path
signal.signal(signal.SIGTERM,lambda *args:sys.exit(0))
p=json.loads(Path(sys.argv[1]).read_text());Path(p['state'],'worker.sock').touch()
time.sleep(30)
"""
        return [sys.executable,'-I','-B','-c',code,str(config)]
    class ShortDeadline(live.Deadline):
        def remaining(self,*,cleanup=False):
            return min(super().remaining(cleanup=cleanup),60 if cleanup else .2)
    monkeypatch.setattr(live,'sdk_flow',stalled)
    monkeypatch.setattr(live,'worker_command',command)
    def denied(*args):pytest.fail('stalled SDK flow crossed final verification')
    monkeypatch.setattr(live,'final_verify',denied)
    with tempfile.TemporaryDirectory(prefix='t23-stall-',dir='/private/tmp') as directory:
        parent=Path(directory);parent.chmod(0o700);evidence=parent/'e'
        started=time.monotonic()
        try:
            with pytest.raises(TimeoutError):live.run_live(packet(),evidence,dict(commit='synthetic-stall-only'),ShortDeadline())
            assert time.monotonic()-started<1.5
        finally:
            assert json.loads((evidence/'sdk-stall-closure.json').read_text())==dict(closed=True)
            receipt=json.loads((evidence/'worker-closure.json').read_text())
            assert receipt['returncode']==0 and receipt['original_wait'] and receipt['echild'] and receipt['group_absent'] and not receipt['forced']
            destination=os.environ.get('T26_RESUME_RUN')
            if destination:shutil.copytree(evidence,Path(destination)/'sdk-stall-closure')
