"""Private causal scheduler: real typed creation and transactional admission."""
from contextlib import closing
from datetime import UTC, datetime
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time

import pytest
from hermes_downloads import ipc, worker
from hermes_downloads.store import SQLiteStore, _DirectDispatchPlan
from test_add_batch import canonical, entry, envelope, exchange
from test_target_authority import authorize, select_jobs, target
from test_direct_publication import (_helpers, _publication_worker, _close_fixture_process,
    _os_argv, _append_record)


def _late_fixture_ledger(evidence, results, release, mode):
    """An actual owned birth whose final evidence can arrive after teardown starts."""
    import hashlib
    from hermes_downloads import processes
    os.setsid()
    argv = _os_argv(os.getpid())
    identity = processes.capture_process_birth(processes.EngineIdentity(
        os.getpid(), os.getpid(), time.monotonic_ns(),
        hashlib.sha256(json.dumps(argv).encode()).hexdigest()))
    assert identity is not None
    ledger = Path(evidence) / f'fixture-{os.getpid()}.jsonl'
    if mode == 'open-empty':
        ledger.touch(mode=0o600)
    results.put(identity.to_record())
    assert release.wait(6)
    time.sleep(.2)
    if mode != 'missing':
        _append_record(ledger, 'worker-birth', identity=identity.to_record(),
            os_argv=argv, argv_source='KERN_PROCARGS2')


@pytest.mark.parametrize('mode,crash', [('delayed', False), ('open-empty', False),
    ('missing', False), ('missing', True)])
def test_fixture_teardown_joins_original_child_before_final_ledger(tmp_path, mode, crash):
    from hermes_downloads import processes
    context = multiprocessing.get_context('spawn')
    results, release = context.Queue(), context.Event()
    process = context.Process(target=_late_fixture_ledger,
        args=(str(tmp_path), results, release, mode))
    process.start()
    identity = None
    try:
        identity = processes.ProcessBirthIdentity.from_record(results.get(timeout=6))
        assert processes.is_current_process_birth(identity)
        ledger = tmp_path / f'fixture-{process.pid}.jsonl'
        if mode == 'open-empty':
            assert ledger.read_bytes() == b''
        else:
            assert not ledger.exists()
        release.set()
        if mode == 'missing':
            with pytest.raises(FileNotFoundError):
                _close_fixture_process(process, tmp_path, crash=crash)
        else:
            _close_fixture_process(process, tmp_path)
            rows = [json.loads(line) for line in (tmp_path / 'parent-closure.jsonl').read_text().splitlines()]
            reaped = next(row for row in rows if row['kind'] == 'worker-reaped')
            assert reaped['identity'] == identity.to_record()
            assert reaped['waitable_child'] and reaped['exitcode'] == 0
        assert process.exitcode == 0, 'ledger validation escaped before original child join'
    finally:
        release.set()
        try:
            process.join(8)
            assert not process.is_alive() and process.exitcode == 0
            assert identity is not None and processes.reconcile_process_birth(identity) == 'absent'
            with pytest.raises(ChildProcessError):
                os.waitpid(process.pid, os.WNOHANG)
            _append_record(tmp_path / 'regression-closure.jsonl', 'original-child-joined',
                identity=identity.to_record(), exitcode=process.exitcode, waitpid='ECHILD',
                reconciliation='absent', independent_finally=True)
        finally:
            try:
                results.close()
            finally:
                results.join_thread()


@pytest.mark.parametrize('batch', [False, True])
def test_private_queued_admission_commits_cause_before_engine(tmp_path, batch):
    with closing(SQLiteStore(tmp_path / 'scheduler.db')) as store:
        store.recover_cold_start()
        if batch:
            store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
            job = 'batch-job-0'
        else:
            job = 'single-job'
            worker._job_add_from_store(store, ipc.JobAddCommand(job, 'single-add',
                'https://example.test/body', 0, 0, 'Other', 'body.bin', 'body.bin'))
        authorize(store, selector=select_jobs([job]))
        store.apply_queue_gate(gate='running', request_id='running', payload_digest='a' * 64,
            expected_revision=store.queue_gate_snapshot()[1])
        before = tuple(store._connection.iterdump())
        prepare = getattr(store, '_prepare_target_dispatch', None)
        assert callable(prepare), 'authorized queued targets have no private admission path'
        assert prepare(expected_worker_epoch=1, owner_slot_ready=False, now=datetime.now(UTC)) is None
        assert tuple(store._connection.iterdump()) == before
        plan = prepare(expected_worker_epoch=1, owner_slot_ready=True, now=datetime.now(UTC))
        assert type(plan) is _DirectDispatchPlan
        assert store.get_job(job).state == 'resolving'
        receipt = store._connection.execute('SELECT * FROM direct_dispatch_commands').fetchone()
        cause = store._connection.execute('SELECT * FROM target_dispatch_causes').fetchone()
        head = store._connection.execute('SELECT * FROM job_authorization_heads').fetchone()
        assert receipt['producer_kind'] == 'target_body' and receipt['status'] == 'pending'
        assert cause['request_id'] == plan.request_id and cause['kind'] == 'body'
        assert cause['admission_serial'] == head['last_admission_serial'] == 1
        assert cause['resolving_audit_id'] == head['current_audit_id']
        assert store.get_direct_engine_record() is None
        assert store.get_direct_engine_activation_fence() is None
        assert store._connection.execute('PRAGMA foreign_key_check').fetchall() == []
        assert prepare(expected_worker_epoch=1, owner_slot_ready=True, now=datetime.now(UTC)) is None
        downloading = store.advance_direct_dispatch_to_downloading(plan)
        assert store.finish_direct_dispatch(downloading).status == 'started'
        assert store._capture_target_head(store._connection, job).current_state == 'downloading'
        if batch:
            assert store.get_batch_creation_intent(job).original_job.authorized is False


@pytest.mark.parametrize('damage', ['missing','legacy','marker','digest','serial','predecessor','resolving'])
def test_static_private_cause_corruption_never_authorizes_effect(tmp_path, damage):
    with closing(SQLiteStore(tmp_path / 'corrupt.db')) as store:
        store.recover_cold_start()
        store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
        authorize(store)
        store.apply_queue_gate(gate='running',request_id='running',payload_digest='a'*64,expected_revision=1)
        plan = store._prepare_target_dispatch(expected_worker_epoch=1,owner_slot_ready=True,now=datetime.now(UTC))
        connection = store._connection
        if damage=='missing': connection.execute('DELETE FROM target_dispatch_causes')
        elif damage in {'legacy','marker'}:
            connection.execute('UPDATE direct_dispatch_commands SET producer_kind=?',
                ('legacy' if damage=='legacy' else 'target_publication',))
        elif damage=='digest': connection.execute('UPDATE direct_dispatch_commands SET payload_digest=?', ('0'*64,))
        elif damage=='serial': connection.execute('UPDATE target_dispatch_causes SET admission_serial=2')
        elif damage=='predecessor': connection.execute('UPDATE target_dispatch_causes SET predecessor_audit_id=resolving_audit_id')
        else: connection.execute('UPDATE target_dispatch_causes SET resolving_audit_id=predecessor_audit_id')
        before = tuple(connection.iterdump())
        with pytest.raises(ValueError): store._read_target_dispatch_cause(connection,plan.request_id,{})
        with pytest.raises(ValueError): store.advance_direct_dispatch_to_downloading(plan)
        assert tuple(connection.iterdump()) == before


def test_queued_again_after_contained_body_is_not_an_implicit_retry(tmp_path):
    with closing(SQLiteStore(tmp_path/'no-retry.db')) as store:
        store.recover_cold_start();store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope()))
        authorize(store)
        store.apply_queue_gate(gate='running',request_id='run',payload_digest='a'*64,expected_revision=1)
        plan=store._prepare_target_dispatch(expected_worker_epoch=1,owner_slot_ready=True,now=datetime.now(UTC))
        assert store.abort_direct_dispatch(plan).state=='paused'
        assert authorize(store,'resume','resume')['results'][0]['outcome']=='existing_authority'
        assert store.get_job('batch-job-0').state=='queued'
        before=tuple(store._connection.iterdump())
        assert store._prepare_target_dispatch(expected_worker_epoch=1,owner_slot_ready=True,now=datetime.now(UTC)) is None
        assert tuple(store._connection.iterdump())==before
        assert store._connection.execute('SELECT count(*) FROM target_dispatch_causes').fetchone()[0]==1


@pytest.mark.parametrize('hold', ['global','manual','collection','cold','epoch'])
def test_independent_holds_and_cold_cannot_create_private_receipt(tmp_path, hold):
    with closing(SQLiteStore(tmp_path / 'holds.db')) as store:
        store.recover_cold_start()
        store.apply_add_batch(ipc.AddBatchCommand.from_record(envelope(collection='held')))
        authorize(store)
        if hold != 'global':
            store.apply_queue_gate(gate='running',request_id='running',payload_digest='a'*64,expected_revision=1)
        if hold=='manual':
            current=store.get_job('batch-job-0')
            command=ipc.JobControlCommand(current.job,'pause','manual',current.revision)
            store.apply_job_control(job_id=current.job,action='pause',request_id='manual',
                payload_digest=command.payload_digest,expected_revision=current.revision)
        if hold=='collection': store.set_collection_hold(store.get_materialized_job('batch-job-0').queue_collection_id,held=True)
        if hold=='cold': store.recover_cold_start()
        before=tuple(store._connection.iterdump())
        assert store._prepare_target_dispatch(expected_worker_epoch=2 if hold=='epoch' else store.worker_epoch(),
            owner_slot_ready=True,now=datetime.now(UTC)) is None
        assert tuple(store._connection.iterdump())==before
        assert store._connection.execute('SELECT count(*) FROM target_dispatch_causes').fetchone()[0]==0


def _scheduler_worker(state, ready, shutdown, stopped, results, origin, evidence, fail_start=False,
    barrier=None, entered=None, release=None, recovery_entered=None, recovery_release=None,
    private_start=True, cleanup_mode=None, cleanup_entered=None, cleanup_release=None,
    late_entry=None, fail_close=False):
    """Record original objects/results while using the real accepted engine fixture."""
    from hermes_downloads import direct, network, paths, store as store_module
    grant = network.LocalOriginGrant.for_url(origin)
    validate = store_module.validate_source_url
    store_module.validate_source_url = lambda value: validate(value,local_origin_grant=grant)
    ledger = Path(evidence) / 'scheduler-effects.jsonl'
    def record(kind, **values):
        with ledger.open('a') as stream:
            os.chmod(ledger,0o600)
            stream.write(json.dumps(dict(kind=kind,monotonic=time.monotonic(),**values))+'\n')
            stream.flush()
    prepare=SQLiteStore._prepare_target_dispatch
    def observed_prepare(self,**kwargs):
        try: result=prepare(self,**kwargs)
        except BaseException as error:
            import traceback
            record('prepare-error',error=str(error),traceback=traceback.format_exc())
            raise
        if result is not None: record('prepared-operation',type=type(result).__name__,request_id=getattr(result,'request_id',None))
        return result
    SQLiteStore._prepare_target_dispatch=observed_prepare
    start = direct.DirectAria2Controller.start
    def causal_start(self):
        with closing(SQLiteStore(Path(state)/'state.db')) as store:
            rows = store._connection.execute("SELECT request_id FROM direct_dispatch_commands WHERE producer_kind='target_body' AND status='pending'").fetchall()
            if rows:
                assert private_start is not False
                assert len(rows)==1
                cause=store._read_target_dispatch_cause(store._connection,rows[0][0],{})
                assert store.get_job(cause['job_id']).state=='resolving'
                record('causal-engine-start',cause=cause)
            else:
                assert private_start is not True
                assert rows==[]
                record('explicit-legacy-engine-start')
        if fail_start: raise RuntimeError('committed engine-start fault')
        result=start(self)
        if late_entry is not None:
            with closing(SQLiteStore(Path(state)/'state.db')) as store:
                job=store.get_materialized_job(cause['job_id'])
            destination=paths.rehydrate_destination(category=job.category,collection=job.destination_collection,
                partial_filename=job.partial_filename,selected_final_filename=job.selected_final_filename,job_id=job.job_id)
            destination.incomplete_dir.mkdir(parents=True,mode=0o700,exist_ok=True)
            entry_path=destination.partial_path
            if late_entry=='control': entry_path=entry_path.with_name(entry_path.name+'.aria2')
            entry_path.write_bytes(b'late unowned bytes')
            record('late-unowned-entry',path=str(entry_path),bytes=entry_path.read_bytes().hex())
        return result
    direct.DirectAria2Controller.start=causal_start
    if fail_close:
        close=direct.DirectAria2Controller.close
        def held_close(self):
            if not release.is_set():
                record('actual-owned-close-refused',pid=self._process.pid)
                raise RuntimeError('finite owned containment failure')
            return close(self)
        direct.DirectAria2Controller.close=held_close
    join=threading.Thread.join
    def observed_join(self,*args,**kwargs):
        result=join(self,*args,**kwargs)
        if self.name.startswith('direct-'):
            record('observer-join',name=self.name,ident=self.ident,alive=self.is_alive())
        return result
    threading.Thread.join=observed_join
    server=worker.HealthServer
    def observed_server(*args,**handlers):
        queue_gate=handlers['queue_gate']
        def gate(command):
            try: return queue_gate(command)
            except BaseException as error:
                record('queue-control-error',error=str(error),type=type(error).__name__)
                raise
        handlers['queue_gate']=gate
        return server(*args,**handlers)
    worker.HealthServer=observed_server
    link=paths.os.link
    def observed_link(*args,**kwargs):
        with sqlite3.connect(Path(state)/'state.db') as connection:
            pending=connection.execute("SELECT request_id FROM direct_dispatch_commands WHERE producer_kind='target_publication' AND status='pending'").fetchone()
        assert pending is None, 'existing-only recovery attempted a new link'
        result=link(*args,**kwargs)
        record('publication-link',result=result)
        return result
    paths.os.link=observed_link
    if cleanup_mode is not None:
        def cleanup_hold(kind):
            record('held-private-cleanup',operation=kind)
            cleanup_entered.set()
            assert cleanup_release.wait(12), 'finite private cleanup barrier expired'
        unlink=paths.os.unlink
        def observed_unlink(name,*args,**kwargs):
            with sqlite3.connect(Path(state)/'state.db') as connection:
                rows=connection.execute("SELECT job_id FROM direct_cleanup_claims WHERE phase='pending'").fetchall()
            descriptor=kwargs.get('dir_fd')
            for row in rows:
                partial=Path.home()/f'Downloads/Hermes/.incomplete/{row[0]}/cleanup.bin'
                if (descriptor is not None and name==partial.name
                    and (os.fstat(descriptor).st_dev,os.fstat(descriptor).st_ino)==(
                        partial.parent.stat().st_dev,partial.parent.stat().st_ino)):
                    if cleanup_mode=='unlink': cleanup_hold('unlink')
            return unlink(name,*args,**kwargs)
        paths.os.unlink=observed_unlink
        hashed=paths._hash_publication_payload
        def observed_cleanup_hash(descriptor,expected,**kwargs):
            with sqlite3.connect(Path(state)/'state.db') as connection:
                pending=connection.execute("SELECT 1 FROM direct_cleanup_claims WHERE phase='pending'").fetchone()
            if cleanup_mode=='hash' and pending is not None and expected[4]==1:
                cleanup_hold('hash')
            return hashed(descriptor,expected,**kwargs)
        paths._hash_publication_payload=observed_cleanup_hash
    if recovery_entered is not None:
        original_hash=paths._hash_publication_payload
        def observed_hash(*args,**kwargs):
            with sqlite3.connect(Path(state)/'state.db') as connection:
                pending=connection.execute("SELECT request_id FROM direct_dispatch_commands WHERE producer_kind='target_publication' AND status='pending'").fetchone()
            if pending is not None:
                record('recovery-hash-enter',request_id=pending[0])
                recovery_entered.set()
                cancelled=kwargs.get('cancelled',lambda:False)
                while not recovery_release.is_set() and not cancelled():
                    recovery_release.wait(.01)
                record('recovery-hash-released',request_id=pending[0],cancelled=cancelled())
            return original_hash(*args,**kwargs)
        paths._hash_publication_payload=observed_hash
    context=multiprocessing.get_context('spawn')
    _publication_worker(state,str(Path(state)/'worker.sock'),ready,shutdown,stopped,results,
        origin,barrier,entered or context.Event(),release or context.Event(),evidence)


@pytest.mark.parametrize('batch', [False, True])
@pytest.mark.parametrize('entry_kind', ['partial', 'empty-partial', 'control', 'empty-control',
    'partial-symlink', 'control-dangling'])
def test_first_private_body_preserves_existing_bytes_without_engine(tmp_path, monkeypatch, batch, entry_kind):
    evidence=tmp_path/'effects';evidence.mkdir(mode=0o700)
    home=tmp_path/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home))
    (home/'Downloads/Hermes').mkdir(parents=True,mode=0o700)
    with tempfile.TemporaryDirectory(dir='/private/tmp',prefix='causal-bytes-') as directory:
        state=Path(directory);state.chmod(0o700)
        context=multiprocessing.get_context('spawn')
        ready,shutdown,stopped=[context.Event() for _ in range(3)]
        results=context.Queue()
        with _helpers._origin_type()(payload_size=1024) as origin:
            origin_thread=origin._thread
            process=context.Process(target=_scheduler_worker,args=(str(state),ready,shutdown,stopped,
                results,origin.url(),str(evidence)))
            process.start()
            try:
                assert ready.wait(6)
                sock=state/'worker.sock';job='batch-job-0' if batch else 'single-job'
                if batch:
                    assert exchange(state,canonical(envelope([entry(source_url=origin.url(),
                        partial_filename='owned.bin',selected_final_filename='owned.bin')])))['status']=='applied'
                else:
                    ipc.add_job(sock,job=job,request_id='typed-single',source_url=origin.url(),
                        priority=0,order_key=0,category='Other',partial_filename='owned.bin',
                        selected_final_filename='owned.bin')
                partial=home/f'Downloads/Hermes/.incomplete/{job}/owned.bin'
                partial.parent.mkdir(parents=True,mode=0o700)
                existing=partial.with_name(partial.name+'.aria2') if 'control' in entry_kind else partial
                target_file=tmp_path/'old-payload';target_file.write_bytes(b'preserved original bytes')
                if entry_kind=='partial-symlink': existing.symlink_to(target_file)
                elif entry_kind=='control-dangling': existing.symlink_to(tmp_path/'absent')
                else: existing.write_bytes(b'' if entry_kind.startswith('empty-') else b'preserved original bytes')
                old_bytes=None if existing.is_symlink() else existing.read_bytes();old_stat=existing.lstat()
                ipc.target_authorize(sock,ipc.TargetAuthorizeCommand.from_record(target(selector=select_jobs([job]))))
                assert origin.ledger.request_count==0
                ipc.set_queue_gate(sock,gate='running',request_id='run',expected_revision=1)
                deadline=time.monotonic()+8
                while time.monotonic()<deadline:
                    current=ipc.request_jobs_page(sock).jobs[0]
                    if current.state in {'paused','completed'}: break
                    time.sleep(.03)
                assert current.state=='paused', 'first private body consumed pre-existing unowned bytes'
                assert origin.ledger.request_count==0
                current_stat=existing.lstat()
                assert (current_stat.st_dev,current_stat.st_ino,current_stat.st_mode,current_stat.st_size,current_stat.st_mtime_ns)==(
                    old_stat.st_dev,old_stat.st_ino,old_stat.st_mode,old_stat.st_size,old_stat.st_mtime_ns)
                if old_bytes is not None: assert existing.read_bytes()==old_bytes
                assert target_file.read_bytes()==b'preserved original bytes'
                with closing(SQLiteStore(state/'state.db')) as store:
                    receipt=store._connection.execute('SELECT * FROM direct_dispatch_commands').fetchone()
                    assert receipt['producer_kind']=='target_body' and receipt['status']=='blocked'
                    assert store._read_target_dispatch_cause(store._connection,receipt['request_id'],{})['admission_serial']==1
                    assert store.get_direct_engine_record() is None
                    assert store.get_direct_engine_activation_fence() is None
                rows=[json.loads(line) for line in (evidence/f'fixture-{process.pid}.jsonl').read_text().splitlines()]
                assert not any(row['kind']=='engine-birth' for row in rows)
                assert not (evidence/'scheduler-effects.jsonl').exists() or not any(
                    json.loads(line)['kind']=='causal-engine-start' for line in
                    (evidence/'scheduler-effects.jsonl').read_text().splitlines())
            finally:
                shutdown.set()
                try:
                    _close_fixture_process(process,evidence)
                finally:
                    try: results.close()
                    finally: results.join_thread()
        assert origin_thread is not None and not origin_thread.is_alive()


@pytest.mark.parametrize('entry_kind', ['partial','control'])
def test_first_private_body_rechecks_unowned_bytes_before_stock_add(tmp_path, monkeypatch, entry_kind):
    evidence=tmp_path/'effects';evidence.mkdir(mode=0o700)
    home=tmp_path/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home))
    (home/'Downloads/Hermes').mkdir(parents=True,mode=0o700)
    with tempfile.TemporaryDirectory(dir='/private/tmp',prefix='causal-late-') as directory:
        state=Path(directory);state.chmod(0o700);context=multiprocessing.get_context('spawn')
        ready,shutdown,stopped=[context.Event() for _ in range(3)];results=context.Queue()
        with _helpers._origin_type()(payload_size=1024) as origin:
            origin_thread=origin._thread
            process=context.Process(target=_scheduler_worker,args=(str(state),ready,shutdown,stopped,
                results,origin.url(),str(evidence)),kwargs={'late_entry':entry_kind})
            process.start()
            try:
                assert ready.wait(6);sock=state/'worker.sock'
                exchange(state,canonical(envelope([entry(source_url=origin.url(),partial_filename='late.bin',
                    selected_final_filename='late.bin')])))
                ipc.target_authorize(sock,ipc.TargetAuthorizeCommand.from_record(target()))
                ipc.set_queue_gate(sock,gate='running',request_id='run',expected_revision=1)
                deadline=time.monotonic()+8
                while time.monotonic()<deadline:
                    current=ipc.request_jobs_page(sock).jobs[0]
                    if current.state=='paused': break
                    time.sleep(.03)
                assert current.state=='paused' and origin.ledger.request_count==0
                partial=home/'Downloads/Hermes/.incomplete/batch-job-0/late.bin'
                existing=partial if entry_kind=='partial' else partial.with_name(partial.name+'.aria2')
                assert existing.read_bytes()==b'late unowned bytes'
                with closing(SQLiteStore(state/'state.db')) as store:
                    assert store.get_direct_engine_record() is None and store.get_direct_engine_activation_fence() is None
                    assert store._connection.execute('SELECT status FROM direct_dispatch_commands').fetchone()[0]=='blocked'
            finally:
                shutdown.set()
                try:
                    _close_fixture_process(process,evidence)
                finally:
                    try: results.close()
                    finally: results.join_thread()
        assert origin_thread is not None and not origin_thread.is_alive()
    rows=[json.loads(line) for line in (evidence/f'fixture-{process.pid}.jsonl').read_text().splitlines()]
    assert sum(row['kind']=='engine-waitable-child-reaped' for row in rows)==1


@pytest.mark.parametrize('legacy_body', [False, True])
@pytest.mark.parametrize('mode', ['unlink','hash'])
def test_private_completed_cleanup_keeps_gate_first_and_owned_join_context(tmp_path, monkeypatch, legacy_body, mode):
    evidence=tmp_path/'effects';evidence.mkdir(mode=0o700)
    home=tmp_path/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home))
    (home/'Downloads/Hermes').mkdir(parents=True,mode=0o700)
    with tempfile.TemporaryDirectory(dir='/private/tmp',prefix='causal-cleanup-') as directory:
        state=Path(directory);state.chmod(0o700);context=multiprocessing.get_context('spawn')
        ready,shutdown,stopped,entered,release,cleanup_entered,cleanup_release=[context.Event() for _ in range(7)]
        results=context.Queue()
        with _helpers._origin_type()(payload_size=1024) as origin:
            origin_thread=origin._thread
            process=context.Process(target=_scheduler_worker,args=(str(state),ready,shutdown,stopped,
                results,origin.url(),str(evidence)),kwargs=dict(private_start=not legacy_body,
                barrier='post-link' if legacy_body else None,entered=entered,release=release,
                cleanup_mode=mode,cleanup_entered=cleanup_entered,cleanup_release=cleanup_release))
            process.start()
            try:
                assert ready.wait(6);sock=state/'worker.sock';job='cleanup-job'
                ipc.add_job(sock,job=job,request_id='typed-single',source_url=origin.url(),priority=0,
                    order_key=0,category='Other',partial_filename='cleanup.bin',selected_final_filename='cleanup.bin')
                if legacy_body:
                    ipc.set_queue_gate(sock,gate='running',request_id='run',expected_revision=1)
                    ipc.control_job(sock,job=job,action='start_now',request_id='start',expected_revision=0)
                    ipc.activate_direct_engine(sock,expected_worker_epoch=1)
                    assert ipc.dispatch_direct_job(sock,job=job,expected_worker_epoch=1,
                        expected_generation=0,expected_revision=1,request_id='legacy-body').status=='started'
                    assert entered.wait(7)
                    assert ipc.set_queue_gate(sock,gate='paused',request_id='legacy-hold',expected_revision=2).applied
                    release.set();time.sleep(.15)
                ipc.target_authorize(sock,ipc.TargetAuthorizeCommand.from_record(target(selector=select_jobs([job]))))
                revision=3 if legacy_body else 1
                ipc.set_queue_gate(sock,gate='running',request_id='private-run',expected_revision=revision)
                assert cleanup_entered.wait(8)
                assert ipc.request_jobs_page(sock).jobs[0].state=='completed'
                with sqlite3.connect(state/'state.db') as connection:
                    producers=connection.execute('SELECT request_id,status,state,producer_kind FROM direct_dispatch_commands').fetchall()
                    original=connection.execute('SELECT attempt_id,proof FROM direct_publication_attempts').fetchone()
                    assert any(row[3]==('target_publication' if legacy_body else 'target_body') for row in producers)
                before=origin.ledger
                with pytest.raises(ipc.IPCError,match='command_conflict'):
                    ipc.set_queue_gate(sock,gate='paused',request_id='private-hold',expected_revision=revision+1)
                assert ipc.request_health(sock).queue_gate=='paused'
                assert not cleanup_release.is_set()
                with sqlite3.connect(state/'state.db') as connection:
                    assert connection.execute('SELECT phase FROM direct_cleanup_claims').fetchone()[0]=='pending'
                cleanup_release.set();time.sleep(.15)
                replay=ipc.set_queue_gate(sock,gate='paused',request_id='private-hold',expected_revision=revision+1)
                assert not replay.applied and replay.revision==revision+2
                with sqlite3.connect(state/'state.db') as connection:
                    assert connection.execute('SELECT request_id,status,state,producer_kind FROM direct_dispatch_commands').fetchall()==producers
                    assert connection.execute('SELECT attempt_id,proof FROM direct_publication_attempts').fetchone()==original
                    assert connection.execute('SELECT phase FROM direct_cleanup_claims').fetchone()[0]=='pending'
                assert origin.ledger==before and before.request_count==1
                assert (home/'Downloads/Hermes/Other/cleanup.bin').read_bytes()==origin.payload
                assert (home/f'Downloads/Hermes/.incomplete/{job}/.hermes-reservation').exists()
                ipc.set_queue_gate(sock,gate='running',request_id='reopen',expected_revision=revision+2)
                assert ipc.set_queue_gate(sock,gate='paused',request_id='private-hold',expected_revision=revision+1)==replay
                assert ipc.request_health(sock).queue_gate=='running' and origin.ledger==before
            finally:
                cleanup_release.set();release.set();shutdown.set()
                try:
                    _close_fixture_process(process,evidence)
                finally:
                    try: results.close()
                    finally: results.join_thread()
        assert origin_thread is not None and not origin_thread.is_alive()
    effects=[json.loads(line) for line in (evidence/'scheduler-effects.jsonl').read_text().splitlines()]
    assert any(row['kind']=='queue-control-error' and row['error']=='direct_dispatch_blocked' for row in effects)
    assert any(row['kind']=='observer-join' and row['name']=='direct-cleanup-observation' and not row['alive'] for row in effects)


@pytest.mark.parametrize('fail_start', [False, True])
def test_real_default_one_causal_bodies_reap_join_cleanup_before_next(tmp_path, fail_start, monkeypatch):
    evidence=tmp_path/'effects';evidence.mkdir(mode=0o700)
    home=tmp_path/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home))
    (Path.home()/'Downloads/Hermes').mkdir(parents=True,mode=0o700,exist_ok=True)
    with tempfile.TemporaryDirectory(dir='/private/tmp',prefix='causal-') as directory:
        state=Path(directory);state.chmod(0o700)
        context=multiprocessing.get_context('spawn')
        ready,shutdown,stopped=[context.Event() for _ in range(3)]
        results=context.Queue()
        with _helpers._origin_type()(payload_size=1024) as origin:
            origin_thread=origin._thread
            process=context.Process(target=_scheduler_worker,args=(str(state),ready,shutdown,stopped,
                results,origin.url(),str(evidence),fail_start),kwargs={'private_start':None})
            process.start()
            try:
                assert ready.wait(6)
                entries=[entry(i,source_url=origin.url('/range'),priority=i,
                    partial_filename=f'causal-{i}.bin',selected_final_filename=f'causal-{i}.bin') for i in range(2)]
                assert exchange(state,canonical(envelope(entries)))['status']=='applied'
                ipc.request_jobs_page(state/'worker.sock')
                time.sleep(.15)
                assert origin.ledger.request_count==0
                reply=ipc.target_authorize(state/'worker.sock',ipc.TargetAuthorizeCommand.from_record(target()))
                assert all(r['outcome']=='new_authority' for r in reply.to_record()['results'])
                assert origin.ledger.request_count==0
                assert ipc.set_queue_gate(state/'worker.sock',gate='running',request_id='run',expected_revision=1).applied
                deadline=time.monotonic()+12
                while time.monotonic()<deadline:
                    with sqlite3.connect(state/'state.db') as connection:
                        states=connection.execute('SELECT state FROM jobs ORDER BY job_id').fetchall()
                        cleanup=connection.execute("SELECT count(*) FROM direct_cleanup_claims WHERE phase='finished'").fetchone()[0]
                    if (fail_start and states==[('paused',),('paused',)]) or (not fail_start and cleanup==2): break
                    time.sleep(.03)
                else: pytest.fail(f'private scheduler did not settle: {states}, cleanup={cleanup}')
                with closing(SQLiteStore(state/'state.db')) as store:
                    causes=[dict(r) for r in store._connection.execute('SELECT * FROM target_dispatch_causes ORDER BY resolving_audit_id')]
                    assert [r['job_id'] for r in causes]==['batch-job-1','batch-job-0']
                    assert all(r['admission_serial']==1 for r in causes)
                    assert store.get_direct_engine_record() is None
                    assert store.get_direct_engine_activation_fence() is None
                    assert store._connection.execute('PRAGMA foreign_key_check').fetchall()==[]
                    assert all(store._capture_target_head(store._connection,r['job_id']).current_state==
                        ('paused' if fail_start else 'completed') for r in causes)
                    before=tuple(store._connection.iterdump())
                    assert store._prepare_target_dispatch(expected_worker_epoch=1,owner_slot_ready=True,now=datetime.now(UTC)) is None
                    assert tuple(store._connection.iterdump())==before
                assert origin.ledger.request_count==(0 if fail_start else 2)
                assert origin.ledger.response_body_bytes==(0 if fail_start else 2048)
                if not fail_start:
                    for i in range(2):
                        final=Path.home()/f'Downloads/Hermes/Other/causal-{i}.bin'
                        assert final.read_bytes()==origin.payload
                        assert not (Path.home()/f'Downloads/Hermes/.incomplete/batch-job-{i}/causal-{i}.bin').exists()
                    assert ipc.activate_direct_engine(state/'worker.sock',expected_worker_epoch=1).status=='active'
                    with closing(SQLiteStore(state/'state.db')) as store:
                        blank_legacy_owner=store.get_direct_engine_record()
                    assert blank_legacy_owner is not None
                    assert ipc.set_queue_gate(state/'worker.sock',gate='paused',
                        request_id='blank-legacy-hold',expected_revision=2).applied
                    with closing(SQLiteStore(state/'state.db')) as store:
                        assert store.get_direct_engine_record()==blank_legacy_owner
                    assert origin.ledger.request_count==2
            finally:
                shutdown.set()
                try:
                    _close_fixture_process(process,evidence)
                finally:
                    try: results.close()
                    finally: results.join_thread()
        assert origin_thread is not None and not origin_thread.is_alive()
    effects=[json.loads(line) for line in (evidence/'scheduler-effects.jsonl').read_text().splitlines()]
    starts=[r for r in effects if r['kind']=='causal-engine-start']
    assert len(starts)==2
    if not fail_start:
        closures=[json.loads(line) for line in (evidence/f'fixture-{process.pid}.jsonl').read_text().splitlines()]
        assert sum(r['kind']=='engine-waitable-child-reaped' for r in closures)==3
        assert any(r['kind']=='observer-join' and not r['alive'] and r['monotonic']<starts[1]['monotonic'] for r in effects)
        first_reap=next(i for i,r in enumerate(closures) if r['kind']=='engine-waitable-child-reaped')
        second_birth=[i for i,r in enumerate(closures) if r['kind']=='engine-birth'][1]
        assert first_reap<second_birth


@pytest.mark.parametrize('fail_close', [False,True])
def test_real_private_post_link_hold_replay_and_existing_only_recovery(tmp_path, monkeypatch, fail_close):
    evidence=tmp_path/'effects';evidence.mkdir(mode=0o700)
    home=tmp_path/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home))
    (Path.home()/'Downloads/Hermes').mkdir(parents=True,mode=0o700,exist_ok=True)
    with tempfile.TemporaryDirectory(dir='/private/tmp',prefix='causal-hold-') as directory:
        state=Path(directory);state.chmod(0o700)
        context=multiprocessing.get_context('spawn')
        ready,shutdown,stopped,entered,release,recovery_entered,recovery_release=[context.Event() for _ in range(7)]
        results=context.Queue()
        with _helpers._origin_type()(payload_size=1024) as origin:
            origin_thread=origin._thread
            process=context.Process(target=_scheduler_worker,args=(str(state),ready,shutdown,stopped,
                results,origin.url(),str(evidence),False,'post-link',entered,release,recovery_entered,recovery_release),
                kwargs={'fail_close':fail_close})
            process.start()
            try:
                assert ready.wait(6)
                assert exchange(state,canonical(envelope([entry(source_url=origin.url(),
                    partial_filename='hold.bin',selected_final_filename='hold.bin')])))['status']=='applied'
                ipc.target_authorize(state/'worker.sock',ipc.TargetAuthorizeCommand.from_record(target()))
                ipc.set_queue_gate(state/'worker.sock',gate='running',request_id='run',expected_revision=1)
                assert entered.wait(7)
                with sqlite3.connect(state/'state.db') as connection:
                    original=connection.execute("SELECT * FROM direct_dispatch_commands WHERE producer_kind='target_body'").fetchone()
                    proof=connection.execute('SELECT attempt_id,proof FROM direct_publication_attempts').fetchone()
                before=origin.ledger
                # A noncooperative fsync observer cannot receive a stopped acknowledgement.
                # Existing queue wire collapses callback errors to command_conflict.
                with pytest.raises(ipc.IPCError,match='command_conflict'):
                    ipc.set_queue_gate(state/'worker.sock',gate='paused',request_id='hold',expected_revision=2)
                errors=[json.loads(line) for line in (evidence/'scheduler-effects.jsonl').read_text().splitlines()]
                assert any(r['kind']=='queue-control-error' and r['error']=='direct_dispatch_blocked' for r in errors)
                assert ipc.request_health(state/'worker.sock').queue_gate=='paused'
                assert ipc.activate_direct_engine(state/'worker.sock',expected_worker_epoch=1).status=='blocked'
                if fail_close:
                    assert any(r['kind']=='actual-owned-close-refused' for r in errors)
                with sqlite3.connect(state/'state.db') as connection:
                    assert connection.execute('SELECT state FROM jobs').fetchone()[0]==('finalizing' if fail_close else 'paused')
                    assert connection.execute('SELECT attempt_id,proof FROM direct_publication_attempts').fetchone()==proof
                release.set()
                time.sleep(.15)
                replay=ipc.set_queue_gate(state/'worker.sock',gate='paused',request_id='hold',expected_revision=2)
                assert not replay.applied and replay.revision==3
                ipc.set_queue_gate(state/'worker.sock',gate='running',request_id='resume',expected_revision=3)
                assert recovery_entered.wait(5)
                with sqlite3.connect(state/'state.db') as connection:
                    parked={name:connection.execute(f'SELECT * FROM {name}').fetchall() for name in
                        ('jobs','events','direct_publication_attempts','target_dispatch_causes','job_authorization_heads','direct_dispatch_commands')}
                    pending=connection.execute("SELECT request_id FROM direct_dispatch_commands WHERE producer_kind='target_publication'").fetchone()[0]
                assert ipc.set_queue_gate(state/'worker.sock',gate='paused',request_id='park',expected_revision=4).applied
                assert not recovery_release.is_set()
                with sqlite3.connect(state/'state.db') as connection:
                    assert all(connection.execute(f'SELECT * FROM {name}').fetchall()==rows for name,rows in parked.items())
                    assert connection.execute('SELECT pending_request_id FROM direct_publication_attempts').fetchone()[0]==pending
                recovery_release.set()
                ipc.set_queue_gate(state/'worker.sock',gate='running',request_id='resume-park',expected_revision=5)
                deadline=time.monotonic()+8
                while time.monotonic()<deadline:
                    with sqlite3.connect(state/'state.db') as connection:
                        done=connection.execute("SELECT count(*) FROM direct_cleanup_claims WHERE phase='finished'").fetchone()[0]
                    if done: break
                    time.sleep(.03)
                assert done==1
                assert origin.ledger==before
                with sqlite3.connect(state/'state.db') as connection:
                    assert connection.execute("SELECT * FROM direct_dispatch_commands WHERE producer_kind='target_body'").fetchone()==original
                    publication=connection.execute("SELECT request_id,status FROM direct_dispatch_commands WHERE producer_kind='target_publication'").fetchall()
                    assert len(publication)==1 and publication[0][1]=='started'
                    assert connection.execute('SELECT attempt_id,proof FROM direct_publication_attempts').fetchone()==proof
                    assert connection.execute('SELECT last_admission_serial FROM job_authorization_heads').fetchone()[0]==1
                    assert connection.execute("SELECT count(*) FROM events WHERE kind='job_completed'").fetchone()[0]==1
                # Historical hold replay has no current queue or filesystem effect.
                assert ipc.set_queue_gate(state/'worker.sock',gate='paused',request_id='hold',expected_revision=2)==replay
                assert ipc.request_health(state/'worker.sock').queue_gate=='running'
            finally:
                release.set();recovery_release.set();shutdown.set()
                try:
                    _close_fixture_process(process,evidence)
                finally:
                    try: results.close()
                    finally: results.join_thread()
        assert origin_thread is not None and not origin_thread.is_alive()


def test_real_sealed_legacy_original_body_gets_only_exact_private_publication(tmp_path, monkeypatch):
    evidence=tmp_path/'effects';evidence.mkdir(mode=0o700)
    home=tmp_path/'home';home.mkdir(mode=0o700);monkeypatch.setenv('HOME',str(home))
    (Path.home()/'Downloads/Hermes').mkdir(parents=True,mode=0o700,exist_ok=True)
    with tempfile.TemporaryDirectory(dir='/private/tmp',prefix='causal-legacy-') as directory:
        state=Path(directory);state.chmod(0o700)
        context=multiprocessing.get_context('spawn')
        ready,shutdown,stopped,entered,release=[context.Event() for _ in range(5)]
        results=context.Queue()
        with _helpers._origin_type()(payload_size=1024) as origin:
            origin_thread=origin._thread
            process=context.Process(target=_scheduler_worker,args=(str(state),ready,shutdown,stopped,
                results,origin.url(),str(evidence),False,'post-link',entered,release,None,None,False))
            process.start()
            try:
                assert ready.wait(6)
                sock=state/'worker.sock'
                ipc.add_job(sock,job='legacy-single',request_id='typed-single',source_url=origin.url(),
                    priority=0,order_key=0,category='Other',partial_filename='legacy-causal.bin',selected_final_filename='legacy-causal.bin')
                ipc.set_queue_gate(sock,gate='running',request_id='run',expected_revision=1)
                ipc.control_job(sock,job='legacy-single',action='start_now',request_id='start',expected_revision=0)
                assert ipc.activate_direct_engine(sock,expected_worker_epoch=1).status=='active'
                body=ipc.dispatch_direct_job(sock,job='legacy-single',expected_worker_epoch=1,
                    expected_generation=0,expected_revision=1,request_id='legacy-body')
                assert body.status=='started'
                assert entered.wait(7)
                with sqlite3.connect(state/'state.db') as connection:
                    original=connection.execute('SELECT * FROM direct_dispatch_commands').fetchone()
                    proof=connection.execute('SELECT attempt_id,proof FROM direct_publication_attempts').fetchone()
                    assert original[-1]=='legacy'
                assert ipc.set_queue_gate(sock,gate='paused',request_id='hold',expected_revision=2).applied
                assert ipc.request_health(sock).queue_gate=='paused'
                # Accepted legacy cancellation retains the busy observer until retirement.
                assert entered.is_set() and not release.is_set()
                release.set();time.sleep(.15)
                assert not ipc.set_queue_gate(sock,gate='paused',request_id='hold',expected_revision=2).applied
                reply=ipc.target_authorize(sock,ipc.TargetAuthorizeCommand.from_record(target(selector=select_jobs(['legacy-single']))))
                assert reply.to_record()['results'][0]['outcome']=='new_authority'
                before=origin.ledger
                ipc.set_queue_gate(sock,gate='running',request_id='resume',expected_revision=3)
                deadline=time.monotonic()+8
                while time.monotonic()<deadline:
                    with sqlite3.connect(state/'state.db') as connection:
                        done=connection.execute("SELECT count(*) FROM direct_cleanup_claims WHERE phase='finished'").fetchone()[0]
                    if done: break
                    time.sleep(.03)
                assert done==1 and origin.ledger==before and before.request_count==1
                with closing(SQLiteStore(state/'state.db')) as store:
                    connection=store._connection
                    assert tuple(connection.execute("SELECT * FROM direct_dispatch_commands WHERE request_id='legacy-body'").fetchone())==original
                    assert tuple(connection.execute('SELECT attempt_id,proof FROM direct_publication_attempts').fetchone())==proof
                    cause=connection.execute('SELECT * FROM target_dispatch_causes').fetchone()
                    assert cause['kind']=='publication' and cause['original_body_request_id']=='legacy-body'
                    assert store._read_target_dispatch_cause(connection,cause['request_id'],{})==dict(cause)
                    assert connection.execute('SELECT last_admission_serial FROM job_authorization_heads').fetchone()[0]==0
            finally:
                release.set();shutdown.set()
                try:
                    _close_fixture_process(process,evidence)
                finally:
                    try: results.close()
                    finally: results.join_thread()
        assert origin_thread is not None and not origin_thread.is_alive()
