"""Physical installed serving lifecycle; all child groups are explicitly owned."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import socket
import threading
import tempfile
import time

import pytest
from hermes_downloads import ipc, worker
from hermes_downloads.store import SQLiteStore

BIN = Path(sys.executable).parent


def record(process):
    path = os.environ.get('T16F_PROCESS_LOG')
    if path:
        with open(path, 'a') as stream:
            stream.write(json.dumps({'pid': process.pid, 'pgid': process.pid,
                'returncode': process.returncode, 'reaped': process.returncode is not None}) + '\n')


def owned_command(argv, *, cwd, capture_output=True, timeout=7):
    process = subprocess.Popen(argv, cwd=cwd, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, stdin=subprocess.DEVNULL, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        return subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        record(process)


@pytest.fixture
def root():
    base = '/private/tmp' if Path('/private/tmp').is_dir() else '/tmp'
    with tempfile.TemporaryDirectory(prefix='t16f-', dir=base) as directory:
        path = Path(directory)
        path.chmod(0o700)
        yield path


def launch(root):
    return subprocess.Popen([str(BIN / 'hermes-downloads-worker'), '--serve',
        '--state-root', str(root)], cwd=root, env=os.environ.copy(),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)


def stop(process, signum=signal.SIGTERM):
    if process.poll() is None:
        os.killpg(process.pid, signum)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
        pytest.fail('owned service did not stop')
    finally:
        record(process)


def ready(process, root):
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        assert process.poll() is None, 'installed --serve exited instead of listening'
        try:
            return ipc.request_health(root / 'worker.sock')
        except ipc.IPCError:
            # Bounded poll of the actual IPC readiness handshake.
            threading.Event().wait(0.02)
    pytest.fail('installed --serve did not answer health')


@pytest.mark.parametrize('signum', [signal.SIGINT, signal.SIGTERM])
def test_installed_serve_paused_health_list_and_orderly_signals(root, signum):
    process = launch(root)
    try:
        health = ready(process, root)
        assert health.queue_gate == 'paused'
        assert ipc.request_jobs_page(root / 'worker.sock').to_record()['jobs'] == []
        assert (root / '.worker-endpoint.json').stat().st_mode & 0o777 == 0o600
        for action in ['health', 'list']:
            result = owned_command([str(BIN / 'hermes-downloads'), action, '--state-root', str(root)],
                cwd=root, capture_output=True, timeout=4)
            assert result.returncode == 0
            assert json.loads(result.stdout)
        stop(process, signum)
        assert process.returncode == 0
        assert not (root / 'worker.sock').exists()
        assert not (root / '.worker-endpoint.json').exists()
    finally:
        stop(process)


def test_cli_unavailable_is_redacted_and_zero_effect(root):
    result = owned_command([str(BIN / 'hermes-downloads'), 'health', '--state-root', str(root)],
        cwd=root, capture_output=True, timeout=4)
    assert result.returncode == 1
    assert result.stderr == b'service_unavailable\n'
    assert list(root.iterdir()) == []


def test_duplicate_and_sigkill_restart_preserve_add_only_jobs(root):
    process = launch(root)
    try:
        first = ready(process, root)
        result = ipc.add_job(root / 'worker.sock', job='idle-job',
            source_url='https://example.test/no-body?secret=hidden', category='Other',
            partial_filename='idle.bin', selected_final_filename='idle.bin',
            request_id='idle-add', priority=0, order_key=0)
        assert result.applied
        before = (root / '.worker-endpoint.json').read_bytes()
        duplicate = launch(root)
        stop_code = duplicate.wait(timeout=4)
        record(duplicate)
        assert stop_code == 1
        assert (root / '.worker-endpoint.json').read_bytes() == before
        assert ipc.request_health(root / 'worker.sock') == first
        assert len(ipc.request_jobs_page(root / 'worker.sock').jobs) == 1
        stop(process, signal.SIGKILL)
        assert (root / '.worker-endpoint.json').read_bytes() == before
        process = launch(root)
        second = ready(process, root)
        assert second.worker_epoch == first.worker_epoch + 1
        assert second.queue_gate == 'paused'
        page = ipc.request_jobs_page(root / 'worker.sock')
        assert page.jobs[0].state == 'paused'
        assert b'secret' not in json.dumps(page.to_record()).encode()
        import sqlite3
        with sqlite3.connect(root / 'state.db') as db:
            assert db.execute('SELECT count(*) FROM engine_instances').fetchone()[0] == 0
        assert not (Path.home() / 'Downloads' / 'Hermes' / 'Other' / 'idle.bin').exists()
    finally:
        stop(process)


@pytest.mark.parametrize('fault', ['missing-record', 'bad-json', 'epoch', 'schema', 'extra',
    'mode', 'hardlink', 'symlink', 'socket-mode', 'socket-substitution', 'root-identity',
    'bool', 'negative', 'record-fifo', 'database-missing', 'database-corrupt', 'database-newer', 'database-symlink'])
def test_uncertified_endpoint_refusal_has_no_effects(root, fault):
    process = launch(root)
    try:
        ready(process, root)
        stop(process, signal.SIGKILL)
        record_path = root / '.worker-endpoint.json'
        value = json.loads(record_path.read_bytes())
        if fault == 'missing-record':
            record_path.unlink()
        elif fault == 'bad-json':
            record_path.write_bytes(b'{broken')
        elif fault in {'epoch', 'schema', 'extra', 'root-identity'}:
            key = {'epoch': 'worker_epoch', 'schema': 'schema', 'extra': 'unknown', 'root-identity': 'root_inode'}[fault]
            value[key] = 999999
            record_path.write_text(json.dumps(value))
        elif fault == 'mode':
            record_path.chmod(0o644)
        elif fault == 'hardlink':
            os.link(record_path, root / 'retained')
        elif fault == 'symlink':
            record_path.rename(root / 'retained')
            record_path.symlink_to(root / 'retained')
        elif fault in {'bool', 'negative'}:
            value['worker_epoch'] = True if fault == 'bool' else -1
            record_path.write_text(json.dumps(value))
        elif fault == 'record-fifo':
            record_path.unlink()
            os.mkfifo(record_path, 0o600)
        elif fault.startswith('database-'):
            database = root / 'state.db'
            if fault == 'database-missing':
                database.unlink()
            elif fault == 'database-corrupt':
                database.write_bytes(b'corrupt')
            elif fault == 'database-symlink':
                database.rename(root / 'retained-db')
                database.symlink_to(root / 'retained-db')
            else:
                import sqlite3
                with sqlite3.connect(database) as db:
                    db.execute('PRAGMA user_version = 999')
        elif fault == 'socket-mode':
            (root / 'worker.sock').chmod(0o666)
        else:
            (root / 'worker.sock').unlink()
            (root / 'worker.sock').write_bytes(b'unknown')
        before = {p.name: (p.lstat().st_ino, p.lstat().st_mode,
            p.read_bytes() if p.is_file() else None) for p in root.iterdir()}
        refused = launch(root)
        assert refused.wait(timeout=4) == 1
        record(refused)
        after = {p.name: (p.lstat().st_ino, p.lstat().st_mode,
            p.read_bytes() if p.is_file() else None) for p in root.iterdir()}
        assert after == before
    finally:
        stop(process)


@pytest.mark.parametrize('signum', [signal.SIGINT, signal.SIGTERM])
def test_signal_during_startup_is_request_only_and_handlers_restored(private_roots, monkeypatch, signum):
    from hermes_downloads import service
    previous = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    original = worker._acquire_worker_lease
    def acquire(root):
        os.kill(os.getpid(), signum)
        return original(root)
    monkeypatch.setattr(worker, '_acquire_worker_lease', acquire)
    # Short root needed for Darwin. Integration fixture is intentionally private.
    import tempfile
    with tempfile.TemporaryDirectory(dir='/private/tmp' if Path('/private/tmp').exists() else '/tmp') as directory:
        root = Path(directory)
        root.chmod(0o700)
        assert service.worker_main(['--serve', '--state-root', str(root)]) == 0
        assert not (root / 'worker.sock').exists()
    assert {s: signal.getsignal(s) for s in previous} == previous


@pytest.mark.parametrize('value', ['relative', '/private/tmp/../tmp', '/nonexistent', '/private/tmp/' + 'x' * 104])
def test_invalid_root_is_redacted_and_argument_status(value, capsys):
    from hermes_downloads import service
    assert service.worker_main(['--serve', '--state-root', value]) == 2
    assert capsys.readouterr().err == 'service_arguments_invalid\n'


def test_legacy_default_still_refuses_unknown_endpoint_before_lease(private_roots):
    root = private_roots['state']
    endpoint = root / 'worker.sock'
    endpoint.write_bytes(b'unknown')
    stopped = threading.Event()
    with pytest.raises(worker.WorkerStateError):
        worker.run_worker(root, socket_path=endpoint, ready_event=threading.Event(),
            shutdown_event=threading.Event(), stopped_event=stopped)
    assert stopped.is_set()
    assert list(root.iterdir()) == [endpoint]


@pytest.mark.parametrize('payload', [b'{"schema":1,"schema":1}', b'\xff', b'[]', b'{}', b'x' * 4097])
def test_invalid_record_without_endpoint_has_zero_artifacts(private_roots, payload):
    from hermes_downloads import endpoint_ownership as ownership
    root = private_roots['state']
    path = root / '.worker-endpoint.json'
    path.write_bytes(payload)
    path.chmod(0o600)
    with pytest.raises(ownership.OwnershipError):
        ownership.preflight(root)
    assert list(root.iterdir()) == [path]
    assert path.read_bytes() == payload


@pytest.mark.parametrize('fault', ['write', 'file-sync', 'dir-sync', 'close', 'record-replaced'])
def test_bind_record_fault_never_ready_and_uncertain_close_retains(root_service, monkeypatch, fault):
    from hermes_downloads import endpoint_ownership as ownership
    root = root_service
    ready, shutdown, stopped = threading.Event(), threading.Event(), threading.Event()
    if fault in {'write', 'file-sync', 'dir-sync'}:
        shutdown.set()
        if fault == 'write':
            monkeypatch.setattr(ownership.os, 'write', lambda *_: (_ for _ in ()).throw(OSError()))
        else:
            original = ownership.os.fsync
            def fsync(fd):
                import stat
                is_dir = stat.S_ISDIR(os.fstat(fd).st_mode)
                if is_dir == (fault == 'dir-sync'):
                    raise OSError()
                return original(fd)
            monkeypatch.setattr(ownership.os, 'fsync', fsync)
        with pytest.raises(ownership.OwnershipError):
            worker.run_worker(root, socket_path=root / 'worker.sock', recover_socket=True,
                ready_event=ready, shutdown_event=shutdown, stopped_event=stopped)
        assert not ready.is_set()
        assert not (root / 'worker.sock').exists()
        assert not any(p.name.startswith('.endpoint-') for p in root.iterdir())
    else:
        class Ready(threading.Event):
            def set(self):
                super().set()
                shutdown.set()
        ready = Ready()
        original = worker.HealthServer.close
        def close(server):
            if fault == 'close':
                server._listener.close()  # Listener closed, unlink outcome uncertain.
            else:
                (root / '.worker-endpoint.json').unlink()
                (root / '.worker-endpoint.json').write_bytes(b'unknown')
                (root / '.worker-endpoint.json').chmod(0o600)
                original(server)
        monkeypatch.setattr(worker.HealthServer, 'close', close)
        with pytest.raises(ownership.OwnershipError):
            worker.run_worker(root, socket_path=root / 'worker.sock', recover_socket=True,
                ready_event=ready, shutdown_event=shutdown, stopped_event=stopped)
        assert ready.is_set()
        assert (root / '.worker-endpoint.json').exists()
    assert stopped.is_set()


@pytest.fixture
def root_service():
    import tempfile
    with tempfile.TemporaryDirectory(dir='/private/tmp' if Path('/private/tmp').exists() else '/tmp') as directory:
        root = Path(directory)
        root.chmod(0o700)
        yield root


def test_missing_endpoint_with_exact_old_record_can_restart(root):
    process = launch(root)
    try:
        epoch = ready(process, root).worker_epoch
        stop(process, signal.SIGKILL)
        (root / 'worker.sock').unlink()
        process = launch(root)
        assert ready(process, root).worker_epoch == epoch + 1
    finally:
        stop(process)


def test_readonly_epoch_observes_live_wal_without_bootstrap(root):
    from hermes_downloads import endpoint_ownership as ownership
    certificate = ownership.preflight(root)
    store = SQLiteStore(root / 'state.db')
    store.recover_cold_start()
    store._connection.execute('PRAGMA journal_mode=WAL')
    store.recover_cold_start()
    server = ipc.HealthServer(root / 'worker.sock', health=lambda: ipc.WorkerHealth(2, 'paused'))
    try:
        owned = ownership.publish(root, certificate, 2, server._identity)
        assert (root / 'state.db-wal').exists()
        assert ownership.preflight(root).record == owned.record
        assert store.worker_epoch() == 2
    finally:
        server.close()
        ownership.clear(root, owned)
        store.close()


def test_reclaim_happens_after_lease_and_before_writable_store(root, monkeypatch):
    import fcntl
    from hermes_downloads import endpoint_ownership as ownership
    process = launch(root)
    ready(process, root)
    stop(process, signal.SIGKILL)
    original_acquire = worker._acquire_worker_lease
    original_store = worker.SQLiteStore
    calls = []
    def acquire(path):
        assert (root / 'worker.sock').exists()
        calls.append('lease')
        return original_acquire(path)
    def writable(path):
        assert not (root / 'worker.sock').exists()
        probe = os.open(root / '.worker.lock', os.O_RDWR)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(probe)
        calls.append('writable')
        return original_store(path)
    monkeypatch.setattr(worker, '_acquire_worker_lease', acquire)
    monkeypatch.setattr(worker, 'SQLiteStore', writable)
    shutdown = threading.Event()
    shutdown.set()
    worker.run_worker(root, socket_path=root / 'worker.sock', recover_socket=True,
        ready_event=threading.Event(), shutdown_event=shutdown, stopped_event=threading.Event())
    assert calls == ['lease', 'writable']


@pytest.mark.parametrize('response', [b'not-json\n', b'{"secret":"private-source"}\n', None])
def test_cli_malformed_and_timeout_are_bounded_redacted_and_ipc_only(root, response):
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(root / 'worker.sock'))
    listener.listen(1)
    release = threading.Event()
    def respond():
        connection, _ = listener.accept()
        with connection:
            connection.recv(4096)
            if response is None:
                release.wait(7)
            else:
                connection.sendall(response)
    thread = threading.Thread(target=respond)
    thread.start()
    try:
        started = time.monotonic()
        result = owned_command([str(BIN / 'hermes-downloads'), 'health', '--state-root', str(root)],
            cwd=root, capture_output=True, timeout=7)
        assert result.returncode == 1
        assert result.stderr == b'service_unavailable\n'
        assert time.monotonic() - started < 6.5
        assert sorted(p.name for p in root.iterdir()) == ['worker.sock']
    finally:
        release.set()
        thread.join(4)
        listener.close()
        assert not thread.is_alive()


def test_symlink_ancestor_rejected_without_redirection(root):
    from hermes_downloads import endpoint_ownership as ownership
    (root / 'real').mkdir(mode=0o700)
    (root / 'alias').symlink_to(root / 'real')
    (root / 'real' / 'state').mkdir(mode=0o700)
    with pytest.raises(ownership.OwnershipError):
        ownership.validate_root(root / 'alias' / 'state')
    assert list((root / 'real' / 'state').iterdir()) == []


def test_unknown_socket_without_database_has_no_bootstrap_artifact(root):
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(root / 'worker.sock'))
    (root / 'worker.sock').chmod(0o600)
    before = (root / 'worker.sock').lstat()
    try:
        process = launch(root)
        try:
            assert process.wait(timeout=4) == 1
            assert sorted(p.name for p in root.iterdir()) == ['worker.sock']
            assert (root / 'worker.sock').lstat().st_ino == before.st_ino
        finally:
            stop(process)
    finally:
        listener.close()


def test_real_local_fixture_add_only_restart_preserves_hold_and_zero_ledger(root, monkeypatch):
    import importlib.util
    from hermes_downloads import network
    spec = importlib.util.spec_from_file_location('_service_http_fixture',
        Path(__file__).parents[1] / 'fixtures' / 'http_origin.py')
    fixture = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = fixture
    spec.loader.exec_module(fixture)
    with fixture.SyntheticHttpOrigin() as origin:
        grant = network.LocalOriginGrant.for_url(origin.url())
        original = worker.validate_source_url
        monkeypatch.setattr(worker, 'validate_source_url',
            lambda value: original(value, local_origin_grant=grant))
        launches = []
        def forbidden(*args, **kwargs):
            launches.append(True)
            raise AssertionError('add-only cannot launch engines')
        monkeypatch.setattr(subprocess, 'Popen', forbidden)
        for epoch in (1, 2):
            ready_event, shutdown, stopped = threading.Event(), threading.Event(), threading.Event()
            errors = []
            def run():
                try:
                    worker.run_worker(root, socket_path=root / 'worker.sock', recover_socket=True,
                        ready_event=ready_event, shutdown_event=shutdown, stopped_event=stopped)
                except BaseException as error:
                    errors.append(error)
            thread = threading.Thread(target=run)
            thread.start()
            try:
                assert ready_event.wait(4)
                assert ipc.request_health(root / 'worker.sock').worker_epoch == epoch
                if epoch == 1:
                    ipc.add_job(root / 'worker.sock', job='local-idle', request_id='local-add',
                        source_url=origin.url(), priority=0, order_key=0, category='Other',
                        partial_filename='local.bin', selected_final_filename='local.bin')
                    ipc.control_job(root / 'worker.sock', job='local-idle', action='pause',
                        request_id='local-hold', expected_revision=0)
                assert len(ipc.request_jobs_page(root / 'worker.sock').jobs) == 1
                assert origin.ledger.request_count == 0
                assert origin.ledger.response_body_bytes == 0
            finally:
                shutdown.set()
                assert stopped.wait(4)
                thread.join(4)
                assert not thread.is_alive()
            assert errors == []
            assert launches == []
            import sqlite3
            with sqlite3.connect(root / 'state.db') as db:
                assert db.execute('SELECT manual_hold FROM materialized_jobs').fetchone()[0] == 1
                assert db.execute('SELECT count(*) FROM engine_instances').fetchone()[0] == 0
            assert not (Path.home() / 'Downloads' / 'Hermes').exists()
