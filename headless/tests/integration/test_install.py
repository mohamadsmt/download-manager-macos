"""Installer behavior on private real files; readiness doubles are synthetic only."""
import importlib.util
import json
import os
from pathlib import Path
import plistlib
import subprocess
import tempfile
import signal
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / 'headless/scripts/install.py'
VERIFY = ROOT / 'headless/scripts/verify-install.py'
BASE = '7720638951e6244e616b5dabed4e48d89ad360ed'


def load():
    assert SCRIPT.is_file(), 'missing installer observable CLI behavior'
    spec = importlib.util.spec_from_file_location('download_installer', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def installer():
    return load()


@pytest.fixture
def layout(installer, tmp_path):
    # Short private home also exercises the real Darwin endpoint limit.
    home = Path(tempfile.mkdtemp(prefix='t21a-', dir='/private/tmp'))
    source = tmp_path / 'canonical'; source.mkdir(mode=0o700)
    return installer.Layout(home, source)


@pytest.mark.parametrize('args', [[], ['--apply'], ['--apply', '--profile', 'other'],
    ['--apply', '--profile', 'default'], ['--profile', 'other'], ['--root', '/tmp'],
    ['--apply', '--rollback', '/tmp/x'], ['--expected-commit', 'bad']])
def test_cli_returns_redacted_report_without_effects(args, tmp_path):
    before = {str(p): p.stat().st_ino for p in tmp_path.rglob('*')}
    argv = [sys.executable, '-I', '-B', str(SCRIPT), *args]
    child = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True, env={**os.environ, 'HOME': str(tmp_path)})
    try:
        out, err = child.communicate(timeout=20)
        process = subprocess.CompletedProcess(argv, child.returncode, out, err)
    finally:
        if child.poll() is None and os.getpgid(child.pid) == child.pid:
            os.killpg(child.pid, signal.SIGTERM); child.wait(timeout=3)
        ledger = os.environ.get('T21A_PROCESS_LOG')
        if ledger:
            with open(ledger, 'a') as stream: stream.write(json.dumps({'pid': child.pid, 'pgid': child.pid, 'kind': 'installer-cli-test', 'returncode': child.returncode, 'reaped': child.returncode is not None}) + '\n')
    assert process.returncode != 0
    assert b'Traceback' not in process.stderr
    assert process.stdout.startswith(b'{'), 'missing script has no structured blocked result'
    result = json.loads(process.stdout)
    assert result['status'] in ('NOT_READY', 'INVALID')
    assert {str(p): p.stat().st_ino for p in tmp_path.rglob('*')} == before


def test_actual_candidate_is_not_ready_without_artifacts(installer, layout):
    actual = installer.Layout(layout.home, ROOT)
    result = installer.run(actual)
    assert result['status'] == 'NOT_READY'
    assert 'MISSING_BUNDLE' in result['reasons']
    assert 'MISSING_TOOLS' in result['reasons']
    assert result['discovered_tools'] == ['downloads_query']
    assert not list(layout.home.iterdir())
    assert not actual.runtime(result['commit']).exists()


def test_direct_requirements_do_not_require_media_helpers(installer):
    assert installer.required_executables() == {'aria2c': Path('/opt/homebrew/bin/aria2c')}


RAW = b'''# user header\nmodel: "keep quoted"  # user choice\nunknown: {nested: 'yes', number: 7}\nmcp_servers:\n  foreign: {command: "/usr/bin/true"}\nplugins:\n  enabled: ["other"] # existing IDs\n  disabled: ['unrelated']\n'''
ENTRY = {'command': '/usr/bin/env', 'args': ['/physical/python', '-I', '-B', '/physical/mcp'],
    'env': {'HERMES_DOWNLOADS_SOCKET': '/private/worker.sock'},
    'sampling': {'enabled': False}, 'tools': {'include': ['downloads_query']},
    'connect_timeout': 15, 'timeout': 30}


def test_yaml_round_trip_preserves_raw_unknown_comments_quotes(installer):
    after, added = installer.patch_config(RAW, ENTRY)
    assert added is True
    for fragment in [b'# user header', b'"keep quoted"  # user choice',
        b"{nested: 'yes', number: 7}", b'foreign: {command: "/usr/bin/true"}',
        b'# existing IDs', b"disabled: ['unrelated']"]:
        assert fragment in after
    assert installer.rollback_config(after, RAW, ENTRY, added) == RAW


@pytest.mark.parametrize('raw', [b'a: 1\na: 2\n', b'x: !!python/object:x {}\n',
    b'---\na: 1\n---\nb: 2\n', b'x: &a [*a]\n', b'plugins: nope\n',
    b'\tbad: yes\n', b'plugins: {enabled: [hermes-downloads]}\n'])
def test_ambiguous_yaml_and_unowned_plugin_choice_block(installer, raw):
    with pytest.raises(installer.Blocked):
        installer.patch_config(raw, ENTRY)


def test_disabled_backend_and_foreign_mcp_are_preserved(installer):
    for raw in (b'plugins: {disabled: [hermes-downloads]}\n',
        b'mcp_servers: {downloads: {command: foreign}}\n'):
        with pytest.raises(installer.Blocked):
            installer.patch_config(raw, ENTRY)


def test_selective_rollback_retains_unrelated_user_changes(installer):
    after, added = installer.patch_config(RAW, ENTRY)
    changed = after.replace(b'number: 7', b'number: 8') + b'new_setting: "new" # later\n'
    restored = installer.rollback_config(changed, RAW, ENTRY, added)
    assert b'number: 8' in restored and b'new_setting: "new" # later' in restored
    assert b'downloads:' not in restored and b'hermes-downloads' not in restored
    assert b'"keep quoted"  # user choice' in restored


def test_rollback_blocks_changed_owned_settings(installer):
    after, added = installer.patch_config(RAW, ENTRY)
    with pytest.raises(installer.Blocked):
        installer.rollback_config(after.replace(b'connect_timeout: 15', b'connect_timeout: 16'), RAW, ENTRY, added)


def test_atomic_write_rechecks_content_and_identity(installer, tmp_path):
    path = tmp_path / 'config'
    path.write_bytes(RAW); path.chmod(0o600)
    before = installer.snapshot(path)
    path.write_bytes(RAW + b'changed: true\n')
    with pytest.raises(installer.Blocked):
        installer.atomic_write(path, b'installer', before)
    assert path.read_bytes().endswith(b'changed: true\n')
    before = installer.snapshot(path)
    path.unlink(); path.write_bytes(RAW); path.chmod(0o600)
    with pytest.raises(installer.Blocked):
        installer.atomic_write(path, b'installer', before)
    assert path.read_bytes() == RAW


@pytest.mark.parametrize('kind', ['symlink', 'hardlink', 'public', 'fifo'])
def test_config_unsafe_objects_are_not_followed(installer, tmp_path, kind):
    path = tmp_path / 'config'; target = tmp_path / 'target'
    target.write_bytes(RAW); target.chmod(0o600)
    if kind == 'symlink': path.symlink_to(target)
    elif kind == 'hardlink': os.link(target, path)
    elif kind == 'fifo': os.mkfifo(path, 0o600)
    else: path.write_bytes(RAW); path.chmod(0o644)
    with pytest.raises(installer.Blocked): installer.snapshot(path)
    assert target.read_bytes() == RAW


def test_fsync_failure_never_reports_success(installer, tmp_path, monkeypatch):
    path = tmp_path / 'file'
    def fail(fd): raise OSError('synthetic sync fault')
    monkeypatch.setattr(installer.os, 'fsync', fail)
    with pytest.raises((OSError, installer.Blocked)):
        installer.atomic_write(path, b'new', None)
    assert not path.exists()


def ready_fixture(installer, layout, monkeypatch):
    """Synthetic logic-only plan. Never used by actual readiness tests."""
    runtime = layout.runtime(BASE)
    bundle = {'dashboard/manifest.json': b'{"name":"hermes-downloads","api":"plugin_api.py"}',
        'dashboard/plugin_api.py': b'# synthetic fixture only\n',
        'desktop/plugin.js': b'// synthetic fixture only\n'}
    plan = {'status': 'PLAN_READY', 'reasons': [], 'commit': BASE,
        'discovered_tools': list(installer.TOOLS), 'bundle': bundle, 'parity': {}}
    monkeypatch.setattr(installer, 'readiness', lambda *args: plan)
    def prepare(*args):
        runtime.mkdir(parents=True, mode=0o700)
        (runtime / 'bin').mkdir(mode=0o700)
        for name in ['python', 'hermes-downloads-worker', 'hermes-downloads-mcp']:
            (runtime / 'bin' / name).write_text('synthetic fixture')
        return {'synthetic': True}
    monkeypatch.setattr(installer, 'prepare_runtime', prepare)
    monkeypatch.setattr(installer, 'physical_probe', lambda *args: {'synthetic': True})
    monkeypatch.setattr(installer, 'ipc_readback', lambda *args: {'worker_epoch': 1, 'queue_gate': 'paused', 'admission_open': False, 'jobs_page': {'jobs': [], 'next_cursor': None}})
    return plan


class FakeLaunchctl:
    """Bounded service authority only; no actual process launch or signal."""
    def __init__(self): self.current = None; self.actions = []
    def __call__(self, action, layout, argv):
        self.actions.append(action)
        if action == 'bootstrap':
            self.current = {'label': 'com.mohamadsmt.hermes-downloads.default',
                'domain': f'gui/{os.getuid()}', 'plist': str(layout.plist), 'argv': argv, 'pid': 12345}
        elif action == 'bootout': self.current = None
        return self.current


def test_synthetic_apply_wires_only_default_and_selective_rollback(installer, layout, monkeypatch):
    ready_fixture(installer, layout, monkeypatch)
    layout.config.parent.mkdir(parents=True, mode=0o700)
    layout.config.write_bytes(RAW); layout.config.chmod(0o600)
    launch = FakeLaunchctl()
    result = installer.run(layout, 'apply', BASE, executor=launch)
    assert result['status'] == 'INSTALLED_WIRING', result
    manifest = Path(result['manifest'])
    assert manifest.stat().st_mode & 0o777 == 0o600
    data = plistlib.loads(layout.plist.read_bytes())
    args = data['ProgramArguments']
    assert args[:5] == ['/usr/bin/env', '-u', 'PYTHONPATH', '-u', 'PYTHONHOME']
    assert args[-3:] == ['--serve', '--state-root', str(layout.state)]
    assert '-I' in args and '-B' in args and '-c' not in args
    assert data['RunAtLoad'] is True
    config = installer.parse_yaml(layout.config.read_bytes())[1]
    assert config['mcp_servers']['downloads']['sampling'] == {'enabled': False}
    assert config['mcp_servers']['downloads']['tools']['include'] == list(installer.TOOLS)
    assert result['renderer_decision'] == 'NOT_OBSERVED'
    # User-owned files are retained, including changes to unrelated config.
    (layout.state / 'state.db').write_bytes(b'user queue')
    layout.output.mkdir(parents=True); (layout.output / 'payload').write_bytes(b'user payload')
    layout.config.write_bytes(layout.config.read_bytes() + b'later: "user" # retained\n')
    reverted = installer.run(layout, 'rollback', manifest=manifest, executor=launch)
    assert reverted['status'] == 'ROLLED_BACK'
    assert b'later: "user" # retained' in layout.config.read_bytes()
    assert (layout.state / 'state.db').read_bytes() == b'user queue'
    assert (layout.output / 'payload').read_bytes() == b'user payload'
    assert not layout.plist.exists()
    assert launch.actions == ['inspect', 'bootstrap', 'inspect', 'inspect', 'bootout', 'inspect']


@pytest.mark.parametrize('collision', ['plist', 'backend', 'renderer'])
def test_synthetic_collisions_block_before_install_effects(installer, layout, monkeypatch, collision):
    ready_fixture(installer, layout, monkeypatch)
    path = {'plist': layout.plist, 'backend': layout.backend / 'plugin_api.py',
        'renderer': layout.renderer}[collision]
    path.parent.mkdir(parents=True, mode=0o700); path.write_bytes(b'foreign'); path.chmod(0o600)
    launch = FakeLaunchctl()
    result = installer.run(layout, 'apply', BASE, executor=launch)
    assert result['status'] == 'NOT_READY'
    assert path.read_bytes() == b'foreign'
    assert not layout.config.exists() and not layout.runtime(BASE).exists()
    assert 'bootstrap' not in launch.actions


def test_synthetic_rollback_replaced_service_has_no_stop(installer, layout, monkeypatch):
    ready_fixture(installer, layout, monkeypatch)
    launch = FakeLaunchctl()
    result = installer.run(layout, 'apply', BASE, executor=launch)
    launch.current['pid'] += 1
    reverted = installer.run(layout, 'rollback', manifest=Path(result['manifest']), executor=launch)
    assert reverted['status'] == 'ROLLBACK_BLOCKED'
    assert 'bootout' not in launch.actions
    assert layout.plist.exists() and layout.config.exists()


def test_synthetic_rollback_user_file_change_is_retained(installer, layout, monkeypatch):
    ready_fixture(installer, layout, monkeypatch)
    launch = FakeLaunchctl()
    result = installer.run(layout, 'apply', BASE, executor=launch)
    layout.renderer.write_bytes(b'user changed renderer')
    reverted = installer.run(layout, 'rollback', manifest=Path(result['manifest']), executor=launch)
    assert reverted['status'] == 'ROLLBACK_BLOCKED'
    assert layout.renderer.read_bytes() == b'user changed renderer'


def test_strict_manifest_rejects_unknown_schema_fields(installer, tmp_path):
    for raw in [b'{"schema":99}', b'{"schema":1,"schema":1}', b'{"schema":true}']:
        path = tmp_path / 'manifest'; path.write_bytes(raw); path.chmod(0o600)
        with pytest.raises(installer.Blocked): installer.read_manifest(path)


def test_verifier_actual_missing_bundle_fails_closed_without_effects(installer, layout):
    assert VERIFY.is_file(), 'missing verifier observable CLI behavior'
    spec = importlib.util.spec_from_file_location('verify_installer', VERIFY)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    result = module.verify(installer.Layout(layout.home, ROOT))
    assert result['status'] == 'NOT_READY'
    assert 'MISSING_BUNDLE' in result['reasons'] and 'MISSING_TOOLS' in result['reasons']
    assert not list(layout.home.iterdir())


def test_long_socket_and_symlink_ancestors_block_without_artifacts(installer, tmp_path):
    home = tmp_path / ('x' * 90); home.mkdir(mode=0o700)
    layout = installer.Layout(home, tmp_path)
    entry, argv, _ = installer.wiring(layout, BASE)
    with pytest.raises(installer.Blocked, match='SOCKET_PATH_TOO_LONG'):
        installer.pre_effect(layout, entry, {}, FakeLaunchctl(), argv)
    assert not list(home.iterdir())
    alias = tmp_path / 'alias'; alias.symlink_to(home)
    with pytest.raises(installer.Blocked): installer.snapshot(alias / 'config')


@pytest.fixture
def committed_source_snapshot(installer, tmp_path):
    """Real current bytes in an independent, private, locally committed source tree."""
    import hashlib
    import stat
    import tomllib

    storage = tmp_path / 'committed-source'; storage.mkdir(mode=0o700)
    source = storage / 'source'; source.mkdir(mode=0o700)
    home = storage / 'home'; home.mkdir(mode=0o700)
    template = storage / 'template'; template.mkdir(mode=0o700)
    hooks = storage / 'hooks'; hooks.mkdir(mode=0o700)
    package = ROOT / 'headless/src/hermes_downloads'
    installer.chain(package)
    entries = sorted(package.rglob('*'))
    assert entries, 'missing package source inventory'
    for path in entries:
        assert not path.is_symlink(), f'symlink source entry: {path}'
        assert path.is_dir() or (stat.S_ISREG(path.stat().st_mode) and path.suffix == '.py'), f'extra source entry: {path}'
    modules = sorted(path.relative_to(package).as_posix() for path in entries if path.is_file())
    assert '__init__.py' in modules, 'missing package initializer'
    originals = [package / name for name in modules] + [ROOT / 'headless/uv.lock', ROOT / 'headless/pyproject.toml']
    hashes = {}
    for original in originals:
        installer.chain(original)
        assert stat.S_ISREG(original.stat().st_mode), f'missing regular source: {original}'
        relative = original.relative_to(ROOT).as_posix()
        copied = source / relative
        copied.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        raw = original.read_bytes(); copied.write_bytes(raw); copied.chmod(0o600)
        assert copied.read_bytes() == raw
        assert (copied.stat().st_dev, copied.stat().st_ino) != (original.stat().st_dev, original.stat().st_ino)
        assert copied.stat().st_nlink == 1
        hashes[relative] = hashlib.sha256(raw).hexdigest()
    env = {'HOME': str(home), 'PATH': '/usr/bin:/bin', 'GIT_CONFIG_NOSYSTEM': '1',
        'GIT_CONFIG_GLOBAL': '/dev/null', 'GIT_CONFIG_SYSTEM': '/dev/null', 'GIT_TERMINAL_PROMPT': '0',
        'GIT_AUTHOR_NAME': 'Parity Fixture', 'GIT_AUTHOR_EMAIL': 'parity@example.invalid',
        'GIT_COMMITTER_NAME': 'Parity Fixture', 'GIT_COMMITTER_EMAIL': 'parity@example.invalid'}
    def git(*args):
        argv = ['/usr/bin/git', '-c', 'core.hooksPath=' + str(hooks), '-c', 'commit.gpgSign=false', *args]
        child = subprocess.Popen(argv, cwd=source, env=env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        identity = {'pid': child.pid, 'pgid': child.pid, 'argv': argv, 'kind': 'private-fixture-git'}
        try:
            out, err = child.communicate(timeout=10)
        finally:
            if child.poll() is None and os.getpgid(child.pid) == child.pid:
                os.killpg(child.pid, signal.SIGTERM); child.wait(timeout=3)
            identity.update(returncode=child.returncode, reaped=child.returncode is not None)
            ledger = os.environ.get('T21A_PROCESS_LOG')
            if ledger:
                with open(ledger, 'a') as stream: stream.write(json.dumps(identity) + '\n')
        assert child.returncode == 0, err.decode(errors='replace')
        return out
    git('init', '--quiet', '--template=' + str(template))
    git('add', '--', *hashes)
    git('commit', '--quiet', '-m', 'Private current-source parity fixture')
    commit = git('rev-parse', 'HEAD').decode().strip()
    assert len(commit) == 40 and all(c in '0123456789abcdef' for c in commit)
    for original in originals:
        relative = original.relative_to(ROOT).as_posix()
        assert (source / relative).read_bytes() == original.read_bytes() == git('show', f'{commit}:{relative}')
    assert sorted(p.relative_to(source / 'headless/src/hermes_downloads').as_posix()
        for p in (source / 'headless/src/hermes_downloads').rglob('*.py')) == modules
    actual = installer.Layout(home, source)
    locked = {p['name']: p['version'] for p in tomllib.loads((source / 'headless/uv.lock').read_text())['package']}
    def probe(runtime):
        record = installer.physical_probe(actual, runtime, commit)
        assert record['python'] == [3, 12] and record['editable'] is False
        assert record['direct_url']['dir_info']['editable'] is False
        assert record['metadata_sha256']
        assert sorted(record['modules']) == sorted(record['module_parity']) == modules
        for name in modules:
            expected = hashes['headless/src/hermes_downloads/' + name]
            item = record['module_parity'][name]
            assert item['source'] == item['site'] == item['commit'] == expected
            copied = source / 'headless/src/hermes_downloads' / name
            original = package / name
            site = Path(item['site_path'])
            assert not site.is_symlink() and site.is_file() and site.is_relative_to(runtime)
            assert len({(p.stat().st_dev, p.stat().st_ino) for p in (original, copied, site)}) == 3
        assert record['entrypoints'] == {'hermes-downloads': 'hermes_downloads.cli:main',
            'hermes-downloads-mcp': 'hermes_downloads.mcp_server:main',
            'hermes-downloads-worker': 'hermes_downloads.service:worker_main'}
        assert set(record['executables']) == set(record['entrypoints'])
        for name, executable in record['executables'].items():
            assert executable['target'] == record['entrypoints'][name]
            assert executable['path'] == str(runtime / 'bin' / name)
        # This physical integration fixture targets the accepted macOS Python 3.12 lock.
        assert record['dependencies'] == {name: version for name, version in locked.items()
            if name not in {'colorama', 'pywin32'}}
        assert record['lock_sha256'] == hashes['headless/uv.lock']
        evidence = storage / 'parity.json'
        evidence.write_text(json.dumps({'commit': commit, 'modules': modules, 'source_hashes': hashes, 'parity': record}, sort_keys=True))
        evidence.chmod(0o600)
        return record
    return {'layout': actual, 'commit': commit, 'hashes': hashes, 'git': git, 'probe': probe}


def test_private_committed_source_snapshot_parity_and_tampered_site_rejected(installer, committed_source_snapshot, tmp_path):
    snapshot = committed_source_snapshot
    actual = snapshot['layout']; commit = snapshot['commit']
    snapshot['probe'](ROOT / 'headless/.venv')
    # Copy the actual physical runtime to a private canonical fixture, never edit BASE env.
    import shutil
    runtime = tmp_path / 'runtime'
    shutil.copytree(ROOT / 'headless/.venv', runtime, symlinks=True)
    for name in ['hermes-downloads', 'hermes-downloads-worker', 'hermes-downloads-mcp']:
        path = runtime / 'bin' / name
        path.write_text(path.read_text().replace(str(ROOT / 'headless/.venv'), str(runtime)))
    site = runtime / 'lib/python3.12/site-packages/hermes_downloads'
    (site / 'models.py').write_bytes((site / 'models.py').read_bytes() + b'\n# tampered\n')
    with pytest.raises(installer.Blocked, match='MODULE_PARITY_MISMATCH'): installer.physical_probe(actual, runtime, commit)


def test_private_committed_module_mismatch_rejected(installer, committed_source_snapshot):
    snapshot = committed_source_snapshot
    relative = 'headless/src/hermes_downloads/models.py'
    module = snapshot['layout'].source / relative
    original = module.read_bytes()
    changed = original + b'\n# different private committed module\n'
    module.write_bytes(changed)
    snapshot['git']('add', '--', relative)
    snapshot['git']('commit', '--quiet', '-m', 'Private mismatched committed module')
    commit = snapshot['git']('rev-parse', 'HEAD').decode().strip()
    assert commit != snapshot['commit']
    assert snapshot['git']('show', f'{commit}:{relative}') == changed
    module.write_bytes(original)
    site = ROOT / 'headless/.venv/lib/python3.12/site-packages/hermes_downloads/models.py'
    assert module.read_bytes() == (ROOT / relative).read_bytes() == site.read_bytes()
    with pytest.raises(installer.Blocked, match='MODULE_PARITY_MISMATCH'):
        installer.physical_probe(snapshot['layout'], ROOT / 'headless/.venv', commit)


@pytest.mark.parametrize('schema', [None, {'type': 'object', 'properties': {}},
    {'type': 'object', 'additionalProperties': False, 'properties': {'x': {}}},
    {'type': 'object', 'additionalProperties': False, 'properties': {'quality': {'type': 'string'}}},
    {'type': 'object', 'additionalProperties': False, 'properties': {'x': {'type': 'object', 'properties': {}}}}])
def test_untyped_open_placeholder_and_video_schemas_rejected(installer, schema):
    with pytest.raises(installer.Blocked): installer.check_schema(schema)


def test_ipc_missing_worker_is_failure_with_no_database_creation(installer, layout):
    layout.state.mkdir(parents=True, mode=0o700)
    before = list(layout.state.iterdir())
    with pytest.raises(Exception): installer.ipc_readback(layout)
    assert list(layout.state.iterdir()) == before


@pytest.mark.parametrize('fault', ['launch', 'health', 'config_race'])
def test_synthetic_failed_apply_retains_manifest_and_reports_failure(installer, layout, monkeypatch, fault):
    ready_fixture(installer, layout, monkeypatch)
    launch = FakeLaunchctl()
    if fault == 'launch':
        original = launch
        def execute(action, root, argv):
            if action == 'bootstrap': raise installer.Blocked('LAUNCHCTL_FAILED')
            return original(action, root, argv)
    else: execute = launch
    if fault == 'health':
        def fail(root): raise installer.Blocked('WORKER_UNAVAILABLE')
        monkeypatch.setattr(installer, 'ipc_readback', fail)
    if fault == 'config_race':
        prepare = installer.prepare_runtime
        def race(root, commit):
            value = prepare(root, commit)
            root.config.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            root.config.write_bytes(b'user: raced\n'); root.config.chmod(0o600)
            return value
        monkeypatch.setattr(installer, 'prepare_runtime', race)
    result = installer.run(layout, 'apply', BASE, executor=execute)
    assert result['status'] == 'INSTALL_FAILED', result
    assert Path(result['manifest']).is_file()
    if fault == 'config_race': assert layout.config.read_bytes() == b'user: raced\n'
    else: assert layout.config.exists()
    assert 'LIVE_PENDING' == result['live']


def test_synthetic_manifest_nested_unknown_field_rejected(installer, layout, monkeypatch):
    ready_fixture(installer, layout, monkeypatch)
    result = installer.run(layout, 'apply', BASE, executor=FakeLaunchctl())
    manifest = Path(result['manifest']); data = json.loads(manifest.read_bytes())
    data['config_after']['unknown'] = 'cannot be trusted'
    manifest.write_text(json.dumps(data))
    with pytest.raises(installer.Blocked): installer.read_manifest(manifest)


def test_synthetic_dry_run_ready_has_zero_files_or_launch_effects(installer, layout, monkeypatch):
    ready_fixture(installer, layout, monkeypatch)
    launch = FakeLaunchctl()
    result = installer.run(layout, executor=launch)
    assert result['status'] == 'PLAN_READY'
    assert not list(layout.home.iterdir()) and not layout.runtime(BASE).exists()
    assert launch.actions == ['inspect']


def test_synthetic_repeated_apply_reuses_only_manifest_certified_install(installer, layout, monkeypatch):
    ready_fixture(installer, layout, monkeypatch)
    launch = FakeLaunchctl()
    first = installer.run(layout, 'apply', BASE, executor=launch)
    before = {str(p): p.read_bytes() for p in layout.home.rglob('*') if p.is_file()}
    second = installer.run(layout, 'apply', BASE, executor=launch)
    assert second['status'] == 'INSTALLED_WIRING'
    assert second['manifest'] == first['manifest']
    assert {str(p): p.read_bytes() for p in layout.home.rglob('*') if p.is_file()} == before
    assert launch.actions.count('bootstrap') == 1


def test_placeholder_named_tool_rejected(installer):
    tool = {'name': 'downloads_add', 'description': 'placeholder', 'schema':
        {'type': 'object', 'additionalProperties': False, 'properties': {'x': {'type': 'string'}}, 'required': ['x']}}
    with pytest.raises(installer.Blocked): installer.check_tool(tool)


def test_failed_runtime_preparation_is_not_success(installer, layout, monkeypatch):
    ready_fixture(installer, layout, monkeypatch)
    def fail(*args): raise installer.Blocked('RUNTIME_PREPARATION_FAILED')
    monkeypatch.setattr(installer, 'prepare_runtime', fail)
    launch = FakeLaunchctl()
    result = installer.run(layout, 'apply', BASE, executor=launch)
    assert result['status'] == 'INSTALL_FAILED'
    assert not layout.config.exists() and not layout.plist.exists()
    assert launch.actions == ['inspect']


def test_synthetic_rollback_changed_owned_config_retains_files(installer, layout, monkeypatch):
    ready_fixture(installer, layout, monkeypatch)
    launch = FakeLaunchctl(); result = installer.run(layout, 'apply', BASE, executor=launch)
    layout.config.write_bytes(layout.config.read_bytes().replace(b'connect_timeout: 15', b'connect_timeout: 16'))
    before = layout.config.read_bytes()
    rollback = installer.run(layout, 'rollback', manifest=Path(result['manifest']), executor=launch)
    assert rollback['status'] == 'ROLLBACK_BLOCKED'
    assert layout.config.read_bytes() == before and layout.plist.exists()
    assert 'bootout' not in launch.actions


def test_parity_requires_no_dev_only_marker_library(installer, committed_source_snapshot, monkeypatch):
    import builtins
    original = builtins.__import__
    def restricted(name, *args, **kwargs):
        if name.startswith('packaging'): raise ImportError('dev-only library unavailable')
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, '__import__', restricted)
    result = committed_source_snapshot['probe'](ROOT / 'headless/.venv')
    assert result['python'] == [3, 12]


def _readiness_repository(layout):
    for args in [('init', '--quiet'), ('add', 'anchor.txt'),
                 ('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                  'commit', '--quiet', '-m', 'Synthetic readiness fixture')]:
        if args[0] == 'add':
            (layout.source / 'anchor.txt').write_text('tracked fixture\n')
        subprocess.run(['/usr/bin/git', *args], cwd=layout.source, check=True,
                       capture_output=True, timeout=10)
    handoff = layout.source / '.hermes/handoffs/retained fixture.md'
    handoff.parent.mkdir(parents=True, mode=0o700)
    handoff.write_text('Retained historical fixture.\n')
    handoff.chmod(0o600)
    return handoff


def test_readiness_allows_only_untracked_retained_handoff_markdown(installer, layout):
    handoff = _readiness_repository(layout)
    # This test's own byte read may change atime; retain all ownership/write fields.
    def identity():
        value = handoff.stat()
        return (value.st_dev, value.st_ino, value.st_mode, value.st_nlink,
            value.st_uid, value.st_gid, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
    contents = handoff.read_bytes()
    before = identity()
    result = installer.readiness(layout)
    assert 'SOURCE_DIRTY' not in result['reasons']
    assert result['status'] == 'NOT_READY'  # Other real prerequisites still absent.
    assert 'MISSING_BUNDLE' in result['reasons']
    assert handoff.read_bytes() == contents
    assert identity() == before
    assert not list(layout.home.iterdir())


@pytest.mark.parametrize('change', ['tracked', 'source', 'root', 'symlink', 'hardlink', 'nested'])
def test_readiness_retains_source_dirty_for_other_changes(installer, layout, change):
    handoff = _readiness_repository(layout)
    if change == 'tracked':
        (layout.source / 'anchor.txt').write_text('changed tracked fixture\n')
    elif change == 'source':
        source = layout.source / 'headless/src/extra.py'
        source.parent.mkdir(parents=True)
        source.write_text('# unknown source\n')
    elif change == 'root':
        (layout.source / 'extra.md').write_text('untracked outside handoffs\n')
    elif change == 'symlink':
        handoff.unlink()
        handoff.symlink_to(layout.source / 'anchor.txt')
    elif change == 'hardlink':
        handoff.unlink()
        os.link(layout.source / 'anchor.txt', handoff)
    else:
        nested = handoff.parent / 'nested/extra.md'
        nested.parent.mkdir()
        nested.write_text('nested untracked file\n')
    result = installer.readiness(layout)
    assert 'SOURCE_DIRTY' in result['reasons']
    assert result['status'] == 'NOT_READY'
    assert not list(layout.home.iterdir())


@pytest.mark.parametrize('manifest', [b'[]', b'null', b'7', b'"invalid"'])
def test_nonobject_bundle_manifest_returns_blocked_report(installer, layout, manifest):
    _readiness_repository(layout)
    bundle = layout.source / 'integrations/hermes-downloads'
    dashboard = bundle / 'dashboard'
    desktop = bundle / 'desktop'
    dashboard.mkdir(parents=True)
    desktop.mkdir()
    (dashboard / 'manifest.json').write_bytes(manifest)
    (dashboard / 'plugin_api.py').write_text('# synthetic parse-only fixture\n')
    (desktop / 'plugin.js').write_text("import {host} from '@hermes/plugin-sdk';\n")
    result = installer.run(layout)
    assert result['status'] == 'NOT_READY'
    assert 'BUNDLE_INVALID' in result['reasons']
    assert not list(layout.home.iterdir())



def test_direct_media_categories_values_and_descriptions_are_supported(installer):
    schema = {'type': 'object', 'additionalProperties': False,
        'properties': {'category': {'type': 'string', 'enum': ['Videos', 'Audio', 'Documents', 'Software', 'Other']},
            'filename': {'type': 'string', 'enum': ['video-quality-playlist.mp4'],
                'description': 'Download direct video bytes without extraction or FFmpeg.'}}}
    installer.check_schema(schema)


@pytest.mark.parametrize('field', ['quality', 'playlist_selection', 'audio_format', 'subtitles', 'video_options', 'ffmpeg'])
@pytest.mark.parametrize('location', ['root', 'array', 'definition'])
def test_nested_retired_extraction_option_fields_are_rejected(installer, field, location):
    options = {'type': 'object', 'additionalProperties': False,
        'properties': {field: {'type': 'string'}}}
    schema = {'type': 'object', 'additionalProperties': False, 'properties': {'options': options}}
    if location == 'array':
        schema['properties']['options'] = {'type': 'array', 'items': options}
    elif location == 'definition':
        schema['properties']['options'] = {'$ref': '#/$defs/options'}
        schema['$defs'] = {'options': options}
    with pytest.raises(installer.Blocked, match='MEDIA_SCHEMA_UNSUPPORTED'):
        installer.check_schema(schema)


def test_private_read_accepts_access_time_only_descriptor_change(installer, tmp_path, monkeypatch):
    path = tmp_path / 'config'; path.write_bytes(RAW); path.chmod(0o600)
    real_fstat = os.fstat
    calls = 0
    def access_time_view(fd):
        nonlocal calls
        value = real_fstat(fd); calls += 1
        fields = {name: getattr(value, name) for name in dir(value) if name.startswith('st_')}
        if calls > 1:
            fields['st_atime'] += 1
            fields['st_atime_ns'] += 1_000_000_000
        return SimpleNamespace(**fields)
    monkeypatch.setattr(installer.os, 'fstat', access_time_view)
    raw, record = installer.read_file(path)
    assert raw == RAW and record['sha256'] == installer.digest(RAW)
    assert calls == 2


@pytest.mark.parametrize('field', ['st_dev', 'st_ino', 'st_mode', 'st_nlink',
    'st_uid', 'st_gid', 'st_size', 'st_mtime_ns', 'st_ctime_ns'])
def test_private_read_refuses_descriptor_write_or_ownership_change(installer, tmp_path, monkeypatch, field):
    path = tmp_path / 'config'; path.write_bytes(RAW); path.chmod(0o600)
    real_fstat = os.fstat
    calls = 0
    def changed_view(fd):
        nonlocal calls
        value = real_fstat(fd); calls += 1
        fields = {name: getattr(value, name) for name in dir(value) if name.startswith('st_')}
        if calls > 1: fields[field] += 1
        return SimpleNamespace(**fields)
    monkeypatch.setattr(installer.os, 'fstat', changed_view)
    with pytest.raises(installer.Blocked, match='^CONCURRENT_EDIT$'):
        installer.read_file(path)


@pytest.mark.parametrize('reference', ['#/$defs/Item', '#/definitions/Item'])
def test_closed_typed_local_reference_is_supported(installer, reference):
    definitions = reference.split('/')[1]
    schema = {'type': 'object', 'additionalProperties': False,
        'properties': {'items': {'type': 'array', 'items': {'$ref': reference}}},
        definitions: {'Item': {'type': 'object', 'additionalProperties': False,
            'properties': {'url': {'type': 'string'}, 'category': {'type': 'string', 'enum': ['Videos', 'Audio']}}}}}
    installer.check_schema(schema)


@pytest.mark.parametrize('kind', ['open', 'untyped', 'dangling', 'external', 'cycle'])
def test_referenced_item_must_be_closed_typed_and_resolvable(installer, kind):
    item = {'type': 'object', 'additionalProperties': False,
        'properties': {'url': {'type': 'string'}}}
    schema = {'type': 'object', 'additionalProperties': False,
        'properties': {'items': {'type': 'array', 'items': {'$ref': '#/$defs/Item'}}},
        '$defs': {'Item': item}}
    if kind == 'open': item.pop('additionalProperties')
    elif kind == 'untyped': item['properties']['url'] = {}
    elif kind == 'dangling': schema['$defs'] = {}
    elif kind == 'external': schema['properties']['items']['items']['$ref'] = 'https://example.invalid/schema'
    else: schema['$defs']['Item'] = {'$ref': '#/$defs/Item'}
    with pytest.raises(installer.Blocked, match='^TOOL_SCHEMA_INVALID$'):
        installer.check_schema(schema)
