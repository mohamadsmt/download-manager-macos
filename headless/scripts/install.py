#!/usr/bin/env python3
"""Default-profile installer. Read-only unless an exact commit is explicitly applied."""
from __future__ import annotations

import argparse
import ast
import asyncio
from dataclasses import dataclass
import hashlib
import io
import json
import os
from pathlib import Path
import plistlib
import pwd
import re
import signal
import stat
import subprocess
import sys
import tempfile
import tomllib
import uuid

from ruamel.yaml import YAML
from ruamel.yaml.tokens import AliasToken, AnchorToken, TagToken

CANONICAL = Path('/Users/mohamadsmt/Documents/Download Manager')
TOOLS = ('downloads_add', 'downloads_query', 'downloads_control', 'downloads_edit',
         'downloads_replace_source', 'downloads_configure', 'downloads_files')
LABEL = 'com.mohamadsmt.hermes-downloads.default'
UV = Path('/opt/homebrew/bin/uv')
MAX_BYTES = 1_048_576


class Blocked(ValueError):
    """Only fixed redacted codes may cross the CLI boundary."""


@dataclass(frozen=True)
class Layout:
    home: Path
    source: Path = CANONICAL

    @property
    def config(self): return self.home / '.hermes/config.yaml'
    @property
    def state(self): return self.home / 'Library/Application Support/HermesDownloadManager/default'
    @property
    def socket(self): return self.state / 'worker.sock'
    @property
    def output(self): return self.home / 'Downloads/Hermes'
    @property
    def plist(self): return self.home / 'Library/LaunchAgents' / (LABEL + '.plist')
    @property
    def backend(self): return self.home / '.hermes/plugins/hermes-downloads/dashboard'
    @property
    def renderer(self): return self.home / '.hermes/desktop-plugins/hermes-downloads/plugin.js'
    @property
    def evidence(self): return self.home / '.hermes/installations/hermes-downloads'
    def runtime(self, commit): return self.source / '.artifacts/download-manager/runtime' / commit
    def files(self):
        return {'dashboard/manifest.json': self.backend / 'manifest.json',
                'dashboard/plugin_api.py': self.backend / 'plugin_api.py',
                'desktop/plugin.js': self.renderer}


def digest(raw): return hashlib.sha256(raw).hexdigest()


def chain(path):
    if not path.is_absolute() or '..' in path.parts or '.' in path.parts:
        raise Blocked('PATH_INVALID')
    for parent in [*reversed(path.parents), path]:
        try: info = parent.lstat()
        except FileNotFoundError: continue
        if stat.S_ISLNK(info.st_mode): raise Blocked('PATH_SYMLINK')
        if parent != path and not stat.S_ISDIR(info.st_mode): raise Blocked('PATH_INVALID')


def private_dir(path, create=False):
    chain(path)
    if not path.exists():
        if not create: return
        missing = []
        ancestor = path
        while not ancestor.exists():
            missing.append(ancestor); ancestor = ancestor.parent
        if ancestor.lstat().st_uid != os.getuid(): raise Blocked('DIRECTORY_UNSAFE')
        for directory in reversed(missing): directory.mkdir(mode=0o700)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise Blocked('DIRECTORY_UNSAFE')


def read_file(path, private=True):
    chain(path)
    try: fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError: return None, None
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or
            (private and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600)) or
            info.st_size > MAX_BYTES): raise Blocked('FILE_UNSAFE')
        raw = bytearray()
        while len(raw) <= MAX_BYTES:
            chunk = os.read(fd, min(65536, MAX_BYTES + 1 - len(raw)))
            if not chunk: break
            raw.extend(chunk)
        end = os.fstat(fd)
        if len(raw) > MAX_BYTES or info != end or path.lstat() != end:
            raise Blocked('CONCURRENT_EDIT')
        record = {'dev': info.st_dev, 'ino': info.st_ino, 'uid': info.st_uid,
            'mode': stat.S_IMODE(info.st_mode), 'size': info.st_size,
            'mtime_ns': info.st_mtime_ns, 'sha256': digest(raw)}
        return bytes(raw), record
    finally: os.close(fd)


def snapshot(path): return read_file(path)[1]


def atomic_write(path, raw, expected):
    """Owner-only compare/recheck, descriptor-relative replacement and readback."""
    private_dir(path.parent)
    parent_before = path.parent.stat()
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    name = '.installer-' + uuid.uuid4().hex
    temporary = None
    try:
        temporary = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
        with os.fdopen(temporary, 'wb', closefd=False) as stream:
            stream.write(raw); stream.flush(); os.fsync(temporary)
        if path.parent.stat() != parent_before and (path.parent.stat().st_ino, path.parent.stat().st_dev) != (parent_before.st_ino, parent_before.st_dev):
            raise Blocked('CONCURRENT_EDIT')
        if snapshot(path) != expected: raise Blocked('CONCURRENT_EDIT')
        os.replace(name, path.name, src_dir_fd=fd, dst_dir_fd=fd)
        os.fsync(fd)
        installed = snapshot(path)
        if installed['sha256'] != digest(raw): raise Blocked('WRITE_READBACK_FAILED')
        return installed
    finally:
        if temporary is not None: os.close(temporary)
        try: os.unlink(name, dir_fd=fd)
        except FileNotFoundError: pass
        os.close(fd)


def remove_exact(path, expected):
    private_dir(path.parent)
    if snapshot(path) != expected: raise Blocked('OWNED_FILE_CHANGED')
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if snapshot(path) != expected: raise Blocked('CONCURRENT_EDIT')
        os.unlink(path.name, dir_fd=fd); os.fsync(fd)
    finally: os.close(fd)


def dump_yaml(parser, data):
    stream = io.StringIO(); parser.dump(data, stream)
    return stream.getvalue().encode('utf-8')


def parse_yaml(raw):
    parser = YAML(typ='rt'); parser.preserve_quotes = True; parser.allow_duplicate_keys = False
    try:
        text = raw.decode('utf-8')
        if any(isinstance(token, (AliasToken, AnchorToken, TagToken)) for token in parser.scan(text)):
            raise Blocked('YAML_UNSUPPORTED')
        data = parser.load(text) if raw else {}
        if not isinstance(data, dict): raise Blocked('YAML_UNSUPPORTED')
        if raw and dump_yaml(parser, data) != raw: raise Blocked('YAML_BYTE_PRESERVATION_UNPROVEN')
        for name in ('plugins', 'mcp_servers'):
            if name in data and not isinstance(data[name], dict): raise Blocked('YAML_UNSUPPORTED')
        for name in ('enabled', 'disabled'):
            values = data.get('plugins', {}).get(name, [])
            if not isinstance(values, list) or any(not isinstance(x, str) for x in values) or len(values) != len(set(values)):
                raise Blocked('YAML_UNSUPPORTED')
        return parser, data
    except Blocked: raise
    except Exception as error: raise Blocked('YAML_INVALID') from error


def patch_config(raw, entry):
    parser, data = parse_yaml(raw)
    plugins = data.setdefault('plugins', {})
    if 'hermes-downloads' in plugins.get('disabled', []): raise Blocked('BACKEND_EXPLICITLY_DISABLED')
    servers = data.setdefault('mcp_servers', {})
    if 'downloads' in servers and servers['downloads'] != entry: raise Blocked('MCP_COLLISION')
    enabled = plugins.setdefault('enabled', [])
    if 'hermes-downloads' in enabled and 'downloads' not in servers: raise Blocked('PLUGIN_COLLISION')
    added = 'hermes-downloads' not in enabled
    servers['downloads'] = entry
    if added: enabled.append('hermes-downloads')
    after = dump_yaml(parser, data)
    # Validate emitted settings rather than assuming a successful dump is safe.
    if parse_yaml(after)[1]['mcp_servers']['downloads'] != entry: raise Blocked('YAML_READBACK_FAILED')
    return after, added


def rollback_config(raw, before, entry, added):
    parser, data = parse_yaml(raw)
    _, original = parse_yaml(before)
    if data.get('mcp_servers', {}).get('downloads') != entry: raise Blocked('OWNED_CONFIG_CHANGED')
    enabled = data.get('plugins', {}).get('enabled', [])
    if added and 'hermes-downloads' not in enabled: raise Blocked('OWNED_CONFIG_CHANGED')
    installed, _ = patch_config(before, entry)
    if raw == installed: return before
    if 'downloads' in original.get('mcp_servers', {}):
        data['mcp_servers']['downloads'] = original['mcp_servers']['downloads']
    else:
        del data['mcp_servers']['downloads']
        if not data['mcp_servers'] and 'mcp_servers' not in original: del data['mcp_servers']
    if added: enabled.remove('hermes-downloads')
    if not enabled and 'enabled' not in original.get('plugins', {}): del data['plugins']['enabled']
    if not data['plugins'] and 'plugins' not in original: del data['plugins']
    return dump_yaml(parser, data)


def clean_env(home):
    env = {'HOME': str(home), 'PATH': '/usr/bin:/bin', 'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONNOUSERSITE': '1'}
    if os.environ.get('T21A_PROCESS_LOG'): env['T21A_PROCESS_LOG'] = os.environ['T21A_PROCESS_LOG']
    return env


def bounded(argv, home, cwd, timeout=20):
    """One owned child/group; uncertainty is retained, never repeatedly signalled."""
    process = subprocess.Popen([str(x) for x in argv], cwd=cwd, env=clean_env(home),
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    identity = {'pid': process.pid, 'pgid': os.getpgid(process.pid), 'argv': [str(x) for x in argv]}
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        if process.poll() is None and os.getpgid(process.pid) == identity['pgid']:
            os.killpg(identity['pgid'], signal.SIGTERM)
        try: process.wait(timeout=3)
        except subprocess.TimeoutExpired: raise Blocked('PROCESS_EXIT_UNCERTAIN')
        raise Blocked('PROCESS_TIMEOUT')
    finally:
        identity.update(returncode=process.returncode, reaped=process.returncode is not None)
        ledger = os.environ.get('T21A_PROCESS_LOG')
        if ledger:
            with open(ledger, 'a', encoding='utf-8') as stream: stream.write(json.dumps(identity) + '\n')
    if len(stdout) > 4 * MAX_BYTES or len(stderr) > MAX_BYTES: raise Blocked('PROCESS_OUTPUT_LIMIT')
    return process.returncode, stdout, stderr


def git(source, *args):
    code, out, _ = bounded(['/usr/bin/git', '--no-optional-locks', '-C', source, *args], source, source)
    if code: raise Blocked('SOURCE_UNAVAILABLE')
    return out


def source_has_changes(source):
    """Retained untracked handoff documents are never executable source."""
    entries = git(source, 'status', '--porcelain', '-z', '--untracked-files=all').split(b'\0')
    for entry in entries:
        if not entry:
            continue
        if not entry.startswith(b'?? '):
            return True
        relative = Path(os.fsdecode(entry[3:]))
        if (len(relative.parts) != 3 or relative.parts[:2] != ('.hermes', 'handoffs')
                or relative.suffix != '.md'):
            return True
        path = source / relative
        try:
            chain(path)
            info = path.lstat()
        except (OSError, Blocked):
            return True
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_nlink != 1):
            return True
    return False


def required_executables(): return {'aria2c': Path('/opt/homebrew/bin/aria2c')}


def check_schema(schema):
    if not isinstance(schema, dict) or schema.get('type') != 'object' or schema.get('additionalProperties') is not False:
        raise Blocked('TOOL_SCHEMA_INVALID')
    if not isinstance(schema.get('properties'), dict) or not schema['properties']:
        raise Blocked('TOOL_SCHEMA_INVALID')
    def typed(value):
        if not isinstance(value, dict): raise Blocked('TOOL_SCHEMA_INVALID')
        if not any(key in value for key in ('type', '$ref', 'anyOf', 'oneOf', 'allOf', 'const', 'enum')):
            raise Blocked('TOOL_SCHEMA_INVALID')
        if value.get('type') == 'object':
            if value.get('additionalProperties') is not False: raise Blocked('TOOL_SCHEMA_INVALID')
            for child in value.get('properties', {}).values(): typed(child)
        if value.get('type') == 'array': typed(value.get('items'))
        for key in ('anyOf', 'oneOf', 'allOf'):
            for child in value.get(key, []): typed(child)
    for value in schema['properties'].values(): typed(value)
    retired_fields = {'quality', 'video_quality', 'video_options', 'media_options',
        'playlist', 'playlist_selection', 'playlist_positions', 'audio', 'audio_format',
        'subtitles', 'subtitle', 'video_format', 'format_selection', 'cookies',
        'cookie_grant', 'extractor', 'extractor_options', 'yt_dlp', 'ffmpeg', 'ffprobe'}
    def check_extraction_options(value):
        if not isinstance(value, dict): return
        properties = value.get('properties', {})
        if isinstance(properties, dict):
            for name, child in properties.items():
                if name in retired_fields: raise Blocked('MEDIA_SCHEMA_UNSUPPORTED')
                check_extraction_options(child)
        for key in ('$defs', 'definitions', 'patternProperties'):
            children = value.get(key, {})
            if isinstance(children, dict):
                for child in children.values(): check_extraction_options(child)
        for key in ('items', 'additionalProperties', 'not', 'if', 'then', 'else'):
            check_extraction_options(value.get(key))
        for key in ('anyOf', 'oneOf', 'allOf', 'prefixItems'):
            children = value.get(key, [])
            if isinstance(children, list):
                for child in children: check_extraction_options(child)
    check_extraction_options(schema)


def check_tool(tool):
    if not isinstance(tool, dict) or tool.get('name') not in TOOLS: raise Blocked('TOOL_SCHEMA_INVALID')
    check_schema(tool.get('schema'))
    required = {
        'downloads_add': {'items', 'request_id'}, 'downloads_query': {'scope'},
        'downloads_control': {'action', 'scope', 'request_id'}, 'downloads_edit': {'ids', 'patch', 'request_id'},
        'downloads_replace_source': {'id', 'url', 'request_id'}, 'downloads_configure': {'request_id'},
        'downloads_files': {'action', 'ids'}}[tool['name']]
    schema = tool['schema']
    if not required.issubset(schema.get('required', [])) or not required.issubset(schema['properties']): raise Blocked('TOOL_PLACEHOLDER_OR_SCHEMA_INVALID')
    if not isinstance(tool.get('description'), str) or not tool['description'].strip() or 'placeholder' in tool['description'].lower(): raise Blocked('TOOL_PLACEHOLDER_OR_SCHEMA_INVALID')


def physical_probe(layout, runtime, commit):
    """Run this same script with the installed isolated interpreter; no -c code."""
    with tempfile.TemporaryDirectory(prefix='hermes-installer-probe-') as directory:
        code, out, _ = bounded([runtime / 'bin/python', '-I', '-B', Path(__file__).absolute(),
            '--internal-probe', str(runtime)], layout.home, directory, 25)
    if code: raise Blocked('PHYSICAL_RUNTIME_INVALID')
    try: record = json.loads(out)
    except Exception as error: raise Blocked('PHYSICAL_RUNTIME_INVALID') from error
    if record['python'] != [3, 12] or record['editable']: raise Blocked('PHYSICAL_RUNTIME_INVALID')
    source = layout.source / 'headless/src/hermes_downloads'
    inventory = sorted(path.relative_to(source).as_posix() for path in source.rglob('*.py'))
    if inventory != sorted(record['modules']): raise Blocked('MODULE_INVENTORY_MISMATCH')
    modules = {}
    for name in inventory:
        path = source / name
        chain(path)
        source_hash = digest(path.read_bytes())
        committed = digest(git(layout.source, 'show', f'{commit}:headless/src/hermes_downloads/{name}'))
        item = record['modules'][name]
        if source_hash != item['sha256'] or source_hash != committed or item['symlink'] or item['inode'] == [path.stat().st_dev, path.stat().st_ino]:
            raise Blocked('MODULE_PARITY_MISMATCH')
        modules[name] = {'source': source_hash, 'site': item['sha256'], 'commit': committed, 'site_path': item['path']}
    expected = {'hermes-downloads': 'hermes_downloads.cli:main',
        'hermes-downloads-mcp': 'hermes_downloads.mcp_server:main',
        'hermes-downloads-worker': 'hermes_downloads.service:worker_main'}
    if record['entrypoints'] != expected: raise Blocked('ENTRYPOINT_MISMATCH')
    lock = tomllib.loads((layout.source / 'headless/uv.lock').read_text())
    packages = {p['name']: p for p in lock['package']}
    required = set()
    pending = [d['name'] for d in packages['hermes-downloads']['dependencies']]
    # ponytail: only the accepted lock's five markers; unknown markers fail closed.
    markers = {"sys_platform == 'win32'": sys.platform == 'win32',
        "sys_platform != 'emscripten'": sys.platform != 'emscripten',
        "implementation_name != 'PyPy'": sys.implementation.name != 'PyPy',
        "platform_python_implementation != 'PyPy'": sys.implementation.name != 'pypy',
        "platform_python_implementation == 'CPython'": sys.implementation.name == 'cpython'}
    def relevant(dependency):
        marker = dependency.get('marker')
        if marker is None: return True
        if marker not in markers: raise Blocked('LOCK_MARKER_UNSUPPORTED')
        return markers[marker]
    while pending:
        name = pending.pop()
        if name in required: continue
        required.add(name)
        package = packages[name]
        pending.extend(d['name'] for d in package.get('dependencies', []) if relevant(d))
        for group in package.get('optional-dependencies', {}).values():
            pending.extend(d['name'] for d in group if relevant(d))
    for package in lock['package']:
        if package['name'] in required:
            if record['dependencies'].get(package['name']) != package['version']:
                raise Blocked('LOCK_DEPENDENCY_MISMATCH')
    record['module_parity'] = modules; record['lock_sha256'] = digest((layout.source / 'headless/uv.lock').read_bytes())
    return record


def internal_probe(runtime):
    import importlib.metadata as metadata
    import hermes_downloads
    site = Path(hermes_downloads.__file__).parent
    if site.is_symlink() or 'site-packages' not in site.parts or not site.is_relative_to(runtime): raise Blocked('PHYSICAL_RUNTIME_INVALID')
    dist = metadata.distribution('hermes-downloads')
    direct = json.loads(dist.read_text('direct_url.json') or '{}')
    modules = {}
    for path in site.rglob('*.py'):
        modules[path.relative_to(site).as_posix()] = {'sha256': digest(path.read_bytes()), 'path': str(path),
            'symlink': path.is_symlink(), 'inode': [path.stat().st_dev, path.stat().st_ino]}
    entrypoints = {x.name: x.value for x in dist.entry_points if x.group == 'console_scripts'}
    executable_records = {}
    for name in entrypoints:
        path = runtime / 'bin' / name
        if path.is_symlink() or not path.is_file() or not path.read_bytes().startswith(('#!' + str(runtime / 'bin/python')).encode()):
            # uv uses an absolute /bin/sh trampoline for paths containing spaces.
            text = path.read_text()
            if str(runtime / 'bin/python') not in text or entrypoints[name].split(':')[0] not in text:
                raise Blocked('ENTRYPOINT_MISMATCH')
        module, function = entrypoints[name].split(':')
        tree = ast.parse(path.read_text())
        imports = [(x.module, tuple(a.name for a in x.names)) for x in ast.walk(tree) if isinstance(x, ast.ImportFrom)]
        if imports != [(module, (function,))]: raise Blocked('ENTRYPOINT_MISMATCH')
        executable_records[name] = {'path': str(path), 'sha256': digest(path.read_bytes()), 'target': entrypoints[name]}
    dependencies = {re.sub(r'[-_.]+', '-', d.metadata['Name']).lower(): d.version for d in metadata.distributions()}
    record = {'python': list(sys.version_info[:2]), 'site': str(site), 'editable': direct.get('dir_info', {}).get('editable', False),
        'metadata_sha256': digest((dist.read_text('METADATA') or '').encode()), 'direct_url': direct, 'modules': modules, 'entrypoints': entrypoints, 'dependencies': dependencies,
        'executables': executable_records, 'interpreter_sha256': digest((runtime / 'bin/python').read_bytes()), 'interpreter': str(runtime / 'bin/python'), 'interpreter_link': os.readlink(runtime / 'bin/python') if (runtime / 'bin/python').is_symlink() else None}
    print(json.dumps(record)); return 0


async def sdk_tools(runtime, home):
    from mcp import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client
    import mcp.client.stdio as transport
    original_create = transport._create_platform_compatible_process
    tracked = []
    async def record_create(*args, **kwargs):
        child = await original_create(*args, **kwargs)
        tracked.append((child, {'pid': child.pid, 'pgid': os.getpgid(child.pid), 'kind': 'official-sdk-stdio'}))
        return child
    transport._create_platform_compatible_process = record_create
    parameters = StdioServerParameters(command='/usr/bin/env',
        args=['-u', 'PYTHONPATH', '-u', 'PYTHONHOME', str(runtime / 'bin/python'), '-I', '-B', str(runtime / 'bin/hermes-downloads-mcp')],
        env=clean_env(home))
    try:
        async with asyncio.timeout(15):
            async with stdio_client(parameters) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    result = await session.list_tools()
                    return [{'name': tool.name, 'description': tool.description, 'schema': tool.inputSchema} for tool in result.tools]
    finally:
        transport._create_platform_compatible_process = original_create
        ledger = os.environ.get('T21A_PROCESS_LOG')
        if ledger:
            with open(ledger, 'a', encoding='utf-8') as stream:
                for child, identity in tracked:
                    identity.update(returncode=child.returncode, reaped=child.returncode is not None)
                    stream.write(json.dumps(identity) + '\n')


def discover(layout, runtime):
    with tempfile.TemporaryDirectory(prefix='hermes-installer-sdk-') as directory:
        code, out, _ = bounded([runtime / 'bin/python', '-I', '-B', Path(__file__).absolute(),
            '--internal-discover', str(runtime), str(layout.home)], layout.home, directory, 22)
    if code: raise Blocked('SDK_DISCOVERY_FAILED')
    try: return json.loads(out)
    except Exception as error: raise Blocked('SDK_DISCOVERY_FAILED') from error


def readiness(layout, expected=None):
    reasons = []; record = {'status': 'NOT_READY', 'reasons': reasons, 'commit': None, 'discovered_tools': [], 'parity': {}, 'bundle': {}}
    try:
        chain(layout.source); chain(layout.home)
        commit = git(layout.source, 'rev-parse', 'HEAD').decode().strip(); record['commit'] = commit
        if expected is not None and expected != commit: reasons.append('COMMIT_MISMATCH')
        if source_has_changes(layout.source): reasons.append('SOURCE_DIRTY')
        for name, path in required_executables().items():
            if not path.is_file() or not os.access(path, os.X_OK): reasons.append('DIRECT_DEPENDENCY_MISSING')
            else: chain(path.parent)
        bundle = layout.source / 'integrations/hermes-downloads'
        for name in layout.files():
            raw, _ = read_file(bundle / name, private=False)
            if raw is None: reasons.append('MISSING_BUNDLE'); break
            record['bundle'][name] = raw
        if len(record['bundle']) == 3:
            manifest = strict_json(record['bundle']['dashboard/manifest.json'])
            if (not isinstance(manifest, dict) or manifest.get('name') != 'hermes-downloads'
                    or manifest.get('api') != 'plugin_api.py'): reasons.append('BUNDLE_INVALID')
            ast.parse(record['bundle']['dashboard/plugin_api.py'].decode())
            renderer = record['bundle']['desktop/plugin.js'].decode()
            if '@hermes/plugin-sdk' not in renderer or 'require(' in renderer: reasons.append('BUNDLE_INVALID')
            # Exact expected commit is the caller's accepted, independently reviewed bundle.
        runtime = layout.source / 'headless/.venv'
        if not runtime.is_dir(): reasons.append('CANDIDATE_ENV_ABSENT')
        else:
            try: record['parity'] = physical_probe(layout, runtime, commit)
            except Blocked as error: reasons.append(str(error))
            try:
                tools = discover(layout, runtime)
                names = [tool['name'] for tool in tools]; record['discovered_tools'] = names
                if sorted(names) != sorted(TOOLS): reasons.append('MISSING_TOOLS')
                for tool in tools: check_tool(tool)
            except Blocked as error: reasons.append(str(error))
    except (OSError, ValueError, KeyError, SyntaxError) as error:
        reasons.append(str(error) if isinstance(error, Blocked) else 'PREFLIGHT_INVALID')
    record['reasons'] = sorted(set(reasons))
    if not record['reasons']: record['status'] = 'PLAN_READY'
    return record


def argv_for(layout, commit, name):
    runtime = layout.runtime(commit)
    return ['/usr/bin/env', '-u', 'PYTHONPATH', '-u', 'PYTHONHOME', str(runtime / 'bin/python'), '-I', '-B', str(runtime / 'bin' / name)]


def wiring(layout, commit):
    argv = argv_for(layout, commit, 'hermes-downloads-mcp')
    entry = {'command': argv[0], 'args': argv[1:], 'env': {'HERMES_DOWNLOADS_SOCKET': str(layout.socket)},
        'tools': {'include': list(TOOLS)}, 'sampling': {'enabled': False}, 'connect_timeout': 15, 'timeout': 30}
    worker = argv_for(layout, commit, 'hermes-downloads-worker') + ['--serve', '--state-root', str(layout.state)]
    plist = {'Label': LABEL, 'ProgramArguments': worker, 'RunAtLoad': True,
        'KeepAlive': {'SuccessfulExit': False}, 'ThrottleInterval': 10,
        'WorkingDirectory': str(layout.state), 'EnvironmentVariables': clean_env(layout.home),
        'StandardOutPath': str(layout.state / 'logs/worker.stdout.log'),
        'StandardErrorPath': str(layout.state / 'logs/worker.stderr.log')}
    return entry, worker, plistlib.dumps(plist, sort_keys=True)


def pre_effect(layout, entry, bundle, executor, argv):
    if len(os.fsencode(layout.socket)) > 103: raise Blocked('SOCKET_PATH_TOO_LONG')
    for path in (layout.config, layout.plist, layout.state, layout.output, *layout.files().values(), layout.evidence): chain(path)
    for path in (layout.state, layout.state / 'logs', layout.config.parent, layout.backend.parent, layout.backend, layout.renderer.parent, layout.evidence): private_dir(path)
    for name in ('worker.stdout.log', 'worker.stderr.log'): snapshot(layout.state / 'logs' / name)
    if layout.socket.exists() or layout.socket.is_symlink() or (layout.state / '.worker-endpoint.json').exists():
        raise Blocked('ENDPOINT_REQUIRES_SERVICE_REVIEW')
    raw, before = read_file(layout.config)
    after, added = patch_config(raw or b'', entry)
    if before is not None and raw == after: raise Blocked('EXISTING_INSTALL_REQUIRES_MANIFEST')
    for path in (layout.plist, *layout.files().values()):
        if snapshot(path) is not None: raise Blocked('FILE_COLLISION')
    if executor('inspect', layout, argv) is not None: raise Blocked('SERVICE_COLLISION')
    return raw or b'', before, after, added


def prepare_runtime(layout, commit):
    runtime = layout.runtime(commit)
    if runtime.exists(): raise Blocked('RUNTIME_COLLISION')
    private_dir(runtime.parent, create=True)
    env = clean_env(layout.home); env['UV_PROJECT_ENVIRONMENT'] = str(runtime)
    process = subprocess.Popen([str(UV), 'sync', '--project', str(layout.source / 'headless'), '--python', '3.12',
        '--locked', '--no-editable', '--no-dev', '--reinstall-package', 'hermes-downloads'],
        env=env, cwd=layout.source, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try: out, err = process.communicate(timeout=120)
    except subprocess.TimeoutExpired:
        if process.poll() is None and os.getpgid(process.pid) == process.pid: os.killpg(process.pid, signal.SIGTERM)
        try: process.wait(timeout=3)
        except subprocess.TimeoutExpired: raise Blocked('PROCESS_EXIT_UNCERTAIN')
        raise Blocked('RUNTIME_PREPARATION_TIMEOUT')
    if process.returncode: raise Blocked('RUNTIME_PREPARATION_FAILED')
    private_dir(runtime)
    return physical_probe(layout, runtime, commit)


def launchctl(action, layout, argv):
    domain = f'gui/{os.getuid()}'; target = domain + '/' + LABEL
    arguments = {'inspect': ['print', target], 'bootstrap': ['bootstrap', domain, str(layout.plist)], 'bootout': ['bootout', target]}[action]
    code, out, err = bounded(['/bin/launchctl', *arguments], layout.home, layout.home, 10)
    if action != 'inspect':
        if code: raise Blocked('LAUNCHCTL_FAILED')
        return None
    if code:
        if b'Could not find service' in err and code == 113: return None
        raise Blocked('SERVICE_OBSERVATION_FAILED')
    text = out.decode('utf-8')
    path = re.search(r'^\s*path = (.+)$', text, re.M)
    pid = re.search(r'^\s*pid = ([1-9][0-9]*)$', text, re.M)
    arguments = re.search(r'^\s*arguments = \{\n(.*?)^\s*\}', text, re.M | re.S)
    if path is None or pid is None or arguments is None: raise Blocked('SERVICE_AUTHORITY_UNCERTAIN')
    observed = [line.strip() for line in arguments[1].splitlines() if line.strip()]
    return {'label': LABEL, 'domain': domain, 'plist': path[1], 'argv': observed, 'pid': int(pid[1])}


def authority(observed, layout, argv):
    return (isinstance(observed, dict) and set(observed) == {'label', 'domain', 'plist', 'argv', 'pid'} and
        observed['label'] == LABEL and observed['domain'] == f'gui/{os.getuid()}' and observed['plist'] == str(layout.plist) and
        observed['argv'] == argv and type(observed['pid']) is int and observed['pid'] > 0)


def ipc_readback(layout):
    from hermes_downloads.ipc import request_health, request_jobs_page
    from hermes_downloads.endpoint_ownership import preflight
    # Existing service certificates and clients provide the read-only authority.
    certificate = preflight(layout.state)
    if certificate.socket is None or certificate.record is None: raise Blocked('WORKER_CERTIFICATE_MISSING')
    health = request_health(layout.socket).to_record()
    if health.get('queue_gate') != 'paused' or health.get('worker_epoch') != certificate.record[0]['worker_epoch']: raise Blocked('WORKER_NOT_PAUSED')
    page = request_jobs_page(layout.socket).to_record()
    return {**health, 'jobs_page': page}


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise Blocked('MANIFEST_INVALID')
            result[key] = value
        return result
    try: return json.loads(raw, object_pairs_hook=pairs)
    except Exception as error: raise Blocked('MANIFEST_INVALID') from error


MANIFEST_KEYS = {'schema', 'commit', 'home', 'source', 'config_before', 'config_after', 'backup_sha256', 'entry',
    'added_plugin', 'argv', 'files', 'service', 'steps', 'state', 'runtime', 'runtime_parity'}


def read_manifest(path):
    private_dir(path.parent)
    raw, info = read_file(path)
    if raw is None: raise Blocked('MANIFEST_INVALID')
    data = strict_json(raw)
    if not isinstance(data, dict) or set(data) != MANIFEST_KEYS or type(data['schema']) is not int or data['schema'] != 1:
        raise Blocked('MANIFEST_INVALID')
    if not re.fullmatch('[0-9a-f]{40}', data['commit']) or type(data['added_plugin']) is not bool or not isinstance(data['files'], dict):
        raise Blocked('MANIFEST_INVALID')
    identity_keys = {'dev', 'ino', 'uid', 'mode', 'size', 'mtime_ns', 'sha256'}
    def valid_identity(record):
        return (isinstance(record, dict) and set(record) == identity_keys and
            all(type(record[k]) is int and record[k] >= 0 for k in identity_keys - {'sha256'}) and
            record['mode'] == 0o600 and record['uid'] == os.getuid() and
            isinstance(record['sha256'], str) and re.fullmatch('[0-9a-f]{64}', record['sha256']))
    for record in (data['config_before'], data['config_after']):
        if record is not None and not valid_identity(record): raise Blocked('MANIFEST_INVALID')
    for path, item in data['files'].items():
        if not isinstance(path, str) or not isinstance(item, dict) or set(item) != {'before', 'after'} or item['before'] is not None or not valid_identity(item['after']): raise Blocked('MANIFEST_INVALID')
    if not isinstance(data['steps'], list) or any(not isinstance(x, str) for x in data['steps']): raise Blocked('MANIFEST_INVALID')
    if not isinstance(data['runtime_parity'], dict) or not isinstance(data['backup_sha256'], str) or not re.fullmatch('[0-9a-f]{64}', data['backup_sha256']): raise Blocked('MANIFEST_INVALID')
    if data['service'] is not None and (not isinstance(data['service'], dict) or set(data['service']) != {'label', 'domain', 'plist', 'argv', 'pid'} or type(data['service']['pid']) is not int): raise Blocked('MANIFEST_INVALID')
    return data


def save_manifest(path, data):
    atomic_write(path, json.dumps(data, sort_keys=True, indent=2).encode() + b'\n', snapshot(path))


def reuse_install(layout, commit, executor):
    for manifest in layout.evidence.glob('*/manifest.json'):
        data = read_manifest(manifest)
        if data['commit'] != commit: continue
        entry, argv, plist = wiring(layout, commit)
        if data['home'] != str(layout.home) or data['source'] != str(layout.source) or data['entry'] != entry or data['argv'] != argv: continue
        if set(data['files']) != {str(layout.plist), *(str(x) for x in layout.files().values())}: continue
        for name, item in data['files'].items():
            if snapshot(Path(name)) != item['after']: raise Blocked('OWNED_FILE_CHANGED')
        raw, _ = read_file(layout.config)
        _, config = parse_yaml(raw)
        if config.get('mcp_servers', {}).get('downloads') != entry or 'hermes-downloads' not in config.get('plugins', {}).get('enabled', []) or 'hermes-downloads' in config.get('plugins', {}).get('disabled', []): raise Blocked('OWNED_CONFIG_CHANGED')
        backup, _ = read_file(manifest.parent / 'config-before.yaml')
        if backup is None or digest(backup) != data['backup_sha256']: raise Blocked('BACKUP_HASH_MISMATCH')
        observed = executor('inspect', layout, argv)
        if not authority(observed, layout, argv) or observed != data['service']: raise Blocked('SERVICE_AUTHORITY_UNCERTAIN')
        physical_probe(layout, layout.runtime(commit), commit)
        ipc = ipc_readback(layout)
        return {'status': 'INSTALLED_WIRING', 'reasons': [], 'commit': commit, 'manifest': str(manifest), 'renderer_decision': 'NOT_OBSERVED', 'live': 'LIVE_PENDING', 'ipc': ipc}
    return None


def apply(layout, plan, executor):
    commit = plan['commit']
    reused = reuse_install(layout, commit, executor)
    if reused is not None: return reused
    entry, argv, plist = wiring(layout, commit)
    before, identity, after, added = pre_effect(layout, entry, plan['bundle'], executor, argv)
    private_dir(layout.evidence, create=True)
    evidence = layout.evidence / uuid.uuid4().hex; private_dir(evidence, create=True)
    backup = evidence / 'config-before.yaml'; atomic_write(backup, before, None)
    atomic_write(evidence / 'config-after.yaml', after, None)
    manifest = evidence / 'manifest.json'
    data = {'schema': 1, 'commit': commit, 'home': str(layout.home), 'source': str(layout.source),
        'config_before': identity, 'config_after': None, 'backup_sha256': digest(before), 'entry': entry,
        'added_plugin': added, 'argv': argv, 'files': {}, 'service': None, 'steps': ['backup'],
        'state': str(layout.state), 'runtime': str(layout.runtime(commit)), 'runtime_parity': {}}
    save_manifest(manifest, data)
    try:
        data['runtime_parity'] = prepare_runtime(layout, commit); data['steps'].append('runtime'); save_manifest(manifest, data)
        private_dir(layout.state, create=True); private_dir(layout.state / 'logs', create=True)
        for name in ('worker.stdout.log', 'worker.stderr.log'):
            path = layout.state / 'logs' / name
            if snapshot(path) is None: atomic_write(path, b'', None)
        for name, path in layout.files().items():
            private_dir(path.parent, create=True)
            installed = atomic_write(path, plan['bundle'][name], None)
            data['files'][str(path)] = {'before': None, 'after': installed}
            data['steps'].append(name); save_manifest(manifest, data)
        private_dir(layout.config.parent, create=True)
        data['config_after'] = atomic_write(layout.config, after, identity)
        data['steps'].append('config'); save_manifest(manifest, data)
        private_dir(layout.plist.parent, create=True)
        installed = atomic_write(layout.plist, plist, None)
        data['files'][str(layout.plist)] = {'before': None, 'after': installed}
        data['steps'].append('plist'); save_manifest(manifest, data)
        executor('bootstrap', layout, argv); data['steps'].append('bootstrap'); save_manifest(manifest, data)
        observed = executor('inspect', layout, argv)
        if not authority(observed, layout, argv): raise Blocked('SERVICE_AUTHORITY_UNCERTAIN')
        data['service'] = observed; save_manifest(manifest, data)
        ipc = ipc_readback(layout); data['steps'].append('paused_ipc'); save_manifest(manifest, data)
        return {'status': 'INSTALLED_WIRING', 'reasons': [], 'commit': commit, 'manifest': str(manifest),
            'renderer_decision': 'NOT_OBSERVED', 'live': 'LIVE_PENDING', 'ipc': ipc}
    except Exception as error:
        return {'status': 'INSTALL_FAILED', 'reasons': [str(error) if isinstance(error, Blocked) else 'INSTALL_EFFECT_FAILED'],
            'commit': commit, 'manifest': str(manifest), 'live': 'LIVE_PENDING'}


def rollback(layout, manifest, executor):
    data = read_manifest(manifest)
    commit = data['commit']; entry, argv, _ = wiring(layout, commit)
    if (data['home'] != str(layout.home) or data['source'] != str(layout.source) or data['state'] != str(layout.state) or
        data['runtime'] != str(layout.runtime(commit)) or data['entry'] != entry or data['argv'] != argv): raise Blocked('MANIFEST_PATH_MISMATCH')
    allowed = {str(layout.plist), *(str(path) for path in layout.files().values())}
    if set(data['files']) != allowed or data['config_after'] is None: raise Blocked('MANIFEST_INCOMPLETE')
    before, _ = read_file(manifest.parent / 'config-before.yaml')
    if before is None or digest(before) != data['backup_sha256']: raise Blocked('BACKUP_HASH_MISMATCH')
    raw, identity = read_file(layout.config)
    restored = rollback_config(raw, before, entry, data['added_plugin'])
    for name, record in data['files'].items():
        if set(record) != {'before', 'after'} or record['before'] is not None or snapshot(Path(name)) != record['after']:
            raise Blocked('OWNED_FILE_CHANGED')
    observed = executor('inspect', layout, argv)
    if not authority(observed, layout, argv) or observed != data['service']: raise Blocked('SERVICE_AUTHORITY_UNCERTAIN')
    executor('bootout', layout, argv)
    if executor('inspect', layout, argv) is not None: raise Blocked('SERVICE_EXIT_UNCERTAIN')
    if data['config_before'] is None and restored == b'': remove_exact(layout.config, identity)
    else: atomic_write(layout.config, restored, identity)
    for name, record in data['files'].items(): remove_exact(Path(name), record['after'])
    return {'status': 'ROLLED_BACK', 'reasons': [], 'commit': commit, 'retained_runtime': str(layout.runtime(commit)), 'live': 'LIVE_PENDING'}


def public(record): return {key: value for key, value in record.items() if key != 'bundle'}


def run(layout, mode='dry-run', expected_commit=None, manifest=None, executor=launchctl):
    try:
        if mode == 'rollback':
            try: return rollback(layout, manifest, executor)
            except (Blocked, OSError, TypeError, KeyError): return {'status': 'ROLLBACK_BLOCKED', 'reasons': ['ROLLBACK_AUTHORITY_OR_OBJECT_CHANGED']}
        plan = readiness(layout, expected_commit)
        if plan['status'] != 'PLAN_READY': return public(plan)
        if mode == 'apply':
            if expected_commit is None: raise Blocked('EXPECTED_COMMIT_REQUIRED')
            return apply(layout, plan, executor)
        entry, argv, _ = wiring(layout, plan['commit'])
        pre_effect(layout, entry, plan['bundle'], executor, argv)
        return {**public(plan), 'renderer_decision': 'NOT_OBSERVED', 'live': 'LIVE_PENDING'}
    except (Blocked, OSError, TypeError, ValueError) as error:
        return {'status': 'NOT_READY', 'reasons': [str(error) if isinstance(error, Blocked) else 'PREFLIGHT_INVALID']}


class Parser(argparse.ArgumentParser):
    def error(self, message): raise Blocked('ARGUMENTS_INVALID')


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    # Private subprocess probes accept only their positional absolute runtime paths.
    if arguments and arguments[0] == '--internal-probe':
        try: return internal_probe(Path(arguments[1])) if len(arguments) == 2 else 2
        except Exception: return 1
    if arguments and arguments[0] == '--internal-discover':
        try:
            if len(arguments) != 3: return 2
            print(json.dumps(asyncio.run(sdk_tools(Path(arguments[1]), Path(arguments[2]))))); return 0
        except Exception: return 1
    try:
        parser = Parser(add_help=False, allow_abbrev=False)
        modes = parser.add_mutually_exclusive_group()
        modes.add_argument('--apply', action='store_true'); modes.add_argument('--rollback', type=Path)
        parser.add_argument('--profile'); parser.add_argument('--expected-commit')
        args = parser.parse_args(arguments)
        if args.profile not in (None, 'default') or ((args.apply or args.rollback) and args.profile != 'default'):
            raise Blocked('DEFAULT_PROFILE_REQUIRED')
        if args.expected_commit is not None and not re.fullmatch('[0-9a-f]{40}', args.expected_commit): raise Blocked('COMMIT_INVALID')
        if args.apply and args.expected_commit is None: raise Blocked('EXPECTED_COMMIT_REQUIRED')
        if args.rollback and args.expected_commit: raise Blocked('ARGUMENTS_INVALID')
        layout = Layout(Path(pwd.getpwuid(os.getuid()).pw_dir))
        result = run(layout, 'rollback' if args.rollback else 'apply' if args.apply else 'dry-run', args.expected_commit, args.rollback)
    except (Blocked, OSError, ValueError): result = {'status': 'INVALID', 'reasons': ['ARGUMENTS_INVALID']}
    print(json.dumps(public(result), sort_keys=True))
    return 0 if result['status'] in ('PLAN_READY', 'INSTALLED_WIRING', 'ROLLED_BACK') else 1


if __name__ == '__main__': raise SystemExit(main())
