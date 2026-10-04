#!/usr/bin/env python3
"""Read-only installed wiring verification; never launches or repairs a worker."""
import importlib.util
import json
from pathlib import Path
import pwd
import os
import sys

spec = importlib.util.spec_from_file_location('download_installer', Path(__file__).with_name('install.py'))
installer = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = installer
spec.loader.exec_module(installer)


def verify(layout, manifest=None, expected=None, executor=installer.launchctl):
    plan = installer.readiness(layout, expected)
    if plan['status'] != 'PLAN_READY': return installer.public(plan)
    try:
        if manifest is None:
            candidates = list(layout.evidence.glob('*/manifest.json'))
            if len(candidates) != 1: raise installer.Blocked('INSTALL_MANIFEST_REQUIRED')
            manifest = candidates[0]
        data = installer.read_manifest(manifest)
        commit = plan['commit']; entry, argv, plist = installer.wiring(layout, commit)
        if (data['commit'] != commit or data['home'] != str(layout.home) or data['source'] != str(layout.source) or
            data['entry'] != entry or data['argv'] != argv or data['state'] != str(layout.state) or
            data['runtime'] != str(layout.runtime(commit))): raise installer.Blocked('MANIFEST_PATH_MISMATCH')
        expected_files = {str(layout.plist), *(str(x) for x in layout.files().values())}
        if set(data['files']) != expected_files: raise installer.Blocked('MANIFEST_INCOMPLETE')
        for name, record in data['files'].items():
            if installer.snapshot(Path(name)) != record['after']: raise installer.Blocked('OWNED_FILE_CHANGED')
        backup, _ = installer.read_file(manifest.parent / 'config-before.yaml')
        if backup is None or installer.digest(backup) != data['backup_sha256']: raise installer.Blocked('BACKUP_HASH_MISMATCH')
        raw, _ = installer.read_file(layout.config)
        _, config = installer.parse_yaml(raw)
        if config.get('mcp_servers', {}).get('downloads') != entry or 'hermes-downloads' not in config.get('plugins', {}).get('enabled', []):
            raise installer.Blocked('OWNED_CONFIG_CHANGED')
        if 'hermes-downloads' in config.get('plugins', {}).get('disabled', []): raise installer.Blocked('BACKEND_EXPLICITLY_DISABLED')
        if layout.plist.read_bytes() != plist: raise installer.Blocked('PLIST_ARGV_MISMATCH')
        parity = installer.physical_probe(layout, layout.runtime(commit), commit)
        tools = installer.discover(layout, layout.runtime(commit))
        if sorted(tool['name'] for tool in tools) != sorted(installer.TOOLS): raise installer.Blocked('MISSING_TOOLS')
        for tool in tools: installer.check_tool(tool)
        service = executor('inspect', layout, argv)
        if not installer.authority(service, layout, argv) or service != data['service']: raise installer.Blocked('SERVICE_AUTHORITY_UNCERTAIN')
        ipc = installer.ipc_readback(layout)
        return {'status': 'VERIFIED_LOCAL', 'commit': commit, 'reasons': [], 'parity': parity, 'ipc': ipc,
            'discovered_tools': [tool['name'] for tool in tools], 'renderer_decision': 'NOT_OBSERVED',
            'hermes_discovery': 'NOT_OBSERVED', 'desktop_route_toast_reveal': 'NOT_OBSERVED', 'live': 'LIVE_PENDING'}
    except Exception as error:
        return {'status': 'NOT_READY', 'reasons': [str(error) if isinstance(error, installer.Blocked) else 'VERIFICATION_FAILED'], 'live': 'LIVE_PENDING'}


def main(argv=None):
    try:
        parser = installer.Parser(add_help=False, allow_abbrev=False)
        parser.add_argument('--manifest', type=Path); parser.add_argument('--expected-commit'); parser.add_argument('--profile')
        args = parser.parse_args(sys.argv[1:] if argv is None else argv)
        if args.profile not in (None, 'default'): raise installer.Blocked('DEFAULT_PROFILE_REQUIRED')
        if args.expected_commit is not None and not installer.re.fullmatch('[0-9a-f]{40}', args.expected_commit): raise installer.Blocked('COMMIT_INVALID')
        result = verify(installer.Layout(Path(pwd.getpwuid(os.getuid()).pw_dir)), args.manifest, args.expected_commit)
    except Exception: result = {'status': 'INVALID', 'reasons': ['ARGUMENTS_INVALID']}
    print(json.dumps(result, sort_keys=True)); return 0 if result['status'] == 'VERIFIED_LOCAL' else 1


if __name__ == '__main__': raise SystemExit(main())
