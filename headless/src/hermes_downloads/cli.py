"""Bounded IPC-only redacted diagnostics; no bootstrap or SQLite access."""
import json
import sys


def main(argv=None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if not arguments:
        return 0
    from hermes_downloads.service import ArgumentsError, Parser, state_root
    from hermes_downloads.endpoint_ownership import OwnershipError
    from hermes_downloads.ipc import IPCError, request_health, request_jobs_page
    try:
        parser = Parser(add_help=False, allow_abbrev=False)
        commands = parser.add_subparsers(dest='command', required=True, parser_class=Parser)
        health = commands.add_parser('health', add_help=False, allow_abbrev=False)
        health.add_argument('--state-root')
        listing = commands.add_parser('list', add_help=False, allow_abbrev=False)
        listing.add_argument('--state-root')
        listing.add_argument('--cursor')
        parsed = parser.parse_args(arguments)
        root = state_root(parsed.state_root)
        if parsed.command == 'list' and parsed.cursor is not None:
            # Existing client validates its closed identifier before touching IPC.
            from hermes_downloads.ipc import _require_identifier
            _require_identifier(parsed.cursor, 'cursor')
    except (ArgumentsError, OwnershipError, TypeError, ValueError):
        print('service_arguments_invalid', file=sys.stderr)
        return 2
    try:
        record = (request_health(root / 'worker.sock') if parsed.command == 'health'
            else request_jobs_page(root / 'worker.sock', cursor=parsed.cursor)).to_record()
        print(json.dumps(record, sort_keys=True, separators=(',', ':')))
        return 0
    except (IPCError, OSError, TypeError, ValueError, RecursionError):
        print('service_unavailable', file=sys.stderr)
        return 1
