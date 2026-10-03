"""Explicit, foreground persistent worker entrypoint; no installation effects."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import signal
import sys
import threading

from hermes_downloads.endpoint_ownership import OwnershipError, validate_root


class ArgumentsError(ValueError):
    pass


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentsError


def state_root(value=None):
    if value is None:
        value = os.environ.get('HERMES_DOWNLOADS_STATE_ROOT')
    if value is None:
        value = Path.home() / 'Library' / 'Application Support' / 'HermesDownloadManager' / 'default'
    return validate_root(value)


def parse_worker_arguments(argv):
    parser = Parser(add_help=False, allow_abbrev=False)
    parser.add_argument('--serve', action='store_true')
    parser.add_argument('--state-root')
    arguments = parser.parse_args(argv)
    if not arguments.serve and arguments.state_root is not None:
        raise ArgumentsError
    return arguments


def serve(root):
    from hermes_downloads.worker import run_worker, worker_busy
    shutdown = threading.Event()
    previous = {}
    def request_shutdown(_signum, _frame):
        shutdown.set()
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, request_shutdown)
        result = run_worker(root, socket_path=root / 'worker.sock', recover_socket=True,
            ready_event=threading.Event(), shutdown_event=shutdown, stopped_event=threading.Event())
        return 1 if result == worker_busy else 0
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def worker_main(argv=None):
    # Only this physical console seam consumes argv; worker.main stays deterministic.
    try:
        arguments = parse_worker_arguments(sys.argv[1:] if argv is None else argv)
        if arguments.serve:
            root = state_root(arguments.state_root)
        else:
            from hermes_downloads.worker import main
            return main()
    except (ArgumentsError, OwnershipError):
        print('service_arguments_invalid', file=sys.stderr)
        return 2
    except Exception:
        print('service_unavailable', file=sys.stderr)
        return 1
    try:
        return serve(root)
    except Exception:
        print('service_unavailable', file=sys.stderr)
        return 1
