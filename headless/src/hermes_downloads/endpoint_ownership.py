"""Certified endpoint recovery for explicit serving, without writable bootstrap.

This is an owner-only same-account boundary. A socket with no durable record
(including a crash between bind and record publication) is deliberately blocked.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat

from hermes_downloads.store import SQLiteStore

RECORD = '.worker-endpoint.json'
LIMIT = 4096
_KEYS = {'schema', 'state_path', 'socket_path', 'root_device', 'root_inode',
         'worker_epoch', 'socket_device', 'socket_inode', 'socket_uid', 'socket_mode'}
_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK


class OwnershipError(ValueError):
    def __init__(self):
        super().__init__('service_endpoint_blocked')


def validate_root(value: str | Path) -> Path:
    """Reject lexical aliases and all symlink ancestors; never resolve a root."""
    try:
        raw = os.fspath(value)
        if type(raw) is not str or '\0' in raw:
            raise OwnershipError
        root = Path(raw)
        if not root.is_absolute() or str(root) != raw or '..' in root.parts:
            raise OwnershipError
        if len(os.fsencode(root / 'worker.sock')) > 103:
            raise OwnershipError
        current = Path(root.anchor)
        for part in root.parts[1:]:
            current /= part
            details = current.lstat()
            if not stat.S_ISDIR(details.st_mode):
                raise OwnershipError
        details = root.lstat()
        if details.st_uid != os.geteuid() or stat.S_IMODE(details.st_mode) & 0o077:
            raise OwnershipError
        return root
    except (OSError, TypeError, ValueError):
        raise OwnershipError from None


def _identity(details):
    return details.st_dev, details.st_ino


def _fingerprint(details):
    return (details.st_dev, details.st_ino, details.st_uid, details.st_mode,
            details.st_nlink, details.st_size, details.st_mtime_ns, details.st_ctime_ns)


def _regular(details):
    if (not stat.S_ISREG(details.st_mode) or details.st_uid != os.geteuid()
            or stat.S_IMODE(details.st_mode) != 0o600 or details.st_nlink != 1):
        raise OwnershipError


@contextmanager
def _directory(root, expected=None):
    root = validate_root(root)
    before = root.lstat()
    if expected is not None and _identity(before) != expected:
        raise OwnershipError
    fd = os.open(root, _FLAGS | os.O_DIRECTORY)
    try:
        if _identity(os.fstat(fd)) != _identity(before):
            raise OwnershipError
        yield fd, _identity(before)
        if _identity(validate_root(root).lstat()) != _identity(before):
            raise OwnershipError
    finally:
        os.close(fd)


def _stat(fd, name):
    try:
        return os.stat(name, dir_fd=fd, follow_symlinks=False)
    except FileNotFoundError:
        return None


def _duplicates(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise OwnershipError
        value[key] = item
    return value


def _read_record(fd, root):
    details = _stat(fd, RECORD)
    if details is None:
        return None
    _regular(details)
    if details.st_size > LIMIT:
        raise OwnershipError
    file_fd = os.open(RECORD, _FLAGS, dir_fd=fd)
    try:
        if _fingerprint(os.fstat(file_fd)) != _fingerprint(details):
            raise OwnershipError
        payload = os.read(file_fd, LIMIT + 1)
        if len(payload) > LIMIT or len(payload) != details.st_size:
            raise OwnershipError
        value = json.loads(payload.decode('utf-8'), object_pairs_hook=_duplicates)
        if type(value) is not dict or set(value) != _KEYS:
            raise OwnershipError
        for key in _KEYS - {'state_path', 'socket_path'}:
            if type(value[key]) is not int or not 0 <= value[key] <= 2**63 - 2:
                raise OwnershipError
        if (value['schema'] != 1 or value['worker_epoch'] < 1
                or value['state_path'] != str(root)
                or value['socket_path'] != str(root / 'worker.sock')
                or value['socket_uid'] != os.geteuid() or value['socket_mode'] != 0o600):
            raise OwnershipError
        if (_fingerprint(os.fstat(file_fd)) != _fingerprint(details)
                or _fingerprint(_stat(fd, RECORD)) != _fingerprint(details)):
            raise OwnershipError
        return value, payload, _fingerprint(details)
    finally:
        os.close(file_fd)


def _socket(fd):
    details = _stat(fd, 'worker.sock')
    if details is None:
        return None
    if (not stat.S_ISSOCK(details.st_mode) or details.st_uid != os.geteuid()
            or stat.S_IMODE(details.st_mode) != 0o600):
        raise OwnershipError
    return details.st_dev, details.st_ino, details.st_uid, stat.S_IMODE(details.st_mode)


def _epoch(fd, root):
    """Bounded read-only lookup, with WAL visible and no repair/migration."""
    details = _stat(fd, 'state.db')
    if details is None:
        raise OwnershipError
    _regular(details)
    database_fd = os.open('state.db', _FLAGS, dir_fd=fd)
    try:
        if _identity(os.fstat(database_fd)) != _identity(details):
            raise OwnershipError
        sidecars = {}
        for name in ('state.db-wal', 'state.db-shm', 'state.db-journal'):
            side = _stat(fd, name)
            if side is not None:
                _regular(side)
                sidecars[name] = _identity(side)
        # A readonly WAL connection can otherwise create an absent shm file.
        if 'state.db-wal' in sidecars and 'state.db-shm' not in sidecars:
            raise OwnershipError
        if 'state.db-journal' in sidecars:
            raise OwnershipError
        connection = sqlite3.connect((root / 'state.db').as_uri() + '?mode=ro', uri=True, timeout=0.2)
        try:
            connection.row_factory = sqlite3.Row
            budget = [0]
            def bounded():
                budget[0] += 1
                return budget[0] > 1000
            connection.set_progress_handler(bounded, 1000)
            connection.execute('PRAGMA query_only = ON')
            SQLiteStore._reject_newer_schema_version(connection)
            if connection.execute('PRAGMA quick_check(1)').fetchone()[0] != 'ok':
                raise OwnershipError
            row = connection.execute("SELECT value, revision FROM settings WHERE key = 'worker_epoch'").fetchone()
            if (row is None or type(row['value']) is not str
                    or re.fullmatch(r'[1-9][0-9]{0,18}', row['value']) is None
                    or type(row['revision']) is not int or not 1 <= row['revision'] < 2**63 - 1):
                raise OwnershipError
            epoch = int(row['value'])
            if epoch > 2**63 - 2:
                raise OwnershipError
        finally:
            connection.close()
        after = _stat(fd, 'state.db')
        if after is None or _identity(after) != _identity(details):
            raise OwnershipError
        _regular(after)
        for name in ('state.db-wal', 'state.db-shm', 'state.db-journal'):
            side = _stat(fd, name)
            if side is not None:
                _regular(side)
            if (None if side is None else _identity(side)) != sidecars.get(name):
                raise OwnershipError
        return epoch, _identity(details)
    finally:
        os.close(database_fd)


@dataclass(frozen=True)
class Certificate:
    root_identity: tuple[int, int]
    record: tuple | None
    socket: tuple | None
    database_identity: tuple[int, int] | None


def preflight(root: Path) -> Certificate:
    """No lease, writable SQLite, or artifact creation for uncertified state."""
    try:
        with _directory(root) as (fd, root_identity):
            record = _read_record(fd, root)
            endpoint = _socket(fd)
            if record is None:
                if endpoint is not None:
                    raise OwnershipError
                return Certificate(root_identity, None, None, None)
            value = record[0]
            if (value['root_device'], value['root_inode']) != root_identity:
                raise OwnershipError
            expected = tuple(value[key] for key in ('socket_device', 'socket_inode', 'socket_uid', 'socket_mode'))
            if endpoint is not None and endpoint != expected:
                raise OwnershipError
            epoch, db_identity = _epoch(fd, root)
            if epoch != value['worker_epoch']:
                raise OwnershipError
            return Certificate(root_identity, record, endpoint, db_identity)
    except (OSError, ValueError, TypeError, AttributeError, RecursionError, sqlite3.Error):
        raise OwnershipError from None


def reclaim(root: Path, certificate: Certificate) -> None:
    """Caller holds the exclusive lease; repeat certification before unlink."""
    if preflight(root) != certificate:
        raise OwnershipError
    try:
        with _directory(root, certificate.root_identity) as (fd, _):
            if _read_record(fd, root) != certificate.record or _socket(fd) != certificate.socket:
                raise OwnershipError
            if certificate.socket is not None:
                # Fresh nofollow identity check immediately precedes exact unlink.
                if _socket(fd) != certificate.socket:
                    raise OwnershipError
                os.unlink('worker.sock', dir_fd=fd)
    except (OSError, ValueError, TypeError, AttributeError):
        raise OwnershipError from None


def publish(root: Path, certificate: Certificate, epoch: int, bound_identity: tuple[int, int]) -> Certificate:
    """Publish only the just-bound verified HealthServer identity, before READY."""
    temporary = '.endpoint-' + secrets.token_hex(16)
    temporary_identity = None
    try:
        with _directory(root, certificate.root_identity) as (fd, root_identity):
            endpoint = _socket(fd)
            if endpoint is None or endpoint[:2] != bound_identity or type(epoch) is not int or epoch < 1:
                raise OwnershipError
            if _read_record(fd, root) != certificate.record:
                raise OwnershipError
            value = dict(schema=1, state_path=str(root), socket_path=str(root / 'worker.sock'),
                root_device=root_identity[0], root_inode=root_identity[1], worker_epoch=epoch,
                socket_device=endpoint[0], socket_inode=endpoint[1], socket_uid=endpoint[2], socket_mode=endpoint[3])
            payload = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
            file_fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
            try:
                temporary_identity = _identity(os.fstat(file_fd))
                _regular(os.fstat(file_fd))
                offset = 0
                while offset < len(payload):
                    written = os.write(file_fd, payload[offset:])
                    if written <= 0:
                        raise OwnershipError
                    offset += written
                os.fsync(file_fd)
            finally:
                os.close(file_fd)
            try:
                if (_read_record(fd, root) != certificate.record or _socket(fd) != endpoint
                        or _identity(validate_root(root).lstat()) != root_identity
                        or _identity(_stat(fd, temporary)) != temporary_identity):
                    raise OwnershipError
                os.replace(temporary, RECORD, src_dir_fd=fd, dst_dir_fd=fd)
                os.fsync(fd)
                record = _read_record(fd, root)
                if record is None or record[0] != value:
                    raise OwnershipError
                return Certificate(root_identity, record, endpoint, certificate.database_identity)
            finally:
                current = _stat(fd, temporary)
                if current is not None and _identity(current) == temporary_identity:
                    os.unlink(temporary, dir_fd=fd)
    except (OSError, ValueError, TypeError, AttributeError):
        # Also clean our exact exclusive temporary artifact if write/fsync failed.
        if temporary_identity is not None:
            try:
                with _directory(root, certificate.root_identity) as (fd, _):
                    current = _stat(fd, temporary)
                    if current is not None and _identity(current) == temporary_identity:
                        os.unlink(temporary, dir_fd=fd)
            except (OSError, OwnershipError):
                pass
        raise OwnershipError from None


def clear(root: Path, certificate: Certificate) -> None:
    """Graceful close clears the exact record only after confirmed socket absence."""
    try:
        with _directory(root, certificate.root_identity) as (fd, _):
            if _stat(fd, 'worker.sock') is not None or _read_record(fd, root) != certificate.record:
                raise OwnershipError
            if _read_record(fd, root) != certificate.record:
                raise OwnershipError
            os.unlink(RECORD, dir_fd=fd)
            os.fsync(fd)
    except (OSError, ValueError, TypeError, AttributeError):
        raise OwnershipError from None
