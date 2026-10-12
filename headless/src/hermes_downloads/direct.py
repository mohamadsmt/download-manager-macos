"""Worker-owned aria2 control for direct downloads.

This module only controls aria2 over its authenticated loopback RPC endpoint.  It
never implements HTTP transfer itself and never decides queue admission.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Final, cast
from urllib.parse import urlsplit

from hermes_downloads.models import Admission
from hermes_downloads.network import SourceURL
from hermes_downloads.paths import (
    DestinationIntent,
    PathValidationError,
    _require_matching_staged_partial_details,
    _staged_partial_payload_details,
)
from hermes_downloads.processes import (
    EngineIdentity,
    ProcessBirthIdentity,
    capture_process_birth,
    reconcile_process_birth,
)
from hermes_downloads.retry import CompletionVerification
from hermes_downloads.store import _DirectEngineRecoveryCapability

__all__ = [
    "CompletionVerification",
    "DirectAria2Controller",
    "DirectAdmissionError",
    "DirectEngineError",
    "DirectTransfer",
    "DirectTransferError",
    "StaleGenerationError",
]

_DEFAULT_ARIA2C: Final = Path("/opt/homebrew/bin/aria2c")
_MAX_RPC_RESPONSE_BYTES: Final = 64 * 1024
_RPC_READY_TIMEOUT_SECONDS: Final = 3.0
_RPC_TIMEOUT_SECONDS: Final = 1.0
_RECOVERY_SHUTDOWN_TIMEOUT_SECONDS: Final = 5.0
_GROUP_STOP_GRACE_SECONDS: Final = 0.5
_GROUP_KILL_GRACE_SECONDS: Final = 0.5
_POLL_INTERVAL_SECONDS: Final = 0.01
_MIN_SPLIT_SIZE: Final = "1M"
_MAX_PAYLOAD_LIMIT_BPS: Final = 2**63 - 1
_MAX_COUNTER_DIGITS: Final = 4300
_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SHA256: Final = re.compile(r"[0-9a-f]{64}\Z")
_GID: Final = re.compile(r"[0-9A-Fa-f]{16}\Z")
_SAFE_ENVIRONMENT_NAMES: Final = frozenset(
    {"PATH", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR"}
)
_TERMINAL_STATUSES: Final = frozenset({"complete", "error", "removed"})


@dataclass(frozen=True, slots=True, repr=False)
class _StockResult:
    known: bool
    http_status: int | None = None
    retry_after_seconds: int | None = None
    resource_digest: str | None = None
    strong_validator: str | None = None
    total_length: int | None = None
    contributors: int = 0
    owner: tuple | None = None

    def __post_init__(self):
        if type(self.known) is not bool or type(self.contributors) is not int or not 0<=self.contributors<=128:
            raise ValueError('invalid stock observation')
        if self.known:
            if type(self.http_status) is not int or not 100<=self.http_status<=599: raise ValueError('invalid stock status')
        elif any(x is not None for x in (self.http_status,self.retry_after_seconds,self.resource_digest,self.strong_validator,self.total_length)) or self.contributors!=0:
            raise ValueError('unknown stock observation has facts')
        for value in (self.resource_digest,self.strong_validator):
            if value is not None and (type(value) is not str or _SHA256.fullmatch(value) is None): raise ValueError('invalid stock identity')
        for value in (self.retry_after_seconds,self.total_length):
            if value is not None and (type(value) is not int or not 0<=value<=(1<<63)-1): raise ValueError('invalid stock scalar')
        if self.owner is not None:
            owner=self.owner
            if (type(owner) is not tuple or len(owner)!=7 or type(owner[0]) is not str or _IDENTIFIER.fullmatch(owner[0]) is None
                or type(owner[1]) is not int or not 0<=owner[1]<=(1<<63)-1 or type(owner[2]) is not str or _GID.fullmatch(owner[2]) is None
                or type(owner[3]) is not int or not 1<=owner[3]<=(1<<63)-1 or type(owner[4]) is not str or _IDENTIFIER.fullmatch(owner[4]) is None
                or any(type(x) is not str or _SHA256.fullmatch(x) is None for x in owner[5:])): raise ValueError('invalid stock owner')


def _stock_retry_after(value: bytes) -> int:
    if re.fullmatch(rb'[0-9]{1,19}', value):
        seconds = int(value)
    else:
        instant = parsedate_to_datetime(value.decode('ascii'))
        if instant.tzinfo is None: raise ValueError('unzoned Retry-After')
        seconds = max(0, math.ceil((instant-datetime.now(UTC)).total_seconds()))
    if seconds > (1 << 63)-1: raise ValueError('Retry-After overflow')
    return seconds


class _StockCapture:
    """Bounded stock INFO heads; discard raw lines and fail closed on observable gaps."""

    def __init__(self):
        self._lock = threading.Lock()
        self._tags = {}
        self._pending = {}
        self._heads = {}
        self._line = bytearray()
        self._dropping = False
        self._frame = None
        self._frame_bytes = 0
        self._lines = []
        self._unknown = False
        self.eof = False

    def bind(self, gid, tag):
        with self._lock:
            if (_GID.fullmatch(gid) is None or re.fullmatch('[0-9a-f]{64}',tag) is None
                or tag.encode() in self._tags or gid in self._tags.values() or len(self._tags)>=64 or self.eof):
                raise DirectEngineError('stock attempt binding invalid')
            self._tags[tag.encode()] = gid
            self._heads[gid] = []

    def fail(self):
        with self._lock: self._unknown = True

    def feed(self, data):
        with self._lock:
            if self.eof: self._unknown = True; return
            for byte in data:
                if self._dropping:
                    if byte==10: self._dropping = False
                    continue
                self._line.append(byte)
                if len(self._line)>2048:
                    if self._frame is not None: self._unknown = True; self._frame = None; self._lines = []
                    self._line.clear(); self._dropping = byte!=10
                elif byte==10:
                    line = bytes(self._line); self._line.clear()
                    self._consume_line(line)

    def _consume_line(self, line):
        prefix = re.search(rb'\[HttpConnection\.cc:[0-9]+\] CUID#([0-9]{1,20}) - (Requesting|Response received):\n$',line)
        if prefix:
            if self._frame is not None: self._unknown = True
            self._frame = (prefix[1],prefix[2]); self._frame_bytes = len(line); self._lines = []
            return
        if self._frame is None: return
        self._frame_bytes += len(line)
        if self._frame_bytes>=2048 or not line.endswith(b'\r\n') or b'\0' in line:
            self._unknown = True; self._frame = None; self._lines = []; return
        if line!=b'\r\n':
            self._lines.append(line[:-2])
            return
        cuid,kind = self._frame; lines = self._lines
        self._frame = None; self._lines = []
        try: self._head(cuid,kind,lines)
        except (ValueError,TypeError,KeyError,UnicodeError,OverflowError): self._unknown = True

    def _head(self, cuid, kind, lines):
        if not lines: raise ValueError('empty head')
        headers = {}
        relevant = {b'host',b'x-hermes-attempt',b'range'} if kind==b'Requesting' else {
            b'etag',b'content-range',b'content-length',b'content-encoding',b'retry-after',b'location'}
        for line in lines[1:]:
            name,sep,value = line.partition(b':')
            if not sep or re.fullmatch(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+",name) is None:
                raise ValueError('malformed header')
            name = name.lower()
            if name not in relevant: continue
            if name in headers: raise ValueError('duplicate header')
            value = value.strip(b' \t')
            if any(byte<32 or byte>=127 for byte in value): raise ValueError('invalid header')
            headers[name] = value
        if kind==b'Requesting':
            match = re.fullmatch(rb'(GET|HEAD) ([^ \r\n]+) HTTP/1\.[01]',lines[0])
            if match is None or headers.get(b'x-hermes-attempt') not in self._tags or not headers.get(b'host'):
                raise ValueError('unbound request')
            gid = self._tags[headers[b'x-hermes-attempt']]
            resource = hashlib.sha256(headers[b'host']+b'\0'+match[2]).hexdigest()
            span = None
            if b'range' in headers:
                interval = re.fullmatch(rb'bytes=([0-9]+)-([0-9]*)',headers[b'range'])
                if interval is None: raise ValueError('unsupported request range')
                span = (int(interval[1]),int(interval[2]) if interval[2] else None)
            if sum(map(len,self._pending.values()))>=64: raise ValueError('request FIFO bound')
            self._pending.setdefault(cuid,[]).append((gid,match[1],resource,span))
            return
        match = re.fullmatch(rb'HTTP/1\.[01] ([1-5][0-9]{2})(?: [^\r\n]*)?',lines[0])
        if match is None or cuid not in self._pending: raise ValueError('unpaired response')
        status = int(match[1]); gid,method,resource,span = self._pending[cuid][0]
        if status<200:
            if status==101: raise ValueError('unsupported switch')
            return
        self._pending[cuid].pop(0)
        if not self._pending[cuid]: del self._pending[cuid]
        if method==b'HEAD' or 300<=status<400: return
        retry = _stock_retry_after(headers[b'retry-after']) if b'retry-after' in headers else None
        total = None
        if b'content-length' in headers:
            if re.fullmatch(rb'[0-9]{1,19}',headers[b'content-length']) is None: raise ValueError('invalid length')
            total = int(headers[b'content-length'])
            if total>(1<<63)-1: raise ValueError('length overflow')
        if status==206:
            interval = re.fullmatch(rb'bytes ([0-9]+)-([0-9]+)/([0-9]+)',headers.get(b'content-range',b''))
            if interval is None or span is None: raise ValueError('unqualified range')
            start,end,length = map(int,interval.groups())
            if max(start,end,length)>(1<<63)-1: raise ValueError('range overflow')
            if not (start<=end<length and start==span[0] and (span[1] is None or end<=span[1]) and total==end-start+1):
                raise ValueError('range mismatch')
            total = length
        elif b'content-range' in headers: raise ValueError('unexpected range')
        validator = headers.get(b'etag')
        strong = (hashlib.sha256(validator).hexdigest() if validator is not None
            and re.fullmatch(rb'"[\x21\x23-\x7e]*"',validator) else None)
        if headers.get(b'content-encoding',b'identity').lower()!=b'identity': strong = None
        heads = self._heads[gid]
        if len(heads)>=128: raise ValueError('contributor bound')
        heads.append((status,retry,resource,strong,total))

    def finish(self):
        with self._lock:
            if self._frame is not None or self._pending or b'HttpConnection.cc' in self._line:
                self._unknown = True
            self._line.clear(); self._lines = []; self.eof = True

    def result(self, gid):
        with self._lock:
            heads = self._heads.get(gid,[])
            if self._unknown or not self.eof or not heads: return _StockResult(False)
            failures = [head for head in heads if head[0]>=400]
            selected = failures or heads
            if len({head[:2] for head in selected})!=1 and (failures or any(head[0] not in {200,206} or head[1] is not None for head in selected)): return _StockResult(False)
            status,retry = selected[0][:2]
            resources = {head[2] for head in selected}
            resource = next(iter(resources)) if len(resources)==1 else None
            successful = [head for head in heads if head[0] in {200,206}]
            identities = {head[2:] for head in successful}
            validator,total = (successful[0][3:] if successful and len(identities)==1 else (None,None))
            if successful and len(identities)==1: resource=successful[0][2]
            return _StockResult(True,status,retry,resource,validator,total,len(heads))


def _read_saved_source(source: SourceURL, original: _StockResult) -> str:
    """Explicit saved consent: HEAD only, before touching the retained pair.

    Compare the actual stock response's resource, strong ETag and length. No
    ambient credentials/proxy, redirect guessing, or response BODY is used.
    """
    if (type(original) is not _StockResult or not original.known
        or original.resource_digest is None or original.strong_validator is None
        or original.total_length is None):
        raise _SavedSourceIdentityError('saved source identity unknown; separate download required')
    _require_source(source)
    parts = urlsplit(source.raw_url.decode('utf-8'))
    target = parts.path or '/'
    if parts.query: target += '?' + parts.query
    resource = hashlib.sha256(parts.netloc.encode('ascii')+b'\0'+target.encode('ascii')).hexdigest()
    if resource != original.resource_digest:
        raise _SavedSourceIdentityError('saved source resource changed; separate download required')
    connection_type = http.client.HTTPSConnection if parts.scheme == 'https' else http.client.HTTPConnection
    connection = connection_type(parts.hostname, parts.port, timeout=3)
    response = None
    try:
        connection.request('HEAD', target, headers={'Accept-Encoding':'identity'})
        response = connection.getresponse()
        headers = response.getheaders()
        def single(name):
            values = [v for k,v in headers if k.lower()==name]
            return values[0] if len(values)==1 else None
        etag = single('etag'); length = single('content-length')
        encoding = single('content-encoding')
        if (response.status != 200 or etag is None
            or len(etag)>2048 or re.fullmatch(r'"[\x21\x23-\x7e]*"',etag) is None
            or hashlib.sha256(etag.encode('ascii')).hexdigest()!=original.strong_validator
            or length is None or re.fullmatch('[0-9]{1,19}',length) is None
            or int(length)!=original.total_length or encoding not in (None,'identity')):
            raise _SavedSourceIdentityError('saved source identity changed or unknown; separate download required')
        return etag
    except (OSError,http.client.HTTPException,UnicodeError,ValueError):
        raise _SavedSourceIdentityError('saved source identity unavailable; separate download required') from None
    finally:
        if response is not None: response.close()
        connection.close()


def _verify_saved_source(source: SourceURL, original: _StockResult, *, cancelled=lambda: False, deadline=None) -> str:
    """A joined identity-only child bounds DNS, TLS and trickled HEAD together.

    The existing saved observer owns this original Popen. No source is placed
    in argv or diagnostics; cancellation never leaves a resolver/reader thread.
    """
    _require_source(source)
    if type(original) is not _StockResult: raise TypeError('invalid saved observation')
    end=min(time.monotonic()+3,deadline) if deadline is not None else time.monotonic()+3
    if cancelled() or time.monotonic()>=end: raise _SavedSourceIdentityError('saved identity observation cancelled')
    code="""import json,sys
from hermes_downloads.direct import _StockResult,_read_saved_source
from hermes_downloads.network import _parse_source_url
try:
    value=json.loads(sys.stdin.buffer.read(16384))
    etag=_read_saved_source(_parse_source_url(value['source']),_StockResult(**value['stock']))
    sys.stdout.write(json.dumps(dict(etag=etag)))
except Exception:
    raise SystemExit(1)
"""
    stock={name:getattr(original,name) for name in ('known','http_status','retry_after_seconds','resource_digest','strong_validator','total_length','contributors')}
    packet=json.dumps(dict(source=source.raw_url.decode('utf-8'),stock=stock)).encode()
    argv=(sys.executable,'-I','-B','-c',code)
    child=subprocess.Popen(argv,stdin=subprocess.PIPE,stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,env=_filtered_environment(),start_new_session=True,close_fds=True)
    group=None
    try:
        group=_bind_process_group(child)
        if group is None: raise _SavedSourceIdentityError('saved identity ownership unavailable')
        identity=capture_process_birth(EngineIdentity(child.pid,group,time.monotonic_ns(),_argv_sha256(argv)))
        if identity is None:raise _SavedSourceIdentityError('saved identity birth unavailable')
        first=True
        while True:
            if cancelled() or time.monotonic()>=end: raise _SavedSourceIdentityError('saved identity observation ended')
            try:
                output,_=child.communicate(packet if first else None,timeout=min(.05,max(.001,end-time.monotonic())))
                break
            except subprocess.TimeoutExpired: first=False
        if child.wait(timeout=max(.001,end-time.monotonic()))!=0 or len(output)>4096:
            raise _SavedSourceIdentityError('saved source identity unavailable; separate download required')
        value=json.loads(output)
        if type(value) is not dict or set(value)!={'etag'} or type(value['etag']) is not str or re.fullmatch(r'"[\x21\x23-\x7e]*"',value['etag']) is None:
            raise _SavedSourceIdentityError('saved source identity unavailable; separate download required')
        return value['etag']
    finally:
        # Even failed/cancelled probes retain their original wait and group proof.
        if child.poll() is None:
            stopped,error=_stop_process(child,group,deadline=time.monotonic()+1)
            if not stopped or error is not None: raise DirectEngineError('saved identity child closure failed')
        child.wait(timeout=1)
        for stream in (child.stdin,child.stdout):
            if stream is not None:stream.close()
        if group is not None and _group_exists(group):raise DirectEngineError('saved identity child group remains')
        try:os.waitpid(child.pid,os.WNOHANG)
        except ChildProcessError:pass
        else:raise DirectEngineError('saved identity child not reaped')


class DirectEngineError(RuntimeError):
    """A bounded aria2 lifecycle or RPC failure with no engine diagnostics."""


class DirectTransferError(DirectEngineError):
    """aria2 could not produce a hash-verified direct payload."""


class _SavedSourceIdentityError(DirectTransferError):
    """Saved bytes require a fresh source decision before any mutation."""


class _StockTerminalError(DirectTransferError):
    def __init__(self, state):
        super().__init__('stock terminal error')
        self.state = state


class DirectAdmissionError(DirectTransferError):
    """A worker attempted engine admission while an independent queue gate was shut."""


class StaleGenerationError(DirectTransferError):
    """A callback did not match the exact currently-owned GID and generation."""


@dataclass(frozen=True, slots=True, repr=False)
class _VerifiedPayloadIdentity:
    """Immutable descriptor evidence from the original terminal verification."""

    st_dev: int
    st_ino: int
    logical_size: int
    mtime_ns: int
    st_mode: int
    st_nlink: int
    ctime_ns: int

    def __post_init__(self) -> None:
        if any(type(value) is not int or value < 0 for value in
               (self.st_dev, self.st_ino, self.logical_size)) or (
                   type(self.mtime_ns) is not int or type(self.ctime_ns) is not int
                   or type(self.st_mode) is not int or not stat.S_ISREG(self.st_mode)
                   or type(self.st_nlink) is not int or self.st_nlink != 1
               ):
            raise ValueError("verified payload identity is invalid")


@dataclass(frozen=True, slots=True)
class DirectTransfer:
    """A readback of one aria2 direct transfer, never authorization itself."""

    job_id: str
    generation: int
    gid: str
    status: str
    total_length: int
    completed_length: int
    partial_path: Path
    hash_verified: bool
    verification: CompletionVerification | None
    verified_identity: _VerifiedPayloadIdentity | None = None
    error_code: int | None = None


@dataclass(frozen=True, slots=True)
class _TrackedTransfer:
    job_id: str
    generation: int
    gid: str
    source: SourceURL
    destination: DestinationIntent
    expected_sha256: str | None


class DirectAria2Controller:
    """Own one blank, loopback-only aria2 daemon for a single worker.

    ``add_paused`` and ``resume`` require the caller's current ``Admission``.
    This controller never schedules, resumes, or reconstructs downloads on its
    own; a restart starts a new daemon with no session input and no GID mapping.
    """

    def __init__(
        self,
        *,
        executable: str | os.PathLike[str] = _DEFAULT_ARIA2C,
        runtime_root: str | os.PathLike[str],
        max_concurrent_downloads: int = 1,
        split: int = 4,
        max_connection_per_server: int = 4,
        on_engine_bound: Callable[[ProcessBirthIdentity], None] | None = None,
    ) -> None:
        self._executable = _require_executable(executable)
        self._runtime_root = _require_absolute_path(runtime_root, "runtime_root")
        self._max_concurrent_downloads = _require_positive(
            max_concurrent_downloads, "max_concurrent_downloads"
        )
        self._split = _require_positive(split, "split")
        self._max_connection_per_server = _require_connection_limit(
            max_connection_per_server
        )
        if on_engine_bound is not None and not callable(on_engine_bound):
            raise TypeError("on_engine_bound must be callable or None")
        self._on_engine_bound = on_engine_bound
        self._process: subprocess.Popen[bytes] | None = None
        self._identity: EngineIdentity | None = None
        self._port: int | None = None
        self._secret: str | None = None
        self._private_runtime_path: Path | None = None
        self._private_config_path: Path | None = None
        self._launch_argv: tuple[str, ...] = ()
        self._by_job_id: dict[str, _TrackedTransfer] = {}
        self._by_gid: dict[str, _TrackedTransfer] = {}
        self._observation_cancelled = threading.Event()
        self._global_limit_lock = threading.RLock()
        self._global_resume_blocked = False
        self._capture = _StockCapture()
        self._capture_reader = None
        self._sealed_stock = {}
        self._stock_owners = {}
        self._stock_tags = {}

    def __enter__(self) -> DirectAria2Controller:
        self.start()
        return self

    def __exit__(self, exc_type: object, *_: object) -> None:
        try:
            self.close()
        except BaseException:
            if exc_type is None:
                raise

    @property
    def engine_identity(self) -> EngineIdentity | None:
        """Return the exact identity of the currently-owned daemon, if any."""

        return self._identity

    def _recovery_capability(self) -> _DirectEngineRecoveryCapability:
        """Snapshot private shutdown authority only after RPC readiness succeeds."""

        _process, port, secret = self._require_running()
        return _DirectEngineRecoveryCapability(rpc_port=port, rpc_secret=secret)

    @property
    def launch_argv(self) -> tuple[str, ...]:
        """Return the daemon argv, which intentionally contains no RPC secret."""

        return self._launch_argv

    @property
    def private_runtime_path(self) -> Path:
        """Return the owner-only transient runtime directory while it is owned."""

        if self._private_runtime_path is None:
            raise DirectEngineError("aria2 is not running")
        return self._private_runtime_path

    @property
    def private_config_path(self) -> Path:
        """Return the owner-only configuration pathname while it is owned."""

        if self._private_config_path is None:
            raise DirectEngineError("aria2 is not running")
        return self._private_config_path

    @property
    def active_job_ids(self) -> tuple[str, ...]:
        """Return only in-memory GID ownership; it is cleared on every restart."""

        return tuple(self._by_job_id)

    def start(self) -> EngineIdentity:
        """Start a fresh, authenticated aria2 daemon with no session resurrection."""

        if self._process is not None:
            raise DirectEngineError("aria2 is already running")
        if self._private_runtime_path is not None or self._private_config_path is not None:
            raise DirectEngineError("aria2 private runtime cleanup is pending")
        self._observation_cancelled.clear()
        self._capture = _StockCapture()
        self._sealed_stock = {}
        self._stock_owners = {}
        self._stock_tags = {}
        runtime_path: Path | None = None
        process: subprocess.Popen[bytes] | None = None
        process_group_id: int | None = None
        try:
            runtime_path, config_path, secret = self._create_private_config()
            port = _reserve_loopback_port()
            argv = self._build_argv(config_path, port)
            process = subprocess.Popen(
                argv,
                cwd=str(runtime_path),
                env=_filtered_environment(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                shell=False,
                start_new_session=True,
                close_fds=True,
            )
            self._process = process
            def drain():
                try:
                    while chunk := process.stdout.read1(4096):
                        self._capture.feed(chunk)
                except BaseException:
                    self._capture.fail()
                finally:
                    try: process.stdout.close()
                    except BaseException: self._capture.fail()
                    finally: self._capture.finish()
            self._capture_reader = threading.Thread(target=drain,name='direct-stock-reader',daemon=True)
            self._capture_reader.start()
            self._port = port
            self._secret = secret
            self._launch_argv = argv
            process_group_id = _bind_process_group(process)
            if process_group_id is None:
                raise DirectEngineError("aria2 containment failed")
            identity = EngineIdentity(
                leader_pid=process.pid,
                process_group_id=process_group_id,
                started_monotonic_ns=time.monotonic_ns(),
                argv_sha256=_argv_sha256(argv),
            )
            self._identity = identity
            if self._on_engine_bound is not None:
                birth_identity = capture_process_birth(identity)
                if birth_identity is None:
                    raise DirectEngineError("aria2 containment failed")
                self._on_engine_bound(birth_identity)
            self._wait_for_rpc_ready()
            return identity
        except (OSError, ValueError):
            stopped, interruption = _stop_process(process, process_group_id)
            if stopped:
                self._join_stock_reader(time.monotonic()+_RECOVERY_SHUTDOWN_TIMEOUT_SECONDS)
                self._clear_stopped_process_state()
                self._cleanup_owned_private_runtime(preserve_primary=True)
            if interruption is not None:
                raise interruption
            if not stopped:
                raise DirectEngineError("aria2 containment failed") from None
            raise DirectEngineError("aria2 could not start") from None
        except BaseException:
            stopped, _cleanup_interruption = _stop_process(process, process_group_id)
            if stopped:
                try: self._join_stock_reader(time.monotonic()+_RECOVERY_SHUTDOWN_TIMEOUT_SECONDS)
                except BaseException: pass
                else:
                    self._clear_stopped_process_state()
                    self._cleanup_owned_private_runtime(preserve_primary=True)
            raise

    def close(self, *, deadline=None) -> None:
        """Stop the owned daemon and erase its secret-bearing temporary config."""

        self._observation_cancelled.set()
        supplied=deadline is not None
        deadline = time.monotonic()+_RECOVERY_SHUTDOWN_TIMEOUT_SECONDS if deadline is None else deadline
        stopped, interruption = self._stop_owned_process(deadline=deadline) if supplied else self._stop_owned_process()
        if stopped:
            self._join_stock_reader(deadline)
            self._clear_mappings()
            self._clear_stopped_process_state()
            self._cleanup_owned_private_runtime(
                preserve_primary=interruption is not None
            )
        if interruption is not None:
            raise interruption
        if not stopped:
            raise DirectEngineError("aria2 containment failed")

    def discard_absent(self) -> None:
        """Discard a worker-reconciled absent owner without signaling its old PGID."""

        self._observation_cancelled.set()
        process, identity = self._process, self._identity
        if (
            process is None
            or identity is None
            or process.pid != identity.leader_pid
        ):
            raise DirectEngineError("aria2 absent owner discard failed")
        try:
            returncode = process.poll()
        except (AttributeError, OSError):
            raise DirectEngineError("aria2 absent owner discard failed") from None
        if type(returncode) is not int:
            raise DirectEngineError("aria2 absent owner discard failed")
        self._join_stock_reader(time.monotonic()+_RECOVERY_SHUTDOWN_TIMEOUT_SECONDS)
        self._clear_mappings()
        self._cleanup_owned_private_runtime(preserve_primary=False)
        self._clear_stopped_process_state()

    def restart(self) -> EngineIdentity:
        """Stop and discard all engine state before starting a blank daemon."""

        self.close()
        return self.start()

    def gid_for_job(self, job_id: str) -> str | None:
        """Return the exact aria2 GID currently owned by a job."""

        _require_identifier(job_id, "job_id")
        transfer = self._by_job_id.get(job_id)
        return None if transfer is None else transfer.gid

    def job_for_gid(self, gid: str) -> str | None:
        """Return the exact job currently owned by an aria2 GID."""

        _require_gid(gid)
        transfer = self._by_gid.get(gid)
        return None if transfer is None else transfer.job_id

    def add_paused(
        self,
        *,
        job_id: str,
        generation: int,
        source: SourceURL,
        destination: DestinationIntent,
        expected_sha256: str | None,
        admission: Admission,
        saved_resume: bool = False,
        _resume_validator: str | None = None,
    ) -> DirectTransfer:
        """Add one admitted transfer with aria2 paused before any body request."""

        _require_admission(admission)
        _require_identifier(job_id, "job_id")
        _require_generation(generation)
        _require_source(source)
        _require_destination(destination, job_id)
        expected_sha256 = _require_optional_sha256(expected_sha256)
        self._require_running()
        if job_id in self._by_job_id:
            raise DirectTransferError("job already has an aria2 GID")
        options = {
            "dir": str(destination.incomplete_dir),
            "out": destination.partial_path.name,
            "pause": "true",
            "continue": "true",
            "allow-overwrite": "false",
            "auto-file-renaming": "false",
            "header": "X-Hermes-Attempt: "+secrets.token_hex(32),
        }
        if _resume_validator is not None:
            if not saved_resume or re.fullmatch(r'"[\x21\x23-\x7e]*"',_resume_validator) is None:
                raise DirectTransferError('invalid saved validator')
            options['header'] += '\nIf-Match: '+_resume_validator
        if type(saved_resume) is not bool: raise TypeError('invalid saved resume flag')
        if saved_resume:
            if expected_sha256 is not None: raise DirectTransferError('saved transport cannot own checksum')
            options['always-resume'] = 'true'
        if expected_sha256 is not None:
            options["checksum"] = f"sha-256={expected_sha256}"
            options["check-integrity"] = "true"
        gid = self._rpc("aria2.addUri", [[source.raw_url.decode("utf-8")], options])
        _require_gid(gid)
        transfer = _TrackedTransfer(
            job_id=job_id,
            generation=generation,
            gid=gid,
            source=source,
            destination=destination,
            expected_sha256=expected_sha256,
        )
        self._by_job_id[job_id] = transfer
        self._by_gid[gid] = transfer
        try:
            actual = self._rpc('aria2.getOption',[gid])
            if type(actual) is not dict or actual.get('header') not in (options['header'],options['header']+'\n'):
                raise DirectTransferError('stock attempt tag not confirmed')
            tag=options['header'].splitlines()[0].split(': ',1)[1]
            self._capture.bind(gid,tag)
            self._stock_tags[gid]=hashlib.sha256(tag.encode()).hexdigest()
            if saved_resume:
                for name in ('continue','always-resume','allow-overwrite','auto-file-renaming'):
                    if type(actual) is not dict or actual.get(name) != options[name]:
                        raise DirectTransferError('saved continuation option not confirmed')
            return self._readback(transfer)
        except BaseException:
            self._by_job_id.pop(job_id, None)
            self._by_gid.pop(gid, None)
            self._discard_unreadable_gid(gid)
            raise

    def resume(
        self, *, job_id: str, generation: int, admission: Admission
    ) -> DirectTransfer:
        """Explicitly unpause one still-admitted job, then read its actual state."""

        _require_admission(admission)
        transfer = self._current_transfer(job_id, generation)
        with self._global_limit_lock:
            if self._global_resume_blocked:
                raise DirectAdmissionError("global payload limit prevents resume")
            result = self._rpc("aria2.unpause", [transfer.gid])
            if result != transfer.gid:
                raise DirectTransferError("aria2 did not acknowledge resume")
            return self._readback(transfer)

    def set_allocation(
        self, *, job_id: str, generation: int, allocation_bps: int | None
    ) -> DirectTransfer:
        """Apply one transfer allocation; zero pauses rather than becoming unlimited."""

        _require_payload_limit(allocation_bps, "allocation_bps")
        transfer = self._current_transfer(job_id, generation)
        if allocation_bps == 0:
            result = self._rpc("aria2.pause", [transfer.gid])
            if result != transfer.gid:
                raise DirectTransferError("aria2 did not acknowledge pause")
            state = self._readback(transfer)
            if state.status != "paused":
                raise DirectTransferError("aria2 did not confirm pause")
            return state
        expected = "0" if allocation_bps is None else str(allocation_bps)
        self._confirm_payload_limit(
            "aria2.changeOption", [transfer.gid, {"max-download-limit": expected}],
            "aria2.getOption", [transfer.gid], "max-download-limit", expected,
        )
        state = None
        try:
            state = self._readback(transfer)
        except DirectEngineError:
            pass
        if state is None or state.status not in _TERMINAL_STATUSES | {"active", "waiting", "paused"}:
            raise DirectTransferError("aria2 did not confirm allocation state")
        return state

    def set_global_limit(self, *, global_limit_bps: int | None) -> int | None:
        """Confirm one engine payload cap; product zero closes explicit resume.

        Zero pauses mapped active/waiting jobs without sending aria2's unlimited
        zero. A failed change also closes resume until a positive/None readback
        succeeds. Clearing the gate never unpauses jobs. This is engine control,
        not an ordered batch reallocation or a worker scheduling decision.
        """

        _require_payload_limit(global_limit_bps, "global_limit_bps")
        with self._global_limit_lock:
            self._global_resume_blocked = True
            if global_limit_bps == 0:
                failed = False
                try:
                    self._require_running()
                except DirectEngineError:
                    failed = True
                if failed:
                    raise DirectTransferError("aria2 global payload limit failed")
                for transfer in tuple(self._by_job_id.values()):
                    try:
                        state = self._readback(transfer)
                        if state.status in {"active", "waiting"}:
                            result = self._rpc("aria2.pause", [transfer.gid])
                            if result != transfer.gid:
                                raise DirectTransferError("aria2 pause failed")
                            state = self._readback(transfer)
                            if state.status != "paused":
                                raise DirectTransferError("aria2 pause failed")
                        elif state.status not in _TERMINAL_STATUSES | {"paused"}:
                            raise DirectTransferError("aria2 pause failed")
                    except DirectEngineError:
                        # Still attempt to pause the remaining owned transfers.
                        failed = True
                if failed:
                    raise DirectTransferError("aria2 global payload limit failed")
                return 0
            expected = "0" if global_limit_bps is None else str(global_limit_bps)
            self._confirm_payload_limit(
                "aria2.changeGlobalOption", [{"max-overall-download-limit": expected}],
                "aria2.getGlobalOption", [], "max-overall-download-limit", expected,
            )
            self._global_resume_blocked = False
            return global_limit_bps

    def _confirm_payload_limit(
        self, change_method: str, change_params: list[Any], read_method: str,
        read_params: list[Any], field: str, expected: str,
    ) -> None:
        confirmed = False
        try:
            result = self._rpc(change_method, change_params)
            if type(result) is str and result == "OK":
                response = self._rpc(read_method, read_params)
                confirmed = (
                    type(response) is dict
                    and type(response.get(field)) is str
                    and response[field] == expected
                )
        except DirectEngineError:
            pass
        if not confirmed:
            raise DirectTransferError("aria2 did not confirm payload limit")

    def readback(self, *, job_id: str, generation: int, gid: str) -> DirectTransfer:
        """Read one exact mapped engine state after validating callback identity."""

        return self._readback(self._callback_transfer(job_id, generation, gid))

    def observe_callback(
        self, *, job_id: str, generation: int, gid: str
    ) -> DirectTransfer:
        """Accept only a current-generation engine callback and read back its state."""

        return self.readback(job_id=job_id, generation=generation, gid=gid)

    def observe_terminal(
        self, *, job_id: str, generation: int, gid: str
    ) -> DirectTransfer | None:
        """Read exactly once; verify only a complete, still-owned payload.

        This operation makes no queue or mapping changes. The worker runs it
        outside its IPC/SQLite thread and fences its immutable result again.
        """

        try:
            transfer = self._callback_transfer(job_id, generation, gid)
            if self._observation_cancelled.is_set():
                raise StaleGenerationError("stale direct observation")
            state = self._readback(transfer)
            if state.status in {"active", "waiting", "paused"}:
                return None
            if state.status == 'error':
                raise _StockTerminalError(state)
            if state.status != "complete" or state.total_length != state.completed_length:
                raise DirectTransferError("aria2 terminal observation failed")
            verified = self._verify_completed_output(state, transfer)
            if (
                self._observation_cancelled.is_set()
                or self._callback_transfer(job_id, generation, gid) is not transfer
            ):
                raise StaleGenerationError("stale direct observation")
            return verified
        except (StaleGenerationError, _StockTerminalError):
            raise
        except Exception:
            raise DirectTransferError("aria2 terminal verification failed") from None

    def wait_for_terminal(
        self, *, job_id: str, generation: int, timeout: float
    ) -> DirectTransfer:
        """Wait for terminal state and verify a supplied SHA-256 when available."""

        if type(timeout) not in {int, float} or timeout <= 0:
            raise TypeError("timeout must be a positive number")
        transfer = self._current_transfer(job_id, generation)
        deadline = time.monotonic() + float(timeout)
        while True:
            state = self._readback(transfer)
            if state.status == "complete":
                return self._verify_completed_output(state, transfer)
            if state.status in _TERMINAL_STATUSES:
                raise DirectTransferError("aria2 transfer failed")
            if time.monotonic() >= deadline:
                raise DirectTransferError("aria2 transfer timed out")
            time.sleep(_POLL_INTERVAL_SECONDS)

    def _current_transfer(self, job_id: str, generation: int) -> _TrackedTransfer:
        _require_identifier(job_id, "job_id")
        _require_generation(generation)
        transfer = self._by_job_id.get(job_id)
        if transfer is None or transfer.generation != generation:
            raise StaleGenerationError("stale direct transfer")
        return transfer

    def _callback_transfer(
        self, job_id: str, generation: int, gid: str
    ) -> _TrackedTransfer:
        transfer = self._current_transfer(job_id, generation)
        _require_gid(gid)
        if transfer.gid != gid or self._by_gid.get(gid) is not transfer:
            raise StaleGenerationError("stale direct callback")
        return transfer

    def _readback(self, transfer: _TrackedTransfer) -> DirectTransfer:
        response = self._rpc(
            "aria2.tellStatus",
            [
                transfer.gid,
                ["status", "totalLength", "completedLength", "errorCode"],
            ],
        )
        if type(response) is not dict:
            raise DirectTransferError("aria2 returned an invalid state")
        status = response.get("status")
        if type(status) is not str:
            raise DirectTransferError("aria2 returned an invalid state")
        total_length = _parse_counter(response.get("totalLength"))
        completed_length = _parse_counter(response.get("completedLength"))
        code = response.get('errorCode')
        if status == 'error' and (type(code) is not str or re.fullmatch(r'[1-9]|[12][0-9]|3[0-2]',code) is None):
            raise DirectTransferError('invalid stock error code')
        return DirectTransfer(
            job_id=transfer.job_id,
            generation=transfer.generation,
            gid=transfer.gid,
            status=status,
            total_length=total_length,
            completed_length=completed_length,
            partial_path=transfer.destination.partial_path,
            hash_verified=False,
            verification=None,
            error_code=int(code) if status == 'error' else None,
        )

    def _verify_completed_output(
        self, state: DirectTransfer, transfer: _TrackedTransfer
    ) -> DirectTransfer:
        _require_destination(transfer.destination, transfer.job_id)
        try:
            descriptor = os.open(
                state.partial_path,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
            )
            with os.fdopen(descriptor, "rb") as payload:
                details = _staged_partial_payload_details(os.fstat(payload.fileno()))
                if (
                    state.total_length != state.completed_length
                    or details[2] != state.completed_length
                ):
                    raise OSError
                digest = hashlib.sha256()
                if transfer.expected_sha256 is not None:
                    while True:
                        if self._observation_cancelled.is_set():
                            raise DirectTransferError("aria2 verification cancelled")
                        chunk = payload.read(1024 * 1024)
                        if not chunk:
                            break
                        digest.update(chunk)
                    if digest.hexdigest() != transfer.expected_sha256:
                        raise DirectTransferError("aria2 hash verification failed")
                current = _staged_partial_payload_details(state.partial_path.lstat())
                _require_matching_staged_partial_details(details, current)
                _require_matching_staged_partial_details(
                    details, _staged_partial_payload_details(os.fstat(payload.fileno()))
                )
                if self._observation_cancelled.is_set():
                    raise OSError
        except (OSError, PathValidationError):
            raise DirectTransferError("aria2 output is unavailable") from None
        identity = _VerifiedPayloadIdentity(
            details[0], details[1], details[2], details[5], details[3], details[4], details[6]
        )
        if transfer.expected_sha256 is None:
            return replace(state, verification=CompletionVerification.TRANSPORT_VERIFIED,
                           verified_identity=identity)
        return replace(
            state,
            hash_verified=True,
            verification=CompletionVerification.CHECKSUM_VERIFIED,
            verified_identity=identity,
        )

    def _create_private_config(self) -> tuple[Path, Path, str]:
        self._runtime_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        runtime_path = Path(
            tempfile.mkdtemp(prefix="aria2-", dir=str(self._runtime_root))
        )
        config_path = runtime_path / "aria2.conf"
        self._own_private_runtime(runtime_path, config_path)
        os.chmod(runtime_path, 0o700)
        secret = secrets.token_urlsafe(32)
        _write_private_file(config_path, f"rpc-secret={secret}\n".encode("ascii"))
        return runtime_path, config_path, secret

    def _build_argv(self, config_path: Path, port: int) -> tuple[str, ...]:
        # aria2 1.37's --no-conf disables even an explicit --conf-path.  The
        # explicit owner-only config replaces the ambient config while keeping
        # the secret out of argv.
        return (
            str(self._executable),
            f"--conf-path={config_path}",
            "--enable-rpc=true",
            f"--rpc-listen-port={port}",
            "--rpc-listen-all=false",
            "--no-netrc=true",
            "--file-allocation=none",
            "--check-certificate=true",
            f"--max-concurrent-downloads={self._max_concurrent_downloads}",
            f"--split={self._split}",
            f"--max-connection-per-server={self._max_connection_per_server}",
            f"--min-split-size={_MIN_SPLIT_SIZE}",
            "--pause=true",
            "--continue=true",
            "--allow-overwrite=false",
            "--auto-file-renaming=false",
            "--auto-save-interval=0",
            "--max-tries=1",
            "--log=-",
            "--log-level=info",
            "--quiet=true",
            "--console-log-level=error",
        )

    def _wait_for_rpc_ready(self) -> None:
        deadline = time.monotonic() + _RPC_READY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                response = self._rpc("aria2.getVersion", [])
            except DirectEngineError:
                time.sleep(_POLL_INTERVAL_SECONDS)
                continue
            if type(response) is dict:
                return
        raise DirectEngineError("aria2 RPC did not become ready")

    def _rpc(self, method: str, params: list[Any], *, deadline=None) -> Any:
        process, port, secret = self._require_running()
        request = {
            "jsonrpc": "2.0",
            "id": "hermes-downloads",
            "method": method,
            "params": [f"token:{secret}", *params],
        }
        try:
            encoded = json.dumps(request, separators=(",", ":")).encode("utf-8")
            connection = http.client.HTTPConnection(
                "127.0.0.1", port, timeout=_RPC_TIMEOUT_SECONDS if deadline is None else max(.001,min(_RPC_TIMEOUT_SECONDS,deadline-time.monotonic()))
            )
            try:
                connection.request(
                    "POST",
                    "/jsonrpc",
                    body=encoded,
                    headers={"Content-Type": "application/json"},
                )
                response = connection.getresponse()
                body = response.read(_MAX_RPC_RESPONSE_BYTES + 1)
            finally:
                connection.close()
            if response.status != 200 or len(body) > _MAX_RPC_RESPONSE_BYTES:
                raise DirectEngineError("aria2 RPC failed")
            parsed = json.loads(body)
            if type(parsed) is not dict or "error" in parsed or "result" not in parsed:
                raise DirectEngineError("aria2 RPC failed")
            return parsed["result"]
        except (OSError, ValueError, json.JSONDecodeError, http.client.HTTPException):
            pass
        raise DirectEngineError("aria2 RPC failed")

    def _discard_unreadable_gid(self, gid: str) -> None:
        try:
            self._rpc("aria2.forceRemove", [gid])
        except DirectEngineError:
            return

    def _require_running(self) -> tuple[subprocess.Popen[bytes], int, str]:
        if self._process is None or self._port is None or self._secret is None:
            raise DirectEngineError("aria2 is not running")
        return self._process, self._port, self._secret

    def _stop_owned_process(self, *, deadline=None) -> tuple[bool, BaseException | None]:
        process, identity = self._process, self._identity
        if process is None:
            return True, None
        interruption: BaseException | None = None
        try:
            if deadline is None:self._rpc("aria2.forceShutdown", [])
            else:self._rpc("aria2.forceShutdown", [],deadline=deadline)
        except DirectEngineError:
            pass
        except BaseException as raised:
            interruption = raised
        stopped, cleanup_interruption = _stop_process(
            process, None if identity is None else identity.process_group_id, **({} if deadline is None else dict(deadline=deadline))
        )
        return stopped, interruption or cleanup_interruption

    def _clear_mappings(self) -> None:
        self._by_job_id.clear()
        self._by_gid.clear()

    def _bind_stock_owner(self, job_id, generation, gid, epoch, request_id, birth):
        transfer=self._callback_transfer(job_id,generation,gid)
        if self._readback(transfer).status!='paused': raise DirectEngineError('stock owner requires paused GID')
        if (type(epoch) is not int or epoch<1 or type(request_id) is not str or _IDENTIFIER.fullmatch(request_id) is None
            or type(birth) is not ProcessBirthIdentity or capture_process_birth(self._identity)!=birth
            or gid not in self._stock_tags or gid in self._stock_owners): raise DirectEngineError('stock owner binding invalid')
        owner=(job_id,generation,gid,epoch,request_id,
            hashlib.sha256(json.dumps(birth.to_record(),sort_keys=True,separators=(',',':')).encode()).hexdigest(),self._stock_tags[gid])
        self._stock_owners[gid]=owner
        return owner

    def _join_stock_reader(self, deadline):
        reader = self._capture_reader
        if reader is None: return
        reader.join(max(0,deadline-time.monotonic()))
        if reader.is_alive() or not self._capture.eof:
            raise DirectEngineError('stock reader drain incomplete')
        for transfer in self._by_job_id.values():
            self._sealed_stock[(transfer.job_id,transfer.generation,transfer.gid)] = replace(self._capture.result(transfer.gid),owner=self._stock_owners.get(transfer.gid))

    def _sealed_stock_result(self, job_id, generation, gid):
        result = self._sealed_stock.get((job_id,generation,gid))
        if result is None: raise StaleGenerationError('sealed stock attempt unavailable')
        return result

    def _clear_stopped_process_state(self) -> None:
        self._process = None
        self._identity = None
        self._port = None
        self._secret = None
        self._launch_argv = ()

    def _own_private_runtime(self, runtime_path: Path, config_path: Path) -> None:
        self._private_runtime_path = runtime_path
        self._private_config_path = config_path

    def _cleanup_owned_private_runtime(self, *, preserve_primary: bool) -> None:
        runtime_path = self._private_runtime_path
        if runtime_path is None:
            return
        try:
            _remove_private_runtime(runtime_path)
        except BaseException:
            if preserve_primary:
                # Cleanup failure cannot mask a primary exception.  The paths
                # stay owned so close() can retry deletion.
                return
            raise
        self._clear_private_runtime_state()

    def _clear_private_runtime_state(self) -> None:
        self._private_runtime_path = None
        self._private_config_path = None

    def _clear_runtime_state(self) -> None:
        self._clear_stopped_process_state()
        self._clear_private_runtime_state()


def _recover_owned_direct_engine(
    identity: ProcessBirthIdentity,
    capability: _DirectEngineRecoveryCapability,
    *, gid: str | None = None,
) -> int | None:
    """Use one persisted loopback authority to stop a reconciled crashed owner.

    This is deliberately not a controller lifecycle: it creates no subprocess,
    reads no job state, submits no URI, and never signals the persisted PGID.
    """

    try:
        identity = ProcessBirthIdentity.from_record(identity.to_record())
        capability = _DirectEngineRecoveryCapability(
            rpc_port=capability.rpc_port,
            rpc_secret=capability.rpc_secret,
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        raise DirectEngineError("aria2 recovery failed") from None
    if reconcile_process_birth(identity) != "current":
        raise DirectEngineError("aria2 recovery failed")

    completed = None
    if gid is not None:
        if type(gid) is not str or re.fullmatch('[0-9a-fA-F]{16}',gid) is None:
            raise DirectEngineError('aria2 recovery GID invalid')
        completed = _request_recovery_force_shutdown(capability,timeout=_RPC_TIMEOUT_SECONDS,gid=gid)
        if type(completed) is not int or completed<=0: completed=None
    deadline = time.monotonic() + _RECOVERY_SHUTDOWN_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        timeout = min(_RPC_TIMEOUT_SECONDS, max(0.001, deadline - time.monotonic()))
        if _request_recovery_force_shutdown(capability, timeout=timeout):
            break
        time.sleep(_POLL_INTERVAL_SECONDS)
    else:
        raise DirectEngineError("aria2 recovery failed")

    while time.monotonic() < deadline:
        reconciliation = reconcile_process_birth(identity)
        if reconciliation == "absent":
            return completed
        # A daemon that has accepted forceShutdown can be briefly unreconcilable
        # while its former parent reaps it.  Keep the state fail-closed until
        # bounded final absence is proven; do not treat that transient as absent.
        if reconciliation not in {"current", "indeterminate"}:
            raise DirectEngineError("aria2 recovery failed")
        time.sleep(_POLL_INTERVAL_SECONDS)
    if reconcile_process_birth(identity) != "absent":
        raise DirectEngineError("aria2 recovery failed")
    return completed


def _request_recovery_force_shutdown(
    capability: _DirectEngineRecoveryCapability, *, timeout: float, gid: str | None = None
) -> bool | int:
    """Issue only the fixed authenticated loopback recovery shutdown request."""

    request = {
        "jsonrpc": "2.0",
        "id": "hermes-downloads-recovery",
        "method": "aria2.forceShutdown" if gid is None else "aria2.tellStatus",
        "params": [f"token:{capability.rpc_secret}"] + ([] if gid is None else [gid,['gid','completedLength']]),
    }
    try:
        encoded = json.dumps(request, separators=(",", ":")).encode("utf-8")
        connection = http.client.HTTPConnection(
            "127.0.0.1", capability.rpc_port, timeout=timeout
        )
        try:
            connection.request(
                "POST",
                "/jsonrpc",
                body=encoded,
                headers={"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            body = response.read(_MAX_RPC_RESPONSE_BYTES + 1)
        finally:
            connection.close()
        if response.status != 200 or len(body) > _MAX_RPC_RESPONSE_BYTES:
            return False
        parsed = json.loads(body)
        if gid is not None:
            if (type(parsed) is not dict or parsed.get('jsonrpc')!='2.0'
                or parsed.get('id')!='hermes-downloads-recovery' or 'error' in parsed
                or type(parsed.get('result')) is not dict or parsed['result'].get('gid')!=gid):
                return False
            return _parse_counter(parsed['result'].get('completedLength'))
        return (
            type(parsed) is dict
            and parsed.get("jsonrpc") == "2.0"
            and parsed.get("id") == "hermes-downloads-recovery"
            and parsed.get("result") == "OK"
            and "error" not in parsed
        )
    except (OSError, ValueError, json.JSONDecodeError, http.client.HTTPException):
        return False


def _require_executable(value: str | os.PathLike[str]) -> Path:
    try:
        executable = Path(os.fspath(value))
    except TypeError:
        raise TypeError("executable must be a filesystem path") from None
    if not executable.is_absolute() or not executable.is_file() or not os.access(executable, os.X_OK):
        raise DirectEngineError("aria2 executable is unavailable")
    return executable


def _require_absolute_path(value: str | os.PathLike[str], name: str) -> Path:
    try:
        path = Path(os.fspath(value))
    except TypeError:
        raise TypeError(f"{name} must be a filesystem path") from None
    if not path.is_absolute():
        raise ValueError(f"{name} must be absolute")
    return path


def _require_positive(value: object, name: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _require_payload_limit(value: object, name: str) -> None:
    if value is not None and (
        type(value) is not int or not 0 <= value <= _MAX_PAYLOAD_LIMIT_BPS
    ):
        raise ValueError(f"{name} must be a nonnegative signed 64-bit integer or None")


def _require_connection_limit(value: object) -> int:
    value = _require_positive(value, "max_connection_per_server")
    if value > 16:
        raise ValueError("max_connection_per_server exceeds aria2's limit")
    return value


def _require_identifier(value: object, name: str) -> str:
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} must be a nonblank identifier")
    return value


def _require_generation(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("generation must be a nonnegative integer")
    return value


def _require_gid(value: object) -> str:
    if type(value) is not str or _GID.fullmatch(value) is None:
        raise DirectTransferError("aria2 returned an invalid GID")
    return value


def _require_sha256(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ValueError("expected_sha256 must be a lowercase SHA-256 digest")
    return value


def _require_optional_sha256(value: object) -> str | None:
    if value is None:
        return None
    return _require_sha256(value)


def _require_admission(admission: object) -> Admission:
    if type(admission) is not Admission:
        raise TypeError("admission must be an Admission")
    if not admission.allowed:
        raise DirectAdmissionError("queue admission is closed")
    return admission


def _require_source(source: object) -> SourceURL:
    if type(source) is not SourceURL:
        raise TypeError("source must be a validated SourceURL")
    return source


def _require_destination(destination: object, job_id: str) -> DestinationIntent:
    if type(destination) is not DestinationIntent:
        raise TypeError("destination must be a DestinationIntent")
    canonical_root = Path.home() / "Downloads" / "Hermes"
    expected_incomplete_dir = canonical_root / ".incomplete" / job_id
    if (
        destination.root != canonical_root
        or destination.job_id != job_id
        or destination.partial_path.parent != destination.incomplete_dir
        or destination.partial_path.name != destination.filename
        or destination.incomplete_dir != expected_incomplete_dir
        or destination.partial_path != expected_incomplete_dir / destination.filename
        or not destination.partial_path.is_absolute()
        or not destination.final_path.is_absolute()
    ):
        raise DirectTransferError("destination is not a job-owned T05 path")
    _require_safe_partial_output(destination)
    return destination


def _require_safe_partial_output(destination: DestinationIntent) -> None:
    current = Path(destination.root.anchor)
    for component in destination.root.parts[1:]:
        current /= component
        _require_real_directory(current)
    _require_real_directory(destination.root / ".incomplete")
    _require_real_directory(destination.incomplete_dir)
    _require_safe_regular_or_absent(
        destination.partial_path, "destination partial output"
    )
    _require_safe_regular_or_absent(
        destination.partial_path.with_name(f"{destination.partial_path.name}.aria2"),
        "destination aria2 sidecar",
    )


def _require_safe_regular_or_absent(path: Path, name: str) -> None:
    try:
        details = os.lstat(path)
    except FileNotFoundError:
        return
    except OSError:
        raise DirectTransferError(f"{name} is inaccessible") from None
    if not stat.S_ISREG(details.st_mode) or details.st_nlink != 1:
        raise DirectTransferError(f"{name} is unsafe")


def _require_real_directory(path: Path) -> None:
    try:
        details = os.lstat(path)
    except OSError:
        raise DirectTransferError("destination directory is inaccessible") from None
    if not stat.S_ISDIR(details.st_mode):
        raise DirectTransferError("destination directory is unsafe")


def _parse_counter(value: object) -> int:
    if (type(value) is not str or len(value) > _MAX_COUNTER_DIGITS
            or not value.isascii() or not value.isdecimal()):
        raise DirectTransferError("aria2 returned an invalid state")
    try:
        return int(value)
    except ValueError:
        raise DirectTransferError("aria2 returned an invalid state") from None


def _write_private_file(path: Path, content: bytes) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
    )
    try:
        offset = 0
        while offset < len(content):
            written = os.write(descriptor, content[offset:])
            if written <= 0:
                raise OSError("could not write private aria2 configuration")
            offset += written
        os.fchmod(descriptor, 0o600)
    finally:
        os.close(descriptor)


def _remove_private_runtime(path: Path) -> None:
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        return
    except OSError:
        raise DirectEngineError("aria2 private runtime cleanup failed") from None


def _reserve_loopback_port() -> int:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    finally:
        listener.close()
    if type(port) is not int or not 1024 <= port <= 65535:
        raise DirectEngineError("could not reserve an aria2 RPC port")
    return port


def _filtered_environment() -> dict[str, str]:
    return {
        name: value
        for name, value in os.environ.items()
        if name in _SAFE_ENVIRONMENT_NAMES and type(value) is str
    }


def _argv_sha256(argv: tuple[str, ...]) -> str:
    return hashlib.sha256(
        b"\0".join(argument.encode("utf-8", "strict") for argument in argv)
    ).hexdigest()


def _bind_process_group(process: subprocess.Popen[bytes]) -> int | None:
    try:
        process_group_id = os.getpgid(process.pid)
        session_id = os.getsid(process.pid)
    except OSError:
        return None
    if process_group_id != process.pid or session_id != process.pid:
        return None
    return process_group_id


def _group_exists(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    return True


def _wait_for_group_absence(process_group_id: int, deadline: float) -> bool:
    while time.monotonic() < deadline:
        if not _group_exists(process_group_id):
            return True
        time.sleep(_POLL_INTERVAL_SECONDS)
    return not _group_exists(process_group_id)


def _signal_group(process_group_id: int, signal_number: signal.Signals) -> bool:
    try:
        os.killpg(process_group_id, signal_number)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    except OSError:
        return False
    return True


def _wait_for_leader(process: subprocess.Popen[bytes], deadline: float) -> bool:
    try:
        process.wait(timeout=max(0.0, deadline - time.monotonic()))
    except (OSError, subprocess.TimeoutExpired):
        return False
    return True


def _stop_process(
    process: subprocess.Popen[bytes] | None, process_group_id: int | None, *, deadline=None
) -> tuple[bool, BaseException | None]:
    if process is None:
        return True, None
    if process_group_id is None:
        try:
            return _stop_unbound_process(process, **({} if deadline is None else dict(deadline=deadline))), None
        except BaseException as raised:
            return False, raised
    try:
        result = _stop_process_group(process, process_group_id, **({} if deadline is None else dict(deadline=deadline)))
    except BaseException as raised:
        return False, raised
    if type(result) is tuple:
        return result
    return result, None


def _stop_unbound_process(process: subprocess.Popen[bytes], *, deadline=None) -> bool:
    try:
        process.terminate()
    except OSError:
        return False
    if _wait_for_leader(process, min(time.monotonic() + _GROUP_STOP_GRACE_SECONDS, float('inf') if deadline is None else deadline)):
        return True
    try:
        process.kill()
    except OSError:
        return False
    return _wait_for_leader(process, min(time.monotonic() + _GROUP_KILL_GRACE_SECONDS, float('inf') if deadline is None else deadline))


def _stop_process_group(
    process: subprocess.Popen[bytes], process_group_id: int, *, deadline=None
) -> tuple[bool, BaseException | None]:
    """Contain the bound group before reaping its leader or reusing its PID."""

    interruption: BaseException | None = None
    term_deadline = min(time.monotonic() + _GROUP_STOP_GRACE_SECONDS, float('inf') if deadline is None else deadline)
    try:
        _signal_group(process_group_id, signal.SIGTERM)
    except BaseException as raised:
        interruption = raised
    try:
        group_absent_before_reap = _wait_for_group_absence(process_group_id, term_deadline)
    except BaseException as raised:
        if interruption is None:
            interruption = raised
        group_absent_before_reap = False
    if not group_absent_before_reap:
        try:
            _signal_group(process_group_id, signal.SIGKILL)
        except BaseException as raised:
            if interruption is None:
                interruption = raised
        try:
            _wait_for_group_absence(
                process_group_id, min(time.monotonic() + _GROUP_KILL_GRACE_SECONDS, float('inf') if deadline is None else deadline)
            )
        except BaseException as raised:
            if interruption is None:
                interruption = raised
    try:
        leader_reaped = _wait_for_leader(
            process, min(time.monotonic() + _GROUP_KILL_GRACE_SECONDS, float('inf') if deadline is None else deadline)
        )
    except BaseException as raised:
        if interruption is None:
            interruption = raised
        leader_reaped = False
    # A just-exited leader can remain a zombie during the pre-reap observation,
    # so only absence after reaping is authoritative.  Never signal after this
    # point: the numerical process-group ID could have been recycled.
    try:
        final_absence = _wait_for_group_absence(
            process_group_id, min(time.monotonic() + _GROUP_KILL_GRACE_SECONDS, float('inf') if deadline is None else deadline)
        )
    except BaseException as raised:
        if interruption is None:
            interruption = raised
        final_absence = False
    # A final absent group proves there is no process left to signal.  On macOS
    # an already-exiting group can report a transient signal error in this
    # window, so the intermediate signal result is not authoritative.
    return leader_reaped and final_absence, interruption
