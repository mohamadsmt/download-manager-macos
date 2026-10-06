"""Bounded owner-worker AF_UNIX health protocol."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import stat
import time
from typing import Callable, Final, Mapping
import unicodedata

from hermes_downloads.models import JobState

__all__ = [
    "DirectEngineActivateCommand",
    "DirectEngineActivateResult",
    "DirectJobDispatchCommand",
    "DirectJobDispatchResult",
    "HealthServer",
    "IPCError",
    "IPCStateError",
    "JobAddCommand",
    "JobAddResult",
    "JobControlCommand",
    "JobControlResult",
    "JobsPage",
    "MAX_MESSAGE_BYTES",
    "PublicJobRecord",
    "QueueGateCommand",
    "QueueGateResult",
    "WorkerHealth",
    "add_job",
    "activate_direct_engine",
    "control_job",
    "dispatch_direct_job",
    "request_health",
    "request_jobs_page",
    "set_queue_gate",
    "validate_available_socket_path",
]

MAX_MESSAGE_BYTES: Final = 4096
_MAX_RESPONSE_BYTES: Final = 36_864
_MAX_JOBS_PAGE_RECORDS: Final = 100
_PROTOCOL_VERSION: Final = 1
_SOCKET_MODE: Final = 0o600
_SOCKET_BACKLOG: Final = 8
_CONNECTION_TIMEOUT_SECONDS: Final = 0.2
_CLIENT_TIMEOUT_SECONDS: Final = 5.0
_MAX_DARWIN_UNIX_SOCKET_PATH_BYTES: Final = 103
_INVALID_REQUEST: Final = {"error": "invalid_request"}
_COMMAND_CONFLICT: Final = {"error": "command_conflict"}
_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_MAX_COUNTER: Final = (1 << 63) - 1
_MIN_PRIORITY: Final = -(1 << 31)
_MAX_PRIORITY: Final = (1 << 31) - 1
_CATEGORIES: Final = frozenset({"Videos", "Audio", "Documents", "Software", "Other"})
_QUEUE_GATES: Final = frozenset({"paused", "running"})
_JOB_CONTROL_ACTIONS: Final = frozenset({"pause", "resume", "start_now", "remove"})
_JOB_CONTROL_STATUSES: Final = frozenset({"applied", "blocked", "stale"})
_DIRECT_ENGINE_ACTIVATE_STATUSES: Final = frozenset(
    {"active", "blocked", "stale_epoch"}
)
_DIRECT_JOB_DISPATCH_STATUSES: Final = frozenset({"started", "blocked", "stale", "pending"})


class IPCError(RuntimeError):
    """Raised for a bounded public IPC failure."""


class IPCStateError(ValueError):
    """Raised when an IPC endpoint cannot be safely owned."""


def _require_identifier(value: object, name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} must be a nonblank identifier")
    return value


def _require_queue_gate(value: object) -> str:
    if type(value) is not str:
        raise TypeError("gate must be a string")
    if value not in _QUEUE_GATES:
        raise ValueError("gate must be 'paused' or 'running'")
    return value


def _require_job_control_action(value: object) -> str:
    if type(value) is not str:
        raise TypeError("action must be a string")
    if value not in _JOB_CONTROL_ACTIONS:
        raise ValueError("action is not a job-control action")
    return value


def _require_job_control_status(value: object) -> str:
    if type(value) is not str:
        raise TypeError("status must be a string")
    if value not in _JOB_CONTROL_STATUSES:
        raise ValueError("status is not a job-control status")
    return value


def _require_public_job_state(value: object) -> str:
    state = _require_identifier(value, "state")
    try:
        JobState(state)
    except ValueError:
        raise ValueError("state is not a public job state") from None
    return state


def _require_counter(value: object, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= _MAX_COUNTER:
        raise ValueError(f"{name} must be a nonnegative persisted counter")
    return value


def _require_positive_counter(value: object, name: str) -> int:
    value = _require_counter(value, name)
    if value <= 0:
        raise ValueError(f"{name} must be a positive persisted counter")
    return value


def _require_priority(value: object) -> int:
    if type(value) is not int:
        raise TypeError("priority must be an integer")
    if not _MIN_PRIORITY <= value <= _MAX_PRIORITY:
        raise ValueError("priority must fit a signed 32-bit integer")
    return value


def _require_category(value: object) -> str:
    if type(value) is not str or value not in _CATEGORIES:
        raise ValueError("category is not a supported output category")
    return value


def _require_path_component(value: object, name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not value or value in {".", ".."} or value.startswith("."):
        raise ValueError(f"{name} is not a valid output name")
    if "/" in value or "\\" in value or "\x00" in value:
        raise ValueError(f"{name} must be one path component")
    if any(unicodedata.category(character) in {"Cc", "Cs"} for character in value):
        raise ValueError(f"{name} contains a control character")
    return value


def _collision_filename(filename: str, job: str) -> str:
    suffix = Path(filename).suffix
    stem = filename[: -len(suffix)] if suffix else filename
    return f"{stem}--{job}{suffix}"


def _require_direct_engine_activate_status(value: object) -> str:
    if type(value) is not str:
        raise TypeError("status must be a string")
    if value not in _DIRECT_ENGINE_ACTIVATE_STATUSES:
        raise ValueError("status is not a direct-engine activation status")
    return value


def _require_direct_job_dispatch_status(value: object) -> str:
    if type(value) is not str:
        raise TypeError("status must be a string")
    if value not in _DIRECT_JOB_DISPATCH_STATUSES:
        raise ValueError("status is not a direct-job dispatch status")
    return value


def _canonical_payload_digest(record: Mapping[str, object]) -> str:
    payload = json.dumps(
        record, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _append_bounded_bytes(output: bytearray, value: bytes, maximum: int = MAX_MESSAGE_BYTES) -> None:
    """Append fixed JSON bytes without exceeding the complete IPC record cap."""

    if len(value) > maximum - len(output):
        raise ValueError("job-add request exceeds the IPC message limit")
    output.extend(value)


def _append_bounded_json_string(output: bytearray, value: str, maximum: int = MAX_MESSAGE_BYTES) -> None:
    """Append one ensure_ascii JSON string without materializing an unbounded value."""

    if maximum != MAX_MESSAGE_BYTES:
        # A v2 scalar is bounded before the C encoder can allocate its escaped form.
        if len(value) > 32768 or len(value.encode('utf-8')) > 32768:
            raise ValueError('batch scalar exceeds limit')
        _append_bounded_bytes(output, json.dumps(value, ensure_ascii=True).encode('ascii'), maximum)
        return
    _append_bounded_bytes(output, b'"')
    for character in value:
        code_point = ord(character)
        if character == '"':
            encoded = b'\\"'
        elif character == "\\":
            encoded = b"\\\\"
        elif character == "\b":
            encoded = b"\\b"
        elif character == "\t":
            encoded = b"\\t"
        elif character == "\n":
            encoded = b"\\n"
        elif character == "\f":
            encoded = b"\\f"
        elif character == "\r":
            encoded = b"\\r"
        elif 0x20 <= code_point <= 0x7E:
            encoded = bytes((code_point,))
        elif code_point <= 0xFFFF:
            encoded = f"\\u{code_point:04x}".encode("ascii")
        else:
            supplementary = code_point - 0x10000
            encoded = (
                f"\\u{0xD800 + (supplementary >> 10):04x}"
                f"\\u{0xDC00 + (supplementary & 0x3FF):04x}"
            ).encode("ascii")
        _append_bounded_bytes(output, encoded)
    _append_bounded_bytes(output, b'"')


def _job_add_wire_request(
    *,
    job: str,
    request_id: str,
    source_url: str,
    priority: int,
    order_key: int,
    category: str,
    partial_filename: str,
    selected_final_filename: str,
) -> bytes:
    """Build the one bounded canonical wire record before its digest exists."""

    output = bytearray()
    _append_bounded_bytes(output, b'{"category":')
    _append_bounded_json_string(output, category)
    _append_bounded_bytes(output, b',"job":')
    _append_bounded_json_string(output, job)
    _append_bounded_bytes(output, b',"op":"job_add","order_key":')
    _append_bounded_bytes(output, str(order_key).encode("ascii"))
    _append_bounded_bytes(output, b',"partial_filename":')
    _append_bounded_json_string(output, partial_filename)
    _append_bounded_bytes(output, b',"priority":')
    _append_bounded_bytes(output, str(priority).encode("ascii"))
    _append_bounded_bytes(output, b',"request_id":')
    _append_bounded_json_string(output, request_id)
    _append_bounded_bytes(output, b',"selected_final_filename":')
    _append_bounded_json_string(output, selected_final_filename)
    _append_bounded_bytes(output, b',"source_kind":"direct","source_url":')
    _append_bounded_json_string(output, source_url)
    _append_bounded_bytes(output, b',"start":false}\n')
    return bytes(output)


_BATCH_PREAMBLE: Final = b'HDM2\n'
_TARGET_PREAMBLE: Final = b'HDT2\n'
_MAX_TARGET_BODY: Final = 128 * 1024
_MAX_TARGET_REPLY: Final = 256 * 1024
_TARGET_ERRORS: Final = frozenset({'target_request_invalid', 'unsupported_selection_size',
    'target_epoch_stale', 'command_conflict', 'target_authority_corrupt', 'target_deadline'})
_MAX_BATCH_BODY: Final = 16 * 1024 * 1024
_MAX_BATCH_ENTRY: Final = 32 * 1024
_MAX_BATCH_REPLY: Final = 256 * 1024
_BATCH_REASONS: Final = frozenset({'invalid_entry', 'invalid_source', 'invalid_destination',
    'job_conflict', 'destination_conflict', 'request_conflict'})
_BATCH_ENTRY_KEYS: Final = frozenset({'job', 'source_kind', 'source_url', 'priority',
    'category', 'partial_filename', 'selected_final_filename', 'expected_sha256'})


def _batch_component(value: object) -> str:
    name = _require_path_component(value, 'batch component')
    if len(name) > 255 or len(name.encode('utf-8')) > 255:
        raise ValueError('batch component exceeds limit')
    return name


def _batch_collection_id(name: str | None) -> str | None:
    return None if name is None else 'collection:' + hashlib.sha256(
        b'hermes-downloads:collection:v1\0' + name.encode('utf-8')).hexdigest()


def _batch_entry(value: object) -> dict[str, object]:
    """Syntactic normalization only. URL policy is applied once by the store."""
    if type(value) is not dict or not (_BATCH_ENTRY_KEYS - {'expected_sha256'}) <= set(value) <= _BATCH_ENTRY_KEYS:
        raise ValueError('invalid batch entry')
    job = _require_identifier(value['job'], 'job')
    if type(value['source_kind']) is not str or value['source_kind'] != 'direct':
        raise ValueError('invalid batch source kind')
    url = value['source_url']
    if type(url) is not str or len(url) > 8192 or len(url.encode('utf-8')) > 8192:
        raise ValueError('invalid batch source bytes')
    _require_priority(value['priority']); _require_category(value['category'])
    original = _batch_component(value['partial_filename'])
    final = _batch_component(value['selected_final_filename'])
    if final not in {original, _collision_filename(original, job)}:
        raise ValueError('invalid batch selected filename')
    expected = value.get('expected_sha256')
    if expected is not None and (type(expected) is not str or re.fullmatch('[0-9a-f]{64}', expected, flags=re.ASCII) is None):
        raise ValueError('invalid batch expected hash')
    return {**value, 'expected_sha256': expected}


def _batch_canonical(value: object, maximum: int, *, depth: int = 0) -> bytes:
    """Bounded canonical JSON for this closed command and its sealed creation record."""
    output = bytearray()
    def append(raw):
        _append_bounded_bytes(output, raw, maximum)
    def encode(item, level):
        if level > 8:
            raise ValueError('batch depth exceeds limit')
        if item is None: append(b'null')
        elif type(item) is bool: append(b'true' if item else b'false')
        elif type(item) is str: _append_bounded_json_string(output, item, maximum)
        elif type(item) is int:
            if not -(1 << 63) <= item <= (1 << 63) - 1:
                raise ValueError('batch scalar exceeds limit')
            append(str(item).encode('ascii'))
        elif type(item) is float:
            append(json.dumps(item, allow_nan=False).encode('ascii'))
        elif type(item) in {list, tuple}:
            if len(item) > 500: raise ValueError('batch array exceeds limit')
            append(b'[')
            for index, child in enumerate(item):
                if index: append(b',')
                encode(child, level + 1)
            append(b']')
        elif type(item) is dict:
            if len(item) > 32 or any(type(key) is not str or len(key) > 128 for key in item):
                raise ValueError('batch object exceeds limit')
            append(b'{')
            for index, key in enumerate(sorted(item)):
                if index: append(b',')
                encode(key, level + 1); append(b':'); encode(item[key], level + 1)
            append(b'}')
        else: raise ValueError('invalid batch scalar')
    encode(value, depth)
    return bytes(output)


def _batch_decode(payload: bytes) -> object:
    # Check lexical nesting before json.loads, ignoring braces in escaped strings.
    nesting = 0; quoted = False; escaped = False
    for byte in payload:
        if quoted:
            if escaped: escaped = False
            elif byte == 92: escaped = True
            elif byte == 34: quoted = False
        elif byte == 34: quoted = True
        elif byte in (91, 123):
            nesting += 1
            if nesting > 8: raise ValueError('batch depth exceeds limit')
        elif byte in (93, 125): nesting -= 1
    def reject_constant(_): raise ValueError('invalid batch number')
    return json.loads(payload.decode('utf-8'), object_pairs_hook=_reject_duplicate_object_keys,
        parse_constant=reject_constant)


@dataclass(frozen=True, slots=True, repr=False)
class AddBatchCommand:
    request_id: str
    collection: str | None
    entries: object = field(repr=False)
    payload_digest: str = field(init=False)
    _wire_request: bytes = field(init=False, repr=False)

    def __post_init__(self):
        _require_identifier(self.request_id, 'request_id')
        if self.collection is not None: _batch_component(self.collection)
        if type(self.entries) is not list or not 1 <= len(self.entries) <= 500:
            raise ValueError('invalid batch count')
        # Canonical bytes are the immutable snapshot; no caller-owned containers survive.
        frozen = []
        for value in self.entries:
            try: normalized = _batch_entry(value)
            except (TypeError, ValueError): normalized = value
            frozen.append(_batch_canonical(normalized, _MAX_BATCH_ENTRY, depth=2))
        prefix = _batch_canonical({'collection': self.collection}, _MAX_BATCH_ENTRY)
        body = bytearray(prefix[:-1]); body.extend(b',"entries":[')
        for index, raw in enumerate(frozen):
            if index: _append_bounded_bytes(body, b',', _MAX_BATCH_BODY)
            _append_bounded_bytes(body, raw, _MAX_BATCH_BODY)
        _append_bounded_bytes(body, b'],"op":"add_batch","protocol_version":2,"request_id":', _MAX_BATCH_BODY)
        _append_bounded_json_string(body, self.request_id, _MAX_BATCH_BODY)
        _append_bounded_bytes(body, b',"start":false}', _MAX_BATCH_BODY)
        raw = bytes(body)
        object.__setattr__(self, 'entries', tuple(frozen))
        object.__setattr__(self, '_wire_request', raw)
        object.__setattr__(self, 'payload_digest', hashlib.sha256(raw).hexdigest())

    def to_record(self):
        return _batch_decode(self._wire_request)

    @classmethod
    def from_record(cls, record):
        if type(record) is not dict or set(record) != {'op', 'protocol_version', 'request_id', 'start', 'collection', 'entries'}:
            raise ValueError('invalid batch envelope')
        if (record['op'] != 'add_batch' or type(record['op']) is not str
                or type(record['protocol_version']) is not int or record['protocol_version'] != 2
                or record['start'] is not False):
            raise ValueError('invalid batch envelope')
        return cls(record['request_id'], record['collection'], record['entries'])


@dataclass(frozen=True, slots=True)
class AddBatchEntryResult:
    index: int
    status: str
    reason: str | None
    job: str | None
    child_request_id: str | None
    generation: int | None
    revision: int | None
    state: str | None
    order_key: int | None

    def __post_init__(self):
        if type(self.index) is not int or not 0 <= self.index < 500: raise ValueError('invalid batch index')
        if self.status == 'applied':
            _require_identifier(self.job, 'job'); _require_identifier(self.child_request_id, 'child_request_id')
            if self.reason is not None or type(self.generation) is not int or self.generation != 0 or type(self.revision) is not int or self.revision != 0 or self.state != 'queued':
                raise ValueError('invalid batch creation receipt')
            _require_counter(self.order_key, 'order_key')
        elif self.status == 'blocked':
            if type(self.reason) is not str or self.reason not in _BATCH_REASONS or any(value is not None for value in (self.job, self.child_request_id, self.generation, self.revision, self.state, self.order_key)):
                raise ValueError('invalid batch rejection receipt')
        else: raise ValueError('invalid batch result status')

    def to_record(self):
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_record(cls, record):
        if type(record) is not dict or set(record) != set(cls.__dataclass_fields__): raise ValueError('invalid batch result')
        return cls(**record)


@dataclass(frozen=True, slots=True)
class AddBatchResult:
    request_id: str
    replayed: bool
    results: tuple[AddBatchEntryResult, ...]

    def __post_init__(self):
        _require_identifier(self.request_id, 'request_id')
        if type(self.replayed) is not bool or type(self.results) is not tuple or not 1 <= len(self.results) <= 500:
            raise ValueError('invalid batch reply')
        for index, result in enumerate(self.results):
            if type(result) is not AddBatchEntryResult or result.index != index: raise ValueError('invalid batch ordered reply')

    def to_record(self):
        return dict(protocol_version=2, request_id=self.request_id, status='applied',
            readback_kind='creation_receipt', replayed=self.replayed,
            results=[result.to_record() for result in self.results])

    @classmethod
    def from_record(cls, record):
        if type(record) is not dict or set(record) != {'protocol_version', 'request_id', 'status', 'readback_kind', 'replayed', 'results'}:
            raise ValueError('invalid batch reply')
        if type(record['protocol_version']) is not int or record['protocol_version'] != 2 or record['status'] != 'applied' or record['readback_kind'] != 'creation_receipt' or type(record['results']) is not list:
            raise ValueError('invalid batch reply')
        return cls(record['request_id'], record['replayed'], tuple(AddBatchEntryResult.from_record(r) for r in record['results']))


def _batch_frame(payload: bytes) -> bytes:
    return _BATCH_PREAMBLE + len(payload).to_bytes(4, 'big') + payload


def _batch_read_exact(connection, count, deadline):
    output = bytearray()
    while len(output) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0: raise TimeoutError
        connection.settimeout(remaining)
        chunk = connection.recv(min(65536, count - len(output)))
        if not chunk: raise ValueError('truncated batch frame')
        output.extend(chunk)
    return bytes(output)


def _batch_read_body(connection, deadline, maximum):
    length = int.from_bytes(_batch_read_exact(connection, 4, deadline), 'big')
    if not 1 <= length <= maximum: raise ValueError('invalid batch length')
    payload = _batch_read_exact(connection, length, deadline)
    remaining = deadline - time.monotonic()
    if remaining <= 0: raise TimeoutError
    connection.settimeout(remaining)
    if connection.recv(1): raise ValueError('trailing batch bytes')
    return payload


def add_batch(socket_path: Path, *, request_id: str, collection: str | None, entries: list) -> AddBatchResult:
    deadline = time.monotonic() + 5.0
    path = _require_socket_path(socket_path)
    try: command = AddBatchCommand(request_id, collection, entries)
    except (TypeError, ValueError, RecursionError): raise IPCError('invalid_request') from None
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0: raise TimeoutError
            client.settimeout(remaining); client.connect(str(path))
            remaining = deadline - time.monotonic()
            if remaining <= 0: raise TimeoutError
            client.settimeout(remaining); client.sendall(_batch_frame(command._wire_request)); client.shutdown(socket.SHUT_WR)
            if _batch_read_exact(client, 5, deadline) != _BATCH_PREAMBLE: raise ValueError('invalid batch preamble')
            record = _batch_decode(_batch_read_body(client, deadline, _MAX_BATCH_REPLY))
        except (OSError, TimeoutError): raise IPCError('ipc_unavailable') from None
        except (TypeError, ValueError, RecursionError): raise IPCError('ipc_response_invalid') from None
    if record == _COMMAND_CONFLICT: raise IPCError('command_conflict')
    if record == _INVALID_REQUEST: raise IPCError('invalid_request')
    if record == {'error': 'batch_state_invalid'}: raise IPCError('batch_state_invalid')
    try:
        result = AddBatchResult.from_record(record)
        if result.request_id != command.request_id or len(result.results) != len(command.entries): raise ValueError('batch reply identity changed')
        return result
    except (TypeError, ValueError): raise IPCError('ipc_response_invalid') from None


@dataclass(frozen=True, slots=True, repr=False)
class TargetAuthorizeCommand:
    request_id: str
    expected_worker_epoch: int
    action: str
    selector: object = field(repr=False)
    payload_digest: str = field(init=False)
    _wire_request: bytes = field(init=False, repr=False)

    def __post_init__(self):
        _require_identifier(self.request_id, 'request_id')
        _require_positive_counter(self.expected_worker_epoch, 'expected_worker_epoch')
        if type(self.action) is not str or self.action not in {'start', 'resume', 'start_now'}:
            raise ValueError('target_request_invalid')
        selector = self.selector
        if type(selector) is not dict: raise ValueError('target_request_invalid')
        if selector.get('kind') == 'jobs' and set(selector) == {'kind', 'targets'}:
            targets = selector['targets']
            if type(targets) is not list or not targets: raise ValueError('target_request_invalid')
            if len(targets) > 500: raise ValueError('unsupported_selection_size')
            jobs = set()
            for item in targets:
                if type(item) is not dict or set(item) != {'job', 'expected_revision'}:
                    raise ValueError('target_request_invalid')
                job = _require_identifier(item['job'], 'job')
                if job in jobs: raise ValueError('target_request_invalid')
                jobs.add(job)
                if item['expected_revision'] is not None:
                    _require_counter(item['expected_revision'], 'expected_revision')
        elif selector.get('kind') == 'creation_cohort' and set(selector) == {'kind', 'creation', 'indices'}:
            creation = selector['creation']
            if (type(creation) is not dict or set(creation) != {'kind', 'request_id'}
                    or type(creation['kind']) is not str or creation['kind'] not in {'single', 'batch'}):
                raise ValueError('target_request_invalid')
            _require_identifier(creation['request_id'], 'creation request_id')
            indices = selector['indices']
            if indices is not None:
                if type(indices) is not list or not indices: raise ValueError('target_request_invalid')
                if len(indices) > 500: raise ValueError('unsupported_selection_size')
                if any(type(i) is not int or not 0 <= i < 500 or (creation['kind'] == 'single' and i != 0) for i in indices):
                    raise ValueError('target_request_invalid')
                if len(set(indices)) != len(indices): raise ValueError('target_request_invalid')
        else: raise ValueError('target_request_invalid')
        raw = _batch_canonical(dict(op='target_authorize', protocol_version=2,
            request_id=self.request_id, expected_worker_epoch=self.expected_worker_epoch,
            action=self.action, selector=selector), _MAX_TARGET_BODY)
        object.__setattr__(self, 'selector', raw)
        object.__setattr__(self, '_wire_request', raw)
        object.__setattr__(self, 'payload_digest', hashlib.sha256(raw).hexdigest())

    def to_record(self):
        return _batch_decode(self._wire_request)

    @classmethod
    def from_record(cls, record):
        if (type(record) is not dict or set(record) != {'op', 'protocol_version', 'request_id',
                'expected_worker_epoch', 'action', 'selector'} or record['op'] != 'target_authorize'
                or type(record['protocol_version']) is not int or record['protocol_version'] != 2):
            raise ValueError('target_request_invalid')
        return cls(record['request_id'], record['expected_worker_epoch'], record['action'], record['selector'])


@dataclass(frozen=True, slots=True, repr=False)
class TargetAuthorizeResult:
    request_id: str
    worker_epoch: int
    replayed: bool
    results: object = field(repr=False)
    _wire_reply: bytes = field(init=False, repr=False)

    def __post_init__(self):
        _require_identifier(self.request_id, 'request_id')
        _require_positive_counter(self.worker_epoch, 'worker_epoch')
        if type(self.replayed) is not bool or type(self.results) is not list or not 1 <= len(self.results) <= 500:
            raise ValueError('invalid target receipt')
        for index, result in enumerate(self.results):
            if type(result) is not dict or set(result) != {'index', 'job', 'outcome', 'reason',
                    'round_generation', 'captured_generation', 'captured_revision', 'held_by'}:
                raise ValueError('invalid target member')
            if type(result['index']) is not int or result['index'] != index:
                raise ValueError('invalid target index')
            _require_identifier(result['job'], 'job')
            outcome, reason = result['outcome'], result['reason']
            if outcome in {'new_authority', 'existing_authority'}:
                if reason is not None: raise ValueError('invalid target outcome')
                _require_positive_counter(result['round_generation'], 'round_generation')
            elif outcome == 'stale':
                if reason != 'stale_revision' or result['round_generation'] is not None: raise ValueError('invalid target outcome')
            elif outcome == 'blocked':
                if reason not in {'unknown_job', 'legacy_kind', 'terminal', 'busy', 'incompatible_authority'} or result['round_generation'] is not None:
                    raise ValueError('invalid target outcome')
            else: raise ValueError('invalid target outcome')
            for name in ('captured_generation', 'captured_revision'):
                if result[name] is not None: _require_counter(result[name], name)
            held = result['held_by']
            if held is not None and (type(held) is not list or held != [gate for gate in ('global','collection','manual','not_due') if gate in held]):
                raise ValueError('invalid target gates')
            if outcome in {'new_authority', 'existing_authority'} and any(result[n] is None for n in ('captured_generation','captured_revision','held_by')):
                raise ValueError('missing target capture')
            if reason == 'unknown_job' and any(result[n] is not None for n in ('captured_generation','captured_revision','held_by')):
                raise ValueError('fabricated unknown target')
        raw = _batch_canonical(dict(op='target_authorize_result', protocol_version=2,
            request_id=self.request_id, status='applied', readback_kind='authorization_receipt',
            replayed=self.replayed, worker_epoch=self.worker_epoch, execution_effect='none', results=self.results), _MAX_TARGET_REPLY)
        object.__setattr__(self, 'results', raw)
        object.__setattr__(self, '_wire_reply', raw)

    def to_record(self):
        return _batch_decode(self._wire_reply)

    @classmethod
    def from_record(cls, record):
        if (type(record) is not dict or set(record) != {'op','protocol_version','request_id','status',
                'readback_kind','replayed','worker_epoch','execution_effect','results'}
                or record['op'] != 'target_authorize_result' or type(record['protocol_version']) is not int
                or record['protocol_version'] != 2 or record['status'] != 'applied'
                or record['readback_kind'] != 'authorization_receipt' or record['execution_effect'] != 'none'):
            raise ValueError('invalid target receipt')
        return cls(record['request_id'], record['worker_epoch'], record['replayed'], record['results'])


def target_authorize(socket_path: Path, command: TargetAuthorizeCommand) -> TargetAuthorizeResult:
    deadline = time.monotonic() + 5.0
    if type(command) is not TargetAuthorizeCommand: raise IPCError('target_request_invalid')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        try:
            client.settimeout(max(0.001, deadline - time.monotonic()))
            client.connect(str(_require_socket_path(socket_path)))
            client.settimeout(max(0.001, deadline - time.monotonic()))
            client.sendall(_TARGET_PREAMBLE + len(command._wire_request).to_bytes(4,'big') + command._wire_request)
            client.shutdown(socket.SHUT_WR)
            if _batch_read_exact(client, 5, deadline) != _TARGET_PREAMBLE: raise ValueError('wrong target preamble')
            record = _batch_decode(_batch_read_body(client, deadline, _MAX_TARGET_REPLY))
        except (OSError, TimeoutError): raise IPCError('ipc_unavailable') from None
        except (TypeError, ValueError, RecursionError): raise IPCError('ipc_response_invalid') from None
    if type(record) is dict and set(record) == {'error'} and type(record['error']) is str and record['error'] in _TARGET_ERRORS:
        raise IPCError(record['error'])
    try:
        result = TargetAuthorizeResult.from_record(record)
        if result.request_id != command.request_id: raise ValueError('target reply identity changed')
        return result
    except (TypeError, ValueError): raise IPCError('ipc_response_invalid') from None


@dataclass(frozen=True, slots=True)
class WorkerHealth:
    """The fixed non-mutating worker health projection."""

    worker_epoch: int
    queue_gate: str

    def __post_init__(self) -> None:
        if type(self.worker_epoch) is not int or self.worker_epoch <= 0:
            raise ValueError("worker_epoch must be a positive integer")
        if self.queue_gate not in {"paused", "running"}:
            raise ValueError("queue_gate is invalid")

    def to_record(self) -> dict[str, int | str]:
        return {
            "protocol_version": _PROTOCOL_VERSION,
            "worker_epoch": self.worker_epoch,
            "queue_gate": self.queue_gate,
        }

    @classmethod
    def from_record(cls, record: object) -> "WorkerHealth":
        if type(record) is not dict or set(record) != {
            "protocol_version",
            "worker_epoch",
            "queue_gate",
        }:
            raise IPCError("ipc_response_invalid")
        protocol_version = record["protocol_version"]
        worker_epoch = record["worker_epoch"]
        queue_gate = record["queue_gate"]
        if type(protocol_version) is not int or protocol_version != _PROTOCOL_VERSION:
            raise IPCError("ipc_response_invalid")
        try:
            return cls(worker_epoch=worker_epoch, queue_gate=queue_gate)
        except (TypeError, ValueError):
            raise IPCError("ipc_response_invalid") from None


@dataclass(frozen=True, slots=True)
class JobAddCommand:
    """One closed direct-job creation request with a locally derived digest."""

    job: str
    request_id: str
    source_url: str
    priority: int
    order_key: int
    category: str
    partial_filename: str
    selected_final_filename: str
    payload_digest: str = field(init=False)
    _wire_request: bytes = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        job = _require_identifier(self.job, "job")
        request_id = _require_identifier(self.request_id, "request_id")
        if type(self.source_url) is not str:
            raise TypeError("source_url must be a string")
        priority = _require_priority(self.priority)
        order_key = _require_counter(self.order_key, "order_key")
        category = _require_category(self.category)
        partial_filename = _require_path_component(
            self.partial_filename, "partial_filename"
        )
        selected_final_filename = _require_path_component(
            self.selected_final_filename, "selected_final_filename"
        )
        if selected_final_filename not in {
            partial_filename,
            _collision_filename(partial_filename, job),
        }:
            raise ValueError("selected_final_filename is not a managed final name")
        object.__setattr__(self, "job", job)
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "priority", priority)
        object.__setattr__(self, "order_key", order_key)
        object.__setattr__(self, "category", category)
        object.__setattr__(self, "partial_filename", partial_filename)
        object.__setattr__(self, "selected_final_filename", selected_final_filename)
        wire_request = _job_add_wire_request(
            job=job,
            request_id=request_id,
            source_url=self.source_url,
            priority=priority,
            order_key=order_key,
            category=category,
            partial_filename=partial_filename,
            selected_final_filename=selected_final_filename,
        )
        object.__setattr__(self, "_wire_request", wire_request)
        object.__setattr__(
            self,
            "payload_digest",
            hashlib.sha256(
                wire_request[:-1].replace(b',"op":"job_add"', b"", 1)
            ).hexdigest(),
        )

    def to_record(self) -> dict[str, bool | int | str]:
        """Return the exact direct-only wire record without an injectable digest."""

        return {
            "op": "job_add",
            "job": self.job,
            "request_id": self.request_id,
            "source_url": self.source_url,
            "source_kind": "direct",
            "priority": self.priority,
            "order_key": self.order_key,
            "category": self.category,
            "partial_filename": self.partial_filename,
            "selected_final_filename": self.selected_final_filename,
            "start": False,
        }

    @classmethod
    def from_record(cls, record: object) -> "JobAddCommand":
        if type(record) is not dict or set(record) != {
            "op",
            "job",
            "request_id",
            "source_url",
            "source_kind",
            "priority",
            "order_key",
            "category",
            "partial_filename",
            "selected_final_filename",
            "start",
        }:
            raise ValueError("job-add request is invalid")
        if (
            type(record["op"]) is not str
            or record["op"] != "job_add"
            or type(record["source_kind"]) is not str
            or record["source_kind"] != "direct"
            or type(record["start"]) is not bool
            or record["start"] is not False
        ):
            raise ValueError("job-add request is invalid")
        return cls(
            job=record["job"],
            request_id=record["request_id"],
            source_url=record["source_url"],
            priority=record["priority"],
            order_key=record["order_key"],
            category=record["category"],
            partial_filename=record["partial_filename"],
            selected_final_filename=record["selected_final_filename"],
        )


@dataclass(frozen=True, slots=True)
class JobAddResult:
    """The exact redacted receipt for one durable direct-job add request."""

    applied: bool
    job: str
    generation: int
    revision: int

    def __post_init__(self) -> None:
        if type(self.applied) is not bool:
            raise TypeError("applied must be a boolean")
        object.__setattr__(self, "job", _require_identifier(self.job, "job"))
        object.__setattr__(
            self, "generation", _require_counter(self.generation, "generation")
        )
        object.__setattr__(self, "revision", _require_counter(self.revision, "revision"))

    def to_record(self) -> dict[str, bool | int | str]:
        return {
            "applied": self.applied,
            "job": self.job,
            "generation": self.generation,
            "revision": self.revision,
        }

    @classmethod
    def from_record(cls, record: object) -> "JobAddResult":
        if type(record) is not dict or set(record) != {
            "applied",
            "job",
            "generation",
            "revision",
        }:
            raise IPCError("ipc_response_invalid")
        try:
            return cls(
                applied=record["applied"],
                job=record["job"],
                generation=record["generation"],
                revision=record["revision"],
            )
        except (TypeError, ValueError):
            raise IPCError("ipc_response_invalid") from None


@dataclass(frozen=True, slots=True)
class DirectEngineActivateCommand:
    """One fixed direct-engine activation request, fenced by worker epoch."""

    expected_worker_epoch: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "expected_worker_epoch",
            _require_positive_counter(
                self.expected_worker_epoch, "expected_worker_epoch"
            ),
        )

    def to_record(self) -> dict[str, int | str]:
        return {
            "op": "direct_engine_activate",
            "expected_worker_epoch": self.expected_worker_epoch,
        }

    @classmethod
    def from_record(cls, record: object) -> "DirectEngineActivateCommand":
        if type(record) is not dict or set(record) != {
            "op",
            "expected_worker_epoch",
        }:
            raise ValueError("direct-engine activation request is invalid")
        if record["op"] != "direct_engine_activate":
            raise ValueError("direct-engine activation request is invalid")
        return cls(expected_worker_epoch=record["expected_worker_epoch"])


@dataclass(frozen=True, slots=True)
class DirectEngineActivateResult:
    """The bounded direct-engine activation readback."""

    worker_epoch: int
    status: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "worker_epoch",
            _require_positive_counter(self.worker_epoch, "worker_epoch"),
        )
        object.__setattr__(
            self,
            "status",
            _require_direct_engine_activate_status(self.status),
        )

    def to_record(self) -> dict[str, int | str]:
        return {"worker_epoch": self.worker_epoch, "status": self.status}

    @classmethod
    def from_record(cls, record: object) -> "DirectEngineActivateResult":
        if type(record) is not dict or set(record) != {"worker_epoch", "status"}:
            raise IPCError("ipc_response_invalid")
        try:
            return cls(worker_epoch=record["worker_epoch"], status=record["status"])
        except (TypeError, ValueError):
            raise IPCError("ipc_response_invalid") from None


@dataclass(frozen=True, slots=True)
class DirectJobDispatchCommand:
    """One idempotent direct-body dispatch fenced by worker and job revisions."""

    job: str
    expected_worker_epoch: int
    expected_generation: int
    expected_revision: int
    request_id: str
    payload_digest: str = field(init=False)

    def __post_init__(self) -> None:
        job = _require_identifier(self.job, "job")
        expected_worker_epoch = _require_positive_counter(
            self.expected_worker_epoch, "expected_worker_epoch"
        )
        expected_generation = _require_counter(
            self.expected_generation, "expected_generation"
        )
        expected_revision = _require_counter(
            self.expected_revision, "expected_revision"
        )
        request_id = _require_identifier(self.request_id, "request_id")
        object.__setattr__(self, "job", job)
        object.__setattr__(self, "expected_worker_epoch", expected_worker_epoch)
        object.__setattr__(self, "expected_generation", expected_generation)
        object.__setattr__(self, "expected_revision", expected_revision)
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(
            self,
            "payload_digest",
            _canonical_payload_digest(
                {
                    "expected_generation": expected_generation,
                    "expected_revision": expected_revision,
                    "expected_worker_epoch": expected_worker_epoch,
                    "job": job,
                    "request_id": request_id,
                }
            ),
        )

    def to_record(self) -> dict[str, int | str]:
        return {
            "op": "direct_job_dispatch",
            "job": self.job,
            "expected_worker_epoch": self.expected_worker_epoch,
            "expected_generation": self.expected_generation,
            "expected_revision": self.expected_revision,
            "request_id": self.request_id,
        }

    @classmethod
    def from_record(cls, record: object) -> "DirectJobDispatchCommand":
        if type(record) is not dict or set(record) != {
            "op",
            "job",
            "expected_worker_epoch",
            "expected_generation",
            "expected_revision",
            "request_id",
        }:
            raise ValueError("direct-job dispatch request is invalid")
        if record["op"] != "direct_job_dispatch":
            raise ValueError("direct-job dispatch request is invalid")
        return cls(
            job=record["job"],
            expected_worker_epoch=record["expected_worker_epoch"],
            expected_generation=record["expected_generation"],
            expected_revision=record["expected_revision"],
            request_id=record["request_id"],
        )


@dataclass(frozen=True, slots=True)
class DirectJobDispatchResult:
    """A redacted bounded dispatch receipt with no engine or source authority."""

    status: str
    job: str
    generation: int
    revision: int
    state: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", _require_direct_job_dispatch_status(self.status))
        object.__setattr__(self, "job", _require_identifier(self.job, "job"))
        object.__setattr__(
            self, "generation", _require_counter(self.generation, "generation")
        )
        object.__setattr__(self, "revision", _require_counter(self.revision, "revision"))
        object.__setattr__(self, "state", _require_public_job_state(self.state))

    def to_record(self) -> dict[str, int | str]:
        return {
            "status": self.status,
            "job": self.job,
            "generation": self.generation,
            "revision": self.revision,
            "state": self.state,
        }

    @classmethod
    def from_record(cls, record: object) -> "DirectJobDispatchResult":
        if type(record) is not dict or set(record) != {
            "status",
            "job",
            "generation",
            "revision",
            "state",
        }:
            raise IPCError("ipc_response_invalid")
        try:
            return cls(
                status=record["status"],
                job=record["job"],
                generation=record["generation"],
                revision=record["revision"],
                state=record["state"],
            )
        except (TypeError, ValueError):
            raise IPCError("ipc_response_invalid") from None


@dataclass(frozen=True, slots=True)
class QueueGateCommand:
    """One typed queue-gate command with a locally derived payload digest."""

    gate: str
    request_id: str
    expected_revision: int
    payload_digest: str = field(init=False)

    def __post_init__(self) -> None:
        gate = _require_queue_gate(self.gate)
        request_id = _require_identifier(self.request_id, "request_id")
        expected_revision = _require_counter(
            self.expected_revision, "expected_revision"
        )
        object.__setattr__(self, "gate", gate)
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "expected_revision", expected_revision)
        object.__setattr__(
            self,
            "payload_digest",
            _canonical_payload_digest(
                {
                    "expected_revision": expected_revision,
                    "gate": gate,
                    "request_id": request_id,
                }
            ),
        )

    def to_record(self) -> dict[str, int | str]:
        """Return the exact queue-gate wire record without an injectable digest."""

        return {
            "op": "queue_gate",
            "gate": self.gate,
            "request_id": self.request_id,
            "expected_revision": self.expected_revision,
        }

    @classmethod
    def from_record(cls, record: object) -> "QueueGateCommand":
        if type(record) is not dict or set(record) != {
            "op",
            "gate",
            "request_id",
            "expected_revision",
        }:
            raise ValueError("queue-gate request is invalid")
        if record["op"] != "queue_gate":
            raise ValueError("queue-gate request is invalid")
        return cls(
            gate=record["gate"],
            request_id=record["request_id"],
            expected_revision=record["expected_revision"],
        )


@dataclass(frozen=True, slots=True)
class QueueGateResult:
    """The fixed public response for a durable queue-gate command."""

    applied: bool
    queue_gate: str
    revision: int

    def __post_init__(self) -> None:
        if type(self.applied) is not bool:
            raise TypeError("applied must be a boolean")
        _require_queue_gate(self.queue_gate)
        _require_counter(self.revision, "revision")

    def to_record(self) -> dict[str, bool | int | str]:
        return {
            "applied": self.applied,
            "queue_gate": self.queue_gate,
            "revision": self.revision,
        }

    @classmethod
    def from_record(cls, record: object) -> "QueueGateResult":
        if type(record) is not dict or set(record) != {
            "applied",
            "queue_gate",
            "revision",
        }:
            raise IPCError("ipc_response_invalid")
        try:
            return cls(
                applied=record["applied"],
                queue_gate=record["queue_gate"],
                revision=record["revision"],
            )
        except (TypeError, ValueError):
            raise IPCError("ipc_response_invalid") from None


@dataclass(frozen=True, slots=True)
class JobControlCommand:
    """One typed, revision-fenced materialized-job control command."""

    job: str
    action: str
    request_id: str
    expected_revision: int
    payload_digest: str = field(init=False)

    def __post_init__(self) -> None:
        job = _require_identifier(self.job, "job")
        action = _require_job_control_action(self.action)
        request_id = _require_identifier(self.request_id, "request_id")
        expected_revision = _require_counter(
            self.expected_revision, "expected_revision"
        )
        object.__setattr__(self, "job", job)
        object.__setattr__(self, "action", action)
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "expected_revision", expected_revision)
        object.__setattr__(
            self,
            "payload_digest",
            _canonical_payload_digest(
                {
                    "action": action,
                    "expected_revision": expected_revision,
                    "job": job,
                    "request_id": request_id,
                }
            ),
        )

    def to_record(self) -> dict[str, int | str]:
        """Return the exact job-control wire record without an injectable digest."""

        return {
            "op": "job_control",
            "job": self.job,
            "action": self.action,
            "request_id": self.request_id,
            "expected_revision": self.expected_revision,
        }

    @classmethod
    def from_record(cls, record: object) -> "JobControlCommand":
        if type(record) is not dict or set(record) != {
            "op",
            "job",
            "action",
            "request_id",
            "expected_revision",
        }:
            raise ValueError("job-control request is invalid")
        if record["op"] != "job_control":
            raise ValueError("job-control request is invalid")
        return cls(
            job=record["job"],
            action=record["action"],
            request_id=record["request_id"],
            expected_revision=record["expected_revision"],
        )


@dataclass(frozen=True, slots=True)
class JobControlResult:
    """The fixed public readback for one materialized-job control command."""

    status: str
    job: str
    generation: int
    revision: int
    state: str
    authorized: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", _require_job_control_status(self.status))
        object.__setattr__(self, "job", _require_identifier(self.job, "job"))
        object.__setattr__(
            self, "generation", _require_counter(self.generation, "generation")
        )
        object.__setattr__(self, "revision", _require_counter(self.revision, "revision"))
        object.__setattr__(self, "state", _require_public_job_state(self.state))
        if type(self.authorized) is not bool:
            raise TypeError("authorized must be a boolean")

    def to_record(self) -> dict[str, bool | int | str]:
        return {
            "status": self.status,
            "job": self.job,
            "generation": self.generation,
            "revision": self.revision,
            "state": self.state,
            "authorized": self.authorized,
        }

    @classmethod
    def from_record(cls, record: object) -> "JobControlResult":
        if type(record) is not dict or set(record) != {
            "status",
            "job",
            "generation",
            "revision",
            "state",
            "authorized",
        }:
            raise IPCError("ipc_response_invalid")
        try:
            return cls(
                status=record["status"],
                job=record["job"],
                generation=record["generation"],
                revision=record["revision"],
                state=record["state"],
                authorized=record["authorized"],
            )
        except (TypeError, ValueError):
            raise IPCError("ipc_response_invalid") from None


@dataclass(frozen=True, slots=True)
class PublicJobRecord:
    """The redacted, immutable job projection exposed by jobs-page IPC."""

    job: str
    generation: int
    revision: int
    state: str

    def __post_init__(self) -> None:
        _require_identifier(self.job, "job")
        _require_counter(self.generation, "generation")
        _require_counter(self.revision, "revision")
        _require_public_job_state(self.state)

    def to_record(self) -> dict[str, int | str]:
        return {
            "job": self.job,
            "generation": self.generation,
            "revision": self.revision,
            "state": self.state,
        }

    @classmethod
    def from_record(cls, record: object) -> "PublicJobRecord":
        if type(record) is not dict or set(record) != {
            "job",
            "generation",
            "revision",
            "state",
        }:
            raise IPCError("ipc_response_invalid")
        try:
            return cls(
                job=record["job"],
                generation=record["generation"],
                revision=record["revision"],
                state=record["state"],
            )
        except (TypeError, ValueError):
            raise IPCError("ipc_response_invalid") from None


@dataclass(frozen=True, slots=True)
class JobsPage:
    """One fixed-size-or-smaller public jobs page with a stable cursor."""

    jobs: tuple[PublicJobRecord, ...]
    next_cursor: str | None

    def __post_init__(self) -> None:
        if type(self.jobs) is not tuple or len(self.jobs) > _MAX_JOBS_PAGE_RECORDS:
            raise ValueError("jobs page is invalid")
        if any(type(job) is not PublicJobRecord for job in self.jobs):
            raise TypeError("jobs page contains an invalid job")
        if self.next_cursor is not None:
            _require_identifier(self.next_cursor, "next_cursor")
            if (
                len(self.jobs) != _MAX_JOBS_PAGE_RECORDS
                or self.jobs[-1].job != self.next_cursor
            ):
                raise ValueError("jobs page cursor is invalid")

    def to_record(self) -> dict[str, list[dict[str, int | str]] | str | None]:
        return {
            "jobs": [job.to_record() for job in self.jobs],
            "next_cursor": self.next_cursor,
        }

    @classmethod
    def from_record(cls, record: object) -> "JobsPage":
        if type(record) is not dict or set(record) != {"jobs", "next_cursor"}:
            raise IPCError("ipc_response_invalid")
        jobs = record["jobs"]
        if type(jobs) is not list:
            raise IPCError("ipc_response_invalid")
        try:
            return cls(
                jobs=tuple(PublicJobRecord.from_record(job) for job in jobs),
                next_cursor=record["next_cursor"],
            )
        except (IPCError, RecursionError, TypeError, ValueError):
            raise IPCError("ipc_response_invalid") from None


def _require_socket_path(value: object) -> Path:
    if not isinstance(value, Path) or not value.is_absolute():
        raise IPCStateError("ipc_socket_invalid")
    try:
        encoded = os.fsencode(value)
    except (TypeError, ValueError):
        raise IPCStateError("ipc_socket_invalid") from None
    if not encoded or len(encoded) > _MAX_DARWIN_UNIX_SOCKET_PATH_BYTES:
        raise IPCStateError("ipc_socket_invalid")
    return value


def _require_absent_socket_path(path: Path) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    except OSError:
        raise IPCStateError("ipc_socket_invalid") from None
    raise IPCStateError("ipc_socket_invalid")


def validate_available_socket_path(socket_path: Path) -> Path:
    """Reject an unsafe or pre-existing endpoint before worker bootstrap writes."""

    path = _require_socket_path(socket_path)
    _require_absent_socket_path(path)
    return path


def _encoded_record(
    record: Mapping[str, object], *, maximum_bytes: int = MAX_MESSAGE_BYTES
) -> bytes:
    payload = json.dumps(record, separators=(",", ":"), sort_keys=True).encode("utf-8") + b"\n"
    if len(payload) > maximum_bytes:
        raise IPCStateError("ipc_response_invalid")
    return payload


def _read_line(
    connection: socket.socket, *, maximum_bytes: int = MAX_MESSAGE_BYTES, initial: bytes = b''
) -> bytes | None:
    received = bytearray(initial)
    while True:
        try:
            chunk = connection.recv(maximum_bytes + 1 - len(received))
        except (OSError, TimeoutError):
            return None
        if not chunk:
            if not received.endswith(b"\n") or received.count(b"\n") != 1:
                return None
            return bytes(received[:-1])
        received.extend(chunk)
        if len(received) > maximum_bytes:
            return None


def _decode_request(payload: bytes | None) -> object | None:
    if payload is None:
        return None
    try:
        return json.loads(
            payload.decode("utf-8"), object_pairs_hook=_reject_duplicate_object_keys
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError, RecursionError):
        return None


def _is_health_request(request: object) -> bool:
    return type(request) is dict and request == {"op": "health"}


def _reject_duplicate_object_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    record: dict[str, object] = {}
    for key, value in pairs:
        if key in record:
            raise ValueError("JSON object contains duplicate keys")
        record[key] = value
    return record


def _decode_jobs_page_request(request: object) -> tuple[bool, str | None]:
    if type(request) is not dict or request.get("op") != "jobs_page":
        return False, None
    if set(request) not in ({"op"}, {"op", "cursor"}):
        return False, None
    cursor = request.get("cursor")
    if cursor is None:
        return True, None
    try:
        return True, _require_identifier(cursor, "cursor")
    except (TypeError, ValueError):
        return False, None


def _decode_queue_gate_request(request: object) -> QueueGateCommand | None:
    try:
        return QueueGateCommand.from_record(request)
    except (TypeError, ValueError, RecursionError):
        return None


def _decode_job_control_request(request: object) -> JobControlCommand | None:
    try:
        return JobControlCommand.from_record(request)
    except (TypeError, ValueError, RecursionError):
        return None


def _decode_job_add_request(request: object) -> JobAddCommand | None:
    try:
        return JobAddCommand.from_record(request)
    except (TypeError, ValueError, RecursionError):
        return None


def _decode_direct_engine_activate_request(
    request: object,
) -> DirectEngineActivateCommand | None:
    try:
        return DirectEngineActivateCommand.from_record(request)
    except (TypeError, ValueError, RecursionError):
        return None


def _decode_direct_job_dispatch_request(
    request: object,
) -> DirectJobDispatchCommand | None:
    try:
        return DirectJobDispatchCommand.from_record(request)
    except (TypeError, ValueError, RecursionError):
        return None


class HealthServer:
    """A one-request AF_UNIX listener owned and serviced by the worker thread."""

    def __init__(
        self,
        socket_path: Path,
        *,
        health: Callable[[], WorkerHealth],
        jobs_page: Callable[[str | None], JobsPage] | None = None,
        queue_gate: Callable[[QueueGateCommand], QueueGateResult] | None = None,
        job_control: Callable[[JobControlCommand], JobControlResult] | None = None,
        job_add: Callable[[JobAddCommand], JobAddResult] | None = None,
        add_batch: Callable[[AddBatchCommand], AddBatchResult] | None = None,
        target_authorize: Callable[[TargetAuthorizeCommand], TargetAuthorizeResult] | None = None,
        direct_engine_activate: Callable[
            [DirectEngineActivateCommand], DirectEngineActivateResult
        ]
        | None = None,
        direct_job_dispatch: Callable[
            [DirectJobDispatchCommand], DirectJobDispatchResult
        ]
        | None = None,
    ) -> None:
        self.socket_path = validate_available_socket_path(socket_path)
        if not callable(health):
            raise TypeError("health must be callable")
        if jobs_page is not None and not callable(jobs_page):
            raise TypeError("jobs_page must be callable")
        if queue_gate is not None and not callable(queue_gate):
            raise TypeError("queue_gate must be callable")
        if job_control is not None and not callable(job_control):
            raise TypeError("job_control must be callable")
        if job_add is not None and not callable(job_add):
            raise TypeError("job_add must be callable")
        if add_batch is not None and not callable(add_batch):
            raise TypeError('add_batch must be callable')
        if target_authorize is not None and not callable(target_authorize):
            raise TypeError('target_authorize must be callable')
        if direct_engine_activate is not None and not callable(direct_engine_activate):
            raise TypeError("direct_engine_activate must be callable")
        if direct_job_dispatch is not None and not callable(direct_job_dispatch):
            raise TypeError("direct_job_dispatch must be callable")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        socket_identity: tuple[int, int] | None = None
        try:
            listener.bind(str(self.socket_path))
            os.chmod(self.socket_path, _SOCKET_MODE)
            socket_stat = self.socket_path.lstat()
            if (
                not stat.S_ISSOCK(socket_stat.st_mode)
                or socket_stat.st_uid != os.geteuid()
                or stat.S_IMODE(socket_stat.st_mode) != _SOCKET_MODE
            ):
                raise IPCStateError("ipc_socket_invalid")
            socket_identity = (socket_stat.st_dev, socket_stat.st_ino)
            listener.listen(_SOCKET_BACKLOG)
            listener.settimeout(_CONNECTION_TIMEOUT_SECONDS)
        except BaseException:
            listener.close()
            _unlink_owned_socket(self.socket_path, socket_identity)
            raise
        self._health = health
        self._jobs_page = jobs_page
        self._queue_gate = queue_gate
        self._job_control = job_control
        self._job_add = job_add
        self._add_batch = add_batch
        self._target_authorize = target_authorize
        self._direct_engine_activate = direct_engine_activate
        self._direct_job_dispatch = direct_job_dispatch
        self._listener = listener
        self._identity = socket_identity

    def serve_once(self) -> None:
        """Service one bounded request without running client work on another thread."""

        try:
            connection, _address = self._listener.accept()
        except TimeoutError:
            return
        except OSError as error:
            raise IPCStateError("ipc_socket_invalid") from error
        with connection:
            deadline = time.monotonic() + 2.0
            connection.settimeout(_CONNECTION_TIMEOUT_SECONDS)
            try:
                first = connection.recv(1)
            except (OSError, TimeoutError):
                return
            if first == b'H':
                try:
                    preamble = b'H' + _batch_read_exact(connection, 4, deadline)
                except (OSError, TimeoutError, ValueError):
                    preamble = b''
                if preamble == _TARGET_PREAMBLE:
                    self._serve_target_authorize(connection, deadline)
                else:
                    self._serve_add_batch(connection, deadline, preamble)
                return
            payload = _read_line(connection, initial=first)
            request = _decode_request(payload)
            try:
                if _is_health_request(request):
                    response = _encoded_record(self._health().to_record())
                else:
                    jobs_page_request, cursor = _decode_jobs_page_request(request)
                    if jobs_page_request and self._jobs_page is not None:
                        page = self._jobs_page(cursor)
                        if type(page) is not JobsPage:
                            raise ValueError("jobs_page result is invalid")
                        response = _encoded_record(
                            page.to_record(), maximum_bytes=_MAX_RESPONSE_BYTES
                        )
                    else:
                        job_add_command = _decode_job_add_request(request)
                        if job_add_command is not None:
                            if self._job_add is None:
                                response = _encoded_record(_INVALID_REQUEST)
                            else:
                                try:
                                    result = self._job_add(job_add_command)
                                    if type(result) is not JobAddResult:
                                        raise TypeError("job_add result is invalid")
                                    response = _encoded_record(result.to_record())
                                except IPCError as error:
                                    if str(error) == "invalid_request":
                                        response = _encoded_record(_INVALID_REQUEST)
                                    else:
                                        response = _encoded_record(_COMMAND_CONFLICT)
                                except Exception:
                                    response = _encoded_record(_COMMAND_CONFLICT)
                        else:
                            direct_engine_command = _decode_direct_engine_activate_request(
                                request
                            )
                            if direct_engine_command is not None:
                                if self._direct_engine_activate is None:
                                    response = _encoded_record(_INVALID_REQUEST)
                                else:
                                    try:
                                        result = self._direct_engine_activate(
                                            direct_engine_command
                                        )
                                        if type(result) is not DirectEngineActivateResult:
                                            raise TypeError(
                                                "direct_engine_activate result is invalid"
                                            )
                                        response = _encoded_record(result.to_record())
                                    except Exception:
                                        response = _encoded_record(_COMMAND_CONFLICT)
                            else:
                                direct_job_command = _decode_direct_job_dispatch_request(
                                    request
                                )
                                if direct_job_command is not None:
                                    if self._direct_job_dispatch is None:
                                        response = _encoded_record(_INVALID_REQUEST)
                                    else:
                                        try:
                                            result = self._direct_job_dispatch(
                                                direct_job_command
                                            )
                                            if type(result) is not DirectJobDispatchResult:
                                                raise TypeError(
                                                    "direct_job_dispatch result is invalid"
                                                )
                                            response = _encoded_record(result.to_record())
                                        except IPCError as error:
                                            if str(error) == "invalid_request":
                                                response = _encoded_record(_INVALID_REQUEST)
                                            else:
                                                response = _encoded_record(_COMMAND_CONFLICT)
                                        except Exception:
                                            response = _encoded_record(_COMMAND_CONFLICT)
                                else:
                                    job_control_command = _decode_job_control_request(request)
                                    if job_control_command is not None:
                                        if self._job_control is None:
                                            response = _encoded_record(_INVALID_REQUEST)
                                        else:
                                            try:
                                                result = self._job_control(job_control_command)
                                                if type(result) is not JobControlResult:
                                                    raise TypeError(
                                                        "job_control result is invalid"
                                                    )
                                                response = _encoded_record(result.to_record())
                                            except IPCError as error:
                                                if str(error) == "invalid_request":
                                                    response = _encoded_record(_INVALID_REQUEST)
                                                else:
                                                    response = _encoded_record(_COMMAND_CONFLICT)
                                            except Exception:
                                                response = _encoded_record(_COMMAND_CONFLICT)
                                    else:
                                        command = _decode_queue_gate_request(request)
                                        if command is None or self._queue_gate is None:
                                            response = _encoded_record(_INVALID_REQUEST)
                                        else:
                                            try:
                                                response = _encoded_record(
                                                    self._queue_gate(command).to_record()
                                                )
                                            except Exception:
                                                response = _encoded_record(_COMMAND_CONFLICT)
            except (IPCStateError, TypeError, ValueError):
                response = _encoded_record(_INVALID_REQUEST)
            try:
                connection.sendall(response)
            except (OSError, TimeoutError):
                return

    def _serve_add_batch(self, connection, deadline, preamble):
        try:
            if preamble != _BATCH_PREAMBLE:
                raise ValueError('invalid batch preamble')
            command = AddBatchCommand.from_record(_batch_decode(
                _batch_read_body(connection, deadline, _MAX_BATCH_BODY)))
        except (OSError, TimeoutError, TypeError, ValueError, RecursionError):
            record = _INVALID_REQUEST
        else:
            if self._add_batch is None:
                record = _INVALID_REQUEST
            else:
                try:
                    result = self._add_batch(command)
                    if type(result) is not AddBatchResult: raise ValueError('invalid batch callback')
                    record = result.to_record()
                except IPCError as error:
                    record = _COMMAND_CONFLICT if str(error) == 'command_conflict' else {'error': 'batch_state_invalid'}
                except Exception:
                    record = {'error': 'batch_state_invalid'}
        try:
            payload = _batch_canonical(record, _MAX_BATCH_REPLY)
        except (TypeError, ValueError):
            payload = b'{"error":"batch_state_invalid"}'
        try:
            connection.settimeout(0.2)
            connection.sendall(_batch_frame(payload))
        except (OSError, TimeoutError):
            return

    def _serve_target_authorize(self, connection, deadline):
        try:
            command = TargetAuthorizeCommand.from_record(_batch_decode(
                _batch_read_body(connection, deadline, _MAX_TARGET_BODY)))
        except (OSError, TimeoutError, TypeError, ValueError, RecursionError) as error:
            record = {'error': 'unsupported_selection_size' if str(error) == 'unsupported_selection_size' else 'target_request_invalid'}
        else:
            try:
                if self._target_authorize is None: raise IPCError('target_request_invalid')
                result = self._target_authorize(command)
                if type(result) is not TargetAuthorizeResult: raise ValueError('invalid target callback')
                record = result.to_record()
            except IPCError as error:
                record = {'error': str(error) if str(error) in _TARGET_ERRORS else 'target_authority_corrupt'}
            except Exception:
                record = {'error': 'target_authority_corrupt'}
        try:
            payload = _batch_canonical(record, _MAX_TARGET_REPLY)
            connection.settimeout(0.2)
            connection.sendall(_TARGET_PREAMBLE + len(payload).to_bytes(4, 'big') + payload)
        except (OSError, TimeoutError, TypeError, ValueError):
            return

    def close(self) -> None:
        """Close the listener and remove only the exact socket this server created."""

        try:
            self._listener.close()
        finally:
            _unlink_owned_socket(self.socket_path, self._identity)


def _unlink_owned_socket(path: Path, identity: tuple[int, int] | None) -> None:
    if identity is None:
        return
    try:
        socket_stat = path.lstat()
    except FileNotFoundError:
        return
    except OSError:
        return
    if not stat.S_ISSOCK(socket_stat.st_mode):
        return
    if (socket_stat.st_dev, socket_stat.st_ino) != identity:
        return
    try:
        path.unlink()
    except OSError:
        return


def request_health(socket_path: Path) -> WorkerHealth:
    """Request the fixed health projection without opening worker SQLite state."""

    path = _require_socket_path(socket_path)
    request = _encoded_record({"op": "health"})
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(_CLIENT_TIMEOUT_SECONDS)
        try:
            client.connect(str(path))
            client.sendall(request)
            client.shutdown(socket.SHUT_WR)
            response = _read_line(client)
        except (OSError, TimeoutError):
            raise IPCError("ipc_unavailable") from None
    if response is None:
        raise IPCError("ipc_response_invalid")
    try:
        record = json.loads(response.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise IPCError("ipc_response_invalid") from None
    return WorkerHealth.from_record(record)


def request_jobs_page(socket_path: Path, *, cursor: str | None = None) -> JobsPage:
    """Read one redacted, bounded job page through the worker-owned socket."""

    path = _require_socket_path(socket_path)
    if cursor is not None:
        cursor = _require_identifier(cursor, "cursor")
    request = _encoded_record({"op": "jobs_page", "cursor": cursor})
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(_CLIENT_TIMEOUT_SECONDS)
        try:
            client.connect(str(path))
            client.sendall(request)
            client.shutdown(socket.SHUT_WR)
            response = _read_line(client, maximum_bytes=_MAX_RESPONSE_BYTES)
        except (OSError, TimeoutError):
            raise IPCError("ipc_unavailable") from None
    if response is None:
        raise IPCError("ipc_response_invalid")
    try:
        record = json.loads(
            response.decode("utf-8"), object_pairs_hook=_reject_duplicate_object_keys
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError, RecursionError):
        raise IPCError("ipc_response_invalid") from None
    return JobsPage.from_record(record)


def add_job(
    socket_path: Path,
    *,
    job: str,
    request_id: str,
    source_url: str,
    priority: int,
    order_key: int,
    category: str,
    partial_filename: str,
    selected_final_filename: str,
) -> JobAddResult:
    """Persist one typed, inactive direct job through the worker-owned connection."""

    path = _require_socket_path(socket_path)
    try:
        command = JobAddCommand(
            job=job,
            request_id=request_id,
            source_url=source_url,
            priority=priority,
            order_key=order_key,
            category=category,
            partial_filename=partial_filename,
            selected_final_filename=selected_final_filename,
        )
    except (RecursionError, TypeError, ValueError):
        raise IPCError("invalid_request") from None
    request = command._wire_request
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(_CLIENT_TIMEOUT_SECONDS)
        try:
            client.connect(str(path))
            client.sendall(request)
            client.shutdown(socket.SHUT_WR)
            response = _read_line(client)
        except (OSError, TimeoutError):
            raise IPCError("ipc_unavailable") from None
    if response is None:
        raise IPCError("ipc_response_invalid")
    try:
        record = json.loads(
            response.decode("utf-8"), object_pairs_hook=_reject_duplicate_object_keys
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError, RecursionError):
        raise IPCError("ipc_response_invalid") from None
    if record == _COMMAND_CONFLICT:
        raise IPCError("command_conflict")
    if record == _INVALID_REQUEST:
        raise IPCError("invalid_request")
    return JobAddResult.from_record(record)


def set_queue_gate(
    socket_path: Path,
    *,
    gate: str,
    request_id: str,
    expected_revision: int,
) -> QueueGateResult:
    """Apply one typed queue-gate command through the worker-owned connection."""

    path = _require_socket_path(socket_path)
    command = QueueGateCommand(
        gate=gate,
        request_id=request_id,
        expected_revision=expected_revision,
    )
    request = _encoded_record(command.to_record())
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(_CLIENT_TIMEOUT_SECONDS)
        try:
            client.connect(str(path))
            client.sendall(request)
            client.shutdown(socket.SHUT_WR)
            response = _read_line(client)
        except (OSError, TimeoutError):
            raise IPCError("ipc_unavailable") from None
    if response is None:
        raise IPCError("ipc_response_invalid")
    try:
        record = json.loads(response.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise IPCError("ipc_response_invalid") from None
    if record == _COMMAND_CONFLICT:
        raise IPCError("command_conflict")
    return QueueGateResult.from_record(record)


def control_job(
    socket_path: Path,
    *,
    job: str,
    action: str,
    request_id: str,
    expected_revision: int,
) -> JobControlResult:
    """Apply one typed materialized-job control command through the worker."""

    path = _require_socket_path(socket_path)
    command = JobControlCommand(
        job=job,
        action=action,
        request_id=request_id,
        expected_revision=expected_revision,
    )
    request = _encoded_record(command.to_record())
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(_CLIENT_TIMEOUT_SECONDS)
        try:
            client.connect(str(path))
            client.sendall(request)
            client.shutdown(socket.SHUT_WR)
            response = _read_line(client)
        except (OSError, TimeoutError):
            raise IPCError("ipc_unavailable") from None
    if response is None:
        raise IPCError("ipc_response_invalid")
    try:
        record = json.loads(
            response.decode("utf-8"), object_pairs_hook=_reject_duplicate_object_keys
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError, RecursionError):
        raise IPCError("ipc_response_invalid") from None
    if record == _COMMAND_CONFLICT:
        raise IPCError("command_conflict")
    if record == _INVALID_REQUEST:
        raise IPCError("invalid_request")
    return JobControlResult.from_record(record)


def dispatch_direct_job(
    socket_path: Path,
    *,
    job: str,
    expected_worker_epoch: int,
    expected_generation: int,
    expected_revision: int,
    request_id: str,
) -> DirectJobDispatchResult:
    """Ask the worker to start one exact admitted direct body transfer."""

    path = _require_socket_path(socket_path)
    try:
        command = DirectJobDispatchCommand(
            job=job,
            expected_worker_epoch=expected_worker_epoch,
            expected_generation=expected_generation,
            expected_revision=expected_revision,
            request_id=request_id,
        )
    except (RecursionError, TypeError, ValueError):
        raise IPCError("invalid_request") from None
    request = _encoded_record(command.to_record())
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(_CLIENT_TIMEOUT_SECONDS)
        try:
            client.connect(str(path))
            client.sendall(request)
            client.shutdown(socket.SHUT_WR)
            response = _read_line(client)
        except (OSError, TimeoutError):
            raise IPCError("ipc_unavailable") from None
    if response is None:
        raise IPCError("ipc_response_invalid")
    try:
        record = json.loads(
            response.decode("utf-8"), object_pairs_hook=_reject_duplicate_object_keys
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError, RecursionError):
        raise IPCError("ipc_response_invalid") from None
    if record == _COMMAND_CONFLICT:
        raise IPCError("command_conflict")
    if record == _INVALID_REQUEST:
        raise IPCError("invalid_request")
    return DirectJobDispatchResult.from_record(record)


def activate_direct_engine(
    socket_path: Path, *, expected_worker_epoch: int
) -> DirectEngineActivateResult:
    """Request one worker-epoch-fenced direct-engine activation."""

    path = _require_socket_path(socket_path)
    command = DirectEngineActivateCommand(expected_worker_epoch=expected_worker_epoch)
    request = _encoded_record(command.to_record())
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(_CLIENT_TIMEOUT_SECONDS)
        try:
            client.connect(str(path))
            client.sendall(request)
            client.shutdown(socket.SHUT_WR)
            response = _read_line(client)
        except (OSError, TimeoutError):
            raise IPCError("ipc_unavailable") from None
    if response is None:
        raise IPCError("ipc_response_invalid")
    try:
        record = json.loads(
            response.decode("utf-8"), object_pairs_hook=_reject_duplicate_object_keys
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError, RecursionError):
        raise IPCError("ipc_response_invalid") from None
    if record == _COMMAND_CONFLICT:
        raise IPCError("command_conflict")
    if record == _INVALID_REQUEST:
        raise IPCError("invalid_request")
    return DirectEngineActivateResult.from_record(record)
