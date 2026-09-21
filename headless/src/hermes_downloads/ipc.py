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
from typing import Callable, Final, Mapping

from hermes_downloads.models import JobState

__all__ = [
    "DirectEngineActivateCommand",
    "DirectEngineActivateResult",
    "HealthServer",
    "IPCError",
    "IPCStateError",
    "JobsPage",
    "MAX_MESSAGE_BYTES",
    "PublicJobRecord",
    "QueueGateCommand",
    "QueueGateResult",
    "WorkerHealth",
    "activate_direct_engine",
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
_QUEUE_GATES: Final = frozenset({"paused", "running"})
_DIRECT_ENGINE_ACTIVATE_STATUSES: Final = frozenset(
    {"active", "blocked", "stale_epoch"}
)


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


def _require_direct_engine_activate_status(value: object) -> str:
    if type(value) is not str:
        raise TypeError("status must be a string")
    if value not in _DIRECT_ENGINE_ACTIVATE_STATUSES:
        raise ValueError("status is not a direct-engine activation status")
    return value


def _canonical_payload_digest(record: Mapping[str, object]) -> str:
    payload = json.dumps(
        record, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


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
    connection: socket.socket, *, maximum_bytes: int = MAX_MESSAGE_BYTES
) -> bytes | None:
    received = bytearray()
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


def _decode_direct_engine_activate_request(
    request: object,
) -> DirectEngineActivateCommand | None:
    try:
        return DirectEngineActivateCommand.from_record(request)
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
        direct_engine_activate: Callable[
            [DirectEngineActivateCommand], DirectEngineActivateResult
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
        if direct_engine_activate is not None and not callable(direct_engine_activate):
            raise TypeError("direct_engine_activate must be callable")
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
        self._direct_engine_activate = direct_engine_activate
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
            connection.settimeout(_CONNECTION_TIMEOUT_SECONDS)
            payload = _read_line(connection)
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
