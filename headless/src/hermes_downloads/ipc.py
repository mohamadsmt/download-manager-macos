"""Bounded owner-worker AF_UNIX health protocol."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import socket
import stat
from typing import Callable, Final, Mapping

__all__ = [
    "HealthServer",
    "IPCError",
    "IPCStateError",
    "MAX_MESSAGE_BYTES",
    "WorkerHealth",
    "request_health",
    "validate_available_socket_path",
]

MAX_MESSAGE_BYTES: Final = 4096
_PROTOCOL_VERSION: Final = 1
_SOCKET_MODE: Final = 0o600
_SOCKET_BACKLOG: Final = 8
_CONNECTION_TIMEOUT_SECONDS: Final = 0.2
_CLIENT_TIMEOUT_SECONDS: Final = 5.0
_MAX_DARWIN_UNIX_SOCKET_PATH_BYTES: Final = 103
_INVALID_REQUEST: Final = {"error": "invalid_request"}


class IPCError(RuntimeError):
    """Raised for a bounded public IPC failure."""


class IPCStateError(ValueError):
    """Raised when an IPC endpoint cannot be safely owned."""


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


def _encoded_record(record: Mapping[str, object]) -> bytes:
    payload = json.dumps(record, separators=(",", ":"), sort_keys=True).encode("utf-8") + b"\n"
    if len(payload) > MAX_MESSAGE_BYTES:
        raise IPCStateError("ipc_response_invalid")
    return payload


def _read_line(connection: socket.socket) -> bytes | None:
    received = bytearray()
    while True:
        try:
            chunk = connection.recv(MAX_MESSAGE_BYTES + 1 - len(received))
        except (OSError, TimeoutError):
            return None
        if not chunk:
            if not received.endswith(b"\n") or received.count(b"\n") != 1:
                return None
            return bytes(received[:-1])
        received.extend(chunk)
        if len(received) > MAX_MESSAGE_BYTES:
            return None


def _decode_health_request(payload: bytes | None) -> bool:
    if payload is None:
        return False
    try:
        request = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    return type(request) is dict and request == {"op": "health"}


class HealthServer:
    """A one-request AF_UNIX listener owned and serviced by the worker thread."""

    def __init__(self, socket_path: Path, *, health: Callable[[], WorkerHealth]) -> None:
        self.socket_path = validate_available_socket_path(socket_path)
        if not callable(health):
            raise TypeError("health must be callable")
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
            try:
                if _decode_health_request(_read_line(connection)):
                    response = _encoded_record(self._health().to_record())
                else:
                    response = _encoded_record(_INVALID_REQUEST)
            except (IPCStateError, ValueError):
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
