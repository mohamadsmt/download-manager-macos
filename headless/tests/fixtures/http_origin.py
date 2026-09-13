"""Synthetic loopback origins used only by headless integration tests.

``LocalHttpOrigin`` remains a literal policy-test grant. ``SyntheticHttpOrigin``
adds a real, stdlib-only loopback listener for deterministic HTTP fault tests.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time
from urllib.parse import urlsplit


SYNTHETIC_ORIGIN_NOTICE = (
    "Synthetic loopback HTTP responses are not internet or benchmark evidence."
)
LEDGER_CAPACITY = 16
_PAYLOAD_BYTES = 1024
_LOOPBACK_HOST = "127.0.0.1"
_HANDLER_IDLE_TIMEOUT_SECONDS = 0.1


def _generated_payload(label: str) -> bytes:
    payload = bytearray()
    index = 0
    while len(payload) < _PAYLOAD_BYTES:
        payload.extend(
            hashlib.sha256(label.encode("ascii") + index.to_bytes(4, "big")).digest()
        )
        index += 1
    return bytes(payload[:_PAYLOAD_BYTES])


@dataclass(frozen=True, slots=True)
class LocalHttpOrigin:
    """A deterministic literal loopback origin for an explicit test grant."""

    host: str = "127.0.0.1"
    port: int = 18080

    @property
    def origin(self) -> str:
        return f"http://{self.host}:{self.port}"

    def url(self, path: str = "/fixture") -> str:
        if not path.startswith("/"):
            raise ValueError("fixture path must start with a slash")
        return f"{self.origin}{path}"


@dataclass(frozen=True, slots=True)
class RequestLedgerEntry:
    endpoint: str
    body_bytes: int
    chunk_count: int


@dataclass(frozen=True, slots=True)
class RequestLedger:
    request_count: int
    response_body_bytes: int
    connection_count: int
    dropped_entries: int
    entries: tuple[RequestLedgerEntry, ...]


class _LoopbackHttpServer(ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True


class _SyntheticHttpOriginMeta(type):
    def __new__(
        metaclass: type[_SyntheticHttpOriginMeta],
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, object],
        **kwargs: object,
    ) -> _SyntheticHttpOriginMeta:
        if any(isinstance(base, _SyntheticHttpOriginMeta) for base in bases):
            raise TypeError("SyntheticHttpOrigin does not support subclassing")
        return super().__new__(metaclass, name, bases, namespace, **kwargs)


class SyntheticHttpOrigin(metaclass=_SyntheticHttpOriginMeta):
    """A context-managed local origin with deterministic download faults."""

    @property
    def host(self) -> str:
        return _LOOPBACK_HOST

    payload = _generated_payload("synthetic-http-origin-v1")
    changed_payload = _generated_payload("synthetic-http-origin-v2")
    chunk_delay_seconds = 0.01
    delayed_chunk_count = 3
    disconnect_after = 256
    service_unavailable_failures = 2

    def __init__(self) -> None:
        self.port = 0
        self._lock = threading.Lock()
        self._entries: deque[RequestLedgerEntry] = deque(maxlen=LEDGER_CAPACITY)
        self._request_count = 0
        self._response_body_bytes = 0
        self._connection_count = 0
        self._dropped_entries = 0
        self._etag_change_requests = 0
        self._service_unavailable_requests = 0
        self._server: _LoopbackHttpServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def origin(self) -> str:
        return f"http://{_LOOPBACK_HOST}:{self.port}"

    def url(self, path: str = "/range") -> str:
        if not path.startswith("/"):
            raise ValueError("fixture path must start with a slash")
        return f"{self.origin}{path}"

    @property
    def ledger(self) -> RequestLedger:
        with self._lock:
            return RequestLedger(
                request_count=self._request_count,
                response_body_bytes=self._response_body_bytes,
                connection_count=self._connection_count,
                dropped_entries=self._dropped_entries,
                entries=tuple(self._entries),
            )

    def __enter__(self) -> SyntheticHttpOrigin:
        if self._server is not None:
            raise RuntimeError("synthetic origin is already running")
        server = _LoopbackHttpServer((_LOOPBACK_HOST, 0), self._handler_type())
        thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            name="synthetic-http-origin",
            daemon=True,
        )
        self._server = server
        self._thread = thread
        self.port = server.server_address[1]
        try:
            thread.start()
        except BaseException:
            self._server = None
            self._thread = None
            server.server_close()
            raise
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        server, thread = self._server, self._thread
        self._server = None
        self._thread = None
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join()

    def _handler_type(self) -> type[BaseHTTPRequestHandler]:
        origin = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def setup(self) -> None:
                self.request.settimeout(_HANDLER_IDLE_TIMEOUT_SECONDS)
                super().setup()
                origin._record_connection()

            def do_GET(self) -> None:
                origin._serve(self)

            def log_message(self, format: str, *_args: object) -> None:
                return

        return Handler

    def _serve(self, handler: BaseHTTPRequestHandler) -> None:
        endpoint = urlsplit(handler.path).path.removeprefix("/")
        if endpoint == "delayed":
            self._send_delayed(handler, endpoint)
            return
        if endpoint == "disconnect":
            self._send_disconnect(handler, endpoint)
            return

        status, headers, body = self._response_for(endpoint, handler.headers.get("Range"))
        self._send_response(handler, status, headers, body)
        self._record_request(endpoint, len(body), 0)

    def _response_for(
        self, endpoint: str, range_header: str | None
    ) -> tuple[int, dict[str, str], bytes]:
        if endpoint == "range":
            return self._range_response(range_header)
        if endpoint == "no-range":
            return 200, {}, self.payload
        if endpoint == "etag-change":
            return self._etag_change_response()
        if endpoint == "forbidden":
            return 403, {}, b""
        if endpoint == "missing":
            return 404, {}, b""
        if endpoint == "too-many-requests":
            return 429, {"Retry-After": "7"}, b""
        if endpoint == "service-unavailable":
            if self._next_service_unavailable():
                return 503, {}, b""
            return 200, {}, self.payload
        return 404, {}, b""

    def _range_response(self, range_header: str | None) -> tuple[int, dict[str, str], bytes]:
        headers = {"Accept-Ranges": "bytes"}
        if range_header is None:
            return 200, headers, self.payload
        if not range_header.startswith("bytes=") or "," in range_header:
            return 416, {**headers, "Content-Range": "bytes */1024"}, b""

        start_text, separator, end_text = range_header[6:].partition("-")
        if not separator:
            return 416, {**headers, "Content-Range": "bytes */1024"}, b""
        try:
            if start_text:
                start = int(start_text)
                end = int(end_text) if end_text else len(self.payload) - 1
            elif end_text:
                suffix_length = int(end_text)
                if suffix_length <= 0:
                    raise ValueError
                start = max(0, len(self.payload) - suffix_length)
                end = len(self.payload) - 1
            else:
                raise ValueError
        except ValueError:
            return 416, {**headers, "Content-Range": "bytes */1024"}, b""

        if start >= len(self.payload) or end < start:
            return 416, {**headers, "Content-Range": "bytes */1024"}, b""
        end = min(end, len(self.payload) - 1)
        body = self.payload[start : end + 1]
        return 206, {**headers, "Content-Range": f"bytes {start}-{end}/1024"}, body

    def _etag_change_response(self) -> tuple[int, dict[str, str], bytes]:
        with self._lock:
            changed = self._etag_change_requests > 0
            self._etag_change_requests += 1
        if changed:
            return 200, {"ETag": '"synthetic-v2"'}, self.changed_payload
        return 200, {"ETag": '"synthetic-v1"'}, self.payload

    def _next_service_unavailable(self) -> bool:
        with self._lock:
            if self._service_unavailable_requests >= self.service_unavailable_failures:
                return False
            self._service_unavailable_requests += 1
            return True

    def _send_response(
        self,
        handler: BaseHTTPRequestHandler,
        status: int,
        headers: dict[str, str],
        body: bytes,
        *,
        content_length: int | None = None,
    ) -> None:
        handler.send_response(status)
        for name, value in headers.items():
            handler.send_header(name, value)
        handler.send_header(
            "Content-Length", str(len(body) if content_length is None else content_length)
        )
        handler.end_headers()
        if body:
            handler.wfile.write(body)
            handler.wfile.flush()

    def _send_delayed(self, handler: BaseHTTPRequestHandler, endpoint: str) -> None:
        handler.send_response(200)
        handler.send_header("Content-Length", str(len(self.payload)))
        handler.end_headers()

        chunk_size = (len(self.payload) + self.delayed_chunk_count - 1) // self.delayed_chunk_count
        chunks = 0
        for start in range(0, len(self.payload), chunk_size):
            chunk = self.payload[start : start + chunk_size]
            handler.wfile.write(chunk)
            handler.wfile.flush()
            chunks += 1
            if start + chunk_size < len(self.payload):
                time.sleep(self.chunk_delay_seconds)
        self._record_request(endpoint, len(self.payload), chunks)

    def _send_disconnect(self, handler: BaseHTTPRequestHandler, endpoint: str) -> None:
        body = self.payload[: self.disconnect_after]
        handler.close_connection = True
        self._send_response(handler, 200, {}, body, content_length=len(self.payload))
        self._record_request(endpoint, len(body), 1)

    def _record_connection(self) -> None:
        with self._lock:
            self._connection_count += 1

    def _record_request(self, endpoint: str, body_bytes: int, chunk_count: int) -> None:
        with self._lock:
            self._request_count += 1
            self._response_body_bytes += body_bytes
            if len(self._entries) == LEDGER_CAPACITY:
                self._dropped_entries += 1
            self._entries.append(RequestLedgerEntry(endpoint, body_bytes, chunk_count))
