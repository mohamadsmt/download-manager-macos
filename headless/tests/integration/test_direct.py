"""Real HTTP checks for the synthetic loopback fixture, not internet evidence."""

from __future__ import annotations

import hashlib
import http.client
import importlib.util
from pathlib import Path
import socket
import sys
import threading
import time
from typing import Any

import pytest


_FIXTURE_SPEC = importlib.util.spec_from_file_location(
    "_direct_http_origin",
    Path(__file__).resolve().parents[1] / "fixtures" / "http_origin.py",
)
assert _FIXTURE_SPEC is not None and _FIXTURE_SPEC.loader is not None
_FIXTURE_MODULE = importlib.util.module_from_spec(_FIXTURE_SPEC)
sys.modules[_FIXTURE_SPEC.name] = _FIXTURE_MODULE
_FIXTURE_SPEC.loader.exec_module(_FIXTURE_MODULE)
LocalHttpOrigin = _FIXTURE_MODULE.LocalHttpOrigin

SYNTHETIC_PAYLOAD_SHA256 = "4384b97e775068d137e5e9bac903b59cf48ede2c981192b2cc4abb96024d0c29"
CHANGED_PAYLOAD_SHA256 = "28eb6fb0f2f24917bbd52f276e6efa30acc3debb89272f4cd27153339594aef9"


def _origin_type() -> type[Any]:
    origin_type = getattr(_FIXTURE_MODULE, "SyntheticHttpOrigin", None)
    assert origin_type is not None, "fixture must expose SyntheticHttpOrigin"
    return origin_type


def _request(
    origin: Any,
    path: str,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    connection = http.client.HTTPConnection(origin.host, origin.port, timeout=1)
    try:
        connection.request("GET", path, headers=headers or {})
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        connection.close()


def _ledger_after_requests(origin: Any, expected_request_count: int) -> Any:
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        ledger = origin.ledger
        if ledger.request_count >= expected_request_count:
            return ledger
        time.sleep(0.001)
    pytest.fail("fixture did not record the completed HTTP request")


def test_fixture_keeps_literal_network_policy_origin_and_is_synthetic() -> None:
    assert LocalHttpOrigin().origin == "http://127.0.0.1:18080"
    assert LocalHttpOrigin().url() == "http://127.0.0.1:18080/fixture"

    notice = getattr(_FIXTURE_MODULE, "SYNTHETIC_ORIGIN_NOTICE", "")
    normalized_notice = notice.lower()
    assert "synthetic" in normalized_notice
    assert "not" in normalized_notice
    assert "internet" in normalized_notice
    assert "benchmark" in normalized_notice

    with _origin_type()() as origin:
        assert origin.host == "127.0.0.1"
        assert origin.port != 0
        assert origin.origin == f"http://127.0.0.1:{origin.port}"


def test_fixture_ignores_an_unsafe_host_override_when_binding() -> None:
    origin_type = _origin_type()

    class UnsafeSyntheticHttpOrigin(origin_type):
        host = "0.0.0.0"

    with UnsafeSyntheticHttpOrigin() as origin:
        server = origin._server
        assert server is not None
        assert server.server_address[0] == "127.0.0.1"
        assert origin.origin == f"http://127.0.0.1:{origin.port}"


def test_fixture_closes_stalled_tcp_handler_when_context_exits() -> None:
    origin = _origin_type()()
    peer: socket.socket | None = None
    preexisting_threads = set(threading.enumerate())
    try:
        with origin:
            peer = socket.create_connection(("127.0.0.1", origin.port), timeout=1)
            deadline = time.monotonic() + 1
            while origin.ledger.connection_count < 1 and time.monotonic() < deadline:
                time.sleep(0.001)
            assert origin.ledger.connection_count == 1

            handler_threads = [
                thread
                for thread in threading.enumerate()
                if thread not in preexisting_threads
                and thread.name != "synthetic-http-origin"
            ]
            assert len(handler_threads) == 1
            handler_thread = handler_threads[0]
            assert handler_thread.is_alive()
            close_started = time.monotonic()

        assert time.monotonic() - close_started < 1
        assert not handler_thread.is_alive()
        assert peer.recv(1) == b""
    finally:
        if peer is not None:
            peer.close()


def test_fixture_serves_real_range_and_no_range_responses() -> None:
    with _origin_type()() as origin:
        status, headers, body = _request(
            origin, "/range", headers={"Range": "bytes=100-227"}
        )
        assert status == 206
        assert headers["Accept-Ranges"] == "bytes"
        assert headers["Content-Range"] == "bytes 100-227/1024"
        assert headers["Content-Length"] == "128"
        assert body == origin.payload[100:228]

        status, headers, body = _request(
            origin, "/no-range", headers={"Range": "bytes=100-227"}
        )
        assert status == 200
        assert "Accept-Ranges" not in headers
        assert "Content-Range" not in headers
        assert headers["Content-Length"] == "1024"
        assert hashlib.sha256(body).hexdigest() == SYNTHETIC_PAYLOAD_SHA256
        assert body == origin.payload


def test_fixture_changes_etag_at_the_same_payload_size() -> None:
    with _origin_type()() as origin:
        first_status, first_headers, first_body = _request(origin, "/etag-change")
        second_status, second_headers, second_body = _request(origin, "/etag-change")

        assert (first_status, second_status) == (200, 200)
        assert first_headers["Content-Length"] == second_headers["Content-Length"] == "1024"
        assert first_headers["ETag"] != second_headers["ETag"]
        assert hashlib.sha256(first_body).hexdigest() == SYNTHETIC_PAYLOAD_SHA256
        assert hashlib.sha256(second_body).hexdigest() == CHANGED_PAYLOAD_SHA256
        assert first_body != second_body


def test_fixture_delays_real_http_chunks() -> None:
    with _origin_type()() as origin:
        started = time.monotonic()
        status, headers, body = _request(origin, "/delayed")
        elapsed = time.monotonic() - started

        assert status == 200
        assert headers["Content-Length"] == "1024"
        assert hashlib.sha256(body).hexdigest() == SYNTHETIC_PAYLOAD_SHA256
        assert elapsed >= origin.chunk_delay_seconds * 2
        ledger = _ledger_after_requests(origin, 1)
        assert ledger.entries[-1].chunk_count == origin.delayed_chunk_count


def test_fixture_disconnects_after_a_real_partial_body() -> None:
    with _origin_type()() as origin:
        connection = http.client.HTTPConnection(origin.host, origin.port, timeout=1)
        try:
            connection.request("GET", "/disconnect")
            response = connection.getresponse()
            assert response.status == 200
            assert response.getheader("Content-Length") == "1024"
            with pytest.raises(http.client.IncompleteRead) as exc_info:
                response.read()
        finally:
            connection.close()

        assert exc_info.value.partial == origin.payload[: origin.disconnect_after]
        ledger = _ledger_after_requests(origin, 1)
        entry = ledger.entries[-1]
        assert entry.endpoint == "disconnect"
        assert entry.body_bytes == origin.disconnect_after


def test_fixture_serves_status_faults_and_a_finite_503_sequence() -> None:
    with _origin_type()() as origin:
        status, headers, body = _request(origin, "/forbidden")
        assert (status, headers["Content-Length"], body) == (403, "0", b"")

        status, headers, body = _request(origin, "/missing")
        assert (status, headers["Content-Length"], body) == (404, "0", b"")

        status, headers, body = _request(origin, "/too-many-requests")
        assert (status, headers["Retry-After"], headers["Content-Length"], body) == (
            429,
            "7",
            "0",
            b"",
        )

        statuses = []
        for _ in range(origin.service_unavailable_failures + 1):
            status, headers, body = _request(origin, "/service-unavailable")
            statuses.append(status)
        assert statuses == [503, 503, 200]
        assert headers["Content-Length"] == "1024"
        assert hashlib.sha256(body).hexdigest() == SYNTHETIC_PAYLOAD_SHA256


def test_fixture_keeps_request_byte_and_connection_evidence_bounded() -> None:
    with _origin_type()() as origin:
        for _ in range(_FIXTURE_MODULE.LEDGER_CAPACITY + 1):
            status, _, body = _request(origin, "/range")
            assert status == 200
            assert body == origin.payload

        ledger = _ledger_after_requests(
            origin, _FIXTURE_MODULE.LEDGER_CAPACITY + 1
        )
        assert ledger.request_count == _FIXTURE_MODULE.LEDGER_CAPACITY + 1
        assert ledger.response_body_bytes == len(origin.payload) * ledger.request_count
        assert ledger.connection_count == ledger.request_count
        assert len(ledger.entries) == _FIXTURE_MODULE.LEDGER_CAPACITY
        assert ledger.dropped_entries == 1
        assert all(entry.endpoint == "range" for entry in ledger.entries)
