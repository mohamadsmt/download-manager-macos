"""Real HTTP checks for the synthetic loopback fixture, not internet evidence."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import http.client
import importlib.util
import os
from pathlib import Path
import signal
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
NOW = datetime(2026, 9, 13, tzinfo=UTC)
_ARIA2C = Path("/opt/homebrew/bin/aria2c")


def _origin_type() -> type[Any]:
    origin_type = getattr(_FIXTURE_MODULE, "SyntheticHttpOrigin", None)
    assert origin_type is not None, "fixture must expose SyntheticHttpOrigin"
    return origin_type


def _direct_module() -> Any:
    spec = importlib.util.find_spec("hermes_downloads.direct")
    assert spec is not None, "hermes_downloads.direct must control aria2 direct transfers"
    return __import__("hermes_downloads.direct", fromlist=["DirectAria2Controller"])


def _queue_module() -> Any:
    spec = importlib.util.find_spec("hermes_downloads.queue")
    assert spec is not None
    return __import__("hermes_downloads.queue", fromlist=["DownloadQueue"])


def _network_module() -> Any:
    spec = importlib.util.find_spec("hermes_downloads.network")
    assert spec is not None
    return __import__("hermes_downloads.network", fromlist=["validate_source_url"])


def _paths_module() -> Any:
    spec = importlib.util.find_spec("hermes_downloads.paths")
    assert spec is not None
    return __import__("hermes_downloads.paths", fromlist=["resolve_destination"])


def _admitted_queue(job_id: str) -> Any:
    queue = _queue_module().DownloadQueue(queue_running=True)
    queue.enqueue(job_id)
    queue.authorize(job_id)
    return queue


def _source(origin: Any, path: str) -> Any:
    network = _network_module()
    return network.validate_source_url(
        origin.url(path),
        local_origin_grant=network.LocalOriginGrant.for_url(origin.url()),
    )


def _destination(job_id: str, filename: str) -> Any:
    paths = _paths_module()
    root = Path.home() / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    return paths.resolve_destination(
        root,
        category="Other",
        filename=filename,
        job_id=job_id,
    )


def _admission(queue: Any, job_id: str) -> Any:
    admission = queue.admission_for(job_id, now=NOW)
    assert admission is not None
    return admission


def _group_is_gone(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def _assert_group_gone(process_group_id: int) -> None:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if _group_is_gone(process_group_id):
            return
        time.sleep(0.01)
    pytest.fail("aria2 process group survived controller cleanup")


def _force_stop_group(process_group_id: int) -> None:
    try:
        os.killpg(process_group_id, signal.SIGKILL)
    except ProcessLookupError:
        return
    except PermissionError:
        return
    _assert_group_gone(process_group_id)


@contextmanager
def _running_direct_controller(direct: Any, tmp_path: Path):
    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
        max_concurrent_downloads=1,
        split=3,
        max_connection_per_server=3,
    )
    identities: list[Any] = []
    try:
        controller.start()
        identity = controller.engine_identity
        assert identity is not None
        identities.append(identity)
        yield controller, identities
    finally:
        active_identity = controller.engine_identity
        if active_identity is not None and active_identity not in identities:
            identities.append(active_identity)
        controller.close()
        try:
            for identity in identities:
                _assert_group_gone(identity.process_group_id)
        finally:
            for identity in identities:
                _force_stop_group(identity.process_group_id)


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


def test_fixture_keeps_literal_network_policy_origin_and_serves_real_loopback_request() -> None:
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

        status, _, body = _request(origin, "/range")
        assert (status, body) == (200, origin.payload)
        ledger = _ledger_after_requests(origin, 1)
        assert (ledger.request_count, ledger.connection_count) == (1, 1)


def test_fixture_rejects_direct_class_body_subclass() -> None:
    origin_type = _origin_type()

    with pytest.raises(TypeError):

        class UnsafeSyntheticHttpOrigin(origin_type):
            host = "192.0.2.1"

    assert "UnsafeSyntheticHttpOrigin" not in locals()


def test_fixture_rejects_subclass_before_descriptor_set_name_can_run() -> None:
    origin_type = _origin_type()

    class HostDescriptor:
        def __init__(self) -> None:
            self.set_name_calls: list[tuple[type[Any], str]] = []

        def __set_name__(self, owner: type[Any], name: str) -> None:
            self.set_name_calls.append((owner, name))

    descriptor = HostDescriptor()
    with pytest.raises(TypeError):

        class UnsafeSyntheticHttpOrigin(origin_type):
            host = descriptor

    assert descriptor.set_name_calls == []
    assert "UnsafeSyntheticHttpOrigin" not in locals()


def test_fixture_rejects_subclass_before_post_class_host_override() -> None:
    origin_type = _origin_type()
    post_class_override_reached = False

    with pytest.raises(TypeError):

        class UnsafeSyntheticHttpOrigin(origin_type):
            pass

        UnsafeSyntheticHttpOrigin.host = "192.0.2.1"
        post_class_override_reached = True

    assert post_class_override_reached is False
    assert "UnsafeSyntheticHttpOrigin" not in locals()


def test_fixture_rejects_intermediate_subclass_that_omits_super() -> None:
    origin_type = _origin_type()
    nested_subclass_attempt_reached = False

    with pytest.raises(TypeError):

        class IntermediateSyntheticHttpOrigin(origin_type):
            def __init_subclass__(cls, **kwargs: object) -> None:
                cls.host = "192.0.2.1"

        class UnsafeSyntheticHttpOrigin(IntermediateSyntheticHttpOrigin):
            pass

        nested_subclass_attempt_reached = True

    assert nested_subclass_attempt_reached is False
    assert "IntermediateSyntheticHttpOrigin" not in locals()
    assert "UnsafeSyntheticHttpOrigin" not in locals()


def test_fixture_rejects_slots_subclass_before_host_slot_can_exist() -> None:
    origin_type = _origin_type()

    with pytest.raises(TypeError):

        class UnsafeSyntheticHttpOrigin(origin_type):
            __slots__ = ("host",)

    assert "UnsafeSyntheticHttpOrigin" not in locals()


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


def test_direct_admission_adds_paused_then_explicit_resume_hash_verifies_payload(
    tmp_path: Path,
) -> None:
    direct = _direct_module()
    queue = _queue_module().DownloadQueue(queue_running=False)
    job_id = "direct-admission"
    queue.enqueue(job_id)

    with _origin_type()() as origin, _running_direct_controller(direct, tmp_path) as (
        controller,
        _,
    ):
        source = _source(origin, "/range")
        destination = _destination(job_id, "payload.bin")
        expected_sha256 = hashlib.sha256(origin.payload).hexdigest()

        with pytest.raises(direct.DirectAdmissionError):
            controller.add_paused(
                job_id=job_id,
                generation=4,
                source=source,
                destination=destination,
                expected_sha256=expected_sha256,
                admission=_admission(queue, job_id),
            )
        assert origin.ledger.response_body_bytes == 0

        queue.resume_all()
        with pytest.raises(direct.DirectAdmissionError):
            controller.add_paused(
                job_id=job_id,
                generation=4,
                source=source,
                destination=destination,
                expected_sha256=expected_sha256,
                admission=_admission(queue, job_id),
            )
        assert origin.ledger.response_body_bytes == 0

        queue.authorize(job_id)
        added = controller.add_paused(
            job_id=job_id,
            generation=4,
            source=source,
            destination=destination,
            expected_sha256=expected_sha256,
            admission=_admission(queue, job_id),
        )
        assert added.status == "paused"
        assert added.gid == controller.gid_for_job(job_id)
        assert controller.job_for_gid(added.gid) == job_id
        assert controller.readback(
            job_id=job_id, generation=4, gid=added.gid
        ).status == "paused"
        time.sleep(0.05)
        assert origin.ledger.response_body_bytes == 0

        queue.pause_queue()
        with pytest.raises(direct.DirectAdmissionError):
            controller.resume(job_id=job_id, generation=4, admission=_admission(queue, job_id))
        assert origin.ledger.response_body_bytes == 0

        queue.resume_all()
        resumed = controller.resume(
            job_id=job_id, generation=4, admission=_admission(queue, job_id)
        )
        assert resumed.gid == added.gid
        completed = controller.wait_for_terminal(job_id=job_id, generation=4, timeout=5)

        assert completed.status == "complete"
        assert completed.hash_verified is True
        assert completed.partial_path == destination.partial_path
        assert completed.partial_path.read_bytes() == origin.payload
        assert hashlib.sha256(completed.partial_path.read_bytes()).hexdigest() == expected_sha256
        assert not destination.final_path.exists()
        argv = controller.launch_argv
        assert "--no-netrc=true" in argv
        assert "--file-allocation=none" in argv
        assert "--check-certificate=true" in argv
        assert "--rpc-listen-all=false" in argv
        assert "--max-concurrent-downloads=1" in argv
        assert "--split=3" in argv
        assert "--max-connection-per-server=3" in argv
        assert any(argument.startswith("--conf-path=") for argument in argv)
        assert not any("rpc-secret" in argument for argument in argv)
        assert source.raw_url.decode("utf-8") not in argv
        assert all(not argument.startswith("--input-file=") for argument in argv)
        assert all(not argument.startswith("--save-session=") for argument in argv)
        assert controller.private_runtime_path.stat().st_mode & 0o777 == 0o700
        assert controller.private_config_path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    ("endpoint", "requires_range"),
    (("range", True), ("no-range", False)),
)
def test_direct_uses_real_segmented_range_or_no_range_fallback(
    tmp_path: Path, endpoint: str, requires_range: bool
) -> None:
    direct = _direct_module()
    job_id = f"direct-{endpoint}"
    queue = _admitted_queue(job_id)

    with _origin_type()(payload_size=3 * 1024 * 1024) as origin, _running_direct_controller(
        direct, tmp_path
    ) as (controller, _):
        destination = _destination(job_id, f"{endpoint}.bin")
        expected_sha256 = hashlib.sha256(origin.payload).hexdigest()
        added = controller.add_paused(
            job_id=job_id,
            generation=1,
            source=_source(origin, f"/{endpoint}"),
            destination=destination,
            expected_sha256=expected_sha256,
            admission=_admission(queue, job_id),
        )
        assert added.status == "paused"
        assert origin.ledger.response_body_bytes == 0

        controller.resume(job_id=job_id, generation=1, admission=_admission(queue, job_id))
        completed = controller.wait_for_terminal(job_id=job_id, generation=1, timeout=10)

        assert completed.status == "complete"
        assert completed.hash_verified is True
        assert completed.partial_path.read_bytes() == origin.payload
        assert hashlib.sha256(completed.partial_path.read_bytes()).hexdigest() == expected_sha256
        entries = [entry for entry in origin.ledger.entries if entry.endpoint == endpoint]
        assert entries
        if requires_range:
            assert sum(entry.range_header is not None for entry in entries) >= 2


def test_direct_rejects_same_length_payload_when_expected_hash_does_not_match(
    tmp_path: Path,
) -> None:
    direct = _direct_module()
    job_id = "direct-wrong-hash"
    queue = _admitted_queue(job_id)

    with _origin_type()() as origin, _running_direct_controller(direct, tmp_path) as (
        controller,
        _,
    ):
        destination = _destination(job_id, "wrong-hash.bin")
        actual_sha256 = hashlib.sha256(origin.payload).hexdigest()
        wrong_same_length_sha256 = hashlib.sha256(origin.changed_payload).hexdigest()
        assert len(origin.changed_payload) == len(origin.payload)
        assert wrong_same_length_sha256 != actual_sha256
        controller.add_paused(
            job_id=job_id,
            generation=1,
            source=_source(origin, "/range"),
            destination=destination,
            expected_sha256=wrong_same_length_sha256,
            admission=_admission(queue, job_id),
        )
        controller.resume(job_id=job_id, generation=1, admission=_admission(queue, job_id))

        with pytest.raises(direct.DirectTransferError):
            controller.wait_for_terminal(job_id=job_id, generation=1, timeout=5)


def test_direct_rejects_stale_generation_callback_before_readback(tmp_path: Path) -> None:
    direct = _direct_module()
    job_id = "direct-stale-generation"
    queue = _admitted_queue(job_id)

    with _origin_type()() as origin, _running_direct_controller(direct, tmp_path) as (
        controller,
        _,
    ):
        added = controller.add_paused(
            job_id=job_id,
            generation=8,
            source=_source(origin, "/range"),
            destination=_destination(job_id, "stale.bin"),
            expected_sha256=hashlib.sha256(origin.payload).hexdigest(),
            admission=_admission(queue, job_id),
        )

        with pytest.raises(direct.StaleGenerationError):
            controller.observe_callback(job_id=job_id, generation=7, gid=added.gid)
        current = controller.observe_callback(job_id=job_id, generation=8, gid=added.gid)
        assert current.status == "paused"
        assert origin.ledger.response_body_bytes == 0


def test_direct_stop_and_restart_do_not_resurrect_paused_transfer(tmp_path: Path) -> None:
    direct = _direct_module()
    job_id = "direct-restart"
    queue = _admitted_queue(job_id)

    with _origin_type()() as origin, _running_direct_controller(direct, tmp_path) as (
        controller,
        identities,
    ):
        added = controller.add_paused(
            job_id=job_id,
            generation=2,
            source=_source(origin, "/range"),
            destination=_destination(job_id, "restart.bin"),
            expected_sha256=hashlib.sha256(origin.payload).hexdigest(),
            admission=_admission(queue, job_id),
        )
        first_identity = controller.engine_identity
        assert first_identity is not None
        assert origin.ledger.response_body_bytes == 0

        controller.restart()
        second_identity = controller.engine_identity
        assert second_identity is not None
        assert second_identity.leader_pid != first_identity.leader_pid
        identities.append(second_identity)
        assert controller.gid_for_job(job_id) is None
        assert controller.job_for_gid(added.gid) is None
        assert controller.active_job_ids == ()
        time.sleep(0.05)
        assert origin.ledger.response_body_bytes == 0
