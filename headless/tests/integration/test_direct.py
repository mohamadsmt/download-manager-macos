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


def _assert_pid_gone(pid: int) -> None:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        except PermissionError:
            pass
        time.sleep(0.01)
    pytest.fail("aria2 leader survived controller cleanup")


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


def test_direct_rejects_forged_incomplete_destination_before_rpc_or_origin_bytes(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    paths = _paths_module()
    job_id = "direct-forged-destination"
    queue = _admitted_queue(job_id)
    root = Path.home() / "Downloads" / "Hermes"
    outside_incomplete_dir = tmp_path / "outside" / job_id
    destination = paths.DestinationIntent(
        root=root,
        category="Other",
        collection=None,
        filename="forged.bin",
        job_id=job_id,
        final_path=root / "Other" / "forged.bin",
        incomplete_dir=outside_incomplete_dir,
        partial_path=outside_incomplete_dir / "forged.bin",
    )
    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )
    rpc_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def unexpected_rpc(*args: Any, **kwargs: Any) -> Any:
        rpc_calls.append((args, kwargs))
        raise AssertionError("forged destination reached aria2 RPC")

    monkeypatch.setattr(controller, "_require_running", lambda: (object(), 4321, "secret"))
    monkeypatch.setattr(controller, "_rpc", unexpected_rpc)

    with _origin_type()() as origin:
        with pytest.raises(direct.DirectTransferError):
            controller.add_paused(
                job_id=job_id,
                generation=1,
                source=_source(origin, "/range"),
                destination=destination,
                expected_sha256=hashlib.sha256(origin.payload).hexdigest(),
                admission=_admission(queue, job_id),
            )

        assert rpc_calls == []
        assert origin.ledger.response_body_bytes == 0


def test_direct_rejects_canonical_partial_symlink_before_aria2_rpc_or_origin_bytes(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    paths = _paths_module()
    job_id = "direct-partial-symlink"
    filename = "partial-symlink.bin"
    queue = _admitted_queue(job_id)

    with _origin_type()() as origin, _running_direct_controller(direct, tmp_path) as (
        controller,
        _,
    ):
        prepared = _destination(job_id, filename)
        destination = paths.DestinationIntent(
            root=prepared.root,
            category=prepared.category,
            collection=prepared.collection,
            filename=prepared.filename,
            job_id=prepared.job_id,
            final_path=prepared.final_path,
            incomplete_dir=prepared.incomplete_dir,
            partial_path=prepared.partial_path,
        )
        canonical_root = Path.home() / "Downloads" / "Hermes"
        assert destination.root == canonical_root
        assert destination.incomplete_dir == canonical_root / ".incomplete" / job_id
        assert destination.partial_path == destination.incomplete_dir / filename

        outside_canary = tmp_path / "outside-canary.bin"
        canary_bytes = b"outside direct partial canary"
        outside_canary.write_bytes(canary_bytes)
        destination.partial_path.symlink_to(outside_canary)
        assert destination.partial_path.is_symlink()

        rpc_calls: list[tuple[str, list[Any]]] = []
        original_rpc = controller._rpc

        def unexpected_rpc(method: str, params: list[Any]) -> Any:
            rpc_calls.append((method, params))
            raise AssertionError("canonical partial symlink reached aria2 RPC")

        monkeypatch.setattr(controller, "_rpc", unexpected_rpc)
        try:
            with pytest.raises(direct.DirectTransferError):
                controller.add_paused(
                    job_id=job_id,
                    generation=1,
                    source=_source(origin, "/range"),
                    destination=destination,
                    expected_sha256=hashlib.sha256(origin.payload).hexdigest(),
                    admission=_admission(queue, job_id),
                )
        finally:
            monkeypatch.setattr(controller, "_rpc", original_rpc)

        assert rpc_calls == []
        assert outside_canary.read_bytes() == canary_bytes
        assert origin.ledger.response_body_bytes == 0


@pytest.mark.parametrize("unsafe_shape", ("symlink", "multilink", "directory"))
def test_direct_rejects_unsafe_aria2_sidecar_before_rpc_or_origin_bytes(
    tmp_path: Path, monkeypatch, unsafe_shape: str
) -> None:
    direct = _direct_module()
    paths = _paths_module()
    job_id = f"direct-sidecar-{unsafe_shape}"
    filename = "sidecar.bin"
    queue = _admitted_queue(job_id)

    with _origin_type()() as origin, _running_direct_controller(direct, tmp_path) as (
        controller,
        _,
    ):
        prepared = _destination(job_id, filename)
        destination = paths.DestinationIntent(
            root=prepared.root,
            category=prepared.category,
            collection=prepared.collection,
            filename=prepared.filename,
            job_id=prepared.job_id,
            final_path=prepared.final_path,
            incomplete_dir=prepared.incomplete_dir,
            partial_path=prepared.partial_path,
        )
        sidecar = destination.partial_path.with_name(f"{filename}.aria2")
        assert sidecar == destination.incomplete_dir / f"{filename}.aria2"
        outside_canary = tmp_path / f"outside-sidecar-{unsafe_shape}.bin"
        canary_bytes = b"outside direct sidecar canary"
        outside_canary.write_bytes(canary_bytes)
        if unsafe_shape == "symlink":
            sidecar.symlink_to(outside_canary)
        elif unsafe_shape == "multilink":
            os.link(outside_canary, sidecar)
            assert sidecar.stat().st_nlink == 2
        else:
            sidecar.mkdir()

        source = _source(origin, "/range")
        source_bytes = source.raw_url
        rpc_calls: list[tuple[str, list[Any]]] = []
        original_rpc = controller._rpc

        def unexpected_rpc(method: str, params: list[Any]) -> Any:
            rpc_calls.append((method, params))
            raise AssertionError("unsafe aria2 sidecar reached aria2 RPC")

        monkeypatch.setattr(controller, "_rpc", unexpected_rpc)
        try:
            with pytest.raises(direct.DirectTransferError):
                controller.add_paused(
                    job_id=job_id,
                    generation=1,
                    source=source,
                    destination=destination,
                    expected_sha256=hashlib.sha256(origin.payload).hexdigest(),
                    admission=_admission(queue, job_id),
                )
        finally:
            monkeypatch.setattr(controller, "_rpc", original_rpc)

        assert rpc_calls == []
        assert outside_canary.read_bytes() == canary_bytes
        assert source.raw_url == source_bytes
        assert origin.ledger.request_count == 0
        assert origin.ledger.response_body_bytes == 0


def test_direct_rejects_forged_noncanonical_root_before_rpc_or_origin_bytes(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    paths = _paths_module()
    job_id = "direct-forged-root"
    queue = _admitted_queue(job_id)
    root = tmp_path / "forged-root"
    destination = paths.DestinationIntent(
        root=root,
        category="Other",
        collection=None,
        filename="forged-root.bin",
        job_id=job_id,
        final_path=root / "Other" / "forged-root.bin",
        incomplete_dir=root / ".incomplete" / job_id,
        partial_path=root / ".incomplete" / job_id / "forged-root.bin",
    )
    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )
    rpc_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def unexpected_rpc(*args: Any, **kwargs: Any) -> Any:
        rpc_calls.append((args, kwargs))
        raise AssertionError("forged destination reached aria2 RPC")

    assert root != Path.home() / "Downloads" / "Hermes"
    monkeypatch.setattr(controller, "_require_running", lambda: (object(), 4321, "secret"))
    monkeypatch.setattr(controller, "_rpc", unexpected_rpc)

    with _origin_type()() as origin:
        with pytest.raises(direct.DirectTransferError):
            controller.add_paused(
                job_id=job_id,
                generation=1,
                source=_source(origin, "/range"),
                destination=destination,
                expected_sha256=hashlib.sha256(origin.payload).hexdigest(),
                admission=_admission(queue, job_id),
            )

        assert rpc_calls == []
        assert origin.ledger.response_body_bytes == 0


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


def test_direct_binding_interrupt_reaps_spawned_daemon_and_removes_private_runtime(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    runtime_root = tmp_path / "aria2-private-runtime"
    private_runtime_paths: list[Path] = []
    spawned_processes: list[Any] = []
    launch_argvs: list[tuple[str, ...]] = []
    launch_options: list[dict[str, Any]] = []
    secret_marker = "fixture-direct-binding-rpc-secret"
    interrupt = KeyboardInterrupt("fixture-direct-binding-interrupt")
    original_popen = direct.subprocess.Popen

    def capture_popen(*args: Any, **kwargs: Any) -> Any:
        process = original_popen(*args, **kwargs)
        spawned_processes.append(process)
        launch_argvs.append(tuple(args[0]))
        launch_options.append(kwargs)
        private_runtime_paths.append(Path(kwargs["cwd"]))
        return process

    def raise_interrupt(process: Any) -> None:
        assert process is spawned_processes[0]
        raise interrupt

    monkeypatch.setattr(direct.secrets, "token_urlsafe", lambda _bytes: secret_marker)
    monkeypatch.setattr(direct.subprocess, "Popen", capture_popen)
    monkeypatch.setattr(direct, "_bind_process_group", raise_interrupt)

    try:
        controller = direct.DirectAria2Controller(
            executable=_ARIA2C,
            runtime_root=runtime_root,
        )
        with pytest.raises(KeyboardInterrupt) as raised:
            controller.start()

        assert raised.value is interrupt
        assert len(spawned_processes) == 1
        assert len(private_runtime_paths) == 1
        assert len(launch_argvs) == len(launch_options) == 1
        process = spawned_processes[0]
        assert process.poll() is not None
        _assert_pid_gone(process.pid)
        _assert_group_gone(process.pid)
        assert not private_runtime_paths[0].exists()
        assert secret_marker not in launch_argvs[0]
        assert launch_options[0]["stdout"] is direct.subprocess.DEVNULL
        assert launch_options[0]["stderr"] is direct.subprocess.DEVNULL
        assert secret_marker not in str(raised.value)
        assert secret_marker not in repr(raised.value)
    finally:
        for process in spawned_processes:
            if process.poll() is None:
                direct._stop_process_group(process, process.pid)
        for runtime_path in private_runtime_paths:
            if runtime_path.exists():
                direct._remove_private_runtime(runtime_path)


def test_direct_rpc_failure_signals_saved_group_before_reaping_its_leader(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    events: list[object] = []

    class FakeProcess:
        pid = 4242

        def __init__(self) -> None:
            self.poll_calls = 0

        def poll(self) -> int:
            self.poll_calls += 1
            events.append("poll")
            return 0

    class FailingConnection:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def request(self, *_args: Any, **_kwargs: Any) -> None:
            raise OSError

        def close(self) -> None:
            events.append("connection-close")

    process = FakeProcess()
    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )
    controller._process = process
    controller._identity = direct.EngineIdentity(
        leader_pid=process.pid,
        process_group_id=process.pid,
        started_monotonic_ns=1,
        argv_sha256="0" * 64,
    )
    controller._port = 4321
    controller._secret = "fixture-rpc-secret"
    group_absences = iter((False, True, True))

    def record_group_signal(
        process_group_id: int, signal_number: signal.Signals
    ) -> bool:
        events.append(("signal", process_group_id, signal_number))
        return True

    def record_group_absence(_process_group_id: int, _deadline: float) -> bool:
        events.append("absence")
        return next(group_absences)

    def record_leader_reap(_process: Any, _deadline: float) -> bool:
        events.append("reap")
        return True

    monkeypatch.setattr(direct.http.client, "HTTPConnection", FailingConnection)
    monkeypatch.setattr(direct, "_signal_group", record_group_signal)
    monkeypatch.setattr(direct, "_wait_for_group_absence", record_group_absence)
    monkeypatch.setattr(direct, "_wait_for_leader", record_leader_reap)

    controller.close()

    assert process.poll_calls == 0
    assert events == [
        "connection-close",
        ("signal", process.pid, signal.SIGTERM),
        "absence",
        ("signal", process.pid, signal.SIGKILL),
        "absence",
        "reap",
        "absence",
    ]


def test_direct_rpc_malformed_response_drops_raw_exception_context(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    marker = "peer-reflected-signed-url-marker"

    class MalformedResponse:
        status = 200

        def read(self, _amount: int) -> bytes:
            return f'{{"peer":"{marker}"'.encode("utf-8")

    class MalformedConnection:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def request(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def getresponse(self) -> MalformedResponse:
            return MalformedResponse()

        def close(self) -> None:
            pass

    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )
    monkeypatch.setattr(controller, "_require_running", lambda: (object(), 4321, "secret"))
    monkeypatch.setattr(direct.http.client, "HTTPConnection", MalformedConnection)

    with pytest.raises(direct.DirectEngineError) as raised:
        controller._rpc("aria2.tellStatus", [])

    assert type(raised.value) is direct.DirectEngineError
    assert marker not in str(raised.value)
    assert marker not in repr(raised.value)
    assert raised.value.__context__ is None
    assert raised.value.__cause__ is None


def test_direct_rpc_transport_failure_drops_raw_exception_context(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    marker = "peer-reflected-rpc-secret-marker"

    class FailingConnection:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def request(self, *_args: Any, **_kwargs: Any) -> None:
            raise OSError(marker)

        def close(self) -> None:
            pass

    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )
    monkeypatch.setattr(controller, "_require_running", lambda: (object(), 4321, "secret"))
    monkeypatch.setattr(direct.http.client, "HTTPConnection", FailingConnection)

    with pytest.raises(direct.DirectEngineError) as raised:
        controller._rpc("aria2.tellStatus", [])

    assert type(raised.value) is direct.DirectEngineError
    assert marker not in str(raised.value)
    assert marker not in repr(raised.value)
    assert raised.value.__context__ is None
    assert raised.value.__cause__ is None


def test_direct_close_keeps_daemon_owned_when_group_cleanup_fails(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()

    class FakeProcess:
        pid = 4343

    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )
    runtime_path, config_path, secret = controller._create_private_config()
    process = FakeProcess()
    identity = direct.EngineIdentity(
        leader_pid=process.pid,
        process_group_id=process.pid,
        started_monotonic_ns=1,
        argv_sha256="0" * 64,
    )
    controller._process = process
    controller._identity = identity
    controller._port = 4321
    controller._secret = secret
    controller._private_runtime_path = runtime_path
    controller._private_config_path = config_path

    def unavailable_rpc(*_args: Any, **_kwargs: Any) -> Any:
        raise direct.DirectEngineError("fixture RPC unavailable")

    monkeypatch.setattr(controller, "_rpc", unavailable_rpc)
    monkeypatch.setattr(direct, "_stop_process_group", lambda *_args: False)

    try:
        with pytest.raises(direct.DirectEngineError):
            controller.close()

        assert controller._process is process
        assert controller.engine_identity is identity
        assert controller.private_runtime_path == runtime_path
        assert controller.private_config_path == config_path
        assert runtime_path.exists()
    finally:
        controller._clear_runtime_state()
        if runtime_path.exists():
            direct._remove_private_runtime(runtime_path)


def test_direct_close_retains_private_runtime_after_cleanup_failure_for_retry(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()

    class FakeProcess:
        pid = 4443

    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )
    runtime_path, config_path, secret = controller._create_private_config()
    process = FakeProcess()
    identity = direct.EngineIdentity(
        leader_pid=process.pid,
        process_group_id=process.pid,
        started_monotonic_ns=1,
        argv_sha256="0" * 64,
    )
    controller._process = process
    controller._identity = identity
    controller._port = 4321
    controller._secret = secret
    controller._private_runtime_path = runtime_path
    controller._private_config_path = config_path
    removal_paths: list[Path] = []
    original_remove = direct._remove_private_runtime

    def fail_once(path: Path) -> None:
        removal_paths.append(path)
        if len(removal_paths) == 1:
            raise direct.DirectEngineError("aria2 private runtime cleanup failed")
        original_remove(path)

    monkeypatch.setattr(controller, "_stop_owned_process", lambda: (True, None))
    monkeypatch.setattr(direct, "_remove_private_runtime", fail_once)

    try:
        with pytest.raises(direct.DirectEngineError) as raised:
            controller.close()

        assert str(raised.value) == "aria2 private runtime cleanup failed"
        assert secret not in str(raised.value)
        assert removal_paths == [runtime_path]
        assert controller._process is None
        assert controller.engine_identity is None
        start_attempts: list[None] = []

        def unexpected_create_private_config() -> tuple[Path, Path, str]:
            start_attempts.append(None)
            raise AssertionError("cleanup-pending controller started a new runtime")

        monkeypatch.setattr(
            controller, "_create_private_config", unexpected_create_private_config
        )
        with pytest.raises(direct.DirectEngineError):
            controller.start()
        assert start_attempts == []
        assert controller.private_runtime_path == runtime_path
        assert controller.private_config_path == config_path
        assert runtime_path.exists()

        controller.close()

        assert removal_paths == [runtime_path, runtime_path]
        assert not runtime_path.exists()
        assert controller.engine_identity is None
        with pytest.raises(direct.DirectEngineError):
            _ = controller.private_runtime_path
    finally:
        if runtime_path.exists():
            original_remove(runtime_path)


def test_direct_start_keeps_daemon_owned_when_bound_cleanup_fails(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    private_runtime_paths: list[Path] = []
    readiness_failure = direct.DirectEngineError("fixture readiness failure")

    class FakeProcess:
        pid = 4444

    process = FakeProcess()
    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )

    def capture_popen(*_args: Any, **kwargs: Any) -> Any:
        private_runtime_paths.append(Path(kwargs["cwd"]))
        return process

    def fail_ready() -> None:
        raise readiness_failure

    monkeypatch.setattr(direct.subprocess, "Popen", capture_popen)
    monkeypatch.setattr(direct, "_bind_process_group", lambda _process: process.pid)
    monkeypatch.setattr(direct, "_stop_process_group", lambda *_args: False)
    monkeypatch.setattr(controller, "_wait_for_rpc_ready", fail_ready)

    try:
        with pytest.raises(direct.DirectEngineError):
            controller.start()

        assert controller._process is process
        identity = controller.engine_identity
        assert identity is not None
        assert identity.process_group_id == process.pid
        assert controller.private_runtime_path == private_runtime_paths[0]
        assert controller.private_config_path.exists()
        assert private_runtime_paths[0].exists()
    finally:
        controller._clear_runtime_state()
        for runtime_path in private_runtime_paths:
            if runtime_path.exists():
                direct._remove_private_runtime(runtime_path)


def test_direct_start_preserves_primary_interrupt_over_cleanup_interrupt(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    primary = KeyboardInterrupt("fixture-primary-interrupt")
    cleanup_interrupt = KeyboardInterrupt("fixture-cleanup-interrupt")
    runtime_paths: list[Path] = []

    class FakeProcess:
        pid = 4545

    process = FakeProcess()
    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )

    def capture_popen(*_args: Any, **kwargs: Any) -> Any:
        runtime_paths.append(Path(kwargs["cwd"]))
        return process

    def raise_primary() -> None:
        raise primary

    def raise_cleanup(*_args: Any) -> tuple[bool, BaseException | None]:
        raise cleanup_interrupt

    monkeypatch.setattr(direct.subprocess, "Popen", capture_popen)
    monkeypatch.setattr(direct, "_bind_process_group", lambda _process: process.pid)
    monkeypatch.setattr(controller, "_wait_for_rpc_ready", raise_primary)
    monkeypatch.setattr(direct, "_stop_process_group", raise_cleanup)

    try:
        with pytest.raises(KeyboardInterrupt) as raised:
            controller.start()

        assert raised.value is primary
        assert controller._process is process
        assert controller.engine_identity is not None
    finally:
        controller._clear_runtime_state()
        for runtime_path in runtime_paths:
            if runtime_path.exists():
                direct._remove_private_runtime(runtime_path)


def test_direct_start_preserves_primary_interrupt_when_private_runtime_cleanup_fails(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    primary = KeyboardInterrupt("fixture-primary-interrupt")
    cleanup_failure = direct.DirectEngineError("fixture-private-runtime-cleanup-failed")
    secret_marker = "fixture-private-runtime-secret"
    runtime_paths: list[Path] = []
    cleanup_paths: list[Path] = []
    stop_calls: list[tuple[Any, int]] = []
    original_remove = direct._remove_private_runtime

    class FakeProcess:
        pid = 4646

    process = FakeProcess()
    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )

    def capture_popen(*_args: Any, **kwargs: Any) -> Any:
        runtime_paths.append(Path(kwargs["cwd"]))
        return process

    def raise_primary() -> None:
        raise primary

    def stop_group(
        stopped_process: Any, process_group_id: int
    ) -> tuple[bool, BaseException | None]:
        stop_calls.append((stopped_process, process_group_id))
        return True, None

    def fail_once(path: Path) -> None:
        cleanup_paths.append(path)
        if len(cleanup_paths) == 1:
            raise cleanup_failure
        original_remove(path)

    monkeypatch.setattr(direct.secrets, "token_urlsafe", lambda _bytes: secret_marker)
    monkeypatch.setattr(direct.subprocess, "Popen", capture_popen)
    monkeypatch.setattr(direct, "_bind_process_group", lambda _process: process.pid)
    monkeypatch.setattr(controller, "_wait_for_rpc_ready", raise_primary)
    monkeypatch.setattr(direct, "_stop_process_group", stop_group)
    monkeypatch.setattr(direct, "_remove_private_runtime", fail_once)

    try:
        with pytest.raises(KeyboardInterrupt) as raised:
            controller.start()

        runtime_path = runtime_paths[0]
        config_path = runtime_path / "aria2.conf"
        assert raised.value is primary
        assert stop_calls == [(process, process.pid)]
        assert cleanup_paths == [runtime_path]
        assert controller._process is None
        assert controller.engine_identity is None
        assert controller._port is None
        assert controller._secret is None
        assert controller.launch_argv == ()
        assert controller.private_runtime_path == runtime_path
        assert controller.private_config_path == config_path
        assert runtime_path.exists()
        assert config_path.stat().st_mode & 0o777 == 0o600
        assert secret_marker not in str(raised.value)
        assert secret_marker not in repr(raised.value)

        start_attempts: list[None] = []

        def unexpected_create_private_config() -> tuple[Path, Path, str]:
            start_attempts.append(None)
            raise AssertionError("cleanup-pending controller started a new runtime")

        monkeypatch.setattr(
            controller, "_create_private_config", unexpected_create_private_config
        )
        with pytest.raises(direct.DirectEngineError):
            controller.start()
        assert start_attempts == []

        controller.close()

        assert cleanup_paths == [runtime_path, runtime_path]
        assert not runtime_path.exists()
        with pytest.raises(direct.DirectEngineError):
            _ = controller.private_runtime_path
    finally:
        controller._clear_runtime_state()
        for runtime_path in runtime_paths:
            if runtime_path.exists():
                original_remove(runtime_path)


def test_direct_close_defers_cleanup_interrupt_until_group_is_reaped_and_state_is_cleared(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    interrupt = KeyboardInterrupt("fixture-direct-term-grace-interrupt")
    events: list[object] = []
    interruption_injected = False
    original_signal_group = direct._signal_group
    original_wait_for_group_absence = direct._wait_for_group_absence
    original_wait_for_leader = direct._wait_for_leader

    def unavailable_rpc(*_args: Any, **_kwargs: Any) -> Any:
        raise direct.DirectEngineError("fixture RPC unavailable")

    def record_group_signal(
        process_group_id: int, signal_number: signal.Signals
    ) -> bool:
        events.append(("signal", process_group_id, signal_number))
        return original_signal_group(process_group_id, signal_number)

    def interrupt_term_grace(process_group_id: int, deadline: float) -> bool:
        nonlocal interruption_injected
        events.append("absence")
        if not interruption_injected:
            interruption_injected = True
            raise interrupt
        return original_wait_for_group_absence(process_group_id, deadline)

    def record_leader_reap(process: Any, deadline: float) -> bool:
        events.append("reap")
        return original_wait_for_leader(process, deadline)

    with _running_direct_controller(direct, tmp_path) as (controller, _):
        process = controller._process
        identity = controller.engine_identity
        assert process is not None
        assert identity is not None
        runtime_path = controller.private_runtime_path
        monkeypatch.setattr(controller, "_rpc", unavailable_rpc)
        monkeypatch.setattr(direct, "_signal_group", record_group_signal)
        monkeypatch.setattr(
            direct, "_wait_for_group_absence", interrupt_term_grace
        )
        monkeypatch.setattr(direct, "_wait_for_leader", record_leader_reap)

        with pytest.raises(KeyboardInterrupt) as raised:
            controller.close()

        assert raised.value is interrupt
        assert interruption_injected
        assert events == [
            ("signal", identity.process_group_id, signal.SIGTERM),
            "absence",
            ("signal", identity.process_group_id, signal.SIGKILL),
            "absence",
            "reap",
            "absence",
        ]
        assert process.returncode is not None
        assert _group_is_gone(identity.process_group_id)
        assert controller.engine_identity is None
        assert not runtime_path.exists()


def test_direct_start_retains_private_runtime_after_ordinary_startup_cleanup_failure(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )
    runtime_paths: list[Path] = []
    cleanup_paths: list[Path] = []
    original_remove = direct._remove_private_runtime

    def fail_popen(*_args: Any, **kwargs: Any) -> Any:
        runtime_paths.append(Path(kwargs["cwd"]))
        raise OSError("fixture ordinary startup failure")

    def fail_once(path: Path) -> None:
        cleanup_paths.append(path)
        if len(cleanup_paths) == 1:
            raise direct.DirectEngineError("fixture private runtime cleanup failure")
        original_remove(path)

    monkeypatch.setattr(direct.subprocess, "Popen", fail_popen)
    monkeypatch.setattr(direct, "_remove_private_runtime", fail_once)

    try:
        with pytest.raises(direct.DirectEngineError) as raised:
            controller.start()

        runtime_path = runtime_paths[0]
        config_path = runtime_path / "aria2.conf"
        assert str(raised.value) == "aria2 could not start"
        assert cleanup_paths == [runtime_path]
        assert controller._process is None
        assert controller.engine_identity is None
        assert controller.private_runtime_path == runtime_path
        assert controller.private_config_path == config_path
        assert runtime_path.exists()
        assert config_path.exists()

        controller.close()

        assert cleanup_paths == [runtime_path, runtime_path]
        assert not runtime_path.exists()
        with pytest.raises(direct.DirectEngineError):
            _ = controller.private_runtime_path
    finally:
        controller._clear_runtime_state()
        for runtime_path in runtime_paths:
            if runtime_path.exists():
                original_remove(runtime_path)


def test_direct_start_preserves_prelaunch_interrupt_when_private_runtime_cleanup_fails(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )
    primary = KeyboardInterrupt("fixture prelaunch interrupt")
    cleanup_paths: list[Path] = []
    original_remove = direct._remove_private_runtime

    def raise_primary() -> int:
        raise primary

    def fail_once(path: Path) -> None:
        cleanup_paths.append(path)
        if len(cleanup_paths) == 1:
            raise direct.DirectEngineError("fixture private runtime cleanup failure")
        original_remove(path)

    monkeypatch.setattr(direct, "_reserve_loopback_port", raise_primary)
    monkeypatch.setattr(direct, "_remove_private_runtime", fail_once)

    try:
        with pytest.raises(KeyboardInterrupt) as raised:
            controller.start()

        runtime_path = cleanup_paths[0]
        config_path = runtime_path / "aria2.conf"
        assert raised.value is primary
        assert cleanup_paths == [runtime_path]
        assert controller._process is None
        assert controller.engine_identity is None
        assert controller.private_runtime_path == runtime_path
        assert controller.private_config_path == config_path
        assert runtime_path.exists()
        assert config_path.exists()

        controller.close()

        assert cleanup_paths == [runtime_path, runtime_path]
        assert not runtime_path.exists()
        with pytest.raises(direct.DirectEngineError):
            _ = controller.private_runtime_path
    finally:
        controller._clear_runtime_state()
        for runtime_path in cleanup_paths:
            if runtime_path.exists():
                original_remove(runtime_path)


def test_direct_start_preserves_config_creation_interrupt_when_private_runtime_cleanup_fails(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )
    primary = KeyboardInterrupt("fixture private config interrupt")
    cleanup_paths: list[Path] = []
    original_remove = direct._remove_private_runtime

    def raise_primary(_bytes: int) -> str:
        raise primary

    def fail_once(path: Path) -> None:
        cleanup_paths.append(path)
        if len(cleanup_paths) == 1:
            raise direct.DirectEngineError("fixture private runtime cleanup failure")
        original_remove(path)

    monkeypatch.setattr(direct.secrets, "token_urlsafe", raise_primary)
    monkeypatch.setattr(direct, "_remove_private_runtime", fail_once)

    try:
        with pytest.raises(KeyboardInterrupt) as raised:
            controller.start()

        runtime_path = cleanup_paths[0]
        config_path = runtime_path / "aria2.conf"
        assert raised.value is primary
        assert cleanup_paths == [runtime_path]
        assert controller._process is None
        assert controller.engine_identity is None
        assert controller.private_runtime_path == runtime_path
        assert controller.private_config_path == config_path
        assert runtime_path.exists()
        assert not config_path.exists()

        controller.close()

        assert cleanup_paths == [runtime_path, runtime_path]
        assert not runtime_path.exists()
        with pytest.raises(direct.DirectEngineError):
            _ = controller.private_runtime_path
    finally:
        controller._clear_runtime_state()
        for runtime_path in cleanup_paths:
            if runtime_path.exists():
                original_remove(runtime_path)


def test_direct_close_preserves_primary_interrupt_when_private_runtime_cleanup_fails(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()

    class FakeProcess:
        pid = 4747

    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )
    runtime_path, config_path, secret = controller._create_private_config()
    process = FakeProcess()
    primary = KeyboardInterrupt("fixture close interrupt")
    cleanup_paths: list[Path] = []
    original_remove = direct._remove_private_runtime
    stop_results = [(True, primary), (True, None)]
    controller._process = process
    controller._identity = direct.EngineIdentity(
        leader_pid=process.pid,
        process_group_id=process.pid,
        started_monotonic_ns=1,
        argv_sha256="0" * 64,
    )
    controller._port = 4321
    controller._secret = secret
    controller._private_runtime_path = runtime_path
    controller._private_config_path = config_path

    def fail_once(path: Path) -> None:
        cleanup_paths.append(path)
        if len(cleanup_paths) == 1:
            raise direct.DirectEngineError("fixture private runtime cleanup failure")
        original_remove(path)

    monkeypatch.setattr(controller, "_stop_owned_process", lambda: stop_results.pop(0))
    monkeypatch.setattr(direct, "_remove_private_runtime", fail_once)

    try:
        with pytest.raises(KeyboardInterrupt) as raised:
            controller.close()

        assert raised.value is primary
        assert cleanup_paths == [runtime_path]
        assert controller._process is None
        assert controller.engine_identity is None
        assert controller.private_runtime_path == runtime_path
        assert controller.private_config_path == config_path
        assert runtime_path.exists()

        controller.close()

        assert cleanup_paths == [runtime_path, runtime_path]
        assert not runtime_path.exists()
        with pytest.raises(direct.DirectEngineError):
            _ = controller.private_runtime_path
    finally:
        controller._clear_runtime_state()
        if runtime_path.exists():
            original_remove(runtime_path)


def test_direct_context_manager_preserves_body_interrupt_when_runtime_cleanup_fails_and_retries(
    tmp_path: Path, monkeypatch
) -> None:
    direct = _direct_module()
    controller = direct.DirectAria2Controller(
        executable=_ARIA2C,
        runtime_root=tmp_path / "aria2-private-runtime",
    )
    primary = KeyboardInterrupt("fixture context body interrupt")
    cleanup_paths: list[Path] = []
    original_remove = direct._remove_private_runtime
    runtime_path: Path | None = None
    config_path: Path | None = None

    def fail_once(path: Path) -> None:
        cleanup_paths.append(path)
        if len(cleanup_paths) == 1:
            raise direct.DirectEngineError("fixture private runtime cleanup failure")
        original_remove(path)

    monkeypatch.setattr(direct, "_remove_private_runtime", fail_once)

    try:
        with pytest.raises(KeyboardInterrupt) as raised:
            with controller:
                runtime_path = controller.private_runtime_path
                config_path = controller.private_config_path
                raise primary

        assert runtime_path is not None
        assert config_path is not None
        assert raised.value is primary
        assert cleanup_paths == [runtime_path]
        assert controller._process is None
        assert controller.engine_identity is None
        assert controller.private_runtime_path == runtime_path
        assert controller.private_config_path == config_path
        assert runtime_path.exists()
        assert config_path.exists()

        controller.close()

        assert cleanup_paths == [runtime_path, runtime_path]
        assert not runtime_path.exists()
        with pytest.raises(direct.DirectEngineError):
            _ = controller.private_runtime_path
    finally:
        controller._clear_runtime_state()
        if runtime_path is not None and runtime_path.exists():
            original_remove(runtime_path)
