"""Timed stock-aria2 payload evidence from an owned, bounded loopback origin.

This controller fixture does not establish whole-worker bandwidth acceptance.
"""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import threading
import time

import pytest

import test_direct as helpers


_BODY_BUDGET = 512 * 1024 * 1024
_WALL_BOUND = 180
_PAYLOAD_SIZE = 128 * 1024 * 1024
_CHUNK = b"synthetic-payload" * 1024
_CAP = 512 * 1024


class _TimedOrigin:
    """Count every successful body send, including partial socket writes."""

    def __init__(self):
        self.events = []
        self.requests = []
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.total = 0
        self.reserved = 0
        self.started = time.monotonic()
        self.server = None
        self.thread = None

    def url(self, path="/"):
        return f"http://127.0.0.1:{self.server.server_port}{path}"

    def snapshot(self):
        with self.lock:
            return list(self.events), list(self.requests), self.total

    def __enter__(self):
        origin = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *_):
                pass

            def do_GET(self):
                # Retry bounded socket timeouts: receiver backpressure must not
                # become a synthetic disconnect when the global cap is shared.
                self.connection.settimeout(0.1)
                self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32 * 1024)
                start = 0
                range_header = self.headers.get("Range")
                if range_header is not None:
                    try:
                        start = int(range_header.removeprefix("bytes=").split("-")[0])
                    except ValueError:
                        self.send_error(416)
                        return
                with origin.lock:
                    origin.requests.append({"timestamp": time.monotonic(), "path": self.path, "range": range_header})
                self.send_response(206 if range_header else 200)
                self.send_header("Content-Length", str(_PAYLOAD_SIZE - start))
                self.send_header("Accept-Ranges", "bytes")
                if range_header:
                    self.send_header("Content-Range", f"bytes {start}-{_PAYLOAD_SIZE - 1}/{_PAYLOAD_SIZE}")
                self.end_headers()
                position = start
                try:
                    while position < _PAYLOAD_SIZE and not origin.stop.is_set():
                        if time.monotonic() - origin.started >= _WALL_BOUND - 5:
                            return
                        size = min(len(_CHUNK), _PAYLOAD_SIZE - position)
                        with origin.lock:
                            if origin.reserved + size > _BODY_BUDGET:
                                return
                            origin.reserved += size
                        sent = 0
                        try:
                            while sent < size:
                                if origin.stop.is_set() or time.monotonic() - origin.started >= _WALL_BOUND - 5:
                                    return
                                try:
                                    count = self.connection.send(memoryview(_CHUNK)[sent:size])
                                except TimeoutError:
                                    continue
                                if count == 0:
                                    return
                                timestamp = time.monotonic()
                                sent += count
                                with origin.lock:
                                    origin.total += count
                                    origin.events.append({"timestamp": timestamp, "bytes": count, "path": self.path})
                        finally:
                            with origin.lock:
                                origin.reserved -= size - sent
                        position += sent
                        # Independently paced above both configured caps.
                        if origin.stop.wait(0.005):
                            return
                except OSError:
                    return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = False
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01})
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        assert not self.thread.is_alive()


def _wait_for_body(origin, previous, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if origin.snapshot()[2] > previous:
            return
        time.sleep(0.01)
    pytest.fail("owned loopback origin did not send a new body")


def _wait_until(deadline):
    while time.monotonic() < deadline:
        time.sleep(min(0.1, deadline - time.monotonic()))


def test_stock_aria2_global_and_per_job_limits_with_timed_body_window(tmp_path):
    direct = helpers._direct_module()
    births = []
    record = {"scope": "synthetic loopback controller only", "body_budget_bytes": _BODY_BUDGET,
              "wall_bound_seconds": _WALL_BOUND, "readbacks": [], "windows": []}
    controller = direct.DirectAria2Controller(
        executable=helpers._ARIA2C, runtime_root=tmp_path / "engine", max_concurrent_downloads=2,
        split=1, max_connection_per_server=1, on_engine_bound=births.append,
    )
    origin = _TimedOrigin()
    process = None
    transfers = []
    queues = {}

    def add(job_id):
        queue = helpers._admitted_queue(job_id)
        queues[job_id] = queue
        root = Path.home() / "Downloads" / "Hermes"
        root.mkdir(parents=True, mode=0o700, exist_ok=True)
        destination = helpers._paths_module().resolve_destination(
            root, category="Other", filename=f"{job_id}.bin", job_id=job_id,
        )
        transfer = controller.add_paused(
            job_id=job_id, generation=1, source=helpers._source(origin, f"/{job_id}"),
            destination=destination, expected_sha256=None,
            admission=helpers._admission(queue, job_id),
        )
        assert transfer.status == "paused"
        transfers.append(transfer)
        return transfer

    def observe_global(value):
        assert controller.set_global_limit(global_limit_bps=value) == value
        options = controller._rpc("aria2.getGlobalOption", [])
        expected = "0" if value is None else str(value)
        assert options["max-overall-download-limit"] == expected
        record["readbacks"].append({"timestamp": time.monotonic(), "scope": "global", "configured": expected})

    def states():
        statuses = [controller.readback(job_id=t.job_id, generation=1, gid=t.gid).status for t in transfers[:2]]
        record.setdefault("status_observations", []).append({"timestamp": time.monotonic(), "statuses": statuses})
        return statuses

    def quiet_window(label, seconds=2):
        start = time.monotonic()
        before = origin.snapshot()[2]
        _wait_until(start + seconds)
        end = time.monotonic()
        assert origin.snapshot()[2] == before
        record["windows"].append({"label": label, "start": start, "end": end, "body_bytes": 0})

    started = time.monotonic()
    try:
        with origin:
            controller.start()
            process = controller._process
            identity = controller.engine_identity
            assert len(births) == 1
            assert direct.reconcile_process_birth(births[0]) == "current"
            record["engine_version"] = controller._rpc("aria2.getVersion", [])
            record["engine_birth"] = births[0].to_record()
            for job in ("timed-a", "timed-b"):
                transfer = add(job)
                allocation = 384 * 1024
                state = controller.set_allocation(job_id=job, generation=1, allocation_bps=allocation)
                assert state.status == "paused"
                options = controller._rpc("aria2.getOption", [transfer.gid])
                assert options["max-download-limit"] == str(allocation)
                record["readbacks"].append({"timestamp": time.monotonic(), "scope": job,
                                           "configured": options["max-download-limit"]})
            observe_global(1024 * 1024)
            assert origin.snapshot()[2] == 0
            for t in transfers:
                controller.resume(job_id=t.job_id, generation=1, admission=helpers._admission(queues[t.job_id], t.job_id))
            _wait_for_body(origin, 0)
            burst_start = time.monotonic()
            _wait_until(burst_start + 2)
            burst_end = time.monotonic()
            record["windows"].append({"label": "initial short burst", "start": burst_start, "end": burst_end})
            observe_global(_CAP)
            lower_set = time.monotonic()
            _wait_until(lower_set + 10)
            assert states() == ["active", "active"]
            steady_start = time.monotonic()
            _wait_until(steady_start + 30)
            steady_end = time.monotonic()
            assert states() == ["active", "active"]
            events = origin.snapshot()[0]
            body_bytes = sum(e["bytes"] for e in events if steady_start <= e["timestamp"] < steady_end)
            duration = steady_end - steady_start
            record["windows"].append({"label": "steady aggregate", "start": steady_start, "end": steady_end,
                                       "body_bytes": body_bytes, "duration": duration, "cap_bps": _CAP,
                                       "measured_bps": body_bytes / duration, "maximum_ratio": 1.05,
                                       "settle_seconds": steady_start - lower_set,
                                       "statuses_start_and_end": ["active", "active"]})
            assert body_bytes > 0
            assert body_bytes / duration <= 1.05 * _CAP
            assert controller.engine_identity == identity
            assert process.poll() is None
            assert controller.set_global_limit(global_limit_bps=0) == 0
            assert states() == ["paused", "paused"]
            _wait_until(time.monotonic() + 1)
            quiet_window("zero after bounded settle")
            future = add("timed-future")
            before = origin.snapshot()[2]
            with pytest.raises(direct.DirectAdmissionError):
                controller.resume(job_id=future.job_id, generation=1, admission=helpers._admission(queues[future.job_id], future.job_id))
            quiet_window("future job blocked by zero")
            observe_global(None)
            assert states() == ["paused", "paused"]
            state = controller.set_allocation(job_id=future.job_id, generation=1, allocation_bps=None)
            assert state.status == "paused"
            options = controller._rpc("aria2.getOption", [future.gid])
            assert options["max-download-limit"] == "0"
            record["readbacks"].append({"timestamp": time.monotonic(), "scope": future.job_id, "configured": "0"})
            quiet_window("None does not unpause")
            queues[future.job_id].pause_queue()
            with pytest.raises(direct.DirectAdmissionError):
                controller.resume(job_id=future.job_id, generation=1, admission=helpers._admission(queues[future.job_id], future.job_id))
            quiet_window("Admission remains required after None")
            controller.resume(job_id=transfers[0].job_id, generation=1,
                              admission=helpers._admission(queues[transfers[0].job_id], transfers[0].job_id))
            _wait_for_body(origin, before)
            assert controller.engine_identity == identity
            assert len(births) == 1
    finally:
        try:
            if process is not None and process.poll() is None:
                record["final_engine_statuses"] = [
                    controller._rpc("aria2.tellStatus", [t.gid, ["status", "errorCode"]]) for t in transfers
                ]
        finally:
            controller.close()
        if process is not None:
            record["engine_returncode"] = process.poll()
            record["reaped"] = process.returncode is not None
        if births:
            record["birth_after_close"] = direct.reconcile_process_birth(births[0])
        events, requests, total = origin.snapshot()
        record.update({"body_events": events, "requests": requests, "total_body_bytes": total,
                       "wall_seconds": time.monotonic() - started,
                       "server_thread_stopped": origin.thread is None or not origin.thread.is_alive()})
        for window in record["windows"]:
            independent = sum(e["bytes"] for e in events if window["start"] <= e["timestamp"] < window["end"])
            window["independent_recomputed_body_bytes"] = independent
            if "body_bytes" in window:
                assert window["body_bytes"] == independent
        evidence = tmp_path / "bandwidth-measurement.json"
        evidence.write_text(json.dumps(record, indent=2))
        evidence.chmod(0o600)
        print(f"TIMED_BODY_EVIDENCE={evidence}")
    assert record["total_body_bytes"] <= _BODY_BUDGET
    assert record["wall_seconds"] <= _WALL_BOUND
    assert record["reaped"] is True
    assert record["birth_after_close"] == "absent"
