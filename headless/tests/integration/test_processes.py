"""Real-process tests for contained engine subprocess lifecycles."""

from __future__ import annotations

import importlib
import importlib.util
import os
from pathlib import Path
import signal
import sys
import threading
import time

import pytest


FIXTURE = Path(__file__).parents[1] / "fixtures" / "process_fixture.py"
_FINITE_STREAM_BYTES = 2 * 1024 * 1024
_STARTUP_INTERRUPT_REPETITIONS = 25
_DELAY_DESCENDANT_PID_WRITE = """
import pathlib
import runpy
import sys
import time

target = pathlib.Path(sys.argv[1])
original_open = pathlib.Path.open


def delayed_open(path, *args, **kwargs):
    opened = original_open(path, *args, **kwargs)
    if path == target:
        time.sleep(0.05)
    return opened


pathlib.Path.open = delayed_open
fixture, *arguments = sys.argv[2:]
sys.argv = [fixture, *arguments]
runpy.run_path(fixture, run_name="__main__")
"""


def _finite_stream_output(name: str, fill: str) -> str:
    prefix = f"fixture-finite-{name}-saturation:"
    suffix = ":complete\n"
    return prefix + (fill * (_FINITE_STREAM_BYTES - len(prefix) - len(suffix))) + suffix


def _processes():
    spec = importlib.util.find_spec("hermes_downloads.processes")
    assert spec is not None, "hermes_downloads.processes must provide process containment"
    return importlib.import_module("hermes_downloads.processes")


def _pid_is_gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def _assert_pid_gone(pid: int) -> None:
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        if _pid_is_gone(pid):
            return
        time.sleep(0.01)
    pytest.fail("fixture process survived containment")


def _assert_pid_reaped(pid: int) -> None:
    try:
        waited, _ = os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return
    if waited == 0:
        pytest.fail("contained leader remained live after cleanup")
    pytest.fail("contained leader was left unreaped after cleanup")


def _reap_direct_child(pid: int) -> None:
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        try:
            waited, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return
        if waited == pid:
            return
        time.sleep(0.01)


def _kill_fixture_process_group(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    _reap_direct_child(pid)
    _assert_pid_gone(pid)


def _kill_fixture_group(pid_file: Path) -> None:
    if pid_file.exists():
        _kill_fixture_process_group(int(pid_file.read_text("ascii")))


def _assert_public_error(error: BaseException, reason: str, *markers: str) -> None:
    assert getattr(error, "reason") == reason
    for marker in markers:
        assert marker not in str(error)
        assert marker not in repr(error)
    assert error.__context__ is None
    assert error.__cause__ is None


def test_runs_fixed_argv_with_filtered_environment_and_persistable_identity(
    tmp_path: Path, monkeypatch
) -> None:
    processes = _processes()
    marker = "fixture-private-environment-marker"
    monkeypatch.setenv("T08_PRIVATE_MARKER", marker)
    pid_file = tmp_path / "leader.pid"

    result = processes.run_contained(
        (
            sys.executable,
            str(FIXTURE),
            "probe",
            str(pid_file),
            "$T08_LITERAL",
        ),
        cwd=tmp_path,
        timeout=1.0,
        output_limit=1024,
        environment={
            "PATH": os.environ.get("PATH", ""),
            "T08_PRIVATE_MARKER": marker,
        },
    )

    assert result.returncode == 0
    assert result.stdout == "probe-ok\n"
    assert result.stderr == ""
    assert result.identity.leader_pid == int(pid_file.read_text("ascii"))
    record = result.identity.to_record()
    assert record["leader_pid"] == result.identity.leader_pid
    assert record["process_group_id"] == result.identity.process_group_id
    assert record["argv_sha256"] not in {"", marker}
    assert processes.EngineIdentity.from_record(record) == result.identity


def test_cancelled_startup_never_launches_the_fixture(tmp_path: Path) -> None:
    processes = _processes()
    cancelled = threading.Event()
    cancelled.set()
    pid_file = tmp_path / "cancelled-before-start.pid"
    marker = "fixture-cancelled-startup-marker"

    with pytest.raises(processes.EngineProcessError) as raised:
        processes.run_contained(
            (sys.executable, str(FIXTURE), "probe", str(pid_file), "$T08_LITERAL"),
            cwd=tmp_path,
            timeout=1.0,
            output_limit=1024,
            cancel_event=cancelled,
        )

    error = raised.value
    _assert_public_error(error, "command_cancelled", marker)
    assert not pid_file.exists()


def test_dual_stream_flood_hits_the_bounded_cap_and_cleans_the_group(
    tmp_path: Path,
) -> None:
    processes = _processes()
    pid_file = tmp_path / "flood.pid"
    started = time.monotonic()

    try:
        with pytest.raises(processes.EngineProcessError) as raised:
            processes.run_contained(
                (sys.executable, str(FIXTURE), "flood-both", str(pid_file)),
                cwd=tmp_path,
                timeout=0.4,
                output_limit=1024,
            )

        error = raised.value
        assert time.monotonic() - started < 0.3
        _assert_public_error(
            error,
            "command_output_too_large",
            "fixture-stdout-flood-marker",
            "fixture-stderr-flood-marker",
        )
        _assert_pid_gone(int(pid_file.read_text("ascii")))
    finally:
        _kill_fixture_group(pid_file)


def test_finite_two_stream_saturation_drains_each_pipe_to_exact_completion(
    tmp_path: Path,
) -> None:
    processes = _processes()
    pid_file = tmp_path / "finite-flood.pid"

    try:
        result = processes.run_contained(
            (
                sys.executable,
                str(FIXTURE),
                "finite-flood-both",
                str(pid_file),
                str(_FINITE_STREAM_BYTES),
            ),
            cwd=tmp_path,
            timeout=3.0,
            output_limit=_FINITE_STREAM_BYTES,
        )

        leader_pid = int(pid_file.read_text("ascii"))
        assert result.returncode == 0
        assert result.identity.leader_pid == leader_pid
        assert result.identity.process_group_id == leader_pid
        assert result.stdout == _finite_stream_output("stdout", "o")
        assert result.stderr == _finite_stream_output("stderr", "e")
        _assert_pid_gone(leader_pid)
    finally:
        _kill_fixture_group(pid_file)


@pytest.mark.parametrize(
    ("mode", "marker"),
    (
        ("invalid-stdout", "fixture-invalid-stdout-marker"),
        ("invalid-stderr", "fixture-invalid-stderr-marker"),
    ),
)
def test_invalid_output_encoding_is_redacted_and_reaped(
    tmp_path: Path, mode: str, marker: str
) -> None:
    processes = _processes()
    pid_file = tmp_path / f"{mode}.pid"

    try:
        with pytest.raises(processes.EngineProcessError) as raised:
            processes.run_contained(
                (sys.executable, str(FIXTURE), mode, str(pid_file)),
                cwd=tmp_path,
                timeout=1.0,
                output_limit=1024,
            )

        _assert_public_error(raised.value, "invalid_output_encoding", marker)
        _assert_pid_gone(int(pid_file.read_text("ascii")))
    finally:
        _kill_fixture_group(pid_file)


def test_nonzero_exit_is_redacted_and_reaped(tmp_path: Path) -> None:
    processes = _processes()
    pid_file = tmp_path / "nonzero.pid"

    try:
        with pytest.raises(processes.EngineProcessError) as raised:
            processes.run_contained(
                (sys.executable, str(FIXTURE), "nonzero", str(pid_file)),
                cwd=tmp_path,
                timeout=1.0,
                output_limit=1024,
            )

        _assert_public_error(
            raised.value, "command_failed", "fixture-nonzero-marker"
        )
        _assert_pid_gone(int(pid_file.read_text("ascii")))
    finally:
        _kill_fixture_group(pid_file)


def test_timeout_reaps_detached_descendant_by_exact_pid(tmp_path: Path) -> None:
    processes = _processes()
    leader_pid_file = tmp_path / "timeout-leader.pid"
    descendant_pid_file = tmp_path / "timeout-descendant.pid"

    try:
        with pytest.raises(processes.EngineProcessError) as raised:
            processes.run_contained(
                (
                    sys.executable,
                    str(FIXTURE),
                    "descendant-and-sleep",
                    str(leader_pid_file),
                    str(descendant_pid_file),
                ),
                cwd=tmp_path,
                timeout=0.4,
                output_limit=1024,
            )

        _assert_public_error(raised.value, "command_timeout")
        _assert_pid_gone(int(leader_pid_file.read_text("ascii")))
        _assert_pid_gone(int(descendant_pid_file.read_text("ascii")))
    finally:
        _kill_fixture_group(leader_pid_file)


def test_normal_exit_reaps_detached_descendant_by_exact_pid(tmp_path: Path) -> None:
    processes = _processes()
    leader_pid_file = tmp_path / "success-leader.pid"
    descendant_pid_file = tmp_path / "success-descendant.pid"

    try:
        result = processes.run_contained(
            (
                sys.executable,
                str(FIXTURE),
                "exit-with-descendant",
                str(leader_pid_file),
                str(descendant_pid_file),
            ),
            cwd=tmp_path,
            timeout=1.0,
            output_limit=1024,
        )

        leader_pid = int(leader_pid_file.read_text("ascii"))
        descendant_pid = int(descendant_pid_file.read_text("ascii"))
        assert result.returncode == 0
        assert result.stdout == "descendant-ok\n"
        assert result.identity.leader_pid == leader_pid
        assert result.identity.process_group_id == leader_pid
        _assert_pid_gone(leader_pid)
        _assert_pid_gone(descendant_pid)
    finally:
        _kill_fixture_group(leader_pid_file)


def test_normal_leader_exit_gives_term_aware_group_descendant_grace(
    tmp_path: Path,
) -> None:
    processes = _processes()
    leader_pid_file = tmp_path / "term-aware-leader.pid"
    descendant_identity_file = tmp_path / "term-aware-descendant.identity"
    descendant_ready_file = tmp_path / "term-aware-descendant.ready"
    cleanup_file = tmp_path / "term-aware-descendant.cleanup"

    try:
        result = processes.run_contained(
            (
                sys.executable,
                str(FIXTURE),
                "exit-with-term-aware-descendant",
                str(leader_pid_file),
                str(descendant_identity_file),
                str(descendant_ready_file),
                str(cleanup_file),
            ),
            cwd=tmp_path,
            timeout=1.0,
            output_limit=1024,
        )

        leader_pid = int(leader_pid_file.read_text("ascii"))
        descendant_pid, descendant_group_id = map(
            int, descendant_identity_file.read_text("ascii").split(":")
        )
        assert result.returncode == 0
        assert result.stdout == "term-aware-descendant-ok\n"
        assert result.identity.leader_pid == leader_pid
        assert result.identity.process_group_id == leader_pid
        assert descendant_group_id == leader_pid
        assert cleanup_file.read_text("ascii") == (
            f"{descendant_pid}:{leader_pid}:{int(signal.SIGTERM)}"
        )
        _assert_pid_gone(leader_pid)
        _assert_pid_gone(descendant_pid)
    finally:
        _kill_fixture_group(leader_pid_file)


def test_selector_creation_failure_prevents_fixture_launch(
    tmp_path: Path, monkeypatch
) -> None:
    processes = _processes()
    pid_file = tmp_path / "selector-never-launched.pid"
    marker = "fixture-selector-initialization-marker"

    class RaisingSelectors:
        class DefaultSelector:
            def __init__(self) -> None:
                raise RuntimeError(marker)

    monkeypatch.setattr(processes, "selectors", RaisingSelectors, raising=False)

    with pytest.raises(processes.EngineProcessError) as raised:
        processes.run_contained(
            (sys.executable, str(FIXTURE), "probe", str(pid_file), "$T08_LITERAL"),
            cwd=tmp_path,
            timeout=1.0,
            output_limit=1024,
        )

    _assert_public_error(raised.value, "command_failed", marker)
    assert not pid_file.exists()


def test_term_resistant_leader_is_killed_and_reaped(tmp_path: Path) -> None:
    processes = _processes()
    leader_pid_file = tmp_path / "term-resistant-leader.pid"

    try:
        with pytest.raises(processes.EngineProcessError) as raised:
            processes.run_contained(
                (
                    sys.executable,
                    str(FIXTURE),
                    "term-resistant-leader",
                    str(leader_pid_file),
                ),
                cwd=tmp_path,
                timeout=0.15,
                output_limit=1024,
            )

        _assert_public_error(raised.value, "command_timeout")
        leader_pid = int(leader_pid_file.read_text("ascii"))
        _assert_pid_reaped(leader_pid)
        _assert_pid_gone(leader_pid)
    finally:
        _kill_fixture_group(leader_pid_file)


def test_pipe_eof_waits_for_non_reaping_leader_completion(tmp_path: Path) -> None:
    processes = _processes()
    leader_pid_file = tmp_path / "pipe-eof-leader.pid"
    completed_file = tmp_path / "pipe-eof-completed"
    term_file = tmp_path / "pipe-eof-term"

    try:
        result = processes.run_contained(
            (
                sys.executable,
                str(FIXTURE),
                "close-pipes-then-finish",
                str(leader_pid_file),
                str(completed_file),
                str(term_file),
            ),
            cwd=tmp_path,
            timeout=1.0,
            output_limit=1024,
        )

        assert result.returncode == 0
        assert result.stdout == ""
        assert result.stderr == ""
        assert completed_file.read_text("ascii") == "completed"
        assert not term_file.exists()
        _assert_pid_gone(int(leader_pid_file.read_text("ascii")))
    finally:
        _kill_fixture_group(leader_pid_file)


def test_lifecycle_signals_bound_group_before_any_leader_reap(
    tmp_path: Path, monkeypatch
) -> None:
    processes = _processes()
    leader_pid = 424242

    class FakeProcess:
        def __init__(self) -> None:
            self.pid = leader_pid
            self.stdout = object()
            self.stderr = object()
            self.returncode: int | None = None
            self.reaped = False
            self.wait_calls = 0

        def poll(self) -> int | None:
            pytest.fail("lifecycle must not poll before group cleanup")

        def wait(self, timeout: float | None = None) -> int:
            self.wait_calls += 1
            self.reaped = True
            self.returncode = 0
            return self.returncode

    class EmptySelector:
        def register(self, *_args, **_kwargs) -> None:
            pass

        def get_map(self) -> dict[object, object]:
            return {}

        def close(self) -> None:
            pass

    class FinishedObserver:
        def close(self) -> None:
            pass

    process = FakeProcess()
    signals: list[signal.Signals] = []
    group_absences = iter((False,))

    monkeypatch.setattr(processes.subprocess, "Popen", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(processes.selectors, "DefaultSelector", EmptySelector)
    monkeypatch.setattr(processes, "_open_exit_observer", lambda _process: FinishedObserver())
    monkeypatch.setattr(processes, "_wait_for_observed_exit", lambda *_args: True)
    monkeypatch.setattr(
        processes,
        "_bind_process_group",
        lambda active_process: leader_pid if active_process is process else None,
        raising=False,
    )
    monkeypatch.setattr(processes, "_close_streams", lambda _process: True)
    monkeypatch.setattr(
        processes,
        "_wait_for_group_absence",
        lambda _group_id, _deadline: next(group_absences),
    )
    monkeypatch.setattr(processes, "_group_exists", lambda _group_id: False)

    def _signal_bound_group(group_id: int, signal_number: signal.Signals) -> bool:
        assert group_id == leader_pid
        assert not process.reaped, "group signal used after the leader was reaped"
        signals.append(signal_number)
        return True

    monkeypatch.setattr(processes, "_signal_group", _signal_bound_group)

    result = processes.run_contained(
        ("validated-engine",),
        cwd=tmp_path,
        timeout=1.0,
        output_limit=1,
    )

    assert result.returncode == 0
    assert signals == [signal.SIGTERM, signal.SIGKILL]
    assert process.reaped
    assert process.wait_calls == 1


def test_unestablished_group_fails_closed_without_group_signal(
    tmp_path: Path, monkeypatch
) -> None:
    processes = _processes()

    class FakeProcess:
        def __init__(self) -> None:
            self.pid = 424243
            self.stdout = object()
            self.stderr = object()
            self.returncode: int | None = None
            self.direct_signals: list[str] = []

        def terminate(self) -> None:
            self.direct_signals.append("terminate")

        def kill(self) -> None:
            self.direct_signals.append("kill")

        def wait(self, timeout: float | None = None) -> int:
            self.returncode = -int(signal.SIGTERM)
            return self.returncode

    class EmptySelector:
        def get_map(self) -> dict[object, object]:
            return {}

        def close(self) -> None:
            pass

    process = FakeProcess()
    group_signals: list[signal.Signals] = []

    monkeypatch.setattr(processes.subprocess, "Popen", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(processes.selectors, "DefaultSelector", EmptySelector)
    monkeypatch.setattr(
        processes,
        "_bind_process_group",
        lambda _active_process: None,
        raising=False,
    )
    monkeypatch.setattr(processes, "_close_streams", lambda _process: True)
    monkeypatch.setattr(
        processes,
        "_wait_for_group_absence",
        lambda _group_id, _deadline: True,
    )
    monkeypatch.setattr(processes, "_group_exists", lambda _group_id: False)

    def _record_group_signal(_group_id: int, signal_number: signal.Signals) -> bool:
        group_signals.append(signal_number)
        return True

    monkeypatch.setattr(processes, "_signal_group", _record_group_signal)

    with pytest.raises(processes.EngineProcessError) as raised:
        processes.run_contained(
            ("validated-engine",),
            cwd=tmp_path,
            timeout=1.0,
            output_limit=1,
        )

    _assert_public_error(raised.value, "command_failed")
    assert group_signals == []
    assert process.direct_signals == ["terminate"]


@pytest.mark.parametrize("interrupt_type", (KeyboardInterrupt, SystemExit))
@pytest.mark.parametrize("attempt", range(_STARTUP_INTERRUPT_REPETITIONS))
def test_selector_base_exception_reaps_group_and_propagates_original_interrupt(
    tmp_path: Path, monkeypatch, interrupt_type: type[BaseException], attempt: int
) -> None:
    processes = _processes()
    leader_pid_file = tmp_path / f"{attempt}-{interrupt_type.__name__}-leader.pid"
    descendant_pid_file = tmp_path / f"{attempt}-{interrupt_type.__name__}-descendant.pid"
    original_selector = processes.selectors.DefaultSelector
    interrupt = interrupt_type("fixture-selector-base-exception")
    observed_pid_contents: list[tuple[str, str]] = []

    class InterruptingSelector:
        def __init__(self) -> None:
            self._selector = original_selector()

        def register(self, *args, **kwargs):
            return self._selector.register(*args, **kwargs)

        def unregister(self, *args, **kwargs):
            return self._selector.unregister(*args, **kwargs)

        def get_map(self):
            return self._selector.get_map()

        def select(self, _timeout: float):
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                if leader_pid_file.exists() and descendant_pid_file.exists():
                    contents = (
                        leader_pid_file.read_text("ascii"),
                        descendant_pid_file.read_text("ascii"),
                    )
                    observed_pid_contents.append(contents)
                    if all(content.isdecimal() for content in contents):
                        raise interrupt
                time.sleep(0)
            pytest.fail("fixture did not start before selector interrupt")

        def close(self) -> None:
            self._selector.close()

    monkeypatch.setattr(processes.selectors, "DefaultSelector", InterruptingSelector)

    try:
        with pytest.raises(interrupt_type) as raised:
            processes.run_contained(
                (
                    sys.executable,
                    "-c",
                    _DELAY_DESCENDANT_PID_WRITE,
                    str(descendant_pid_file),
                    str(FIXTURE),
                    "descendant-and-sleep",
                    str(leader_pid_file),
                    str(descendant_pid_file),
                ),
                cwd=tmp_path,
                timeout=2.0,
                output_limit=1024,
            )

        assert raised.value is interrupt
        assert observed_pid_contents
        assert all(
            content.isdecimal()
            for contents in observed_pid_contents
            for content in contents
        )
        _assert_pid_gone(int(leader_pid_file.read_text("ascii")))
        _assert_pid_gone(int(descendant_pid_file.read_text("ascii")))
    finally:
        _kill_fixture_group(leader_pid_file)


def test_binding_base_exception_reaps_spawned_leader_and_propagates_interrupt(
    tmp_path: Path, monkeypatch
) -> None:
    processes = _processes()
    leader_pid_file = tmp_path / "binding-interrupt-leader.pid"
    spawned_pids: list[int] = []
    original_popen = processes.subprocess.Popen
    interrupt = KeyboardInterrupt("fixture-binding-base-exception")

    def capture_popen(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        spawned_pids.append(process.pid)
        return process

    def raise_interrupt(_process) -> None:
        raise interrupt

    monkeypatch.setattr(processes.subprocess, "Popen", capture_popen)
    monkeypatch.setattr(processes, "_bind_process_group", raise_interrupt)

    try:
        with pytest.raises(KeyboardInterrupt) as raised:
            processes.run_contained(
                (
                    sys.executable,
                    str(FIXTURE),
                    "term-resistant-leader",
                    str(leader_pid_file),
                ),
                cwd=tmp_path,
                timeout=1.0,
                output_limit=1024,
            )

        assert raised.value is interrupt
        assert len(spawned_pids) == 1
        _assert_pid_reaped(spawned_pids[0])
        _assert_pid_gone(spawned_pids[0])
    finally:
        _kill_fixture_group(leader_pid_file)


def test_post_popen_base_exception_reaps_spawned_leader_and_propagates_interrupt(
    tmp_path: Path, monkeypatch
) -> None:
    processes = _processes()
    leader_pid_file = tmp_path / "post-popen-interrupt-leader.pid"
    spawned_pids: list[int] = []
    original_popen = processes.subprocess.Popen
    interrupt = KeyboardInterrupt("fixture-post-popen-base-exception")
    injected = False

    def capture_popen(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        spawned_pids.append(process.pid)
        return process

    def interrupt_after_popen_return(frame, event, _arg):
        nonlocal injected
        if (
            event == "line"
            and not injected
            and spawned_pids
            and frame.f_code is processes.run_contained.__code__
            and frame.f_locals.get("process") is not None
        ):
            injected = True
            raise interrupt
        return interrupt_after_popen_return

    monkeypatch.setattr(processes.subprocess, "Popen", capture_popen)
    previous_trace = sys.gettrace()
    sys.settrace(interrupt_after_popen_return)
    try:
        with pytest.raises(KeyboardInterrupt) as raised:
            processes.run_contained(
                (
                    sys.executable,
                    str(FIXTURE),
                    "term-resistant-leader",
                    str(leader_pid_file),
                ),
                cwd=tmp_path,
                timeout=1.0,
                output_limit=1024,
            )

        assert raised.value is interrupt
        assert injected
        assert len(spawned_pids) == 1
        _assert_pid_reaped(spawned_pids[0])
        _assert_pid_gone(spawned_pids[0])
    finally:
        sys.settrace(previous_trace)
        for pid in spawned_pids:
            _kill_fixture_process_group(pid)


def test_term_grace_base_exception_kills_term_resistant_leader_and_propagates_interrupt(
    tmp_path: Path, monkeypatch
) -> None:
    processes = _processes()
    leader_pid_file = tmp_path / "term-grace-interrupt-leader.pid"
    spawned_pids: list[int] = []
    signals: list[signal.Signals] = []
    original_popen = processes.subprocess.Popen
    original_signal_group = processes._signal_group
    original_wait_for_group_absence = processes._wait_for_group_absence
    interrupt = KeyboardInterrupt("fixture-term-grace-base-exception")
    interrupted = False

    def capture_popen(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        spawned_pids.append(process.pid)
        return process

    def record_group_signal(group_id: int, signal_number: signal.Signals) -> bool:
        signals.append(signal_number)
        return original_signal_group(group_id, signal_number)

    def interrupt_term_grace(group_id: int, deadline: float) -> bool:
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            assert leader_pid_file.exists()
            assert signals == [signal.SIGTERM]
            raise interrupt
        return original_wait_for_group_absence(group_id, deadline)

    monkeypatch.setattr(processes.subprocess, "Popen", capture_popen)
    monkeypatch.setattr(processes, "_signal_group", record_group_signal)
    monkeypatch.setattr(processes, "_wait_for_group_absence", interrupt_term_grace)

    try:
        with pytest.raises(KeyboardInterrupt) as raised:
            processes.run_contained(
                (
                    sys.executable,
                    str(FIXTURE),
                    "term-resistant-leader",
                    str(leader_pid_file),
                ),
                cwd=tmp_path,
                timeout=0.15,
                output_limit=1024,
            )

        assert raised.value is interrupt
        assert interrupted
        assert signals == [signal.SIGTERM, signal.SIGKILL]
        assert len(spawned_pids) == 1
        _assert_pid_reaped(spawned_pids[0])
        _assert_pid_gone(spawned_pids[0])
    finally:
        for pid in spawned_pids:
            _kill_fixture_process_group(pid)


def test_surrogate_argv_fails_closed_before_fixture_launch(tmp_path: Path) -> None:
    processes = _processes()
    pid_file = tmp_path / "surrogate-never-launched.pid"
    marker = "fixture-surrogate-argv-marker-\udcff"

    try:
        with pytest.raises(processes.EngineProcessError) as raised:
            processes.run_contained(
                (sys.executable, str(FIXTURE), "probe", str(pid_file), marker),
                cwd=tmp_path,
                timeout=1.0,
                output_limit=1024,
            )

        _assert_public_error(raised.value, "command_failed", marker)
        assert not pid_file.exists()
    finally:
        _kill_fixture_group(pid_file)
