"""Bounded POSIX process containment for already-validated engine argv."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import selectors
import signal
import subprocess
import threading
import time
from typing import Mapping, NoReturn, Sequence, cast


_SAFE_ENVIRONMENT_NAMES = frozenset({"PATH", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR"})
_CHUNK_SIZE = 8192
_TERM_GRACE_SECONDS = 0.2
_KILL_GRACE_SECONDS = 0.5
_POLL_INTERVAL_SECONDS = 0.01


class EngineProcessError(RuntimeError):
    """A bounded public failure reason with no child diagnostics."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class EngineIdentity:
    """A serializable exact process identity for later reconciliation."""

    leader_pid: int
    process_group_id: int
    started_monotonic_ns: int
    argv_sha256: str

    def to_record(self) -> dict[str, int | str]:
        return {
            "leader_pid": self.leader_pid,
            "process_group_id": self.process_group_id,
            "started_monotonic_ns": self.started_monotonic_ns,
            "argv_sha256": self.argv_sha256,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, int | str]) -> "EngineIdentity":
        return cls(
            leader_pid=cast(int, record["leader_pid"]),
            process_group_id=cast(int, record["process_group_id"]),
            started_monotonic_ns=cast(int, record["started_monotonic_ns"]),
            argv_sha256=cast(str, record["argv_sha256"]),
        )


@dataclass(frozen=True, slots=True)
class EngineResult:
    identity: EngineIdentity
    returncode: int
    stdout: str
    stderr: str


def _filtered_environment(environment: Mapping[str, str] | None) -> dict[str, str]:
    source = os.environ if environment is None else environment
    return {
        name: value
        for name, value in source.items()
        if name in _SAFE_ENVIRONMENT_NAMES and isinstance(value, str)
    }


def _raise_public(reason: str) -> NoReturn:
    raise EngineProcessError(reason)


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


def _wait_for_group_absence(process_group_id: int, deadline: float) -> bool:
    while time.monotonic() < deadline:
        if not _group_exists(process_group_id):
            return True
        time.sleep(_POLL_INTERVAL_SECONDS)
    return not _group_exists(process_group_id)


def _close_streams(process: subprocess.Popen[bytes]) -> bool:
    success = True
    for stream in (process.stdout, process.stderr):
        if stream is None:
            continue
        try:
            stream.close()
        except OSError:
            success = False
    return success


def _cleanup_process_group(process: subprocess.Popen[bytes]) -> bool:
    """Reap the complete session, including descendants after leader exit."""

    process_group_id = process.pid
    term_deadline = time.monotonic() + _TERM_GRACE_SECONDS
    success = _signal_group(process_group_id, signal.SIGTERM)
    try:
        process.wait(timeout=max(0.0, term_deadline - time.monotonic()))
    except subprocess.TimeoutExpired:
        success = False
    except OSError:
        success = False
    if not _wait_for_group_absence(process_group_id, term_deadline):
        success = _signal_group(process_group_id, signal.SIGKILL) and success
        if not _wait_for_group_absence(
            process_group_id, time.monotonic() + _KILL_GRACE_SECONDS
        ):
            success = False
    return _close_streams(process) and success and not _group_exists(process_group_id)


def _snapshot_invocation(
    argv: Sequence[str], cwd: Path, timeout: float, output_limit: int
) -> tuple[tuple[str, ...], str] | None:
    if os.name != "posix":
        return None
    if type(timeout) not in {int, float} or timeout <= 0:
        return None
    if type(output_limit) is not int or output_limit < 0:
        return None
    try:
        command = tuple(argv)
        working_directory = os.fspath(cwd)
    except (TypeError, ValueError):
        return None
    if not command or any(type(argument) is not str or not argument for argument in command):
        return None
    if type(working_directory) is not str:
        return None
    return command, working_directory


def run_contained(
    argv: Sequence[str],
    *,
    cwd: Path,
    timeout: float,
    output_limit: int,
    environment: Mapping[str, str] | None = None,
    cancel_event: threading.Event | None = None,
) -> EngineResult:
    """Run a fixed argv under one bounded POSIX process-group lifecycle."""

    if cancel_event is not None and cancel_event.is_set():
        _raise_public("command_cancelled")
    invocation = _snapshot_invocation(argv, cwd, timeout, output_limit)
    if invocation is None:
        _raise_public("command_failed")
    command, working_directory = invocation
    started_monotonic_ns = time.monotonic_ns()
    deadline = time.monotonic() + timeout

    selector: selectors.BaseSelector | None = None
    try:
        selector = selectors.DefaultSelector()
    except Exception:
        pass
    if selector is None:
        _raise_public("command_failed")
    active_selector = selector

    process: subprocess.Popen[bytes] | None = None
    try:
        process = subprocess.Popen(
            command,
            cwd=working_directory,
            env=_filtered_environment(environment),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            start_new_session=True,
            close_fds=True,
            bufsize=0,
        )
    except OSError:
        active_selector.close()
    except Exception:
        active_selector.close()
    if process is None:
        _raise_public("command_unavailable")
    active_process = process

    reason: str | None = None
    stdout = bytearray()
    stderr = bytearray()
    try:
        if active_process.stdout is None or active_process.stderr is None:
            reason = "command_failed"
        else:
            active_selector.register(active_process.stdout, selectors.EVENT_READ, "stdout")
            active_selector.register(active_process.stderr, selectors.EVENT_READ, "stderr")
            while active_selector.get_map() and reason is None:
                if cancel_event is not None and cancel_event.is_set():
                    reason = "command_cancelled"
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    reason = "command_timeout"
                    break
                events = active_selector.select(remaining)
                if not events:
                    continue
                for key, _ in events:
                    try:
                        chunk = os.read(key.fd, _CHUNK_SIZE)
                    except OSError:
                        reason = "command_failed"
                        break
                    if not chunk:
                        active_selector.unregister(key.fileobj)
                        continue
                    target = stdout if key.data == "stdout" else stderr
                    if len(target) + len(chunk) > output_limit:
                        reason = "command_output_too_large"
                        break
                    target.extend(chunk)
            if reason is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    reason = "command_timeout"
                else:
                    try:
                        active_process.wait(timeout=remaining)
                    except subprocess.TimeoutExpired:
                        reason = "command_timeout"
                    except OSError:
                        reason = "command_failed"
    except Exception:
        reason = "command_failed"
    finally:
        try:
            active_selector.close()
        except Exception:
            reason = reason or "command_failed"

    cleaned = _cleanup_process_group(active_process)
    if not cleaned:
        reason = reason or "command_failed"
    if reason is None and active_process.returncode != 0:
        reason = "command_failed"

    decoded_stdout = ""
    decoded_stderr = ""
    if reason is None:
        try:
            decoded_stdout = bytes(stdout).decode("utf-8", "strict")
            decoded_stderr = bytes(stderr).decode("utf-8", "strict")
        except UnicodeDecodeError:
            reason = "invalid_output_encoding"

    if reason is not None:
        _raise_public(reason)
    command_bytes = b"\0".join(argument.encode("utf-8") for argument in command)
    identity = EngineIdentity(
        leader_pid=active_process.pid,
        process_group_id=active_process.pid,
        started_monotonic_ns=started_monotonic_ns,
        argv_sha256=hashlib.sha256(command_bytes).hexdigest(),
    )
    return EngineResult(
        identity=identity,
        returncode=active_process.returncode,
        stdout=decoded_stdout,
        stderr=decoded_stderr,
    )
