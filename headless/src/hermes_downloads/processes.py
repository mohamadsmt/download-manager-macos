"""Bounded POSIX process containment for already-validated engine argv."""

from __future__ import annotations

import ctypes
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import select
import selectors
import signal
import subprocess
import sys
import threading
import time
from typing import Mapping, NoReturn, Sequence, cast


_SAFE_ENVIRONMENT_NAMES = frozenset({"PATH", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR"})
_CHUNK_SIZE = 8192
_TERM_GRACE_SECONDS = 0.2
_KILL_GRACE_SECONDS = 0.5
_POLL_INTERVAL_SECONDS = 0.01
_PROC_PIDTBSDINFO = 3


class _ProcBsdInfo(ctypes.Structure):
    """Darwin's SDK-declared ``struct proc_bsdinfo`` ABI."""

    _fields_ = [
        ("pbi_flags", ctypes.c_uint32),
        ("pbi_status", ctypes.c_uint32),
        ("pbi_xstatus", ctypes.c_uint32),
        ("pbi_pid", ctypes.c_uint32),
        ("pbi_ppid", ctypes.c_uint32),
        ("pbi_uid", ctypes.c_uint32),
        ("pbi_gid", ctypes.c_uint32),
        ("pbi_ruid", ctypes.c_uint32),
        ("pbi_rgid", ctypes.c_uint32),
        ("pbi_svuid", ctypes.c_uint32),
        ("pbi_svgid", ctypes.c_uint32),
        ("rfu_1", ctypes.c_uint32),
        ("pbi_comm", ctypes.c_char * 16),
        ("pbi_name", ctypes.c_char * 32),
        ("pbi_nfiles", ctypes.c_uint32),
        ("pbi_pgid", ctypes.c_uint32),
        ("pbi_pjobc", ctypes.c_uint32),
        ("e_tdev", ctypes.c_uint32),
        ("e_tpgid", ctypes.c_uint32),
        ("pbi_nice", ctypes.c_int32),
        ("pbi_start_tvsec", ctypes.c_uint64),
        ("pbi_start_tvusec", ctypes.c_uint64),
    ]


@dataclass(frozen=True, slots=True)
class _DarwinProcessSnapshot:
    leader_pid: int
    process_group_id: int
    owner_uid: int
    started_unix_us: int


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
class ProcessBirthIdentity:
    """A Darwin-bound identity that distinguishes a reused PID from its prior process."""

    leader_pid: int
    process_group_id: int
    session_id: int
    owner_uid: int
    started_unix_us: int
    argv_sha256: str

    def to_record(self) -> dict[str, int | str]:
        return {
            "leader_pid": self.leader_pid,
            "process_group_id": self.process_group_id,
            "session_id": self.session_id,
            "owner_uid": self.owner_uid,
            "started_unix_us": self.started_unix_us,
            "argv_sha256": self.argv_sha256,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, int | str]) -> "ProcessBirthIdentity":
        required = {
            "leader_pid",
            "process_group_id",
            "session_id",
            "owner_uid",
            "started_unix_us",
            "argv_sha256",
        }
        if set(record) != required:
            raise ValueError("invalid process-birth record")
        integer_names = required - {"argv_sha256"}
        values: dict[str, int] = {}
        for name in integer_names:
            value = record[name]
            if type(value) is not int or value < 0:
                raise ValueError("invalid process-birth record")
            values[name] = value
        if any(values[name] == 0 for name in integer_names - {"owner_uid"}):
            raise ValueError("invalid process-birth record")
        argv_sha256 = record["argv_sha256"]
        if (
            type(argv_sha256) is not str
            or len(argv_sha256) != 64
            or any(character not in "0123456789abcdef" for character in argv_sha256)
        ):
            raise ValueError("invalid process-birth record")
        return cls(argv_sha256=argv_sha256, **values)


def _read_darwin_process_snapshot(leader_pid: int) -> _DarwinProcessSnapshot | None:
    if (
        sys.platform != "darwin"
        or leader_pid <= 0
        or ctypes.sizeof(_ProcBsdInfo) != 136
    ):
        return None
    try:
        library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        proc_pidinfo = library.proc_pidinfo
        proc_pidinfo.argtypes = (
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        )
        proc_pidinfo.restype = ctypes.c_int
        info = _ProcBsdInfo()
        bytes_read = proc_pidinfo(
            leader_pid,
            _PROC_PIDTBSDINFO,
            0,
            ctypes.byref(info),
            ctypes.sizeof(info),
        )
    except (AttributeError, OSError):
        return None
    if (
        bytes_read != ctypes.sizeof(info)
        or info.pbi_pid != leader_pid
        or info.pbi_pgid <= 0
        or info.pbi_start_tvusec >= 1_000_000
    ):
        return None
    return _DarwinProcessSnapshot(
        leader_pid=info.pbi_pid,
        process_group_id=info.pbi_pgid,
        owner_uid=info.pbi_uid,
        started_unix_us=(info.pbi_start_tvsec * 1_000_000) + info.pbi_start_tvusec,
    )


def _read_stable_darwin_process_identity(
    leader_pid: int,
) -> tuple[_DarwinProcessSnapshot, int] | None:
    first = _read_darwin_process_snapshot(leader_pid)
    if first is None:
        return None
    try:
        process_group_id = os.getpgid(leader_pid)
        session_id = os.getsid(leader_pid)
    except OSError:
        return None
    second = _read_darwin_process_snapshot(leader_pid)
    if (
        second != first
        or process_group_id != first.process_group_id
        or session_id <= 0
    ):
        return None
    return first, session_id


def capture_process_birth(identity: EngineIdentity) -> ProcessBirthIdentity | None:
    """Capture a fresh Darwin session identity without signaling or reaping it."""

    stable_identity = _read_stable_darwin_process_identity(identity.leader_pid)
    if stable_identity is None:
        return None
    snapshot, session_id = stable_identity
    if (
        identity.process_group_id != identity.leader_pid
        or session_id != identity.leader_pid
        or snapshot.process_group_id != identity.process_group_id
        or snapshot.owner_uid != os.geteuid()
    ):
        return None
    return ProcessBirthIdentity(
        leader_pid=identity.leader_pid,
        process_group_id=identity.process_group_id,
        session_id=session_id,
        owner_uid=snapshot.owner_uid,
        started_unix_us=snapshot.started_unix_us,
        argv_sha256=identity.argv_sha256,
    )


def is_current_process_birth(identity: ProcessBirthIdentity) -> bool:
    """Return whether the exact live Darwin process still matches this identity."""

    if not isinstance(identity, ProcessBirthIdentity) or identity.owner_uid != os.geteuid():
        return False
    stable_identity = _read_stable_darwin_process_identity(identity.leader_pid)
    if stable_identity is None:
        return False
    snapshot, session_id = stable_identity
    return (
        snapshot.leader_pid == identity.leader_pid
        and snapshot.process_group_id == identity.process_group_id
        and session_id == identity.session_id
        and snapshot.owner_uid == identity.owner_uid
        and snapshot.started_unix_us == identity.started_unix_us
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


def _bind_process_group(process: subprocess.Popen[bytes]) -> int | None:
    """Bind the fresh session before any operation can reap its leader."""

    try:
        process_group_id = os.getpgid(process.pid)
        session_id = os.getsid(process.pid)
    except OSError:
        return None
    if process_group_id != process.pid or session_id != process.pid:
        return None
    return process_group_id


def _open_exit_observer(process: subprocess.Popen[bytes]):
    """Observe leader exit without consuming its wait status on macOS."""

    observer = None
    try:
        observer = select.kqueue()
        observer.control(
            [
                select.kevent(
                    process.pid,
                    filter=select.KQ_FILTER_PROC,
                    flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_ONESHOT,
                    fflags=select.KQ_NOTE_EXIT,
                )
            ],
            0,
            0,
        )
    except (AttributeError, OSError):
        if observer is not None:
            try:
                observer.close()
            except (AttributeError, OSError):
                pass
        return None
    return observer


def _wait_for_observed_exit(observer, deadline: float) -> bool:
    try:
        return bool(observer.control(None, 1, max(0.0, deadline - time.monotonic())))
    except OSError:
        return False


def _close_streams(process: subprocess.Popen[bytes]) -> bool:
    success = True
    for stream in (process.stdout, process.stderr):
        if stream is None:
            continue
        try:
            stream.close()
        except (OSError, AttributeError):
            success = False
    return success


def _wait_for_leader(process: subprocess.Popen[bytes], deadline: float) -> bool:
    try:
        process.wait(timeout=max(0.0, deadline - time.monotonic()))
    except subprocess.TimeoutExpired:
        return False
    except OSError:
        return False
    return True


def _cleanup_unbound_process(process: subprocess.Popen[bytes]) -> bool:
    """Terminate only the known leader when session ownership was not proven."""

    try:
        process.terminate()
    except (OSError, AttributeError):
        return False
    if _wait_for_leader(process, time.monotonic() + _TERM_GRACE_SECONDS):
        _close_streams(process)
        return False
    try:
        process.kill()
    except (OSError, AttributeError):
        _close_streams(process)
        return False
    _wait_for_leader(process, time.monotonic() + _KILL_GRACE_SECONDS)
    _close_streams(process)
    return False


def _cleanup_process_group(process: subprocess.Popen[bytes], process_group_id: int | None) -> bool:
    """Signal an owned group before reaping its leader, then prove final absence."""

    if process_group_id is None:
        return _cleanup_unbound_process(process)

    term_deadline = time.monotonic() + _TERM_GRACE_SECONDS
    _signal_group(process_group_id, signal.SIGTERM)
    interruption: BaseException | None = None
    try:
        group_absent_after_term = _wait_for_group_absence(
            process_group_id, term_deadline
        )
    except BaseException as raised:
        interruption = raised
        group_absent_after_term = False
    if not group_absent_after_term:
        _signal_group(process_group_id, signal.SIGKILL)

    # No group signal is permitted after this wait: process.pid can be recycled once reaped.
    leader_reaped = _wait_for_leader(process, time.monotonic() + _KILL_GRACE_SECONDS)
    if _group_exists(process_group_id):
        final_absence = _wait_for_group_absence(
            process_group_id, time.monotonic() + _KILL_GRACE_SECONDS
        )
    else:
        final_absence = True
    streams_closed = _close_streams(process)
    if interruption is not None:
        raise interruption
    # macOS can report transient EPERM after a successful signal; final absence is authoritative.
    return leader_reaped and final_absence and streams_closed


def _snapshot_invocation(
    argv: Sequence[str], cwd: Path, timeout: float, output_limit: int
) -> tuple[tuple[str, ...], str, bytes] | None:
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
    try:
        command_bytes = b"\0".join(argument.encode("utf-8", "strict") for argument in command)
    except UnicodeEncodeError:
        return None
    return command, working_directory, command_bytes


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
    command, working_directory, command_bytes = invocation
    started_monotonic_ns = time.monotonic_ns()
    deadline = time.monotonic() + timeout

    active_selector: selectors.BaseSelector | None = None
    try:
        active_selector = selectors.DefaultSelector()
    except Exception:
        pass
    if active_selector is None:
        _raise_public("command_failed")

    process: subprocess.Popen[bytes] | None = None
    process_group_id: int | None = None
    reason: str | None = None
    stdout = bytearray()
    stderr = bytearray()
    cleaned = False
    exit_observer = None
    try:
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
        except (OSError, ValueError):
            pass
        except Exception:
            pass
        if process is not None:
            process_group_id = _bind_process_group(process)
            if process_group_id is None:
                reason = "command_failed"
            else:
                exit_observer = _open_exit_observer(process)
                if exit_observer is None:
                    reason = "command_failed"
            if reason is None and (process.stdout is None or process.stderr is None):
                reason = "command_failed"
            if reason is None:
                assert process.stdout is not None
                assert process.stderr is not None
                active_selector.register(process.stdout, selectors.EVENT_READ, "stdout")
                active_selector.register(process.stderr, selectors.EVENT_READ, "stderr")
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
                if reason is None and not _wait_for_observed_exit(exit_observer, deadline):
                    reason = "command_timeout"
    except Exception:
        if process is not None:
            reason = "command_failed"
    finally:
        try:
            active_selector.close()
        except Exception:
            if process is not None:
                reason = reason or "command_failed"
        finally:
            try:
                if exit_observer is not None:
                    exit_observer.close()
            except Exception:
                if process is not None:
                    reason = reason or "command_failed"
            finally:
                if process is not None:
                    cleaned = _cleanup_process_group(process, process_group_id)

    if process is None:
        _raise_public("command_unavailable")
    if not cleaned:
        reason = reason or "command_failed"
    if reason is None and process.returncode != 0:
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
    if process_group_id is None:
        _raise_public("command_failed")
    identity = EngineIdentity(
        leader_pid=process.pid,
        process_group_id=process_group_id,
        started_monotonic_ns=started_monotonic_ns,
        argv_sha256=hashlib.sha256(command_bytes).hexdigest(),
    )
    return EngineResult(
        identity=identity,
        returncode=process.returncode,
        stdout=decoded_stdout,
        stderr=decoded_stderr,
    )
