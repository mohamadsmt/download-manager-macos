"""Worker-owned aria2 control for direct downloads.

This module only controls aria2 over its authenticated loopback RPC endpoint.  It
never implements HTTP transfer itself and never decides queue admission.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import tempfile
import time
from typing import Any, Final, cast

from hermes_downloads.models import Admission
from hermes_downloads.network import SourceURL
from hermes_downloads.paths import DestinationIntent
from hermes_downloads.processes import EngineIdentity

__all__ = [
    "DirectAria2Controller",
    "DirectAdmissionError",
    "DirectEngineError",
    "DirectTransfer",
    "DirectTransferError",
    "StaleGenerationError",
]

_DEFAULT_ARIA2C: Final = Path("/opt/homebrew/bin/aria2c")
_MAX_RPC_RESPONSE_BYTES: Final = 64 * 1024
_RPC_READY_TIMEOUT_SECONDS: Final = 3.0
_RPC_TIMEOUT_SECONDS: Final = 1.0
_GROUP_STOP_GRACE_SECONDS: Final = 0.5
_GROUP_KILL_GRACE_SECONDS: Final = 0.5
_POLL_INTERVAL_SECONDS: Final = 0.01
_MIN_SPLIT_SIZE: Final = "1M"
_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SHA256: Final = re.compile(r"[0-9a-f]{64}\Z")
_GID: Final = re.compile(r"[0-9A-Fa-f]{16}\Z")
_SAFE_ENVIRONMENT_NAMES: Final = frozenset(
    {"PATH", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR"}
)
_TERMINAL_STATUSES: Final = frozenset({"complete", "error", "removed"})


class DirectEngineError(RuntimeError):
    """A bounded aria2 lifecycle or RPC failure with no engine diagnostics."""


class DirectTransferError(DirectEngineError):
    """aria2 could not produce a hash-verified direct payload."""


class DirectAdmissionError(DirectTransferError):
    """A worker attempted engine admission while an independent queue gate was shut."""


class StaleGenerationError(DirectTransferError):
    """A callback did not match the exact currently-owned GID and generation."""


@dataclass(frozen=True, slots=True)
class DirectTransfer:
    """A readback of one aria2 direct transfer, never authorization itself."""

    job_id: str
    generation: int
    gid: str
    status: str
    total_length: int
    completed_length: int
    partial_path: Path
    hash_verified: bool


@dataclass(frozen=True, slots=True)
class _TrackedTransfer:
    job_id: str
    generation: int
    gid: str
    source: SourceURL
    destination: DestinationIntent
    expected_sha256: str


class DirectAria2Controller:
    """Own one blank, loopback-only aria2 daemon for a single worker.

    ``add_paused`` and ``resume`` require the caller's current ``Admission``.
    This controller never schedules, resumes, or reconstructs downloads on its
    own; a restart starts a new daemon with no session input and no GID mapping.
    """

    def __init__(
        self,
        *,
        executable: str | os.PathLike[str] = _DEFAULT_ARIA2C,
        runtime_root: str | os.PathLike[str],
        max_concurrent_downloads: int = 1,
        split: int = 4,
        max_connection_per_server: int = 4,
    ) -> None:
        self._executable = _require_executable(executable)
        self._runtime_root = _require_absolute_path(runtime_root, "runtime_root")
        self._max_concurrent_downloads = _require_positive(
            max_concurrent_downloads, "max_concurrent_downloads"
        )
        self._split = _require_positive(split, "split")
        self._max_connection_per_server = _require_connection_limit(
            max_connection_per_server
        )
        self._process: subprocess.Popen[bytes] | None = None
        self._identity: EngineIdentity | None = None
        self._port: int | None = None
        self._secret: str | None = None
        self._private_runtime_path: Path | None = None
        self._private_config_path: Path | None = None
        self._launch_argv: tuple[str, ...] = ()
        self._by_job_id: dict[str, _TrackedTransfer] = {}
        self._by_gid: dict[str, _TrackedTransfer] = {}

    def __enter__(self) -> DirectAria2Controller:
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def engine_identity(self) -> EngineIdentity | None:
        """Return the exact identity of the currently-owned daemon, if any."""

        return self._identity

    @property
    def launch_argv(self) -> tuple[str, ...]:
        """Return the daemon argv, which intentionally contains no RPC secret."""

        return self._launch_argv

    @property
    def private_runtime_path(self) -> Path:
        """Return the owner-only transient runtime directory while it is owned."""

        if self._private_runtime_path is None:
            raise DirectEngineError("aria2 is not running")
        return self._private_runtime_path

    @property
    def private_config_path(self) -> Path:
        """Return the owner-only configuration pathname while it is owned."""

        if self._private_config_path is None:
            raise DirectEngineError("aria2 is not running")
        return self._private_config_path

    @property
    def active_job_ids(self) -> tuple[str, ...]:
        """Return only in-memory GID ownership; it is cleared on every restart."""

        return tuple(self._by_job_id)

    def start(self) -> EngineIdentity:
        """Start a fresh, authenticated aria2 daemon with no session resurrection."""

        if self._process is not None:
            raise DirectEngineError("aria2 is already running")
        if self._private_runtime_path is not None or self._private_config_path is not None:
            raise DirectEngineError("aria2 private runtime cleanup is pending")
        runtime_path: Path | None = None
        process: subprocess.Popen[bytes] | None = None
        process_group_id: int | None = None
        try:
            runtime_path, config_path, secret = self._create_private_config()
            port = _reserve_loopback_port()
            argv = self._build_argv(config_path, port)
            process = subprocess.Popen(
                argv,
                cwd=str(runtime_path),
                env=_filtered_environment(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                shell=False,
                start_new_session=True,
                close_fds=True,
            )
            self._process = process
            self._port = port
            self._secret = secret
            self._launch_argv = argv
            process_group_id = _bind_process_group(process)
            if process_group_id is None:
                raise DirectEngineError("aria2 containment failed")
            identity = EngineIdentity(
                leader_pid=process.pid,
                process_group_id=process_group_id,
                started_monotonic_ns=time.monotonic_ns(),
                argv_sha256=_argv_sha256(argv),
            )
            self._identity = identity
            self._wait_for_rpc_ready()
            return identity
        except (OSError, ValueError):
            stopped, interruption = _stop_process(process, process_group_id)
            if stopped:
                self._clear_stopped_process_state()
                self._cleanup_owned_private_runtime(preserve_primary=True)
            if interruption is not None:
                raise interruption
            if not stopped:
                raise DirectEngineError("aria2 containment failed") from None
            raise DirectEngineError("aria2 could not start") from None
        except BaseException:
            stopped, _cleanup_interruption = _stop_process(process, process_group_id)
            if stopped:
                self._clear_stopped_process_state()
                self._cleanup_owned_private_runtime(preserve_primary=True)
            raise

    def close(self) -> None:
        """Stop the owned daemon and erase its secret-bearing temporary config."""

        stopped, interruption = self._stop_owned_process()
        if stopped:
            self._clear_mappings()
            self._clear_stopped_process_state()
            self._cleanup_owned_private_runtime(
                preserve_primary=interruption is not None
            )
        if interruption is not None:
            raise interruption
        if not stopped:
            raise DirectEngineError("aria2 containment failed")

    def restart(self) -> EngineIdentity:
        """Stop and discard all engine state before starting a blank daemon."""

        self.close()
        return self.start()

    def gid_for_job(self, job_id: str) -> str | None:
        """Return the exact aria2 GID currently owned by a job."""

        _require_identifier(job_id, "job_id")
        transfer = self._by_job_id.get(job_id)
        return None if transfer is None else transfer.gid

    def job_for_gid(self, gid: str) -> str | None:
        """Return the exact job currently owned by an aria2 GID."""

        _require_gid(gid)
        transfer = self._by_gid.get(gid)
        return None if transfer is None else transfer.job_id

    def add_paused(
        self,
        *,
        job_id: str,
        generation: int,
        source: SourceURL,
        destination: DestinationIntent,
        expected_sha256: str,
        admission: Admission,
    ) -> DirectTransfer:
        """Add one admitted transfer with aria2 paused before any body request."""

        _require_admission(admission)
        _require_identifier(job_id, "job_id")
        _require_generation(generation)
        _require_source(source)
        _require_destination(destination, job_id)
        _require_sha256(expected_sha256)
        self._require_running()
        if job_id in self._by_job_id:
            raise DirectTransferError("job already has an aria2 GID")
        options = {
            "dir": str(destination.incomplete_dir),
            "out": destination.partial_path.name,
            "pause": "true",
            "continue": "true",
            "allow-overwrite": "false",
            "auto-file-renaming": "false",
            "checksum": f"sha-256={expected_sha256}",
            "check-integrity": "true",
        }
        gid = self._rpc("aria2.addUri", [[source.raw_url.decode("utf-8")], options])
        _require_gid(gid)
        transfer = _TrackedTransfer(
            job_id=job_id,
            generation=generation,
            gid=gid,
            source=source,
            destination=destination,
            expected_sha256=expected_sha256,
        )
        self._by_job_id[job_id] = transfer
        self._by_gid[gid] = transfer
        try:
            return self._readback(transfer)
        except BaseException:
            self._by_job_id.pop(job_id, None)
            self._by_gid.pop(gid, None)
            self._discard_unreadable_gid(gid)
            raise

    def resume(
        self, *, job_id: str, generation: int, admission: Admission
    ) -> DirectTransfer:
        """Explicitly unpause one still-admitted job, then read its actual state."""

        _require_admission(admission)
        transfer = self._current_transfer(job_id, generation)
        result = self._rpc("aria2.unpause", [transfer.gid])
        if result != transfer.gid:
            raise DirectTransferError("aria2 did not acknowledge resume")
        return self._readback(transfer)

    def readback(self, *, job_id: str, generation: int, gid: str) -> DirectTransfer:
        """Read one exact mapped engine state after validating callback identity."""

        return self._readback(self._callback_transfer(job_id, generation, gid))

    def observe_callback(
        self, *, job_id: str, generation: int, gid: str
    ) -> DirectTransfer:
        """Accept only a current-generation engine callback and read back its state."""

        return self.readback(job_id=job_id, generation=generation, gid=gid)

    def wait_for_terminal(
        self, *, job_id: str, generation: int, timeout: float
    ) -> DirectTransfer:
        """Wait for aria2 terminal state and independently verify expected SHA-256."""

        if type(timeout) not in {int, float} or timeout <= 0:
            raise TypeError("timeout must be a positive number")
        transfer = self._current_transfer(job_id, generation)
        deadline = time.monotonic() + float(timeout)
        while True:
            state = self._readback(transfer)
            if state.status == "complete":
                return self._verify_completed_hash(state, transfer)
            if state.status in _TERMINAL_STATUSES:
                raise DirectTransferError("aria2 transfer failed")
            if time.monotonic() >= deadline:
                raise DirectTransferError("aria2 transfer timed out")
            time.sleep(_POLL_INTERVAL_SECONDS)

    def _current_transfer(self, job_id: str, generation: int) -> _TrackedTransfer:
        _require_identifier(job_id, "job_id")
        _require_generation(generation)
        transfer = self._by_job_id.get(job_id)
        if transfer is None or transfer.generation != generation:
            raise StaleGenerationError("stale direct transfer")
        return transfer

    def _callback_transfer(
        self, job_id: str, generation: int, gid: str
    ) -> _TrackedTransfer:
        transfer = self._current_transfer(job_id, generation)
        _require_gid(gid)
        if transfer.gid != gid or self._by_gid.get(gid) is not transfer:
            raise StaleGenerationError("stale direct callback")
        return transfer

    def _readback(self, transfer: _TrackedTransfer) -> DirectTransfer:
        response = self._rpc(
            "aria2.tellStatus",
            [
                transfer.gid,
                ["status", "totalLength", "completedLength"],
            ],
        )
        if type(response) is not dict:
            raise DirectTransferError("aria2 returned an invalid state")
        status = response.get("status")
        if type(status) is not str:
            raise DirectTransferError("aria2 returned an invalid state")
        total_length = _parse_counter(response.get("totalLength"))
        completed_length = _parse_counter(response.get("completedLength"))
        return DirectTransfer(
            job_id=transfer.job_id,
            generation=transfer.generation,
            gid=transfer.gid,
            status=status,
            total_length=total_length,
            completed_length=completed_length,
            partial_path=transfer.destination.partial_path,
            hash_verified=False,
        )

    def _verify_completed_hash(
        self, state: DirectTransfer, transfer: _TrackedTransfer
    ) -> DirectTransfer:
        try:
            details = state.partial_path.stat()
            if not stat.S_ISREG(details.st_mode):
                raise OSError
            with state.partial_path.open("rb") as payload:
                actual_sha256 = hashlib.file_digest(payload, "sha256").hexdigest()
        except OSError:
            raise DirectTransferError("aria2 output is unavailable") from None
        if actual_sha256 != transfer.expected_sha256:
            raise DirectTransferError("aria2 hash verification failed")
        return replace(state, hash_verified=True)

    def _create_private_config(self) -> tuple[Path, Path, str]:
        self._runtime_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        runtime_path = Path(
            tempfile.mkdtemp(prefix="aria2-", dir=str(self._runtime_root))
        )
        config_path = runtime_path / "aria2.conf"
        self._own_private_runtime(runtime_path, config_path)
        os.chmod(runtime_path, 0o700)
        secret = secrets.token_urlsafe(32)
        _write_private_file(config_path, f"rpc-secret={secret}\n".encode("ascii"))
        return runtime_path, config_path, secret

    def _build_argv(self, config_path: Path, port: int) -> tuple[str, ...]:
        # aria2 1.37's --no-conf disables even an explicit --conf-path.  The
        # explicit owner-only config replaces the ambient config while keeping
        # the secret out of argv.
        return (
            str(self._executable),
            f"--conf-path={config_path}",
            "--enable-rpc=true",
            f"--rpc-listen-port={port}",
            "--rpc-listen-all=false",
            "--no-netrc=true",
            "--file-allocation=none",
            "--check-certificate=true",
            f"--max-concurrent-downloads={self._max_concurrent_downloads}",
            f"--split={self._split}",
            f"--max-connection-per-server={self._max_connection_per_server}",
            f"--min-split-size={_MIN_SPLIT_SIZE}",
            "--pause=true",
            "--continue=true",
            "--allow-overwrite=false",
            "--auto-file-renaming=false",
            "--auto-save-interval=0",
            "--max-tries=1",
        )

    def _wait_for_rpc_ready(self) -> None:
        deadline = time.monotonic() + _RPC_READY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                response = self._rpc("aria2.getVersion", [])
            except DirectEngineError:
                time.sleep(_POLL_INTERVAL_SECONDS)
                continue
            if type(response) is dict:
                return
        raise DirectEngineError("aria2 RPC did not become ready")

    def _rpc(self, method: str, params: list[Any]) -> Any:
        process, port, secret = self._require_running()
        request = {
            "jsonrpc": "2.0",
            "id": "hermes-downloads",
            "method": method,
            "params": [f"token:{secret}", *params],
        }
        try:
            encoded = json.dumps(request, separators=(",", ":")).encode("utf-8")
            connection = http.client.HTTPConnection(
                "127.0.0.1", port, timeout=_RPC_TIMEOUT_SECONDS
            )
            try:
                connection.request(
                    "POST",
                    "/jsonrpc",
                    body=encoded,
                    headers={"Content-Type": "application/json"},
                )
                response = connection.getresponse()
                body = response.read(_MAX_RPC_RESPONSE_BYTES + 1)
            finally:
                connection.close()
            if response.status != 200 or len(body) > _MAX_RPC_RESPONSE_BYTES:
                raise DirectEngineError("aria2 RPC failed")
            parsed = json.loads(body)
            if type(parsed) is not dict or "error" in parsed or "result" not in parsed:
                raise DirectEngineError("aria2 RPC failed")
            return parsed["result"]
        except (OSError, ValueError, json.JSONDecodeError, http.client.HTTPException):
            raise DirectEngineError("aria2 RPC failed") from None

    def _discard_unreadable_gid(self, gid: str) -> None:
        try:
            self._rpc("aria2.forceRemove", [gid])
        except DirectEngineError:
            return

    def _require_running(self) -> tuple[subprocess.Popen[bytes], int, str]:
        if self._process is None or self._port is None or self._secret is None:
            raise DirectEngineError("aria2 is not running")
        return self._process, self._port, self._secret

    def _stop_owned_process(self) -> tuple[bool, BaseException | None]:
        process, identity = self._process, self._identity
        if process is None:
            return True, None
        interruption: BaseException | None = None
        try:
            self._rpc("aria2.forceShutdown", [])
        except DirectEngineError:
            pass
        except BaseException as raised:
            interruption = raised
        stopped, cleanup_interruption = _stop_process(
            process, None if identity is None else identity.process_group_id
        )
        return stopped, interruption or cleanup_interruption

    def _clear_mappings(self) -> None:
        self._by_job_id.clear()
        self._by_gid.clear()

    def _clear_stopped_process_state(self) -> None:
        self._process = None
        self._identity = None
        self._port = None
        self._secret = None
        self._launch_argv = ()

    def _own_private_runtime(self, runtime_path: Path, config_path: Path) -> None:
        self._private_runtime_path = runtime_path
        self._private_config_path = config_path

    def _cleanup_owned_private_runtime(self, *, preserve_primary: bool) -> None:
        runtime_path = self._private_runtime_path
        if runtime_path is None:
            return
        try:
            _remove_private_runtime(runtime_path)
        except BaseException:
            if preserve_primary:
                # Cleanup failure cannot mask a primary exception.  The paths
                # stay owned so close() can retry deletion.
                return
            raise
        self._clear_private_runtime_state()

    def _clear_private_runtime_state(self) -> None:
        self._private_runtime_path = None
        self._private_config_path = None

    def _clear_runtime_state(self) -> None:
        self._clear_stopped_process_state()
        self._clear_private_runtime_state()


def _require_executable(value: str | os.PathLike[str]) -> Path:
    try:
        executable = Path(os.fspath(value))
    except TypeError:
        raise TypeError("executable must be a filesystem path") from None
    if not executable.is_absolute() or not executable.is_file() or not os.access(executable, os.X_OK):
        raise DirectEngineError("aria2 executable is unavailable")
    return executable


def _require_absolute_path(value: str | os.PathLike[str], name: str) -> Path:
    try:
        path = Path(os.fspath(value))
    except TypeError:
        raise TypeError(f"{name} must be a filesystem path") from None
    if not path.is_absolute():
        raise ValueError(f"{name} must be absolute")
    return path


def _require_positive(value: object, name: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _require_connection_limit(value: object) -> int:
    value = _require_positive(value, "max_connection_per_server")
    if value > 16:
        raise ValueError("max_connection_per_server exceeds aria2's limit")
    return value


def _require_identifier(value: object, name: str) -> str:
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} must be a nonblank identifier")
    return value


def _require_generation(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("generation must be a nonnegative integer")
    return value


def _require_gid(value: object) -> str:
    if type(value) is not str or _GID.fullmatch(value) is None:
        raise DirectTransferError("aria2 returned an invalid GID")
    return value


def _require_sha256(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ValueError("expected_sha256 must be a lowercase SHA-256 digest")
    return value


def _require_admission(admission: object) -> Admission:
    if type(admission) is not Admission:
        raise TypeError("admission must be an Admission")
    if not admission.allowed:
        raise DirectAdmissionError("queue admission is closed")
    return admission


def _require_source(source: object) -> SourceURL:
    if type(source) is not SourceURL:
        raise TypeError("source must be a validated SourceURL")
    return source


def _require_destination(destination: object, job_id: str) -> DestinationIntent:
    if type(destination) is not DestinationIntent:
        raise TypeError("destination must be a DestinationIntent")
    canonical_root = Path.home() / "Downloads" / "Hermes"
    expected_incomplete_dir = canonical_root / ".incomplete" / job_id
    if (
        destination.root != canonical_root
        or destination.job_id != job_id
        or destination.partial_path.parent != destination.incomplete_dir
        or destination.partial_path.name != destination.filename
        or destination.incomplete_dir != expected_incomplete_dir
        or destination.partial_path != expected_incomplete_dir / destination.filename
        or not destination.partial_path.is_absolute()
        or not destination.final_path.is_absolute()
    ):
        raise DirectTransferError("destination is not a job-owned T05 path")
    return destination


def _parse_counter(value: object) -> int:
    if type(value) is not str or not value.isascii() or not value.isdecimal():
        raise DirectTransferError("aria2 returned an invalid state")
    return int(value)


def _write_private_file(path: Path, content: bytes) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
    )
    try:
        offset = 0
        while offset < len(content):
            written = os.write(descriptor, content[offset:])
            if written <= 0:
                raise OSError("could not write private aria2 configuration")
            offset += written
        os.fchmod(descriptor, 0o600)
    finally:
        os.close(descriptor)


def _remove_private_runtime(path: Path) -> None:
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        return
    except OSError:
        raise DirectEngineError("aria2 private runtime cleanup failed") from None


def _reserve_loopback_port() -> int:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    finally:
        listener.close()
    if type(port) is not int or not 1024 <= port <= 65535:
        raise DirectEngineError("could not reserve an aria2 RPC port")
    return port


def _filtered_environment() -> dict[str, str]:
    return {
        name: value
        for name, value in os.environ.items()
        if name in _SAFE_ENVIRONMENT_NAMES and type(value) is str
    }


def _argv_sha256(argv: tuple[str, ...]) -> str:
    return hashlib.sha256(
        b"\0".join(argument.encode("utf-8", "strict") for argument in argv)
    ).hexdigest()


def _bind_process_group(process: subprocess.Popen[bytes]) -> int | None:
    try:
        process_group_id = os.getpgid(process.pid)
        session_id = os.getsid(process.pid)
    except OSError:
        return None
    if process_group_id != process.pid or session_id != process.pid:
        return None
    return process_group_id


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


def _wait_for_group_absence(process_group_id: int, deadline: float) -> bool:
    while time.monotonic() < deadline:
        if not _group_exists(process_group_id):
            return True
        time.sleep(_POLL_INTERVAL_SECONDS)
    return not _group_exists(process_group_id)


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


def _wait_for_leader(process: subprocess.Popen[bytes], deadline: float) -> bool:
    try:
        process.wait(timeout=max(0.0, deadline - time.monotonic()))
    except (OSError, subprocess.TimeoutExpired):
        return False
    return True


def _stop_process(
    process: subprocess.Popen[bytes] | None, process_group_id: int | None
) -> tuple[bool, BaseException | None]:
    if process is None:
        return True, None
    if process_group_id is None:
        try:
            return _stop_unbound_process(process), None
        except BaseException as raised:
            return False, raised
    try:
        result = _stop_process_group(process, process_group_id)
    except BaseException as raised:
        return False, raised
    if type(result) is tuple:
        return result
    return result, None


def _stop_unbound_process(process: subprocess.Popen[bytes]) -> bool:
    try:
        process.terminate()
    except OSError:
        return False
    if _wait_for_leader(process, time.monotonic() + _GROUP_STOP_GRACE_SECONDS):
        return True
    try:
        process.kill()
    except OSError:
        return False
    return _wait_for_leader(process, time.monotonic() + _GROUP_KILL_GRACE_SECONDS)


def _stop_process_group(
    process: subprocess.Popen[bytes], process_group_id: int
) -> tuple[bool, BaseException | None]:
    """Contain the bound group before reaping its leader or reusing its PID."""

    interruption: BaseException | None = None
    term_deadline = time.monotonic() + _GROUP_STOP_GRACE_SECONDS
    try:
        _signal_group(process_group_id, signal.SIGTERM)
    except BaseException as raised:
        interruption = raised
    try:
        group_absent_before_reap = _wait_for_group_absence(process_group_id, term_deadline)
    except BaseException as raised:
        if interruption is None:
            interruption = raised
        group_absent_before_reap = False
    if not group_absent_before_reap:
        try:
            _signal_group(process_group_id, signal.SIGKILL)
        except BaseException as raised:
            if interruption is None:
                interruption = raised
        try:
            _wait_for_group_absence(
                process_group_id, time.monotonic() + _GROUP_KILL_GRACE_SECONDS
            )
        except BaseException as raised:
            if interruption is None:
                interruption = raised
    try:
        leader_reaped = _wait_for_leader(
            process, time.monotonic() + _GROUP_KILL_GRACE_SECONDS
        )
    except BaseException as raised:
        if interruption is None:
            interruption = raised
        leader_reaped = False
    # A just-exited leader can remain a zombie during the pre-reap observation,
    # so only absence after reaping is authoritative.  Never signal after this
    # point: the numerical process-group ID could have been recycled.
    try:
        final_absence = _wait_for_group_absence(
            process_group_id, time.monotonic() + _GROUP_KILL_GRACE_SECONDS
        )
    except BaseException as raised:
        if interruption is None:
            interruption = raised
        final_absence = False
    # A final absent group proves there is no process left to signal.  On macOS
    # an already-exiting group can report a transient signal error in this
    # window, so the intermediate signal result is not authoritative.
    return leader_reaped and final_absence, interruption
