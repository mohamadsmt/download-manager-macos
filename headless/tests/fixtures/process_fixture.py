"""Private real-process fixture for bounded subprocess integration tests."""

from __future__ import annotations

import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time


_FINITE_STREAM_CHUNK_SIZE = 4096
_TERM_AWARE_CLEANUP_SECONDS = 0.05
_READY_WAIT_SECONDS = 1.0


def _write_pid(path: str, pid: int | None = None) -> None:
    target = Path(path)
    file_descriptor, temporary_path = tempfile.mkstemp(
        dir=target.parent, prefix=f".{target.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(file_descriptor, "wb") as temporary_file:
            temporary_file.write(str(os.getpid() if pid is None else pid).encode("ascii"))
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.replace(temporary_path, target)
    except BaseException:
        Path(temporary_path).unlink(missing_ok=True)
        raise


def _probe(arguments: list[str]) -> int:
    _write_pid(arguments[0])
    if arguments[1] != "$T08_LITERAL":
        return 31
    if "T08_PRIVATE_MARKER" in os.environ:
        return 32
    if sys.stdin.buffer.read(1) != b"":
        return 33
    sys.stdout.buffer.write(b"probe-ok\n")
    sys.stdout.buffer.flush()
    return 0


def _flood_both(arguments: list[str]) -> int:
    _write_pid(arguments[0])
    stdout_chunk = b"fixture-stdout-flood-marker:" + (b"o" * 2048)
    stderr_chunk = b"fixture-stderr-flood-marker:" + (b"e" * 2048)
    while True:
        os.write(sys.stdout.fileno(), stdout_chunk)
        os.write(sys.stderr.fileno(), stderr_chunk)
        time.sleep(0.005)


def _write_all(file_descriptor: int, payload: bytes) -> None:
    remaining = payload
    while remaining:
        written = os.write(file_descriptor, remaining)
        if written <= 0:
            raise RuntimeError("fixture write failed")
        remaining = remaining[written:]


def _finite_stream_payload(prefix: bytes, fill_byte: bytes, total_bytes: int) -> bytes:
    suffix = b":complete\n"
    return prefix + (fill_byte * (total_bytes - len(prefix) - len(suffix))) + suffix


def _finite_flood_both(arguments: list[str]) -> int:
    _write_pid(arguments[0])
    total_bytes = int(arguments[1])
    stdout_prefix = b"fixture-finite-stdout-saturation:"
    stderr_prefix = b"fixture-finite-stderr-saturation:"
    if total_bytes < max(len(stdout_prefix), len(stderr_prefix)) + len(b":complete\n"):
        return 34
    stdout_payload = _finite_stream_payload(stdout_prefix, b"o", total_bytes)
    stderr_payload = _finite_stream_payload(stderr_prefix, b"e", total_bytes)
    for offset in range(0, total_bytes, _FINITE_STREAM_CHUNK_SIZE):
        _write_all(
            sys.stdout.fileno(), stdout_payload[offset : offset + _FINITE_STREAM_CHUNK_SIZE]
        )
        _write_all(
            sys.stderr.fileno(), stderr_payload[offset : offset + _FINITE_STREAM_CHUNK_SIZE]
        )
    return 0


def _invalid_utf8(arguments: list[str], *, stderr: bool) -> int:
    _write_pid(arguments[0])
    stream = sys.stderr.buffer if stderr else sys.stdout.buffer
    marker = (
        b"fixture-invalid-stderr-marker"
        if stderr
        else b"fixture-invalid-stdout-marker"
    )
    stream.write(marker + b":\xff\n")
    stream.flush()
    return 0


def _spawn_detached_descendant(path: str) -> None:
    descendant = subprocess.Popen(
        (sys.executable, "-c", "import time; time.sleep(60)"),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )
    _write_pid(path, descendant.pid)


def _descendant_and_sleep(arguments: list[str]) -> int:
    _write_pid(arguments[0])
    _spawn_detached_descendant(arguments[1])
    time.sleep(60)
    return 0


def _term_resistant_leader(arguments: list[str]) -> int:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    _write_pid(arguments[0])
    while True:
        signal.pause()


def _close_pipes_then_finish(arguments: list[str]) -> int:
    completed_path, term_path = arguments[1:]

    def _on_term(_signal_number: int, _frame: object) -> None:
        Path(term_path).write_text("term", encoding="ascii")
        os._exit(36)

    signal.signal(signal.SIGTERM, _on_term)
    _write_pid(arguments[0])
    os.close(sys.stdout.fileno())
    os.close(sys.stderr.fileno())
    time.sleep(0.05)
    Path(completed_path).write_text("completed", encoding="ascii")
    os._exit(0)


def _exit_with_descendant(arguments: list[str]) -> int:
    _write_pid(arguments[0])
    _spawn_detached_descendant(arguments[1])
    sys.stdout.buffer.write(b"descendant-ok\n")
    sys.stdout.buffer.flush()
    return 0


def _wait_for_file(path: str) -> bool:
    deadline = time.monotonic() + _READY_WAIT_SECONDS
    while time.monotonic() < deadline:
        if Path(path).exists():
            return True
        time.sleep(0.005)
    return Path(path).exists()


def _term_aware_descendant(arguments: list[str]) -> int:
    identity_path, ready_path, cleanup_path = arguments
    identity = f"{os.getpid()}:{os.getpgrp()}"

    def _on_term(signal_number: int, _frame: object) -> None:
        time.sleep(_TERM_AWARE_CLEANUP_SECONDS)
        Path(cleanup_path).write_text(
            f"{identity}:{signal_number}", encoding="ascii"
        )
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _on_term)
    Path(identity_path).write_text(identity, encoding="ascii")
    Path(ready_path).write_text("ready", encoding="ascii")
    while True:
        signal.pause()


def _exit_with_term_aware_descendant(arguments: list[str]) -> int:
    _write_pid(arguments[0])
    descendant = subprocess.Popen(
        (
            sys.executable,
            str(Path(__file__).resolve()),
            "term-aware-descendant",
            *arguments[1:],
        ),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )
    if not _wait_for_file(arguments[2]):
        descendant.kill()
        descendant.wait()
        return 35
    sys.stdout.buffer.write(b"term-aware-descendant-ok\n")
    sys.stdout.buffer.flush()
    return 0


def _nonzero(arguments: list[str]) -> int:
    _write_pid(arguments[0])
    sys.stderr.buffer.write(b"fixture-nonzero-marker\n")
    sys.stderr.buffer.flush()
    return 23


def main() -> int:
    mode, *arguments = sys.argv[1:]
    if mode == "probe" and len(arguments) == 2:
        return _probe(arguments)
    if mode == "flood-both" and len(arguments) == 1:
        return _flood_both(arguments)
    if mode == "finite-flood-both" and len(arguments) == 2:
        return _finite_flood_both(arguments)
    if mode == "invalid-stdout" and len(arguments) == 1:
        return _invalid_utf8(arguments, stderr=False)
    if mode == "invalid-stderr" and len(arguments) == 1:
        return _invalid_utf8(arguments, stderr=True)
    if mode == "descendant-and-sleep" and len(arguments) == 2:
        return _descendant_and_sleep(arguments)
    if mode == "term-resistant-leader" and len(arguments) == 1:
        return _term_resistant_leader(arguments)
    if mode == "close-pipes-then-finish" and len(arguments) == 3:
        return _close_pipes_then_finish(arguments)
    if mode == "exit-with-descendant" and len(arguments) == 2:
        return _exit_with_descendant(arguments)
    if mode == "term-aware-descendant" and len(arguments) == 3:
        return _term_aware_descendant(arguments)
    if mode == "exit-with-term-aware-descendant" and len(arguments) == 4:
        return _exit_with_term_aware_descendant(arguments)
    if mode == "nonzero" and len(arguments) == 1:
        return _nonzero(arguments)
    return 64


if __name__ == "__main__":
    raise SystemExit(main())
