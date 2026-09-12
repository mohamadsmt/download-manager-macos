"""Private real-process fixture for bounded subprocess integration tests."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import time


def _write_pid(path: str) -> None:
    Path(path).write_text(str(os.getpid()), encoding="ascii")


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
    Path(path).write_text(str(descendant.pid), encoding="ascii")


def _descendant_and_sleep(arguments: list[str]) -> int:
    _write_pid(arguments[0])
    _spawn_detached_descendant(arguments[1])
    time.sleep(60)
    return 0


def _exit_with_descendant(arguments: list[str]) -> int:
    _write_pid(arguments[0])
    _spawn_detached_descendant(arguments[1])
    sys.stdout.buffer.write(b"descendant-ok\n")
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
    if mode == "invalid-stdout" and len(arguments) == 1:
        return _invalid_utf8(arguments, stderr=False)
    if mode == "invalid-stderr" and len(arguments) == 1:
        return _invalid_utf8(arguments, stderr=True)
    if mode == "descendant-and-sleep" and len(arguments) == 2:
        return _descendant_and_sleep(arguments)
    if mode == "exit-with-descendant" and len(arguments) == 2:
        return _exit_with_descendant(arguments)
    if mode == "nonzero" and len(arguments) == 1:
        return _nonzero(arguments)
    return 64


if __name__ == "__main__":
    raise SystemExit(main())
