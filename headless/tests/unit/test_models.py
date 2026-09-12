"""Bootstrap packaging and isolation tests for the headless service."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import stat
import subprocess
import tomllib
import zipfile

import pytest


HEADLESS_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PACKAGE = HEADLESS_ROOT / "src" / "hermes_downloads"
EXPECTED_WHEEL_MODULES = {
    "hermes_downloads/__init__.py",
    "hermes_downloads/cli.py",
    "hermes_downloads/mcp_server.py",
    "hermes_downloads/worker.py",
}
ENTRYPOINTS = (
    "hermes-downloads",
    "hermes-downloads-mcp",
    "hermes-downloads-worker",
)
LAUNCHERS = {
    "run-tests": ("--version",),
    "run-mcp": (),
    "run-worker": (),
}


def _environment(private_roots: dict[str, Path]) -> dict[str, str]:
    environment = {
        key: os.environ[key]
        for key in ("PATH", "LANG", "LC_ALL", "TMPDIR")
        if key in os.environ
    }
    environment.update(
        {
            "HOME": str(private_roots["home"]),
            "HERMES_HOME": str(private_roots["hermes_home"]),
            "HERMES_DOWNLOADS_STATE_ROOT": str(private_roots["state"]),
            "HERMES_DOWNLOADS_OUTPUT_ROOT": str(private_roots["output"]),
            "PYTHONDONTWRITEBYTECODE": "1",
            "UV_CACHE_DIR": str(private_roots["uv_cache"]),
            "UV_LINK_MODE": "copy",
            "UV_NO_PROGRESS": "1",
        }
    )
    return environment


def _run(
    argv: list[str], *, cwd: Path, environment: dict[str, str], timeout: float = 120
) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(
            argv,
            cwd=cwd,
            env=environment,
            stdin=subprocess.DEVNULL,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(f"command timed out: {argv[0]}")
    assert completed.returncode == 0, completed.stderr[-4000:]
    return completed


def _assert_runtime_roots_are_empty(private_roots: dict[str, Path]) -> None:
    for name in ("home", "hermes_home", "state", "output"):
        assert list(private_roots[name].iterdir()) == []


def test_installed_wheel_imports_from_scratch_without_ambient_python_paths(
    private_roots: dict[str, Path],
) -> None:
    """The product must run from a physical wheel, never checkout injection."""
    project = tomllib.loads((HEADLESS_ROOT / "pyproject.toml").read_text("utf-8"))
    assert project["project"]["requires-python"] == ">=3.12,<3.13"
    assert project["project"]["dependencies"] == []
    assert project["project"]["scripts"] == {
        "hermes-downloads": "hermes_downloads.cli:main",
        "hermes-downloads-mcp": "hermes_downloads.mcp_server:main",
        "hermes-downloads-worker": "hermes_downloads.worker:main",
    }

    uv = shutil.which("uv")
    assert uv is not None
    environment = _environment(private_roots)
    artifact_root = private_roots["artifact"]
    wheel_root = artifact_root / "wheel"
    scratch = artifact_root / "scratch"
    venv = artifact_root / "venv"
    for path in (wheel_root, scratch):
        path.mkdir(mode=0o700)

    _run(
        [uv, "build", "--wheel", "--out-dir", str(wheel_root)],
        cwd=HEADLESS_ROOT,
        environment=environment,
    )
    wheels = sorted(wheel_root.glob("*.whl"))
    assert len(wheels) == 1
    wheel = wheels[0]

    _run(
        [uv, "venv", "--python", "3.12", str(venv)],
        cwd=artifact_root,
        environment=environment,
    )
    installed_python = venv / "bin" / "python"
    _run(
        [
            uv,
            "pip",
            "install",
            "--python",
            str(installed_python),
            "--no-deps",
            str(wheel),
        ],
        cwd=artifact_root,
        environment=environment,
    )
    _run(
        [
            str(installed_python),
            "-I",
            "-c",
            "import sys; assert sys.version_info[:2] == (3, 12)",
        ],
        cwd=scratch,
        environment=environment,
    )

    ambient = artifact_root / "ambient"
    canary_package = ambient / "hermes_downloads"
    canary_package.mkdir(parents=True, mode=0o700)
    (canary_package / "__init__.py").write_text(
        "raise RuntimeError('ambient hermes_downloads was imported')\n", encoding="utf-8"
    )
    contaminated = dict(environment)
    contaminated["PYTHONPATH"] = str(ambient)
    contaminated["PYTHONHOME"] = str(private_roots["invalid_pythonhome"])
    probe = _run(
        [
            str(installed_python),
            "-I",
            "-c",
            "from pathlib import Path; import hermes_downloads; "
            "print(Path(hermes_downloads.__file__).resolve())",
        ],
        cwd=scratch,
        environment=contaminated,
    )
    installed_package = next((venv / "lib").glob("python*/site-packages")) / "hermes_downloads"
    assert Path(probe.stdout.strip()) == (installed_package / "__init__.py").resolve()
    assert not Path(probe.stdout.strip()).is_relative_to(SOURCE_PACKAGE.resolve())
    with zipfile.ZipFile(wheel) as archive:
        members = archive.namelist()
        assert len(members) == len(set(members))
        assert {
            name for name in members if ".dist-info/" not in name
        } == EXPECTED_WHEEL_MODULES

    for entrypoint in ENTRYPOINTS:
        command = venv / "bin" / entrypoint
        assert command.is_file() and os.access(command, os.X_OK)
        _run([str(command)], cwd=scratch, environment=environment)

    canonical_python = HEADLESS_ROOT / ".venv" / "bin" / "python"
    canonical_probe = _run(
        [
            str(canonical_python),
            "-I",
            "-c",
            "from pathlib import Path; import hermes_downloads; "
            "print(Path(hermes_downloads.__file__).resolve())",
        ],
        cwd=scratch,
        environment=contaminated,
    )
    canonical_origin = Path(canonical_probe.stdout.strip())
    assert canonical_origin.is_relative_to(HEADLESS_ROOT / ".venv")
    assert not canonical_origin.is_relative_to(SOURCE_PACKAGE.resolve())
    for module in sorted(EXPECTED_WHEEL_MODULES):
        relative_module = Path(module).relative_to("hermes_downloads")
        assert (installed_package / relative_module).read_bytes() == (
            SOURCE_PACKAGE / relative_module
        ).read_bytes()
        assert (canonical_origin.parent / relative_module).read_bytes() == (
            SOURCE_PACKAGE / relative_module
        ).read_bytes()

    for launcher, arguments in LAUNCHERS.items():
        script = HEADLESS_ROOT / "scripts" / launcher
        assert script.is_file()
        assert stat.S_IMODE(script.stat().st_mode) == 0o755
        _run([str(script), *arguments], cwd=scratch, environment=contaminated)

    _assert_runtime_roots_are_empty(private_roots)
