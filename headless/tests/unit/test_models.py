"""Packaging, isolation, and immutable model tests for the headless service."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
import importlib
import importlib.util
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
    "hermes_downloads/models.py",
    "hermes_downloads/store.py",
    "hermes_downloads/worker.py",
}
ENTRYPOINTS = (
    "hermes-downloads",
    "hermes-downloads-mcp",
    "hermes-downloads-worker",
)
LAUNCHERS = {
    "run-tests": (("--version",),),
    "run-mcp": ((),),
    "run-worker": ((),),
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


def _assert_runtime_roots_are_isolated(private_roots: dict[str, Path]) -> None:
    for name in ("home", "hermes_home", "output"):
        assert list(private_roots[name].iterdir()) == []
    assert [path.name for path in private_roots["state"].iterdir()] == ["state.db"]


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
    clean_environment = _environment(private_roots)
    assert "PYTHONPATH" not in clean_environment
    assert "PYTHONHOME" not in clean_environment
    artifact_root = private_roots["artifact"]
    wheel_root = artifact_root / "wheel"
    scratch = artifact_root / "scratch"
    venv = artifact_root / "venv"
    for path in (wheel_root, scratch):
        path.mkdir(mode=0o700)

    _run(
        [uv, "build", "--wheel", "--out-dir", str(wheel_root)],
        cwd=HEADLESS_ROOT,
        environment=clean_environment,
    )
    wheels = sorted(wheel_root.glob("*.whl"))
    assert len(wheels) == 1
    wheel = wheels[0]

    _run(
        [uv, "venv", "--python", "3.12", str(venv)],
        cwd=artifact_root,
        environment=clean_environment,
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
        environment=clean_environment,
    )
    _run(
        [
            str(installed_python),
            "-c",
            "import sys; assert sys.version_info[:2] == (3, 12)",
        ],
        cwd=scratch,
        environment=clean_environment,
    )

    installed_package = (
        next((venv / "lib").glob("python*/site-packages")) / "hermes_downloads"
    )
    probe = _run(
        [
            str(installed_python),
            "-c",
            "from pathlib import Path; import hermes_downloads; "
            "print(Path(hermes_downloads.__file__).resolve())",
        ],
        cwd=scratch,
        environment=clean_environment,
    )
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
        _run([str(command)], cwd=scratch, environment=clean_environment)

    canonical_python = HEADLESS_ROOT / ".venv" / "bin" / "python"
    canonical_probe = _run(
        [
            str(canonical_python),
            "-c",
            "from pathlib import Path; import hermes_downloads; "
            "print(Path(hermes_downloads.__file__).resolve())",
        ],
        cwd=scratch,
        environment=clean_environment,
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

    ambient = artifact_root / "ambient"
    canary_package = ambient / "hermes_downloads"
    canary_package.mkdir(parents=True, mode=0o700)
    canary_message = "ambient hermes_downloads was imported"
    (canary_package / "__init__.py").write_text(
        f"raise RuntimeError({canary_message!r})\n", encoding="utf-8"
    )
    contaminated = dict(clean_environment)
    contaminated["PYTHONPATH"] = str(ambient)
    contaminated["PYTHONHOME"] = str(private_roots["invalid_pythonhome"])
    launcher_probe = artifact_root / "test_launcher_import.py"
    launcher_probe.write_text(
        "def test_imports_hermes_downloads() -> None:\n"
        "    import hermes_downloads\n"
        "\n"
        "    assert hermes_downloads.__file__\n",
        encoding="utf-8",
    )

    for launcher, argument_sets in LAUNCHERS.items():
        script = HEADLESS_ROOT / "scripts" / launcher
        assert script.is_file()
        assert stat.S_IMODE(script.stat().st_mode) == 0o755
        if launcher == "run-tests":
            argument_sets = (*argument_sets, (str(launcher_probe),))
        for arguments in argument_sets:
            completed = _run(
                [str(script), *arguments], cwd=scratch, environment=contaminated
            )
            assert canary_message not in completed.stdout
            assert canary_message not in completed.stderr
        script_text = script.read_text("utf-8")
        assert "unset PYTHONPATH PYTHONHOME" in script_text
        assert script_text.index("unset PYTHONPATH PYTHONHOME") < script_text.index("exec ")
        assert "-I" not in script_text

    _assert_runtime_roots_are_isolated(private_roots)


def _models():
    spec = importlib.util.find_spec("hermes_downloads.models")
    assert spec is not None, "hermes_downloads.models must provide the public models"
    return importlib.import_module("hermes_downloads.models")


def test_job_states_are_exactly_the_planned_public_states() -> None:
    job_state = _models().JobState
    expected = {
        "queued",
        "resolving",
        "downloading",
        "pausing",
        "paused",
        "retry_wait",
        "needs_link",
        "needs_auth",
        "blocked",
        "finalizing",
        "completed",
        "cancelled",
        "removed",
        "failed",
    }

    assert {state.value for state in job_state} == expected
    assert len(job_state.__members__) == len(expected)


def test_all_open_admission_gates_allow_transfer() -> None:
    admission = _models().Admission(
        queue_running=True,
        collection_held=False,
        authorized=True,
        item_held=False,
        due=True,
    )

    assert admission.allowed is True


@pytest.mark.parametrize(
    ("closed_field", "closed_value"),
    (
        ("queue_running", False),
        ("collection_held", True),
        ("authorized", False),
        ("item_held", True),
        ("due", False),
    ),
)
def test_each_independent_admission_gate_closes_transfer(
    closed_field: str, closed_value: bool
) -> None:
    values = {
        "queue_running": True,
        "collection_held": False,
        "authorized": True,
        "item_held": False,
        "due": True,
    }
    values[closed_field] = closed_value

    assert _models().Admission(**values).allowed is False


def test_due_time_never_authorizes_transfer() -> None:
    admission = _models().Admission(
        queue_running=True,
        collection_held=False,
        authorized=False,
        item_held=False,
        due=True,
    )

    assert admission.allowed is False


def test_manual_hold_survives_queue_resume():
    _models()
    from hermes_downloads.models import Admission

    admission = Admission(queue_running=True, collection_held=False,
                          authorized=True, item_held=True, due=True)
    assert admission.allowed is False


def _download_intent(**overrides):
    values = {
        "job_id": "job-1",
        "request_id": "request-1",
        "payload_digest": "a" * 64,
        "source_url": b"https://example.test/%2Fsource?copy=1&copy=1",
        "expected_revision": None,
        "generation": 0,
        "revision": 0,
    }
    values.update(overrides)
    return _models().DownloadIntent(**values)


def test_download_intent_is_immutable_and_preserves_raw_source_bytes() -> None:
    submitted = bytearray(b"https://example.test/%2Fsource?copy=1&copy=1")
    intent = _download_intent(
        source_url=submitted,
        expected_revision=4,
        generation=5,
        revision=6,
    )
    submitted[-1] = ord("2")

    assert intent.job_id == "job-1"
    assert intent.request_id == "request-1"
    assert intent.payload_digest == "a" * 64
    assert intent.expected_revision == 4
    assert intent.generation == 5
    assert intent.revision == 6
    assert type(intent.source_url) is bytes
    assert intent.source_url == b"https://example.test/%2Fsource?copy=1&copy=1"
    with pytest.raises(FrozenInstanceError):
        intent.revision = 7


@pytest.mark.parametrize(
    "overrides",
    (
        {"job_id": ""},
        {"job_id": " job-1"},
        {"request_id": "request id"},
        {"payload_digest": "a" * 63},
        {"payload_digest": "A" * 64},
        {"source_url": b""},
        {"source_url": "https://example.test/source"},
        {"expected_revision": -1},
        {"expected_revision": float("inf")},
        {"expected_revision": True},
        {"expected_revision": False},
        {"generation": -1},
        {"generation": float("inf")},
        {"generation": True},
        {"generation": False},
        {"revision": -1},
        {"revision": float("nan")},
        {"revision": True},
        {"revision": False},
    ),
)
def test_download_intent_rejects_malformed_or_nonfinite_values(
    overrides: dict[str, object]
) -> None:
    with pytest.raises((TypeError, ValueError)):
        _download_intent(**overrides)
