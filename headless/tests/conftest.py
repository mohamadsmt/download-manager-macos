"""Private, side-effect-free test environment for the headless service."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _isolate_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> dict[str, Path]:
    root = tmp_path / "private"
    roots = {
        "home": root / "home",
        "hermes_home": root / "hermes-home",
        "state": root / "state",
        "output": root / "output",
        "uv_cache": root / "uv-cache",
        "artifact": root / "artifact",
        "invalid_pythonhome": root / "invalid-pythonhome",
    }
    root.mkdir(mode=0o700)
    for path in roots.values():
        if path.name != "invalid-pythonhome":
            path.mkdir(mode=0o700)
    monkeypatch.setenv("HOME", str(roots["home"]))
    monkeypatch.setenv("HERMES_HOME", str(roots["hermes_home"]))
    monkeypatch.setenv("HERMES_DOWNLOADS_STATE_ROOT", str(roots["state"]))
    monkeypatch.setenv("HERMES_DOWNLOADS_OUTPUT_ROOT", str(roots["output"]))
    monkeypatch.delenv("PYTHONPATH", raising=False)
    monkeypatch.delenv("PYTHONHOME", raising=False)
    monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")
    return roots


@pytest.fixture
def private_roots(_isolate_runtime: dict[str, Path]) -> dict[str, Path]:
    return _isolate_runtime
