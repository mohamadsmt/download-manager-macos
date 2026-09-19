"""Path-scoped, side-effect-free job disk-accounting contract."""

from __future__ import annotations

import importlib
import importlib.util
import os
from pathlib import Path
from typing import Any

import pytest


def _paths() -> Any:
    spec = importlib.util.find_spec("hermes_downloads.paths")
    assert spec is not None, "hermes_downloads.paths must provide job-space accounting"
    return importlib.import_module("hermes_downloads.paths")


def _destination(paths: Any, *, job_id: str = "job-space") -> Any:
    root = Path.home() / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    return paths.resolve_destination(
        root,
        category="Other",
        filename="output.bin",
        job_id=job_id,
    )


def _observed_allocated_bytes(paths: tuple[Path, ...]) -> int | None:
    blocks: list[int] = []
    for path in paths:
        value = getattr(os.lstat(path), "st_blocks", None)
        if type(value) is not int or value < 0:
            return None
        blocks.append(value)
    return sum(value * 512 for value in blocks)


def test_observes_a_fresh_job_and_then_current_appended_artifacts() -> None:
    paths = _paths()
    destination = _destination(paths, job_id="job-space-fresh")
    unlisted = destination.incomplete_dir / "unlisted.bin"
    unlisted.write_bytes(b"not in the owned ledger")

    fresh = paths.observe_job_space(
        destination,
        owned_paths=(),
        output_path=destination.partial_path,
        expected_output_logical_bytes=11,
    )

    assert fresh.current == paths.StorageUsage(logical_bytes=0, allocated_bytes=0)
    filesystem = os.statvfs(destination.incomplete_dir)
    assert fresh.available_bytes == filesystem.f_bavail * filesystem.f_frsize
    assert fresh.expected_output_logical_bytes == 11
    assert fresh.expected_peak_logical_bytes == 11

    sidecar = destination.incomplete_dir / "segments.part"
    sidecar.write_bytes(b"segment")
    destination.partial_path.write_bytes(b"output")

    observed = paths.observe_job_space(
        destination,
        owned_paths=(sidecar,),
        output_path=destination.partial_path,
        expected_output_logical_bytes=2,
    )

    assert observed.current.logical_bytes == len(b"segment") + len(b"output")
    assert observed.current.allocated_bytes == _observed_allocated_bytes(
        (sidecar, destination.partial_path)
    )
    assert observed.expected_peak_logical_bytes == len(b"segment") + len(b"output")
    assert unlisted.read_bytes() == b"not in the owned ledger"


def test_reports_known_logical_merge_peak_without_projecting_allocation() -> None:
    paths = _paths()
    destination = _destination(paths, job_id="job-space-peak")
    video = destination.incomplete_dir / "video.part"
    audio = destination.incomplete_dir / "audio.part"
    video.write_bytes(b"v" * 13)
    audio.write_bytes(b"a" * 17)
    destination.partial_path.write_bytes(b"o" * 5)

    observed = paths.observe_job_space(
        destination,
        owned_paths=(video, audio),
        output_path=destination.partial_path,
        expected_output_logical_bytes=19,
    )

    assert observed.current.logical_bytes == 35
    assert observed.current.allocated_bytes == _observed_allocated_bytes(
        (video, audio, destination.partial_path)
    )
    assert observed.expected_output_logical_bytes == 19
    assert observed.expected_peak_logical_bytes == 49


def test_preserves_unknown_expected_output_size() -> None:
    paths = _paths()
    destination = _destination(paths, job_id="job-space-unknown")
    sidecar = destination.incomplete_dir / "known.part"
    sidecar.write_bytes(b"known")

    observed = paths.observe_job_space(
        destination,
        owned_paths=(sidecar,),
        output_path=destination.partial_path,
        expected_output_logical_bytes=None,
    )

    assert observed.current.logical_bytes == len(b"known")
    assert observed.current.allocated_bytes == _observed_allocated_bytes((sidecar,))
    assert observed.expected_output_logical_bytes is None
    assert observed.expected_peak_logical_bytes is None


@pytest.mark.parametrize("invalid_expected", (-1, True, False, 1.5, "1"))
def test_rejects_invalid_expected_output_sizes_without_changing_artifacts(
    invalid_expected: object,
) -> None:
    paths = _paths()
    destination = _destination(paths, job_id="job-space-invalid-expected")
    sidecar = destination.incomplete_dir / "owned.part"
    sidecar.write_bytes(b"unchanged")

    with pytest.raises(paths.PathValidationError):
        paths.observe_job_space(
            destination,
            owned_paths=(sidecar,),
            output_path=destination.partial_path,
            expected_output_logical_bytes=invalid_expected,
        )

    assert sidecar.read_bytes() == b"unchanged"
    assert not destination.partial_path.exists()


@pytest.mark.parametrize(
    "invalid_kind",
    ("missing", "outside", "symlink", "directory", "hardlink", "duplicate", "output-collision"),
)
def test_rejects_unsafe_owned_artifact_ledgers_without_changing_content(
    tmp_path: Path, invalid_kind: str
) -> None:
    paths = _paths()
    destination = _destination(paths, job_id=f"job-space-{invalid_kind}")
    owned = destination.incomplete_dir / "owned.part"
    owned.write_bytes(b"owned bytes")
    preserved: dict[Path, bytes] = {owned: b"owned bytes"}
    owned_paths: tuple[Path, ...] = (owned,)
    output_path = destination.partial_path
    invalid_path: Path | None = None

    if invalid_kind == "missing":
        missing = destination.incomplete_dir / "missing.part"
        owned_paths = (missing,)
        invalid_path = missing
    elif invalid_kind == "outside":
        outside = tmp_path / "outside.part"
        outside.write_bytes(b"outside bytes")
        preserved[outside] = b"outside bytes"
        owned_paths = (outside,)
        invalid_path = outside
    elif invalid_kind == "symlink":
        external = tmp_path / "external.part"
        external.write_bytes(b"external bytes")
        symlink = destination.incomplete_dir / "symlink.part"
        symlink.symlink_to(external)
        preserved[external] = b"external bytes"
        owned_paths = (symlink,)
        invalid_path = symlink
    elif invalid_kind == "directory":
        directory = destination.incomplete_dir / "directory.part"
        directory.mkdir()
        owned_paths = (directory,)
        invalid_path = directory
    elif invalid_kind == "hardlink":
        source = tmp_path / "hardlink-source.part"
        source.write_bytes(b"hardlink bytes")
        hardlink = destination.incomplete_dir / "hardlink.part"
        os.link(source, hardlink)
        preserved[source] = b"hardlink bytes"
        owned_paths = (hardlink,)
        invalid_path = hardlink
    elif invalid_kind == "duplicate":
        owned_paths = (owned, owned)
        invalid_path = owned
    elif invalid_kind == "output-collision":
        output_path = owned
        invalid_path = owned

    with pytest.raises(paths.PathValidationError):
        paths.observe_job_space(
            destination,
            owned_paths=owned_paths,
            output_path=output_path,
            expected_output_logical_bytes=8,
        )

    for path, expected_bytes in preserved.items():
        assert path.read_bytes() == expected_bytes
    assert invalid_path is not None
    if invalid_kind == "missing":
        assert not invalid_path.exists()
    elif invalid_kind == "symlink":
        assert invalid_path.is_symlink()
    elif invalid_kind == "directory":
        assert invalid_path.is_dir()
    else:
        assert invalid_path.is_file()
    assert not destination.partial_path.exists()


def test_rejects_a_non_tuple_owned_ledger_without_changing_content() -> None:
    paths = _paths()
    destination = _destination(paths, job_id="job-space-non-tuple")
    sidecar = destination.incomplete_dir / "owned.part"
    sidecar.write_bytes(b"unchanged")

    with pytest.raises(paths.PathValidationError):
        paths.observe_job_space(
            destination,
            owned_paths=[sidecar],
            output_path=destination.partial_path,
            expected_output_logical_bytes=1,
        )

    assert sidecar.read_bytes() == b"unchanged"


def test_rejects_replaced_incomplete_component_without_touching_external_or_original_files(
    tmp_path: Path,
) -> None:
    paths = _paths()
    destination = _destination(paths, job_id="job-space-replaced-incomplete")
    original_owned = destination.incomplete_dir / "owned.part"
    original_owned.write_bytes(b"original owned")
    destination.partial_path.write_bytes(b"original output")
    destination.final_path.write_bytes(b"final output")

    original_incomplete = destination.incomplete_dir.parent
    relocated_incomplete = tmp_path / "original-incomplete"
    external_incomplete = tmp_path / "external-incomplete"
    external_owned = external_incomplete / destination.job_id / "owned.part"
    external_owned.parent.mkdir(parents=True)
    external_owned.write_bytes(b"external owned")
    original_incomplete.rename(relocated_incomplete)
    original_incomplete.symlink_to(external_incomplete, target_is_directory=True)

    with pytest.raises(paths.PathValidationError):
        paths.observe_job_space(
            destination,
            owned_paths=(original_owned,),
            output_path=destination.partial_path,
            expected_output_logical_bytes=23,
        )

    assert external_owned.read_bytes() == b"external owned"
    assert (relocated_incomplete / destination.job_id / "owned.part").read_bytes() == (
        b"original owned"
    )
    assert (relocated_incomplete / destination.job_id / destination.filename).read_bytes() == (
        b"original output"
    )
    assert destination.final_path.read_bytes() == b"final output"
    assert original_incomplete.is_symlink()


def test_rejects_same_inode_lexical_owned_aliases_before_accounting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    destination = _destination(paths, job_id="job-space-inode-alias")
    first_alias = destination.incomplete_dir / "artifact.part"
    second_alias = destination.incomplete_dir / "ARTIFACT.PART"
    first_alias.write_bytes(b"one physical artifact")
    assert first_alias != second_alias

    original_observe_file = paths._require_job_file
    observed_paths: list[Path] = []

    def observe_normalized_alias(path: Path) -> os.stat_result:
        observed_paths.append(path)
        assert path in (first_alias, second_alias)
        return original_observe_file(first_alias)

    monkeypatch.setattr(paths, "_require_job_file", observe_normalized_alias)

    with pytest.raises(paths.PathValidationError):
        paths.observe_job_space(
            destination,
            owned_paths=(first_alias, second_alias),
            output_path=destination.partial_path,
            expected_output_logical_bytes=0,
        )

    assert observed_paths == [first_alias, second_alias]


def test_rejects_replaced_downloads_root_chain_without_observing_external_artifacts(
    tmp_path: Path,
) -> None:
    paths = _paths()
    destination = _destination(paths, job_id="job-space-replaced-downloads")
    original_owned = destination.incomplete_dir / "owned.part"
    original_owned_bytes = b"original owned artifact"
    original_output_bytes = b"original output artifact"
    original_owned.write_bytes(original_owned_bytes)
    destination.partial_path.write_bytes(original_output_bytes)

    downloads = destination.root.parent
    relocated_downloads = tmp_path / "relocated-downloads"
    external_downloads = tmp_path / "external-downloads"
    external_incomplete = (
        external_downloads / "Hermes" / ".incomplete" / destination.job_id
    )
    external_owned = external_incomplete / "owned.part"
    external_output = external_incomplete / destination.filename
    external_owned_bytes = b"external owned artifact with distinct size"
    external_output_bytes = b"external output artifact with distinct size"
    external_incomplete.mkdir(parents=True)
    external_owned.write_bytes(external_owned_bytes)
    external_output.write_bytes(external_output_bytes)

    downloads_moved = False
    try:
        downloads.rename(relocated_downloads)
        downloads_moved = True
        downloads.symlink_to(external_downloads, target_is_directory=True)

        with pytest.raises(paths.PathValidationError):
            observed = paths.observe_job_space(
                destination,
                owned_paths=(original_owned,),
                output_path=destination.partial_path,
                expected_output_logical_bytes=len(external_output_bytes),
            )
            assert observed.current.logical_bytes == (
                len(external_owned_bytes) + len(external_output_bytes)
            )
            pytest.fail("observer followed the replaced Downloads root chain")
    finally:
        if downloads.is_symlink():
            downloads.unlink()
        if downloads_moved:
            relocated_downloads.rename(downloads)

    assert original_owned.read_bytes() == original_owned_bytes
    assert destination.partial_path.read_bytes() == original_output_bytes
    assert external_owned.read_bytes() == external_owned_bytes
    assert external_output.read_bytes() == external_output_bytes
    assert downloads.is_dir()
    assert not downloads.is_symlink()
