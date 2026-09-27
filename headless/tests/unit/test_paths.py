"""Behavioral contract for safe deterministic download destinations."""

from __future__ import annotations

from dataclasses import replace
import importlib
import importlib.util
import os
from pathlib import Path
import stat

import pytest


CATEGORIES = ("Videos", "Audio", "Documents", "Software", "Other")
_PERSISTED_INTENT_SHAPES = tuple(
    (category, collection, selected_final_filename)
    for category in CATEGORIES
    for collection in (None, "Course material")
    for selected_final_filename in ("selected.webm", "selected--job-42.webm")
)


def _paths():
    spec = importlib.util.find_spec("hermes_downloads.paths")
    assert spec is not None, "hermes_downloads.paths must provide output path validation"
    return importlib.import_module("hermes_downloads.paths")


def _root() -> Path:
    root = Path.home() / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    return root


def _resolve(paths, root: Path, **overrides):
    values = {
        "category": "Videos",
        "filename": "selected.webm",
        "job_id": "job-42",
        "collection": None,
    }
    values.update(overrides)
    return paths.resolve_destination(root, **values)


@pytest.mark.parametrize("category", CATEGORIES)
def test_resolves_each_exact_type_category_under_the_supplied_root(
    tmp_path: Path, category: str
) -> None:
    paths = _paths()
    root = _root()

    destination = _resolve(paths, root, category=category)

    assert destination.root == root
    assert destination.final_path == root / category / "selected.webm"
    assert destination.incomplete_dir == root / ".incomplete" / "job-42"
    assert destination.partial_path == root / ".incomplete" / "job-42" / "selected.webm"
    assert destination.final_path.parent.is_dir()
    assert destination.incomplete_dir.is_dir()


def test_explicit_collection_takes_precedence_over_type_category(tmp_path: Path) -> None:
    paths = _paths()
    root = _root()

    destination = _resolve(
        paths,
        root,
        category="Audio",
        collection="Course material",
        filename="lesson.opus",
    )

    assert destination.final_path == root / "Course material" / "lesson.opus"
    assert not (root / "Audio" / "Course material").exists()


def test_preserves_valid_persian_unicode_and_selected_extension(tmp_path: Path) -> None:
    paths = _paths()
    root = _root()
    collection = "مجموعه\u200cی آموزشی"
    filename = "ویدیوی نمونه.webm"

    destination = _resolve(
        paths,
        root,
        collection=collection,
        filename=filename,
    )

    assert destination.final_path == root / collection / filename
    assert destination.partial_path.name == filename
    assert destination.final_path.suffix == ".webm"


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("collection", ".incomplete"),
        ("filename", ".incomplete"),
        ("collection", "."),
        ("collection", ".."),
        ("filename", "."),
        ("filename", ".."),
        ("collection", "../outside"),
        ("filename", "nested/file.webm"),
        ("collection", "/absolute"),
        ("filename", "/absolute.webm"),
        ("collection", "nested\\collection"),
        ("filename", "nested\\file.webm"),
        ("collection", "contains\x00nul"),
        ("filename", "contains\x00nul.webm"),
    ),
)
def test_rejects_reserved_dot_absolute_and_multicomponent_names(
    tmp_path: Path, field: str, value: str
) -> None:
    paths = _paths()
    root = _root()

    with pytest.raises(paths.PathValidationError):
        _resolve(paths, root, **{field: value})


@pytest.mark.parametrize("category", ("video", "Archive", ".incomplete", ""))
def test_rejects_categories_outside_the_fixed_contract(
    tmp_path: Path, category: str
) -> None:
    paths = _paths()

    with pytest.raises(paths.PathValidationError):
        _resolve(paths, _root(), category=category)


def test_rejects_a_writable_noncanonical_root_even_with_a_private_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    root = tmp_path / "cache" / "output"
    root.mkdir(parents=True, mode=0o700)
    monkeypatch.setenv("HERMES_DOWNLOADS_OUTPUT_ROOT", str(root))

    assert root != Path.home() / "Downloads" / "Hermes"
    assert os.access(root, os.W_OK | os.X_OK)
    with pytest.raises(paths.PathValidationError):
        _resolve(paths, root)


def test_rejects_a_destination_root_that_is_not_a_writable_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    root = _root()
    fallback = tmp_path / "unexpected-fallback"
    monkeypatch.setenv("HERMES_DOWNLOADS_OUTPUT_ROOT", str(fallback))
    root.chmod(0o500)
    try:
        with pytest.raises(paths.PathValidationError):
            _resolve(paths, root)
    finally:
        root.chmod(0o700)

    assert not fallback.exists()


def test_rejects_a_symlink_in_the_supplied_root_chain(tmp_path: Path) -> None:
    paths = _paths()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "Hermes").mkdir()
    downloads = Path.home() / "Downloads"
    downloads.symlink_to(outside, target_is_directory=True)

    with pytest.raises(paths.UnsafePathError):
        _resolve(paths, downloads / "Hermes")


def test_rejects_existing_symlink_destination_and_incomplete_components(
    tmp_path: Path,
) -> None:
    paths = _paths()
    root = _root()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "Videos").symlink_to(outside, target_is_directory=True)

    with pytest.raises(paths.UnsafePathError):
        _resolve(paths, root)

    (root / "Videos").unlink()
    (root / ".incomplete").symlink_to(outside, target_is_directory=True)
    with pytest.raises(paths.UnsafePathError):
        _resolve(paths, root)


def test_rejects_an_existing_final_symlink_instead_of_following_it(
    tmp_path: Path,
) -> None:
    paths = _paths()
    root = _root()
    video_directory = root / "Videos"
    video_directory.mkdir()
    outside = tmp_path / "outside-final"
    outside.write_text("do not change", encoding="utf-8")
    (video_directory / "selected.webm").symlink_to(outside)

    with pytest.raises(paths.UnsafePathError):
        _resolve(paths, root)

    assert outside.read_text(encoding="utf-8") == "do not change"


def test_claims_a_collision_name_without_overwriting_the_existing_final(
    tmp_path: Path,
) -> None:
    paths = _paths()
    root = _root()
    video_directory = root / "Videos"
    video_directory.mkdir()
    existing = video_directory / "selected.webm"
    existing.write_bytes(b"existing final bytes")

    destination = _resolve(paths, root)
    claim = paths.claim_final_path(destination)

    assert destination.final_path == video_directory / "selected--job-42.webm"
    assert claim == destination.final_path
    assert existing.read_bytes() == b"existing final bytes"
    assert claim.read_bytes() == b""
    assert os.stat(claim.parent).st_dev == os.stat(destination.incomplete_dir).st_dev


def test_claim_never_overwrites_an_existing_resolved_final(tmp_path: Path) -> None:
    paths = _paths()
    root = _root()
    destination = _resolve(paths, root)
    destination.final_path.parent.mkdir(exist_ok=True)
    destination.final_path.write_bytes(b"already complete")

    with pytest.raises(paths.FinalPathCollisionError):
        paths.claim_final_path(destination)

    assert destination.final_path.read_bytes() == b"already complete"


def test_rehydrate_destination_preserves_selected_collision_without_filesystem_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    home = tmp_path / "cold-home"
    home.mkdir()
    root = home / "Downloads" / "Hermes"
    assert not root.exists()
    calls: list[str] = []

    def forbidden(name: str):
        def _forbidden(*_args, **_kwargs):
            calls.append(name)
            raise AssertionError(f"rehydration must not call {name}")

        return _forbidden

    with monkeypatch.context() as patched:
        patched.setattr(paths.Path, "home", classmethod(lambda _cls: home))
        for name in ("mkdir", "stat", "lstat", "access", "open", "fstat", "fstatvfs"):
            patched.setattr(paths.os, name, forbidden(f"os.{name}"))
        patched.setattr(paths.Path, "resolve", forbidden("Path.resolve"))
        patched.setattr(paths, "resolve_destination", forbidden("resolve_destination"))
        patched.setattr(paths, "claim_final_path", forbidden("claim_final_path"))
        patched.setattr(paths, "_select_available_name", forbidden("_select_available_name"))

        destination = paths.rehydrate_destination(
            category="Videos",
            collection="Course material",
            partial_filename="selected.webm",
            selected_final_filename="selected--job-42.webm",
            job_id="job-42",
        )

    assert calls == []
    assert destination.root == root
    assert destination.category == "Videos"
    assert destination.collection == "Course material"
    assert destination.filename == "selected.webm"
    assert destination.job_id == "job-42"
    assert destination.final_path == root / "Course material" / "selected--job-42.webm"
    assert destination.incomplete_dir == root / ".incomplete" / "job-42"
    assert destination.partial_path == root / ".incomplete" / "job-42" / "selected.webm"
    assert not root.exists()


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("collection", "../outside"),
        ("partial_filename", "nested/partial.webm"),
        ("selected_final_filename", "nested/final.webm"),
        ("selected_final_filename", "renamed.webm"),
    ),
)
def test_rehydrate_destination_rejects_unmanaged_components_and_final_names(
    field: str, value: str
) -> None:
    paths = _paths()
    values = {
        "category": "Videos",
        "collection": None,
        "partial_filename": "selected.webm",
        "selected_final_filename": "selected.webm",
        "job_id": "job-42",
    }
    values[field] = value

    with pytest.raises(paths.PathValidationError):
        paths.rehydrate_destination(**values)


def _reservation(destination, **overrides):
    values = {
        "job_id": destination.job_id,
        "target_component": destination.collection or destination.category,
        "final_filename": destination.final_path.name,
        "claim_token": "a" * 64,
    }
    values.update(overrides)
    models = importlib.import_module("hermes_downloads.models")
    return models.PublicationReservation(**values)


def _entry_signature(path: Path) -> tuple[int, int, int, int, int]:
    details = os.lstat(path)
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_nlink,
        details.st_size,
    )


def _directory_identity_inventory(path: Path):
    return (
        _entry_signature(path),
        tuple(sorted((entry.name, _entry_signature(entry)) for entry in path.iterdir())),
    )


def _marker_path(destination) -> Path:
    return destination.incomplete_dir / ".hermes-reservation"


def test_attests_a_durable_publication_reservation_marker_without_claiming_payload_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    reservation = _reservation(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker = _marker_path(destination)
    fsync_kinds: list[int] = []
    original_fsync = paths.os.fsync

    def record_fsync(descriptor: int) -> None:
        fsync_kinds.append(stat.S_IFMT(os.fstat(descriptor).st_mode))
        original_fsync(descriptor)

    monkeypatch.setattr(paths.os, "fsync", record_fsync)

    created = paths.attest_publication_reservation_marker(destination, reservation)

    details = os.lstat(marker)
    assert "PublicationReservationMarker" in paths.__all__
    assert "attest_publication_reservation_marker" in paths.__all__
    assert tuple(paths.PublicationReservationMarker.__dataclass_fields__) == (
        "path",
        "st_dev",
        "st_ino",
    )
    assert created == paths.PublicationReservationMarker(
        path=marker,
        st_dev=details.st_dev,
        st_ino=details.st_ino,
    )
    assert stat.S_ISREG(details.st_mode)
    assert details.st_nlink == 1
    assert details.st_mode & 0o7777 == 0o600
    assert sidecar.read_bytes() == b"preserve this sidecar"
    assert not destination.final_path.exists()
    assert not destination.partial_path.exists()
    assert fsync_kinds == [stat.S_IFREG, stat.S_IFDIR, stat.S_IFREG]

    before = _entry_signature(marker)
    reattached = paths.attest_publication_reservation_marker(destination, reservation)

    assert reattached == created
    assert _entry_signature(marker) == before
    assert sidecar.read_bytes() == b"preserve this sidecar"
    assert not destination.final_path.exists()
    assert not destination.partial_path.exists()
    assert fsync_kinds == [
        stat.S_IFREG,
        stat.S_IFDIR,
        stat.S_IFREG,
        stat.S_IFREG,
        stat.S_IFDIR,
        stat.S_IFREG,
    ]


def test_existing_marker_file_fsync_precedes_job_directory_sync_on_reattest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    reservation = _reservation(destination)
    created = paths.attest_publication_reservation_marker(destination, reservation)
    marker = _marker_path(destination)
    marker_details = os.lstat(marker)
    job_details = os.lstat(destination.incomplete_dir)
    fsync_targets: list[tuple[int, int, int]] = []
    original_fsync = paths.os.fsync

    def record_fsync(descriptor: int) -> None:
        details = os.fstat(descriptor)
        fsync_targets.append(
            (stat.S_IFMT(details.st_mode), details.st_dev, details.st_ino)
        )
        original_fsync(descriptor)

    monkeypatch.setattr(paths.os, "fsync", record_fsync)

    reattached = paths.attest_publication_reservation_marker(destination, reservation)

    assert reattached == created
    assert fsync_targets[:2] == [
        (stat.S_IFREG, marker_details.st_dev, marker_details.st_ino),
        (stat.S_IFDIR, job_details.st_dev, job_details.st_ino),
    ]
    assert not destination.final_path.exists()
    assert not destination.partial_path.exists()


def test_marker_creation_is_exclusive_descriptor_relative_and_handles_short_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    opens: list[tuple[object, int, int, int | None]] = []
    writes: list[bytes] = []
    original_open = paths.os.open
    original_write = paths.os.write

    def record_open(
        path: object, flags: int, mode: int = 0o777, *, dir_fd: int | None = None
    ) -> int:
        if path == ".hermes-reservation":
            opens.append((path, flags, mode, dir_fd))
        return original_open(path, flags, mode, dir_fd=dir_fd)

    def short_write(descriptor: int, data: bytes) -> int:
        writes.append(bytes(data))
        return original_write(descriptor, data[: max(1, len(data) // 2)])

    monkeypatch.setattr(paths.os, "open", record_open)
    monkeypatch.setattr(paths.os, "write", short_write)

    paths.attest_publication_reservation_marker(destination, _reservation(destination))

    creation_opens = [entry for entry in opens if entry[1] & os.O_CREAT]
    assert len(creation_opens) == 1
    _, flags, mode, parent_fd = creation_opens[0]
    assert flags & os.O_WRONLY
    assert flags & os.O_CREAT
    assert flags & os.O_EXCL
    assert flags & os.O_NOFOLLOW
    assert flags & os.O_CLOEXEC
    assert mode == 0o600
    assert parent_fd is not None
    assert len(writes) > 1


@pytest.mark.parametrize(
    "reservation_overrides",
    (
        {"job_id": "other-job"},
        {"target_component": "Audio"},
        {"final_filename": "other.webm"},
    ),
)
def test_reservation_destination_mismatch_creates_nothing(
    tmp_path: Path, reservation_overrides: dict[str, str]
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"sidecar remains untouched")
    before_entries = {
        path.name: _entry_signature(path) for path in destination.incomplete_dir.iterdir()
    }

    with pytest.raises(paths.PathValidationError):
        paths.attest_publication_reservation_marker(
            destination,
            _reservation(destination, **reservation_overrides),
        )

    assert {
        path.name: _entry_signature(path) for path in destination.incomplete_dir.iterdir()
    } == before_entries
    assert sidecar.read_bytes() == b"sidecar remains untouched"
    assert not _marker_path(destination).exists()
    assert not destination.final_path.exists()
    assert not destination.partial_path.exists()


@pytest.mark.parametrize(
    "shape",
    ("symlink", "hardlink", "directory", "fifo", "wrong-mode", "wrong-content"),
)
def test_rejects_every_unsafe_preexisting_reservation_marker_without_touching_it(
    tmp_path: Path, shape: str
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    reservation = _reservation(destination)
    marker = _marker_path(destination)
    outside = tmp_path / f"outside-{shape}"
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"sidecar remains untouched")

    if shape == "symlink":
        outside.write_bytes(b"outside marker bytes")
        marker.symlink_to(outside)
    elif shape == "hardlink":
        outside.write_bytes(b"outside marker bytes")
        outside.chmod(0o600)
        os.link(outside, marker)
    elif shape == "directory":
        marker.mkdir(mode=0o700)
    elif shape == "fifo":
        os.mkfifo(marker, 0o600)
    elif shape == "wrong-mode":
        paths.attest_publication_reservation_marker(destination, reservation)
        marker.chmod(0o644)
    else:
        marker.write_bytes(b"wrong marker contents")
        marker.chmod(0o600)

    marker_before = _entry_signature(marker)
    sidecar_before = _entry_signature(sidecar)
    outside_before = _entry_signature(outside) if outside.exists() else None

    with pytest.raises(paths.PathValidationError):
        paths.attest_publication_reservation_marker(destination, reservation)

    assert _entry_signature(marker) == marker_before
    assert _entry_signature(sidecar) == sidecar_before
    assert sidecar.read_bytes() == b"sidecar remains untouched"
    if outside_before is not None:
        assert _entry_signature(outside) == outside_before
        assert outside.read_bytes() == b"outside marker bytes"
    assert not destination.final_path.exists()
    assert not destination.partial_path.exists()


@pytest.mark.parametrize(
    "reservation_overrides",
    (
        {"claim_token": "b" * 64},
        {"target_component": "Audio"},
    ),
)
def test_rejects_stale_reservation_marker_bytes_without_replacing_them(
    tmp_path: Path, reservation_overrides: dict[str, str]
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    marker = _marker_path(destination)
    expected_reservation = _reservation(destination)

    if "target_component" in reservation_overrides:
        other_destination = _resolve(
            paths,
            destination.root,
            category=reservation_overrides["target_component"],
            filename=destination.filename,
            job_id=destination.job_id,
        )
        paths.attest_publication_reservation_marker(
            other_destination,
            _reservation(other_destination),
        )
    else:
        paths.attest_publication_reservation_marker(destination, expected_reservation)

    before = _entry_signature(marker)
    before_bytes = marker.read_bytes()

    requested_reservation = (
        expected_reservation
        if "target_component" in reservation_overrides
        else _reservation(destination, **reservation_overrides)
    )
    with pytest.raises(paths.PathValidationError):
        paths.attest_publication_reservation_marker(destination, requested_reservation)

    assert _entry_signature(marker) == before
    assert marker.read_bytes() == before_bytes
    assert not destination.final_path.exists()
    assert not destination.partial_path.exists()


def test_rejects_a_missing_incomplete_job_parent_without_recreating_it(tmp_path: Path) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    destination.incomplete_dir.rmdir()

    with pytest.raises(paths.PathValidationError):
        paths.attest_publication_reservation_marker(destination, _reservation(destination))

    assert not destination.incomplete_dir.exists()
    assert not _marker_path(destination).exists()
    assert not destination.final_path.exists()
    assert not destination.partial_path.exists()


@pytest.mark.parametrize("chain_component", ("final", "incomplete"))
def test_rejects_a_replaced_managed_directory_chain_without_creating_a_marker(
    tmp_path: Path, chain_component: str
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    outside = tmp_path / f"outside-{chain_component}"
    outside.mkdir()

    if chain_component == "final":
        destination.final_path.parent.rmdir()
        destination.final_path.parent.symlink_to(outside, target_is_directory=True)
    else:
        destination.incomplete_dir.rmdir()
        (destination.root / ".incomplete").rmdir()
        (destination.root / ".incomplete").symlink_to(outside, target_is_directory=True)

    with pytest.raises(paths.UnsafePathError):
        paths.attest_publication_reservation_marker(destination, _reservation(destination))

    assert not (outside / ".hermes-reservation").exists()
    assert not destination.final_path.exists()
    assert not destination.partial_path.exists()


def test_marker_write_failure_preserves_sidecars_and_never_touches_payload_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"sidecar remains untouched")

    def fail_write(_descriptor: int, _data: bytes) -> int:
        raise OSError("injected marker write failure")

    monkeypatch.setattr(paths.os, "write", fail_write)

    with pytest.raises(paths.PathValidationError):
        paths.attest_publication_reservation_marker(destination, _reservation(destination))

    assert sidecar.read_bytes() == b"sidecar remains untouched"
    assert _marker_path(destination).exists()
    assert not destination.final_path.exists()
    assert not destination.partial_path.exists()


def test_marker_directory_fsync_failure_preserves_marker_and_sidecars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"sidecar remains untouched")
    calls = 0
    original_fsync = paths.os.fsync

    def fail_job_directory_sync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected job directory fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(paths.os, "fsync", fail_job_directory_sync)

    with pytest.raises(paths.PathValidationError):
        paths.attest_publication_reservation_marker(destination, _reservation(destination))

    assert calls == 2
    assert _marker_path(destination).is_file()
    assert sidecar.read_bytes() == b"sidecar remains untouched"
    assert not destination.final_path.exists()
    assert not destination.partial_path.exists()


def test_existing_marker_fsync_failure_does_not_replace_or_delete_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    reservation = _reservation(destination)
    marker = _marker_path(destination)
    paths.attest_publication_reservation_marker(destination, reservation)
    before = _entry_signature(marker)
    before_bytes = marker.read_bytes()

    def fail_fsync(_descriptor: int) -> None:
        raise OSError("injected existing marker fsync failure")

    monkeypatch.setattr(paths.os, "fsync", fail_fsync)

    with pytest.raises(paths.PathValidationError):
        paths.attest_publication_reservation_marker(destination, reservation)

    assert _entry_signature(marker) == before
    assert marker.read_bytes() == before_bytes
    assert not destination.final_path.exists()
    assert not destination.partial_path.exists()


def test_attesting_a_marker_never_claims_or_changes_existing_final_or_partial_payload(
    tmp_path: Path,
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    destination.final_path.write_bytes(b"preexisting final bytes")
    destination.partial_path.write_bytes(b"partial payload bytes")
    final_before = _entry_signature(destination.final_path)
    partial_before = _entry_signature(destination.partial_path)

    paths.attest_publication_reservation_marker(destination, _reservation(destination))

    assert _entry_signature(destination.final_path) == final_before
    assert destination.final_path.read_bytes() == b"preexisting final bytes"
    assert _entry_signature(destination.partial_path) == partial_before
    assert destination.partial_path.read_bytes() == b"partial payload bytes"


def _rehydrate_under_home(paths, monkeypatch: pytest.MonkeyPatch, home: Path, **overrides):
    values = {
        "category": "Videos",
        "collection": None,
        "partial_filename": "selected.webm",
        "selected_final_filename": "selected.webm",
        "job_id": "job-42",
    }
    values.update(overrides)
    monkeypatch.setattr(paths.Path, "home", classmethod(lambda _cls: home))
    return paths.rehydrate_destination(**values)


def test_prepares_only_the_exact_rehydrated_workspace_and_preserves_collision_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    home = tmp_path / "persisted-home"
    root = home / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    destination = _rehydrate_under_home(
        paths,
        monkeypatch,
        home,
        collection="Course material",
        selected_final_filename="selected--job-42.webm",
    )

    def forbidden(name: str):
        def _forbidden(*_args, **_kwargs):
            raise AssertionError(f"workspace preparation must not call {name}")

        return _forbidden

    monkeypatch.setattr(paths, "resolve_destination", forbidden("resolve_destination"))
    monkeypatch.setattr(paths, "claim_final_path", forbidden("claim_final_path"))
    monkeypatch.setattr(paths, "_select_available_name", forbidden("_select_available_name"))
    monkeypatch.setattr(
        paths,
        "attest_publication_reservation_marker",
        forbidden("attest_publication_reservation_marker"),
    )

    prepared = paths.prepare_persisted_destination_workspace(destination)

    assert prepared == destination
    assert prepared.root == root
    assert prepared.final_path == root / "Course material" / "selected--job-42.webm"
    assert prepared.incomplete_dir == root / ".incomplete" / "job-42"
    assert {path.name for path in root.iterdir()} == {"Course material", ".incomplete"}
    assert {path.name for path in prepared.incomplete_dir.parent.iterdir()} == {"job-42"}
    assert not (root / "Videos").exists()
    assert not prepared.final_path.exists()
    assert not prepared.partial_path.exists()
    assert not _marker_path(prepared).exists()


def test_prepare_persisted_workspace_is_idempotent_for_existing_safe_directories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    home = tmp_path / "persisted-home"
    root = home / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    destination = _rehydrate_under_home(paths, monkeypatch, home)

    first = paths.prepare_persisted_destination_workspace(destination)
    before = {
        path: _entry_signature(path)
        for path in (
            root,
            first.final_path.parent,
            first.incomplete_dir.parent,
            first.incomplete_dir,
        )
    }
    second = paths.prepare_persisted_destination_workspace(destination)

    assert second == first
    assert {
        path: _entry_signature(path)
        for path in (
            root,
            second.final_path.parent,
            second.incomplete_dir.parent,
            second.incomplete_dir,
        )
    } == before
    assert not second.final_path.exists()
    assert not second.partial_path.exists()
    assert not _marker_path(second).exists()


@pytest.mark.parametrize("unsafe_component", ("final", "incomplete", "job"))
@pytest.mark.parametrize("shape", ("symlink", "file"))
def test_prepare_persisted_workspace_rejects_unsafe_existing_components_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe_component: str,
    shape: str,
) -> None:
    paths = _paths()
    home = tmp_path / "persisted-home"
    root = home / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    destination = _rehydrate_under_home(paths, monkeypatch, home)

    if unsafe_component == "final":
        target = destination.final_path.parent
    elif unsafe_component == "incomplete":
        target = destination.incomplete_dir.parent
    else:
        target = destination.incomplete_dir
    target.parent.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / f"outside-{unsafe_component}-{shape}"
    if shape == "symlink":
        outside.mkdir()
        target.symlink_to(outside, target_is_directory=True)
    else:
        target.write_bytes(b"unsafe existing component")

    root_before = _entry_signature(root)
    parent_before = {
        path.name: _entry_signature(path) for path in target.parent.iterdir()
    }
    target_before = _entry_signature(target)

    with pytest.raises(paths.PathValidationError):
        paths.prepare_persisted_destination_workspace(destination)

    assert _entry_signature(root) == root_before
    assert {
        path.name: _entry_signature(path) for path in target.parent.iterdir()
    } == parent_before
    assert _entry_signature(target) == target_before
    if shape == "symlink":
        assert tuple(outside.iterdir()) == ()
    assert not os.path.lexists(destination.final_path)
    assert not os.path.lexists(destination.partial_path)
    assert not os.path.lexists(_marker_path(destination))


def test_prepare_persisted_workspace_rejects_a_mismatched_intent_without_creating_directories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    home = tmp_path / "persisted-home"
    root = home / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    destination = _rehydrate_under_home(paths, monkeypatch, home)
    mismatched = replace(
        destination,
        final_path=root / "Audio" / destination.final_path.name,
    )
    root_before = _entry_signature(root)

    with pytest.raises(paths.PathValidationError):
        paths.prepare_persisted_destination_workspace(mismatched)

    assert _entry_signature(root) == root_before
    assert tuple(root.iterdir()) == ()


def test_provisioned_rehydrated_workspace_can_be_separately_marker_attested(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    home = tmp_path / "persisted-home"
    root = home / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    destination = _rehydrate_under_home(paths, monkeypatch, home)

    prepared = paths.prepare_persisted_destination_workspace(destination)
    assert not _marker_path(prepared).exists()

    marker = paths.attest_publication_reservation_marker(
        prepared,
        _reservation(prepared),
    )

    assert marker.path == _marker_path(prepared)
    assert marker.path.is_file()
    assert not prepared.final_path.exists()
    assert not prepared.partial_path.exists()


@pytest.mark.parametrize(
    ("category", "collection", "selected_final_filename"),
    _PERSISTED_INTENT_SHAPES,
)
def test_prepare_persisted_workspace_preflights_existing_nonwritable_final_before_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    category: str,
    collection: str | None,
    selected_final_filename: str,
) -> None:
    paths = _paths()
    home = tmp_path / "persisted-home"
    root = home / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    destination = _rehydrate_under_home(
        paths,
        monkeypatch,
        home,
        category=category,
        collection=collection,
        selected_final_filename=selected_final_filename,
    )
    final_directory = destination.final_path.parent
    final_directory.mkdir(mode=0o700)
    final_directory.chmod(0o500)
    root_before = _directory_identity_inventory(root)
    original_access = paths.os.access

    def deny_final_directory_access(path, mode, *args, **kwargs):
        if Path(path) == final_directory and mode == os.W_OK | os.X_OK:
            return False
        return original_access(path, mode, *args, **kwargs)

    monkeypatch.setattr(paths.os, "access", deny_final_directory_access)

    try:
        with pytest.raises(paths.PathValidationError, match="not writable"):
            paths.prepare_persisted_destination_workspace(destination)

        assert _directory_identity_inventory(root) == root_before
        assert not os.path.lexists(destination.incomplete_dir.parent)
        assert not os.path.lexists(destination.final_path)
        assert not os.path.lexists(destination.partial_path)
        assert not os.path.lexists(_marker_path(destination))
    finally:
        final_directory.chmod(0o700)


@pytest.mark.parametrize(
    ("category", "collection", "selected_final_filename"),
    _PERSISTED_INTENT_SHAPES,
)
def test_prepare_persisted_workspace_preflights_cross_filesystem_final_before_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    category: str,
    collection: str | None,
    selected_final_filename: str,
) -> None:
    paths = _paths()
    home = tmp_path / "persisted-home"
    root = home / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700)
    destination = _rehydrate_under_home(
        paths,
        monkeypatch,
        home,
        category=category,
        collection=collection,
        selected_final_filename=selected_final_filename,
    )
    final_directory = destination.final_path.parent
    final_directory.mkdir(mode=0o700)
    root_before = _directory_identity_inventory(root)
    expected_checked_identities = (
        (os.stat(final_directory).st_dev, os.stat(final_directory).st_ino),
        (os.stat(root).st_dev, os.stat(root).st_ino),
    )
    checked_identities: list[tuple[tuple[int, int], tuple[int, int]]] = []
    mkdir_names: list[str] = []
    original_mkdir = paths.os.mkdir

    def record_mkdir(name, *args, **kwargs):
        mkdir_names.append(os.fspath(name))
        return original_mkdir(name, *args, **kwargs)

    def reject_cross_filesystem(final_fd: int, incomplete_fd: int) -> None:
        checked_identities.append(
            (
                (os.fstat(final_fd).st_dev, os.fstat(final_fd).st_ino),
                (os.fstat(incomplete_fd).st_dev, os.fstat(incomplete_fd).st_ino),
            )
        )
        raise paths.PathValidationError(
            "final and incomplete destinations must share a filesystem"
        )

    monkeypatch.setattr(paths.os, "mkdir", record_mkdir)
    monkeypatch.setattr(paths, "_require_same_filesystem", reject_cross_filesystem)

    with pytest.raises(paths.PathValidationError, match="share a filesystem"):
        paths.prepare_persisted_destination_workspace(destination)

    assert checked_identities == [expected_checked_identities]
    assert mkdir_names == []
    assert _directory_identity_inventory(root) == root_before
    assert not os.path.lexists(destination.incomplete_dir.parent)
    assert not os.path.lexists(destination.final_path)
    assert not os.path.lexists(destination.partial_path)
    assert not os.path.lexists(_marker_path(destination))
