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


def _payload_identity(path: Path) -> tuple[int, int, int]:
    details = os.lstat(path)
    return details.st_dev, details.st_ino, details.st_size


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


def _prepare_staged_partial(paths, payload: bytes = b"complete payload bytes"):
    destination = _resolve(paths, _root())
    reservation = _reservation(destination)
    paths.attest_publication_reservation_marker(destination, reservation)
    destination.partial_path.write_bytes(payload)
    return destination, reservation


def test_attests_staged_partial_payload_descriptor_relatively_without_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    payload = b"complete payload bytes"
    destination, reservation = _prepare_staged_partial(paths, payload)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    marker_bytes = marker.read_bytes()
    partial_before = _entry_signature(destination.partial_path)
    partial_identity = partial_before[:2]
    sidecar_before = _entry_signature(sidecar)
    job_details = os.lstat(destination.incomplete_dir)
    opens: list[tuple[int, int, int | None]] = []
    fsync_targets: list[tuple[int, int, int]] = []
    original_open = paths.os.open
    original_read = paths.os.read
    original_fsync = paths.os.fsync

    def record_open(
        path: object, flags: int, mode: int = 0o777, *, dir_fd: int | None = None
    ) -> int:
        if path == destination.partial_path.name:
            opens.append((flags, mode, dir_fd))
        return original_open(path, flags, mode, dir_fd=dir_fd)

    def reject_partial_read(descriptor: int, count: int) -> bytes:
        details = os.fstat(descriptor)
        assert (details.st_dev, details.st_ino) != partial_identity
        return original_read(descriptor, count)

    def record_fsync(descriptor: int) -> None:
        details = os.fstat(descriptor)
        fsync_targets.append((stat.S_IFMT(details.st_mode), details.st_dev, details.st_ino))
        original_fsync(descriptor)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("staged payload attestation must not resolve a destination")

    monkeypatch.setattr(paths.os, "open", record_open)
    monkeypatch.setattr(paths.os, "read", reject_partial_read)
    monkeypatch.setattr(paths.os, "fsync", record_fsync)
    monkeypatch.setattr(paths, "resolve_destination", forbidden)

    attested = paths.attest_staged_partial_payload(destination, reservation)

    assert "StagedPartialPayload" in paths.__all__
    assert "attest_staged_partial_payload" in paths.__all__
    assert tuple(paths.StagedPartialPayload.__dataclass_fields__) == (
        "path",
        "st_dev",
        "st_ino",
        "logical_size",
        "mtime_ns",
    )
    assert attested == paths.StagedPartialPayload(
        path=destination.partial_path,
        st_dev=partial_before[0],
        st_ino=partial_before[1],
        logical_size=len(payload),
        mtime_ns=destination.partial_path.stat().st_mtime_ns,
    )
    assert reservation.claim_token not in repr(attested)
    assert len(opens) == 1
    flags, _mode, parent_fd = opens[0]
    assert flags == os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC
    assert parent_fd is not None
    assert fsync_targets == [
        (stat.S_IFREG, marker_before[0], marker_before[1]),
        (stat.S_IFREG, partial_before[0], partial_before[1]),
        (stat.S_IFDIR, job_details.st_dev, job_details.st_ino),
    ]
    assert _entry_signature(marker) == marker_before
    assert marker.read_bytes() == marker_bytes
    assert _entry_signature(destination.partial_path) == partial_before
    assert destination.partial_path.read_bytes() == payload
    assert _entry_signature(sidecar) == sidecar_before
    assert sidecar.read_bytes() == b"preserve this sidecar"
    assert not os.path.lexists(destination.final_path)


def test_staged_partial_attestation_requires_a_preexisting_matching_marker(
    tmp_path: Path,
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    reservation = _reservation(destination)
    destination.partial_path.write_bytes(b"complete payload bytes")
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    partial_before = _entry_signature(destination.partial_path)
    sidecar_before = _entry_signature(sidecar)

    with pytest.raises(paths.PathValidationError, match="marker is missing"):
        paths.attest_staged_partial_payload(destination, reservation)

    assert not os.path.lexists(_marker_path(destination))
    assert _entry_signature(destination.partial_path) == partial_before
    assert destination.partial_path.read_bytes() == b"complete payload bytes"
    assert _entry_signature(sidecar) == sidecar_before
    assert sidecar.read_bytes() == b"preserve this sidecar"
    assert not os.path.lexists(destination.final_path)


@pytest.mark.parametrize("shape", ("symlink", "hardlink", "directory", "fifo"))
def test_staged_partial_attestation_rejects_unsafe_partial_shapes_without_mutation(
    tmp_path: Path, shape: str
) -> None:
    paths = _paths()
    destination = _resolve(paths, _root())
    reservation = _reservation(destination)
    paths.attest_publication_reservation_marker(destination, reservation)
    marker = _marker_path(destination)
    partial = destination.partial_path
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    outside = tmp_path / f"outside-partial-{shape}"

    if shape == "symlink":
        outside.write_bytes(b"outside payload bytes")
        partial.symlink_to(outside)
    elif shape == "hardlink":
        outside.write_bytes(b"outside payload bytes")
        os.link(outside, partial)
    elif shape == "directory":
        partial.mkdir(mode=0o700)
    else:
        os.mkfifo(partial, 0o600)

    marker_before = _entry_signature(marker)
    marker_bytes = marker.read_bytes()
    partial_before = _entry_signature(partial)
    sidecar_before = _entry_signature(sidecar)
    outside_before = _entry_signature(outside) if outside.exists() else None

    with pytest.raises(paths.PathValidationError):
        paths.attest_staged_partial_payload(destination, reservation)

    assert _entry_signature(marker) == marker_before
    assert marker.read_bytes() == marker_bytes
    assert _entry_signature(partial) == partial_before
    assert _entry_signature(sidecar) == sidecar_before
    assert sidecar.read_bytes() == b"preserve this sidecar"
    if outside_before is not None:
        assert _entry_signature(outside) == outside_before
        assert outside.read_bytes() == b"outside payload bytes"
    assert not os.path.lexists(destination.final_path)


def test_staged_partial_attestation_rejects_partial_replacement_during_file_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    destination, reservation = _prepare_staged_partial(paths)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    partial_before = _entry_signature(destination.partial_path)
    marker_before = _entry_signature(marker)
    marker_bytes = marker.read_bytes()
    sidecar_before = _entry_signature(sidecar)
    replacement = destination.incomplete_dir / "attacker-replacement"
    replacement.write_bytes(b"attacker replacement bytes")
    original_fsync = paths.os.fsync
    replaced = False

    def replace_partial_during_sync(descriptor: int) -> None:
        nonlocal replaced
        details = os.fstat(descriptor)
        if (details.st_dev, details.st_ino) == partial_before[:2]:
            os.replace(replacement, destination.partial_path)
            replaced = True
        original_fsync(descriptor)

    monkeypatch.setattr(paths.os, "fsync", replace_partial_during_sync)

    with pytest.raises(paths.UnsafePathError, match="partial payload changed"):
        paths.attest_staged_partial_payload(destination, reservation)

    assert replaced
    assert _entry_signature(marker) == marker_before
    assert marker.read_bytes() == marker_bytes
    assert _entry_signature(destination.partial_path) != partial_before
    assert destination.partial_path.read_bytes() == b"attacker replacement bytes"
    assert _entry_signature(sidecar) == sidecar_before
    assert sidecar.read_bytes() == b"preserve this sidecar"
    assert not os.path.lexists(destination.final_path)


def test_staged_partial_attestation_rejects_in_place_metadata_change_during_file_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    payload = b"complete payload bytes"
    destination, reservation = _prepare_staged_partial(paths, payload)
    marker = _marker_path(destination)
    partial_before = os.lstat(destination.partial_path)
    marker_before = _entry_signature(marker)
    marker_bytes = marker.read_bytes()
    original_fsync = paths.os.fsync
    changed = False

    def change_metadata_during_sync(descriptor: int) -> None:
        nonlocal changed
        details = os.fstat(descriptor)
        if (details.st_dev, details.st_ino) == (
            partial_before.st_dev,
            partial_before.st_ino,
        ):
            os.utime(
                destination.partial_path,
                ns=(partial_before.st_atime_ns, partial_before.st_mtime_ns + 1_000_000_000),
            )
            changed = True
        original_fsync(descriptor)

    monkeypatch.setattr(paths.os, "fsync", change_metadata_during_sync)

    with pytest.raises(paths.UnsafePathError, match="partial payload changed"):
        paths.attest_staged_partial_payload(destination, reservation)

    assert changed
    assert destination.partial_path.read_bytes() == payload
    assert os.lstat(destination.partial_path).st_mtime_ns != partial_before.st_mtime_ns
    assert _entry_signature(marker) == marker_before
    assert marker.read_bytes() == marker_bytes
    assert not os.path.lexists(destination.final_path)


def test_staged_partial_attestation_rejects_existing_final_without_mutation(
    tmp_path: Path,
) -> None:
    paths = _paths()
    destination, reservation = _prepare_staged_partial(paths)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    destination.final_path.write_bytes(b"ordinary preexisting final")
    final_before = _entry_signature(destination.final_path)
    partial_before = _entry_signature(destination.partial_path)
    marker_before = _entry_signature(marker)
    marker_bytes = marker.read_bytes()
    sidecar_before = _entry_signature(sidecar)

    with pytest.raises(paths.FinalPathCollisionError):
        paths.attest_staged_partial_payload(destination, reservation)

    assert _entry_signature(destination.final_path) == final_before
    assert destination.final_path.read_bytes() == b"ordinary preexisting final"
    assert _entry_signature(destination.partial_path) == partial_before
    assert destination.partial_path.read_bytes() == b"complete payload bytes"
    assert _entry_signature(marker) == marker_before
    assert marker.read_bytes() == marker_bytes
    assert _entry_signature(sidecar) == sidecar_before
    assert sidecar.read_bytes() == b"preserve this sidecar"


def test_staged_partial_attestation_rejects_final_created_during_file_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    destination, reservation = _prepare_staged_partial(paths)
    marker = _marker_path(destination)
    partial_before = _entry_signature(destination.partial_path)
    marker_before = _entry_signature(marker)
    marker_bytes = marker.read_bytes()
    original_fsync = paths.os.fsync
    created = False

    def create_final_during_sync(descriptor: int) -> None:
        nonlocal created
        details = os.fstat(descriptor)
        if (details.st_dev, details.st_ino) == partial_before[:2]:
            destination.final_path.write_bytes(b"racing final bytes")
            created = True
        original_fsync(descriptor)

    monkeypatch.setattr(paths.os, "fsync", create_final_during_sync)

    with pytest.raises(paths.FinalPathCollisionError):
        paths.attest_staged_partial_payload(destination, reservation)

    assert created
    assert destination.final_path.read_bytes() == b"racing final bytes"
    assert _entry_signature(destination.partial_path) == partial_before
    assert destination.partial_path.read_bytes() == b"complete payload bytes"
    assert _entry_signature(marker) == marker_before
    assert marker.read_bytes() == marker_bytes


def test_staged_partial_attestation_rejects_partial_replacement_after_marker_reattest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    destination, reservation = _prepare_staged_partial(paths)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    partial_before = _entry_signature(destination.partial_path)
    marker_before = _entry_signature(marker)
    marker_bytes = marker.read_bytes()
    sidecar_before = _entry_signature(sidecar)
    replacement = destination.incomplete_dir / "attacker-replacement"
    replacement.write_bytes(b"attacker replacement bytes")
    original_verify = paths._verify_attested_reservation_marker
    replaced = False

    def replace_partial_after_marker_verification(*args, **kwargs) -> None:
        nonlocal replaced
        original_verify(*args, **kwargs)
        os.replace(replacement, destination.partial_path)
        replaced = True

    monkeypatch.setattr(
        paths,
        "_verify_attested_reservation_marker",
        replace_partial_after_marker_verification,
    )

    with pytest.raises(paths.UnsafePathError, match="partial payload changed"):
        paths.attest_staged_partial_payload(destination, reservation)

    assert replaced
    assert _entry_signature(marker) == marker_before
    assert marker.read_bytes() == marker_bytes
    assert _entry_signature(destination.partial_path) != partial_before
    assert destination.partial_path.read_bytes() == b"attacker replacement bytes"
    assert _entry_signature(sidecar) == sidecar_before
    assert sidecar.read_bytes() == b"preserve this sidecar"
    assert not os.path.lexists(destination.final_path)


def test_staged_partial_attestation_rejects_marker_mismatch_without_touching_payload(
    tmp_path: Path,
) -> None:
    paths = _paths()
    destination, reservation = _prepare_staged_partial(paths)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    marker_bytes = marker.read_bytes()
    partial_before = _entry_signature(destination.partial_path)
    sidecar_before = _entry_signature(sidecar)

    with pytest.raises(paths.PathValidationError, match="marker contents do not match"):
        paths.attest_staged_partial_payload(
            destination,
            _reservation(destination, claim_token="b" * 64),
        )

    assert _entry_signature(marker) == marker_before
    assert marker.read_bytes() == marker_bytes
    assert _entry_signature(destination.partial_path) == partial_before
    assert destination.partial_path.read_bytes() == b"complete payload bytes"
    assert _entry_signature(sidecar) == sidecar_before
    assert sidecar.read_bytes() == b"preserve this sidecar"
    assert not os.path.lexists(destination.final_path)


@pytest.mark.parametrize("failure_target", ("partial", "job-directory"))
def test_staged_partial_attestation_fsync_failure_preserves_all_entries(
    monkeypatch: pytest.MonkeyPatch, failure_target: str
) -> None:
    paths = _paths()
    destination, reservation = _prepare_staged_partial(paths)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    marker_bytes = marker.read_bytes()
    partial_before = _entry_signature(destination.partial_path)
    sidecar_before = _entry_signature(sidecar)
    job_identity = _entry_signature(destination.incomplete_dir)[:2]
    original_fsync = paths.os.fsync
    injected = 0

    def fail_selected_sync(descriptor: int) -> None:
        nonlocal injected
        details = os.fstat(descriptor)
        identity = (details.st_dev, details.st_ino)
        should_fail = (
            failure_target == "partial" and identity == partial_before[:2]
        ) or (failure_target == "job-directory" and identity == job_identity)
        if should_fail:
            injected += 1
            raise OSError("injected staged payload sync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(paths.os, "fsync", fail_selected_sync)

    with pytest.raises(paths.PathValidationError):
        paths.attest_staged_partial_payload(destination, reservation)

    assert injected == 1
    assert _entry_signature(marker) == marker_before
    assert marker.read_bytes() == marker_bytes
    assert _entry_signature(destination.partial_path) == partial_before
    assert destination.partial_path.read_bytes() == b"complete payload bytes"
    assert _entry_signature(sidecar) == sidecar_before
    assert sidecar.read_bytes() == b"preserve this sidecar"
    assert not os.path.lexists(destination.final_path)


def _attest_prepared_staged_partial(
    paths, payload: bytes = b"complete payload bytes"
):
    destination, reservation = _prepare_staged_partial(paths, payload)
    return (
        destination,
        reservation,
        paths.attest_staged_partial_payload(destination, reservation),
    )


def test_publishes_attested_staged_payload_with_a_descriptor_relative_hard_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    payload = b"complete payload bytes"
    destination, reservation, staged = _attest_prepared_staged_partial(paths, payload)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    partial_before = _entry_signature(destination.partial_path)
    sidecar_before = _entry_signature(sidecar)
    final_directory_identity = _entry_signature(destination.final_path.parent)[:2]
    events: list[tuple[object, ...]] = []
    original_link = paths.os.link
    original_fsync = paths.os.fsync

    def record_link(
        source: object,
        target: object,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        events.append(
            (
                "link",
                source,
                target,
                src_dir_fd,
                dst_dir_fd,
                follow_symlinks,
            )
        )
        original_link(
            source,
            target,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    def record_fsync(descriptor: int) -> None:
        details = os.fstat(descriptor)
        events.append(("fsync", details.st_dev, details.st_ino, stat.S_IFMT(details.st_mode)))
        original_fsync(descriptor)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("publication must not resolve, rename, or delete a payload")

    monkeypatch.setattr(paths.os, "link", record_link)
    monkeypatch.setattr(paths.os, "fsync", record_fsync)
    monkeypatch.setattr(paths, "resolve_destination", forbidden)
    monkeypatch.setattr(paths, "_select_available_name", forbidden)
    monkeypatch.setattr(paths, "claim_final_path", forbidden)
    monkeypatch.setattr(paths.os, "replace", forbidden)
    monkeypatch.setattr(paths.os, "rename", forbidden)
    monkeypatch.setattr(paths.os, "unlink", forbidden)
    monkeypatch.setattr(paths.os, "remove", forbidden)
    monkeypatch.setattr(paths.os, "truncate", forbidden)

    published = paths.publish_staged_partial_payload(destination, reservation, staged)

    final_details = os.lstat(destination.final_path)
    partial_details = os.lstat(destination.partial_path)
    assert "PublishedFinalPayload" in paths.__all__
    assert "publish_staged_partial_payload" in paths.__all__
    assert tuple(paths.PublishedFinalPayload.__dataclass_fields__) == (
        "path",
        "st_dev",
        "st_ino",
        "logical_size",
    )
    assert published == paths.PublishedFinalPayload(
        path=destination.final_path,
        st_dev=staged.st_dev,
        st_ino=staged.st_ino,
        logical_size=len(payload),
    )
    assert reservation.claim_token not in repr(published)
    assert stat.S_ISREG(final_details.st_mode)
    assert (final_details.st_dev, final_details.st_ino, final_details.st_size) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert (partial_details.st_dev, partial_details.st_ino, partial_details.st_size) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert final_details.st_nlink == partial_details.st_nlink == 2
    assert destination.final_path.read_bytes() == payload
    assert destination.partial_path.read_bytes() == payload
    assert _entry_signature(marker) == marker_before
    assert _entry_signature(sidecar) == sidecar_before
    assert sidecar.read_bytes() == b"preserve this sidecar"
    assert partial_before[:3] == _entry_signature(destination.partial_path)[:3]

    link_event = next(event for event in events if event[0] == "link")
    assert link_event[1] == destination.partial_path.name
    assert link_event[2] == destination.final_path.name
    assert link_event[3] is not None
    assert link_event[4] is not None
    assert link_event[5] is False
    link_index = events.index(link_event)
    marker_sync_index = next(
        index
        for index, event in enumerate(events)
        if event[:3] == ("fsync", marker_before[0], marker_before[1])
    )
    final_directory_sync_index = next(
        index
        for index, event in enumerate(events)
        if event[:3] == ("fsync", *final_directory_identity)
    )
    assert marker_sync_index < link_index < final_directory_sync_index


@pytest.mark.parametrize(
    "shape",
    (
        "same-bytes-new-inode",
        "symlink",
        "directory",
        "fifo",
        "hardlink-to-unrelated",
    ),
)
def test_publication_never_overwrites_or_accepts_an_unrelated_existing_final(
    tmp_path: Path, shape: str
) -> None:
    paths = _paths()
    payload = b"complete payload bytes"
    destination, reservation, staged = _attest_prepared_staged_partial(paths, payload)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    outside = tmp_path / f"outside-final-{shape}"

    if shape == "same-bytes-new-inode":
        destination.final_path.write_bytes(payload)
    elif shape == "symlink":
        outside.write_bytes(payload)
        destination.final_path.symlink_to(outside)
    elif shape == "directory":
        destination.final_path.mkdir(mode=0o700)
    elif shape == "fifo":
        os.mkfifo(destination.final_path, 0o600)
    else:
        outside.write_bytes(payload)
        os.link(outside, destination.final_path)

    final_before = _entry_signature(destination.final_path)
    marker_before = _entry_signature(marker)
    partial_before = _entry_signature(destination.partial_path)
    sidecar_before = _entry_signature(sidecar)
    outside_before = _entry_signature(outside) if os.path.lexists(outside) else None

    with pytest.raises(paths.PathValidationError) as raised:
        paths.publish_staged_partial_payload(destination, reservation, staged)

    assert reservation.claim_token not in str(raised.value)
    assert _entry_signature(destination.final_path) == final_before
    assert _entry_signature(marker) == marker_before
    assert _entry_signature(destination.partial_path) == partial_before
    assert _entry_signature(sidecar) == sidecar_before
    assert destination.partial_path.read_bytes() == payload
    assert sidecar.read_bytes() == b"preserve this sidecar"
    if outside_before is not None:
        assert _entry_signature(outside) == outside_before
        assert outside.read_bytes() == payload


def test_publication_retries_an_exact_link_after_final_directory_sync_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    payload = b"complete payload bytes"
    destination, reservation, staged = _attest_prepared_staged_partial(paths, payload)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    sidecar_before = _entry_signature(sidecar)
    final_directory_identity = _entry_signature(destination.final_path.parent)[:2]
    original_fsync = paths.os.fsync
    marker_syncs = 0
    final_syncs = 0

    def fail_first_final_directory_sync(descriptor: int) -> None:
        nonlocal marker_syncs, final_syncs
        details = os.fstat(descriptor)
        identity = (details.st_dev, details.st_ino)
        if identity == marker_before[:2]:
            marker_syncs += 1
        if identity == final_directory_identity:
            final_syncs += 1
            if final_syncs == 1:
                raise OSError("injected final directory sync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(paths.os, "fsync", fail_first_final_directory_sync)

    with pytest.raises(paths.PathValidationError) as raised:
        paths.publish_staged_partial_payload(destination, reservation, staged)

    assert reservation.claim_token not in str(raised.value)
    assert final_syncs == 1
    assert marker_syncs == 1
    assert os.path.lexists(destination.final_path)
    assert _payload_identity(destination.final_path) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert _payload_identity(destination.partial_path) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert os.lstat(destination.final_path).st_nlink == 2
    assert _entry_signature(marker) == marker_before
    assert _entry_signature(sidecar) == sidecar_before
    assert sidecar.read_bytes() == b"preserve this sidecar"

    retried = paths.publish_staged_partial_payload(destination, reservation, staged)

    assert retried.st_dev == staged.st_dev
    assert retried.st_ino == staged.st_ino
    assert retried.logical_size == staged.logical_size
    assert retried.path == destination.final_path
    assert marker_syncs == 2
    assert final_syncs == 2
    assert _payload_identity(destination.final_path) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert _payload_identity(destination.partial_path) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert _entry_signature(marker) == marker_before
    assert _entry_signature(sidecar) == sidecar_before


def test_publication_does_not_link_when_marker_reattest_sync_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    destination, reservation, staged = _attest_prepared_staged_partial(paths)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    partial_before = _entry_signature(destination.partial_path)
    sidecar_before = _entry_signature(sidecar)
    original_fsync = paths.os.fsync
    marker_syncs = 0

    def fail_marker_sync(descriptor: int) -> None:
        nonlocal marker_syncs
        details = os.fstat(descriptor)
        if (details.st_dev, details.st_ino) == marker_before[:2]:
            marker_syncs += 1
            raise OSError("injected marker sync failure")
        original_fsync(descriptor)

    def forbidden_link(*_args, **_kwargs) -> None:
        pytest.fail("publication must not link after a marker reattestation failure")

    monkeypatch.setattr(paths.os, "fsync", fail_marker_sync)
    monkeypatch.setattr(paths.os, "link", forbidden_link)

    with pytest.raises(paths.PathValidationError) as raised:
        paths.publish_staged_partial_payload(destination, reservation, staged)

    assert reservation.claim_token not in str(raised.value)
    assert marker_syncs == 1
    assert not os.path.lexists(destination.final_path)
    assert _entry_signature(marker) == marker_before
    assert _entry_signature(destination.partial_path) == partial_before
    assert _entry_signature(sidecar) == sidecar_before
    assert destination.partial_path.read_bytes() == b"complete payload bytes"
    assert sidecar.read_bytes() == b"preserve this sidecar"


def test_publication_requires_a_matching_marker_and_exact_staged_identity() -> None:
    paths = _paths()
    destination, reservation, staged = _attest_prepared_staged_partial(paths)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    partial_before = _entry_signature(destination.partial_path)
    sidecar_before = _entry_signature(sidecar)
    mismatched_reservation = _reservation(destination, claim_token="b" * 64)
    mismatched_staged = replace(staged, logical_size=staged.logical_size + 1)

    with pytest.raises(paths.PathValidationError) as marker_raised:
        paths.publish_staged_partial_payload(
            destination,
            mismatched_reservation,
            staged,
        )
    with pytest.raises(paths.PathValidationError) as staged_raised:
        paths.publish_staged_partial_payload(destination, reservation, mismatched_staged)

    assert reservation.claim_token not in str(marker_raised.value)
    assert mismatched_reservation.claim_token not in str(marker_raised.value)
    assert reservation.claim_token not in str(staged_raised.value)
    assert _entry_signature(marker) == marker_before
    assert _entry_signature(destination.partial_path) == partial_before
    assert _entry_signature(sidecar) == sidecar_before
    assert destination.partial_path.read_bytes() == b"complete payload bytes"
    assert sidecar.read_bytes() == b"preserve this sidecar"
    assert not os.path.lexists(destination.final_path)


def test_publication_retains_the_link_when_post_link_final_verification_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    destination, reservation, staged = _attest_prepared_staged_partial(paths)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    sidecar_before = _entry_signature(sidecar)
    final_directory_identity = _entry_signature(destination.final_path.parent)[:2]
    original_link = paths.os.link
    original_stat = paths.os.stat
    linked = False

    def record_link(*args, **kwargs) -> None:
        nonlocal linked
        original_link(*args, **kwargs)
        linked = True

    def fail_post_link_final_stat(path, *args, **kwargs):
        if linked and path == destination.final_path.name:
            descriptor = kwargs.get("dir_fd")
            if descriptor is not None:
                details = os.fstat(descriptor)
                if (details.st_dev, details.st_ino) == final_directory_identity:
                    raise OSError("injected final verification failure")
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(paths.os, "link", record_link)
    monkeypatch.setattr(paths.os, "stat", fail_post_link_final_stat)

    with pytest.raises(paths.PathValidationError) as raised:
        paths.publish_staged_partial_payload(destination, reservation, staged)

    assert linked
    assert reservation.claim_token not in str(raised.value)
    assert _payload_identity(destination.final_path) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert _payload_identity(destination.partial_path) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert os.lstat(destination.final_path).st_nlink == 2
    assert _entry_signature(marker) == marker_before
    assert _entry_signature(sidecar) == sidecar_before

    monkeypatch.setattr(paths.os, "link", original_link)
    monkeypatch.setattr(paths.os, "stat", original_stat)
    retried = paths.publish_staged_partial_payload(destination, reservation, staged)

    assert retried == paths.PublishedFinalPayload(
        path=destination.final_path,
        st_dev=staged.st_dev,
        st_ino=staged.st_ino,
        logical_size=staged.logical_size,
    )
    assert _payload_identity(destination.final_path) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert _payload_identity(destination.partial_path) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )


def test_publication_rejects_a_final_replaced_after_link_without_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    payload = b"complete payload bytes"
    destination, reservation, staged = _attest_prepared_staged_partial(paths, payload)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    sidecar_before = _entry_signature(sidecar)
    replacement = destination.final_path.parent / "attacker-replacement"
    replacement.write_bytes(b"attacker replacement bytes")
    original_link = paths.os.link
    replaced = False

    def link_then_replace_final(*args, **kwargs) -> None:
        nonlocal replaced
        original_link(*args, **kwargs)
        os.replace(replacement, destination.final_path)
        replaced = True

    monkeypatch.setattr(paths.os, "link", link_then_replace_final)

    with pytest.raises(paths.PathValidationError) as raised:
        paths.publish_staged_partial_payload(destination, reservation, staged)

    assert replaced
    assert reservation.claim_token not in str(raised.value)
    assert destination.final_path.read_bytes() == b"attacker replacement bytes"
    assert _payload_identity(destination.final_path)[:2] != (staged.st_dev, staged.st_ino)
    assert destination.partial_path.read_bytes() == payload
    assert _payload_identity(destination.partial_path)[:2] == (staged.st_dev, staged.st_ino)
    assert _entry_signature(marker) == marker_before
    assert _entry_signature(sidecar) == sidecar_before


def test_publication_rejects_a_final_replaced_during_post_sync_source_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    payload = b"complete payload bytes"
    destination, reservation, staged = _attest_prepared_staged_partial(paths, payload)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    sidecar_before = _entry_signature(sidecar)
    final_directory_identity = _entry_signature(destination.final_path.parent)[:2]
    job_directory_identity = _entry_signature(destination.incomplete_dir)[:2]
    replacement = destination.final_path.parent / "attacker-replacement"
    replacement.write_bytes(b"attacker replacement bytes")
    original_fsync = paths.os.fsync
    original_verify = paths._verify_visible_publication_payload
    final_directory_synced = False
    replaced = False

    def record_final_directory_sync(descriptor: int) -> None:
        nonlocal final_directory_synced
        original_fsync(descriptor)
        details = os.fstat(descriptor)
        if (details.st_dev, details.st_ino) == final_directory_identity:
            final_directory_synced = True

    def replace_final_after_source_verification(
        parent_fd: int,
        name: str,
        expected: tuple[int, int, int],
        *,
        minimum_links: int,
    ) -> None:
        nonlocal replaced
        original_verify(parent_fd, name, expected, minimum_links=minimum_links)
        details = os.fstat(parent_fd)
        if (
            final_directory_synced
            and not replaced
            and (details.st_dev, details.st_ino) == job_directory_identity
        ):
            os.replace(replacement, destination.final_path)
            replaced = True

    monkeypatch.setattr(paths.os, "fsync", record_final_directory_sync)
    monkeypatch.setattr(
        paths,
        "_verify_visible_publication_payload",
        replace_final_after_source_verification,
    )

    with pytest.raises(paths.PathValidationError) as raised:
        paths.publish_staged_partial_payload(destination, reservation, staged)

    assert replaced
    assert reservation.claim_token not in str(raised.value)
    assert destination.final_path.read_bytes() == b"attacker replacement bytes"
    assert _payload_identity(destination.final_path)[:2] != (staged.st_dev, staged.st_ino)
    assert _payload_identity(destination.partial_path) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert _entry_signature(marker) == marker_before
    assert _entry_signature(sidecar) == sidecar_before


def test_publication_rejects_a_final_parent_replaced_during_post_sync_source_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    destination, reservation, staged = _attest_prepared_staged_partial(paths)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    sidecar_before = _entry_signature(sidecar)
    final_directory = destination.final_path.parent
    final_directory_identity = _entry_signature(final_directory)[:2]
    job_directory_identity = _entry_signature(destination.incomplete_dir)[:2]
    detached_directory = destination.root / "detached-final-directory"
    original_fsync = paths.os.fsync
    original_verify = paths._verify_visible_publication_payload
    final_directory_synced = False
    replaced = False

    def record_final_directory_sync(descriptor: int) -> None:
        nonlocal final_directory_synced
        original_fsync(descriptor)
        details = os.fstat(descriptor)
        if (details.st_dev, details.st_ino) == final_directory_identity:
            final_directory_synced = True

    def replace_final_parent_after_source_verification(
        parent_fd: int,
        name: str,
        expected: tuple[int, int, int],
        *,
        minimum_links: int,
    ) -> None:
        nonlocal replaced
        original_verify(parent_fd, name, expected, minimum_links=minimum_links)
        details = os.fstat(parent_fd)
        if (
            final_directory_synced
            and not replaced
            and (details.st_dev, details.st_ino) == job_directory_identity
        ):
            os.rename(final_directory, detached_directory)
            final_directory.mkdir(mode=0o700)
            replaced = True

    monkeypatch.setattr(paths.os, "fsync", record_final_directory_sync)
    monkeypatch.setattr(
        paths,
        "_verify_visible_publication_payload",
        replace_final_parent_after_source_verification,
    )

    with pytest.raises(paths.PathValidationError) as raised:
        paths.publish_staged_partial_payload(destination, reservation, staged)

    assert replaced
    assert reservation.claim_token not in str(raised.value)
    assert _payload_identity(detached_directory / destination.final_path.name) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert not os.path.lexists(destination.final_path)
    assert _payload_identity(destination.partial_path) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert _entry_signature(marker) == marker_before
    assert _entry_signature(sidecar) == sidecar_before


def test_publication_rejects_a_partial_replaced_between_validation_and_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths()
    destination, reservation, staged = _attest_prepared_staged_partial(paths)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    sidecar_before = _entry_signature(sidecar)
    replacement = destination.incomplete_dir / "attacker-replacement"
    replacement.write_bytes(b"attacker replacement bytes")
    original_link = paths.os.link
    replaced = False

    def replace_partial_then_link(*args, **kwargs) -> None:
        nonlocal replaced
        os.replace(replacement, destination.partial_path)
        replaced = True
        original_link(*args, **kwargs)

    monkeypatch.setattr(paths.os, "link", replace_partial_then_link)

    with pytest.raises(paths.PathValidationError) as raised:
        paths.publish_staged_partial_payload(destination, reservation, staged)

    assert replaced
    assert reservation.claim_token not in str(raised.value)
    assert destination.partial_path.read_bytes() == b"attacker replacement bytes"
    assert destination.final_path.read_bytes() == b"attacker replacement bytes"
    assert _payload_identity(destination.partial_path)[:2] == _payload_identity(
        destination.final_path
    )[:2]
    assert _payload_identity(destination.partial_path)[:2] != (staged.st_dev, staged.st_ino)
    assert _entry_signature(marker) == marker_before
    assert _entry_signature(sidecar) == sidecar_before


@pytest.mark.parametrize("replaced_chain", ("final", "job"))
def test_publication_rejects_a_replaced_parent_before_accepting_visibility(
    monkeypatch: pytest.MonkeyPatch,
    replaced_chain: str,
) -> None:
    paths = _paths()
    destination, reservation, staged = _attest_prepared_staged_partial(paths)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    sidecar_before = _entry_signature(sidecar)
    if replaced_chain == "final":
        original_directory = destination.final_path.parent
        detached_directory = destination.root / "detached-final-directory"
    else:
        original_directory = destination.incomplete_dir
        detached_directory = destination.incomplete_dir.parent / "detached-job-directory"
    original_link = paths.os.link
    replaced = False

    def link_then_replace_parent(*args, **kwargs) -> None:
        nonlocal replaced
        original_link(*args, **kwargs)
        os.rename(original_directory, detached_directory)
        original_directory.mkdir(mode=0o700)
        replaced = True

    monkeypatch.setattr(paths.os, "link", link_then_replace_parent)

    with pytest.raises(paths.PathValidationError) as raised:
        paths.publish_staged_partial_payload(destination, reservation, staged)

    assert replaced
    assert reservation.claim_token not in str(raised.value)
    detached_final = (
        detached_directory / destination.final_path.name
        if replaced_chain == "final"
        else destination.final_path
    )
    assert _payload_identity(detached_final) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    if replaced_chain == "final":
        assert not os.path.lexists(destination.final_path)
        assert _payload_identity(destination.partial_path) == (
            staged.st_dev,
            staged.st_ino,
            staged.logical_size,
        )
        assert _entry_signature(marker) == marker_before
        assert _entry_signature(sidecar) == sidecar_before
    else:
        detached_marker = detached_directory / marker.name
        detached_sidecar = detached_directory / sidecar.name
        detached_partial = detached_directory / destination.partial_path.name
        assert _entry_signature(detached_marker) == marker_before
        assert _entry_signature(detached_sidecar) == sidecar_before
        assert _payload_identity(detached_partial) == (
            staged.st_dev,
            staged.st_ino,
            staged.logical_size,
        )
        assert not os.path.lexists(destination.partial_path)


@pytest.mark.parametrize("replaced_chain", ("root", "incomplete"))
def test_publication_rejects_a_replaced_root_or_incomplete_ancestor_before_accepting_visibility(
    monkeypatch: pytest.MonkeyPatch,
    replaced_chain: str,
) -> None:
    paths = _paths()
    destination, reservation, staged = _attest_prepared_staged_partial(paths)
    marker = _marker_path(destination)
    sidecar = destination.incomplete_dir / "owned.sidecar"
    sidecar.write_bytes(b"preserve this sidecar")
    marker_before = _entry_signature(marker)
    sidecar_before = _entry_signature(sidecar)
    if replaced_chain == "root":
        original_directory = destination.root
        detached_directory = destination.root.parent / "detached-root"
        detached_final = (
            detached_directory
            / destination.final_path.parent.name
            / destination.final_path.name
        )
        detached_partial = (
            detached_directory
            / ".incomplete"
            / destination.job_id
            / destination.partial_path.name
        )
    else:
        original_directory = destination.incomplete_dir.parent
        detached_directory = destination.root / "detached-incomplete-directory"
        detached_final = destination.final_path
        detached_partial = detached_directory / destination.job_id / destination.partial_path.name
    detached_marker = detached_partial.parent / marker.name
    detached_sidecar = detached_partial.parent / sidecar.name
    original_link = paths.os.link
    replaced = False

    def link_then_replace_ancestor(*args, **kwargs) -> None:
        nonlocal replaced
        original_link(*args, **kwargs)
        os.rename(original_directory, detached_directory)
        original_directory.mkdir(mode=0o700)
        replaced = True

    monkeypatch.setattr(paths.os, "link", link_then_replace_ancestor)

    with pytest.raises(paths.PathValidationError) as raised:
        paths.publish_staged_partial_payload(destination, reservation, staged)

    assert replaced
    assert reservation.claim_token not in str(raised.value)
    assert _payload_identity(detached_final) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert _payload_identity(detached_partial) == (
        staged.st_dev,
        staged.st_ino,
        staged.logical_size,
    )
    assert _entry_signature(detached_marker) == marker_before
    assert _entry_signature(detached_sidecar) == sidecar_before
    assert not os.path.lexists(destination.partial_path)
    if replaced_chain == "root":
        assert not os.path.lexists(destination.final_path)
    else:
        assert _payload_identity(destination.final_path) == (
            staged.st_dev,
            staged.st_ino,
            staged.logical_size,
        )


@pytest.mark.parametrize("final_state", ("existing", "absent", "replacement"))
def test_existing_only_publication_never_attempts_a_link(monkeypatch, final_state):
    paths = _paths()
    destination, reservation, staged = _attest_prepared_staged_partial(paths)
    paths.publish_staged_partial_payload(destination, reservation, staged)
    if final_state != "existing":
        destination.final_path.unlink()
    if final_state == "replacement":
        destination.final_path.write_bytes(b"replacement")
    partial_before = _entry_signature(destination.partial_path)
    marker_before = _entry_signature(_marker_path(destination))
    link_calls = []

    def forbidden_link(*args, **kwargs):
        link_calls.append("attempted")
        raise AssertionError("existing-only verification must not link")

    monkeypatch.setattr(paths.os, "link", forbidden_link)
    if final_state == "existing":
        result = paths.publish_staged_partial_payload(destination, reservation, staged, existing_only=True)
        assert (result.st_dev, result.st_ino) == (staged.st_dev, staged.st_ino)
    else:
        with pytest.raises(paths.PathValidationError):
            paths.publish_staged_partial_payload(destination, reservation, staged, existing_only=True)
        if final_state == "absent":
            assert not destination.final_path.exists()
        else:
            assert destination.final_path.read_bytes() == b"replacement"
    assert link_calls == []
    assert _entry_signature(destination.partial_path) == partial_before
    assert _entry_signature(_marker_path(destination)) == marker_before


@pytest.mark.parametrize("invalid", (None, 0, 1, "true"))
def test_existing_only_option_is_validated_before_namespace_operations(monkeypatch, invalid):
    paths = _paths()
    destination, reservation, staged = _attest_prepared_staged_partial(paths)

    def forbidden_open(*args, **kwargs):
        raise AssertionError("invalid option must fail before opening paths")

    monkeypatch.setattr(paths, "_open_root", forbidden_open)
    with pytest.raises(paths.PathValidationError):
        paths.publish_staged_partial_payload(destination, reservation, staged, existing_only=invalid)
