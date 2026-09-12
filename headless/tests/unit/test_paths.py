"""Behavioral contract for safe deterministic download destinations."""

from __future__ import annotations

import importlib
import importlib.util
import os
from pathlib import Path

import pytest


CATEGORIES = ("Videos", "Audio", "Documents", "Software", "Other")


def _paths():
    spec = importlib.util.find_spec("hermes_downloads.paths")
    assert spec is not None, "hermes_downloads.paths must provide output path validation"
    return importlib.import_module("hermes_downloads.paths")


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "Downloads" / "Hermes"
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
    root = _root(tmp_path)

    destination = _resolve(paths, root, category=category)

    assert destination.root == root
    assert destination.final_path == root / category / "selected.webm"
    assert destination.incomplete_dir == root / ".incomplete" / "job-42"
    assert destination.partial_path == root / ".incomplete" / "job-42" / "selected.webm"
    assert destination.final_path.parent.is_dir()
    assert destination.incomplete_dir.is_dir()


def test_explicit_collection_takes_precedence_over_type_category(tmp_path: Path) -> None:
    paths = _paths()
    root = _root(tmp_path)

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
    root = _root(tmp_path)
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
    root = _root(tmp_path)

    with pytest.raises(paths.PathValidationError):
        _resolve(paths, root, **{field: value})


@pytest.mark.parametrize("category", ("video", "Archive", ".incomplete", ""))
def test_rejects_categories_outside_the_fixed_contract(
    tmp_path: Path, category: str
) -> None:
    paths = _paths()

    with pytest.raises(paths.PathValidationError):
        _resolve(paths, _root(tmp_path), category=category)


def test_rejects_a_destination_root_that_is_not_a_writable_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths()
    root = _root(tmp_path)
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
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(outside, target_is_directory=True)

    with pytest.raises(paths.UnsafePathError):
        _resolve(paths, linked_parent / "Hermes")


def test_rejects_existing_symlink_destination_and_incomplete_components(
    tmp_path: Path,
) -> None:
    paths = _paths()
    root = _root(tmp_path)
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
    root = _root(tmp_path)
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
    root = _root(tmp_path)
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
    root = _root(tmp_path)
    destination = _resolve(paths, root)
    destination.final_path.parent.mkdir(exist_ok=True)
    destination.final_path.write_bytes(b"already complete")

    with pytest.raises(paths.FinalPathCollisionError):
        paths.claim_final_path(destination)

    assert destination.final_path.read_bytes() == b"already complete"
