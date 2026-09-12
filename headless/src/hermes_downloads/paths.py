"""Safe, deterministic destination intent for Hermes downloads.

This module owns pathname validation and final-name reservation only.  It does
not write payload bytes, publish completed files, clean partials, or fsync.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import stat
from typing import Final
import unicodedata

__all__ = [
    "CATEGORIES",
    "DestinationIntent",
    "FinalPathCollisionError",
    "PathValidationError",
    "UnsafePathError",
    "claim_final_path",
    "resolve_destination",
]

CATEGORIES: Final = frozenset({"Videos", "Audio", "Documents", "Software", "Other"})
_INCOMPLETE: Final = ".incomplete"
_DIRECTORY_FLAGS: Final = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_CLAIM_FLAGS: Final = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC


class PathValidationError(ValueError):
    """A supplied destination cannot safely represent a managed output path."""


class UnsafePathError(PathValidationError):
    """An existing symlink or replaced path makes the destination unsafe."""


class FinalPathCollisionError(PathValidationError):
    """A final name is already occupied and cannot be overwritten."""


@dataclass(frozen=True, slots=True)
class DestinationIntent:
    """The exact final and job-owned incomplete locations for one download."""

    root: Path
    category: str
    collection: str | None
    filename: str
    job_id: str
    final_path: Path
    incomplete_dir: Path
    partial_path: Path


def resolve_destination(
    root: str | os.PathLike[str],
    *,
    category: str,
    filename: str,
    job_id: str,
    collection: str | None = None,
) -> DestinationIntent:
    """Validate and prepare a deterministic final and partial destination.

    ``root`` must already be the intended, writable Downloads/Hermes directory.
    No environment-derived fallback or implicit alternate root is considered.
    """

    root_path = _require_root(root)
    category = _require_category(category)
    filename = _require_component(filename, "filename")
    job_id = _require_component(job_id, "job_id")
    if collection is not None:
        collection = _require_component(collection, "collection")

    _require_safe_writable_root(root_path)
    final_component = collection or category
    final_path = root_path / final_component / filename
    incomplete_dir = root_path / _INCOMPLETE / job_id
    partial_path = incomplete_dir / filename

    root_fd = _open_root(root_path)
    try:
        final_fd = _open_or_create_directory(root_fd, final_component)
        try:
            incomplete_fd = _open_or_create_directory(root_fd, _INCOMPLETE)
            try:
                job_fd = _open_or_create_directory(incomplete_fd, job_id)
                try:
                    _require_writable_directory(final_path.parent)
                    _require_writable_directory(incomplete_dir)
                    _require_same_filesystem(final_fd, job_fd)
                    final_path = final_path.with_name(
                        _select_available_name(final_fd, filename, job_id)
                    )
                finally:
                    os.close(job_fd)
            finally:
                os.close(incomplete_fd)
        finally:
            os.close(final_fd)
    finally:
        os.close(root_fd)

    return DestinationIntent(
        root=root_path,
        category=category,
        collection=collection,
        filename=filename,
        job_id=job_id,
        final_path=final_path,
        incomplete_dir=incomplete_dir,
        partial_path=partial_path,
    )


def claim_final_path(destination: DestinationIntent) -> Path:
    """Atomically reserve a final name without overwriting an existing file.

    The resulting empty file is only a name claim.  Payload publication,
    ownership cleanup, and durability ordering are deliberately deferred.
    """

    root, final_component = _validate_destination_intent(destination)
    _require_safe_writable_root(root)

    root_fd = _open_root(root)
    try:
        final_fd = _open_existing_directory(root_fd, final_component)
        try:
            incomplete_fd = _open_existing_directory(root_fd, _INCOMPLETE)
            try:
                job_fd = _open_existing_directory(incomplete_fd, destination.job_id)
                try:
                    _require_writable_directory(destination.final_path.parent)
                    _require_writable_directory(destination.incomplete_dir)
                    _require_same_filesystem(final_fd, job_fd)
                    name = destination.final_path.name
                    _claim_name(final_fd, name)
                finally:
                    os.close(job_fd)
            finally:
                os.close(incomplete_fd)
        finally:
            os.close(final_fd)
    finally:
        os.close(root_fd)

    return destination.final_path.parent / name


def _validate_destination_intent(destination: DestinationIntent) -> tuple[Path, str]:
    if type(destination) is not DestinationIntent:
        raise TypeError("destination must be a DestinationIntent")

    root = _require_root(destination.root)
    category = _require_category(destination.category)
    filename = _require_component(destination.filename, "filename")
    job_id = _require_component(destination.job_id, "job_id")
    collection = destination.collection
    if collection is not None:
        collection = _require_component(collection, "collection")

    final_component = collection or category
    final_directory = root / final_component
    incomplete_dir = root / _INCOMPLETE / job_id
    allowed_names = {filename, _collision_name(filename, job_id)}
    if (
        destination.final_path.parent != final_directory
        or destination.final_path.name not in allowed_names
        or destination.incomplete_dir != incomplete_dir
        or destination.partial_path != incomplete_dir / filename
    ):
        raise PathValidationError("destination intent does not match the managed root")
    return root, final_component


def _require_root(value: str | os.PathLike[str]) -> Path:
    try:
        root = Path(value)
    except TypeError as error:
        raise TypeError("root must be a filesystem path") from error
    if not root.is_absolute():
        raise PathValidationError("root must be an absolute path")
    if any(part in {".", ".."} for part in root.parts):
        raise PathValidationError("root must not contain traversal components")
    return root


def _require_category(value: object) -> str:
    if type(value) is not str or value not in CATEGORIES:
        raise PathValidationError("category is not a supported output category")
    return value


def _require_component(value: object, name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not value or value in {".", ".."} or value.startswith("."):
        raise PathValidationError(f"{name} is not a valid output name")
    if "/" in value or "\\" in value or "\x00" in value:
        raise PathValidationError(f"{name} must be one path component")
    if any(unicodedata.category(character) in {"Cc", "Cs"} for character in value):
        raise PathValidationError(f"{name} contains a control character")
    return value


def _require_safe_writable_root(root: Path) -> None:
    for component in _root_components(root):
        _require_real_directory(component)
    if not os.access(root, os.W_OK | os.X_OK):
        raise PathValidationError("destination root is not writable")


def _root_components(root: Path) -> tuple[Path, ...]:
    current = Path(root.anchor)
    components: list[Path] = []
    for name in root.parts[1:]:
        current /= name
        components.append(current)
    return tuple(components)


def _require_real_directory(path: Path) -> os.stat_result:
    try:
        details = os.lstat(path)
    except FileNotFoundError as error:
        raise PathValidationError("destination directory does not exist") from error
    except OSError as error:
        raise PathValidationError("destination directory is inaccessible") from error
    if stat.S_ISLNK(details.st_mode):
        raise UnsafePathError("destination contains a symlink")
    if not stat.S_ISDIR(details.st_mode):
        raise PathValidationError("destination component is not a directory")
    return details


def _open_root(root: Path) -> int:
    expected = _require_real_directory(root)
    try:
        descriptor = os.open(root, _DIRECTORY_FLAGS)
    except OSError as error:
        raise PathValidationError("destination root is inaccessible") from error
    try:
        actual = os.fstat(descriptor)
        if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
            raise UnsafePathError("destination root changed during validation")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_or_create_directory(parent_fd: int, name: str) -> int:
    try:
        os.mkdir(name, 0o700, dir_fd=parent_fd)
    except FileExistsError:
        pass
    except OSError as error:
        raise PathValidationError("destination directory cannot be created") from error
    return _open_existing_directory(parent_fd, name)


def _open_existing_directory(parent_fd: int, name: str) -> int:
    try:
        details = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError as error:
        raise PathValidationError("managed destination directory is missing") from error
    except OSError as error:
        raise PathValidationError("managed destination directory is inaccessible") from error
    if stat.S_ISLNK(details.st_mode):
        raise UnsafePathError("managed destination contains a symlink")
    if not stat.S_ISDIR(details.st_mode):
        raise PathValidationError("managed destination component is not a directory")

    try:
        descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError as error:
        raise UnsafePathError("managed destination directory cannot be opened safely") from error
    try:
        actual = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(actual.st_mode)
            or (actual.st_dev, actual.st_ino) != (details.st_dev, details.st_ino)
        ):
            raise UnsafePathError("managed destination changed during validation")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _require_writable_directory(path: Path) -> None:
    if not os.access(path, os.W_OK | os.X_OK):
        raise PathValidationError("destination directory is not writable")


def _require_same_filesystem(first_fd: int, second_fd: int) -> None:
    if os.fstat(first_fd).st_dev != os.fstat(second_fd).st_dev:
        raise PathValidationError("final and incomplete destinations must share a filesystem")


def _select_available_name(parent_fd: int, filename: str, job_id: str) -> str:
    if _entry_is_symlink(parent_fd, filename):
        raise UnsafePathError("final destination is a symlink")
    if _entry_exists(parent_fd, filename):
        candidate = _collision_name(filename, job_id)
        if _entry_is_symlink(parent_fd, candidate):
            raise UnsafePathError("collision destination is a symlink")
        if _entry_exists(parent_fd, candidate):
            raise FinalPathCollisionError("stable collision name is already occupied")
        return candidate
    return filename


def _collision_name(filename: str, job_id: str) -> str:
    suffix = Path(filename).suffix
    stem = filename[: -len(suffix)] if suffix else filename
    return f"{stem}--{job_id}{suffix}"


def _entry_exists(parent_fd: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError as error:
        raise PathValidationError("destination entry is inaccessible") from error
    return True


def _entry_is_symlink(parent_fd: int, name: str) -> bool:
    try:
        details = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError as error:
        raise PathValidationError("destination entry is inaccessible") from error
    return stat.S_ISLNK(details.st_mode)


def _claim_name(parent_fd: int, name: str) -> None:
    try:
        descriptor = os.open(name, _CLAIM_FLAGS, 0o600, dir_fd=parent_fd)
    except FileExistsError as error:
        if _entry_is_symlink(parent_fd, name):
            raise UnsafePathError("final destination is a symlink") from error
        raise FinalPathCollisionError("final destination is already occupied") from error
    except OSError as error:
        raise PathValidationError("final destination cannot be claimed") from error

    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise UnsafePathError("final claim is not a regular file")
        visible = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(visible.st_mode)
            or (visible.st_dev, visible.st_ino) != (details.st_dev, details.st_ino)
        ):
            raise UnsafePathError("final claim changed during creation")
    finally:
        os.close(descriptor)
