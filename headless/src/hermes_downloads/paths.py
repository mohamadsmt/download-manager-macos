"""Safe, deterministic destination intent for Hermes downloads.

This module owns pathname validation, reservation markers, and no-clobber
publication and success-owned cleanup of an already-attested staged payload.
It does not write payload bytes.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import stat
from threading import Lock
from typing import Final
import unicodedata

from hermes_downloads.models import PublicationReservation

__all__ = [
    "CATEGORIES",
    "DestinationIntent",
    "FinalPathCollisionError",
    "JobSpace",
    "PathValidationError",
    "PublishedFinalPayload",
    "PublicationReservationMarker",
    "PreparedPublicationPayload",
    "PublicationCreationPermit",
    "CreationPermitSnapshot",
    "prepare_publication_payload",
    "require_current_publication_payload",
    "StagedPartialPayload",
    "StorageUsage",
    "UnsafePathError",
    "attest_publication_reservation_marker",
    "attest_staged_partial_payload",
    "claim_final_path",
    "observe_job_space",
    "publish_staged_partial_payload",
    "prepare_persisted_destination_workspace",
    "rehydrate_destination",
    "resolve_destination",
]

CATEGORIES: Final = frozenset({"Videos", "Audio", "Documents", "Software", "Other"})
_INCOMPLETE: Final = ".incomplete"
_DIRECTORY_FLAGS: Final = (
    os.O_RDONLY | os.O_NONBLOCK | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
)
_CLAIM_FLAGS: Final = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
_RESERVATION_MARKER: Final = ".hermes-reservation"
_RESERVATION_MARKER_VERSION: Final = "v1"
_RESERVATION_MARKER_MODE: Final = 0o600
_MAX_RESERVATION_MARKER_BYTES: Final = 4096
_RESERVATION_MARKER_CREATE_FLAGS: Final = (
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
)
_RESERVATION_MARKER_READ_FLAGS: Final = (
    os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC
)
_STAGED_PARTIAL_READ_FLAGS: Final = (
    os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC
)


class PathValidationError(ValueError):
    """A supplied destination cannot safely represent a managed output path."""


class UnsafePathError(PathValidationError):
    """An existing symlink or replaced path makes the destination unsafe."""


class FinalPathCollisionError(PathValidationError):
    """A final name is already occupied and cannot be overwritten."""


@dataclass(frozen=True, slots=True)
class StorageUsage:
    """Current logical and, when observable, allocated file bytes."""

    logical_bytes: int
    allocated_bytes: int | None


@dataclass(frozen=True, slots=True)
class JobSpace:
    """Path-scoped current usage and logical output-space expectations."""

    current: StorageUsage
    available_bytes: int
    expected_output_logical_bytes: int | None
    expected_peak_logical_bytes: int | None


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


@dataclass(frozen=True, slots=True)
class PublicationReservationMarker:
    """The visible marker inode that durably binds one publication reservation."""

    path: Path
    st_dev: int
    st_ino: int


@dataclass(frozen=True, slots=True)
class StagedPartialPayload:
    """The visible completed partial payload identity retained for publication."""

    path: Path
    st_dev: int
    st_ino: int
    logical_size: int
    mtime_ns: int | None = None
    st_mode: int | None = None
    st_nlink: int | None = None
    ctime_ns: int | None = None


@dataclass(frozen=True, slots=True)
class PublishedFinalPayload:
    """The final namespace identity returned after a no-clobber publication."""

    path: Path
    st_dev: int
    st_ino: int
    logical_size: int
    st_mode: int | None = None
    st_nlink: int | None = None
    mtime_ns: int | None = None
    ctime_ns: int | None = None
    sha256: str | None = None
    marker: PublicationReservationMarker | None = None
    directory_identities: tuple[tuple[int, int], ...] | None = None


@dataclass(frozen=True, slots=True, repr=False)
class PreparedPublicationPayload:
    """Original stage authority and internal byte commitment, never a vendor checksum."""

    destination: DestinationIntent
    reservation: PublicationReservation
    marker: PublicationReservationMarker
    staged_payload: StagedPartialPayload
    sha256: str
    directory_identities: tuple[tuple[int, int], ...]


@dataclass(frozen=True, slots=True)
class CreationPermitSnapshot:
    revoked: bool
    creation_attempted: bool
    in_flight: bool


class PublicationCreationPermit:
    """Irreversible single-syscall permit; revocation never waits for filesystem IO.

    An in-flight revoked syscall may still create its final. The future owner
    must gate control acknowledgement on quiescence and discard late results.
    """

    def __init__(self) -> None:
        self._mutex = Lock()
        self._revoked = False
        self._creation_attempted = False
        self._in_flight = False

    def snapshot(self) -> CreationPermitSnapshot:
        with self._mutex:
            return self._snapshot_locked()

    def revoke(self) -> CreationPermitSnapshot:
        with self._mutex:
            self._revoked = True
            return self._snapshot_locked()

    def _snapshot_locked(self) -> CreationPermitSnapshot:
        return CreationPermitSnapshot(
            self._revoked, self._creation_attempted, self._in_flight
        )

    def _enter_creation(self) -> None:
        with self._mutex:
            if self._revoked or self._creation_attempted:
                raise PathValidationError("publication creation permit is unavailable")
            self._creation_attempted = True
            self._in_flight = True

    def _finish_creation(self) -> None:
        with self._mutex:
            self._in_flight = False


@dataclass(frozen=True, slots=True)
class _UnlinkPermitSnapshot:
    revoked: bool
    unlink_attempted: bool
    in_flight: bool


class _UnlinkPermit:
    """One deletion syscall; the mutex never covers filesystem IO."""

    def __init__(self) -> None:
        self._mutex = Lock()
        self._revoked = False
        self._unlink_attempted = False
        self._in_flight = False

    def snapshot(self) -> _UnlinkPermitSnapshot:
        with self._mutex:
            return self._snapshot_locked()

    def revoke(self) -> _UnlinkPermitSnapshot:
        with self._mutex:
            self._revoked = True
            return self._snapshot_locked()

    def _snapshot_locked(self) -> _UnlinkPermitSnapshot:
        return _UnlinkPermitSnapshot(self._revoked, self._unlink_attempted, self._in_flight)

    def _enter_unlink(self) -> None:
        with self._mutex:
            if self._revoked or self._unlink_attempted:
                raise PathValidationError("cleanup unlink permit is unavailable")
            self._unlink_attempted = True
            self._in_flight = True

    def _finish_unlink(self) -> None:
        with self._mutex:
            self._in_flight = False


class _PartialUnlinkPermit(_UnlinkPermit):
    """Delete authority only for the exact retained partial name."""


class _MarkerUnlinkPermit(_UnlinkPermit):
    """Delete authority only for the exact original reservation marker."""


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


def rehydrate_destination(
    category: str,
    collection: str | None,
    partial_filename: str,
    selected_final_filename: str,
    job_id: str,
) -> DestinationIntent:
    """Reconstruct a persisted managed destination without touching the filesystem."""

    category = _require_category(category)
    if collection is not None:
        collection = _require_component(collection, "collection")
    partial_filename = _require_component(partial_filename, "partial_filename")
    selected_final_filename = _require_component(
        selected_final_filename, "selected_final_filename"
    )
    job_id = _require_component(job_id, "job_id")
    if selected_final_filename not in {
        partial_filename,
        _collision_name(partial_filename, job_id),
    }:
        raise PathValidationError("selected final filename is not a managed output name")

    root = Path.home() / "Downloads" / "Hermes"
    final_component = collection or category
    incomplete_dir = root / _INCOMPLETE / job_id
    return DestinationIntent(
        root=root,
        category=category,
        collection=collection,
        filename=partial_filename,
        job_id=job_id,
        final_path=root / final_component / selected_final_filename,
        incomplete_dir=incomplete_dir,
        partial_path=incomplete_dir / partial_filename,
    )


def prepare_persisted_destination_workspace(
    destination: DestinationIntent,
) -> DestinationIntent:
    """Provision only the managed directories for an exact persisted intent.

    The final and partial payload paths remain absent.  In particular, this
    does not resolve a collision, claim a final name, or attest a reservation.
    """

    root, final_component = _validate_destination_intent(destination)
    _require_safe_writable_root(root)

    root_fd = _open_root(root)
    try:
        _preflight_persisted_workspace(root_fd, destination, final_component)
        final_fd = _open_or_create_directory(root_fd, final_component)
        try:
            incomplete_fd = _open_or_create_directory(root_fd, _INCOMPLETE)
            try:
                job_fd = _open_or_create_directory(incomplete_fd, destination.job_id)
                try:
                    _require_writable_directory(destination.final_path.parent)
                    _require_writable_directory(destination.incomplete_dir)
                    _require_same_filesystem(final_fd, job_fd)
                finally:
                    os.close(job_fd)
            finally:
                os.close(incomplete_fd)
        finally:
            os.close(final_fd)
    finally:
        os.close(root_fd)

    return destination


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


def attest_publication_reservation_marker(
    destination: DestinationIntent,
    reservation: PublicationReservation,
) -> PublicationReservationMarker:
    """Create or attest the durable job-local receipt for one final destination.

    This binds persisted reservation data to the incomplete-job directory only.
    It never claims a final name or reads, writes, or publishes payload bytes.
    """

    root, final_component = _validate_destination_intent(destination)
    reservation = _validate_publication_reservation(
        destination,
        final_component,
        reservation,
    )
    expected_bytes = _reservation_marker_bytes(reservation)
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
                    marker_path = destination.incomplete_dir / _RESERVATION_MARKER
                    before_directory_sync = _create_reservation_marker(
                        job_fd,
                        marker_path,
                        expected_bytes,
                    )
                    if before_directory_sync is None:
                        before_directory_sync = _attest_existing_reservation_marker(
                            job_fd,
                            marker_path,
                            expected_bytes,
                        )
                    _fsync_reservation_marker_directory(job_fd)
                    attested = _attest_existing_reservation_marker(
                        job_fd,
                        marker_path,
                        expected_bytes,
                    )
                    if (attested.st_dev, attested.st_ino) != (
                        before_directory_sync.st_dev,
                        before_directory_sync.st_ino,
                    ):
                        raise UnsafePathError(
                            "publication reservation marker changed during attestation"
                        )
                    return attested
                finally:
                    os.close(job_fd)
            finally:
                os.close(incomplete_fd)
        finally:
            os.close(final_fd)
    finally:
        os.close(root_fd)


def attest_staged_partial_payload(
    destination: DestinationIntent,
    reservation: PublicationReservation,
) -> StagedPartialPayload:
    """Attest one completed staged payload without publishing or claiming its final name."""

    root, final_component = _validate_destination_intent(destination)
    reservation = _validate_publication_reservation(
        destination,
        final_component,
        reservation,
    )
    expected_marker_bytes = _reservation_marker_bytes(reservation)
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
                    marker = _attest_existing_reservation_marker(
                        job_fd,
                        destination.incomplete_dir / _RESERVATION_MARKER,
                        expected_marker_bytes,
                    )
                    _require_absent_final_name(final_fd, destination.final_path.name)
                    preflight = _stat_staged_partial_payload(
                        job_fd,
                        destination.partial_path.name,
                    )
                    payload_fd = _open_staged_partial_payload(
                        job_fd,
                        destination.partial_path.name,
                    )
                    try:
                        opened = _fstat_staged_partial_payload(payload_fd)
                        _require_matching_staged_partial_details(preflight, opened)
                        visible_before_sync = _stat_staged_partial_payload(
                            job_fd,
                            destination.partial_path.name,
                        )
                        _require_matching_staged_partial_details(
                            opened,
                            visible_before_sync,
                        )
                        _require_absent_final_name(final_fd, destination.final_path.name)
                        _fsync_staged_partial_payload(payload_fd)
                        after_file_sync = _fstat_staged_partial_payload(payload_fd)
                        _require_matching_staged_partial_details(
                            opened,
                            after_file_sync,
                        )
                        visible_after_file_sync = _stat_staged_partial_payload(
                            job_fd,
                            destination.partial_path.name,
                        )
                        _require_matching_staged_partial_details(
                            after_file_sync,
                            visible_after_file_sync,
                        )
                        _require_absent_final_name(final_fd, destination.final_path.name)
                        _fsync_staged_partial_directory(job_fd)
                        after_directory_sync = _fstat_staged_partial_payload(payload_fd)
                        _require_matching_staged_partial_details(
                            after_file_sync,
                            after_directory_sync,
                        )
                        visible_after_directory_sync = _stat_staged_partial_payload(
                            job_fd,
                            destination.partial_path.name,
                        )
                        _require_matching_staged_partial_details(
                            after_directory_sync,
                            visible_after_directory_sync,
                        )
                        _verify_attested_reservation_marker(
                            job_fd,
                            expected_marker_bytes,
                            marker,
                        )
                        after_marker_reattest = _fstat_staged_partial_payload(payload_fd)
                        _require_matching_staged_partial_details(
                            after_directory_sync,
                            after_marker_reattest,
                        )
                        visible_after_marker_reattest = _stat_staged_partial_payload(
                            job_fd,
                            destination.partial_path.name,
                        )
                        _require_matching_staged_partial_details(
                            after_marker_reattest,
                            visible_after_marker_reattest,
                        )
                        _require_absent_final_name(final_fd, destination.final_path.name)
                        return StagedPartialPayload(
                            path=destination.partial_path,
                            st_dev=preflight[0],
                            st_ino=preflight[1],
                            logical_size=preflight[2],
                            mtime_ns=preflight[5],
                            st_mode=preflight[3],
                            st_nlink=preflight[4],
                            ctime_ns=preflight[6],
                        )
                    finally:
                        os.close(payload_fd)
                finally:
                    os.close(job_fd)
            finally:
                os.close(incomplete_fd)
        finally:
            os.close(final_fd)
    finally:
        os.close(root_fd)


def _require_current_staged_payload(
    destination: DestinationIntent,
    reservation: PublicationReservation,
    marker: PublicationReservationMarker,
    staged: StagedPartialPayload,
) -> None:
    """Fresh descriptor/namespace evidence for a bind; no hash, writes or fsync."""

    root, component = _validate_destination_intent(destination)
    reservation = _validate_publication_reservation(destination, component, reservation)
    identity = _validate_staged_partial_payload_identity(destination, staged)
    if any(type(value) is not int for value in (
        staged.st_mode, staged.st_nlink, staged.mtime_ns, staged.ctime_ns
    )):
        raise PathValidationError("staged payload metadata evidence is absent or invalid")
    expected = (*identity, staged.st_mode, staged.st_nlink, staged.mtime_ns, staged.ctime_ns)
    directories = (root, destination.final_path.parent, root / _INCOMPLETE,
                   destination.incomplete_dir)
    chain = tuple((details.st_dev, details.st_ino)
                  for details in map(_require_real_directory, directories))
    descriptors = _open_visible_publication_chain(root, component, destination.job_id, chain)
    try:
        _verify_attested_reservation_marker(
            descriptors[3], _reservation_marker_bytes(reservation), marker
        )
        before = _stat_staged_partial_payload(descriptors[3], destination.partial_path.name)
        payload_fd = _open_staged_partial_payload(descriptors[3], destination.partial_path.name)
        try:
            opened = _fstat_staged_partial_payload(payload_fd)
            _require_matching_staged_partial_details(before, opened)
            _require_matching_staged_partial_details(expected, opened)
            # Reopen the visible directories after descriptor checks; detached
            # old directory descriptors must not confer namespace authority.
            fresh = _open_visible_publication_chain(root, component, destination.job_id, chain)
            try:
                _verify_attested_reservation_marker(
                    fresh[3], _reservation_marker_bytes(reservation), marker
                )
                visible = _stat_staged_partial_payload(fresh[3], destination.partial_path.name)
                _require_matching_staged_partial_details(opened, visible)
                _require_matching_staged_partial_details(opened, _fstat_staged_partial_payload(payload_fd))
                _require_absent_final_name(fresh[1], destination.final_path.name)
            finally:
                for descriptor in reversed(fresh):
                    os.close(descriptor)
        finally:
            os.close(payload_fd)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def publish_staged_partial_payload(
    destination: DestinationIntent,
    reservation: PublicationReservation,
    staged_payload: StagedPartialPayload,
    *,
    existing_only: bool = False,
    prepared: PreparedPublicationPayload | None = None,
    creation_permit: PublicationCreationPermit | None = None,
    _cancelled=None,
) -> PublishedFinalPayload:
    """Publish one previously attested partial via a no-clobber hard link.

    The partial and receipt remain in place.  A visible final is accepted only
    when it is the exact staged inode from a prior interrupted publication.
    With existing_only=True, verify an existing final without creating a link.
    """

    if type(existing_only) is not bool:
        raise PathValidationError("existing_only must be a boolean")
    if prepared is not None or creation_permit is not None:
        return _publish_prepared_payload(
            destination, reservation, staged_payload, prepared, creation_permit,
            existing_only=existing_only, cancelled=_cancelled,
        )

    root, final_component = _validate_destination_intent(destination)
    reservation = _validate_publication_reservation(
        destination,
        final_component,
        reservation,
    )
    expected_marker_bytes = _reservation_marker_bytes(reservation)
    expected_payload = _validate_staged_partial_payload_identity(
        destination,
        staged_payload,
    )
    result = PublishedFinalPayload(
        path=destination.final_path,
        st_dev=expected_payload[0],
        st_ino=expected_payload[1],
        logical_size=expected_payload[2],
    )
    _require_safe_writable_root(root)

    root_fd = _open_root(root)
    try:
        final_fd = _open_existing_directory(root_fd, final_component)
        try:
            incomplete_fd = _open_existing_directory(root_fd, _INCOMPLETE)
            try:
                job_fd = _open_existing_directory(incomplete_fd, destination.job_id)
                try:
                    _require_same_filesystem(final_fd, job_fd)
                    expected_chain = _publication_chain_identities(
                        root_fd,
                        final_fd,
                        incomplete_fd,
                        job_fd,
                    )
                    marker = _attest_existing_reservation_marker(
                        job_fd,
                        destination.incomplete_dir / _RESERVATION_MARKER,
                        expected_marker_bytes,
                    )
                    _verify_visible_publication_payload(
                        job_fd,
                        destination.partial_path.name,
                        expected_payload,
                        minimum_links=1,
                    )
                    payload_fd = _open_staged_partial_payload(
                        job_fd,
                        destination.partial_path.name,
                    )
                    try:
                        _verify_publication_payload_descriptor(
                            payload_fd,
                            expected_payload,
                            minimum_links=1,
                        )
                        _verify_visible_publication_payload(
                            job_fd,
                            destination.partial_path.name,
                            expected_payload,
                            minimum_links=1,
                        )
                        _verify_attested_reservation_marker(
                            job_fd,
                            expected_marker_bytes,
                            marker,
                        )
                        if existing_only:
                            _require_existing_final_payload(
                                final_fd,
                                destination.final_path.name,
                                expected_payload,
                            )
                        else:
                            _link_staged_partial_payload(
                                job_fd,
                                final_fd,
                                destination.partial_path.name,
                                destination.final_path.name,
                                expected_payload,
                            )
                        _verify_publication_payload_descriptor(
                            payload_fd,
                            expected_payload,
                            minimum_links=2,
                        )
                        _verify_attested_reservation_marker(
                            job_fd,
                            expected_marker_bytes,
                            marker,
                        )
                        _verify_visible_published_payloads(
                            root,
                            final_component,
                            destination.job_id,
                            expected_chain,
                            destination.final_path.name,
                            destination.partial_path.name,
                            expected_payload,
                        )
                        _fsync_published_final_directory(final_fd)
                        _verify_publication_payload_descriptor(
                            payload_fd,
                            expected_payload,
                            minimum_links=2,
                        )
                        _verify_attested_reservation_marker(
                            job_fd,
                            expected_marker_bytes,
                            marker,
                        )
                        _verify_visible_published_payloads(
                            root,
                            final_component,
                            destination.job_id,
                            expected_chain,
                            destination.final_path.name,
                            destination.partial_path.name,
                            expected_payload,
                        )
                        return result
                    finally:
                        os.close(payload_fd)
                finally:
                    os.close(job_fd)
            finally:
                os.close(incomplete_fd)
        finally:
            os.close(final_fd)
    finally:
        os.close(root_fd)


def observe_job_space(
    destination: DestinationIntent,
    *,
    owned_paths: tuple[Path, ...],
    output_path: Path,
    expected_output_logical_bytes: int | None,
) -> JobSpace:
    """Observe only the explicit job artifacts without creating or changing them."""

    root, _ = _validate_destination_intent(destination)
    expected_output_logical_bytes = _require_expected_output_logical_bytes(
        expected_output_logical_bytes
    )
    owned_paths, output_path = _validate_job_artifact_paths(
        destination.incomplete_dir,
        owned_paths=owned_paths,
        output_path=output_path,
    )

    _require_safe_writable_root(root)
    root_fd = _open_root(root)
    try:
        incomplete_fd = _open_existing_directory(root_fd, _INCOMPLETE)
        try:
            job_fd = _open_existing_directory(incomplete_fd, destination.job_id)
            try:
                owned_details = tuple(
                    _require_job_file(job_fd, path.name) for path in owned_paths
                )
                output_details = _observe_output_file(job_fd, output_path.name)
                included_details = owned_details + (
                    () if output_details is None else (output_details,)
                )
                _require_unique_job_file_identities(included_details)

                owned_logical_bytes = sum(details.st_size for details in owned_details)
                output_logical_bytes = 0 if output_details is None else output_details.st_size
                current = StorageUsage(
                    logical_bytes=owned_logical_bytes + output_logical_bytes,
                    allocated_bytes=_allocated_bytes(included_details),
                )
                expected_peak_logical_bytes = (
                    None
                    if expected_output_logical_bytes is None
                    else owned_logical_bytes
                    + max(output_logical_bytes, expected_output_logical_bytes)
                )
                return JobSpace(
                    current=current,
                    available_bytes=_available_bytes(job_fd),
                    expected_output_logical_bytes=expected_output_logical_bytes,
                    expected_peak_logical_bytes=expected_peak_logical_bytes,
                )
            finally:
                os.close(job_fd)
        finally:
            os.close(incomplete_fd)
    finally:
        os.close(root_fd)


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


def _validate_publication_reservation(
    destination: DestinationIntent,
    final_component: str,
    reservation: PublicationReservation,
) -> PublicationReservation:
    if type(reservation) is not PublicationReservation:
        raise TypeError("reservation must be a PublicationReservation")
    try:
        validated = PublicationReservation(
            job_id=reservation.job_id,
            target_component=reservation.target_component,
            final_filename=reservation.final_filename,
            claim_token=reservation.claim_token,
        )
    except (AttributeError, TypeError, ValueError) as error:
        raise PathValidationError("publication reservation is invalid") from error
    if (
        validated.job_id != destination.job_id
        or validated.target_component != final_component
        or validated.final_filename != destination.final_path.name
    ):
        raise PathValidationError("publication reservation does not match destination")
    return validated


def _reservation_marker_bytes(reservation: PublicationReservation) -> bytes:
    try:
        marker_bytes = (
            "\n".join(
                (
                    _RESERVATION_MARKER_VERSION,
                    reservation.job_id,
                    reservation.target_component,
                    reservation.final_filename,
                    reservation.claim_token,
                )
            ).encode("utf-8")
            + b"\n"
        )
    except UnicodeError as error:
        raise PathValidationError("publication reservation marker cannot be encoded") from error
    if not marker_bytes or len(marker_bytes) > _MAX_RESERVATION_MARKER_BYTES:
        raise PathValidationError("publication reservation marker exceeds the size limit")
    return marker_bytes


def _create_reservation_marker(
    parent_fd: int,
    marker_path: Path,
    expected_bytes: bytes,
) -> PublicationReservationMarker | None:
    try:
        descriptor = os.open(
            _RESERVATION_MARKER,
            _RESERVATION_MARKER_CREATE_FLAGS,
            _RESERVATION_MARKER_MODE,
            dir_fd=parent_fd,
        )
    except FileExistsError:
        return None
    except OSError as error:
        raise PathValidationError("publication reservation marker cannot be created") from error

    try:
        try:
            os.fchmod(descriptor, _RESERVATION_MARKER_MODE)
        except OSError as error:
            raise PathValidationError("publication reservation marker mode cannot be set") from error
        created = _require_reservation_marker_details(
            _fstat_reservation_marker(descriptor),
            expected_size=0,
        )
        visible = _require_reservation_marker_details(
            _stat_reservation_marker(parent_fd),
            expected_size=0,
        )
        _require_matching_reservation_marker_identity(created, visible)
        _write_reservation_marker(descriptor, expected_bytes)
        _fsync_reservation_marker(descriptor)
        details = _require_reservation_marker_details(
            _fstat_reservation_marker(descriptor),
            expected_size=len(expected_bytes),
        )
        st_dev, st_ino = _reservation_marker_identity(details)
        return PublicationReservationMarker(
            path=marker_path,
            st_dev=st_dev,
            st_ino=st_ino,
        )
    finally:
        os.close(descriptor)


def _attest_existing_reservation_marker(
    parent_fd: int,
    marker_path: Path,
    expected_bytes: bytes,
) -> PublicationReservationMarker:
    preflight = _require_reservation_marker_details(
        _stat_reservation_marker(parent_fd),
        expected_size=len(expected_bytes),
    )
    try:
        descriptor = os.open(
            _RESERVATION_MARKER,
            _RESERVATION_MARKER_READ_FLAGS,
            dir_fd=parent_fd,
        )
    except OSError as error:
        raise UnsafePathError("publication reservation marker cannot be opened safely") from error

    try:
        opened = _require_reservation_marker_details(
            _fstat_reservation_marker(descriptor),
            expected_size=len(expected_bytes),
        )
        _require_matching_reservation_marker_identity(preflight, opened)
        contents = _read_reservation_marker(descriptor, len(expected_bytes))
        after_read = _require_reservation_marker_details(
            _fstat_reservation_marker(descriptor),
            expected_size=len(expected_bytes),
        )
        _require_matching_reservation_marker_identity(opened, after_read)
        visible = _require_reservation_marker_details(
            _stat_reservation_marker(parent_fd),
            expected_size=len(expected_bytes),
        )
        _require_matching_reservation_marker_identity(after_read, visible)
        if contents != expected_bytes:
            raise PathValidationError("publication reservation marker contents do not match")
        _fsync_reservation_marker(descriptor)
        st_dev, st_ino = _reservation_marker_identity(visible)
        return PublicationReservationMarker(
            path=marker_path,
            st_dev=st_dev,
            st_ino=st_ino,
        )
    finally:
        os.close(descriptor)


def _stat_reservation_marker(parent_fd: int) -> os.stat_result:
    try:
        return os.stat(_RESERVATION_MARKER, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError as error:
        raise PathValidationError("publication reservation marker is missing") from error
    except OSError as error:
        raise PathValidationError("publication reservation marker is inaccessible") from error


def _fstat_reservation_marker(descriptor: int) -> os.stat_result:
    try:
        return os.fstat(descriptor)
    except OSError as error:
        raise PathValidationError("publication reservation marker is inaccessible") from error


def _require_reservation_marker_details(
    details: os.stat_result,
    *,
    expected_size: int,
) -> os.stat_result:
    details = _require_regular_single_link_file(details)
    try:
        mode = details.st_mode
        size = details.st_size
    except (AttributeError, TypeError, ValueError) as error:
        raise PathValidationError("publication reservation marker metadata is invalid") from error
    if (mode & 0o7777) != _RESERVATION_MARKER_MODE:
        raise PathValidationError("publication reservation marker mode is not private")
    if size != expected_size:
        raise PathValidationError("publication reservation marker size does not match")
    _reservation_marker_identity(details)
    return details


def _reservation_marker_identity(details: os.stat_result) -> tuple[int, int]:
    try:
        st_dev = details.st_dev
        st_ino = details.st_ino
    except (AttributeError, TypeError, ValueError) as error:
        raise PathValidationError("publication reservation marker identity is invalid") from error
    if type(st_dev) is not int or st_dev < 0 or type(st_ino) is not int or st_ino < 0:
        raise PathValidationError("publication reservation marker identity is invalid")
    return st_dev, st_ino


def _require_matching_reservation_marker_identity(
    first: os.stat_result,
    second: os.stat_result,
) -> None:
    if _reservation_marker_identity(first) != _reservation_marker_identity(second):
        raise UnsafePathError("publication reservation marker changed during attestation")


def _write_reservation_marker(descriptor: int, marker_bytes: bytes) -> None:
    offset = 0
    while offset < len(marker_bytes):
        try:
            written = os.write(descriptor, marker_bytes[offset:])
        except OSError as error:
            raise PathValidationError("publication reservation marker cannot be written") from error
        remaining = len(marker_bytes) - offset
        if type(written) is not int or written <= 0 or written > remaining:
            raise PathValidationError("publication reservation marker write was incomplete")
        offset += written


def _read_reservation_marker(descriptor: int, expected_size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = expected_size + 1
    while remaining:
        try:
            chunk = os.read(descriptor, remaining)
        except OSError as error:
            raise PathValidationError("publication reservation marker cannot be read") from error
        if type(chunk) is not bytes or len(chunk) > remaining:
            raise PathValidationError("publication reservation marker read is invalid")
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    marker_bytes = b"".join(chunks)
    if len(marker_bytes) > expected_size:
        raise PathValidationError("publication reservation marker exceeds the size limit")
    return marker_bytes


def _fsync_reservation_marker(descriptor: int) -> None:
    try:
        os.fsync(descriptor)
    except OSError as error:
        raise PathValidationError("publication reservation marker cannot be synced") from error


def _fsync_reservation_marker_directory(descriptor: int) -> None:
    try:
        os.fsync(descriptor)
    except OSError as error:
        raise PathValidationError("publication reservation directory cannot be synced") from error


def _verify_attested_reservation_marker(
    parent_fd: int,
    expected_bytes: bytes,
    expected_marker: PublicationReservationMarker,
) -> None:
    preflight = _require_reservation_marker_details(
        _stat_reservation_marker(parent_fd),
        expected_size=len(expected_bytes),
    )
    _require_attested_reservation_marker_identity(preflight, expected_marker)
    try:
        descriptor = os.open(
            _RESERVATION_MARKER,
            _RESERVATION_MARKER_READ_FLAGS,
            dir_fd=parent_fd,
        )
    except OSError as error:
        raise UnsafePathError("publication reservation marker cannot be opened safely") from error

    try:
        opened = _require_reservation_marker_details(
            _fstat_reservation_marker(descriptor),
            expected_size=len(expected_bytes),
        )
        _require_matching_reservation_marker_identity(preflight, opened)
        _require_attested_reservation_marker_identity(opened, expected_marker)
        contents = _read_reservation_marker(descriptor, len(expected_bytes))
        after_read = _require_reservation_marker_details(
            _fstat_reservation_marker(descriptor),
            expected_size=len(expected_bytes),
        )
        _require_matching_reservation_marker_identity(opened, after_read)
        _require_attested_reservation_marker_identity(after_read, expected_marker)
        visible = _require_reservation_marker_details(
            _stat_reservation_marker(parent_fd),
            expected_size=len(expected_bytes),
        )
        _require_matching_reservation_marker_identity(after_read, visible)
        _require_attested_reservation_marker_identity(visible, expected_marker)
        if contents != expected_bytes:
            raise PathValidationError("publication reservation marker contents do not match")
    finally:
        os.close(descriptor)


def _require_attested_reservation_marker_identity(
    details: os.stat_result,
    expected_marker: PublicationReservationMarker,
) -> None:
    if _reservation_marker_identity(details) != (
        expected_marker.st_dev,
        expected_marker.st_ino,
    ):
        raise UnsafePathError("publication reservation marker changed during attestation")


def _require_absent_final_name(parent_fd: int, name: str) -> None:
    try:
        details = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    except OSError as error:
        raise PathValidationError("final destination is inaccessible") from error
    try:
        mode = details.st_mode
    except (AttributeError, TypeError, ValueError) as error:
        raise PathValidationError("final destination metadata is invalid") from error
    if type(mode) is not int:
        raise PathValidationError("final destination metadata is invalid")
    if stat.S_ISLNK(mode):
        raise UnsafePathError("final destination is a symlink")
    raise FinalPathCollisionError("final destination is already occupied")


def _stat_staged_partial_payload(
    parent_fd: int,
    name: str,
) -> tuple[int, int, int, int, int, int, int]:
    try:
        details = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError as error:
        raise PathValidationError("partial payload is missing") from error
    except OSError as error:
        raise PathValidationError("partial payload is inaccessible") from error
    return _staged_partial_payload_details(details)


def _open_staged_partial_payload(parent_fd: int, name: str) -> int:
    try:
        return os.open(name, _STAGED_PARTIAL_READ_FLAGS, dir_fd=parent_fd)
    except OSError as error:
        raise UnsafePathError("partial payload cannot be opened safely") from error


def _fstat_staged_partial_payload(
    descriptor: int,
) -> tuple[int, int, int, int, int, int, int]:
    try:
        details = os.fstat(descriptor)
    except OSError as error:
        raise PathValidationError("partial payload is inaccessible") from error
    try:
        return _staged_partial_payload_details(details)
    except UnsafePathError:
        raise
    except PathValidationError as error:
        raise UnsafePathError("partial payload changed during attestation") from error


def _staged_partial_payload_details(
    details: os.stat_result,
) -> tuple[int, int, int, int, int, int, int]:
    details = _require_regular_single_link_file(details)
    try:
        st_dev = details.st_dev
        st_ino = details.st_ino
        st_size = details.st_size
        st_mode = details.st_mode
        st_nlink = details.st_nlink
        st_mtime_ns = details.st_mtime_ns
        st_ctime_ns = details.st_ctime_ns
    except (AttributeError, TypeError, ValueError) as error:
        raise PathValidationError("partial payload metadata is invalid") from error
    if (
        type(st_dev) is not int
        or st_dev < 0
        or type(st_ino) is not int
        or st_ino < 0
        or type(st_size) is not int
        or st_size < 0
        or type(st_mode) is not int
        or type(st_nlink) is not int
        or st_nlink != 1
        or type(st_mtime_ns) is not int
        or type(st_ctime_ns) is not int
    ):
        raise PathValidationError("partial payload metadata is invalid")
    return st_dev, st_ino, st_size, st_mode, st_nlink, st_mtime_ns, st_ctime_ns


def _require_matching_staged_partial_details(
    first: tuple[int, int, int, int, int, int, int],
    second: tuple[int, int, int, int, int, int, int],
) -> None:
    if first != second:
        raise UnsafePathError("partial payload changed during attestation")


def _fsync_staged_partial_payload(descriptor: int) -> None:
    try:
        os.fsync(descriptor)
    except OSError as error:
        raise PathValidationError("partial payload cannot be synced") from error


def _fsync_staged_partial_directory(descriptor: int) -> None:
    try:
        os.fsync(descriptor)
    except OSError as error:
        raise PathValidationError("incomplete job directory cannot be synced") from error


def _validate_staged_partial_payload_identity(
    destination: DestinationIntent,
    staged_payload: StagedPartialPayload,
) -> tuple[int, int, int]:
    if type(staged_payload) is not StagedPartialPayload:
        raise TypeError("staged_payload must be a StagedPartialPayload")
    try:
        path = staged_payload.path
        st_dev = staged_payload.st_dev
        st_ino = staged_payload.st_ino
        logical_size = staged_payload.logical_size
    except AttributeError as error:
        raise PathValidationError("staged payload identity is invalid") from error
    if not isinstance(path, Path) or path != destination.partial_path:
        raise PathValidationError("staged payload path does not match destination")
    if (
        type(st_dev) is not int
        or st_dev < 0
        or type(st_ino) is not int
        or st_ino < 0
        or type(logical_size) is not int
        or logical_size < 0
    ):
        raise PathValidationError("staged payload identity is invalid")
    return st_dev, st_ino, logical_size


def _publication_chain_identities(
    root_fd: int,
    final_fd: int,
    incomplete_fd: int,
    job_fd: int,
) -> tuple[tuple[int, int], tuple[int, int], tuple[int, int], tuple[int, int]]:
    return (
        _publication_directory_identity(root_fd),
        _publication_directory_identity(final_fd),
        _publication_directory_identity(incomplete_fd),
        _publication_directory_identity(job_fd),
    )


def _publication_directory_identity(descriptor: int) -> tuple[int, int]:
    try:
        details = os.fstat(descriptor)
    except OSError as error:
        raise PathValidationError("managed publication directory is inaccessible") from error
    try:
        mode = details.st_mode
        st_dev = details.st_dev
        st_ino = details.st_ino
    except (AttributeError, TypeError, ValueError) as error:
        raise PathValidationError("managed publication directory metadata is invalid") from error
    if (
        type(mode) is not int
        or not stat.S_ISDIR(mode)
        or type(st_dev) is not int
        or st_dev < 0
        or type(st_ino) is not int
        or st_ino < 0
    ):
        raise PathValidationError("managed publication directory metadata is invalid")
    return st_dev, st_ino


def _require_publication_directory_identity(
    descriptor: int,
    expected: tuple[int, int],
) -> None:
    if _publication_directory_identity(descriptor) != expected:
        raise UnsafePathError("managed publication directory changed during publication")


def _publication_payload_details(
    details: os.stat_result,
) -> tuple[int, int, int, int, int, int, int]:
    try:
        st_dev = details.st_dev
        st_ino = details.st_ino
        st_size = details.st_size
        st_mode = details.st_mode
        st_nlink = details.st_nlink
        st_mtime_ns = details.st_mtime_ns
        st_ctime_ns = details.st_ctime_ns
    except (AttributeError, TypeError, ValueError) as error:
        raise PathValidationError("published payload metadata is invalid") from error
    if type(st_mode) is not int:
        raise PathValidationError("published payload metadata is invalid")
    if stat.S_ISLNK(st_mode):
        raise UnsafePathError("published payload is a symlink")
    if (
        not stat.S_ISREG(st_mode)
        or type(st_dev) is not int
        or st_dev < 0
        or type(st_ino) is not int
        or st_ino < 0
        or type(st_size) is not int
        or st_size < 0
        or type(st_nlink) is not int
        or st_nlink < 1
        or type(st_mtime_ns) is not int
        or type(st_ctime_ns) is not int
    ):
        raise PathValidationError("published payload metadata is invalid")
    return st_dev, st_ino, st_size, st_mode, st_nlink, st_mtime_ns, st_ctime_ns


def _stat_visible_publication_payload(
    parent_fd: int,
    name: str,
) -> tuple[int, int, int, int, int, int, int]:
    try:
        details = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError as error:
        raise PathValidationError("published payload is missing") from error
    except OSError as error:
        raise PathValidationError("published payload is inaccessible") from error
    return _publication_payload_details(details)


def _open_publication_payload(parent_fd: int, name: str) -> int:
    try:
        return os.open(name, _STAGED_PARTIAL_READ_FLAGS, dir_fd=parent_fd)
    except OSError as error:
        raise UnsafePathError("published payload cannot be opened safely") from error


def _fstat_publication_payload(
    descriptor: int,
) -> tuple[int, int, int, int, int, int, int]:
    try:
        details = os.fstat(descriptor)
    except OSError as error:
        raise PathValidationError("published payload is inaccessible") from error
    try:
        return _publication_payload_details(details)
    except UnsafePathError:
        raise
    except PathValidationError as error:
        raise UnsafePathError("published payload changed during verification") from error


def _require_publication_payload_identity(
    details: tuple[int, int, int, int, int, int, int],
    expected: tuple[int, int, int],
    *,
    minimum_links: int,
) -> None:
    if details[:3] != expected or details[4] < minimum_links:
        raise UnsafePathError("published payload identity does not match staged payload")


def _verify_publication_payload_descriptor(
    descriptor: int,
    expected: tuple[int, int, int],
    *,
    minimum_links: int,
) -> None:
    details = _fstat_publication_payload(descriptor)
    _require_publication_payload_identity(
        details,
        expected,
        minimum_links=minimum_links,
    )


def _verify_visible_publication_payload(
    parent_fd: int,
    name: str,
    expected: tuple[int, int, int],
    *,
    minimum_links: int,
) -> None:
    preflight = _stat_visible_publication_payload(parent_fd, name)
    _require_publication_payload_identity(
        preflight,
        expected,
        minimum_links=minimum_links,
    )
    descriptor = _open_publication_payload(parent_fd, name)
    try:
        opened = _fstat_publication_payload(descriptor)
        _require_publication_payload_identity(
            opened,
            expected,
            minimum_links=minimum_links,
        )
        if opened != preflight:
            raise UnsafePathError("published payload changed during verification")
        visible = _stat_visible_publication_payload(parent_fd, name)
        _require_publication_payload_identity(
            visible,
            expected,
            minimum_links=minimum_links,
        )
        if visible != opened:
            raise UnsafePathError("published payload changed during verification")
    finally:
        os.close(descriptor)


def _require_existing_final_payload(
    parent_fd: int,
    name: str,
    expected: tuple[int, int, int],
) -> None:
    try:
        preflight = _stat_visible_publication_payload(parent_fd, name)
    except UnsafePathError:
        raise
    except PathValidationError as error:
        raise FinalPathCollisionError("final destination is already occupied") from error
    if preflight[:3] != expected or preflight[4] < 2:
        raise FinalPathCollisionError("final destination is already occupied")
    descriptor = _open_publication_payload(parent_fd, name)
    try:
        opened = _fstat_publication_payload(descriptor)
        if opened != preflight:
            raise UnsafePathError("final destination changed during publication")
        if opened[:3] != expected or opened[4] < 2:
            raise UnsafePathError("final destination changed during publication")
        visible = _stat_visible_publication_payload(parent_fd, name)
        if visible != opened:
            raise UnsafePathError("final destination changed during publication")
    finally:
        os.close(descriptor)


def _link_staged_partial_payload(
    source_parent_fd: int,
    final_parent_fd: int,
    source_name: str,
    final_name: str,
    expected: tuple[int, int, int],
    *,
    creation_permit: PublicationCreationPermit | None = None,
) -> None:
    # Only this actual creation branch enters the permit, immediately before
    # os.link. No mutex is held over the syscall or any post-link verification.
    if creation_permit is not None:
        creation_permit._enter_creation()
    try:
        os.link(
            source_name,
            final_name,
            src_dir_fd=source_parent_fd,
            dst_dir_fd=final_parent_fd,
            follow_symlinks=False,
        )
    except FileExistsError as error:
        if creation_permit is not None:
            raise FinalPathCollisionError("final destination is already occupied") from error
        _require_existing_final_payload(final_parent_fd, final_name, expected)
    except OSError as error:
        raise PathValidationError("staged payload cannot be linked to final destination") from error
    finally:
        if creation_permit is not None:
            creation_permit._finish_creation()


def _open_visible_publication_chain(
    root: Path,
    final_component: str,
    job_id: str,
    expected_chain: tuple[tuple[int, int], tuple[int, int], tuple[int, int], tuple[int, int]],
) -> tuple[int, int, int, int]:
    root_fd = _open_root(root)
    final_fd: int | None = None
    incomplete_fd: int | None = None
    job_fd: int | None = None
    try:
        _require_publication_directory_identity(root_fd, expected_chain[0])
        final_fd = _open_existing_directory(root_fd, final_component)
        _require_publication_directory_identity(final_fd, expected_chain[1])
        incomplete_fd = _open_existing_directory(root_fd, _INCOMPLETE)
        _require_publication_directory_identity(incomplete_fd, expected_chain[2])
        job_fd = _open_existing_directory(incomplete_fd, job_id)
        _require_publication_directory_identity(job_fd, expected_chain[3])
        return root_fd, final_fd, incomplete_fd, job_fd
    except BaseException:
        if job_fd is not None:
            os.close(job_fd)
        if incomplete_fd is not None:
            os.close(incomplete_fd)
        if final_fd is not None:
            os.close(final_fd)
        os.close(root_fd)
        raise


def _verify_visible_published_payloads(
    root: Path,
    final_component: str,
    job_id: str,
    expected_chain: tuple[tuple[int, int], tuple[int, int], tuple[int, int], tuple[int, int]],
    final_name: str,
    partial_name: str,
    expected: tuple[int, int, int],
) -> None:
    root_fd, final_fd, incomplete_fd, job_fd = _open_visible_publication_chain(
        root,
        final_component,
        job_id,
        expected_chain,
    )
    try:
        _verify_visible_publication_payload(
            job_fd,
            partial_name,
            expected,
            minimum_links=2,
        )
    finally:
        os.close(job_fd)
        os.close(incomplete_fd)
        os.close(final_fd)
        os.close(root_fd)

    # Rebind the lexical chain after source verification, then check the final
    # entry last so a source-check race cannot return a stale final namespace.
    root_fd, final_fd, incomplete_fd, job_fd = _open_visible_publication_chain(
        root,
        final_component,
        job_id,
        expected_chain,
    )
    try:
        _verify_visible_publication_payload(
            final_fd,
            final_name,
            expected,
            minimum_links=2,
        )
    finally:
        os.close(job_fd)
        os.close(incomplete_fd)
        os.close(final_fd)
        os.close(root_fd)


def _fsync_published_final_directory(descriptor: int) -> None:
    try:
        os.fsync(descriptor)
    except OSError as error:
        raise PathValidationError("final destination directory cannot be synced") from error


def _require_expected_output_logical_bytes(value: object) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 0:
        raise PathValidationError("expected output size must be a nonnegative integer")
    return value


def _validate_job_artifact_paths(
    incomplete_dir: Path,
    *,
    owned_paths: object,
    output_path: object,
) -> tuple[tuple[Path, ...], Path]:
    if type(owned_paths) is not tuple:
        raise PathValidationError("owned paths must be a tuple")

    output = _require_direct_incomplete_child(output_path, incomplete_dir)
    owned: list[Path] = []
    seen: set[Path] = set()
    for value in owned_paths:
        path = _require_direct_incomplete_child(value, incomplete_dir)
        if path == output:
            raise PathValidationError("output path cannot be an owned artifact")
        if path in seen:
            raise PathValidationError("owned artifact paths must be unique")
        seen.add(path)
        owned.append(path)
    return tuple(owned), output


def _require_direct_incomplete_child(value: object, incomplete_dir: Path) -> Path:
    if not isinstance(value, Path):
        raise PathValidationError("job artifact path must be a Path")
    if value.parent != incomplete_dir:
        raise PathValidationError("job artifact must be a direct incomplete child")
    return value


def _require_job_file(parent_fd: int, name: str) -> os.stat_result:
    try:
        details = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError as error:
        raise PathValidationError("owned job artifact is missing") from error
    except OSError as error:
        raise PathValidationError("owned job artifact is inaccessible") from error
    return _require_regular_single_link_file(details)


def _observe_output_file(parent_fd: int, name: str) -> os.stat_result | None:
    try:
        details = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise PathValidationError("job output artifact is inaccessible") from error
    return _require_regular_single_link_file(details)


def _require_regular_single_link_file(details: os.stat_result) -> os.stat_result:
    try:
        mode = details.st_mode
        links = details.st_nlink
        logical_bytes = details.st_size
    except (AttributeError, TypeError, ValueError) as error:
        raise PathValidationError("job artifact metadata is invalid") from error
    if type(mode) is not int:
        raise PathValidationError("job artifact metadata is invalid")
    if stat.S_ISLNK(mode):
        raise UnsafePathError("job artifact is a symlink")
    if not stat.S_ISREG(mode):
        raise PathValidationError("job artifact is not a regular file")
    if type(links) is not int or links != 1:
        raise PathValidationError("job artifact is not a single-link file")
    if type(logical_bytes) is not int or logical_bytes < 0:
        raise PathValidationError("job artifact size is invalid")
    return details


def _require_unique_job_file_identities(details: tuple[os.stat_result, ...]) -> None:
    identities: set[tuple[int, int]] = set()
    for item in details:
        try:
            device = item.st_dev
            inode = item.st_ino
        except (AttributeError, TypeError, ValueError) as error:
            raise PathValidationError("job artifact identity is invalid") from error
        if type(device) is not int or device < 0 or type(inode) is not int or inode < 0:
            raise PathValidationError("job artifact identity is invalid")
        identity = (device, inode)
        if identity in identities:
            raise PathValidationError("job artifacts must identify unique files")
        identities.add(identity)


def _allocated_bytes(details: tuple[os.stat_result, ...]) -> int | None:
    allocated_bytes = 0
    for item in details:
        try:
            blocks = item.st_blocks
        except (AttributeError, TypeError, ValueError):
            return None
        if type(blocks) is not int or blocks < 0:
            return None
        allocated_bytes += blocks * 512
    return allocated_bytes


def _available_bytes(directory_fd: int) -> int:
    try:
        filesystem = os.fstatvfs(directory_fd)
    except (OSError, TypeError, ValueError) as error:
        raise PathValidationError("incomplete filesystem is inaccessible") from error
    try:
        available_blocks = filesystem.f_bavail
        fragment_size = filesystem.f_frsize
    except (AttributeError, TypeError, ValueError) as error:
        raise PathValidationError("incomplete filesystem statistics are invalid") from error
    if (
        type(available_blocks) is not int
        or available_blocks < 0
        or type(fragment_size) is not int
        or fragment_size <= 0
    ):
        raise PathValidationError("incomplete filesystem statistics are invalid")
    return available_blocks * fragment_size


def _require_root(value: str | os.PathLike[str]) -> Path:
    try:
        root = Path(value)
    except TypeError as error:
        raise TypeError("root must be a filesystem path") from error
    if not root.is_absolute():
        raise PathValidationError("root must be an absolute path")
    if any(part in {".", ".."} for part in root.parts):
        raise PathValidationError("root must not contain traversal components")
    if root != Path.home() / "Downloads" / "Hermes":
        raise PathValidationError("root must be the canonical Downloads/Hermes directory")
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


def _preflight_persisted_workspace(
    root_fd: int,
    destination: DestinationIntent,
    final_component: str,
) -> None:
    """Validate all existing workspace components before creating any entry."""

    final_fd: int | None = None
    incomplete_fd: int | None = None
    job_fd: int | None = None
    final_filesystem_fd = root_fd
    incomplete_filesystem_fd = root_fd
    try:
        if _entry_exists(root_fd, final_component):
            final_fd = _open_existing_directory(root_fd, final_component)
            final_filesystem_fd = final_fd
            _require_writable_directory(destination.final_path.parent)

        if _entry_exists(root_fd, _INCOMPLETE):
            incomplete_fd = _open_existing_directory(root_fd, _INCOMPLETE)
            incomplete_filesystem_fd = incomplete_fd
            if _entry_exists(incomplete_fd, destination.job_id):
                job_fd = _open_existing_directory(incomplete_fd, destination.job_id)
                incomplete_filesystem_fd = job_fd
                _require_writable_directory(destination.incomplete_dir)
            else:
                _require_writable_directory(destination.incomplete_dir.parent)

        _require_same_filesystem(final_filesystem_fd, incomplete_filesystem_fd)
    finally:
        if job_fd is not None:
            os.close(job_fd)
        if incomplete_fd is not None:
            os.close(incomplete_fd)
        if final_fd is not None:
            os.close(final_fd)


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


def _strict_staged_metadata(
    destination: DestinationIntent, staged: StagedPartialPayload,
) -> tuple[int, int, int, int, int, int, int]:
    identity = _validate_staged_partial_payload_identity(destination, staged)
    if any(type(value) is not int for value in (
        staged.st_mode, staged.st_nlink, staged.mtime_ns, staged.ctime_ns,
    )) or not stat.S_ISREG(staged.st_mode) or staged.st_nlink != 1:
        raise PathValidationError("original seven-field staged evidence is absent or invalid")
    return (*identity, staged.st_mode, staged.st_nlink, staged.mtime_ns, staged.ctime_ns)


def _validate_strict_marker(
    destination: DestinationIntent, marker: PublicationReservationMarker,
) -> None:
    if (type(marker) is not PublicationReservationMarker
        or marker.path != destination.incomplete_dir / _RESERVATION_MARKER
        or any(type(value) is not int or value < 0 for value in (marker.st_dev, marker.st_ino))):
        raise PathValidationError("original publication marker authority is invalid")


def _hash_publication_payload(
    descriptor: int, expected: tuple[int, int, int, int, int, int, int],
    *, cancelled=None,
) -> str:
    """Bounded descriptor reads, fenced by complete metadata before and after."""
    if _fstat_publication_payload(descriptor) != expected:
        raise UnsafePathError("payload changed before byte commitment")
    digest = hashlib.sha256()
    offset = 0
    try:
        while offset < expected[2]:
            if cancelled is not None and cancelled():
                raise PathValidationError('publication was cancelled')
            count = min(1024 * 1024, expected[2] - offset)
            chunk = os.pread(descriptor, count, offset)
            if type(chunk) is not bytes or not chunk or len(chunk) > count:
                raise PathValidationError("payload byte commitment read is incomplete")
            digest.update(chunk)
            offset += len(chunk)
        if cancelled is not None and cancelled():
            raise PathValidationError('publication was cancelled')
        if os.pread(descriptor, 1, offset) != b"":
            raise UnsafePathError("payload changed during byte commitment")
        if cancelled is not None and cancelled():
            raise PathValidationError('publication was cancelled')
    except OSError as error:
        raise PathValidationError("payload byte commitment cannot be read") from error
    if _fstat_publication_payload(descriptor) != expected:
        raise UnsafePathError("payload changed during byte commitment")
    return digest.hexdigest()


def _require_strict_publication_namespace(
    prepared: PreparedPublicationPayload, descriptor: int,
    expected: tuple[int, int, int, int, int, int, int], *, published: bool,
) -> None:
    destination = prepared.destination
    root, component = _validate_destination_intent(destination)
    _require_safe_writable_root(root)
    fresh = _open_visible_publication_chain(
        root, component, destination.job_id, prepared.directory_identities,
    )
    try:
        _verify_attested_reservation_marker(
            fresh[3], _reservation_marker_bytes(prepared.reservation), prepared.marker,
        )
        if _stat_visible_publication_payload(fresh[3], destination.partial_path.name) != expected:
            raise UnsafePathError("partial payload changed during publication")
        if published:
            if _stat_visible_publication_payload(fresh[1], destination.final_path.name) != expected:
                raise UnsafePathError("final payload changed during publication")
        else:
            _require_absent_final_name(fresh[1], destination.final_path.name)
        if _fstat_publication_payload(descriptor) != expected:
            raise UnsafePathError("payload descriptor changed during publication")
    finally:
        for fd in reversed(fresh):
            os.close(fd)


def prepare_publication_payload(
    destination: DestinationIntent, reservation: PublicationReservation,
    marker: PublicationReservationMarker, staged_payload: StagedPartialPayload,
    *, cancelled=None,
) -> PreparedPublicationPayload:
    """Prepare strict byte evidence off the owner thread from an original live stage.

    This performs no namespace mutation, and cannot mint absent stage metadata.
    Its digest is an internal commitment to retained bytes, not vendor verification.
    """
    root, component = _validate_destination_intent(destination)
    reservation = _validate_publication_reservation(destination, component, reservation)
    _validate_strict_marker(destination, marker)
    expected = _strict_staged_metadata(destination, staged_payload)
    _require_safe_writable_root(root)
    directories = (root, destination.final_path.parent, root / _INCOMPLETE,
                   destination.incomplete_dir)
    chain = tuple((item.st_dev, item.st_ino) for item in map(_require_real_directory, directories))
    descriptors = _open_visible_publication_chain(root, component, destination.job_id, chain)
    try:
        provisional = PreparedPublicationPayload(
            destination, reservation, marker, staged_payload, "", chain,
        )
        _verify_attested_reservation_marker(
            descriptors[3], _reservation_marker_bytes(reservation), marker,
        )
        if _stat_staged_partial_payload(descriptors[3], destination.partial_path.name) != expected:
            raise UnsafePathError("original staged metadata changed before preparation")
        payload_fd = _open_staged_partial_payload(descriptors[3], destination.partial_path.name)
        try:
            _require_strict_publication_namespace(provisional, payload_fd, expected, published=False)
            digest = (_hash_publication_payload(payload_fd, expected) if cancelled is None else
                _hash_publication_payload(payload_fd, expected, cancelled=cancelled))
            _require_strict_publication_namespace(provisional, payload_fd, expected, published=False)
            return PreparedPublicationPayload(
                destination, reservation, marker, staged_payload, digest, chain,
            )
        finally:
            os.close(payload_fd)
    finally:
        for fd in reversed(descriptors):
            os.close(fd)


def _publish_prepared_payload(
    destination: DestinationIntent, reservation: PublicationReservation,
    staged_payload: StagedPartialPayload, prepared: PreparedPublicationPayload | None,
    creation_permit: PublicationCreationPermit | None, *, existing_only: bool, cancelled=None,
) -> PublishedFinalPayload:
    root, component = _validate_destination_intent(destination)
    reservation = _validate_publication_reservation(destination, component, reservation)
    expected = _strict_staged_metadata(destination, staged_payload)
    if (type(prepared) is not PreparedPublicationPayload
        or (not existing_only and type(creation_permit) is not PublicationCreationPermit)
        or (existing_only and creation_permit is not None)):
        raise PathValidationError("strict publication requires prepared evidence and a creation permit")
    if (prepared.destination != destination or prepared.reservation != reservation
        or prepared.staged_payload != staged_payload):
        raise PathValidationError("prepared publication authority does not match original stage")
    _validate_strict_marker(destination, prepared.marker)
    if (type(prepared.sha256) is not str or len(prepared.sha256) != 64
        or any(value not in "0123456789abcdef" for value in prepared.sha256)
        or type(prepared.directory_identities) is not tuple or len(prepared.directory_identities) != 4
        or any(type(item) is not tuple or len(item) != 2
               or any(type(value) is not int or value < 0 for value in item)
               for item in prepared.directory_identities)):
        raise PathValidationError("prepared publication byte or directory evidence is invalid")
    _require_safe_writable_root(root)
    descriptors = _open_visible_publication_chain(
        root, component, destination.job_id, prepared.directory_identities,
    )
    try:
        _require_same_filesystem(descriptors[1], descriptors[3])
        payload_fd = _open_staged_partial_payload(descriptors[3], destination.partial_path.name)
        try:
            if cancelled is not None and cancelled():
                raise PathValidationError('publication was cancelled')
            if not existing_only:
                _require_strict_publication_namespace(prepared, payload_fd, expected, published=False)
                _link_staged_partial_payload(
                    descriptors[3], descriptors[1], destination.partial_path.name,
                    destination.final_path.name, expected[:3], creation_permit=creation_permit,
                )
            post = _fstat_publication_payload(payload_fd)
            if post[:4] != expected[:4] or post[5] != expected[5] or post[4] != 2:
                raise UnsafePathError("published payload changed from original staged identity")
            # Link legitimately changes ctime/nlink. Freeze the fresh seven-field
            # baseline, then prove actual descriptor bytes and every visible name.
            _require_strict_publication_namespace(prepared, payload_fd, post, published=True)
            digest = (_hash_publication_payload(payload_fd, post) if cancelled is None else
                _hash_publication_payload(payload_fd, post, cancelled=cancelled))
            if digest != prepared.sha256:
                raise UnsafePathError("published payload byte commitment does not match")
            _require_strict_publication_namespace(prepared, payload_fd, post, published=True)
            if cancelled is not None and cancelled():
                raise PathValidationError('publication was cancelled')
            _fsync_published_final_directory(descriptors[1])
            _require_strict_publication_namespace(prepared, payload_fd, post, published=True)
            if cancelled is not None and cancelled():
                raise PathValidationError('publication was cancelled')
            return PublishedFinalPayload(
                path=destination.final_path, st_dev=post[0], st_ino=post[1], logical_size=post[2],
                st_mode=post[3], st_nlink=post[4], mtime_ns=post[5], ctime_ns=post[6],
                sha256=prepared.sha256, marker=prepared.marker,
                directory_identities=prepared.directory_identities,
            )
        finally:
            os.close(payload_fd)
    finally:
        for fd in reversed(descriptors):
            os.close(fd)


def require_current_publication_payload(
    prepared: PreparedPublicationPayload, published: PublishedFinalPayload,
) -> None:
    """Cheap exact descriptor/name proof for the owner's atomic transaction.

    No hashing or fsync occurs here. The complete post-read snapshot fences
    changes since background verification, including restored-mtime rewrites.
    """
    if type(prepared) is not PreparedPublicationPayload or type(published) is not PublishedFinalPayload:
        raise PathValidationError("publication evidence is invalid")
    original = _strict_staged_metadata(prepared.destination, prepared.staged_payload)
    post = (published.st_dev, published.st_ino, published.logical_size, published.st_mode,
            published.st_nlink, published.mtime_ns, published.ctime_ns)
    if (any(type(value) is not int or value < 0 for value in post)
        or post[:4] != original[:4] or post[5] != original[5] or post[4] != 2
        or published.path != prepared.destination.final_path
        or published.sha256 != prepared.sha256 or published.marker != prepared.marker
        or published.directory_identities != prepared.directory_identities):
        raise PathValidationError("publication post evidence does not match")
    root, component = _validate_destination_intent(prepared.destination)
    descriptors = _open_visible_publication_chain(root, component,
        prepared.destination.job_id, prepared.directory_identities)
    try:
        payload_fd = _open_staged_partial_payload(descriptors[3], prepared.destination.partial_path.name)
        try:
            _require_strict_publication_namespace(prepared, payload_fd, post, published=True)
        finally:
            os.close(payload_fd)
    finally:
        for fd in reversed(descriptors):
            os.close(fd)


def _validate_direct_cleanup_evidence(prepared, snapshot, *, links):
    if type(prepared) is not PreparedPublicationPayload:
        raise PathValidationError("cleanup publication evidence is invalid")
    destination = prepared.destination
    root, component = _validate_destination_intent(destination)
    _validate_publication_reservation(destination, component, prepared.reservation)
    _validate_strict_marker(destination, prepared.marker)
    original = _strict_staged_metadata(destination, prepared.staged_payload)
    if (type(snapshot) is not tuple or len(snapshot) != 7
        or any(type(value) is not int or not 0 <= value <= 9223372036854775807 for value in snapshot)
        or not stat.S_ISREG(snapshot[3]) or snapshot[4] != links
        or snapshot[:4] != original[:4] or snapshot[5] != original[5]
        or type(prepared.sha256) is not str or len(prepared.sha256) != 64
        or any(value not in "0123456789abcdef" for value in prepared.sha256)
        or type(prepared.directory_identities) is not tuple or len(prepared.directory_identities) != 4
        or any(type(item) is not tuple or len(item) != 2
               or any(type(value) is not int or not 0 <= value <= 9223372036854775807 for value in item)
               for item in prepared.directory_identities)):
        raise PathValidationError("cleanup snapshot does not match original publication")
    return root, component


def _fstat_direct_cleanup_payload(descriptor):
    try:
        details = os.fstat(descriptor)
    except OSError as error:
        raise PathValidationError("cleanup payload is inaccessible") from error
    if type(details.st_uid) is not int or details.st_uid != os.getuid():
        raise UnsafePathError("cleanup payload owner changed")
    return _publication_payload_details(details)


def _require_direct_cleanup_namespace(prepared, descriptor, snapshot, *, marker_present, partial_present):
    destination = prepared.destination
    root, component = _validate_destination_intent(destination)
    _require_safe_writable_root(root)
    fresh = _open_visible_publication_chain(root, component, destination.job_id,
        prepared.directory_identities)
    try:
        if any(os.fstat(fd).st_uid != os.getuid() for fd in fresh):
            raise UnsafePathError("cleanup directory owner changed")
        if marker_present:
            _verify_attested_reservation_marker(fresh[3],
                _reservation_marker_bytes(prepared.reservation), prepared.marker)
            if os.stat(_RESERVATION_MARKER, dir_fd=fresh[3], follow_symlinks=False).st_uid != os.getuid():
                raise UnsafePathError("cleanup marker owner changed")
        elif _entry_exists(fresh[3], _RESERVATION_MARKER):
            raise UnsafePathError("cleanup marker is not absent")
        if partial_present:
            if _stat_visible_publication_payload(fresh[3], destination.partial_path.name) != snapshot:
                raise UnsafePathError("cleanup partial changed")
        elif _entry_exists(fresh[3], destination.partial_path.name):
            raise UnsafePathError("cleanup partial is not absent")
        if (_stat_visible_publication_payload(fresh[1], destination.final_path.name) != snapshot
            or _fstat_direct_cleanup_payload(descriptor) != snapshot):
            raise UnsafePathError("cleanup final changed")
    finally:
        for fd in reversed(fresh):
            os.close(fd)


def _require_cleanup_final_baseline(current, published_stat):
    if (current[:4] != published_stat[:4] or current[5] != published_stat[5]
        or current[4] != 1
        or any(type(value) is not int or not 0 <= value <= 9223372036854775807 for value in current)):
        raise UnsafePathError("cleanup final no longer matches publication")


def _direct_cleanup_payload(prepared, snapshot):
    return PublishedFinalPayload(path=prepared.destination.final_path,
        st_dev=snapshot[0], st_ino=snapshot[1], logical_size=snapshot[2],
        st_mode=snapshot[3], st_nlink=snapshot[4], mtime_ns=snapshot[5], ctime_ns=snapshot[6],
        sha256=prepared.sha256, marker=prepared.marker,
        directory_identities=prepared.directory_identities)


def _require_cleanup_not_cancelled(cancelled, permit):
    if permit.snapshot().revoked or (cancelled is not None and cancelled()):
        raise PathValidationError("cleanup was cancelled")


def require_direct_cleanup_namespace(prepared, stat_tuple, *, phase):
    """Cheap phase-specific namespace evidence; missing partial is not a certificate."""
    if type(phase) is not str or phase not in {"pending", "certified", "finished"}:
        raise PathValidationError("cleanup phase is invalid")
    root, component = _validate_direct_cleanup_evidence(prepared, stat_tuple,
        links=2 if phase == "pending" else 1)
    _require_safe_writable_root(root)
    descriptors = _open_visible_publication_chain(root, component,
        prepared.destination.job_id, prepared.directory_identities)
    try:
        fd = _open_publication_payload(descriptors[1], prepared.destination.final_path.name)
        try:
            current = _fstat_direct_cleanup_payload(fd)
            partial_present = _entry_exists(descriptors[3], prepared.destination.partial_path.name)
            marker_present = _entry_exists(descriptors[3], _RESERVATION_MARKER)
            if phase == "pending":
                if not marker_present:
                    raise UnsafePathError("pending cleanup marker is missing")
                if partial_present:
                    if current != stat_tuple:
                        raise UnsafePathError("pending cleanup publication changed")
                else:
                    _require_cleanup_final_baseline(current, stat_tuple)
            elif current != stat_tuple or partial_present or (phase == "finished" and marker_present):
                raise UnsafePathError("certified cleanup namespace changed")
            _require_direct_cleanup_namespace(prepared, fd, current,
                marker_present=marker_present, partial_present=partial_present)
        finally:
            os.close(fd)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def require_current_direct_cleanup_payload(prepared, published, *, marker_present):
    """Owner check of a fresh final-only certificate without hashing or fsync."""
    if type(published) is not PublishedFinalPayload or type(marker_present) is not bool:
        raise PathValidationError("cleanup final evidence is invalid")
    snapshot = (published.st_dev, published.st_ino, published.logical_size,
        published.st_mode, published.st_nlink, published.mtime_ns, published.ctime_ns)
    root, component = _validate_direct_cleanup_evidence(prepared, snapshot, links=1)
    if (published.path != prepared.destination.final_path or published.sha256 != prepared.sha256
        or published.marker != prepared.marker or published.directory_identities != prepared.directory_identities):
        raise PathValidationError("cleanup final evidence does not match publication")
    descriptors = _open_visible_publication_chain(root, component,
        prepared.destination.job_id, prepared.directory_identities)
    try:
        fd = _open_publication_payload(descriptors[1], prepared.destination.final_path.name)
        try:
            _require_direct_cleanup_namespace(prepared, fd, snapshot,
                marker_present=marker_present, partial_present=False)
        finally:
            os.close(fd)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def prepare_direct_cleanup_payload(prepared, published_stat, *, cancelled=None, partial_permit):
    """Unlink only the original partial, then durably prove the retained final bytes."""
    if type(partial_permit) is not _PartialUnlinkPermit or partial_permit.snapshot().unlink_attempted:
        raise PathValidationError("exact partial unlink permit is required")
    root, component = _validate_direct_cleanup_evidence(prepared, published_stat, links=2)
    _require_cleanup_not_cancelled(cancelled, partial_permit)
    _require_safe_writable_root(root)
    descriptors = _open_visible_publication_chain(root, component,
        prepared.destination.job_id, prepared.directory_identities)
    try:
        destination = prepared.destination
        fd = _open_publication_payload(descriptors[1], destination.final_path.name)
        try:
            current = _fstat_direct_cleanup_payload(fd)
            if _entry_exists(descriptors[3], destination.partial_path.name):
                if current != published_stat:
                    raise UnsafePathError("cleanup publication changed before partial unlink")
                _require_direct_cleanup_namespace(prepared, fd, current,
                    marker_present=True, partial_present=True)
                _require_cleanup_not_cancelled(cancelled, partial_permit)
                partial_permit._enter_unlink()
                try:
                    os.unlink(destination.partial_path.name, dir_fd=descriptors[3])
                except OSError as error:
                    raise PathValidationError("owned cleanup partial cannot be unlinked") from error
                finally:
                    partial_permit._finish_unlink()
                current = _fstat_direct_cleanup_payload(fd)
            _require_cleanup_final_baseline(current, published_stat)
            _require_direct_cleanup_namespace(prepared, fd, current,
                marker_present=True, partial_present=False)
            _require_cleanup_not_cancelled(cancelled, partial_permit)
            _fsync_staged_partial_directory(descriptors[3])
            _require_cleanup_not_cancelled(cancelled, partial_permit)
            # The original descriptor spans unlink; hashing uses a freshly opened one.
            fresh_fd = _open_publication_payload(descriptors[1], destination.final_path.name)
            try:
                _require_direct_cleanup_namespace(prepared, fresh_fd, current,
                    marker_present=True, partial_present=False)
                digest = (_hash_publication_payload(fresh_fd, current) if cancelled is None else
                    _hash_publication_payload(fresh_fd, current, cancelled=cancelled))
                if digest != prepared.sha256:
                    raise UnsafePathError("cleanup final byte commitment does not match")
                _require_direct_cleanup_namespace(prepared, fresh_fd, current,
                    marker_present=True, partial_present=False)
                if _fstat_direct_cleanup_payload(fd) != current:
                    raise UnsafePathError("retained cleanup final changed")
                _require_cleanup_not_cancelled(cancelled, partial_permit)
                return _direct_cleanup_payload(prepared, current)
            finally:
                os.close(fresh_fd)
        finally:
            os.close(fd)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def finish_direct_cleanup_payload(prepared, certified_stat, *, cancelled=None, marker_permit):
    """Remove only the exact marker after the owner has committed its certificate."""
    if type(marker_permit) is not _MarkerUnlinkPermit or marker_permit.snapshot().unlink_attempted:
        raise PathValidationError("exact marker unlink permit is required")
    root, component = _validate_direct_cleanup_evidence(prepared, certified_stat, links=1)
    _require_cleanup_not_cancelled(cancelled, marker_permit)
    _require_safe_writable_root(root)
    descriptors = _open_visible_publication_chain(root, component,
        prepared.destination.job_id, prepared.directory_identities)
    try:
        fd = _open_publication_payload(descriptors[1], prepared.destination.final_path.name)
        try:
            marker_present = _entry_exists(descriptors[3], _RESERVATION_MARKER)
            _require_direct_cleanup_namespace(prepared, fd, certified_stat,
                marker_present=marker_present, partial_present=False)
            _require_cleanup_not_cancelled(cancelled, marker_permit)
            if marker_present:
                marker_permit._enter_unlink()
                try:
                    os.unlink(_RESERVATION_MARKER, dir_fd=descriptors[3])
                except OSError as error:
                    raise PathValidationError("owned cleanup marker cannot be unlinked") from error
                finally:
                    marker_permit._finish_unlink()
            _require_cleanup_not_cancelled(cancelled, marker_permit)
            _fsync_staged_partial_directory(descriptors[3])
            _require_direct_cleanup_namespace(prepared, fd, certified_stat,
                marker_present=False, partial_present=False)
            _require_cleanup_not_cancelled(cancelled, marker_permit)
            return _direct_cleanup_payload(prepared, certified_stat)
        finally:
            os.close(fd)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
