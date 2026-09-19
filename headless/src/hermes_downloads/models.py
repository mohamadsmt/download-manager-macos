"""Durable download intent and admission gates, independent of engine observation."""

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
import re
from typing import Final
import unicodedata

__all__ = [
    "Admission",
    "DownloadIntent",
    "JobState",
    "MaterializedJob",
    "SourceKind",
]

_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SHA256_DIGEST: Final = re.compile(r"[0-9a-f]{64}\Z")
_MAX_COUNTER: Final = (1 << 63) - 1
_MIN_PRIORITY: Final = -(1 << 31)
_MAX_PRIORITY: Final = (1 << 31) - 1
_CATEGORIES: Final = frozenset({"Videos", "Audio", "Documents", "Software", "Other"})


class JobState(str, Enum):
    """Public job lifecycle states; engine observations are not authorization."""

    QUEUED = "queued"
    RESOLVING = "resolving"
    DOWNLOADING = "downloading"
    PAUSING = "pausing"
    PAUSED = "paused"
    RETRY_WAIT = "retry_wait"
    NEEDS_LINK = "needs_link"
    NEEDS_AUTH = "needs_auth"
    BLOCKED = "blocked"
    FINALIZING = "finalizing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    REMOVED = "removed"
    FAILED = "failed"


class SourceKind(str, Enum):
    """The immutable source family selected while materializing a job."""

    DIRECT = "direct"
    VIDEO = "video"


def _require_identifier(value: object, name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} must be a nonblank identifier")
    return value


def _require_payload_digest(value: object) -> str:
    if type(value) is not str:
        raise TypeError("payload_digest must be a string")
    if _SHA256_DIGEST.fullmatch(value) is None:
        raise ValueError("payload_digest must be a lowercase SHA-256 digest")
    return value


def _require_counter(value: object, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= _MAX_COUNTER:
        raise ValueError(f"{name} must be a nonnegative persisted counter")
    return value


def _snapshot_source_url(value: object) -> bytes:
    if not isinstance(value, (bytes, bytearray)):
        raise TypeError("source_url must be bytes")
    source_url = bytes(value)
    if not source_url:
        raise ValueError("source_url must not be empty")
    return source_url


def _require_path_component(value: object, name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not value or value in {".", ".."} or value.startswith("."):
        raise ValueError(f"{name} is not a valid output name")
    if "/" in value or "\\" in value or "\x00" in value:
        raise ValueError(f"{name} must be one path component")
    if any(unicodedata.category(character) in {"Cc", "Cs"} for character in value):
        raise ValueError(f"{name} contains a control character")
    return value


def _collision_filename(filename: str, job_id: str) -> str:
    suffix = Path(filename).suffix
    stem = filename[: -len(suffix)] if suffix else filename
    return f"{stem}--{job_id}{suffix}"


@dataclass(frozen=True, slots=True)
class Admission:
    """The independent gates that must all be open before a transfer may start."""

    queue_running: bool
    collection_held: bool
    authorized: bool
    item_held: bool
    due: bool

    def __post_init__(self) -> None:
        for name in (
            "queue_running",
            "collection_held",
            "authorized",
            "item_held",
            "due",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be a boolean")

    @property
    def allowed(self) -> bool:
        return (
            self.queue_running
            and not self.collection_held
            and self.authorized
            and not self.item_held
            and self.due
        )


@dataclass(frozen=True, slots=True)
class DownloadIntent:
    """An immutable command snapshot, including idempotency and concurrency data."""

    job_id: str
    request_id: str
    payload_digest: str
    source_url: bytes | bytearray
    expected_revision: int | None = None
    generation: int = 0
    revision: int = 0

    def __post_init__(self) -> None:
        _require_identifier(self.job_id, "job_id")
        _require_identifier(self.request_id, "request_id")
        _require_payload_digest(self.payload_digest)
        if self.expected_revision is not None:
            _require_counter(self.expected_revision, "expected_revision")
        _require_counter(self.generation, "generation")
        _require_counter(self.revision, "revision")
        # URL parsing and policy are intentionally deferred to T07.
        object.__setattr__(self, "source_url", _snapshot_source_url(self.source_url))


@dataclass(frozen=True, slots=True)
class MaterializedJob:
    """Durable queue, admission, and destination projections for one intent."""

    job_id: str
    intent: DownloadIntent
    source_kind: SourceKind
    queue_collection_id: str
    priority: int
    order_key: int
    scheduled_for: datetime
    authorized: bool
    manual_hold: bool
    start_now_requested: bool
    category: str
    destination_collection: str | None
    partial_filename: str
    selected_final_filename: str

    def __post_init__(self) -> None:
        job_id = _require_identifier(self.job_id, "job_id")
        if type(self.intent) is not DownloadIntent:
            raise TypeError("intent must be a DownloadIntent")
        if self.intent.job_id != job_id:
            raise ValueError("job_id must match intent.job_id")
        if type(self.source_kind) is not SourceKind:
            raise TypeError("source_kind must be a SourceKind")
        _require_identifier(self.queue_collection_id, "queue_collection_id")
        if type(self.priority) is not int:
            raise TypeError("priority must be an integer")
        if not _MIN_PRIORITY <= self.priority <= _MAX_PRIORITY:
            raise ValueError("priority must fit a signed 32-bit integer")
        _require_counter(self.order_key, "order_key")
        if type(self.scheduled_for) is not datetime:
            raise TypeError("scheduled_for must be a datetime")
        if self.scheduled_for.tzinfo is None or self.scheduled_for.utcoffset() is None:
            raise ValueError("scheduled_for must be timezone-aware")
        object.__setattr__(self, "scheduled_for", self.scheduled_for.astimezone(UTC))
        for name in ("authorized", "manual_hold", "start_now_requested"):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be a boolean")
        if type(self.category) is not str or self.category not in _CATEGORIES:
            raise ValueError("category is not a supported output category")
        if self.destination_collection is not None:
            _require_path_component(self.destination_collection, "destination_collection")
        partial_filename = _require_path_component(
            self.partial_filename, "partial_filename"
        )
        selected_final_filename = _require_path_component(
            self.selected_final_filename, "selected_final_filename"
        )
        if selected_final_filename not in {
            partial_filename,
            _collision_filename(partial_filename, job_id),
        }:
            raise ValueError("selected_final_filename is not a managed final name")
