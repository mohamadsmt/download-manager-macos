"""Durable download intent and admission gates, independent of engine observation."""

from dataclasses import dataclass
from enum import Enum
import re
from typing import Final

__all__ = ["Admission", "DownloadIntent", "JobState"]

_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SHA256_DIGEST: Final = re.compile(r"[0-9a-f]{64}\Z")
_MAX_COUNTER: Final = (1 << 63) - 1


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
