"""Deterministic, in-memory queue admission and dispatch ordering."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import Enum
import re
from typing import Final

from hermes_downloads.models import Admission

__all__ = ["DownloadQueue", "QueueGate", "QueueJob"]


_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_MAX_ORDER_KEY: Final = (1 << 63) - 1
_MIN_PRIORITY: Final = -(1 << 31)
_MAX_PRIORITY: Final = (1 << 31) - 1


class QueueGate(str, Enum):
    """The global scheduling gate; only ``RUNNING`` admits new dispatches."""

    PAUSED = "paused"
    RUNNING = "running"


def _require_identifier(value: object, name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} must be a nonblank identifier")
    return value


def _require_boolean(value: object, name: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{name} must be a boolean")
    return value


def _require_priority(value: object) -> int:
    if type(value) is not int:
        raise TypeError("priority must be an integer")
    if not _MIN_PRIORITY <= value <= _MAX_PRIORITY:
        raise ValueError("priority is outside the supported range")
    return value


def _require_order_key(value: object) -> int:
    if type(value) is not int:
        raise TypeError("order_key must be an integer")
    if not 0 <= value <= _MAX_ORDER_KEY:
        raise ValueError("order_key must be a nonnegative persisted counter")
    return value


def _canonical_time(value: object, name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _optional_time(value: object, name: str) -> datetime | None:
    if value is None:
        return None
    return _canonical_time(value, name)


@dataclass(frozen=True, slots=True)
class QueueJob:
    """Immutable scheduling intent for one job; it contains no engine state."""

    job_id: str
    collection_id: str | None
    priority: int
    order_key: int
    scheduled_for: datetime | None
    authorized: bool
    manual_hold: bool
    start_now_requested: bool
    removed: bool

    def __post_init__(self) -> None:
        _require_identifier(self.job_id, "job_id")
        if self.collection_id is not None:
            _require_identifier(self.collection_id, "collection_id")
        _require_priority(self.priority)
        _require_order_key(self.order_key)
        object.__setattr__(
            self,
            "scheduled_for",
            _optional_time(self.scheduled_for, "scheduled_for"),
        )
        for name in ("authorized", "manual_hold", "start_now_requested", "removed"):
            _require_boolean(getattr(self, name), name)


class DownloadQueue:
    """A pure in-memory scheduler with explicit intent and admission gates.

    This class only decides which stored intent is eligible to dispatch.  It does
    not start engines or cancel running work.  ``pause_queue`` closes admission
    immediately; worker-side cancellation is deliberately outside this layer.
    """

    def __init__(self, *, queue_running: bool = False) -> None:
        _require_boolean(queue_running, "queue_running")
        self._gate = QueueGate.RUNNING if queue_running else QueueGate.PAUSED
        self._jobs: dict[str, QueueJob] = {}
        self._held_collections: set[str] = set()
        self._next_order_key = 0

    @property
    def gate(self) -> QueueGate:
        """Return the current global admission gate."""

        return self._gate

    @property
    def queue_running(self) -> bool:
        """Whether the global gate currently admits new dispatches."""

        return self._gate is QueueGate.RUNNING

    def job(self, job_id: str) -> QueueJob:
        """Return one job by its exact stable identifier."""

        _require_identifier(job_id, "job_id")
        try:
            return self._jobs[job_id]
        except KeyError as error:
            raise KeyError(f"unknown job_id: {job_id}") from error

    def enqueue(
        self,
        job_id: str,
        *,
        collection_id: str | None = None,
        priority: int = 0,
        scheduled_for: datetime | None = None,
        authorized: bool = False,
    ) -> QueueJob:
        """Add one unambiguous job intent with a FIFO default order key."""

        _require_identifier(job_id, "job_id")
        if job_id in self._jobs:
            raise ValueError("job_id is already present")
        if self._next_order_key > _MAX_ORDER_KEY:
            raise OverflowError("no order keys remain")
        job = QueueJob(
            job_id=job_id,
            collection_id=collection_id,
            priority=priority,
            order_key=self._next_order_key,
            scheduled_for=scheduled_for,
            authorized=authorized,
            manual_hold=False,
            start_now_requested=False,
            removed=False,
        )
        self._jobs[job_id] = job
        self._next_order_key += 1
        return job

    def admission_for(self, job_id: str, *, now: datetime) -> Admission | None:
        """Return all existing admission gates, or ``None`` for a tombstone."""

        job = self.job(job_id)
        at = _canonical_time(now, "now")
        return self._admission_for_job(job, at)

    def next_job(self, *, now: datetime) -> QueueJob | None:
        """Return the next eligible job without mutating queue or engine state."""

        at = _canonical_time(now, "now")
        eligible = [
            job
            for job in self._jobs.values()
            if (admission := self._admission_for_job(job, at)) is not None
            and admission.allowed
        ]
        if not eligible:
            return None
        return min(eligible, key=self._dispatch_key)

    def pause_queue(self) -> QueueGate:
        """Synchronously close the global admission gate before returning."""

        self._gate = QueueGate.PAUSED
        return self._gate

    def resume_all(self) -> QueueGate:
        """Open only the global gate; collection and manual holds remain intact."""

        self._gate = QueueGate.RUNNING
        return self._gate

    def hold_collection(self, collection_id: str) -> None:
        """Close the independent collection gate for one exact collection ID."""

        _require_identifier(collection_id, "collection_id")
        self._held_collections.add(collection_id)

    def resume_collection(self, collection_id: str) -> None:
        """Open one collection gate without touching per-item manual holds."""

        _require_identifier(collection_id, "collection_id")
        self._held_collections.discard(collection_id)

    def hold_item(self, job_id: str) -> QueueJob:
        """Persist a manual per-item hold in memory."""

        return self._replace_live_job(job_id, manual_hold=True)

    def resume_item(self, job_id: str) -> QueueJob:
        """Clear only a manual per-item hold."""

        return self._replace_live_job(job_id, manual_hold=False)

    def authorize(self, job_id: str) -> QueueJob:
        """Grant explicit transfer authorization without changing schedule gates."""

        return self._replace_live_job(job_id, authorized=True)

    def start_now(self, job_id: str) -> QueueJob:
        """Authorize and preempt a job only while the global queue is running."""

        job = self._live_job(job_id)
        if not self.queue_running:
            return job
        return self._replace_live_job(
            job_id,
            authorized=True,
            start_now_requested=True,
        )

    def remove(self, job_id: str) -> QueueJob:
        """Tombstone one job so it remains inspectable but can never dispatch."""

        job = self.job(job_id)
        if job.removed:
            return job
        tombstone = replace(job, removed=True)
        self._jobs[job_id] = tombstone
        return tombstone

    def reorder(self, job_id: str, *, before_job_id: str) -> QueueJob:
        """Move one live job before another live job in the same priority band."""

        moving = self._live_job(job_id)
        before = self._live_job(before_job_id)
        if moving.job_id == before.job_id:
            raise ValueError("job_id and before_job_id must differ")
        if moving.priority != before.priority:
            raise ValueError("reorder requires jobs with the same priority")

        ordered = sorted(
            self._jobs.values(),
            key=lambda job: (job.order_key, job.job_id),
        )
        ordered = [job for job in ordered if job.job_id != moving.job_id]
        target_index = next(
            index for index, job in enumerate(ordered) if job.job_id == before.job_id
        )
        ordered.insert(target_index, moving)
        for order_key, job in enumerate(ordered):
            self._jobs[job.job_id] = replace(job, order_key=order_key)
        self._next_order_key = max(self._next_order_key, len(ordered))
        return self._jobs[job_id]

    def _admission_for_job(self, job: QueueJob, now: datetime) -> Admission | None:
        if job.removed:
            return None
        collection_held = (
            job.collection_id is not None and job.collection_id in self._held_collections
        )
        due = job.start_now_requested or (
            job.scheduled_for is None or job.scheduled_for <= now
        )
        return Admission(
            queue_running=self.queue_running,
            collection_held=collection_held,
            authorized=job.authorized,
            item_held=job.manual_hold,
            due=due,
        )

    @staticmethod
    def _dispatch_key(job: QueueJob) -> tuple[int, int, int, str]:
        return (
            0 if job.start_now_requested else 1,
            -job.priority,
            job.order_key,
            job.job_id,
        )

    def _live_job(self, job_id: str) -> QueueJob:
        job = self.job(job_id)
        if job.removed:
            raise ValueError("removed jobs cannot be changed")
        return job

    def _replace_live_job(self, job_id: str, **changes: object) -> QueueJob:
        job = self._live_job(job_id)
        updated = replace(job, **changes)
        self._jobs[job_id] = updated
        return updated
