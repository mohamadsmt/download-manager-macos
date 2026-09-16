"""Bounded retry decisions and identity-only source replacement gates.

This module does not sleep, issue HTTP requests, or start download engines.  A
worker records and schedules the returned decisions while keeping the single
outer attempt budget authoritative.
"""

from __future__ import annotations

from _thread import LockType
from dataclasses import dataclass
from enum import Enum
import math
import re
from threading import Lock
from typing import Final, cast

from hermes_downloads.network import SourceURL

__all__ = [
    "CompletionVerification",
    "FailureKind",
    "RetryAction",
    "RetryAuthority",
    "RetryAuditEvent",
    "RetryAuditKind",
    "RetryBudget",
    "RetryDecision",
    "RetryFailure",
    "RetryPolicy",
    "SourceIdentity",
    "SourceIdentityEvidence",
    "SourceReplacementAction",
    "SourceReplacementDecision",
    "StaleGenerationError",
    "failure_from_http_status",
    "validate_source_replacement",
]

_MAX_COUNTER: Final = (1 << 63) - 1
_MAX_ORDINARY_ATTEMPTS: Final = 5
# Audit snapshots are immutable tuples.  This cap bounds both validation of
# hydrated snapshots and transition-time tuple copies while retaining dozens
# of explicit exhausted-resume cycles for one job.
_MAX_RETRY_AUDIT_EVENTS: Final = 256
_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SHA256: Final = re.compile(r"[0-9a-f]{64}\Z")
_MAX_STRONG_VALIDATOR_BYTES: Final = 8192


class StaleGenerationError(RuntimeError):
    """A callback does not belong to the current job generation."""


class CompletionVerification(str, Enum):
    """What a terminal direct-transfer observation actually verified."""

    TRANSPORT_VERIFIED = "transport-verified"
    CHECKSUM_VERIFIED = "checksum-verified"


class FailureKind(str, Enum):
    """Typed failure categories supplied by a worker or engine adapter."""

    TRANSIENT_HOST = "transient_host"
    OFFLINE = "offline"
    AUTH_NEEDED = "auth_needed"
    LINK_NEEDED = "link_needed"
    FORBIDDEN = "forbidden"
    DISK_FULL = "disk_full"
    PERMANENT_HOST = "permanent_host"


class RetryAction(str, Enum):
    """The worker action selected by a bounded retry decision."""

    RETRY_WAIT = "retry_wait"
    OFFLINE_WAIT = "offline_wait"
    NEEDS_AUTH = "needs_auth"
    NEEDS_LINK = "needs_link"
    NEEDS_DECISION = "needs_decision"
    BLOCKED = "blocked"
    EXHAUSTED = "exhausted"
    PAUSED = "paused"


class RetryAuditKind(str, Enum):
    """Persistable audit events for one outer retry budget."""

    OPENED = "opened"
    RETRY_SCHEDULED = "retry_scheduled"
    EXHAUSTED = "exhausted"
    PAUSED = "paused"
    EXPLICIT_RESUME = "explicit_resume"


class SourceReplacementAction(str, Enum):
    """Whether a replacement source can safely continue a partial file."""

    RESUME = "resume"
    NEEDS_DECISION = "needs_decision"


class SourceIdentityEvidence(str, Enum):
    """The actual proof (or lack of proof) for a replacement decision."""

    STRONG_VALIDATOR = "strong_validator"
    TRUSTED_DIGEST = "trusted_digest"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Fixed upper-bound policy for one direct job's ordinary attempts."""

    max_ordinary_attempts: int = _MAX_ORDINARY_ATTEMPTS
    base_delay_seconds: int = 5
    max_delay_seconds: int = 5 * 60
    max_jitter_fraction: float = 0.1

    def __post_init__(self) -> None:
        _require_int_in_range(
            self.max_ordinary_attempts,
            "max_ordinary_attempts",
            minimum=1,
            maximum=_MAX_ORDINARY_ATTEMPTS,
        )
        _require_int_in_range(
            self.base_delay_seconds,
            "base_delay_seconds",
            minimum=1,
            maximum=5,
        )
        _require_int_in_range(
            self.max_delay_seconds,
            "max_delay_seconds",
            minimum=self.base_delay_seconds,
            maximum=5 * 60,
        )
        _require_fraction(self.max_jitter_fraction, "max_jitter_fraction", maximum=0.1)

    def delay_for_attempt(
        self,
        attempt: int,
        *,
        retry_after_seconds: int | None = None,
        jitter_seconds: float = 0,
    ) -> float:
        """Return a bounded retry delay without sleeping or starting an engine."""

        _require_int_in_range(attempt, "attempt", minimum=1, maximum=_MAX_COUNTER)
        retry_after = _require_retry_after(retry_after_seconds)
        backoff = self._backoff_seconds(attempt)
        jitter = _require_jitter(
            jitter_seconds, maximum=backoff * self.max_jitter_fraction
        )
        requested = backoff + jitter
        if retry_after is not None:
            requested = max(requested, retry_after)
        return float(min(self.max_delay_seconds, requested))

    def _backoff_seconds(self, attempt: int) -> int:
        delay = self.base_delay_seconds
        for _ in range(attempt - 1):
            if delay >= self.max_delay_seconds:
                break
            delay = min(self.max_delay_seconds, delay * 2)
        return delay


@dataclass(frozen=True, slots=True)
class RetryFailure:
    """A classified error observation, with an already-parsed Retry-After."""

    kind: FailureKind
    retry_after_seconds: int | None = None

    def __post_init__(self) -> None:
        if type(self.kind) is not FailureKind:
            raise TypeError("kind must be a FailureKind")
        retry_after = _require_retry_after(self.retry_after_seconds)
        if retry_after is not None and self.kind is not FailureKind.TRANSIENT_HOST:
            raise ValueError("Retry-After requires a transient host failure")


@dataclass(frozen=True, slots=True)
class RetryAuditEvent:
    """Small, serializable evidence of an outer-budget transition."""

    kind: RetryAuditKind
    generation: int
    budget_number: int
    ordinary_attempts: int

    def __post_init__(self) -> None:
        if type(self.kind) is not RetryAuditKind:
            raise TypeError("kind must be a RetryAuditKind")
        _require_generation(self.generation)
        _require_int_in_range(
            self.budget_number, "budget_number", minimum=1, maximum=_MAX_COUNTER
        )
        _require_int_in_range(
            self.ordinary_attempts,
            "ordinary_attempts",
            minimum=0,
            maximum=_MAX_ORDINARY_ATTEMPTS,
        )


@dataclass(frozen=True, slots=True)
class RetryBudget:
    """An immutable audited outer-job snapshot capped at 256 events."""

    job_id: str
    generation: int
    budget_number: int
    ordinary_attempts: int
    paused: bool
    exhausted: bool
    audit: tuple[RetryAuditEvent, ...]

    def __post_init__(self) -> None:
        _require_identifier(self.job_id, "job_id")
        _require_generation(self.generation)
        _require_int_in_range(
            self.budget_number, "budget_number", minimum=1, maximum=_MAX_COUNTER
        )
        _require_int_in_range(
            self.ordinary_attempts,
            "ordinary_attempts",
            minimum=0,
            maximum=_MAX_ORDINARY_ATTEMPTS,
        )
        if type(self.paused) is not bool or type(self.exhausted) is not bool:
            raise TypeError("paused and exhausted must be booleans")
        if type(self.audit) is not tuple or not self.audit:
            raise ValueError("audit must contain the budget opening")
        if len(self.audit) > _MAX_RETRY_AUDIT_EVENTS:
            raise ValueError("audit exceeds the retry audit capacity")
        if any(type(event) is not RetryAuditEvent for event in self.audit):
            raise TypeError("audit must contain RetryAuditEvent values")
        _validate_retry_audit(self)


@dataclass(frozen=True, slots=True)
class RetryDecision:
    """Pure retry result; a worker performs any later wait or state mutation."""

    action: RetryAction
    budget: RetryBudget
    delay_seconds: float | None

    def __post_init__(self) -> None:
        if type(self.action) is not RetryAction:
            raise TypeError("action must be a RetryAction")
        if type(self.budget) is not RetryBudget:
            raise TypeError("budget must be a RetryBudget")
        if self.delay_seconds is None:
            return
        _require_jitter(self.delay_seconds, maximum=float("inf"))


class RetryAuthority:
    """Own one job's current immutable retry budget and generation.

    The supported construction path is :meth:`open`, which owns the current
    budget rather than accepting a snapshot as mutable-state input.
    """

    _budget: RetryBudget
    _lock: LockType
    _policy: RetryPolicy
    __slots__ = ("_budget", "_lock", "_policy")

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError(
            "RetryAuthority instances must be opened with RetryAuthority.open()"
        )

    @classmethod
    def open(
        cls, *, policy: RetryPolicy, job_id: str, generation: int
    ) -> RetryAuthority:
        """Open and own the initial retry budget for one job."""

        if type(policy) is not RetryPolicy:
            raise TypeError("policy must be a RetryPolicy")
        authority = object.__new__(cls)
        authority._policy = policy
        authority._budget = _open_retry_budget(job_id=job_id, generation=generation)
        authority._lock = Lock()
        return authority

    @property
    def budget(self) -> RetryBudget:
        """Return the immutable current snapshot owned by this authority."""

        with self._lock:
            return self._budget

    def decide(
        self,
        failure: RetryFailure,
        *,
        generation: int,
        jitter_seconds: float = 0,
    ) -> RetryDecision:
        """Record a callback only when its captured generation is still current."""

        if type(failure) is not RetryFailure:
            raise TypeError("failure must be a RetryFailure")
        with self._lock:
            _require_current_generation(self._budget, generation)
            budget = self._budget
            policy = self._policy
            if budget.paused:
                decision = RetryDecision(RetryAction.PAUSED, budget, None)
            elif budget.exhausted:
                decision = RetryDecision(RetryAction.EXHAUSTED, budget, None)
            elif failure.kind is FailureKind.OFFLINE:
                decision = RetryDecision(RetryAction.OFFLINE_WAIT, budget, None)
            elif failure.kind is FailureKind.AUTH_NEEDED:
                decision = RetryDecision(RetryAction.NEEDS_AUTH, budget, None)
            elif failure.kind is FailureKind.LINK_NEEDED:
                decision = RetryDecision(RetryAction.NEEDS_LINK, budget, None)
            elif failure.kind is FailureKind.FORBIDDEN:
                decision = RetryDecision(RetryAction.NEEDS_DECISION, budget, None)
            elif failure.kind in {FailureKind.DISK_FULL, FailureKind.PERMANENT_HOST}:
                decision = RetryDecision(RetryAction.BLOCKED, budget, None)
            else:
                next_attempt = budget.ordinary_attempts + 1
                if next_attempt >= policy.max_ordinary_attempts:
                    exhausted = _transition_budget(
                        budget,
                        kind=RetryAuditKind.EXHAUSTED,
                        generation=budget.generation,
                        budget_number=budget.budget_number,
                        ordinary_attempts=next_attempt,
                        paused=False,
                        exhausted=True,
                    )
                    decision = RetryDecision(RetryAction.EXHAUSTED, exhausted, None)
                else:
                    retrying = _transition_budget(
                        budget,
                        kind=RetryAuditKind.RETRY_SCHEDULED,
                        generation=budget.generation,
                        budget_number=budget.budget_number,
                        ordinary_attempts=next_attempt,
                        paused=False,
                        exhausted=False,
                    )
                    decision = RetryDecision(
                        RetryAction.RETRY_WAIT,
                        retrying,
                        policy.delay_for_attempt(
                            next_attempt,
                            retry_after_seconds=failure.retry_after_seconds,
                            jitter_seconds=jitter_seconds,
                        ),
                    )
            self._budget = decision.budget
            return decision

    def pause(self, *, generation: int) -> RetryDecision:
        """Close the current budget before a pending timer may start an engine."""

        with self._lock:
            _require_current_generation(self._budget, generation)
            decision = _pause_retry(self._budget)
            self._budget = decision.budget
            return decision

    def resume_after_exhaustion(self, *, new_generation: int) -> RetryBudget:
        """Open a distinct audited budget after an explicit exhausted resume."""

        with self._lock:
            self._budget = _resume_after_exhaustion(
                self._budget, new_generation=new_generation
            )
            return self._budget


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    """Identity evidence for a validated direct source; name and length are not proof."""

    source: SourceURL
    strong_validator: str | None
    trusted_sha256: str | None
    filename: str | None = None
    total_length: int | None = None

    def __post_init__(self) -> None:
        if type(self.source) is not SourceURL:
            raise TypeError("source must be a validated SourceURL")
        _require_optional_strong_validator(self.strong_validator)
        _require_optional_sha256(self.trusted_sha256, "trusted_sha256")
        if self.filename is not None and type(self.filename) is not str:
            raise TypeError("filename must be a string or None")
        if self.total_length is not None:
            _require_int_in_range(
                self.total_length,
                "total_length",
                minimum=0,
                maximum=_MAX_COUNTER,
            )


@dataclass(frozen=True, slots=True)
class SourceReplacementDecision:
    """Identity result that never authorizes overwriting or discarding a partial."""

    action: SourceReplacementAction
    evidence: SourceIdentityEvidence
    preserve_partial: bool = True

    def __post_init__(self) -> None:
        if type(self.action) is not SourceReplacementAction:
            raise TypeError("action must be a SourceReplacementAction")
        if type(self.evidence) is not SourceIdentityEvidence:
            raise TypeError("evidence must be a SourceIdentityEvidence")
        if self.preserve_partial is not True:
            raise ValueError("source replacement must preserve the existing partial")


def _open_retry_budget(*, job_id: str, generation: int) -> RetryBudget:
    """Open the initial recorded budget for a job generation."""

    _require_identifier(job_id, "job_id")
    _require_generation(generation)
    event = RetryAuditEvent(
        kind=RetryAuditKind.OPENED,
        generation=generation,
        budget_number=1,
        ordinary_attempts=0,
    )
    return RetryBudget(
        job_id=job_id,
        generation=generation,
        budget_number=1,
        ordinary_attempts=0,
        paused=False,
        exhausted=False,
        audit=(event,),
    )


def failure_from_http_status(
    status: int, *, retry_after_seconds: int | None = None
) -> RetryFailure:
    """Classify an HTTP response without making an HTTP request.

    In particular, 403 remains ambiguous: it is neither automatic expiry nor
    automatic authentication failure.
    """

    _require_int_in_range(status, "status", minimum=100, maximum=599)
    if status in {401, 407}:
        return RetryFailure(FailureKind.AUTH_NEEDED)
    if status == 403:
        return RetryFailure(FailureKind.FORBIDDEN)
    if status in {404, 410}:
        return RetryFailure(FailureKind.LINK_NEEDED)
    if status in {408, 425, 429} or 500 <= status <= 599:
        return RetryFailure(
            FailureKind.TRANSIENT_HOST, retry_after_seconds=retry_after_seconds
        )
    return RetryFailure(FailureKind.PERMANENT_HOST)


def _pause_retry(budget: RetryBudget) -> RetryDecision:
    """Close a pending retry synchronously before a timer may start an engine."""

    if type(budget) is not RetryBudget:
        raise TypeError("budget must be a RetryBudget")
    if budget.exhausted:
        return RetryDecision(RetryAction.EXHAUSTED, budget, None)
    if budget.paused:
        return RetryDecision(RetryAction.PAUSED, budget, None)
    paused = _transition_budget(
        budget,
        kind=RetryAuditKind.PAUSED,
        generation=budget.generation + 1,
        budget_number=budget.budget_number,
        ordinary_attempts=budget.ordinary_attempts,
        paused=True,
        exhausted=False,
    )
    return RetryDecision(RetryAction.PAUSED, paused, None)


def _resume_after_exhaustion(
    budget: RetryBudget, *, new_generation: int
) -> RetryBudget:
    """Open a distinct audited budget only after an explicit exhausted resume."""

    if type(budget) is not RetryBudget:
        raise TypeError("budget must be a RetryBudget")
    _require_generation(new_generation)
    if not budget.exhausted:
        raise ValueError("only an exhausted budget can be explicitly resumed")
    if new_generation <= budget.generation:
        raise ValueError("new_generation must advance the exhausted generation")
    if budget.budget_number >= _MAX_COUNTER:
        raise OverflowError("no retry budget numbers remain")
    return _transition_budget(
        budget,
        kind=RetryAuditKind.EXPLICIT_RESUME,
        generation=new_generation,
        budget_number=budget.budget_number + 1,
        ordinary_attempts=0,
        paused=False,
        exhausted=False,
    )


def validate_source_replacement(
    previous: SourceIdentity,
    replacement: SourceIdentity,
    *,
    current_generation: int,
    callback_generation: int,
) -> SourceReplacementDecision:
    """Allow append/resume only when source content identity is actually proven."""

    if type(previous) is not SourceIdentity or type(replacement) is not SourceIdentity:
        raise TypeError("source identities must be SourceIdentity values")
    _require_generation(current_generation)
    _require_generation(callback_generation)
    if callback_generation != current_generation:
        raise StaleGenerationError("stale source replacement callback")

    if (
        previous.trusted_sha256 is not None
        and replacement.trusted_sha256 is not None
        and previous.trusted_sha256 != replacement.trusted_sha256
    ):
        return SourceReplacementDecision(
            SourceReplacementAction.NEEDS_DECISION, SourceIdentityEvidence.UNKNOWN
        )
    if (
        previous.trusted_sha256 is not None
        and previous.trusted_sha256 == replacement.trusted_sha256
    ):
        return SourceReplacementDecision(
            SourceReplacementAction.RESUME, SourceIdentityEvidence.TRUSTED_DIGEST
        )
    if (
        previous.strong_validator is not None
        and previous.strong_validator == replacement.strong_validator
        and previous.source.raw_url == replacement.source.raw_url
    ):
        return SourceReplacementDecision(
            SourceReplacementAction.RESUME, SourceIdentityEvidence.STRONG_VALIDATOR
        )
    return SourceReplacementDecision(
        SourceReplacementAction.NEEDS_DECISION, SourceIdentityEvidence.UNKNOWN
    )


def _transition_budget(
    budget: RetryBudget,
    *,
    kind: RetryAuditKind,
    generation: int,
    budget_number: int,
    ordinary_attempts: int,
    paused: bool,
    exhausted: bool,
) -> RetryBudget:
    if len(budget.audit) >= _MAX_RETRY_AUDIT_EVENTS:
        raise OverflowError("retry audit capacity is exhausted")
    event = RetryAuditEvent(
        kind=kind,
        generation=generation,
        budget_number=budget_number,
        ordinary_attempts=ordinary_attempts,
    )
    return RetryBudget(
        job_id=budget.job_id,
        generation=generation,
        budget_number=budget_number,
        ordinary_attempts=ordinary_attempts,
        paused=paused,
        exhausted=exhausted,
        audit=(*budget.audit, event),
    )


def _validate_retry_audit(budget: RetryBudget) -> None:
    opening = budget.audit[0]
    if (
        opening.kind is not RetryAuditKind.OPENED
        or opening.budget_number != 1
        or opening.ordinary_attempts != 0
    ):
        raise ValueError("audit must begin with an unopened first budget")

    generation = opening.generation
    budget_number = opening.budget_number
    ordinary_attempts = opening.ordinary_attempts
    paused = False
    exhausted = False
    for event in budget.audit[1:]:
        if event.kind is RetryAuditKind.RETRY_SCHEDULED:
            if (
                paused
                or exhausted
                or event.generation != generation
                or event.budget_number != budget_number
                or event.ordinary_attempts != ordinary_attempts + 1
                or event.ordinary_attempts >= _MAX_ORDINARY_ATTEMPTS
            ):
                raise ValueError("audit contains an invalid scheduled retry")
            ordinary_attempts = event.ordinary_attempts
            continue
        if event.kind is RetryAuditKind.EXHAUSTED:
            if (
                paused
                or exhausted
                or event.generation != generation
                or event.budget_number != budget_number
                or event.ordinary_attempts != ordinary_attempts + 1
            ):
                raise ValueError("audit contains an invalid exhaustion")
            ordinary_attempts = event.ordinary_attempts
            exhausted = True
            continue
        if event.kind is RetryAuditKind.PAUSED:
            if (
                paused
                or exhausted
                or event.generation != generation + 1
                or event.budget_number != budget_number
                or event.ordinary_attempts != ordinary_attempts
            ):
                raise ValueError("audit contains an invalid pause")
            generation = event.generation
            paused = True
            continue
        if event.kind is RetryAuditKind.EXPLICIT_RESUME:
            if (
                paused
                or not exhausted
                or event.generation <= generation
                or event.budget_number != budget_number + 1
                or event.ordinary_attempts != 0
            ):
                raise ValueError("audit contains an invalid explicit resume")
            generation = event.generation
            budget_number = event.budget_number
            ordinary_attempts = 0
            exhausted = False
            continue
        raise ValueError("audit contains an unexpected opening")

    if (
        budget.generation != generation
        or budget.budget_number != budget_number
        or budget.ordinary_attempts != ordinary_attempts
        or budget.paused is not paused
        or budget.exhausted is not exhausted
    ):
        raise ValueError("audit does not match retry budget state")


def _require_current_generation(budget: RetryBudget, generation: int) -> None:
    _require_generation(generation)
    if generation != budget.generation:
        raise StaleGenerationError("stale retry callback")


def _require_identifier(value: object, name: str) -> str:
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} must be a nonblank identifier")
    return value


def _require_generation(value: object) -> int:
    return _require_int_in_range(
        value, "generation", minimum=0, maximum=_MAX_COUNTER
    )


def _require_int_in_range(
    value: object, name: str, *, minimum: int, maximum: int
) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} is outside the supported range")
    return value


def _require_fraction(value: object, name: str, *, maximum: float) -> float:
    if type(value) not in {int, float}:
        raise TypeError(f"{name} must be a finite number")
    number = float(cast(int | float, value))
    if not math.isfinite(number):
        raise TypeError(f"{name} must be a finite number")
    if not 0 <= number <= maximum:
        raise ValueError(f"{name} is outside the supported range")
    return number


def _require_retry_after(value: object) -> int | None:
    if value is None:
        return None
    return _require_int_in_range(
        value, "retry_after_seconds", minimum=0, maximum=_MAX_COUNTER
    )


def _require_jitter(value: object, *, maximum: float) -> float:
    if type(value) not in {int, float}:
        raise TypeError("jitter_seconds must be a finite number")
    jitter = float(cast(int | float, value))
    if not math.isfinite(jitter):
        raise TypeError("jitter_seconds must be a finite number")
    if not 0 <= jitter <= maximum:
        raise ValueError("jitter_seconds exceeds the bounded retry window")
    return jitter


def _require_optional_sha256(value: object, name: str) -> str | None:
    if value is None:
        return None
    if type(value) is not str:
        raise TypeError(f"{name} must be a string or None")
    if _SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _require_optional_strong_validator(value: object) -> str | None:
    if value is None:
        return None
    if type(value) is not str:
        raise TypeError("strong_validator must be a string or None")
    if (
        not value.isascii()
        or len(value.encode("ascii")) > _MAX_STRONG_VALIDATOR_BYTES
        or value.startswith(("W/", "w/"))
        or len(value) < 2
        or not (value.startswith('"') and value.endswith('"'))
        or '"' in value[1:-1]
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value)
    ):
        raise ValueError("strong_validator must be a strong quoted HTTP validator")
    return value
