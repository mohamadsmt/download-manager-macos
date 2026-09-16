"""Behavioral contract for bounded retry decisions and source replacement."""

from __future__ import annotations

import importlib
import importlib.util

import pytest


def _retry():
    spec = importlib.util.find_spec("hermes_downloads.retry")
    assert spec is not None, "hermes_downloads.retry must provide bounded retry decisions"
    return importlib.import_module("hermes_downloads.retry")


def _budget(retry, *, generation: int = 7):
    return retry.open_retry_budget(job_id="retry-job", generation=generation)


def _failure(retry, kind, **overrides):
    return retry.RetryFailure(kind=kind, **overrides)


def test_transient_host_failures_consume_at_most_five_outer_attempts() -> None:
    retry = _retry()
    policy = retry.RetryPolicy()
    budget = _budget(retry)
    decisions = []

    for expected_attempt in range(1, policy.max_ordinary_attempts + 1):
        decision = retry.decide_retry(
            policy,
            budget,
            _failure(retry, retry.FailureKind.TRANSIENT_HOST),
            generation=7,
            jitter_seconds=0,
        )
        decisions.append(decision)
        budget = decision.budget
        assert budget.ordinary_attempts == expected_attempt

    assert [decision.action for decision in decisions] == [
        retry.RetryAction.RETRY_WAIT,
        retry.RetryAction.RETRY_WAIT,
        retry.RetryAction.RETRY_WAIT,
        retry.RetryAction.RETRY_WAIT,
        retry.RetryAction.EXHAUSTED,
    ]
    assert budget.exhausted is True
    assert [event.kind for event in budget.audit] == [
        retry.RetryAuditKind.OPENED,
        retry.RetryAuditKind.RETRY_SCHEDULED,
        retry.RetryAuditKind.RETRY_SCHEDULED,
        retry.RetryAuditKind.RETRY_SCHEDULED,
        retry.RetryAuditKind.RETRY_SCHEDULED,
        retry.RetryAuditKind.EXHAUSTED,
    ]

    sixth = retry.decide_retry(
        policy,
        budget,
        _failure(retry, retry.FailureKind.TRANSIENT_HOST),
        generation=7,
        jitter_seconds=0,
    )
    assert sixth.action is retry.RetryAction.EXHAUSTED
    assert sixth.budget.ordinary_attempts == policy.max_ordinary_attempts
    assert sixth.budget.audit == budget.audit


def test_backoff_uses_bounded_jitter_retry_after_and_five_minute_cap() -> None:
    retry = _retry()
    policy = retry.RetryPolicy()

    assert policy.delay_for_attempt(1, jitter_seconds=0.5) == 5.5
    assert policy.delay_for_attempt(2, jitter_seconds=1.0) == 11.0
    assert policy.delay_for_attempt(1, retry_after_seconds=7, jitter_seconds=0) == 7
    assert policy.delay_for_attempt(8, retry_after_seconds=999, jitter_seconds=0) == 300
    with pytest.raises(ValueError):
        policy.delay_for_attempt(1, jitter_seconds=0.51)


def test_offline_wait_is_distinct_from_a_host_failure_and_does_not_spend_budget() -> None:
    retry = _retry()
    policy = retry.RetryPolicy()
    budget = _budget(retry)

    offline = retry.decide_retry(
        policy,
        budget,
        _failure(retry, retry.FailureKind.OFFLINE),
        generation=7,
    )
    host = retry.decide_retry(
        policy,
        budget,
        _failure(retry, retry.FailureKind.TRANSIENT_HOST),
        generation=7,
        jitter_seconds=0,
    )

    assert offline.action is retry.RetryAction.OFFLINE_WAIT
    assert offline.delay_seconds is None
    assert offline.budget.ordinary_attempts == 0
    assert host.action is retry.RetryAction.RETRY_WAIT
    assert host.budget.ordinary_attempts == 1


@pytest.mark.parametrize(
    ("kind", "action"),
    (
        ("AUTH_NEEDED", "NEEDS_AUTH"),
        ("LINK_NEEDED", "NEEDS_LINK"),
    ),
)
def test_auth_and_link_needed_stop_ordinary_retries(
    kind: str, action: str
) -> None:
    retry = _retry()
    decision = retry.decide_retry(
        retry.RetryPolicy(),
        _budget(retry),
        _failure(retry, getattr(retry.FailureKind, kind)),
        generation=7,
    )

    assert decision.action is getattr(retry.RetryAction, action)
    assert decision.delay_seconds is None
    assert decision.budget.ordinary_attempts == 0


def test_forbidden_http_response_stays_ambiguous_and_does_not_assume_expiry() -> None:
    retry = _retry()
    forbidden = retry.failure_from_http_status(403)

    decision = retry.decide_retry(
        retry.RetryPolicy(), _budget(retry), forbidden, generation=7
    )

    assert forbidden.kind is retry.FailureKind.FORBIDDEN
    assert decision.action is retry.RetryAction.NEEDS_DECISION
    assert decision.budget.ordinary_attempts == 0


def test_disk_full_is_blocked_without_a_network_retry() -> None:
    retry = _retry()

    decision = retry.decide_retry(
        retry.RetryPolicy(),
        _budget(retry),
        _failure(retry, retry.FailureKind.DISK_FULL),
        generation=7,
    )

    assert decision.action is retry.RetryAction.BLOCKED
    assert decision.delay_seconds is None
    assert decision.budget.ordinary_attempts == 0


def test_pause_closes_a_pending_retry_before_its_timer_can_retry() -> None:
    retry = _retry()
    policy = retry.RetryPolicy()
    pending = retry.decide_retry(
        policy,
        _budget(retry),
        _failure(retry, retry.FailureKind.TRANSIENT_HOST),
        generation=7,
        jitter_seconds=0,
    )

    paused = retry.pause_retry(pending.budget, generation=7)
    timer = retry.decide_retry(
        policy,
        paused.budget,
        _failure(retry, retry.FailureKind.TRANSIENT_HOST),
        generation=7,
        jitter_seconds=0,
    )

    assert paused.action is retry.RetryAction.PAUSED
    assert paused.budget.paused is True
    assert timer.action is retry.RetryAction.PAUSED
    assert timer.delay_seconds is None
    assert timer.budget.ordinary_attempts == 1


def test_stale_generation_cannot_change_a_retry_budget() -> None:
    retry = _retry()

    with pytest.raises(retry.StaleGenerationError):
        retry.decide_retry(
            retry.RetryPolicy(),
            _budget(retry, generation=7),
            _failure(retry, retry.FailureKind.TRANSIENT_HOST),
            generation=6,
            jitter_seconds=0,
        )


def test_explicit_resume_after_exhaustion_opens_a_new_audited_budget() -> None:
    retry = _retry()
    policy = retry.RetryPolicy()
    exhausted = _budget(retry, generation=7)
    for _ in range(policy.max_ordinary_attempts):
        exhausted = retry.decide_retry(
            policy,
            exhausted,
            _failure(retry, retry.FailureKind.TRANSIENT_HOST),
            generation=7,
            jitter_seconds=0,
        ).budget

    resumed = retry.resume_after_exhaustion(exhausted, new_generation=8)

    assert exhausted.exhausted is True
    assert resumed is not exhausted
    assert resumed.generation == 8
    assert resumed.budget_number == exhausted.budget_number + 1
    assert resumed.ordinary_attempts == 0
    assert resumed.exhausted is False
    assert resumed.audit[-1].kind is retry.RetryAuditKind.EXPLICIT_RESUME
    assert resumed.audit[-1].generation == 8
    with pytest.raises(ValueError):
        retry.resume_after_exhaustion(exhausted, new_generation=7)


def test_matching_strong_validator_permits_resuming_the_preserved_partial() -> None:
    retry = _retry()
    previous = retry.SourceIdentity(
        strong_validator='"revision-1"',
        trusted_sha256=None,
        filename="same-name.bin",
        total_length=1024,
    )
    replacement = retry.SourceIdentity(
        strong_validator='"revision-1"',
        trusted_sha256=None,
        filename="renamed.bin",
        total_length=2048,
    )

    decision = retry.validate_source_replacement(
        previous,
        replacement,
        current_generation=9,
        callback_generation=9,
    )

    assert decision.action is retry.SourceReplacementAction.RESUME
    assert decision.evidence is retry.SourceIdentityEvidence.STRONG_VALIDATOR
    assert decision.preserve_partial is True


def test_matching_trusted_digest_permits_resume_when_validators_differ() -> None:
    retry = _retry()
    previous = retry.SourceIdentity(
        strong_validator='"old-validator"',
        trusted_sha256="a" * 64,
        filename="old.bin",
        total_length=1024,
    )
    replacement = retry.SourceIdentity(
        strong_validator='"new-validator"',
        trusted_sha256="a" * 64,
        filename="new.bin",
        total_length=2048,
    )

    decision = retry.validate_source_replacement(
        previous,
        replacement,
        current_generation=9,
        callback_generation=9,
    )

    assert decision.action is retry.SourceReplacementAction.RESUME
    assert decision.evidence is retry.SourceIdentityEvidence.TRUSTED_DIGEST
    assert decision.preserve_partial is True


def test_same_name_and_size_without_identity_proof_preserves_partial_for_decision() -> None:
    retry = _retry()
    previous = retry.SourceIdentity(
        strong_validator=None,
        trusted_sha256=None,
        filename="same-name.bin",
        total_length=1024,
    )
    replacement = retry.SourceIdentity(
        strong_validator=None,
        trusted_sha256=None,
        filename="same-name.bin",
        total_length=1024,
    )

    decision = retry.validate_source_replacement(
        previous,
        replacement,
        current_generation=9,
        callback_generation=9,
    )

    assert decision.action is retry.SourceReplacementAction.NEEDS_DECISION
    assert decision.evidence is retry.SourceIdentityEvidence.UNKNOWN
    assert decision.preserve_partial is True


def test_conflicting_trusted_digests_override_matching_validator() -> None:
    retry = _retry()
    previous = retry.SourceIdentity(
        strong_validator='"same-validator"',
        trusted_sha256="a" * 64,
        filename="same-name.bin",
        total_length=1024,
    )
    replacement = retry.SourceIdentity(
        strong_validator='"same-validator"',
        trusted_sha256="b" * 64,
        filename="same-name.bin",
        total_length=1024,
    )

    decision = retry.validate_source_replacement(
        previous,
        replacement,
        current_generation=9,
        callback_generation=9,
    )

    assert decision.action is retry.SourceReplacementAction.NEEDS_DECISION
    assert decision.preserve_partial is True


def test_weak_or_stale_identity_cannot_authorize_source_replacement() -> None:
    retry = _retry()
    with pytest.raises(ValueError):
        retry.SourceIdentity(
            strong_validator='W/"weak"',
            trusted_sha256=None,
            filename="payload.bin",
            total_length=1024,
        )

    unknown = retry.SourceIdentity(
        strong_validator=None,
        trusted_sha256=None,
        filename="payload.bin",
        total_length=1024,
    )
    with pytest.raises(retry.StaleGenerationError):
        retry.validate_source_replacement(
            unknown,
            unknown,
            current_generation=9,
            callback_generation=8,
        )
