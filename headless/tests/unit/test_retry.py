"""Behavioral contract for bounded retry decisions and source replacement."""

from __future__ import annotations

import builtins
import importlib
import importlib.util

import pytest


_RETRY_AUDIT_CAPACITY = 256


def _retry():
    spec = importlib.util.find_spec("hermes_downloads.retry")
    assert spec is not None, "hermes_downloads.retry must provide bounded retry decisions"
    return importlib.import_module("hermes_downloads.retry")


def _authority(retry, *, generation: int = 7, policy=None):
    return retry.RetryAuthority.open(
        policy=retry.RetryPolicy() if policy is None else policy,
        job_id="retry-job",
        generation=generation,
    )


def _failure(retry, kind, **overrides):
    return retry.RetryFailure(kind=kind, **overrides)


def _valid_retry_audit(retry, *, event_count: int):
    assert event_count >= 1
    generation = 7
    budget_number = 1
    ordinary_attempts = 0
    exhausted = False
    audit = [
        retry.RetryAuditEvent(
            retry.RetryAuditKind.OPENED,
            generation=generation,
            budget_number=budget_number,
            ordinary_attempts=ordinary_attempts,
        )
    ]
    for _ in range(1, event_count):
        if exhausted:
            generation += 1
            budget_number += 1
            ordinary_attempts = 0
            exhausted = False
            kind = retry.RetryAuditKind.EXPLICIT_RESUME
        elif ordinary_attempts + 1 >= retry.RetryPolicy().max_ordinary_attempts:
            ordinary_attempts += 1
            exhausted = True
            kind = retry.RetryAuditKind.EXHAUSTED
        else:
            ordinary_attempts += 1
            kind = retry.RetryAuditKind.RETRY_SCHEDULED
        audit.append(
            retry.RetryAuditEvent(
                kind,
                generation=generation,
                budget_number=budget_number,
                ordinary_attempts=ordinary_attempts,
            )
        )
    return (
        tuple(audit),
        generation,
        budget_number,
        ordinary_attempts,
        exhausted,
    )


def test_retry_authority_rejects_direct_construction_from_valid_budget_snapshots() -> None:
    retry = _retry()
    policy = retry.RetryPolicy()
    authority = _authority(retry, policy=policy)
    snapshot = authority.budget
    forged_snapshot = retry.RetryBudget(
        job_id=snapshot.job_id,
        generation=snapshot.generation,
        budget_number=snapshot.budget_number,
        ordinary_attempts=snapshot.ordinary_attempts,
        paused=snapshot.paused,
        exhausted=snapshot.exhausted,
        audit=snapshot.audit,
    )

    assert forged_snapshot == snapshot
    for candidate in (snapshot, forged_snapshot):
        with pytest.raises(TypeError, match=r"RetryAuthority\.open"):
            retry.RetryAuthority(policy=policy, budget=candidate)

    decision = authority.decide(
        _failure(retry, retry.FailureKind.TRANSIENT_HOST),
        generation=snapshot.generation,
        jitter_seconds=0,
    )
    assert decision.budget.ordinary_attempts == 1


def test_retry_budget_rejects_valid_over_capacity_audit_before_scanning_or_replaying(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retry = _retry()
    audit, generation, budget_number, ordinary_attempts, exhausted = _valid_retry_audit(
        retry, event_count=_RETRY_AUDIT_CAPACITY + 1
    )

    def unexpected_history_work(*_args, **_kwargs):
        raise AssertionError("over-capacity audit must not be scanned or replayed")

    monkeypatch.setattr(builtins, "any", unexpected_history_work)
    monkeypatch.setattr(retry, "_validate_retry_audit", unexpected_history_work)

    with pytest.raises(ValueError, match=r"audit.*capacity"):
        retry.RetryBudget(
            job_id="retry-job",
            generation=generation,
            budget_number=budget_number,
            ordinary_attempts=ordinary_attempts,
            paused=False,
            exhausted=exhausted,
            audit=audit,
        )


def test_retry_transitions_fail_closed_when_audit_capacity_is_reached() -> None:
    retry = _retry()
    authority = _authority(retry)

    while len(authority.budget.audit) < _RETRY_AUDIT_CAPACITY:
        budget = authority.budget
        if budget.exhausted:
            authority.resume_after_exhaustion(new_generation=budget.generation + 1)
        else:
            authority.decide(
                _failure(retry, retry.FailureKind.TRANSIENT_HOST),
                generation=budget.generation,
                jitter_seconds=0,
            )

    at_capacity = authority.budget
    with pytest.raises(OverflowError, match=r"audit.*capacity"):
        if at_capacity.exhausted:
            authority.resume_after_exhaustion(
                new_generation=at_capacity.generation + 1
            )
        else:
            authority.decide(
                _failure(retry, retry.FailureKind.TRANSIENT_HOST),
                generation=at_capacity.generation,
                jitter_seconds=0,
            )

    assert authority.budget is at_capacity
    assert len(authority.budget.audit) == _RETRY_AUDIT_CAPACITY


def test_transient_host_failures_consume_at_most_five_outer_attempts() -> None:
    retry = _retry()
    policy = retry.RetryPolicy()
    authority = _authority(retry, policy=policy)
    budget = authority.budget
    decisions = []

    for expected_attempt in range(1, policy.max_ordinary_attempts + 1):
        decision = authority.decide(
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

    sixth = authority.decide(
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
    authority = _authority(retry, policy=policy)

    offline = authority.decide(
        _failure(retry, retry.FailureKind.OFFLINE),
        generation=7,
    )
    host = authority.decide(
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
    decision = _authority(retry).decide(
        _failure(retry, getattr(retry.FailureKind, kind)),
        generation=7,
    )

    assert decision.action is getattr(retry.RetryAction, action)
    assert decision.delay_seconds is None
    assert decision.budget.ordinary_attempts == 0


def test_forbidden_http_response_stays_ambiguous_and_does_not_assume_expiry() -> None:
    retry = _retry()
    forbidden = retry.failure_from_http_status(403)

    decision = _authority(retry).decide(forbidden, generation=7)

    assert forbidden.kind is retry.FailureKind.FORBIDDEN
    assert decision.action is retry.RetryAction.NEEDS_DECISION
    assert decision.budget.ordinary_attempts == 0


def test_disk_full_is_blocked_without_a_network_retry() -> None:
    retry = _retry()

    decision = _authority(retry).decide(
        _failure(retry, retry.FailureKind.DISK_FULL),
        generation=7,
    )

    assert decision.action is retry.RetryAction.BLOCKED
    assert decision.delay_seconds is None
    assert decision.budget.ordinary_attempts == 0


def test_pause_closes_a_pending_retry_before_its_timer_can_retry() -> None:
    retry = _retry()
    policy = retry.RetryPolicy()
    authority = _authority(retry, policy=policy)
    pending = authority.decide(
        _failure(retry, retry.FailureKind.TRANSIENT_HOST),
        generation=7,
        jitter_seconds=0,
    )

    paused = authority.pause(generation=7)
    timer = authority.decide(
        _failure(retry, retry.FailureKind.TRANSIENT_HOST),
        generation=paused.budget.generation,
        jitter_seconds=0,
    )

    assert paused.action is retry.RetryAction.PAUSED
    assert paused.budget.paused is True
    assert timer.action is retry.RetryAction.PAUSED
    assert timer.delay_seconds is None
    assert timer.budget.ordinary_attempts == 1


def test_pause_invalidates_a_pending_timer_holding_the_prior_budget() -> None:
    retry = _retry()
    policy = retry.RetryPolicy()
    authority = _authority(retry, policy=policy)
    pending = authority.decide(
        _failure(retry, retry.FailureKind.TRANSIENT_HOST),
        generation=7,
        jitter_seconds=0,
    )
    paused = authority.pause(generation=7)

    with pytest.raises(retry.StaleGenerationError):
        authority.decide(
            _failure(retry, retry.FailureKind.TRANSIENT_HOST),
            generation=7,
            jitter_seconds=0,
        )

    assert pending.action is retry.RetryAction.RETRY_WAIT
    assert paused.action is retry.RetryAction.PAUSED
    assert paused.budget.ordinary_attempts == pending.budget.ordinary_attempts == 1
    assert paused.budget.audit[-1].kind is retry.RetryAuditKind.PAUSED


def test_stale_generation_cannot_change_a_retry_budget() -> None:
    retry = _retry()

    with pytest.raises(retry.StaleGenerationError):
        _authority(retry, generation=7).decide(
            _failure(retry, retry.FailureKind.TRANSIENT_HOST),
            generation=6,
            jitter_seconds=0,
        )


def test_explicit_resume_after_exhaustion_opens_a_new_audited_budget() -> None:
    retry = _retry()
    policy = retry.RetryPolicy()
    authority = _authority(retry, generation=7, policy=policy)
    exhausted = authority.budget
    for _ in range(policy.max_ordinary_attempts):
        exhausted = authority.decide(
            _failure(retry, retry.FailureKind.TRANSIENT_HOST),
            generation=7,
            jitter_seconds=0,
        ).budget

    resumed = authority.resume_after_exhaustion(new_generation=8)

    assert exhausted.exhausted is True
    assert resumed is not exhausted
    assert resumed.generation == 8
    assert resumed.budget_number == exhausted.budget_number + 1
    assert resumed.ordinary_attempts == 0
    assert resumed.exhausted is False
    assert resumed.audit[-1].kind is retry.RetryAuditKind.EXPLICIT_RESUME
    assert resumed.audit[-1].generation == 8
    with pytest.raises(ValueError):
        authority.resume_after_exhaustion(new_generation=7)


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


def test_retry_authority_rejects_a_callback_forging_the_old_generation_after_pause() -> None:
    retry = _retry()
    authority = _authority(retry)
    pending = authority.decide(
        _failure(retry, retry.FailureKind.TRANSIENT_HOST),
        generation=7,
        jitter_seconds=0,
    )
    paused = authority.pause(generation=7)

    with pytest.raises(retry.StaleGenerationError):
        authority.decide(
            _failure(retry, retry.FailureKind.TRANSIENT_HOST),
            generation=7,
            jitter_seconds=0,
        )
    with pytest.raises(TypeError):
        authority.decide(
            _failure(retry, retry.FailureKind.TRANSIENT_HOST),
            generation=7,
            current_generation=7,
        )

    assert pending.action is retry.RetryAction.RETRY_WAIT
    assert paused.action is retry.RetryAction.PAUSED
    assert authority.budget == paused.budget


def test_retry_module_exposes_no_free_retry_state_transition_bypass() -> None:
    retry = _retry()

    for name in (
        "open_retry_budget",
        "decide_retry",
        "pause_retry",
        "resume_after_exhaustion",
    ):
        assert name not in retry.__all__
        assert not hasattr(retry, name)


def test_retry_budget_rejects_hydrated_exhaustion_audit_with_false_terminal_flag() -> None:
    retry = _retry()
    audit = (
        retry.RetryAuditEvent(
            retry.RetryAuditKind.OPENED,
            generation=7,
            budget_number=1,
            ordinary_attempts=0,
        ),
        retry.RetryAuditEvent(
            retry.RetryAuditKind.EXHAUSTED,
            generation=7,
            budget_number=1,
            ordinary_attempts=1,
        ),
    )

    with pytest.raises(ValueError, match="audit"):
        retry.RetryBudget(
            job_id="retry-job",
            generation=7,
            budget_number=1,
            ordinary_attempts=1,
            paused=False,
            exhausted=False,
            audit=audit,
        )


@pytest.mark.parametrize(
    (
        "audit_spec",
        "generation",
        "budget_number",
        "ordinary_attempts",
        "paused",
        "exhausted",
    ),
    (
        (
            (("RETRY_SCHEDULED", 7, 1, 1),),
            7,
            1,
            1,
            False,
            False,
        ),
        (
            (("OPENED", 7, 1, 0), ("OPENED", 7, 1, 0)),
            7,
            1,
            0,
            False,
            False,
        ),
        (
            (("OPENED", 7, 1, 0), ("RETRY_SCHEDULED", 7, 1, 2)),
            7,
            1,
            2,
            False,
            False,
        ),
        (
            (("OPENED", 7, 1, 0), ("EXHAUSTED", 7, 1, 0)),
            7,
            1,
            0,
            False,
            True,
        ),
        (
            (("OPENED", 7, 1, 0), ("PAUSED", 7, 1, 0)),
            7,
            1,
            0,
            True,
            False,
        ),
        (
            (("OPENED", 7, 1, 0), ("EXPLICIT_RESUME", 8, 2, 0)),
            8,
            2,
            0,
            False,
            False,
        ),
        (
            (
                ("OPENED", 7, 1, 0),
                ("EXHAUSTED", 7, 1, 1),
                ("EXPLICIT_RESUME", 8, 1, 0),
            ),
            8,
            1,
            0,
            False,
            False,
        ),
        ((("OPENED", 7, 1, 0),), 8, 1, 0, False, False),
    ),
    ids=(
        "missing-opening",
        "duplicate-opening",
        "retry-skips-attempt",
        "exhaustion-does-not-advance-attempt",
        "pause-does-not-advance-generation",
        "resume-without-exhaustion",
        "resume-does-not-advance-budget-number",
        "snapshot-does-not-match-terminal-event",
    ),
)
def test_retry_budget_rejects_nonrepresentable_hydrated_audit_history(
    audit_spec,
    generation: int,
    budget_number: int,
    ordinary_attempts: int,
    paused: bool,
    exhausted: bool,
) -> None:
    retry = _retry()
    audit = tuple(
        retry.RetryAuditEvent(
            getattr(retry.RetryAuditKind, kind),
            generation=event_generation,
            budget_number=event_budget_number,
            ordinary_attempts=event_attempts,
        )
        for kind, event_generation, event_budget_number, event_attempts in audit_spec
    )

    with pytest.raises(ValueError, match="audit"):
        retry.RetryBudget(
            job_id="retry-job",
            generation=generation,
            budget_number=budget_number,
            ordinary_attempts=ordinary_attempts,
            paused=paused,
            exhausted=exhausted,
            audit=audit,
        )
