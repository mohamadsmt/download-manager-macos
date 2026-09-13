"""Behavioral contract for deterministic in-memory queue admission."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import importlib
import importlib.util

import pytest


NOW = datetime(2030, 1, 2, 3, 4, tzinfo=UTC)


def _queue_module():
    spec = importlib.util.find_spec("hermes_downloads.queue")
    assert spec is not None, "hermes_downloads.queue must provide deterministic scheduling"
    return importlib.import_module("hermes_downloads.queue")


def _queue(*, running: bool = True):
    return _queue_module().DownloadQueue(queue_running=running)


def test_higher_priority_dispatches_before_lower_priority() -> None:
    queue = _queue()
    queue.enqueue("job-low", priority=-4, authorized=True)
    queue.enqueue("job-high", priority=9, authorized=True)

    next_job = queue.next_job(now=NOW)

    assert next_job is not None
    assert next_job.job_id == "job-high"


def test_equal_priority_defaults_to_fifo_using_generated_order_keys() -> None:
    queue = _queue()
    first = queue.enqueue("job-first", priority=2, authorized=True)
    second = queue.enqueue("job-second", priority=2, authorized=True)

    next_job = queue.next_job(now=NOW)

    assert first.order_key < second.order_key
    assert next_job is not None
    assert next_job.job_id == "job-first"


def test_manual_reorder_changes_dispatch_within_an_equal_priority_band() -> None:
    queue = _queue()
    first = queue.enqueue("job-first", priority=2, authorized=True)
    second = queue.enqueue("job-second", priority=2, authorized=True)
    third = queue.enqueue("job-third", priority=2, authorized=True)

    queue.reorder("job-third", before_job_id="job-first")

    assert queue.job("job-third").order_key < queue.job("job-first").order_key
    assert queue.job("job-first").order_key < queue.job("job-second").order_key
    assert third.order_key > first.order_key
    next_job = queue.next_job(now=NOW)
    assert next_job is not None
    assert next_job.job_id == "job-third"


def test_due_schedule_is_a_gate_and_never_authorizes_a_job() -> None:
    queue = _queue()
    queue.enqueue(
        "job-future",
        priority=10,
        scheduled_for=NOW + timedelta(hours=1),
        authorized=True,
    )
    queue.enqueue(
        "job-due-but-unauthorized",
        priority=1,
        scheduled_for=NOW - timedelta(seconds=1),
    )

    future_admission = queue.admission_for("job-future", now=NOW)
    due_admission = queue.admission_for("job-due-but-unauthorized", now=NOW)

    assert future_admission.due is False
    assert future_admission.authorized is True
    assert future_admission.allowed is False
    assert due_admission.due is True
    assert due_admission.authorized is False
    assert due_admission.allowed is False
    assert queue.next_job(now=NOW) is None

    queue.authorize("job-due-but-unauthorized")
    next_job = queue.next_job(now=NOW)
    assert next_job is not None
    assert next_job.job_id == "job-due-but-unauthorized"


def test_collection_hold_and_manual_item_hold_are_distinct_admission_gates() -> None:
    queue = _queue()
    queue.enqueue("job-collection", collection_id="collection-1", authorized=True)
    queue.enqueue("job-other", collection_id="collection-2", authorized=True)

    queue.hold_collection("collection-1")
    collection_admission = queue.admission_for("job-collection", now=NOW)

    assert queue.job("job-collection").manual_hold is False
    assert collection_admission.collection_held is True
    assert collection_admission.item_held is False
    assert collection_admission.allowed is False
    next_job = queue.next_job(now=NOW)
    assert next_job is not None
    assert next_job.job_id == "job-other"

    queue.resume_collection("collection-1")
    queue.hold_item("job-other")
    manual_admission = queue.admission_for("job-other", now=NOW)

    assert manual_admission.collection_held is False
    assert manual_admission.item_held is True
    assert manual_admission.allowed is False
    next_job = queue.next_job(now=NOW)
    assert next_job is not None
    assert next_job.job_id == "job-collection"


def test_start_now_authorizes_overrides_schedule_and_preempts_when_running() -> None:
    queue = _queue()
    queue.enqueue("job-high", priority=100, authorized=True)
    queue.enqueue(
        "job-start-now",
        priority=-100,
        scheduled_for=NOW + timedelta(days=1),
    )

    started = queue.start_now("job-start-now")
    admission = queue.admission_for("job-start-now", now=NOW)

    assert started.authorized is True
    assert started.start_now_requested is True
    assert admission.due is True
    assert admission.allowed is True
    next_job = queue.next_job(now=NOW)
    assert next_job is not None
    assert next_job.job_id == "job-start-now"


def test_start_now_does_not_authorize_or_preempt_when_globally_paused() -> None:
    queue = _queue(running=False)
    queue.enqueue("job-high", priority=100, authorized=True)
    queue.enqueue(
        "job-start-now",
        priority=-100,
        scheduled_for=NOW + timedelta(days=1),
    )

    unchanged = queue.start_now("job-start-now")

    assert unchanged.authorized is False
    assert unchanged.start_now_requested is False
    assert queue.next_job(now=NOW) is None

    queue.resume_all()
    next_job = queue.next_job(now=NOW)
    assert next_job is not None
    assert next_job.job_id == "job-high"


def test_remove_tombstones_a_job_and_it_never_dispatches() -> None:
    queue = _queue()
    queue.enqueue("job-removed", priority=10, authorized=True)
    queue.enqueue("job-fallback", priority=1, authorized=True)

    tombstone = queue.remove("job-removed")

    assert tombstone.removed is True
    assert queue.admission_for("job-removed", now=NOW) is None
    next_job = queue.next_job(now=NOW)
    assert next_job is not None
    assert next_job.job_id == "job-fallback"


def test_resume_all_opens_the_global_gate_without_clearing_manual_item_holds() -> None:
    queue = _queue(running=False)
    queue.enqueue("job-held", priority=10, authorized=True)
    queue.enqueue("job-free", priority=1, authorized=True)
    queue.hold_item("job-held")

    queue.resume_all()

    assert queue.queue_running is True
    assert queue.job("job-held").manual_hold is True
    held_admission = queue.admission_for("job-held", now=NOW)
    assert held_admission.queue_running is True
    assert held_admission.item_held is True
    assert held_admission.allowed is False
    next_job = queue.next_job(now=NOW)
    assert next_job is not None
    assert next_job.job_id == "job-free"


def test_pause_queue_closes_admission_before_returning() -> None:
    queue = _queue()
    queue.enqueue("job-ready", authorized=True)
    assert queue.next_job(now=NOW) is not None

    gate = queue.pause_queue()

    assert gate is _queue_module().QueueGate.PAUSED
    assert queue.queue_running is False
    admission = queue.admission_for("job-ready", now=NOW)
    assert admission.queue_running is False
    assert admission.allowed is False
    assert queue.next_job(now=NOW) is None


@pytest.mark.parametrize("job_id", ("", "job id", 7))
def test_enqueue_rejects_ambiguous_job_identifiers(job_id: object) -> None:
    queue = _queue()

    with pytest.raises((TypeError, ValueError)):
        queue.enqueue(job_id)


def test_queue_rejects_non_boolean_flags_bad_collection_ids_and_naive_schedule() -> None:
    queue_module = _queue_module()

    with pytest.raises(TypeError):
        queue_module.DownloadQueue(queue_running=1)

    queue = _queue()
    with pytest.raises(ValueError):
        queue.enqueue("job-collection", collection_id="collection id")
    with pytest.raises(TypeError):
        queue.enqueue("job-priority", priority=True)
    with pytest.raises(ValueError):
        queue.enqueue("job-naive", scheduled_for=datetime(2030, 1, 2, 3, 4))
    with pytest.raises(KeyError):
        queue.authorize("job-missing")
