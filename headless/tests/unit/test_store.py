"""Transactional persistence and idempotency tests for the queue store."""

from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Any

import pytest

from hermes_downloads.models import DownloadIntent
from hermes_downloads.store import RequestConflictError, SQLiteStore


def _intent(**overrides: Any) -> DownloadIntent:
    values: dict[str, Any] = {
        "job_id": "job-1",
        "request_id": "request-1",
        "payload_digest": "a" * 64,
        "source_url": b"https://example.test/files/one.bin?signature=unchanged",
        "generation": 4,
        "revision": 7,
    }
    values.update(overrides)
    return DownloadIntent(**values)


def _install_failing_insert_trigger(
    database_path: Path, *, table: str, trigger_name: str, message: str
) -> None:
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            f"""
            CREATE TRIGGER {trigger_name}
            BEFORE INSERT ON {table}
            BEGIN
                SELECT RAISE(FAIL, '{message}');
            END
            """
        )


def test_state_survives_reopen_with_job_command_event_and_queue_gate(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected = _intent()

    store = SQLiteStore(database_path)
    try:
        result = store.apply_add(expected)
        assert result.applied is True
        assert result.job == expected.job_id
        assert result.generation == expected.generation
        assert result.revision == expected.revision
        assert store.initialize_cold_start() == "paused"
    finally:
        store.close()

    reopened = SQLiteStore(database_path)
    try:
        job = reopened.get_job(expected.job_id)
        command = reopened.get_command(expected.request_id)
        events = reopened.list_events()

        assert job is not None
        assert job.source_url == expected.source_url
        assert (job.generation, job.revision) == (4, 7)
        assert command is not None
        assert command.payload_digest == expected.payload_digest
        assert (command.generation, command.revision) == (4, 7)
        assert [(event.kind, event.job, event.generation, event.revision) for event in events] == [
            ("job_added", "job-1", 4, 7)
        ]
        assert reopened.queue_gate() == "paused"
    finally:
        reopened.close()


def test_duplicate_exact_request_is_idempotent_without_extra_queue_mutation(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        first = store.apply_add(_intent())
        repeated = store.apply_add(_intent())

        assert first.applied is True
        assert repeated.applied is False
        assert (repeated.job, repeated.generation, repeated.revision) == (
            first.job,
            first.generation,
            first.revision,
        )
        assert [job.job for job in store.list_jobs()] == ["job-1"]
        assert store.get_command("request-1") is not None
        assert [event.kind for event in store.list_events()] == ["job_added"]
    finally:
        store.close()


def test_reused_request_with_different_payload_conflicts_without_mutation(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(_intent())
        before = (store.list_jobs(), store.get_command("request-1"), store.list_events())

        with pytest.raises(RequestConflictError):
            store.apply_add(_intent(payload_digest="b" * 64))

        assert (store.list_jobs(), store.get_command("request-1"), store.list_events()) == before
    finally:
        store.close()


def test_injected_command_write_failure_rolls_back_the_entire_transaction(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    initialized = SQLiteStore(database_path)
    initialized.close()
    _install_failing_insert_trigger(
        database_path,
        table="commands",
        trigger_name="fail_command_insert",
        message="injected command write failure",
    )

    store = SQLiteStore(database_path)
    try:
        with pytest.raises(sqlite3.DatabaseError, match="injected command write failure"):
            store.apply_add(_intent())

        assert store.list_jobs() == ()
        assert store.get_command("request-1") is None
        assert store.list_events() == ()
    finally:
        store.close()


def test_sqlite_full_during_event_write_rolls_back_job_command_and_event(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        connection = store._connection
        connection.execute("CREATE TABLE space_probe (payload BLOB NOT NULL)")
        connection.execute(
            """
            CREATE TRIGGER fill_database_before_event
            BEFORE INSERT ON events
            BEGIN
                INSERT INTO space_probe (payload) VALUES (randomblob(65536));
            END
            """
        )
        page_count = connection.execute("PRAGMA page_count").fetchone()[0]
        max_page_count = connection.execute(
            f"PRAGMA max_page_count = {page_count}"
        ).fetchone()[0]
        assert max_page_count == page_count

        # The trigger allocates pages while SQLite processes the real events INSERT;
        # it does not inject a synthetic RAISE failure.
        with pytest.raises(sqlite3.OperationalError) as failure:
            store.apply_add(_intent())

        assert failure.value.sqlite_errorcode == sqlite3.SQLITE_FULL
        assert failure.value.sqlite_errorname == "SQLITE_FULL"
        assert store.list_jobs() == ()
        assert store.get_job("job-1") is None
        assert store.get_command("request-1") is None
        assert store.list_events() == ()
    finally:
        store.close()


def test_cold_start_persists_global_queue_gate_paused_without_worker(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        assert store.queue_gate() is None
        assert store.initialize_cold_start() == "paused"
        assert store.queue_gate() == "paused"
    finally:
        store.close()

    reopened = SQLiteStore(database_path)
    try:
        assert reopened.queue_gate() == "paused"
    finally:
        reopened.close()
