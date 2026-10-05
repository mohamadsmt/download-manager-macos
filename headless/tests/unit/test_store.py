"""Transactional persistence and idempotency tests for the queue store."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
import hashlib
import os
import secrets
import sqlite3
import threading
from types import SimpleNamespace
from typing import Any

import pytest

import hermes_downloads.models as models_module
import hermes_downloads.retry as retry_module
import hermes_downloads.store as store_module
from hermes_downloads.models import DownloadIntent, MaterializedJob, SourceKind
from hermes_downloads.processes import ProcessBirthIdentity
from hermes_downloads.store import RequestConflictError, SQLiteStore


_V1_SCHEMA = """
CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    revision INTEGER NOT NULL
);

CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY,
    source_url BLOB NOT NULL,
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    state TEXT NOT NULL
);

CREATE TABLE commands (
    request_id TEXT PRIMARY KEY,
    payload_digest TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL
);

CREATE TABLE events (
    event_id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL
);
"""

_V8_JOB_CONTROL_COMMANDS_SCHEMA = """
CREATE TABLE job_control_commands (
    request_id TEXT PRIMARY KEY,
    payload_digest TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    action TEXT NOT NULL CHECK (action IN ('pause', 'resume', 'start_now')),
    status TEXT NOT NULL CHECK (status IN ('applied', 'blocked', 'stale')),
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    state TEXT NOT NULL,
    authorized INTEGER NOT NULL CHECK (authorized IN (0, 1))
);
"""

_V8_COMMAND_RECEIPTS_SCHEMA = """
CREATE TABLE command_receipts (
    request_id TEXT PRIMARY KEY,
    payload_digest TEXT NOT NULL CHECK (
        length(payload_digest) = 64 AND payload_digest NOT GLOB '*[^0-9a-f]*'
    ),
    scope TEXT NOT NULL CHECK (scope IN ('add', 'queue_gate', 'job_control')),
    action TEXT NOT NULL CHECK (
        (scope = 'add' AND action = 'add')
        OR (scope = 'queue_gate' AND action = 'queue_gate')
        OR (scope = 'job_control' AND action IN ('pause', 'resume', 'start_now'))
    )
);
"""


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


def _materialized_job(**overrides: Any) -> MaterializedJob:
    values: dict[str, Any] = {
        "job_id": "job-1",
        "intent": _intent(expected_revision=3),
        "source_kind": SourceKind.DIRECT,
        "queue_collection_id": "queue-1",
        "priority": -12,
        "order_key": 42,
        "scheduled_for": datetime(
            2031,
            7,
            2,
            9,
            30,
            15,
            123456,
            tzinfo=timezone(timedelta(hours=3)),
        ),
        "authorized": True,
        "manual_hold": False,
        "start_now_requested": True,
        "category": "Videos",
        "destination_collection": "Course material",
        "partial_filename": "selected.webm",
        "selected_final_filename": "selected--job-1.webm",
    }
    values.update(overrides)
    return MaterializedJob(**values)


def _retry_budget(**overrides: Any) -> retry_module.RetryBudget:
    values: dict[str, Any] = {
        "job_id": "job-1",
        "generation": 4,
        "budget_number": 1,
        "ordinary_attempts": 1,
        "paused": False,
        "exhausted": False,
    }
    values.update(overrides)
    if "audit" not in overrides:
        values["audit"] = (
            retry_module.RetryAuditEvent(
                retry_module.RetryAuditKind.OPENED,
                generation=values["generation"],
                budget_number=1,
                ordinary_attempts=0,
            ),
            retry_module.RetryAuditEvent(
                retry_module.RetryAuditKind.RETRY_SCHEDULED,
                generation=values["generation"],
                budget_number=1,
                ordinary_attempts=1,
            ),
        )
    return retry_module.RetryBudget(**values)


def _retry_budget_at_audit_capacity() -> retry_module.RetryBudget:
    authority = retry_module.RetryAuthority.open(
        policy=retry_module.RetryPolicy(), job_id="job-1", generation=4
    )
    while len(authority.budget.audit) < 256:
        budget = authority.budget
        if budget.exhausted:
            authority.resume_after_exhaustion(new_generation=budget.generation + 1)
        else:
            authority.decide(
                retry_module.RetryFailure(retry_module.FailureKind.TRANSIENT_HOST),
                generation=budget.generation,
            )
    assert authority.budget.exhausted is False
    return authority.budget


def _create_v1_database(database_path: Path) -> tuple[object, ...]:
    raw_source = b"https://example.test/%2Flegacy?signature=unchanged"
    with sqlite3.connect(database_path) as connection:
        connection.executescript(_V1_SCHEMA)
        connection.execute("PRAGMA user_version = 1")
        connection.execute(
            """
            INSERT INTO jobs (job_id, source_url, generation, revision, state)
            VALUES ('legacy-job', ?, 23, 41, 'downloading')
            """,
            (raw_source,),
        )
        connection.execute(
            """
            INSERT INTO commands (
                request_id, payload_digest, job_id, generation, revision
            )
            VALUES ('legacy-request', ?, 'legacy-job', 23, 41)
            """,
            ("b" * 64,),
        )
        connection.execute(
            """
            INSERT INTO events (kind, job_id, generation, revision)
            VALUES ('job_added', 'legacy-job', 23, 41)
            """
        )
        return connection.execute(
            """
            SELECT job_id, source_url, generation, revision, state
            FROM jobs
            WHERE job_id = 'legacy-job'
            """
        ).fetchone()


def _create_v2_database(database_path: Path) -> tuple[object, ...]:
    legacy_job = _create_v1_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._MATERIALIZED_JOBS_SCHEMA)
        connection.execute(store_module._COLLECTION_HOLDS_SCHEMA)
        connection.execute("PRAGMA user_version = 2")
    return legacy_job


def _create_v3_database(database_path: Path) -> tuple[object, ...]:
    legacy_job = _create_v2_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._JOB_RETRY_SCHEMA)
        connection.execute(store_module._JOB_RETRY_AUDIT_SCHEMA)
        connection.execute("PRAGMA user_version = 3")
    return legacy_job


def _create_v4_database(database_path: Path) -> tuple[object, ...]:
    legacy_job = _create_v3_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._QUEUE_COMMANDS_SCHEMA)
        connection.execute("PRAGMA user_version = 4")
    return legacy_job


def _create_v5_database(database_path: Path) -> tuple[object, ...]:
    legacy_job = _create_v4_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._ENGINE_INSTANCES_SCHEMA)
        connection.execute("PRAGMA user_version = 5")
    return legacy_job


def _create_v6_database(database_path: Path) -> tuple[object, ...]:
    legacy_job = _create_v5_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._DIRECT_ENGINE_ACTIVATION_FENCES_SCHEMA)
        connection.execute("PRAGMA user_version = 6")
    return legacy_job


def _create_v7_database(database_path: Path) -> tuple[object, ...]:
    legacy_job = _create_v6_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._V7_JOB_CONTROL_COMMANDS_SCHEMA)
        connection.execute("PRAGMA user_version = 7")
    return legacy_job


def _create_v8_database(database_path: Path) -> tuple[object, ...]:
    legacy_job = _create_v6_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(_V8_JOB_CONTROL_COMMANDS_SCHEMA)
        connection.execute(_V8_COMMAND_RECEIPTS_SCHEMA)
        connection.execute(
            """
            INSERT INTO job_control_commands (
                request_id,
                payload_digest,
                job_id,
                action,
                status,
                generation,
                revision,
                state,
                authorized
            )
            VALUES ('control-request', ?, 'legacy-job', 'pause', 'blocked', 23, 41, 'completed', 0)
            """,
            ("d" * 64,),
        )
        connection.executemany(
            """
            INSERT INTO command_receipts (request_id, payload_digest, scope, action)
            VALUES (?, ?, ?, ?)
            """,
            (
                ("legacy-request", "b" * 64, "add", "add"),
                ("control-request", "d" * 64, "job_control", "pause"),
            ),
        )
        connection.execute("PRAGMA user_version = 8")
    return legacy_job


def _create_v9_database(database_path: Path) -> tuple[object, ...]:
    legacy_job = _create_v8_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "ALTER TABLE job_control_commands RENAME TO job_control_commands_v8"
        )
        connection.execute(store_module._JOB_CONTROL_COMMANDS_SCHEMA)
        connection.execute(
            """
            INSERT INTO job_control_commands (
                request_id,
                payload_digest,
                job_id,
                action,
                status,
                generation,
                revision,
                state,
                authorized
            )
            SELECT
                request_id,
                payload_digest,
                job_id,
                action,
                status,
                generation,
                revision,
                state,
                authorized
            FROM job_control_commands_v8
            """
        )
        connection.execute("DROP TABLE job_control_commands_v8")
        connection.execute("ALTER TABLE command_receipts RENAME TO command_receipts_v8")
        connection.execute(store_module._COMMAND_RECEIPTS_SCHEMA)
        connection.execute(
            """
            INSERT INTO command_receipts (request_id, payload_digest, scope, action)
            SELECT request_id, payload_digest, scope, action
            FROM command_receipts_v8
            """
        )
        connection.execute("DROP TABLE command_receipts_v8")
        connection.execute("PRAGMA user_version = 9")
    return legacy_job


def _create_v10_database(database_path: Path) -> tuple[object, ...]:
    legacy_job = _create_v9_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._PUBLICATION_RESERVATIONS_SCHEMA)
        connection.execute("PRAGMA user_version = 10")
    return legacy_job


def _process_birth_identity(**overrides: Any) -> ProcessBirthIdentity:
    values: dict[str, int | str] = {
        "leader_pid": 4242,
        "process_group_id": 4242,
        "session_id": 4242,
        "owner_uid": 501,
        "started_unix_us": 1_700_000_000_000_001,
        "argv_sha256": "a" * 64,
    }
    values.update(overrides)
    return ProcessBirthIdentity.from_record(values)


def _table_names(database_path: Path) -> frozenset[str]:
    with sqlite3.connect(database_path) as connection:
        return frozenset(
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        )


def _queue_command_rows(store: SQLiteStore) -> tuple[tuple[object, ...], ...]:
    return tuple(
        tuple(row)
        for row in store._connection.execute(
            """
            SELECT request_id, payload_digest, gate, revision
            FROM queue_commands
            ORDER BY request_id
            """
        ).fetchall()
    )


def _job_control_command_rows(store: SQLiteStore) -> tuple[tuple[object, ...], ...]:
    return tuple(
        tuple(row)
        for row in store._connection.execute(
            """
            SELECT
                request_id,
                payload_digest,
                job_id,
                action,
                status,
                generation,
                revision,
                state,
                authorized
            FROM job_control_commands
            ORDER BY request_id
            """
        ).fetchall()
    )


def _command_receipt_rows(store: SQLiteStore) -> tuple[tuple[object, ...], ...]:
    return tuple(
        tuple(row)
        for row in store._connection.execute(
            """
            SELECT request_id, payload_digest, scope, action
            FROM command_receipts
            ORDER BY request_id
            """
        ).fetchall()
    )


def _add_page_of_jobs(store: SQLiteStore) -> None:
    for index in range(101):
        store.apply_add(
            _intent(
                job_id=f"job-{index:03d}",
                request_id=f"request-{index:03d}",
            )
        )


class _BootstrapFailureConnection:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self.closed = False

    @property
    def row_factory(self) -> Any:
        return self._connection.row_factory

    @row_factory.setter
    def row_factory(self, value: Any) -> None:
        self._connection.row_factory = value

    def execute(self, statement: str, *args: Any, **kwargs: Any) -> Any:
        if statement.lstrip().startswith("CREATE TABLE IF NOT EXISTS settings"):
            raise sqlite3.OperationalError("injected bootstrap failure")
        return self._connection.execute(statement, *args, **kwargs)

    def close(self) -> None:
        self.closed = True
        self._connection.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


class _MigrationFailureConnection:
    def __init__(
        self,
        connection: sqlite3.Connection,
        failure_statement_prefix: str = "CREATE TABLE collection_holds",
    ) -> None:
        self._connection = connection
        self.closed = False
        self._failure_statement_prefix = failure_statement_prefix

    @property
    def row_factory(self) -> Any:
        return self._connection.row_factory

    @row_factory.setter
    def row_factory(self, value: Any) -> None:
        self._connection.row_factory = value

    def execute(self, statement: str, *args: Any, **kwargs: Any) -> Any:
        if statement.lstrip().startswith(self._failure_statement_prefix):
            raise sqlite3.OperationalError("injected migration failure")
        return self._connection.execute(statement, *args, **kwargs)

    def close(self) -> None:
        self.closed = True
        self._connection.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


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


def test_queue_gate_control_is_idempotent_and_revision_fenced(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.initialize_cold_start() == "paused"

        opened = store.apply_queue_gate(
            gate="running",
            request_id="queue-open-request",
            payload_digest="c" * 64,
            expected_revision=1,
        )
        assert (opened.applied, opened.gate, opened.revision) == (True, "running", 2)
        assert store.queue_gate() == "running"

        repeated = store.apply_queue_gate(
            gate="running",
            request_id="queue-open-request",
            payload_digest="c" * 64,
            expected_revision=1,
        )
        assert (repeated.applied, repeated.gate, repeated.revision) == (
            False,
            "running",
            2,
        )

        before = (store.queue_gate(), store.list_events())
        with pytest.raises(store_module.RevisionConflictError):
            store.apply_queue_gate(
                gate="paused",
                request_id="queue-close-stale",
                payload_digest="d" * 64,
                expected_revision=1,
            )
        assert (store.queue_gate(), store.list_events()) == before
    finally:
        store.close()


def test_queue_gate_control_rolls_back_gate_revision_and_receipt_when_receipt_insert_fails(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        assert store.initialize_cold_start() == "paused"
        _install_failing_insert_trigger(
            database_path,
            table="queue_commands",
            trigger_name="fail_queue_command_insert",
            message="injected queue command receipt failure",
        )

        with pytest.raises(
            sqlite3.DatabaseError, match="injected queue command receipt failure"
        ):
            store.apply_queue_gate(
                gate="running",
                request_id="queue-open-request",
                payload_digest="c" * 64,
                expected_revision=1,
            )

        setting = store._connection.execute(
            "SELECT value, revision FROM settings WHERE key = 'queue_gate'"
        ).fetchone()
        assert setting is not None
        assert tuple(setting) == ("paused", 1)
        assert _queue_command_rows(store) == ()

        store._connection.execute("DROP TRIGGER fail_queue_command_insert")
        assert store.apply_queue_gate(
            gate="running",
            request_id="queue-open-request",
            payload_digest="c" * 64,
            expected_revision=1,
        ) == store_module.QueueGateResult(
            applied=True,
            gate="running",
            revision=2,
        )
    finally:
        store.close()


def test_queue_gate_control_replays_its_durable_receipt_after_reopen(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        assert store.initialize_cold_start() == "paused"
        assert store.apply_queue_gate(
            gate="running",
            request_id="queue-open-request",
            payload_digest="c" * 64,
            expected_revision=1,
        ) == store_module.QueueGateResult(
            applied=True,
            gate="running",
            revision=2,
        )
    finally:
        store.close()

    reopened = SQLiteStore(database_path)
    try:
        assert reopened.apply_queue_gate(
            gate="running",
            request_id="queue-open-request",
            payload_digest="c" * 64,
            expected_revision=1,
        ) == store_module.QueueGateResult(
            applied=False,
            gate="running",
            revision=2,
        )
        assert reopened.queue_gate() == "running"
        assert reopened.list_jobs() == ()
        assert reopened.list_events() == ()
    finally:
        reopened.close()


def test_queue_gate_control_reused_request_conflicts_on_digest_or_gate(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.initialize_cold_start() == "paused"
        store.apply_queue_gate(
            gate="running",
            request_id="queue-open-request",
            payload_digest="c" * 64,
            expected_revision=1,
        )
        before = (
            store.queue_gate(),
            _queue_command_rows(store),
            store.list_jobs(),
            store.list_events(),
        )

        with pytest.raises(RequestConflictError):
            store.apply_queue_gate(
                gate="running",
                request_id="queue-open-request",
                payload_digest="d" * 64,
                expected_revision=2,
            )
        with pytest.raises(RequestConflictError):
            store.apply_queue_gate(
                gate="paused",
                request_id="queue-open-request",
                payload_digest="c" * 64,
                expected_revision=2,
            )

        assert (
            store.queue_gate(),
            _queue_command_rows(store),
            store.list_jobs(),
            store.list_events(),
        ) == before
    finally:
        store.close()


def test_queue_gate_control_stale_revision_rolls_back_every_queue_mutation(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(_intent())
        assert store.initialize_cold_start() == "paused"
        before = (
            store.queue_gate(),
            _queue_command_rows(store),
            store.list_jobs(),
            store.list_events(),
        )

        with pytest.raises(store_module.RevisionConflictError):
            store.apply_queue_gate(
                gate="running",
                request_id="queue-open-stale-request",
                payload_digest="e" * 64,
                expected_revision=0,
            )

        assert (
            store.queue_gate(),
            _queue_command_rows(store),
            store.list_jobs(),
            store.list_events(),
        ) == before
    finally:
        store.close()


def test_queue_gate_control_fails_closed_before_cold_start_without_a_receipt(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        with pytest.raises(RuntimeError, match="not initialized"):
            store.apply_queue_gate(
                gate="running",
                request_id="queue-open-request",
                payload_digest="c" * 64,
                expected_revision=0,
            )

        assert store.queue_gate() is None
        assert _queue_command_rows(store) == ()
        assert store.list_jobs() == ()
        assert store.list_events() == ()
    finally:
        store.close()


@pytest.mark.parametrize(
    ("gate", "request_id", "payload_digest", "expected_revision"),
    (
        ("pausing", "queue-open-request", "c" * 64, 1),
        ("running", "queue/open-request", "c" * 64, 1),
        ("running", "queue-open-request", "C" * 64, 1),
        ("running", "queue-open-request", "c" * 64, -1),
    ),
)
def test_queue_gate_control_rejects_invalid_command_arguments_without_mutation(
    tmp_path: Path,
    gate: object,
    request_id: object,
    payload_digest: object,
    expected_revision: object,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.initialize_cold_start() == "paused"
        before = (store.queue_gate(), _queue_command_rows(store))

        with pytest.raises((TypeError, ValueError)):
            store.apply_queue_gate(
                gate=gate,
                request_id=request_id,
                payload_digest=payload_digest,
                expected_revision=expected_revision,
            )

        assert (store.queue_gate(), _queue_command_rows(store)) == before
    finally:
        store.close()


def test_list_jobs_uses_an_exact_fixed_page_size(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        _add_page_of_jobs(store)

        assert [job.job for job in store.list_jobs()] == [
            f"job-{index:03d}" for index in range(100)
        ]
    finally:
        store.close()


def test_list_events_uses_an_exact_fixed_page_size(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        _add_page_of_jobs(store)

        assert [event.event_id for event in store.list_events()] == list(range(1, 101))
    finally:
        store.close()


def test_list_jobs_resumes_after_the_previous_page_cursor(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        _add_page_of_jobs(store)

        first_page = store.list_jobs()
        next_page = store.list_jobs(cursor=first_page[-1].job)

        assert [job.job for job in next_page] == ["job-100"]
    finally:
        store.close()


def test_list_job_page_avoids_source_url_reads_and_resumes_after_cursor(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        _add_page_of_jobs(store)

        def deny_source_url_reads(
            action: int,
            first_argument: str | None,
            second_argument: str | None,
            _database_name: str | None,
            _trigger_name: str | None,
        ) -> int:
            if (
                action == sqlite3.SQLITE_READ
                and first_argument == "jobs"
                and second_argument == "source_url"
            ):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store._connection.set_authorizer(deny_source_url_reads)
        try:
            first_page = store.list_job_page()
            next_page = store.list_job_page(cursor=first_page[-1].job)
        finally:
            store._connection.set_authorizer(None)

        assert [
            (job.job, job.generation, job.revision, job.state) for job in first_page
        ] == [
            (f"job-{index:03d}", 4, 7, "queued") for index in range(100)
        ]
        assert [
            (job.job, job.generation, job.revision, job.state) for job in next_page
        ] == [("job-100", 4, 7, "queued")]
    finally:
        store.close()


def test_list_events_resumes_after_the_previous_page_cursor(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        _add_page_of_jobs(store)

        first_page = store.list_events()
        next_page = store.list_events(cursor=first_page[-1].event_id)

        assert [event.event_id for event in next_page] == [101]
    finally:
        store.close()


def test_failed_bootstrap_closes_the_connection_before_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    original_connect = sqlite3.connect
    failed_connection: _BootstrapFailureConnection | None = None
    connection_count = 0

    def connect_with_first_bootstrap_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal connection_count, failed_connection
        kwargs["timeout"] = 0
        connection = original_connect(*args, **kwargs)
        if connection_count == 0:
            connection_count += 1
            failed_connection = _BootstrapFailureConnection(connection)
            return failed_connection
        connection_count += 1
        return connection

    monkeypatch.setattr(store_module.sqlite3, "connect", connect_with_first_bootstrap_failure)

    with pytest.raises(sqlite3.OperationalError, match="injected bootstrap failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    retried = SQLiteStore(database_path)
    try:
        assert failed_connection.closed is True
        assert retried.queue_gate() is None
    finally:
        retried.close()


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
    store = SQLiteStore(database_path)
    try:
        _install_failing_insert_trigger(
            database_path,
            table="commands",
            trigger_name="fail_command_insert",
            message="injected command write failure",
        )

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


def test_cold_recovery_fences_incomplete_jobs_and_allocates_epochs(tmp_path: Path) -> None:
    recoverable_states = (
        "queued",
        "resolving",
        "downloading",
        "pausing",
        "paused",
        "retry_wait",
        "finalizing",
    )
    untouched_states = (
        "completed",
        "cancelled",
        "removed",
        "failed",
        "needs_link",
        "needs_auth",
        "blocked",
    )
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        recovered: list[tuple[str, int, int]] = []
        untouched: list[tuple[str, str, int, int]] = []
        for index, state in enumerate(recoverable_states):
            job_id = f"recover-{index}"
            generation = 10 + index
            revision = 20 + index
            store.apply_add(
                _intent(
                    job_id=job_id,
                    request_id=f"recover-request-{index}",
                    generation=generation,
                    revision=revision,
                )
            )
            store._connection.execute(
                "UPDATE jobs SET state = ? WHERE job_id = ?", (state, job_id)
            )
            recovered.append((job_id, generation, revision))
        for index, state in enumerate(untouched_states):
            job_id = f"untouched-{index}"
            generation = 30 + index
            revision = 40 + index
            store.apply_add(
                _intent(
                    job_id=job_id,
                    request_id=f"untouched-request-{index}",
                    generation=generation,
                    revision=revision,
                )
            )
            store._connection.execute(
                "UPDATE jobs SET state = ? WHERE job_id = ?", (state, job_id)
            )
            untouched.append((job_id, state, generation, revision))
        commands_before = tuple(
            store.get_command(f"recover-request-{index}")
            for index in range(len(recoverable_states))
        ) + tuple(
            store.get_command(f"untouched-request-{index}")
            for index in range(len(untouched_states))
        )
        event_count_before_recovery = len(store.list_events())

        assert store.recover_cold_start() == 1
        assert store.worker_epoch() == 1
        assert store.queue_gate() == "paused"
        for job_id, generation, revision in recovered:
            assert store.get_job(job_id) is not None
            assert store.get_job(job_id) == store_module.JobRecord(
                job=job_id,
                source_url=b"https://example.test/files/one.bin?signature=unchanged",
                generation=generation + 1,
                revision=revision + 1,
                state="paused",
            )
        for job_id, state, generation, revision in untouched:
            assert store.get_job(job_id) is not None
            assert store.get_job(job_id) == store_module.JobRecord(
                job=job_id,
                source_url=b"https://example.test/files/one.bin?signature=unchanged",
                generation=generation,
                revision=revision,
                state=state,
            )
        expected_first_events = [
            ("job_paused", job_id, generation + 1, revision + 1)
            for job_id, generation, revision in recovered
        ]
        assert sorted(
            (event.kind, event.job, event.generation, event.revision)
            for event in store.list_events()[event_count_before_recovery:]
        ) == sorted(expected_first_events)
        assert tuple(
            store.get_command(f"recover-request-{index}")
            for index in range(len(recoverable_states))
        ) + tuple(
            store.get_command(f"untouched-request-{index}")
            for index in range(len(untouched_states))
        ) == commands_before

        assert store.recover_cold_start() == 2
        assert store.worker_epoch() == 2
        for job_id, generation, revision in recovered:
            assert store.get_job(job_id) == store_module.JobRecord(
                job=job_id,
                source_url=b"https://example.test/files/one.bin?signature=unchanged",
                generation=generation + 2,
                revision=revision + 2,
                state="paused",
            )
        recovery_events = [
            (event.kind, event.job, event.generation, event.revision)
            for event in store.list_events()[event_count_before_recovery:]
        ]
        assert sorted(recovery_events) == sorted(
            expected_first_events
            + [
                ("job_paused", job_id, generation + 2, revision + 2)
                for job_id, generation, revision in recovered
            ]
        )
    finally:
        store.close()


def test_cold_recovery_advances_epoch_without_events_when_no_job_needs_conversion(
    tmp_path: Path,
) -> None:
    untouched_states = (
        "completed",
        "cancelled",
        "removed",
        "failed",
        "needs_link",
        "needs_auth",
        "blocked",
    )
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        for index, state in enumerate(untouched_states):
            job_id = f"untouched-{index}"
            store.apply_add(
                _intent(
                    job_id=job_id,
                    request_id=f"untouched-request-{index}",
                    generation=50 + index,
                    revision=60 + index,
                )
            )
            store._connection.execute(
                "UPDATE jobs SET state = ? WHERE job_id = ?", (state, job_id)
            )
        jobs_before = store.list_jobs()
        commands_before = tuple(
            store.get_command(f"untouched-request-{index}")
            for index in range(len(untouched_states))
        )
        events_before = store.list_events()

        assert store.recover_cold_start() == 1
        assert store.worker_epoch() == 1
        assert store.queue_gate() == "paused"
        assert store.list_jobs() == jobs_before
        assert (
            tuple(
                store.get_command(f"untouched-request-{index}")
                for index in range(len(untouched_states))
            )
            == commands_before
        )
        assert store.list_events() == events_before

        assert store.recover_cold_start() == 2
        assert store.worker_epoch() == 2
        assert store.list_jobs() == jobs_before
        assert store.list_events() == events_before
    finally:
        store.close()


def test_cold_recovery_event_failure_rolls_back_epoch_gate_jobs_and_events(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        store.apply_add(_intent())
        store._connection.execute(
            "UPDATE jobs SET state = 'downloading' WHERE job_id = 'job-1'"
        )
        store._connection.execute(
            """
            INSERT INTO settings (key, value, revision)
            VALUES ('queue_gate', 'running', 9)
            """
        )
        job_before = store.get_job("job-1")
        command_before = store.get_command("request-1")
        events_before = store.list_events()
        _install_failing_insert_trigger(
            database_path,
            table="events",
            trigger_name="fail_recovery_event_insert",
            message="injected recovery event write failure",
        )

        with pytest.raises(sqlite3.DatabaseError, match="injected recovery event write failure"):
            store.recover_cold_start()

        assert store.worker_epoch() is None
        assert store.queue_gate() == "running"
        assert store.get_job("job-1") == job_before
        assert store.get_command("request-1") == command_before
        assert store.list_events() == events_before
    finally:
        store.close()


def test_cold_recovery_epoch_survives_reopen(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    first = SQLiteStore(database_path)
    try:
        assert first.recover_cold_start() == 1
    finally:
        first.close()

    reopened = SQLiteStore(database_path)
    try:
        assert reopened.worker_epoch() == 1
        assert reopened.recover_cold_start() == 2
    finally:
        reopened.close()

    final = SQLiteStore(database_path)
    try:
        assert final.worker_epoch() == 2
    finally:
        final.close()


def test_v8_migrates_v1_database_without_changing_legacy_job_data(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected_legacy_job = _create_v1_database(database_path)
    legacy_source_url = expected_legacy_job[1]
    assert isinstance(legacy_source_url, bytes)

    store = SQLiteStore(database_path)
    try:
        assert store.get_job("legacy-job") == store_module.JobRecord(
            job="legacy-job",
            source_url=legacy_source_url,
            generation=23,
            revision=41,
            state="downloading",
        )
        assert store.get_materialized_job("legacy-job") is None
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert connection.execute(
            """
            SELECT job_id, source_url, generation, revision, state
            FROM jobs
            WHERE job_id = 'legacy-job'
            """
        ).fetchone() == expected_legacy_job
    v1_tables = {"settings", "jobs", "commands", "events"}
    v8_tables = _table_names(database_path)
    assert v1_tables <= v8_tables
    assert {
        "collection_holds",
        "job_retry",
        "job_retry_audit",
        "queue_commands",
        "engine_instances",
        "direct_engine_activation_fences",
        "job_control_commands",
        "command_receipts",
    } <= v8_tables
    assert len(v8_tables - v1_tables) >= 9


def test_v2_migration_failure_leaves_v1_database_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected_legacy_job = _create_v1_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_migration_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(original_connect(*args, **kwargs))
        return failed_connection

    monkeypatch.setattr(store_module.sqlite3, "connect", connect_with_migration_failure)

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert connection.execute(
            """
            SELECT job_id, source_url, generation, revision, state
            FROM jobs
            WHERE job_id = 'legacy-job'
            """
        ).fetchone() == expected_legacy_job
        assert {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        } == {"settings", "jobs", "commands", "events"}


def test_v3_migration_failure_leaves_v2_database_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected_legacy_job = _create_v2_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_migration_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="CREATE TABLE job_retry_audit",
        )
        return failed_connection

    monkeypatch.setattr(store_module.sqlite3, "connect", connect_with_migration_failure)

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 2
        assert connection.execute(
            """
            SELECT job_id, source_url, generation, revision, state
            FROM jobs
            WHERE job_id = 'legacy-job'
            """
        ).fetchone() == expected_legacy_job
    monkeypatch.setattr(store_module.sqlite3, "connect", original_connect)
    assert _table_names(database_path) == {
        "settings",
        "jobs",
        "commands",
        "events",
        "materialized_jobs",
        "collection_holds",
    }


def test_v8_migrates_v3_database_without_changing_legacy_job_data(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected_legacy_job = _create_v3_database(database_path)
    legacy_source_url = expected_legacy_job[1]
    assert isinstance(legacy_source_url, bytes)

    store = SQLiteStore(database_path)
    try:
        assert store.get_job("legacy-job") == store_module.JobRecord(
            job="legacy-job",
            source_url=legacy_source_url,
            generation=23,
            revision=41,
            state="downloading",
        )
        assert store.queue_gate() is None
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        schema = connection.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'queue_commands'
            """
        ).fetchone()
        assert schema is not None
        assert store_module._normalize_table_schema(schema[0]) == (
            store_module._V4_TABLE_SCHEMAS["queue_commands"]
        )
        engine_schema = connection.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'engine_instances'
            """
        ).fetchone()
        assert engine_schema is not None
        assert store_module._normalize_table_schema(engine_schema[0]) == (
            store_module._V5_TABLE_SCHEMAS["engine_instances"]
        )
    assert "queue_commands" in _table_names(database_path)
    assert "engine_instances" in _table_names(database_path)
    assert "direct_engine_activation_fences" in _table_names(database_path)


def test_v4_migration_failure_leaves_v3_database_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected_legacy_job = _create_v3_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_migration_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="CREATE TABLE queue_commands",
        )
        return failed_connection

    monkeypatch.setattr(store_module.sqlite3, "connect", connect_with_migration_failure)

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        assert connection.execute(
            """
            SELECT job_id, source_url, generation, revision, state
            FROM jobs
            WHERE job_id = 'legacy-job'
            """
        ).fetchone() == expected_legacy_job
    monkeypatch.setattr(store_module.sqlite3, "connect", original_connect)
    assert "queue_commands" not in _table_names(database_path)


def test_v4_migration_rolls_back_queue_command_ddl_when_version_bump_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v3_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_version_bump_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="PRAGMA user_version = 4",
        )
        return failed_connection

    monkeypatch.setattr(store_module.sqlite3, "connect", connect_with_version_bump_failure)

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V3_TABLE_SCHEMAS
    assert "queue_commands" not in table_schemas


def test_v5_migration_rolls_back_engine_instance_ddl_when_version_bump_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v4_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_version_bump_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="PRAGMA user_version = 5",
        )
        return failed_connection

    monkeypatch.setattr(store_module.sqlite3, "connect", connect_with_version_bump_failure)

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 4
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V4_TABLE_SCHEMAS
    assert "engine_instances" not in table_schemas


def test_v3_rejects_incomplete_retry_schema_without_bootstrap_writes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v2_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE job_retry (job_id TEXT PRIMARY KEY)")
        connection.execute(store_module._JOB_RETRY_AUDIT_SCHEMA)
        connection.execute("PRAGMA user_version = 3")

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        assert connection.execute("PRAGMA table_info(job_retry)").fetchall() == [
            (0, "job_id", "TEXT", 0, None, 1)
        ]
    assert "queue_commands" not in _table_names(database_path)


def test_v3_rejects_retry_schema_missing_required_constraints(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v2_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE job_retry (
                job_id TEXT PRIMARY KEY REFERENCES jobs(job_id),
                generation INTEGER NOT NULL,
                budget_number INTEGER NOT NULL,
                ordinary_attempts INTEGER NOT NULL,
                paused INTEGER NOT NULL,
                exhausted INTEGER NOT NULL
            )
            """
        )
        connection.execute(store_module._JOB_RETRY_AUDIT_SCHEMA)
        connection.execute("PRAGMA user_version = 3")

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)


def test_v0_rejects_nonempty_unknown_schema_before_bootstrap_writes(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE unknown_bootstrap_table (value TEXT NOT NULL)")
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 0

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 0
    assert _table_names(database_path) == {"unknown_bootstrap_table"}


def test_v0_rejects_sqlite_prefix_lookalike_before_bootstrap_writes(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    lookalike = "sqliteX_evil"
    with sqlite3.connect(database_path) as connection:
        connection.execute(f"CREATE TABLE {lookalike} (value TEXT NOT NULL)")
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 0

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 0
    assert _table_names(database_path) == {lookalike}


def test_v15_rejects_newer_schema_without_creating_legacy_tables(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE future_jobs (job_id TEXT PRIMARY KEY)")
        connection.execute("PRAGMA user_version = 17")

    with pytest.raises(RuntimeError, match="newer than supported"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 17
    assert _table_names(database_path) == {"future_jobs"}


def test_v4_rejects_malformed_queue_command_schema_without_bootstrap_writes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v3_database(database_path)
    v3_tables = _table_names(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE queue_commands (request_id TEXT PRIMARY KEY)")
        connection.execute("PRAGMA user_version = 4")

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 4
        assert connection.execute("PRAGMA table_info(queue_commands)").fetchall() == [
            (0, "request_id", "TEXT", 0, None, 1)
        ]
    assert _table_names(database_path) == v3_tables | {"queue_commands"}


def test_v4_rejects_unknown_trigger_before_v5_bootstrap_writes(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v4_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TRIGGER unexpected_v4_trigger
            BEFORE INSERT ON settings
            BEGIN
                SELECT RAISE(FAIL, 'unexpected trigger');
            END
            """
        )

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 4
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall() == [("unexpected_v4_trigger",)]
    assert "engine_instances" not in _table_names(database_path)


def test_v5_rejects_malformed_engine_instance_schema_without_bootstrap_writes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v4_database(database_path)
    v4_tables = _table_names(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE engine_instances (engine_kind TEXT PRIMARY KEY)")
        connection.execute("PRAGMA user_version = 5")

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 5
        assert connection.execute("PRAGMA table_info(engine_instances)").fetchall() == [
            (0, "engine_kind", "TEXT", 0, None, 1)
        ]
    assert _table_names(database_path) == v4_tables | {"engine_instances"}


def test_v5_rejects_unknown_current_table_without_bootstrap_writes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v4_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._ENGINE_INSTANCES_SCHEMA)
        connection.execute("CREATE TABLE unexpected_current_table (value TEXT NOT NULL)")
        connection.execute("PRAGMA user_version = 5")

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 5
    assert _table_names(database_path) == {
        *store_module._V5_TABLE_SCHEMAS,
        "unexpected_current_table",
    }


def test_v5_rejects_unknown_current_trigger_without_bootstrap_writes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v4_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._ENGINE_INSTANCES_SCHEMA)
        connection.execute(
            """
            CREATE TRIGGER unexpected_current_trigger
            BEFORE INSERT ON engine_instances
            BEGIN
                SELECT RAISE(FAIL, 'unexpected trigger');
            END
            """
        )
        connection.execute("PRAGMA user_version = 5")

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 5
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall() == [("unexpected_current_trigger",)]


def test_direct_engine_record_rejects_malformed_birth_identity() -> None:
    malformed_identity = object.__new__(ProcessBirthIdentity)

    with pytest.raises(ValueError, match="valid process-birth identity"):
        store_module.DirectEngineRecord(worker_epoch=1, identity=malformed_identity)


def test_direct_engine_record_crud_is_exact_and_durable(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    initial_identity = _process_birth_identity()
    initial = store_module.DirectEngineRecord(worker_epoch=1, identity=initial_identity)
    updated = replace(
        initial,
        identity=_process_birth_identity(
            started_unix_us=initial_identity.started_unix_us + 1,
            argv_sha256="b" * 64,
        ),
    )

    store = SQLiteStore(database_path)
    try:
        assert store.recover_cold_start() == 1
        assert store.get_direct_engine_record() is None

        store.set_direct_engine_record(initial)
        assert store.get_direct_engine_record() == initial

        store.set_direct_engine_record(updated)
        assert store.get_direct_engine_record() == updated
        assert store.clear_direct_engine_record(initial) is False
        assert store.get_direct_engine_record() == updated
        assert store.clear_direct_engine_record(updated) is True
        assert store.get_direct_engine_record() is None

        store.set_direct_engine_record(updated)
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert [
            row[1]
            for row in connection.execute("PRAGMA table_info(engine_instances)").fetchall()
        ] == [
            "engine_kind",
            "worker_epoch",
            "leader_pid",
            "process_group_id",
            "session_id",
            "owner_uid",
            "started_unix_us",
            "argv_sha256",
        ]
        assert connection.execute(
            """
            SELECT
                engine_kind,
                worker_epoch,
                leader_pid,
                process_group_id,
                session_id,
                owner_uid,
                started_unix_us,
                argv_sha256
            FROM engine_instances
            """
        ).fetchall() == [
            (
                "direct",
                updated.worker_epoch,
                updated.identity.leader_pid,
                updated.identity.process_group_id,
                updated.identity.session_id,
                updated.identity.owner_uid,
                updated.identity.started_unix_us,
                updated.identity.argv_sha256,
            )
        ]

    reopened = SQLiteStore(database_path)
    try:
        assert reopened.get_direct_engine_record() == updated
        with pytest.raises(ValueError, match="worker epoch"):
            reopened.set_direct_engine_record(
                store_module.DirectEngineRecord(
                    worker_epoch=updated.worker_epoch + 1,
                    identity=updated.identity,
                )
            )
        assert reopened.get_direct_engine_record() == updated
    finally:
        reopened.close()


@pytest.mark.parametrize(
    "legacy_builder",
    (
        _create_v1_database,
        _create_v2_database,
        _create_v3_database,
        _create_v4_database,
        _create_v5_database,
    ),
    ids=("v1", "v2", "v3", "v4", "v5"),
)
def test_v8_migrates_every_supported_legacy_schema_to_the_exact_catalog(
    tmp_path: Path, legacy_builder: Any
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected_legacy_job = legacy_builder(database_path)

    store = SQLiteStore(database_path)
    try:
        assert store.get_job("legacy-job") == store_module.JobRecord(
            job="legacy-job",
            source_url=expected_legacy_job[1],
            generation=23,
            revision=41,
            state="downloading",
        )
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V16_TABLE_SCHEMAS


def test_v8_migration_preserves_a_v5_direct_engine_record(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v5_database(database_path)
    record = store_module.DirectEngineRecord(
        worker_epoch=1, identity=_process_birth_identity()
    )
    with sqlite3.connect(database_path) as connection:
        identity = record.identity
        connection.execute(
            """
            INSERT INTO engine_instances (
                engine_kind,
                worker_epoch,
                leader_pid,
                process_group_id,
                session_id,
                owner_uid,
                started_unix_us,
                argv_sha256
            )
            VALUES ('direct', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.worker_epoch,
                identity.leader_pid,
                identity.process_group_id,
                identity.session_id,
                identity.owner_uid,
                identity.started_unix_us,
                identity.argv_sha256,
            ),
        )

    store = SQLiteStore(database_path)
    try:
        assert store.get_direct_engine_record() == record
        assert store.get_direct_engine_activation_fence() is None
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert [
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(direct_engine_activation_fences)"
            ).fetchall()
        ] == ["engine_kind", "worker_epoch", "reservation_token"]


def test_v6_migration_ddl_failure_rolls_back_to_the_v5_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v5_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_migration_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="CREATE TABLE direct_engine_activation_fences",
        )
        return failed_connection

    monkeypatch.setattr(store_module.sqlite3, "connect", connect_with_migration_failure)

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 5
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V5_TABLE_SCHEMAS


def test_v6_migration_version_bump_failure_rolls_back_activation_fence_ddl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v5_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_version_bump_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="PRAGMA user_version = 6",
        )
        return failed_connection

    monkeypatch.setattr(store_module.sqlite3, "connect", connect_with_version_bump_failure)

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 5
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V5_TABLE_SCHEMAS


def test_v6_rejects_activation_fence_schema_missing_reservation_token_before_bootstrap_writes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v5_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE direct_engine_activation_fences (
                engine_kind TEXT PRIMARY KEY NOT NULL CHECK (engine_kind = 'direct'),
                worker_epoch INTEGER NOT NULL CHECK (worker_epoch > 0)
            )
            """
        )
        connection.execute("PRAGMA user_version = 6")

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 6
        assert [
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(direct_engine_activation_fences)"
            ).fetchall()
        ] == ["engine_kind", "worker_epoch"]
    assert _table_names(database_path) == {
        *store_module._V5_TABLE_SCHEMAS,
        "direct_engine_activation_fences",
    }


def test_v6_rejects_unknown_current_table_before_bootstrap_writes(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v5_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE direct_engine_activation_fences (
                engine_kind TEXT PRIMARY KEY NOT NULL CHECK (engine_kind = 'direct'),
                worker_epoch INTEGER NOT NULL CHECK (worker_epoch > 0),
                reservation_token TEXT NOT NULL CHECK (
                    length(reservation_token) = 64
                    AND reservation_token NOT GLOB '*[^0-9a-f]*'
                )
            )
            """
        )
        connection.execute("CREATE TABLE unexpected_v6_table (value TEXT NOT NULL)")
        connection.execute("PRAGMA user_version = 6")

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 6
    assert _table_names(database_path) == {
        *store_module._V5_TABLE_SCHEMAS,
        "direct_engine_activation_fences",
        "unexpected_v6_table",
    }


def test_v6_rejects_unknown_current_trigger_before_bootstrap_writes(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v5_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE direct_engine_activation_fences (
                engine_kind TEXT PRIMARY KEY NOT NULL CHECK (engine_kind = 'direct'),
                worker_epoch INTEGER NOT NULL CHECK (worker_epoch > 0),
                reservation_token TEXT NOT NULL CHECK (
                    length(reservation_token) = 64
                    AND reservation_token NOT GLOB '*[^0-9a-f]*'
                )
            )
            """
        )
        connection.execute(
            """
            CREATE TRIGGER unexpected_v6_trigger
            BEFORE INSERT ON direct_engine_activation_fences
            BEGIN
                SELECT RAISE(FAIL, 'unexpected trigger');
            END
            """
        )
        connection.execute("PRAGMA user_version = 6")

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 6
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall() == [("unexpected_v6_trigger",)]


def test_direct_engine_activation_fence_requires_a_positive_worker_epoch() -> None:
    with pytest.raises((TypeError, ValueError)):
        store_module.DirectEngineActivationFence(
            worker_epoch=0, reservation_token="a" * 64
        )


def test_direct_engine_activation_fence_carries_a_fixed_lowercase_hex_token() -> None:
    fence = store_module.DirectEngineActivationFence(
        worker_epoch=1, reservation_token="a" * 64
    )

    assert fence.reservation_token == "a" * 64


@pytest.mark.parametrize("reservation_token", ("a" * 63, "A" * 64, b"a" * 64))
def test_direct_engine_activation_fence_rejects_an_invalid_reservation_token(
    reservation_token: Any,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        store_module.DirectEngineActivationFence(
            worker_epoch=1, reservation_token=reservation_token
        )


def test_direct_engine_activation_reservation_is_durable_and_conflicts_nonthrowingly(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        assert store.recover_cold_start() == 1
        reserved = store.reserve_direct_engine_activation(worker_epoch=1)
        assert reserved is not None
        assert reserved.worker_epoch == 1
        assert type(reserved.reservation_token) is str
        assert len(reserved.reservation_token) == 64
        assert set(reserved.reservation_token) <= set("0123456789abcdef")
        assert store.get_direct_engine_activation_fence() == reserved
        assert store.reserve_direct_engine_activation(worker_epoch=1) is None
        assert store.get_direct_engine_record() is None
    finally:
        store.close()

    reopened = SQLiteStore(database_path)
    try:
        assert reopened.get_direct_engine_activation_fence() == reserved
        assert reopened.get_direct_engine_record() is None
    finally:
        reopened.close()


def test_direct_engine_activation_reservation_requires_current_epoch_and_no_record(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.recover_cold_start() == 1
        with pytest.raises(ValueError, match="current"):
            store.reserve_direct_engine_activation(worker_epoch=2)
        assert store.get_direct_engine_activation_fence() is None

        record = store_module.DirectEngineRecord(
            worker_epoch=1, identity=_process_birth_identity()
        )
        store.set_direct_engine_record(record)
        assert store.reserve_direct_engine_activation(worker_epoch=1) is None
        assert store.get_direct_engine_activation_fence() is None
        assert store.get_direct_engine_record() == record
    finally:
        store.close()


def test_direct_engine_activation_fence_compare_and_clear_preserves_a_stale_fence(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.recover_cold_start() == 1
        fence = store.reserve_direct_engine_activation(worker_epoch=1)
        assert fence is not None
        assert store.clear_direct_engine_activation_fence(
            store_module.DirectEngineActivationFence(
                worker_epoch=2, reservation_token="a" * 64
            )
        ) is False
        assert store.get_direct_engine_activation_fence() == fence

        assert store.recover_cold_start() == 2
        assert store.reserve_direct_engine_activation(worker_epoch=2) is None
        assert store.clear_direct_engine_activation_fence(fence) is True
        assert store.get_direct_engine_activation_fence() is None
        replacement = store.reserve_direct_engine_activation(worker_epoch=2)
        assert replacement is not None
        assert replacement.worker_epoch == 2
    finally:
        store.close()


def test_direct_engine_activation_bind_converts_the_exact_current_fence_atomically(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    record = store_module.DirectEngineRecord(
        worker_epoch=1, identity=_process_birth_identity()
    )
    try:
        assert store.recover_cold_start() == 1
        fence = store.reserve_direct_engine_activation(worker_epoch=1)
        assert fence is not None

        store.bind_direct_engine_activation_fence(fence, record)

        assert store.get_direct_engine_activation_fence() is None
        assert store.get_direct_engine_record() == record
    finally:
        store.close()

    reopened = SQLiteStore(database_path)
    try:
        assert reopened.get_direct_engine_activation_fence() is None
        assert reopened.get_direct_engine_record() == record
    finally:
        reopened.close()


def test_direct_engine_activation_fence_rejects_a_stale_same_epoch_clear_after_rereservation(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.recover_cold_start() == 1
        delayed_fence = store.reserve_direct_engine_activation(worker_epoch=1)
        assert delayed_fence is not None
        assert store.clear_direct_engine_activation_fence(delayed_fence) is True
        current_fence = store.reserve_direct_engine_activation(worker_epoch=1)
        assert current_fence is not None

        assert store.clear_direct_engine_activation_fence(delayed_fence) is False
        assert store.get_direct_engine_activation_fence() == current_fence
        assert store.get_direct_engine_record() is None
    finally:
        store.close()


def test_direct_engine_activation_fence_rejects_a_stale_same_epoch_bind_after_rereservation(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    record = store_module.DirectEngineRecord(
        worker_epoch=1, identity=_process_birth_identity()
    )
    try:
        assert store.recover_cold_start() == 1
        delayed_fence = store.reserve_direct_engine_activation(worker_epoch=1)
        assert delayed_fence is not None
        assert store.clear_direct_engine_activation_fence(delayed_fence) is True
        current_fence = store.reserve_direct_engine_activation(worker_epoch=1)
        assert current_fence is not None

        with pytest.raises(ValueError, match="fence"):
            store.bind_direct_engine_activation_fence(delayed_fence, record)
        assert store.get_direct_engine_activation_fence() == current_fence
        assert store.get_direct_engine_record() is None
    finally:
        store.close()


def test_direct_engine_record_write_rejects_a_live_activation_fence_without_mutation(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    record = store_module.DirectEngineRecord(
        worker_epoch=1, identity=_process_birth_identity()
    )
    try:
        assert store.recover_cold_start() == 1
        fence = store.reserve_direct_engine_activation(worker_epoch=1)
        assert fence is not None

        with pytest.raises(ValueError, match="fence"):
            store.set_direct_engine_record(record)
        assert store.get_direct_engine_activation_fence() == fence
        assert store.get_direct_engine_record() is None
    finally:
        store.close()


def test_direct_engine_activation_bind_rejects_a_current_record_without_mutating_either(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    initial_identity = _process_birth_identity()
    initial = store_module.DirectEngineRecord(
        worker_epoch=1, identity=initial_identity
    )
    replacement = replace(
        initial,
        identity=_process_birth_identity(
            started_unix_us=initial_identity.started_unix_us + 1,
            argv_sha256="b" * 64,
        ),
    )
    fence = store_module.DirectEngineActivationFence(
        worker_epoch=1, reservation_token="c" * 64
    )
    try:
        assert store.recover_cold_start() == 1
        store.set_direct_engine_record(initial)
        store._connection.execute(
            """
            INSERT INTO direct_engine_activation_fences (
                engine_kind, worker_epoch, reservation_token
            )
            VALUES ('direct', ?, ?)
            """,
            (fence.worker_epoch, fence.reservation_token),
        )

        with pytest.raises(ValueError, match="record"):
            store.bind_direct_engine_activation_fence(fence, replacement)
        assert store.get_direct_engine_activation_fence() == fence
        assert store.get_direct_engine_record() == initial
    finally:
        store.close()


def test_direct_engine_activation_bind_requires_the_exact_current_fence_and_epoch(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.recover_cold_start() == 1
        fence = store.reserve_direct_engine_activation(worker_epoch=1)
        assert fence is not None
        record = store_module.DirectEngineRecord(
            worker_epoch=1, identity=_process_birth_identity()
        )

        with pytest.raises(ValueError, match="fence"):
            store.bind_direct_engine_activation_fence(
                store_module.DirectEngineActivationFence(
                    worker_epoch=2, reservation_token="b" * 64
                ),
                record,
            )
        assert store.get_direct_engine_activation_fence() == fence
        assert store.get_direct_engine_record() is None

        assert store.recover_cold_start() == 2
        with pytest.raises(ValueError, match="current"):
            store.bind_direct_engine_activation_fence(fence, record)
        assert store.get_direct_engine_activation_fence() == fence
        assert store.get_direct_engine_record() is None
    finally:
        store.close()


def test_direct_engine_activation_reservation_rolls_back_when_fence_insert_fails(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        assert store.recover_cold_start() == 1
        _install_failing_insert_trigger(
            database_path,
            table="direct_engine_activation_fences",
            trigger_name="fail_direct_activation_fence_insert",
            message="injected direct activation fence insert failure",
        )

        with pytest.raises(
            sqlite3.DatabaseError, match="injected direct activation fence insert failure"
        ):
            store.reserve_direct_engine_activation(worker_epoch=1)

        assert store.get_direct_engine_activation_fence() is None
        assert store.get_direct_engine_record() is None
        store._connection.execute("DROP TRIGGER fail_direct_activation_fence_insert")
        reserved = store.reserve_direct_engine_activation(worker_epoch=1)
        assert reserved is not None
        assert reserved.worker_epoch == 1
    finally:
        store.close()


def test_direct_engine_activation_bind_rolls_back_record_insert_when_fence_delete_fails(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    record = store_module.DirectEngineRecord(
        worker_epoch=1, identity=_process_birth_identity()
    )
    try:
        assert store.recover_cold_start() == 1
        fence = store.reserve_direct_engine_activation(worker_epoch=1)
        assert fence is not None
        with sqlite3.connect(database_path) as connection:
            connection.execute(
                """
                CREATE TRIGGER fail_direct_activation_fence_delete
                BEFORE DELETE ON direct_engine_activation_fences
                BEGIN
                    SELECT RAISE(FAIL, 'injected direct activation fence delete failure');
                END
                """
            )

        with pytest.raises(
            sqlite3.DatabaseError, match="injected direct activation fence delete failure"
        ):
            store.bind_direct_engine_activation_fence(fence, record)

        assert store.get_direct_engine_activation_fence() == fence
        assert store.get_direct_engine_record() is None
        store._connection.execute("DROP TRIGGER fail_direct_activation_fence_delete")
        store.bind_direct_engine_activation_fence(fence, record)
        assert store.get_direct_engine_activation_fence() is None
        assert store.get_direct_engine_record() == record
    finally:
        store.close()


def test_materialized_domain_apply_add_reopens_exact_projection(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    materialized = _materialized_job()

    store = SQLiteStore(database_path)
    try:
        result = store.apply_add(materialized.intent, materialized=materialized)

        assert result.applied is True
        assert materialized.scheduled_for == datetime(
            2031, 7, 2, 6, 30, 15, 123456, tzinfo=UTC
        )
        assert materialized.selected_final_filename == "selected--job-1.webm"
        assert materialized.intent.expected_revision == 3
        assert store.get_materialized_job(materialized.job_id) == materialized
    finally:
        store.close()

    reopened = SQLiteStore(database_path)
    try:
        assert reopened.get_materialized_job(materialized.job_id) == materialized
    finally:
        reopened.close()


def test_materialized_add_persists_durable_publication_reservation_and_replays_its_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    materialized = _materialized_job()
    generated_sizes: list[int] = []

    def token_hex(size: int) -> str:
        generated_sizes.append(size)
        return "c" * 64

    monkeypatch.setattr(store_module.secrets, "token_hex", token_hex)
    store = SQLiteStore(database_path)
    try:
        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        reservation = store.get_publication_reservation(materialized.job_id)

        assert type(reservation) is models_module.PublicationReservation
        assert reservation == models_module.PublicationReservation(
            job_id=materialized.job_id,
            target_component=materialized.destination_collection,
            final_filename=materialized.selected_final_filename,
            claim_token="c" * 64,
        )
        assert store.get_publication_reservation("missing-job") is None
        assert store.apply_add(materialized.intent, materialized=materialized).applied is False
        assert store.get_publication_reservation(materialized.job_id) == reservation
        assert generated_sizes == [32]
    finally:
        store.close()

    reopened = SQLiteStore(database_path)
    try:
        assert reopened.apply_add(materialized.intent, materialized=materialized).applied is False
        assert reopened.get_publication_reservation(materialized.job_id) == reservation
        assert generated_sizes == [32]
    finally:
        reopened.close()


def test_materialized_add_uses_category_when_no_destination_collection_is_selected(
    tmp_path: Path,
) -> None:
    materialized = replace(_materialized_job(), destination_collection=None)
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        reservation = store.get_publication_reservation(materialized.job_id)

        assert reservation is not None
        assert reservation.target_component == materialized.category
        assert reservation.final_filename == materialized.selected_final_filename
    finally:
        store.close()


def test_conflicting_publication_target_and_final_filename_rolls_back_add_transaction(
    tmp_path: Path,
) -> None:
    first = replace(
        _materialized_job(),
        partial_filename="shared.webm",
        selected_final_filename="shared.webm",
    )
    second = replace(
        first,
        job_id="job-2",
        intent=_intent(job_id="job-2", request_id="request-2"),
    )
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.apply_add(first.intent, materialized=first).applied is True
        before = (
            store.list_jobs(),
            store.get_command(first.intent.request_id),
            _command_receipt_rows(store),
            store.list_events(),
            store.get_materialized_job(first.job_id),
            store.get_publication_reservation(first.job_id),
        )

        with pytest.raises(sqlite3.IntegrityError):
            store.apply_add(second.intent, materialized=second)

        assert store.get_job(second.job_id) is None
        assert store.get_command(second.intent.request_id) is None
        assert store.get_materialized_job(second.job_id) is None
        assert store.get_publication_reservation(second.job_id) is None
        assert (
            store.list_jobs(),
            store.get_command(first.intent.request_id),
            _command_receipt_rows(store),
            store.list_events(),
            store.get_materialized_job(first.job_id),
            store.get_publication_reservation(first.job_id),
        ) == before
    finally:
        store.close()


def test_materialized_domain_duplicate_is_idempotent_but_changed_domain_conflicts(
    tmp_path: Path,
) -> None:
    materialized = _materialized_job()
    changed_domain = replace(materialized, queue_collection_id="queue-2")
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        first = store.apply_add(materialized.intent, materialized=materialized)
        repeated = store.apply_add(materialized.intent, materialized=materialized)

        assert first.applied is True
        assert repeated.applied is False
        assert store.get_materialized_job(materialized.job_id) == materialized
        before = (
            store.list_jobs(),
            store.get_command(materialized.intent.request_id),
            store.list_events(),
            store.get_materialized_job(materialized.job_id),
        )

        with pytest.raises(RequestConflictError):
            store.apply_add(materialized.intent, materialized=changed_domain)

        assert (
            store.list_jobs(),
            store.get_command(materialized.intent.request_id),
            store.list_events(),
            store.get_materialized_job(materialized.job_id),
        ) == before
    finally:
        store.close()


@pytest.mark.parametrize("action", ("pause", "start_now"))
def test_materialized_add_replay_ignores_lifecycle_changes_but_not_domain_changes(
    tmp_path: Path, action: str
) -> None:
    materialized = replace(
        _materialized_job(),
        authorized=False,
        manual_hold=False,
        start_now_requested=False,
    )
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        if action == "start_now":
            assert store.initialize_cold_start() == "paused"
            assert store.apply_queue_gate(
                gate="running",
                request_id="open-queue-request",
                payload_digest="b" * 64,
                expected_revision=1,
            ) == store_module.QueueGateResult(
                applied=True, gate="running", revision=2
            )

        control = store.apply_job_control(
            job_id=materialized.job_id,
            action=action,
            request_id=f"{action}-request",
            payload_digest="c" * 64,
            expected_revision=materialized.intent.revision,
        )
        assert control.status == "applied"
        assert control.revision == materialized.intent.revision + 1
        before_replay = (
            store.list_jobs(),
            store.get_command(materialized.intent.request_id),
            _command_receipt_rows(store),
            store.list_events(),
            store.get_materialized_job(materialized.job_id),
            _job_control_command_rows(store),
        )

        replay = store.apply_add(materialized.intent, materialized=materialized)

        assert (replay.applied, replay.job, replay.generation, replay.revision) == (
            False,
            materialized.job_id,
            materialized.intent.generation,
            materialized.intent.revision,
        )
        assert (
            store.list_jobs(),
            store.get_command(materialized.intent.request_id),
            _command_receipt_rows(store),
            store.list_events(),
            store.get_materialized_job(materialized.job_id),
            _job_control_command_rows(store),
        ) == before_replay

        with pytest.raises(RequestConflictError):
            store.apply_add(
                materialized.intent,
                materialized=replace(
                    materialized, priority=materialized.priority + 1
                ),
            )

        assert (
            store.list_jobs(),
            store.get_command(materialized.intent.request_id),
            _command_receipt_rows(store),
            store.list_events(),
            store.get_materialized_job(materialized.job_id),
            _job_control_command_rows(store),
        ) == before_replay
    finally:
        store.close()


def test_collection_hold_set_read_reopen_and_clear_are_idempotent(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        assert store.collection_hold("collection-1") is False
        store.set_collection_hold("collection-1", held=True)
        assert store.collection_hold("collection-1") is True
    finally:
        store.close()

    reopened = SQLiteStore(database_path)
    try:
        assert reopened.collection_hold("collection-1") is True
        reopened.set_collection_hold("collection-1", held=False)
        reopened.set_collection_hold("collection-1", held=False)
        assert reopened.collection_hold("collection-1") is False
    finally:
        reopened.close()

    cleared = SQLiteStore(database_path)
    try:
        assert cleared.collection_hold("collection-1") is False
    finally:
        cleared.close()


def test_collection_hold_rejects_an_invalid_collection_identifier(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        with pytest.raises((TypeError, ValueError)):
            store.set_collection_hold("collection/one", held=True)
    finally:
        store.close()


def test_materialized_domain_cold_recovery_preserves_projection_fields(
    tmp_path: Path,
) -> None:
    materialized = _materialized_job()
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        before_job = store.get_job(materialized.job_id)
        before_domain = store.get_materialized_job(materialized.job_id)

        assert before_job is not None
        assert before_domain == materialized
        assert store.recover_cold_start() == 1

        assert store.get_job(materialized.job_id) == replace(
            before_job,
            generation=before_job.generation + 1,
            revision=before_job.revision + 1,
            state="paused",
        )
        assert before_domain is not None
        assert store.get_materialized_job(materialized.job_id) == replace(
            before_domain,
            intent=replace(
                before_domain.intent,
                generation=before_job.generation + 1,
                revision=before_job.revision + 1,
            ),
        )
    finally:
        store.close()


def test_v3_retry_budget_reopens_exactly_with_its_bounded_audit(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    budget = _retry_budget()
    store = SQLiteStore(database_path)
    try:
        store.apply_add(_intent())
        store.set_retry_budget(budget)
        assert store.get_retry_budget(budget.job_id) == budget
    finally:
        store.close()

    reopened = SQLiteStore(database_path)
    try:
        assert reopened.get_retry_budget(budget.job_id) == budget
    finally:
        reopened.close()


def test_v3_retry_budget_write_requires_current_job_generation(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(_intent())
        with pytest.raises(ValueError, match="generation"):
            store.set_retry_budget(_retry_budget(generation=5))
        assert store.get_retry_budget("job-1") is None
    finally:
        store.close()


def test_v3_cold_recovery_fences_persisted_retry_budget_atomically(tmp_path: Path) -> None:
    budget = _retry_budget()
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(_intent())
        store._connection.execute("UPDATE jobs SET state = 'retry_wait' WHERE job_id = 'job-1'")
        store.set_retry_budget(budget)

        assert store.recover_cold_start() == 1

        job = store.get_job("job-1")
        retry = store.get_retry_budget("job-1")
        assert job is not None
        assert retry is not None
        assert (job.state, job.generation, job.revision) == ("paused", 5, 8)
        assert retry.generation == job.generation
        assert retry.paused is True
        assert retry.audit[:-1] == budget.audit
        assert retry.audit[-1] == retry_module.RetryAuditEvent(
            retry_module.RetryAuditKind.FENCED,
            generation=job.generation,
            budget_number=budget.budget_number,
            ordinary_attempts=budget.ordinary_attempts,
        )
    finally:
        store.close()


def test_v3_cold_recovery_rolls_back_when_retry_audit_cannot_be_fenced(
    tmp_path: Path,
) -> None:
    budget = _retry_budget_at_audit_capacity()
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(_intent(generation=budget.generation))
        store._connection.execute("UPDATE jobs SET state = 'retry_wait' WHERE job_id = 'job-1'")
        store.set_retry_budget(budget)
        before_job = store.get_job("job-1")

        with pytest.raises(OverflowError, match="audit.*capacity"):
            store.recover_cold_start()

        assert store.worker_epoch() is None
        assert store.get_job("job-1") == before_job
        assert store.get_retry_budget("job-1") == budget
    finally:
        store.close()


def test_job_control_persists_pause_resume_and_start_now_without_implicit_authorization(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    materialized = replace(
        _materialized_job(),
        authorized=False,
        manual_hold=False,
        start_now_requested=False,
    )
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        assert store.initialize_cold_start() == "paused"

        paused = store.apply_job_control(
            job_id=materialized.job_id,
            action="pause",
            request_id="pause-request",
            payload_digest="c" * 64,
            expected_revision=materialized.intent.revision,
        )
        assert paused == store_module.JobControlResult(
            status="applied",
            job="job-1",
            generation=4,
            revision=8,
            state="paused",
            authorized=False,
        )
        assert store.get_materialized_job("job-1") == replace(
            materialized,
            intent=replace(materialized.intent, revision=8),
            manual_hold=True,
        )

        resumed = store.apply_job_control(
            job_id=materialized.job_id,
            action="resume",
            request_id="resume-request",
            payload_digest="d" * 64,
            expected_revision=8,
        )
        assert resumed == store_module.JobControlResult(
            status="applied",
            job="job-1",
            generation=4,
            revision=9,
            state="queued",
            authorized=False,
        )
        assert store.get_materialized_job("job-1") == replace(
            materialized,
            intent=replace(materialized.intent, revision=9),
        )

        events_before_noop = store.list_events()
        unchanged_resume = store.apply_job_control(
            job_id=materialized.job_id,
            action="resume",
            request_id="resume-again-request",
            payload_digest="e" * 64,
            expected_revision=9,
        )
        assert unchanged_resume == store_module.JobControlResult(
            status="applied",
            job="job-1",
            generation=4,
            revision=9,
            state="queued",
            authorized=False,
        )
        assert store.list_events() == events_before_noop

        blocked = store.apply_job_control(
            job_id=materialized.job_id,
            action="start_now",
            request_id="start-blocked-request",
            payload_digest="f" * 64,
            expected_revision=9,
        )
        assert blocked == store_module.JobControlResult(
            status="blocked",
            job="job-1",
            generation=4,
            revision=9,
            state="queued",
            authorized=False,
        )
        assert store.get_materialized_job("job-1") == replace(
            materialized,
            intent=replace(materialized.intent, revision=9),
        )

        assert store.apply_queue_gate(
            gate="running",
            request_id="queue-open-request",
            payload_digest="a" * 64,
            expected_revision=1,
        ) == store_module.QueueGateResult(applied=True, gate="running", revision=2)
        started = store.apply_job_control(
            job_id=materialized.job_id,
            action="start_now",
            request_id="start-request",
            payload_digest="b" * 64,
            expected_revision=9,
        )
        assert started == store_module.JobControlResult(
            status="applied",
            job="job-1",
            generation=4,
            revision=10,
            state="queued",
            authorized=True,
        )
        assert store.get_materialized_job("job-1") == replace(
            materialized,
            intent=replace(materialized.intent, revision=10),
            authorized=True,
            start_now_requested=True,
        )
        assert [event.kind for event in store.list_events()] == [
            "job_added",
            "job_paused",
            "job_resumed",
            "job_start_now_requested",
        ]
    finally:
        store.close()


def test_job_control_replays_its_original_result_and_conflicts_on_digest_reuse(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    materialized = _materialized_job()
    store = SQLiteStore(database_path)
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        first = store.apply_job_control(
            job_id=materialized.job_id,
            action="pause",
            request_id="pause-request",
            payload_digest="c" * 64,
            expected_revision=7,
        )
        replay = store.apply_job_control(
            job_id=materialized.job_id,
            action="pause",
            request_id="pause-request",
            payload_digest="c" * 64,
            expected_revision=7,
        )
        assert replay == first
        before_conflict = (
            store.get_job("job-1"),
            store.get_materialized_job("job-1"),
            store.list_events(),
            _job_control_command_rows(store),
        )

        with pytest.raises(RequestConflictError):
            store.apply_job_control(
                job_id=materialized.job_id,
                action="pause",
                request_id="pause-request",
                payload_digest="d" * 64,
                expected_revision=8,
            )

        assert (
            store.get_job("job-1"),
            store.get_materialized_job("job-1"),
            store.list_events(),
            _job_control_command_rows(store),
        ) == before_conflict
    finally:
        store.close()

    reopened = SQLiteStore(database_path)
    try:
        assert reopened.apply_job_control(
            job_id=materialized.job_id,
            action="pause",
            request_id="pause-request",
            payload_digest="c" * 64,
            expected_revision=7,
        ) == first
    finally:
        reopened.close()


@pytest.mark.parametrize(
    ("job_id", "request_id", "expected_revision"),
    (
        pytest.param("missing-job", "missing-job-request", 0, id="unknown-job"),
        pytest.param("job-1", "nonmaterialized-job-request", 7, id="nonmaterialized-job"),
    ),
)
def test_job_control_rejects_nonmaterialized_targets_without_mutation(
    tmp_path: Path,
    job_id: str,
    request_id: str,
    expected_revision: int,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(_intent())
        before = (store.list_jobs(), store.list_events(), _job_control_command_rows(store))

        with pytest.raises(ValueError, match="not a materialized job"):
            store.apply_job_control(
                job_id=job_id,
                action="pause",
                request_id=request_id,
                payload_digest="c" * 64,
                expected_revision=expected_revision,
            )

        assert (store.list_jobs(), store.list_events(), _job_control_command_rows(store)) == before
    finally:
        store.close()


def test_job_control_stale_fence_persists_a_readback_without_mutating_the_job(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    materialized = replace(_materialized_job(), authorized=False, start_now_requested=False)
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        before = (
            store.get_job("job-1"),
            store.get_materialized_job("job-1"),
            store.list_events(),
        )
        stale = store.apply_job_control(
            job_id=materialized.job_id,
            action="pause",
            request_id="stale-request",
            payload_digest="c" * 64,
            expected_revision=materialized.intent.revision - 1,
        )
        assert stale == store_module.JobControlResult(
            status="stale",
            job="job-1",
            generation=4,
            revision=7,
            state="queued",
            authorized=False,
        )
        assert store.apply_job_control(
            job_id=materialized.job_id,
            action="pause",
            request_id="stale-request",
            payload_digest="c" * 64,
            expected_revision=materialized.intent.revision - 1,
        ) == stale
        assert (
            store.get_job("job-1"),
            store.get_materialized_job("job-1"),
            store.list_events(),
        ) == before
        assert _job_control_command_rows(store) == (
            (
                "stale-request",
                "c" * 64,
                "job-1",
                "pause",
                "stale",
                4,
                7,
                "queued",
                0,
            ),
        )
    finally:
        store.close()


def test_job_control_receipt_failure_rolls_back_lifecycle_projection_and_audit(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    materialized = _materialized_job()
    store = SQLiteStore(database_path)
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        before = (
            store.get_job("job-1"),
            store.get_materialized_job("job-1"),
            store.list_events(),
            _job_control_command_rows(store),
        )
        _install_failing_insert_trigger(
            database_path,
            table="job_control_commands",
            trigger_name="fail_job_control_receipt_insert",
            message="injected job control receipt failure",
        )

        with pytest.raises(sqlite3.DatabaseError, match="injected job control receipt failure"):
            store.apply_job_control(
                job_id=materialized.job_id,
                action="pause",
                request_id="pause-request",
                payload_digest="c" * 64,
                expected_revision=7,
            )

        assert (
            store.get_job("job-1"),
            store.get_materialized_job("job-1"),
            store.list_events(),
            _job_control_command_rows(store),
        ) == before
    finally:
        store.close()


def test_v8_migrates_v6_database_to_the_exact_command_receipt_catalog(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected_legacy_job = _create_v6_database(database_path)

    store = SQLiteStore(database_path)
    try:
        assert store.get_job("legacy-job") == store_module.JobRecord(
            job="legacy-job",
            source_url=expected_legacy_job[1],
            generation=23,
            revision=41,
            state="downloading",
        )
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V16_TABLE_SCHEMAS


def test_v7_migration_rolls_back_job_control_ddl_when_creation_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v6_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_migration_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="CREATE TABLE job_control_commands",
        )
        return failed_connection

    monkeypatch.setattr(store_module.sqlite3, "connect", connect_with_migration_failure)

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 6
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V6_TABLE_SCHEMAS


def test_v7_rejects_incomplete_job_control_schema_before_bootstrap_writes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v6_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE job_control_commands (request_id TEXT PRIMARY KEY)")
        connection.execute("PRAGMA user_version = 7")

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 7
        assert connection.execute("PRAGMA table_info(job_control_commands)").fetchall() == [
            (0, "request_id", "TEXT", 0, None, 1)
        ]
    assert _table_names(database_path) == {
        *store_module._V6_TABLE_SCHEMAS,
        "job_control_commands",
    }


@pytest.mark.parametrize(
    "payload_digest", ("a" * 64, "b" * 64), ids=("same_digest", "different_digest")
)
def test_global_receipt_blocks_add_request_id_reused_by_job_control(
    tmp_path: Path, payload_digest: str
) -> None:
    materialized = _materialized_job()
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        before = (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_events(),
            _job_control_command_rows(store),
        )

        with pytest.raises(RequestConflictError):
            store.apply_job_control(
                job_id=materialized.job_id,
                action="pause",
                request_id=materialized.intent.request_id,
                payload_digest=payload_digest,
                expected_revision=materialized.intent.revision,
            )

        assert (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_events(),
            _job_control_command_rows(store),
        ) == before
    finally:
        store.close()


def test_global_receipt_blocks_queue_gate_request_id_reused_by_job_control(
    tmp_path: Path,
) -> None:
    materialized = _materialized_job()
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        assert store.initialize_cold_start() == "paused"
        assert store.apply_queue_gate(
            gate="running",
            request_id="global-request",
            payload_digest="b" * 64,
            expected_revision=1,
        ) == store_module.QueueGateResult(applied=True, gate="running", revision=2)
        before = (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.queue_gate(),
            store.list_events(),
            _job_control_command_rows(store),
        )

        with pytest.raises(RequestConflictError):
            store.apply_job_control(
                job_id=materialized.job_id,
                action="pause",
                request_id="global-request",
                payload_digest="c" * 64,
                expected_revision=materialized.intent.revision,
            )

        assert (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.queue_gate(),
            store.list_events(),
            _job_control_command_rows(store),
        ) == before
    finally:
        store.close()


@pytest.mark.parametrize("surface", ("add", "queue_gate"))
def test_global_receipt_blocks_job_control_request_id_reused_by_other_surface(
    tmp_path: Path, surface: str
) -> None:
    materialized = _materialized_job()
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        if surface == "queue_gate":
            assert store.initialize_cold_start() == "paused"
        assert store.apply_job_control(
            job_id=materialized.job_id,
            action="pause",
            request_id="global-request",
            payload_digest="b" * 64,
            expected_revision=materialized.intent.revision,
        ) == store_module.JobControlResult(
            status="applied",
            job=materialized.job_id,
            generation=materialized.intent.generation,
            revision=materialized.intent.revision + 1,
            state="paused",
            authorized=materialized.authorized,
        )
        before = (
            store.list_jobs(),
            store.get_materialized_job(materialized.job_id),
            store.queue_gate(),
            store.list_events(),
            _queue_command_rows(store),
            _job_control_command_rows(store),
        )

        with pytest.raises(RequestConflictError):
            if surface == "add":
                store.apply_add(
                    _intent(
                        job_id="job-2",
                        request_id="global-request",
                        payload_digest="c" * 64,
                    )
                )
            else:
                store.apply_queue_gate(
                    gate="running",
                    request_id="global-request",
                    payload_digest="c" * 64,
                    expected_revision=1,
                )

        assert (
            store.list_jobs(),
            store.get_materialized_job(materialized.job_id),
            store.queue_gate(),
            store.list_events(),
            _queue_command_rows(store),
            _job_control_command_rows(store),
        ) == before
    finally:
        store.close()


def test_v8_migrates_v7_receipts_to_the_shared_global_registry(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected_legacy_job = _create_v7_database(database_path)
    legacy_source_url = expected_legacy_job[1]
    assert isinstance(legacy_source_url, bytes)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO queue_commands (request_id, payload_digest, gate, revision)
            VALUES ('queue-request', ?, 'running', 2)
            """,
            ("c" * 64,),
        )
        connection.execute(
            """
            INSERT INTO job_control_commands (
                request_id,
                payload_digest,
                job_id,
                action,
                status,
                generation,
                revision,
                state,
                authorized
            )
            VALUES ('control-request', ?, 'legacy-job', 'pause', 'applied', 23, 42, 'paused', 0)
            """,
            ("d" * 64,),
        )

    store = SQLiteStore(database_path)
    try:
        assert store.get_job("legacy-job") == store_module.JobRecord(
            job="legacy-job",
            source_url=expected_legacy_job[1],
            generation=23,
            revision=41,
            state="downloading",
        )
        assert _command_receipt_rows(store) == (
            ("control-request", "d" * 64, "job_control", "pause"),
            ("legacy-request", "b" * 64, "add", "add"),
            ("queue-request", "c" * 64, "queue_gate", "queue_gate"),
        )
        assert _queue_command_rows(store) == (("queue-request", "c" * 64, "running", 2),)
        assert _job_control_command_rows(store) == (
            (
                "control-request",
                "d" * 64,
                "legacy-job",
                "pause",
                "applied",
                23,
                42,
                "paused",
                0,
            ),
        )
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
    assert "command_receipts" in _table_names(database_path)


@pytest.mark.parametrize(
    ("first_table", "second_table"),
    (
        ("commands", "queue_commands"),
        ("commands", "job_control_commands"),
        ("queue_commands", "job_control_commands"),
    ),
)
def test_v8_migration_rejects_duplicate_request_ids_across_legacy_receipt_tables(
    tmp_path: Path, first_table: str, second_table: str
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v7_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO queue_commands (request_id, payload_digest, gate, revision)
            VALUES ('queue-request', ?, 'running', 2)
            """,
            ("c" * 64,),
        )
        connection.execute(
            """
            INSERT INTO job_control_commands (
                request_id,
                payload_digest,
                job_id,
                action,
                status,
                generation,
                revision,
                state,
                authorized
            )
            VALUES ('control-request', ?, 'legacy-job', 'pause', 'applied', 23, 42, 'paused', 0)
            """,
            ("d" * 64,),
        )
        for table in (first_table, second_table):
            connection.execute(
                f"UPDATE {table} SET request_id = 'collision-request'"
            )

    with pytest.raises(RuntimeError, match="duplicate request_id"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 7
        receipt_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT request_id FROM commands
                UNION ALL
                SELECT request_id FROM queue_commands
                UNION ALL
                SELECT request_id FROM job_control_commands
            )
            WHERE request_id = 'collision-request'
            """
        ).fetchone()
        assert receipt_count == (2,)
    assert _table_names(database_path) == set(store_module._V7_TABLE_SCHEMAS)


@pytest.mark.parametrize("state", ("removed", "completed", "cancelled", "failed"))
@pytest.mark.parametrize("action", ("pause", "resume", "start_now", "remove"))
def test_job_control_blocks_terminal_materialized_jobs_without_lifecycle_mutation(
    tmp_path: Path, state: str, action: str
) -> None:
    materialized = replace(
        _materialized_job(),
        authorized=False,
        manual_hold=True,
        start_now_requested=False,
    )
    request_id = f"terminal-{state}-{action}-request"
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        assert store.initialize_cold_start() == "paused"
        assert store.apply_queue_gate(
            gate="running",
            request_id="queue-open-request",
            payload_digest="a" * 64,
            expected_revision=1,
        ) == store_module.QueueGateResult(applied=True, gate="running", revision=2)
        store._connection.execute(
            "UPDATE jobs SET state = ? WHERE job_id = ?", (state, materialized.job_id)
        )
        before_target = (
            tuple(
                tuple(row)
                for row in store._connection.execute(
                    "SELECT * FROM jobs WHERE job_id = ?", (materialized.job_id,)
                ).fetchall()
            ),
            tuple(
                tuple(row)
                for row in store._connection.execute(
                    "SELECT * FROM materialized_jobs WHERE job_id = ?",
                    (materialized.job_id,),
                ).fetchall()
            ),
        )
        events_before = store.list_events()
        expected = store_module.JobControlResult(
            status="blocked",
            job=materialized.job_id,
            generation=materialized.intent.generation,
            revision=materialized.intent.revision,
            state=state,
            authorized=False,
        )

        assert store.apply_job_control(
            job_id=materialized.job_id,
            action=action,
            request_id=request_id,
            payload_digest="b" * 64,
            expected_revision=materialized.intent.revision,
        ) == expected
        assert store.apply_job_control(
            job_id=materialized.job_id,
            action=action,
            request_id=request_id,
            payload_digest="b" * 64,
            expected_revision=materialized.intent.revision,
        ) == expected
        assert (
            tuple(
                tuple(row)
                for row in store._connection.execute(
                    "SELECT * FROM jobs WHERE job_id = ?", (materialized.job_id,)
                ).fetchall()
            ),
            tuple(
                tuple(row)
                for row in store._connection.execute(
                    "SELECT * FROM materialized_jobs WHERE job_id = ?",
                    (materialized.job_id,),
                ).fetchall()
            ),
        ) == before_target
        assert store.list_events() == events_before
        assert _job_control_command_rows(store) == (
            (
                request_id,
                "b" * 64,
                materialized.job_id,
                action,
                "blocked",
                materialized.intent.generation,
                materialized.intent.revision,
                state,
                0,
            ),
        )
        receipt = store._connection.execute(
            """
            SELECT request_id, payload_digest, scope, action
            FROM command_receipts
            WHERE request_id = ?
            """,
            (request_id,),
        ).fetchone()
        assert receipt is not None
        assert tuple(receipt) == (request_id, "b" * 64, "job_control", action)
    finally:
        store.close()


def test_job_control_remove_tombstones_an_inactive_materialized_job_and_preserves_history(
    tmp_path: Path,
) -> None:
    materialized = replace(
        _materialized_job(),
        source_kind=SourceKind.DIRECT,
        queue_collection_id=None,
        scheduled_for=None,
        authorized=True,
        manual_hold=False,
        start_now_requested=True,
        category="Other",
        destination_collection=None,
        partial_filename="remove.bin",
        selected_final_filename="remove--job-1.bin",
    )
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        original_job = store.get_job(materialized.job_id)
        assert original_job is not None

        removed = store.apply_job_control(
            job_id=materialized.job_id,
            action="remove",
            request_id="remove-request",
            payload_digest="b" * 64,
            expected_revision=materialized.intent.revision,
        )

        assert removed == store_module.JobControlResult(
            status="applied",
            job=materialized.job_id,
            generation=materialized.intent.generation,
            revision=materialized.intent.revision + 1,
            state="removed",
            authorized=False,
        )
        assert store.get_job(materialized.job_id) == store_module.JobRecord(
            job=materialized.job_id,
            source_url=original_job.source_url,
            generation=materialized.intent.generation,
            revision=materialized.intent.revision + 1,
            state="removed",
        )
        assert store.get_materialized_job(materialized.job_id) == replace(
            materialized,
            intent=replace(
                materialized.intent, revision=materialized.intent.revision + 1
            ),
            authorized=False,
            manual_hold=True,
            start_now_requested=False,
        )
        assert [
            (event.kind, event.job, event.generation, event.revision)
            for event in store.list_events()
        ] == [
            (
                "job_added",
                materialized.job_id,
                materialized.intent.generation,
                materialized.intent.revision,
            ),
            (
                "job_removed",
                materialized.job_id,
                materialized.intent.generation,
                materialized.intent.revision + 1,
            ),
        ]
        assert _job_control_command_rows(store) == (
            (
                "remove-request",
                "b" * 64,
                materialized.job_id,
                "remove",
                "applied",
                materialized.intent.generation,
                materialized.intent.revision + 1,
                "removed",
                0,
            ),
        )
        assert _command_receipt_rows(store) == (
            ("remove-request", "b" * 64, "job_control", "remove"),
            ("request-1", "a" * 64, "add", "add"),
        )
    finally:
        store.close()


def test_job_control_remove_replays_and_preserves_global_request_collisions(
    tmp_path: Path,
) -> None:
    materialized = replace(
        _materialized_job(),
        authorized=True,
        manual_hold=False,
        start_now_requested=True,
    )
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        first = store.apply_job_control(
            job_id=materialized.job_id,
            action="remove",
            request_id="remove-request",
            payload_digest="b" * 64,
            expected_revision=materialized.intent.revision,
        )
        after_first = (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_events(),
            _job_control_command_rows(store),
            _command_receipt_rows(store),
        )

        assert store.apply_job_control(
            job_id=materialized.job_id,
            action="remove",
            request_id="remove-request",
            payload_digest="b" * 64,
            expected_revision=materialized.intent.revision,
        ) == first
        assert (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_events(),
            _job_control_command_rows(store),
            _command_receipt_rows(store),
        ) == after_first

        with pytest.raises(RequestConflictError):
            store.apply_job_control(
                job_id=materialized.job_id,
                action="remove",
                request_id="remove-request",
                payload_digest="c" * 64,
                expected_revision=materialized.intent.revision + 1,
            )
        with pytest.raises(RequestConflictError):
            store.apply_job_control(
                job_id=materialized.job_id,
                action="pause",
                request_id="remove-request",
                payload_digest="b" * 64,
                expected_revision=materialized.intent.revision + 1,
            )
        with pytest.raises(RequestConflictError):
            store.apply_add(
                _intent(
                    job_id="job-2",
                    request_id="remove-request",
                    payload_digest="d" * 64,
                )
            )
        assert (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_events(),
            _job_control_command_rows(store),
            _command_receipt_rows(store),
        ) == after_first
    finally:
        store.close()


def test_job_control_remove_stale_revision_preserves_the_target_and_replays(
    tmp_path: Path,
) -> None:
    materialized = replace(
        _materialized_job(),
        authorized=True,
        manual_hold=False,
        start_now_requested=True,
    )
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        before = (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_events(),
        )

        stale = store.apply_job_control(
            job_id=materialized.job_id,
            action="remove",
            request_id="stale-remove-request",
            payload_digest="b" * 64,
            expected_revision=materialized.intent.revision - 1,
        )

        assert stale == store_module.JobControlResult(
            status="stale",
            job=materialized.job_id,
            generation=materialized.intent.generation,
            revision=materialized.intent.revision,
            state="queued",
            authorized=True,
        )
        assert store.apply_job_control(
            job_id=materialized.job_id,
            action="remove",
            request_id="stale-remove-request",
            payload_digest="b" * 64,
            expected_revision=materialized.intent.revision - 1,
        ) == stale
        assert (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_events(),
        ) == before
        assert _job_control_command_rows(store) == (
            (
                "stale-remove-request",
                "b" * 64,
                materialized.job_id,
                "remove",
                "stale",
                materialized.intent.generation,
                materialized.intent.revision,
                "queued",
                1,
            ),
        )
    finally:
        store.close()


@pytest.mark.parametrize("state", ("resolving", "downloading", "pausing", "finalizing"))
def test_job_control_remove_blocks_active_like_states_without_lifecycle_mutation(
    tmp_path: Path, state: str
) -> None:
    materialized = replace(
        _materialized_job(),
        authorized=True,
        manual_hold=False,
        start_now_requested=True,
    )
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store.apply_add(materialized.intent, materialized=materialized)
        store._connection.execute(
            "UPDATE jobs SET state = ? WHERE job_id = ?", (state, materialized.job_id)
        )
        before = (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_events(),
        )
        expected = store_module.JobControlResult(
            status="blocked",
            job=materialized.job_id,
            generation=materialized.intent.generation,
            revision=materialized.intent.revision,
            state=state,
            authorized=True,
        )

        assert store.apply_job_control(
            job_id=materialized.job_id,
            action="remove",
            request_id=f"active-{state}-remove-request",
            payload_digest="b" * 64,
            expected_revision=materialized.intent.revision,
        ) == expected
        assert store.apply_job_control(
            job_id=materialized.job_id,
            action="remove",
            request_id=f"active-{state}-remove-request",
            payload_digest="b" * 64,
            expected_revision=materialized.intent.revision,
        ) == expected
        assert (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_events(),
        ) == before
    finally:
        store.close()


def test_v9_migrates_v8_job_control_constraints_without_losing_receipts(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected_legacy_job = _create_v8_database(database_path)
    legacy_source_url = expected_legacy_job[1]
    assert isinstance(legacy_source_url, bytes)

    store = SQLiteStore(database_path)
    try:
        assert store.get_job("legacy-job") == store_module.JobRecord(
            job="legacy-job",
            source_url=legacy_source_url,
            generation=23,
            revision=41,
            state="downloading",
        )
        assert _job_control_command_rows(store) == (
            (
                "control-request",
                "d" * 64,
                "legacy-job",
                "pause",
                "blocked",
                23,
                41,
                "completed",
                0,
            ),
        )
        assert _command_receipt_rows(store) == (
            ("control-request", "d" * 64, "job_control", "pause"),
            ("legacy-request", "b" * 64, "add", "add"),
        )
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V16_TABLE_SCHEMAS


def test_v9_receipt_rebuild_failure_restores_v8_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected_legacy_job = _create_v8_database(database_path)
    expected_job_control_commands = (
        (
            "control-request",
            "d" * 64,
            "legacy-job",
            "pause",
            "blocked",
            23,
            41,
            "completed",
            0,
        ),
    )
    expected_command_receipts = (
        ("control-request", "d" * 64, "job_control", "pause"),
        ("legacy-request", "b" * 64, "add", "add"),
    )
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_receipt_rebuild_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="INSERT INTO command_receipts",
        )
        return failed_connection

    monkeypatch.setattr(
        store_module.sqlite3, "connect", connect_with_receipt_rebuild_failure
    )

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 8
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        assert connection.execute(
            """
            SELECT job_id, source_url, generation, revision, state
            FROM jobs
            WHERE job_id = 'legacy-job'
            """
        ).fetchone() == expected_legacy_job
        assert connection.execute(
            """
            SELECT
                request_id,
                payload_digest,
                job_id,
                action,
                status,
                generation,
                revision,
                state,
                authorized
            FROM job_control_commands
            ORDER BY request_id
            """
        ).fetchall() == list(expected_job_control_commands)
        assert connection.execute(
            """
            SELECT request_id, payload_digest, scope, action
            FROM command_receipts
            ORDER BY request_id
            """
        ).fetchall() == list(expected_command_receipts)
    assert table_schemas == store_module._V8_TABLE_SCHEMAS
    assert not {name for name in table_schemas if name.endswith("_v8")}


def test_v11_migrates_v9_materialized_projection_without_inventing_a_receipt(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected_legacy_job = _create_v9_database(database_path)
    legacy_source_url = expected_legacy_job[1]
    assert type(legacy_source_url) is bytes
    materialized = MaterializedJob(
        job_id="legacy-job",
        intent=DownloadIntent(
            job_id="legacy-job",
            request_id="legacy-request",
            payload_digest="b" * 64,
            source_url=legacy_source_url,
            generation=23,
            revision=41,
        ),
        source_kind=SourceKind("video"),
        queue_collection_id="legacy-queue",
        priority=9,
        order_key=17,
        scheduled_for=None,
        authorized=True,
        manual_hold=False,
        start_now_requested=False,
        category="Videos",
        destination_collection="Legacy collection",
        partial_filename="legacy.webm",
        selected_final_filename="legacy.webm",
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO materialized_jobs (
                job_id,
                source_kind,
                queue_collection_id,
                priority,
                order_key,
                scheduled_for_us,
                authorized,
                manual_hold,
                start_now_requested,
                category,
                destination_collection,
                partial_filename,
                selected_final_filename,
                expected_revision
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (materialized.job_id, *SQLiteStore._projection_values(materialized)),
        )
        projection_before = connection.execute(
            """
            SELECT
                job_id,
                source_kind,
                queue_collection_id,
                priority,
                order_key,
                scheduled_for_us,
                authorized,
                manual_hold,
                start_now_requested,
                category,
                destination_collection,
                partial_filename,
                selected_final_filename,
                expected_revision
            FROM materialized_jobs
            WHERE job_id = 'legacy-job'
            """
        ).fetchone()

    conflicting = replace(
        materialized,
        job_id="new-job",
        intent=_intent(job_id="new-job", request_id="new-request"),
    )

    store = SQLiteStore(database_path)
    try:
        assert store.get_job("legacy-job") == store_module.JobRecord(
            job="legacy-job",
            source_url=legacy_source_url,
            generation=23,
            revision=41,
            state="downloading",
        )
        assert store.get_materialized_job("legacy-job") == materialized
        before = (
            tuple(store.list_jobs()),
            tuple(
                tuple(row)
                for row in store._connection.execute(
                    "SELECT request_id FROM commands ORDER BY request_id"
                ).fetchall()
            ),
            tuple(store.list_events()),
            tuple(
                tuple(row)
                for row in store._connection.execute(
                    "SELECT job_id FROM materialized_jobs ORDER BY job_id"
                ).fetchall()
            ),
            tuple(
                tuple(row)
                for row in store._connection.execute(
                    "SELECT job_id FROM publication_reservations ORDER BY job_id"
                ).fetchall()
            ),
        )

        with pytest.raises(ValueError, match="unsupported source kind"):
            store.apply_add(conflicting.intent, materialized=conflicting)

        assert (
            tuple(store.list_jobs()),
            tuple(
                tuple(row)
                for row in store._connection.execute(
                    "SELECT request_id FROM commands ORDER BY request_id"
                ).fetchall()
            ),
            tuple(store.list_events()),
            tuple(
                tuple(row)
                for row in store._connection.execute(
                    "SELECT job_id FROM materialized_jobs ORDER BY job_id"
                ).fetchall()
            ),
            tuple(
                tuple(row)
                for row in store._connection.execute(
                    "SELECT job_id FROM publication_reservations ORDER BY job_id"
                ).fetchall()
            ),
        ) == before
        assert store.get_publication_reservation("legacy-job") is None
        assert store.get_publication_marker_binding("legacy-job") is None
        with pytest.raises(ValueError, match="publication reservation"):
            store.bind_publication_marker(
                "legacy-job",
                claim_token="a" * 64,
                marker_device=1,
                marker_inode=2,
            )
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        projection_after = connection.execute(
            """
            SELECT
                job_id,
                source_kind,
                queue_collection_id,
                priority,
                order_key,
                scheduled_for_us,
                authorized,
                manual_hold,
                start_now_requested,
                category,
                destination_collection,
                partial_filename,
                selected_final_filename,
                expected_revision
            FROM materialized_jobs
            WHERE job_id = 'legacy-job'
            """
        ).fetchone()
        receipts = connection.execute(
            "SELECT job_id, target_component, final_filename, claim_token FROM publication_reservations"
        ).fetchall()
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert projection_after == projection_before
    assert receipts == []
    assert table_schemas == store_module._V16_TABLE_SCHEMAS


def test_v10_migration_rolls_back_publication_receipt_ddl_when_version_bump_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    expected_legacy_job = _create_v9_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_version_bump_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="PRAGMA user_version = 10",
        )
        return failed_connection

    monkeypatch.setattr(store_module.sqlite3, "connect", connect_with_version_bump_failure)

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 9
        assert connection.execute(
            """
            SELECT job_id, source_url, generation, revision, state
            FROM jobs
            WHERE job_id = 'legacy-job'
            """
        ).fetchone() == expected_legacy_job
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V9_TABLE_SCHEMAS
    assert "publication_reservations" not in table_schemas


def test_fresh_v11_bootstrap_retries_after_publication_receipt_ddl_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_publication_receipt_ddl_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="CREATE TABLE publication_reservations",
        )
        return failed_connection

    monkeypatch.setattr(
        store_module.sqlite3, "connect", connect_with_publication_receipt_ddl_failure
    )

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert version == 0
    assert table_schemas == {}

    monkeypatch.setattr(store_module.sqlite3, "connect", original_connect)
    recovered = SQLiteStore(database_path)
    try:
        assert recovered._connection.execute("PRAGMA user_version").fetchone()[0] == 16
    finally:
        recovered.close()

    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V16_TABLE_SCHEMAS


@pytest.mark.parametrize(
    ("column", "corrupt_value"),
    (
        ("target_component", "Other collection"),
        ("final_filename", "different.webm"),
    ),
)
def test_materialized_receipt_readback_and_replay_reject_mismatched_projection(
    tmp_path: Path, column: str, corrupt_value: str
) -> None:
    materialized = _materialized_job()
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        store._connection.execute(
            f"UPDATE publication_reservations SET {column} = ? WHERE job_id = ?",
            (corrupt_value, materialized.job_id),
        )

        with pytest.raises(ValueError):
            store.get_publication_reservation(materialized.job_id)
        with pytest.raises(ValueError):
            store.apply_add(materialized.intent)
        with pytest.raises(ValueError):
            store.apply_add(materialized.intent, materialized=materialized)
    finally:
        store.close()


def test_materialized_receipt_replay_rejects_missing_receipt_without_materialized_argument(
    tmp_path: Path,
) -> None:
    materialized = _materialized_job()
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        store._connection.execute(
            "DELETE FROM publication_reservations WHERE job_id = ?", (materialized.job_id,)
        )

        assert store.get_publication_reservation(materialized.job_id) is None
        with pytest.raises(ValueError, match="missing publication reservation"):
            store.apply_add(materialized.intent)
        with pytest.raises(ValueError, match="missing publication reservation"):
            store.apply_add(materialized.intent, materialized=materialized)
        assert (
            store._connection.execute(
                "SELECT claim_token FROM publication_reservations WHERE job_id = ?",
                (materialized.job_id,),
            ).fetchall()
            == []
        )
    finally:
        store.close()


def test_publication_reservation_readback_rejects_receipt_without_projection(
    tmp_path: Path,
) -> None:
    materialized = _materialized_job()
    target_component = materialized.destination_collection or materialized.category
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store._connection.execute("PRAGMA foreign_keys = OFF")
        try:
            store._connection.execute(
                """
                INSERT INTO publication_reservations (
                    job_id, target_component, final_filename, claim_token
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    materialized.job_id,
                    target_component,
                    materialized.selected_final_filename,
                    "a" * 64,
                ),
            )
        finally:
            store._connection.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(ValueError, match="owner"):
            store.get_publication_reservation(materialized.job_id)
    finally:
        store.close()


def test_publication_reservation_readback_rejects_projection_and_receipt_without_job_parent(
    tmp_path: Path,
) -> None:
    materialized = _materialized_job()
    target_component = materialized.destination_collection or materialized.category
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store._connection.execute("PRAGMA foreign_keys = OFF")
        try:
            store._connection.execute(
                """
                INSERT INTO materialized_jobs (
                    job_id,
                    source_kind,
                    queue_collection_id,
                    priority,
                    order_key,
                    scheduled_for_us,
                    authorized,
                    manual_hold,
                    start_now_requested,
                    category,
                    destination_collection,
                    partial_filename,
                    selected_final_filename,
                    expected_revision
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (materialized.job_id, *SQLiteStore._projection_values(materialized)),
            )
            store._connection.execute(
                """
                INSERT INTO publication_reservations (
                    job_id, target_component, final_filename, claim_token
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    materialized.job_id,
                    target_component,
                    materialized.selected_final_filename,
                    "a" * 64,
                ),
            )
        finally:
            store._connection.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(ValueError, match="owner"):
            store.get_publication_reservation(materialized.job_id)
    finally:
        store.close()


@pytest.mark.parametrize(
    ("column", "blob_value"),
    (
        ("job_id", sqlite3.Binary(b"job-1")),
        ("target_component", sqlite3.Binary(b"Course material")),
        ("final_filename", sqlite3.Binary(b"selected--job-1.webm")),
        ("claim_token", sqlite3.Binary(b"a" * 64)),
    ),
)
def test_publication_reservation_schema_rejects_blob_insert_and_update(
    tmp_path: Path, column: str, blob_value: object
) -> None:
    materialized = _materialized_job()
    target_component = materialized.destination_collection or materialized.category
    assert target_component is not None
    values: dict[str, object] = {
        "job_id": materialized.job_id,
        "target_component": target_component,
        "final_filename": materialized.selected_final_filename,
        "claim_token": "a" * 64,
    }
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        store._connection.execute("PRAGMA foreign_keys = OFF")
        values[column] = blob_value
        try:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
                store._connection.execute(
                    """
                    INSERT INTO publication_reservations (
                        job_id, target_component, final_filename, claim_token
                    )
                    VALUES (:job_id, :target_component, :final_filename, :claim_token)
                    """,
                    values,
                )
        finally:
            store._connection.execute("PRAGMA foreign_keys = ON")
        assert (
            store._connection.execute("SELECT COUNT(*) FROM publication_reservations").fetchone()[0]
            == 0
        )

        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            store._connection.execute(
                f"UPDATE publication_reservations SET {column} = ? WHERE job_id = ?",
                (blob_value, materialized.job_id),
            )
    finally:
        store.close()


def test_publication_marker_binding_is_narrow_durable_and_exactly_idempotent(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    materialized = _materialized_job()
    store = SQLiteStore(database_path)
    try:
        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        reservation = store.get_publication_reservation(materialized.job_id)

        assert reservation is not None
        assert store.get_publication_marker_binding(materialized.job_id) is None
        first = store.bind_publication_marker(
            materialized.job_id,
            claim_token=reservation.claim_token,
            marker_device=901,
            marker_inode=902,
        )
        repeated = store.bind_publication_marker(
            materialized.job_id,
            claim_token=reservation.claim_token,
            marker_device=901,
            marker_inode=902,
        )

        assert tuple(store_module.PublicationMarkerBinding.__dataclass_fields__) == (
            "job_id",
            "marker_device",
            "marker_inode",
        )
        assert first == store_module.PublicationMarkerBinding(
            job_id=materialized.job_id,
            marker_device=901,
            marker_inode=902,
        )
        assert repeated == first
        assert store.get_publication_marker_binding(materialized.job_id) == first
        assert not hasattr(first, "claim_token")
        assert [
            tuple(row)
            for row in store._connection.execute(
                """
                SELECT job_id, marker_device, marker_inode
                FROM publication_marker_bindings
                """
            ).fetchall()
        ] == [(materialized.job_id, 901, 902)]
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert [
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(publication_marker_bindings)"
            ).fetchall()
        ] == ["job_id", "marker_device", "marker_inode"]

    reopened = SQLiteStore(database_path)
    try:
        assert reopened.get_publication_marker_binding(materialized.job_id) == first
        assert reopened.bind_publication_marker(
            materialized.job_id,
            claim_token=reservation.claim_token,
            marker_device=901,
            marker_inode=902,
        ) == first
    finally:
        reopened.close()


def test_publication_marker_binding_rejects_mismatches_without_replacing_original(
    tmp_path: Path,
) -> None:
    materialized = _materialized_job()
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        reservation = store.get_publication_reservation(materialized.job_id)
        assert reservation is not None
        original = store.bind_publication_marker(
            materialized.job_id,
            claim_token=reservation.claim_token,
            marker_device=901,
            marker_inode=902,
        )
        wrong_token = "0" * 64 if reservation.claim_token != "0" * 64 else "1" * 64
        before = store._connection.execute(
            """
            SELECT job_id, marker_device, marker_inode
            FROM publication_marker_bindings
            WHERE job_id = ?
            """,
            (materialized.job_id,),
        ).fetchall()

        with pytest.raises(ValueError, match="claim token"):
            store.bind_publication_marker(
                materialized.job_id,
                claim_token=wrong_token,
                marker_device=901,
                marker_inode=902,
            )
        with pytest.raises(ValueError, match="does not match"):
            store.bind_publication_marker(
                materialized.job_id,
                claim_token=reservation.claim_token,
                marker_device=903,
                marker_inode=902,
            )

        assert store.get_publication_marker_binding(materialized.job_id) == original
        assert store._connection.execute(
            """
            SELECT job_id, marker_device, marker_inode
            FROM publication_marker_bindings
            WHERE job_id = ?
            """,
            (materialized.job_id,),
        ).fetchall() == before
    finally:
        store.close()


def test_publication_marker_binding_requires_a_current_valid_reservation(
    tmp_path: Path,
) -> None:
    missing = _materialized_job()
    malformed = replace(
        missing,
        job_id="job-2",
        intent=_intent(job_id="job-2", request_id="request-2"),
        selected_final_filename="selected--job-2.webm",
    )
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.apply_add(missing.intent, materialized=missing).applied is True
        missing_reservation = store.get_publication_reservation(missing.job_id)
        assert missing_reservation is not None
        store._connection.execute(
            "DELETE FROM publication_reservations WHERE job_id = ?", (missing.job_id,)
        )

        with pytest.raises(ValueError, match="publication reservation"):
            store.bind_publication_marker(
                missing.job_id,
                claim_token=missing_reservation.claim_token,
                marker_device=901,
                marker_inode=902,
            )
        assert store.get_publication_marker_binding(missing.job_id) is None

        assert store.apply_add(malformed.intent, materialized=malformed).applied is True
        malformed_reservation = store.get_publication_reservation(malformed.job_id)
        assert malformed_reservation is not None
        store._connection.execute(
            """
            UPDATE publication_reservations
            SET target_component = 'Other collection'
            WHERE job_id = ?
            """,
            (malformed.job_id,),
        )

        with pytest.raises(ValueError, match="publication reservation"):
            store.get_publication_marker_binding(malformed.job_id)
        with pytest.raises(ValueError, match="publication reservation"):
            store.bind_publication_marker(
                malformed.job_id,
                claim_token=malformed_reservation.claim_token,
                marker_device=901,
                marker_inode=902,
            )
        assert store._connection.execute(
            "SELECT job_id FROM publication_marker_bindings ORDER BY job_id"
        ).fetchall() == []
    finally:
        store.close()


def test_publication_marker_binding_getter_rejects_orphaned_binding(
    tmp_path: Path,
) -> None:
    materialized = _materialized_job()
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        reservation = store.get_publication_reservation(materialized.job_id)
        assert reservation is not None
        binding = store.bind_publication_marker(
            materialized.job_id,
            claim_token=reservation.claim_token,
            marker_device=901,
            marker_inode=902,
        )
        store._connection.execute("PRAGMA foreign_keys = OFF")
        try:
            store._connection.execute(
                "DELETE FROM publication_reservations WHERE job_id = ?", (materialized.job_id,)
            )
        finally:
            store._connection.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(ValueError, match="publication marker binding"):
            store.get_publication_marker_binding(materialized.job_id)
        with pytest.raises(ValueError, match="publication reservation"):
            store.bind_publication_marker(
                materialized.job_id,
                claim_token=reservation.claim_token,
                marker_device=901,
                marker_inode=902,
            )
        assert [
            tuple(row)
            for row in store._connection.execute(
                """
                SELECT job_id, marker_device, marker_inode
                FROM publication_marker_bindings
                WHERE job_id = ?
                """,
                (materialized.job_id,),
            ).fetchall()
        ] == [(binding.job_id, binding.marker_device, binding.marker_inode)]
    finally:
        store.close()


def test_publication_marker_binding_insert_failure_rolls_back(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    materialized = _materialized_job()
    store = SQLiteStore(database_path)
    try:
        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        reservation = store.get_publication_reservation(materialized.job_id)
        assert reservation is not None
        _install_failing_insert_trigger(
            database_path,
            table="publication_marker_bindings",
            trigger_name="fail_publication_marker_binding_insert",
            message="injected publication marker binding insert failure",
        )

        with pytest.raises(
            sqlite3.DatabaseError, match="injected publication marker binding insert failure"
        ):
            store.bind_publication_marker(
                materialized.job_id,
                claim_token=reservation.claim_token,
                marker_device=901,
                marker_inode=902,
            )

        assert store.get_publication_marker_binding(materialized.job_id) is None
        store._connection.execute("DROP TRIGGER fail_publication_marker_binding_insert")
        assert store.bind_publication_marker(
            materialized.job_id,
            claim_token=reservation.claim_token,
            marker_device=901,
            marker_inode=902,
        ) == store_module.PublicationMarkerBinding(
            job_id=materialized.job_id,
            marker_device=901,
            marker_inode=902,
        )
    finally:
        store.close()


@pytest.mark.parametrize(
    ("marker_device", "marker_inode"),
    (
        (-1, 2),
        (1, -1),
        (store_module._MAX_COUNTER + 1, 2),
        (1, store_module._MAX_COUNTER + 1),
        (True, 2),
        (1, False),
    ),
)
def test_publication_marker_binding_rejects_invalid_identity_without_mutation(
    tmp_path: Path, marker_device: object, marker_inode: object
) -> None:
    materialized = _materialized_job()
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        reservation = store.get_publication_reservation(materialized.job_id)
        assert reservation is not None

        with pytest.raises((TypeError, ValueError)):
            store.bind_publication_marker(
                materialized.job_id,
                claim_token=reservation.claim_token,
                marker_device=marker_device,
                marker_inode=marker_inode,
            )

        assert store.get_publication_marker_binding(materialized.job_id) is None
    finally:
        store.close()


def test_v11_migrates_v10_without_backfilling_marker_bindings(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    legacy_job = _create_v10_database(database_path)
    legacy_source_url = legacy_job[1]
    assert type(legacy_source_url) is bytes
    materialized = MaterializedJob(
        job_id="legacy-job",
        intent=DownloadIntent(
            job_id="legacy-job",
            request_id="legacy-request",
            payload_digest="b" * 64,
            source_url=legacy_source_url,
            generation=23,
            revision=41,
        ),
        source_kind=SourceKind("video"),
        queue_collection_id="legacy-queue",
        priority=9,
        order_key=17,
        scheduled_for=None,
        authorized=True,
        manual_hold=False,
        start_now_requested=False,
        category="Videos",
        destination_collection="Legacy collection",
        partial_filename="legacy.webm",
        selected_final_filename="legacy.webm",
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO materialized_jobs (
                job_id,
                source_kind,
                queue_collection_id,
                priority,
                order_key,
                scheduled_for_us,
                authorized,
                manual_hold,
                start_now_requested,
                category,
                destination_collection,
                partial_filename,
                selected_final_filename,
                expected_revision
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (materialized.job_id, *SQLiteStore._projection_values(materialized)),
        )
        connection.execute(
            """
            INSERT INTO publication_reservations (
                job_id, target_component, final_filename, claim_token
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                materialized.job_id,
                materialized.destination_collection,
                materialized.selected_final_filename,
                "c" * 64,
            ),
        )

    store = SQLiteStore(database_path)
    try:
        assert store.get_publication_reservation(materialized.job_id) == models_module.PublicationReservation(
            job_id=materialized.job_id,
            target_component="Legacy collection",
            final_filename=materialized.selected_final_filename,
            claim_token="c" * 64,
        )
        assert store.get_publication_marker_binding(materialized.job_id) is None
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert connection.execute(
            "SELECT job_id, marker_device, marker_inode FROM publication_marker_bindings"
        ).fetchall() == []
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V16_TABLE_SCHEMAS


def test_v11_migration_ddl_failure_leaves_the_exact_v10_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v10_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_marker_binding_ddl_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="CREATE TABLE publication_marker_bindings",
        )
        return failed_connection

    monkeypatch.setattr(
        store_module.sqlite3, "connect", connect_with_marker_binding_ddl_failure
    )

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 10
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V10_TABLE_SCHEMAS
    assert "publication_marker_bindings" not in table_schemas


def test_fresh_v11_bootstrap_retries_after_marker_binding_ddl_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_marker_binding_ddl_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="CREATE TABLE publication_marker_bindings",
        )
        return failed_connection

    monkeypatch.setattr(
        store_module.sqlite3, "connect", connect_with_marker_binding_ddl_failure
    )

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert version == 0
    assert table_schemas == {}

    monkeypatch.setattr(store_module.sqlite3, "connect", original_connect)
    recovered = SQLiteStore(database_path)
    try:
        assert recovered._connection.execute("PRAGMA user_version").fetchone()[0] == 16
    finally:
        recovered.close()

    with original_connect(database_path) as connection:
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V16_TABLE_SCHEMAS


def test_v15_rejects_unknown_current_table_without_bootstrap_writes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    store.close()
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        connection.execute("CREATE TABLE unexpected_v12_table (value TEXT NOT NULL)")

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
    assert _table_names(database_path) == {
        *store_module._V16_TABLE_SCHEMAS,
        "unexpected_v12_table",
    }


@pytest.mark.parametrize(
    ("column", "blob_value"),
    (
        ("job_id", sqlite3.Binary(b"job-1")),
        ("marker_device", sqlite3.Binary(b"901")),
        ("marker_inode", sqlite3.Binary(b"902")),
    ),
)
def test_publication_marker_binding_schema_rejects_blob_insert_and_update(
    tmp_path: Path, column: str, blob_value: object
) -> None:
    materialized = _materialized_job()
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        values: dict[str, object] = {
            "job_id": materialized.job_id,
            "marker_device": 901,
            "marker_inode": 902,
        }
        values[column] = blob_value
        store._connection.execute("PRAGMA foreign_keys = OFF")
        try:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
                store._connection.execute(
                    """
                    INSERT INTO publication_marker_bindings (
                        job_id, marker_device, marker_inode
                    )
                    VALUES (:job_id, :marker_device, :marker_inode)
                    """,
                    values,
                )
        finally:
            store._connection.execute("PRAGMA foreign_keys = ON")
        assert store._connection.execute(
            "SELECT COUNT(*) FROM publication_marker_bindings"
        ).fetchone()[0] == 0

        reservation = store.get_publication_reservation(materialized.job_id)
        assert reservation is not None
        binding = store.bind_publication_marker(
            materialized.job_id,
            claim_token=reservation.claim_token,
            marker_device=901,
            marker_inode=902,
        )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            store._connection.execute(
                f"UPDATE publication_marker_bindings SET {column} = ? WHERE job_id = ?",
                (blob_value, materialized.job_id),
            )
        assert store.get_publication_marker_binding(materialized.job_id) == binding
    finally:
        store.close()


def _create_v11_database(database_path: Path) -> tuple[object, ...]:
    legacy_job = _create_v10_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._PUBLICATION_MARKER_BINDINGS_SCHEMA)
        connection.execute("PRAGMA user_version = 11")
    return legacy_job


def _recovery_capability(*, rpc_port: int = 43123) -> Any:
    return store_module._DirectEngineRecoveryCapability(
        rpc_port=rpc_port,
        rpc_secret=secrets.token_urlsafe(32),
    )


def test_v12_migrates_v11_direct_record_without_backfilling_a_recovery_capability(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v11_database(database_path)
    record = store_module.DirectEngineRecord(
        worker_epoch=1, identity=_process_birth_identity()
    )
    with sqlite3.connect(database_path) as connection:
        identity = record.identity
        connection.execute(
            """
            INSERT INTO engine_instances (
                engine_kind,
                worker_epoch,
                leader_pid,
                process_group_id,
                session_id,
                owner_uid,
                started_unix_us,
                argv_sha256
            )
            VALUES ('direct', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.worker_epoch,
                identity.leader_pid,
                identity.process_group_id,
                identity.session_id,
                identity.owner_uid,
                identity.started_unix_us,
                identity.argv_sha256,
            ),
        )

    store = SQLiteStore(database_path)
    try:
        assert store.get_direct_engine_record() == record
        assert store._get_direct_engine_recovery_capability(record) is None
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert connection.execute(
            "SELECT COUNT(*) FROM direct_engine_recovery_capabilities"
        ).fetchone()[0] == 0
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V16_TABLE_SCHEMAS


def test_v12_recovery_capability_migration_rolls_back_and_retries_from_v11(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v11_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_recovery_capability_ddl_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="CREATE TABLE direct_engine_recovery_capabilities",
        )
        return failed_connection

    monkeypatch.setattr(
        store_module.sqlite3, "connect", connect_with_recovery_capability_ddl_failure
    )
    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 11
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V11_TABLE_SCHEMAS
    assert "direct_engine_recovery_capabilities" not in table_schemas

    monkeypatch.setattr(store_module.sqlite3, "connect", original_connect)
    recovered = SQLiteStore(database_path)
    try:
        assert recovered._connection.execute("PRAGMA user_version").fetchone()[0] == 16
    finally:
        recovered.close()


def test_private_direct_recovery_capability_is_exactly_bound_and_compare_cleared(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    record = store_module.DirectEngineRecord(
        worker_epoch=1, identity=_process_birth_identity()
    )
    other_record = replace(
        record,
        identity=_process_birth_identity(
            leader_pid=4343,
            process_group_id=4343,
            session_id=4343,
            argv_sha256="b" * 64,
        ),
    )
    capability = _recovery_capability()
    different_capability = _recovery_capability(rpc_port=43124)
    try:
        assert store.recover_cold_start() == 1
        store.set_direct_engine_record(record)
        store._bind_direct_engine_recovery_capability(record, capability)

        assert store._get_direct_engine_recovery_capability(record) == capability
        assert store._clear_direct_engine_record_and_recovery_capability(
            other_record, capability
        ) is False
        assert store._clear_direct_engine_record_and_recovery_capability(
            record, different_capability
        ) is False
        assert store.get_direct_engine_record() == record
        assert store._get_direct_engine_recovery_capability(record) == capability

        assert store._clear_direct_engine_record_and_recovery_capability(
            record, capability
        ) is True
        assert store.get_direct_engine_record() is None
        assert store._get_direct_engine_recovery_capability(record) is None
    finally:
        store.close()


def test_private_direct_recovery_capability_rejects_raw_types_malformed_values_and_record_mismatch(
    tmp_path: Path,
) -> None:
    mutations: tuple[tuple[str, tuple[object, ...]], ...] = (
        (
            "UPDATE direct_engine_recovery_capabilities SET rpc_port = ?",
            (sqlite3.Binary(b"43123"),),
        ),
        (
            "UPDATE direct_engine_recovery_capabilities SET rpc_secret = ?",
            (sqlite3.Binary(b"opaque"),),
        ),
        (
            "UPDATE direct_engine_recovery_capabilities SET rpc_secret = ?",
            ("malformed",),
        ),
        (
            """
            UPDATE direct_engine_recovery_capabilities
            SET leader_pid = ?, process_group_id = ?, session_id = ?
            """,
            (4343, 4343, 4343),
        ),
    )
    for index, (statement, values) in enumerate(mutations):
        store = SQLiteStore(tmp_path / f"queue-{index}.sqlite3")
        record = store_module.DirectEngineRecord(
            worker_epoch=1, identity=_process_birth_identity()
        )
        capability = _recovery_capability(rpc_port=43123 + index)
        try:
            assert store.recover_cold_start() == 1
            store.set_direct_engine_record(record)
            store._bind_direct_engine_recovery_capability(record, capability)
            store._connection.execute("PRAGMA ignore_check_constraints = ON")
            try:
                store._connection.execute(statement, values)
            finally:
                store._connection.execute("PRAGMA ignore_check_constraints = OFF")

            with pytest.raises(ValueError, match="recovery capability") as raised:
                store._get_direct_engine_recovery_capability(record)
            if capability.rpc_secret in str(raised.value) or capability.rpc_secret in repr(
                raised.value
            ):
                pytest.fail("store rejection exposed a private capability")
            assert store.get_direct_engine_record() == record
        finally:
            store.close()


def test_private_direct_recovery_capability_rejects_wrong_api_input_types(
    tmp_path: Path,
) -> None:
    valid_secret = secrets.token_urlsafe(32)
    for rpc_port, rpc_secret in (
        ("43123", valid_secret),
        (True, valid_secret),
        (43123, b"not-a-text-secret"),
        (43123, "malformed"),
    ):
        with pytest.raises(ValueError):
            store_module._DirectEngineRecoveryCapability(
                rpc_port=rpc_port,  # type: ignore[arg-type]
                rpc_secret=rpc_secret,  # type: ignore[arg-type]
            )

    store = SQLiteStore(tmp_path / "queue.sqlite3")
    record = store_module.DirectEngineRecord(
        worker_epoch=1, identity=_process_birth_identity()
    )
    invalid: Any = object()
    try:
        assert store.recover_cold_start() == 1
        store.set_direct_engine_record(record)
        with pytest.raises(TypeError, match="capability"):
            store._bind_direct_engine_recovery_capability(record, invalid)
        with pytest.raises(TypeError, match="record"):
            store._get_direct_engine_recovery_capability(invalid)
        with pytest.raises(TypeError, match="capability"):
            store._clear_direct_engine_record_and_recovery_capability(record, invalid)
        assert store.get_direct_engine_record() == record
        assert store._get_direct_engine_recovery_capability(record) is None
    finally:
        store.close()


def test_private_direct_recovery_capability_is_absent_from_public_store_models(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    record = store_module.DirectEngineRecord(
        worker_epoch=1, identity=_process_birth_identity()
    )
    capability = _recovery_capability()
    try:
        assert store.recover_cold_start() == 1
        store.set_direct_engine_record(record)
        store._bind_direct_engine_recovery_capability(record, capability)

        assert "_DirectEngineRecoveryCapability" not in store_module.__all__
        assert tuple(store_module.DirectEngineRecord.__dataclass_fields__) == (
            "worker_epoch",
            "identity",
        )
        if capability.rpc_secret in repr(capability):
            pytest.fail("private capability representation exposed its secret")
        assert store.list_jobs() == ()
        assert store.list_events() == ()
    finally:
        store.close()


def _admit_direct_dispatch_target(store: SQLiteStore, *, job_id: str = "job-1") -> None:
    """Materialize one current-epoch direct target with every durable gate open."""

    intent = _intent(
        job_id=job_id,
        request_id=f"{job_id}-add",
        payload_digest=("b" if job_id == "job-1" else "c") * 64,
        generation=0,
        revision=0,
        expected_revision=None,
    )
    materialized = _materialized_job(
        job_id=job_id,
        intent=intent,
        source_kind=SourceKind.DIRECT,
        queue_collection_id=None,
        scheduled_for=None,
        authorized=False,
        manual_hold=False,
        start_now_requested=False,
    )
    assert store.apply_add(intent, materialized=materialized).applied is True
    assert store.recover_cold_start() == 1
    assert store.apply_queue_gate(
        gate="running",
        request_id=f"{job_id}-queue",
        payload_digest=("d" if job_id == "job-1" else "e") * 64,
        expected_revision=1,
    ).applied is True
    assert store.apply_job_control(
        job_id=job_id,
        action="start_now",
        request_id=f"{job_id}-start",
        payload_digest=("f" if job_id == "job-1" else "a") * 64,
        expected_revision=1,
    ).status == "applied"


def _seed_finalizing_direct_publication(
    store: SQLiteStore, *, final_bound: bool
) -> models_module.PublicationReservation:
    """Build one durable finalization chain without starting a transfer."""

    _admit_direct_dispatch_target(store)
    store._connection.execute(
        "UPDATE jobs SET revision = 3, state = 'finalizing' WHERE job_id = 'job-1'"
    )
    store._connection.execute(
        """
        INSERT INTO events (kind, job_id, generation, revision)
        VALUES ('job_finalizing', 'job-1', 1, 3)
        """
    )
    reservation = store.get_publication_reservation("job-1")
    assert reservation is not None
    store.bind_publication_marker(
        "job-1",
        claim_token=reservation.claim_token,
        marker_device=901,
        marker_inode=902,
    )
    store._bind_staged_payload(
        "job-1",
        claim_token=reservation.claim_token,
        partial_device=903,
        partial_inode=904,
        logical_size=905,
    )
    if final_bound:
        store._bind_final_publication(
            "job-1",
            claim_token=reservation.claim_token,
            final_device=903,
            final_inode=904,
            logical_size=905,
        )
    return reservation


@pytest.mark.parametrize("final_bound", (False, True))
def test_cold_finalization_pauses_and_fences_before_explicit_reconciliation(
    tmp_path: Path, final_bound: bool
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        _seed_finalizing_direct_publication(store, final_bound=final_bound)
        store._connection.execute(
            "UPDATE materialized_jobs SET manual_hold = 1 WHERE job_id = 'job-1'"
        )
        pending = store.prepare_direct_dispatch(
            job_id="job-1", expected_worker_epoch=1, expected_generation=1,
            expected_revision=3, request_id="old-pending", payload_digest="9" * 64,
            controller_ready=False, now=datetime(2032, 1, 2, tzinfo=UTC),
        )
        assert type(pending) is store_module._DirectPublicationReconciliationPlan
        before = store.get_materialized_job("job-1")
        reservation = store.get_publication_reservation("job-1")
        marker = store._read_publication_marker_binding(store._connection, "job-1")
        staged = store._read_staged_payload_binding(store._connection, "job-1")
        assert store.recover_cold_start() == 2
        job = store.get_job("job-1")
        assert job is not None
        assert (job.state, job.generation, job.revision) == ("paused", 2, 4)
        after = store.get_materialized_job("job-1")
        assert before is not None and after is not None
        assert after == replace(before, intent=replace(before.intent, generation=2, revision=4))
        assert store.get_publication_reservation("job-1") == reservation
        assert store._read_publication_marker_binding(store._connection, "job-1") == marker
        assert store._read_staged_payload_binding(store._connection, "job-1") == staged
        assert store.queue_gate() == "paused"
        assert [e.kind for e in store.list_events()][-1] == "job_paused"
        old = store.prepare_direct_dispatch(
            job_id="job-1", expected_worker_epoch=1, expected_generation=1,
            expected_revision=3, request_id="old-pending", payload_digest="9" * 64,
            controller_ready=False, now=datetime(2032, 1, 2, tzinfo=UTC),
        )
        assert old.status == "blocked"
        with pytest.raises(ValueError, match="pending"):
            store.complete_direct_publication_reconciliation(
                pending, final_device=903, final_inode=904, logical_size=905,
            )
        for epoch, generation, revision in ((1, 2, 4), (2, 1, 4), (2, 2, 3)):
            stale = store.prepare_direct_dispatch(
                job_id="job-1", expected_worker_epoch=epoch,
                expected_generation=generation, expected_revision=revision,
                request_id=f"stale-{epoch}-{generation}-{revision}",
                payload_digest="a" * 64, controller_ready=False,
                now=datetime(2032, 1, 2, tzinfo=UTC),
            )
            assert stale.status == "stale"
        prepared = store.prepare_direct_dispatch(
            job_id="job-1", expected_worker_epoch=2, expected_generation=2,
            expected_revision=4, request_id="cold-reconcile", payload_digest="8" * 64,
            controller_ready=False, now=datetime(2032, 1, 2, tzinfo=UTC),
        )
        assert type(prepared) is store_module._DirectPublicationReconciliationPlan
        result = store.complete_direct_publication_reconciliation(
            prepared, final_device=903, final_inode=904, logical_size=905,
        )
        assert (result.state, result.generation, result.revision) == ("completed", 2, 5)
        replay = store.prepare_direct_dispatch(
            job_id="job-1", expected_worker_epoch=2, expected_generation=2,
            expected_revision=4, request_id="cold-reconcile", payload_digest="8" * 64,
            controller_ready=False, now=datetime(2032, 1, 2, tzinfo=UTC),
        )
        assert replay == result
        assert [e.kind for e in store.list_events()].count("job_completed") == 1
        assert store.get_materialized_job("job-1").manual_hold is True
        assert store.queue_gate() == "paused"
    finally:
        store.close()


@pytest.mark.parametrize("damage", ("missing", "irrelevant", "intervening", "repeat-cold"))
def test_cold_reconciliation_requires_immediate_exact_finalizing_audit(
    tmp_path: Path, damage: str
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        _seed_finalizing_direct_publication(store, final_bound=False)
        store.recover_cold_start()
        if damage == "missing":
            store._connection.execute("DELETE FROM events WHERE kind = 'job_finalizing'")
        elif damage == "irrelevant":
            store._connection.execute("UPDATE events SET generation = 0 WHERE kind = 'job_finalizing'")
        elif damage == "intervening":
            store._connection.execute(
                "INSERT INTO events (kind, job_id, generation, revision) VALUES ('job_paused', 'job-1', 2, 4)"
            )
        else:
            store.recover_cold_start()
        job = store.get_job("job-1")
        assert job is not None
        result = store.prepare_direct_dispatch(
            job_id="job-1", expected_worker_epoch=store.worker_epoch(),
            expected_generation=job.generation, expected_revision=job.revision,
            request_id="unproven-cutpoint", payload_digest="b" * 64,
            controller_ready=False, now=datetime(2032, 1, 2, tzinfo=UTC),
        )
        assert result.status == "blocked"
        assert store._get_final_publication_binding("job-1") is None
        assert "job_completed" not in [e.kind for e in store.list_events()]
    finally:
        store.close()


@pytest.mark.parametrize("final_bound", (False, True))
def test_direct_dispatch_reconciles_a_durable_final_publication_and_replays_exactly(
    tmp_path: Path, final_bound: bool
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        reservation = _seed_finalizing_direct_publication(store, final_bound=final_bound)
        prepared = store.prepare_direct_dispatch(
            job_id="job-1",
            expected_worker_epoch=1,
            expected_generation=1,
            expected_revision=3,
            request_id="reconcile-final-publication",
            payload_digest="6" * 64,
            controller_ready=False,
            now=datetime(2032, 1, 2, tzinfo=UTC),
        )

        assert type(prepared) is store_module._DirectPublicationReconciliationPlan
        assert (
            store.prepare_direct_dispatch(
                job_id="job-1",
                expected_worker_epoch=1,
                expected_generation=1,
                expected_revision=3,
                request_id="reconcile-final-publication",
                payload_digest="6" * 64,
                controller_ready=False,
                now=datetime(2032, 1, 2, tzinfo=UTC),
            )
            == prepared
        )
        completed = store.complete_direct_publication_reconciliation(
            prepared,
            final_device=903,
            final_inode=904,
            logical_size=905,
        )

        assert completed == store_module.DirectDispatchResult(
            status="started",
            job="job-1",
            generation=1,
            revision=4,
            state="completed",
        )
        assert store.get_job("job-1") == store_module.JobRecord(
            job="job-1",
            source_url=bytes(_intent().source_url),
            generation=1,
            revision=4,
            state="completed",
        )
        assert store._get_final_publication_binding("job-1") == (
            store_module._FinalPublicationBinding(
                job_id="job-1",
                final_device=903,
                final_inode=904,
                logical_size=905,
            )
        )
        assert [event.kind for event in store.list_events()][-2:] == [
            "job_finalizing",
            "job_completed",
        ]
        assert reservation.claim_token not in repr(store.list_job_page())
        assert reservation.claim_token not in repr(store.list_events())
        assert reservation.claim_token not in repr(completed)

        assert (
            store.prepare_direct_dispatch(
                job_id="job-1",
                expected_worker_epoch=1,
                expected_generation=1,
                expected_revision=3,
                request_id="reconcile-final-publication",
                payload_digest="6" * 64,
                controller_ready=False,
                now=datetime(2032, 1, 2, tzinfo=UTC),
            )
            == completed
        )
        assert [event.kind for event in store.list_events()].count("job_completed") == 1
    finally:
        store.close()


def test_direct_dispatch_blocks_a_finalizing_chain_without_its_terminal_audit(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        _seed_finalizing_direct_publication(store, final_bound=False)
        store._connection.execute(
            "DELETE FROM events WHERE kind = 'job_finalizing' AND job_id = 'job-1'"
        )

        assert store.prepare_direct_dispatch(
            job_id="job-1",
            expected_worker_epoch=1,
            expected_generation=1,
            expected_revision=3,
            request_id="reconcile-missing-finalizing-audit",
            payload_digest="e" * 64,
            controller_ready=False,
            now=datetime(2032, 1, 2, tzinfo=UTC),
        ) == store_module.DirectDispatchResult(
            status="blocked",
            job="job-1",
            generation=1,
            revision=3,
            state="finalizing",
        )
        assert store._get_final_publication_binding("job-1") is None
        assert "job_completed" not in [event.kind for event in store.list_events()]
    finally:
        store.close()


@pytest.mark.parametrize("cold", (False, True))
def test_direct_publication_reconciliation_rolls_back_mismatch_and_audit_failure(
    tmp_path: Path, cold: bool,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        _seed_finalizing_direct_publication(store, final_bound=False)
        if cold:
            store.recover_cold_start()
        prepared = store.prepare_direct_dispatch(
            job_id="job-1",
            expected_worker_epoch=2 if cold else 1,
            expected_generation=2 if cold else 1,
            expected_revision=4 if cold else 3,
            request_id="reconcile-rejects-mismatch",
            payload_digest="7" * 64,
            controller_ready=False,
            now=datetime(2032, 1, 2, tzinfo=UTC),
        )
        assert type(prepared) is store_module._DirectPublicationReconciliationPlan

        with pytest.raises(ValueError, match="final publication"):
            store.complete_direct_publication_reconciliation(
                prepared,
                final_device=903,
                final_inode=999,
                logical_size=905,
            )
        assert store.get_job("job-1").state == ("paused" if cold else "finalizing")  # type: ignore[union-attr]
        assert store._get_final_publication_binding("job-1") is None
        assert "job_completed" not in [event.kind for event in store.list_events()]

        store._connection.execute(
            """
            CREATE TRIGGER fail_completed_audit
            BEFORE INSERT ON events
            WHEN NEW.kind = 'job_completed'
            BEGIN SELECT RAISE(ABORT, 'injected completed audit failure'); END
            """
        )
        with pytest.raises(sqlite3.IntegrityError, match="injected completed audit failure"):
            store.complete_direct_publication_reconciliation(
                prepared,
                final_device=903,
                final_inode=904,
                logical_size=905,
            )
        assert store.get_job("job-1").state == ("paused" if cold else "finalizing")  # type: ignore[union-attr]
        assert store._get_final_publication_binding("job-1") is None
        assert "job_completed" not in [event.kind for event in store.list_events()]
    finally:
        store.close()


def test_direct_dispatch_is_fenced_idempotent_and_allows_only_one_active_body(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        _admit_direct_dispatch_target(store)
        now = datetime(2032, 1, 2, tzinfo=UTC)

        stale = store.prepare_direct_dispatch(
            job_id="job-1",
            expected_worker_epoch=2,
            expected_generation=1,
            expected_revision=2,
            request_id="dispatch-stale",
            payload_digest="1" * 64,
            controller_ready=True,
            now=now,
        )
        assert stale == store_module.DirectDispatchResult(
            status="stale", job="job-1", generation=1, revision=2, state="queued"
        )

        blocked = store.prepare_direct_dispatch(
            job_id="job-1",
            expected_worker_epoch=1,
            expected_generation=1,
            expected_revision=2,
            request_id="dispatch-no-controller",
            payload_digest="2" * 64,
            controller_ready=False,
            now=now,
        )
        assert blocked == store_module.DirectDispatchResult(
            status="blocked", job="job-1", generation=1, revision=2, state="queued"
        )
        assert (
            store.prepare_direct_dispatch(
                job_id="job-1",
                expected_worker_epoch=1,
                expected_generation=1,
                expected_revision=2,
                request_id="dispatch-no-controller",
                payload_digest="2" * 64,
                controller_ready=False,
                now=now,
            )
            == blocked
        )
        with pytest.raises(RequestConflictError):
            store.prepare_direct_dispatch(
                job_id="job-1",
                expected_worker_epoch=1,
                expected_generation=1,
                expected_revision=3,
                request_id="dispatch-no-controller",
                payload_digest="3" * 64,
                controller_ready=False,
                now=now,
            )

        prepared = store.prepare_direct_dispatch(
            job_id="job-1",
            expected_worker_epoch=1,
            expected_generation=1,
            expected_revision=2,
            request_id="dispatch-started",
            payload_digest="4" * 64,
            controller_ready=True,
            now=now,
        )
        assert type(prepared) is store_module._DirectDispatchPlan
        assert store.get_job("job-1") == store_module.JobRecord(
            job="job-1",
            source_url=bytes(_intent().source_url),
            generation=1,
            revision=3,
            state="resolving",
        )
        downloading = store.advance_direct_dispatch_to_downloading(prepared)
        started = store.finish_direct_dispatch(downloading)
        assert started == store_module.DirectDispatchResult(
            status="started",
            job="job-1",
            generation=1,
            revision=4,
            state="downloading",
        )
        assert (
            store.prepare_direct_dispatch(
                job_id="job-1",
                expected_worker_epoch=1,
                expected_generation=1,
                expected_revision=2,
                request_id="dispatch-started",
                payload_digest="4" * 64,
                controller_ready=False,
                now=now,
            )
            == started
        )

        second_intent = _intent(
            job_id="job-2",
            request_id="job-2-add",
            payload_digest="8" * 64,
            generation=0,
            revision=0,
            expected_revision=None,
        )
        second_materialized = _materialized_job(
            job_id="job-2",
            intent=second_intent,
            source_kind=SourceKind.DIRECT,
            queue_collection_id=None,
            scheduled_for=None,
            authorized=False,
            manual_hold=False,
            start_now_requested=False,
            selected_final_filename="selected--job-2.webm",
        )
        assert store.apply_add(second_intent, materialized=second_materialized).applied
        assert store.apply_job_control(
            job_id="job-2",
            action="start_now",
            request_id="job-2-start",
            payload_digest="9" * 64,
            expected_revision=0,
        ).status == "applied"
        second = store.prepare_direct_dispatch(
            job_id="job-2",
            expected_worker_epoch=1,
            expected_generation=0,
            expected_revision=1,
            request_id="dispatch-second",
            payload_digest="5" * 64,
            controller_ready=True,
            now=now,
        )
        assert second == store_module.DirectDispatchResult(
            status="blocked", job="job-2", generation=0, revision=1, state="queued"
        )
    finally:
        store.close()


def test_direct_dispatch_prepare_rolls_back_if_its_pending_receipt_cannot_persist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        _admit_direct_dispatch_target(store)

        def fail_insert(*_args: object, **_kwargs: object) -> None:
            raise sqlite3.OperationalError("injected direct dispatch receipt failure")

        monkeypatch.setattr(
            SQLiteStore, "_insert_direct_dispatch_command", staticmethod(fail_insert)
        )
        with pytest.raises(sqlite3.OperationalError, match="injected direct dispatch"):
            store.prepare_direct_dispatch(
                job_id="job-1",
                expected_worker_epoch=1,
                expected_generation=1,
                expected_revision=2,
                request_id="dispatch-rollback",
                payload_digest="6" * 64,
                controller_ready=True,
                now=datetime(2032, 1, 2, tzinfo=UTC),
            )
        assert store.get_job("job-1") == store_module.JobRecord(
            job="job-1",
            source_url=bytes(_intent().source_url),
            generation=1,
            revision=2,
            state="queued",
        )
        assert store._connection.execute(
            "SELECT request_id FROM direct_dispatch_commands"
        ).fetchall() == []
    finally:
        store.close()


@pytest.mark.parametrize("cutpoint", ("resolving", "downloading", "started"))
def test_direct_dispatch_cold_recovery_never_resumes_a_cutpoint(
    tmp_path: Path, cutpoint: str
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        _admit_direct_dispatch_target(store)
        prepared = store.prepare_direct_dispatch(
            job_id="job-1",
            expected_worker_epoch=1,
            expected_generation=1,
            expected_revision=2,
            request_id="dispatch-cutpoint",
            payload_digest="7" * 64,
            controller_ready=True,
            now=datetime(2032, 1, 2, tzinfo=UTC),
        )
        assert type(prepared) is store_module._DirectDispatchPlan
        plan = prepared
        if cutpoint in {"downloading", "started"}:
            plan = store.advance_direct_dispatch_to_downloading(plan)
        if cutpoint == "started":
            assert store.finish_direct_dispatch(plan).status == "started"
    finally:
        store.close()

    recovered = SQLiteStore(database_path)
    try:
        assert recovered.recover_cold_start() == 2
        job = recovered.get_job("job-1")
        assert job is not None
        assert job.state == "paused"
        assert recovered.queue_gate() == "paused"
        replay = recovered.prepare_direct_dispatch(
            job_id="job-1",
            expected_worker_epoch=1,
            expected_generation=1,
            expected_revision=2,
            request_id="dispatch-cutpoint",
            payload_digest="7" * 64,
            controller_ready=True,
            now=datetime(2032, 1, 2, tzinfo=UTC),
        )
        if cutpoint == "started":
            assert replay == store_module.DirectDispatchResult(
                status="started",
                job="job-1",
                generation=1,
                revision=4,
                state="downloading",
            )
        else:
            assert replay == store_module.DirectDispatchResult(
                status="blocked",
                job="job-1",
                generation=2,
                revision=4 if cutpoint == "resolving" else 5,
                state="paused",
            )
        paused = recovered.get_job("job-1")
        assert paused is not None
        assert paused.state == "paused"
    finally:
        recovered.close()


def _create_v13_database(database_path: Path) -> tuple[object, ...]:
    legacy_job = _create_v11_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._DIRECT_ENGINE_RECOVERY_CAPABILITIES_SCHEMA)
        connection.execute(store_module._DIRECT_DISPATCH_COMMANDS_SCHEMA)
        connection.execute("PRAGMA user_version = 13")
    return legacy_job


def _seed_v13_materialized_marker_chain(
    database_path: Path,
) -> tuple[MaterializedJob, str]:
    legacy_job = _create_v13_database(database_path)
    source_url = legacy_job[1]
    assert type(source_url) is bytes
    materialized = MaterializedJob(
        job_id="legacy-job",
        intent=DownloadIntent(
            job_id="legacy-job",
            request_id="legacy-request",
            payload_digest="b" * 64,
            source_url=source_url,
            generation=23,
            revision=41,
        ),
        source_kind=SourceKind.DIRECT,
        queue_collection_id="legacy-queue",
        priority=9,
        order_key=17,
        scheduled_for=None,
        authorized=True,
        manual_hold=False,
        start_now_requested=False,
        category="Videos",
        destination_collection="Legacy collection",
        partial_filename="legacy.webm",
        selected_final_filename="legacy--legacy-job.webm",
    )
    claim_token = "c" * 64
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO materialized_jobs (
                job_id,
                source_kind,
                queue_collection_id,
                priority,
                order_key,
                scheduled_for_us,
                authorized,
                manual_hold,
                start_now_requested,
                category,
                destination_collection,
                partial_filename,
                selected_final_filename,
                expected_revision
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (materialized.job_id, *SQLiteStore._projection_values(materialized)),
        )
        connection.execute(
            """
            INSERT INTO publication_reservations (
                job_id, target_component, final_filename, claim_token
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                materialized.job_id,
                materialized.destination_collection,
                materialized.selected_final_filename,
                claim_token,
            ),
        )
        connection.execute(
            """
            INSERT INTO publication_marker_bindings (job_id, marker_device, marker_inode)
            VALUES (?, 901, 902)
            """,
            (materialized.job_id,),
        )
    return materialized, claim_token


def _materialize_marker_for_staged_payload(
    store: SQLiteStore,
) -> tuple[MaterializedJob, models_module.PublicationReservation]:
    materialized = _materialized_job()
    assert store.apply_add(materialized.intent, materialized=materialized).applied is True
    reservation = store.get_publication_reservation(materialized.job_id)
    assert reservation is not None
    assert store.bind_publication_marker(
        materialized.job_id,
        claim_token=reservation.claim_token,
        marker_device=901,
        marker_inode=902,
    ) == store_module.PublicationMarkerBinding(
        job_id=materialized.job_id,
        marker_device=901,
        marker_inode=902,
    )
    return materialized, reservation


def test_v14_migrates_v13_owner_chain_without_backfilling_a_staged_payload_binding(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    materialized, claim_token = _seed_v13_materialized_marker_chain(database_path)

    store = SQLiteStore(database_path)
    try:
        assert store.get_materialized_job(materialized.job_id) == materialized
        assert store.get_publication_reservation(materialized.job_id) == models_module.PublicationReservation(
            job_id=materialized.job_id,
            target_component="Legacy collection",
            final_filename=materialized.selected_final_filename,
            claim_token=claim_token,
        )
        assert store.get_publication_marker_binding(materialized.job_id) == (
            store_module.PublicationMarkerBinding(
                job_id=materialized.job_id,
                marker_device=901,
                marker_inode=902,
            )
        )
        assert store._get_staged_payload_binding(materialized.job_id) is None
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert connection.execute(
            """
            SELECT job_id, partial_device, partial_inode, logical_size
            FROM staged_payload_bindings
            """
        ).fetchall() == []
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V16_TABLE_SCHEMAS


def test_v14_migration_rolls_back_and_retries_from_v13(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v13_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_staged_payload_ddl_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="CREATE TABLE staged_payload_bindings",
        )
        return failed_connection

    monkeypatch.setattr(
        store_module.sqlite3, "connect", connect_with_staged_payload_ddl_failure
    )
    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 13
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V13_TABLE_SCHEMAS
    assert "staged_payload_bindings" not in table_schemas

    monkeypatch.setattr(store_module.sqlite3, "connect", original_connect)
    recovered = SQLiteStore(database_path)
    try:
        assert recovered._connection.execute("PRAGMA user_version").fetchone()[0] == 16
    finally:
        recovered.close()


def test_fresh_v14_bootstrap_retries_after_staged_payload_ddl_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_staged_payload_ddl_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix="CREATE TABLE staged_payload_bindings",
        )
        return failed_connection

    monkeypatch.setattr(
        store_module.sqlite3, "connect", connect_with_staged_payload_ddl_failure
    )
    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 0
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == {}

    monkeypatch.setattr(store_module.sqlite3, "connect", original_connect)
    recovered = SQLiteStore(database_path)
    try:
        assert recovered._connection.execute("PRAGMA user_version").fetchone()[0] == 16
    finally:
        recovered.close()

    with original_connect(database_path) as connection:
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V16_TABLE_SCHEMAS


def test_private_staged_payload_binding_is_exactly_idempotent_and_durable(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        public_before = (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_jobs(),
            store.list_job_page(),
            store.list_events(),
        )

        first = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        repeated = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )

        assert tuple(store_module._StagedPayloadBinding.__dataclass_fields__) == (
            "job_id",
            "partial_device",
            "partial_inode",
            "logical_size",
        )
        assert first == store_module._StagedPayloadBinding(
            job_id=materialized.job_id,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        assert repeated == first
        assert store._get_staged_payload_binding(materialized.job_id) == first
        assert not hasattr(first, "claim_token")
        assert reservation.claim_token not in repr(first)
        assert "_StagedPayloadBinding" not in store_module.__all__
        assert not hasattr(store, "get_staged_payload_binding")
        assert public_before == (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_jobs(),
            store.list_job_page(),
            store.list_events(),
        )
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert [
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(staged_payload_bindings)"
            ).fetchall()
        ] == ["job_id", "partial_device", "partial_inode", "logical_size"]
        assert connection.execute(
            """
            SELECT job_id, partial_device, partial_inode, logical_size
            FROM staged_payload_bindings
            """
        ).fetchall() == [(materialized.job_id, 903, 904, 905)]

    reopened = SQLiteStore(database_path)
    try:
        assert reopened._get_staged_payload_binding(materialized.job_id) == first
        assert reopened._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        ) == first
    finally:
        reopened.close()


def test_private_staged_payload_binding_rejects_conflicts_and_never_leaks_claim_tokens(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        original = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        wrong_token = "0" * 64 if reservation.claim_token != "0" * 64 else "1" * 64
        before = store._connection.execute(
            """
            SELECT job_id, partial_device, partial_inode, logical_size
            FROM staged_payload_bindings
            WHERE job_id = ?
            """,
            (materialized.job_id,),
        ).fetchall()

        for claim_token, partial_device, partial_inode, logical_size in (
            (wrong_token, 903, 904, 905),
            (reservation.claim_token, 906, 904, 905),
            (reservation.claim_token, 903, 907, 905),
            (reservation.claim_token, 903, 904, 908),
        ):
            with pytest.raises(ValueError) as raised:
                store._bind_staged_payload(
                    materialized.job_id,
                    claim_token=claim_token,
                    partial_device=partial_device,
                    partial_inode=partial_inode,
                    logical_size=logical_size,
                )
            assert reservation.claim_token not in str(raised.value)
            assert reservation.claim_token not in repr(raised.value)
            assert wrong_token not in str(raised.value)
            assert wrong_token not in repr(raised.value)

        assert store._get_staged_payload_binding(materialized.job_id) == original
        assert store._connection.execute(
            """
            SELECT job_id, partial_device, partial_inode, logical_size
            FROM staged_payload_bindings
            WHERE job_id = ?
            """,
            (materialized.job_id,),
        ).fetchall() == before
    finally:
        store.close()


def test_private_staged_payload_binding_requires_an_intact_reservation_marker_chain(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized = _materialized_job()
        assert store.apply_add(materialized.intent, materialized=materialized).applied is True
        reservation = store.get_publication_reservation(materialized.job_id)
        assert reservation is not None

        with pytest.raises(ValueError, match="marker"):
            store._bind_staged_payload(
                materialized.job_id,
                claim_token=reservation.claim_token,
                partial_device=903,
                partial_inode=904,
                logical_size=905,
            )
        assert store._connection.execute(
            "SELECT COUNT(*) FROM staged_payload_bindings"
        ).fetchone()[0] == 0

        store.bind_publication_marker(
            materialized.job_id,
            claim_token=reservation.claim_token,
            marker_device=901,
            marker_inode=902,
        )
        store._connection.execute(
            """
            UPDATE publication_reservations
            SET target_component = 'Other collection'
            WHERE job_id = ?
            """,
            (materialized.job_id,),
        )
        with pytest.raises(ValueError, match="publication reservation") as raised:
            store._bind_staged_payload(
                materialized.job_id,
                claim_token=reservation.claim_token,
                partial_device=903,
                partial_inode=904,
                logical_size=905,
            )
        assert reservation.claim_token not in str(raised.value)
        assert reservation.claim_token not in repr(raised.value)
        assert store._connection.execute(
            "SELECT COUNT(*) FROM staged_payload_bindings"
        ).fetchone()[0] == 0
    finally:
        store.close()


def test_private_staged_payload_binding_rejects_an_orphaned_marker_chain_without_mutation(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        binding = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        store._connection.execute("PRAGMA foreign_keys = OFF")
        try:
            store._connection.execute(
                "DELETE FROM publication_marker_bindings WHERE job_id = ?",
                (materialized.job_id,),
            )
        finally:
            store._connection.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(ValueError, match="staged payload binding"):
            store._get_staged_payload_binding(materialized.job_id)
        with pytest.raises(ValueError, match="marker"):
            store._bind_staged_payload(
                materialized.job_id,
                claim_token=reservation.claim_token,
                partial_device=903,
                partial_inode=904,
                logical_size=905,
            )
        assert [
            tuple(row)
            for row in store._connection.execute(
                """
                SELECT job_id, partial_device, partial_inode, logical_size
                FROM staged_payload_bindings
                """
            ).fetchall()
        ] == [
            (
                binding.job_id,
                binding.partial_device,
                binding.partial_inode,
                binding.logical_size,
            )
        ]
    finally:
        store.close()


@pytest.mark.parametrize(
    ("column", "blob_value"),
    (
        ("job_id", sqlite3.Binary(b"job-1")),
        ("partial_device", sqlite3.Binary(b"903")),
        ("partial_inode", sqlite3.Binary(b"904")),
        ("logical_size", sqlite3.Binary(b"905")),
    ),
)
def test_staged_payload_binding_schema_rejects_blob_insert_and_update(
    tmp_path: Path, column: str, blob_value: object
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        values: dict[str, object] = {
            "job_id": materialized.job_id,
            "partial_device": 903,
            "partial_inode": 904,
            "logical_size": 905,
        }
        values[column] = blob_value
        store._connection.execute("PRAGMA foreign_keys = OFF")
        try:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
                store._connection.execute(
                    """
                    INSERT INTO staged_payload_bindings (
                        job_id, partial_device, partial_inode, logical_size
                    )
                    VALUES (:job_id, :partial_device, :partial_inode, :logical_size)
                    """,
                    values,
                )
        finally:
            store._connection.execute("PRAGMA foreign_keys = ON")
        assert store._connection.execute(
            "SELECT COUNT(*) FROM staged_payload_bindings"
        ).fetchone()[0] == 0

        binding = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            store._connection.execute(
                f"UPDATE staged_payload_bindings SET {column} = ? WHERE job_id = ?",
                (blob_value, materialized.job_id),
            )
        assert store._get_staged_payload_binding(materialized.job_id) == binding
    finally:
        store.close()


@pytest.mark.parametrize(
    ("partial_device", "partial_inode", "logical_size"),
    (
        (-1, 904, 905),
        (903, -1, 905),
        (903, 904, -1),
        (store_module._MAX_COUNTER + 1, 904, 905),
        (903, store_module._MAX_COUNTER + 1, 905),
        (903, 904, store_module._MAX_COUNTER + 1),
        (True, 904, 905),
        (903, False, 905),
        (903, 904, True),
        ("903", 904, 905),
    ),
)
def test_private_staged_payload_binding_rejects_invalid_identity_without_mutation(
    tmp_path: Path,
    partial_device: object,
    partial_inode: object,
    logical_size: object,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        with pytest.raises((TypeError, ValueError)):
            store._bind_staged_payload(
                materialized.job_id,
                claim_token=reservation.claim_token,
                partial_device=partial_device,
                partial_inode=partial_inode,
                logical_size=logical_size,
            )
        assert store._get_staged_payload_binding(materialized.job_id) is None
    finally:
        store.close()


@pytest.mark.parametrize(
    ("statement", "values", "disable_foreign_keys"),
    (
        (
            "UPDATE staged_payload_bindings SET job_id = ?",
            (sqlite3.Binary(b"job-1"),),
            True,
        ),
        (
            "UPDATE staged_payload_bindings SET partial_device = ?",
            (sqlite3.Binary(b"903"),),
            False,
        ),
        (
            "UPDATE staged_payload_bindings SET partial_inode = ?",
            ("not-an-integer",),
            False,
        ),
        (
            "UPDATE staged_payload_bindings SET logical_size = ?",
            (-1,),
            False,
        ),
    ),
)
def test_private_staged_payload_binding_getter_rejects_corrupt_rows_without_token_leak(
    tmp_path: Path,
    statement: str,
    values: tuple[object, ...],
    disable_foreign_keys: bool,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        if disable_foreign_keys:
            store._connection.execute("PRAGMA foreign_keys = OFF")
        store._connection.execute("PRAGMA ignore_check_constraints = ON")
        try:
            store._connection.execute(statement, values)
        finally:
            store._connection.execute("PRAGMA ignore_check_constraints = OFF")
            if disable_foreign_keys:
                store._connection.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(ValueError, match="staged payload binding") as raised:
            store._get_staged_payload_binding(materialized.job_id)
        assert reservation.claim_token not in str(raised.value)
        assert reservation.claim_token not in repr(raised.value)
        assert store._connection.execute(
            "SELECT COUNT(*) FROM staged_payload_bindings"
        ).fetchone()[0] == 1
    finally:
        store.close()


def test_private_staged_payload_binding_insert_failure_rolls_back_and_retries(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        _install_failing_insert_trigger(
            database_path,
            table="staged_payload_bindings",
            trigger_name="fail_staged_payload_binding_insert",
            message="injected staged payload binding insert failure",
        )

        with pytest.raises(
            sqlite3.DatabaseError, match="injected staged payload binding insert failure"
        ):
            store._bind_staged_payload(
                materialized.job_id,
                claim_token=reservation.claim_token,
                partial_device=903,
                partial_inode=904,
                logical_size=905,
            )

        assert store._get_staged_payload_binding(materialized.job_id) is None
        store._connection.execute("DROP TRIGGER fail_staged_payload_binding_insert")
        assert store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        ) == store_module._StagedPayloadBinding(
            job_id=materialized.job_id,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
    finally:
        store.close()


def test_private_final_publication_binding_is_exactly_idempotent_and_durable(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        staged = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        public_before = (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_jobs(),
            store.list_job_page(),
            store.list_events(),
        )

        first = store._bind_final_publication(
            materialized.job_id,
            claim_token=reservation.claim_token,
            final_device=staged.partial_device,
            final_inode=staged.partial_inode,
            logical_size=staged.logical_size,
        )
        repeated = store._bind_final_publication(
            materialized.job_id,
            claim_token=reservation.claim_token,
            final_device=staged.partial_device,
            final_inode=staged.partial_inode,
            logical_size=staged.logical_size,
        )

        assert tuple(store_module._FinalPublicationBinding.__dataclass_fields__) == (
            "job_id",
            "final_device",
            "final_inode",
            "logical_size",
        )
        assert first == store_module._FinalPublicationBinding(
            job_id=materialized.job_id,
            final_device=903,
            final_inode=904,
            logical_size=905,
        )
        assert repeated == first
        assert store._get_final_publication_binding(materialized.job_id) == first
        assert not hasattr(first, "claim_token")
        assert reservation.claim_token not in repr(first)
        assert "_FinalPublicationBinding" not in store_module.__all__
        assert not hasattr(store, "get_final_publication_binding")
        assert public_before == (
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_jobs(),
            store.list_job_page(),
            store.list_events(),
        )
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert [
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(final_publication_bindings)"
            ).fetchall()
        ] == ["job_id", "final_device", "final_inode", "logical_size"]
        assert connection.execute(
            """
            SELECT job_id, final_device, final_inode, logical_size
            FROM final_publication_bindings
            """
        ).fetchall() == [(materialized.job_id, 903, 904, 905)]

    reopened = SQLiteStore(database_path)
    try:
        assert reopened._get_final_publication_binding(materialized.job_id) == first
        assert reopened._bind_final_publication(
            materialized.job_id,
            claim_token=reservation.claim_token,
            final_device=903,
            final_inode=904,
            logical_size=905,
        ) == first
    finally:
        reopened.close()


def _create_v14_database(database_path: Path) -> tuple[object, ...]:
    legacy_job = _create_v13_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._STAGED_PAYLOAD_BINDINGS_SCHEMA)
        connection.execute("PRAGMA user_version = 14")
    return legacy_job


def _seed_v14_staged_payload_chain(
    database_path: Path,
) -> tuple[MaterializedJob, str]:
    materialized, claim_token = _seed_v13_materialized_marker_chain(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(store_module._STAGED_PAYLOAD_BINDINGS_SCHEMA)
        connection.execute(
            """
            INSERT INTO staged_payload_bindings (
                job_id, partial_device, partial_inode, logical_size
            )
            VALUES (?, 903, 904, 905)
            """,
            (materialized.job_id,),
        )
        connection.execute("PRAGMA user_version = 14")
    return materialized, claim_token


def test_v15_migrates_v14_owner_chain_without_backfilling_final_publication_binding(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    materialized, claim_token = _seed_v14_staged_payload_chain(database_path)

    store = SQLiteStore(database_path)
    try:
        assert store.get_materialized_job(materialized.job_id) == materialized
        assert store.get_publication_reservation(materialized.job_id) == models_module.PublicationReservation(
            job_id=materialized.job_id,
            target_component="Legacy collection",
            final_filename=materialized.selected_final_filename,
            claim_token=claim_token,
        )
        assert store.get_publication_marker_binding(materialized.job_id) == (
            store_module.PublicationMarkerBinding(
                job_id=materialized.job_id,
                marker_device=901,
                marker_inode=902,
            )
        )
        assert store._get_staged_payload_binding(materialized.job_id) == (
            store_module._StagedPayloadBinding(
                job_id=materialized.job_id,
                partial_device=903,
                partial_inode=904,
                logical_size=905,
            )
        )
        assert store._get_final_publication_binding(materialized.job_id) is None
    finally:
        store.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert connection.execute(
            """
            SELECT job_id, final_device, final_inode, logical_size
            FROM final_publication_bindings
            """
        ).fetchall() == []
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V16_TABLE_SCHEMAS


@pytest.mark.parametrize(
    "failure_statement_prefix",
    (
        "CREATE TABLE final_publication_bindings",
        "PRAGMA user_version = 15",
    ),
)
def test_v15_migration_rolls_back_and_retries_from_v14(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_statement_prefix: str,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v14_database(database_path)
    original_connect = sqlite3.connect
    failed_connection: _MigrationFailureConnection | None = None

    def connect_with_final_publication_ddl_failure(*args: Any, **kwargs: Any) -> Any:
        nonlocal failed_connection
        failed_connection = _MigrationFailureConnection(
            original_connect(*args, **kwargs),
            failure_statement_prefix=failure_statement_prefix,
        )
        return failed_connection

    monkeypatch.setattr(
        store_module.sqlite3, "connect", connect_with_final_publication_ddl_failure
    )
    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        SQLiteStore(database_path)

    assert failed_connection is not None
    assert failed_connection.closed is True
    with original_connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 14
        table_schemas = {
            row[0]: store_module._normalize_table_schema(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert table_schemas == store_module._V14_TABLE_SCHEMAS
    assert "final_publication_bindings" not in table_schemas

    monkeypatch.setattr(store_module.sqlite3, "connect", original_connect)
    recovered = SQLiteStore(database_path)
    try:
        assert recovered._connection.execute("PRAGMA user_version").fetchone()[0] == 16
    finally:
        recovered.close()


def test_v15_rejects_malformed_final_publication_schema_without_bootstrap_writes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    _create_v14_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "CREATE TABLE final_publication_bindings (job_id TEXT PRIMARY KEY)"
        )
        connection.execute("PRAGMA user_version = 15")

    with pytest.raises(RuntimeError, match="incomplete"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 15
        assert connection.execute(
            "PRAGMA table_info(final_publication_bindings)"
        ).fetchall() == [(0, "job_id", "TEXT", 0, None, 1)]
    assert _table_names(database_path) == {
        *store_module._V14_TABLE_SCHEMAS,
        "final_publication_bindings",
    }


def test_private_final_publication_binding_rejects_conflicts_without_mutation(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        staged = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        original = store._bind_final_publication(
            materialized.job_id,
            claim_token=reservation.claim_token,
            final_device=staged.partial_device,
            final_inode=staged.partial_inode,
            logical_size=staged.logical_size,
        )
        wrong_token = "0" * 64 if reservation.claim_token != "0" * 64 else "1" * 64
        before = (
            store._connection.execute(
                """
                SELECT job_id, final_device, final_inode, logical_size
                FROM final_publication_bindings
                WHERE job_id = ?
                """,
                (materialized.job_id,),
            ).fetchall(),
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_events(),
        )

        for claim_token, final_device, final_inode, logical_size in (
            (wrong_token, 903, 904, 905),
            (reservation.claim_token, 906, 904, 905),
            (reservation.claim_token, 903, 907, 905),
            (reservation.claim_token, 903, 904, 908),
        ):
            with pytest.raises(ValueError) as raised:
                store._bind_final_publication(
                    materialized.job_id,
                    claim_token=claim_token,
                    final_device=final_device,
                    final_inode=final_inode,
                    logical_size=logical_size,
                )
            assert reservation.claim_token not in str(raised.value)
            assert reservation.claim_token not in repr(raised.value)
            assert wrong_token not in str(raised.value)
            assert wrong_token not in repr(raised.value)

        assert store._get_final_publication_binding(materialized.job_id) == original
        assert (
            store._connection.execute(
                """
                SELECT job_id, final_device, final_inode, logical_size
                FROM final_publication_bindings
                WHERE job_id = ?
                """,
                (materialized.job_id,),
            ).fetchall(),
            store.get_job(materialized.job_id),
            store.get_materialized_job(materialized.job_id),
            store.list_events(),
        ) == before
    finally:
        store.close()


def test_private_final_publication_binding_requires_an_intact_staged_owner_chain(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)

        with pytest.raises(ValueError, match="staged payload"):
            store._bind_final_publication(
                materialized.job_id,
                claim_token=reservation.claim_token,
                final_device=903,
                final_inode=904,
                logical_size=905,
            )
        assert store._connection.execute(
            "SELECT COUNT(*) FROM final_publication_bindings"
        ).fetchone()[0] == 0

        staged = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        binding = store._bind_final_publication(
            materialized.job_id,
            claim_token=reservation.claim_token,
            final_device=staged.partial_device,
            final_inode=staged.partial_inode,
            logical_size=staged.logical_size,
        )
        store._connection.execute("PRAGMA foreign_keys = OFF")
        try:
            store._connection.execute(
                "DELETE FROM publication_marker_bindings WHERE job_id = ?",
                (materialized.job_id,),
            )
        finally:
            store._connection.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(ValueError, match="marker"):
            store._get_final_publication_binding(materialized.job_id)
        with pytest.raises(ValueError, match="marker"):
            store._bind_final_publication(
                materialized.job_id,
                claim_token=reservation.claim_token,
                final_device=binding.final_device,
                final_inode=binding.final_inode,
                logical_size=binding.logical_size,
            )
        assert [
            tuple(row)
            for row in store._connection.execute(
                """
                SELECT job_id, final_device, final_inode, logical_size
                FROM final_publication_bindings
                """
            ).fetchall()
        ] == [
            (
                binding.job_id,
                binding.final_device,
                binding.final_inode,
                binding.logical_size,
            )
        ]
    finally:
        store.close()


@pytest.mark.parametrize(
    ("corrupt_owner", "expected_error"),
    (
        (
            "DELETE FROM materialized_jobs WHERE job_id = ?",
            "materialized job",
        ),
        (
            "UPDATE publication_reservations SET target_component = 'Other collection' WHERE job_id = ?",
            "publication reservation",
        ),
    ),
)
def test_private_final_publication_binding_requires_an_intact_materialized_reservation_chain(
    tmp_path: Path,
    corrupt_owner: str,
    expected_error: str,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        staged = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        binding = store._bind_final_publication(
            materialized.job_id,
            claim_token=reservation.claim_token,
            final_device=staged.partial_device,
            final_inode=staged.partial_inode,
            logical_size=staged.logical_size,
        )
        store._connection.execute("PRAGMA foreign_keys = OFF")
        try:
            store._connection.execute(corrupt_owner, (materialized.job_id,))
        finally:
            store._connection.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(ValueError, match=expected_error):
            store._get_final_publication_binding(materialized.job_id)
        with pytest.raises(ValueError, match=expected_error):
            store._bind_final_publication(
                materialized.job_id,
                claim_token=reservation.claim_token,
                final_device=binding.final_device,
                final_inode=binding.final_inode,
                logical_size=binding.logical_size,
            )
        assert [
            tuple(row)
            for row in store._connection.execute(
                """
                SELECT job_id, final_device, final_inode, logical_size
                FROM final_publication_bindings
                WHERE job_id = ?
                """,
                (materialized.job_id,),
            ).fetchall()
        ] == [
            (
                binding.job_id,
                binding.final_device,
                binding.final_inode,
                binding.logical_size,
            )
        ]
    finally:
        store.close()


@pytest.mark.parametrize(
    ("column", "blob_value"),
    (
        ("job_id", sqlite3.Binary(b"job-1")),
        ("final_device", sqlite3.Binary(b"903")),
        ("final_inode", sqlite3.Binary(b"904")),
        ("logical_size", sqlite3.Binary(b"905")),
    ),
)
def test_final_publication_binding_schema_rejects_blob_insert_and_update(
    tmp_path: Path, column: str, blob_value: object
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        staged = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        values: dict[str, object] = {
            "job_id": materialized.job_id,
            "final_device": staged.partial_device,
            "final_inode": staged.partial_inode,
            "logical_size": staged.logical_size,
        }
        values[column] = blob_value
        store._connection.execute("PRAGMA foreign_keys = OFF")
        try:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
                store._connection.execute(
                    """
                    INSERT INTO final_publication_bindings (
                        job_id, final_device, final_inode, logical_size
                    )
                    VALUES (:job_id, :final_device, :final_inode, :logical_size)
                    """,
                    values,
                )
        finally:
            store._connection.execute("PRAGMA foreign_keys = ON")
        assert store._connection.execute(
            "SELECT COUNT(*) FROM final_publication_bindings"
        ).fetchone()[0] == 0

        binding = store._bind_final_publication(
            materialized.job_id,
            claim_token=reservation.claim_token,
            final_device=staged.partial_device,
            final_inode=staged.partial_inode,
            logical_size=staged.logical_size,
        )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            store._connection.execute(
                f"UPDATE final_publication_bindings SET {column} = ? WHERE job_id = ?",
                (blob_value, materialized.job_id),
            )
        assert store._get_final_publication_binding(materialized.job_id) == binding
    finally:
        store.close()


@pytest.mark.parametrize(
    ("statement", "values", "disable_foreign_keys"),
    (
        (
            "UPDATE final_publication_bindings SET job_id = ?",
            (sqlite3.Binary(b"job-1"),),
            True,
        ),
        (
            "UPDATE final_publication_bindings SET final_device = ?",
            (sqlite3.Binary(b"903"),),
            False,
        ),
        (
            "UPDATE final_publication_bindings SET final_inode = ?",
            ("not-an-integer",),
            False,
        ),
        (
            "UPDATE final_publication_bindings SET logical_size = ?",
            (-1,),
            False,
        ),
    ),
)
def test_private_final_publication_binding_getter_rejects_corrupt_rows_without_token_leak(
    tmp_path: Path,
    statement: str,
    values: tuple[object, ...],
    disable_foreign_keys: bool,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        staged = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        store._bind_final_publication(
            materialized.job_id,
            claim_token=reservation.claim_token,
            final_device=staged.partial_device,
            final_inode=staged.partial_inode,
            logical_size=staged.logical_size,
        )
        if disable_foreign_keys:
            store._connection.execute("PRAGMA foreign_keys = OFF")
        store._connection.execute("PRAGMA ignore_check_constraints = ON")
        try:
            store._connection.execute(statement, values)
        finally:
            store._connection.execute("PRAGMA ignore_check_constraints = OFF")
            if disable_foreign_keys:
                store._connection.execute("PRAGMA foreign_keys = ON")

        with pytest.raises(ValueError, match="final publication binding") as raised:
            store._get_final_publication_binding(materialized.job_id)
        assert reservation.claim_token not in str(raised.value)
        assert reservation.claim_token not in repr(raised.value)
        assert store._connection.execute(
            "SELECT COUNT(*) FROM final_publication_bindings"
        ).fetchone()[0] == 1
    finally:
        store.close()


def test_private_final_publication_binding_rejects_staged_identity_drift_without_mutation(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        staged = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        binding = store._bind_final_publication(
            materialized.job_id,
            claim_token=reservation.claim_token,
            final_device=staged.partial_device,
            final_inode=staged.partial_inode,
            logical_size=staged.logical_size,
        )
        store._connection.execute(
            "UPDATE staged_payload_bindings SET partial_device = 906 WHERE job_id = ?",
            (materialized.job_id,),
        )

        with pytest.raises(ValueError, match="does not match staged payload"):
            store._get_final_publication_binding(materialized.job_id)
        with pytest.raises(ValueError, match="does not match staged payload"):
            store._bind_final_publication(
                materialized.job_id,
                claim_token=reservation.claim_token,
                final_device=binding.final_device,
                final_inode=binding.final_inode,
                logical_size=binding.logical_size,
            )
        assert [
            tuple(row)
            for row in store._connection.execute(
                """
                SELECT job_id, final_device, final_inode, logical_size
                FROM final_publication_bindings
                WHERE job_id = ?
                """,
                (materialized.job_id,),
            ).fetchall()
        ] == [
            (
                binding.job_id,
                binding.final_device,
                binding.final_inode,
                binding.logical_size,
            )
        ]
    finally:
        store.close()


@pytest.mark.parametrize(
    ("final_device", "final_inode", "logical_size"),
    (
        (-1, 904, 905),
        (903, -1, 905),
        (903, 904, -1),
        (store_module._MAX_COUNTER + 1, 904, 905),
        (903, store_module._MAX_COUNTER + 1, 905),
        (903, 904, store_module._MAX_COUNTER + 1),
        (True, 904, 905),
        (903, False, 905),
        (903, 904, True),
        ("903", 904, 905),
    ),
)
def test_private_final_publication_binding_rejects_invalid_identity_without_mutation(
    tmp_path: Path,
    final_device: object,
    final_inode: object,
    logical_size: object,
) -> None:
    store = SQLiteStore(tmp_path / "queue.sqlite3")
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        with pytest.raises((TypeError, ValueError)):
            store._bind_final_publication(
                materialized.job_id,
                claim_token=reservation.claim_token,
                final_device=final_device,
                final_inode=final_inode,
                logical_size=logical_size,
            )
        with pytest.raises(TypeError):
            store._bind_final_publication(
                materialized.job_id,
                claim_token=b"not-a-token",  # type: ignore[arg-type]
                final_device=903,
                final_inode=904,
                logical_size=905,
            )
        assert store._get_final_publication_binding(materialized.job_id) is None
    finally:
        store.close()


def test_private_final_publication_binding_insert_failure_rolls_back_and_retries(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "queue.sqlite3"
    store = SQLiteStore(database_path)
    try:
        materialized, reservation = _materialize_marker_for_staged_payload(store)
        staged = store._bind_staged_payload(
            materialized.job_id,
            claim_token=reservation.claim_token,
            partial_device=903,
            partial_inode=904,
            logical_size=905,
        )
        _install_failing_insert_trigger(
            database_path,
            table="final_publication_bindings",
            trigger_name="fail_final_publication_binding_insert",
            message="injected final publication binding insert failure",
        )

        with pytest.raises(
            sqlite3.DatabaseError, match="injected final publication binding insert failure"
        ):
            store._bind_final_publication(
                materialized.job_id,
                claim_token=reservation.claim_token,
                final_device=staged.partial_device,
                final_inode=staged.partial_inode,
                logical_size=staged.logical_size,
            )

        assert store._get_final_publication_binding(materialized.job_id) is None
        store._connection.execute("DROP TRIGGER fail_final_publication_binding_insert")
        assert store._bind_final_publication(
            materialized.job_id,
            claim_token=reservation.claim_token,
            final_device=staged.partial_device,
            final_inode=staged.partial_inode,
            logical_size=staged.logical_size,
        ) == store_module._FinalPublicationBinding(
            job_id=materialized.job_id,
            final_device=903,
            final_inode=904,
            logical_size=905,
        )
    finally:
        store.close()


def _started_terminal_fixture(store):
    from hermes_downloads.direct import DirectTransfer
    _admit_direct_dispatch_target(store)
    plan = store.prepare_direct_dispatch(job_id="job-1", expected_worker_epoch=1,
        expected_generation=1, expected_revision=2, request_id="terminal-start",
        payload_digest="1" * 64, controller_ready=True, now=datetime(2032, 1, 2, tzinfo=UTC))
    marker = store.bind_publication_marker("job-1", claim_token=plan.reservation.claim_token,
        marker_device=901, marker_inode=902)
    plan = store.advance_direct_dispatch_to_downloading(plan)
    started = store.finish_direct_dispatch(plan)
    record = store_module.DirectEngineRecord(worker_epoch=1, identity=_process_birth_identity())
    store.set_direct_engine_record(record)
    store._bind_direct_engine_recovery_capability(record, _recovery_capability())
    path = Path("/private/synthetic/job-1") / plan.job.partial_filename
    terminal = store_module._DirectTerminalPlan(dispatch=plan, record=record, marker=marker,
        gid="0123456789abcdef", partial_path=path)
    observed = DirectTransfer(job_id="job-1", generation=1, gid=terminal.gid, status="complete",
        total_length=4, completed_length=4, partial_path=path, hash_verified=False,
        verification=retry_module.CompletionVerification.TRANSPORT_VERIFIED)
    return terminal, observed, started


def test_direct_terminal_transaction_finalizes_once_keeps_started_receipt(tmp_path):
    store = SQLiteStore(tmp_path / "state.db")
    try:
        terminal, observed, started = _started_terminal_fixture(store)
        before = store.get_materialized_job("job-1")
        receipt = tuple(store._connection.execute("SELECT * FROM direct_dispatch_commands").fetchone())
        finalizing = store.finalize_direct_terminal(terminal, observed)
        assert (finalizing.state, finalizing.generation, finalizing.revision) == ("finalizing", 1, 5)
        assert store.get_materialized_job("job-1") == replace(before, intent=replace(before.intent, revision=5))
        assert tuple(store._connection.execute("SELECT * FROM direct_dispatch_commands").fetchone()) == receipt
        assert store.prepare_direct_dispatch(job_id="job-1", expected_worker_epoch=1,
            expected_generation=1, expected_revision=2, request_id="terminal-start",
            payload_digest="1" * 64, controller_ready=False, now=datetime(2032, 1, 2, tzinfo=UTC)) == started
        assert store._get_staged_payload_binding("job-1") is None
        assert store._get_final_publication_binding("job-1") is None
        with pytest.raises(ValueError):
            store.finalize_direct_terminal(terminal, observed)
        events = store.list_events()
        assert [e.kind for e in events].count("job_finalizing") == 1
        assert (events[-1].generation, events[-1].revision) == (1, 5)
    finally:
        store.close()


@pytest.mark.parametrize("damage", ("epoch", "revision", "generation", "receipt", "record", "owner", "reservation", "marker", "job", "gid", "mapping", "verification", "status", "length", "path", "plan"))
def test_direct_terminal_transaction_rejects_stale_or_unverified_without_writes(tmp_path, damage):
    store = SQLiteStore(tmp_path / "state.db")
    try:
        terminal, observed, _ = _started_terminal_fixture(store)
        sql = {
            "epoch": "UPDATE settings SET value = '2' WHERE key = 'worker_epoch'",
            "revision": "UPDATE jobs SET revision = 9 WHERE job_id = 'job-1'",
            "generation": "UPDATE jobs SET generation = 2 WHERE job_id = 'job-1'",
            "receipt": "UPDATE direct_dispatch_commands SET revision = 9",
            "record": "UPDATE engine_instances SET started_unix_us = started_unix_us + 1",
            "owner": "UPDATE materialized_jobs SET partial_filename = 'changed.bin'",
            "reservation": "UPDATE publication_reservations SET final_filename = 'changed.bin'",
            "marker": "DELETE FROM publication_marker_bindings",
        }
        if damage in sql:
            store._connection.execute(sql[damage])
        elif damage == "plan":
            terminal = replace(terminal, dispatch=replace(terminal.dispatch, payload_digest="2" * 64))
        elif damage == "mapping":
            terminal = replace(terminal, gid="ffffffffffffffff")
        else:
            updates = {"job": {"job_id": "other"}, "gid": {"gid": "ffffffffffffffff"},
                "verification": {"verification": None}, "status": {"status": "active"},
                "length": {"completed_length": 3}, "path": {"partial_path": Path("/wrong")}}
            observed = replace(observed, **updates[damage])
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises((TypeError, ValueError)):
            store.finalize_direct_terminal(terminal, observed)
        assert tuple(store._connection.iterdump()) == snapshot
    finally:
        store.close()


def test_direct_terminal_audit_failure_rolls_back_revision_and_receipt(tmp_path):
    store = SQLiteStore(tmp_path / "state.db")
    try:
        terminal, observed, _ = _started_terminal_fixture(store)
        store._connection.execute("CREATE TRIGGER fail_terminal BEFORE INSERT ON events WHEN NEW.kind = 'job_finalizing' BEGIN SELECT RAISE(ABORT, 'terminal audit failure'); END")
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(sqlite3.IntegrityError, match="terminal audit failure"):
            store.finalize_direct_terminal(terminal, observed)
        assert tuple(store._connection.iterdump()) == snapshot
    finally:
        store.close()


def _stage_producer_fixture(store):
    from hermes_downloads import direct, paths
    assert hasattr(store, "prepare_direct_stage"), "missing fenced stage plan producer"
    terminal, observed, started = _started_terminal_fixture(store)
    job = terminal.dispatch.job
    root = Path.home() / "Downloads" / "Hermes"
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    destination = paths.rehydrate_destination(category=job.category,
        collection=job.destination_collection, partial_filename=job.partial_filename,
        selected_final_filename=job.selected_final_filename, job_id=job.job_id)
    paths.prepare_persisted_destination_workspace(destination)
    marker = paths.attest_publication_reservation_marker(destination, terminal.dispatch.reservation)
    store._connection.execute("UPDATE publication_marker_bindings SET marker_device = ?, marker_inode = ?",
        (marker.st_dev, marker.st_ino))
    terminal = replace(terminal, marker=store.get_publication_marker_binding(job.job_id), partial_path=destination.partial_path,
        capability=store._get_direct_engine_recovery_capability(terminal.record))
    destination.partial_path.write_bytes(b"body")
    details = destination.partial_path.stat()
    observed = replace(observed, partial_path=destination.partial_path,
        verified_identity=direct._VerifiedPayloadIdentity(details.st_dev, details.st_ino, details.st_size, details.st_mtime_ns,
            details.st_mode, details.st_nlink, details.st_ctime_ns))
    store.finalize_direct_terminal(terminal, observed)
    plan = store.prepare_direct_stage(terminal, observed)
    staged = paths.attest_staged_partial_payload(destination, terminal.dispatch.reservation)
    return plan, staged, destination, started


@pytest.mark.parametrize("rewrite_at", ("before-attestation", "after-attestation"))
def test_stage_producer_rejects_same_inode_rewrite_restored_mtime(tmp_path, rewrite_at):
    """Use the real checksum verifier, attester and fenced transactional binder."""
    from hermes_downloads import direct, paths
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        terminal, observed, started = _started_terminal_fixture(store)
        job = terminal.dispatch.job
        (Path.home() / "Downloads/Hermes").mkdir(parents=True, mode=0o700, exist_ok=True)
        destination = paths.rehydrate_destination(category=job.category,
            collection=job.destination_collection, partial_filename=job.partial_filename,
            selected_final_filename=job.selected_final_filename, job_id=job.job_id)
        paths.prepare_persisted_destination_workspace(destination)
        marker = paths.attest_publication_reservation_marker(destination, terminal.dispatch.reservation)
        store._connection.execute("UPDATE publication_marker_bindings SET marker_device = ?, marker_inode = ?",
            (marker.st_dev, marker.st_ino))
        terminal = replace(terminal, marker=store.get_publication_marker_binding(job.job_id),
            partial_path=destination.partial_path,
            capability=store._get_direct_engine_recovery_capability(terminal.record))
        destination.partial_path.write_bytes(b"body")
        controller = object.__new__(direct.DirectAria2Controller)
        controller._observation_cancelled = threading.Event()
        observed = controller._verify_completed_output(
            replace(observed, partial_path=destination.partial_path), SimpleNamespace(
                destination=destination, job_id=job.job_id,
                expected_sha256=hashlib.sha256(b"body").hexdigest()))
        assert observed.hash_verified
        assert observed.verification is retry_module.CompletionVerification.CHECKSUM_VERIFIED
        before = destination.partial_path.stat()
        store.finalize_direct_terminal(terminal, observed)
        plan = store.prepare_direct_stage(terminal, observed)
        if rewrite_at == "after-attestation":
            staged = paths.attest_staged_partial_payload(destination, terminal.dispatch.reservation)
        with destination.partial_path.open("r+b") as payload:
            payload.write(b"evil")
            payload.flush()
            os.fsync(payload.fileno())
        os.utime(destination.partial_path, ns=(before.st_atime_ns, before.st_mtime_ns))
        after = destination.partial_path.stat()
        assert (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) == (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        assert before.st_ctime_ns != after.st_ctime_ns
        assert hashlib.sha256(destination.partial_path.read_bytes()).hexdigest() != hashlib.sha256(b"body").hexdigest()
        if rewrite_at == "before-attestation":
            staged = paths.attest_staged_partial_payload(destination, terminal.dispatch.reservation)
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises((ValueError, TypeError, paths.PathValidationError)):
            store.bind_direct_staged_payload(plan, staged)
        assert tuple(store._connection.iterdump()) == snapshot
        assert store._get_staged_payload_binding(job.job_id) is None
        assert store.get_job(job.job_id).state == "finalizing"
        assert destination.partial_path.read_bytes() == b"evil"
        assert (destination.incomplete_dir / ".hermes-reservation").exists()
        assert not destination.final_path.exists()
        assert store.prepare_direct_dispatch(job_id=job.job_id, expected_worker_epoch=1,
            expected_generation=1, expected_revision=2, request_id="terminal-start",
            payload_digest="1" * 64, controller_ready=False, now=datetime(2032, 1, 2, tzinfo=UTC)) == started


def test_stage_producer_rejects_rewrite_between_fresh_bind_checks(tmp_path, monkeypatch):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        plan, staged, destination, _ = _stage_producer_fixture(store)
        before = destination.partial_path.stat()
        original_bind = store._bind_staged_payload_in_transaction
        def rewrite_after_insert(*args, **kwargs):
            result = original_bind(*args, **kwargs)
            with destination.partial_path.open("r+b") as payload:
                payload.write(b"evil")
                payload.flush()
                os.fsync(payload.fileno())
            os.utime(destination.partial_path, ns=(before.st_atime_ns, before.st_mtime_ns))
            return result
        monkeypatch.setattr(store, "_bind_staged_payload_in_transaction", rewrite_after_insert)
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(paths.PathValidationError):
            store.bind_direct_staged_payload(plan, staged)
        assert tuple(store._connection.iterdump()) == snapshot
        assert store._get_staged_payload_binding(plan.job.job_id) is None
        assert destination.partial_path.read_bytes() == b"evil"
        assert (destination.incomplete_dir / ".hermes-reservation").exists()
        assert not destination.final_path.exists()


def test_stage_producer_binding_exact_replay_preserves_lifecycle_and_receipt(tmp_path):
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        plan, staged, destination, started = _stage_producer_fixture(store)
        receipt = tuple(store._connection.execute("SELECT * FROM direct_dispatch_commands").fetchone())
        events = store.list_events()
        binding = store.bind_direct_staged_payload(plan, staged)
        assert (binding.partial_device, binding.partial_inode, binding.logical_size) == (staged.st_dev, staged.st_ino, 4)
        snapshot = tuple(store._connection.iterdump())
        assert store.bind_direct_staged_payload(plan, staged) == binding
        assert tuple(store._connection.iterdump()) == snapshot
        assert store.list_events() == events
        assert store.get_job("job-1").state == "finalizing"
        assert tuple(store._connection.execute("SELECT * FROM direct_dispatch_commands").fetchone()) == receipt
        assert not destination.final_path.exists()
        assert store._get_final_publication_binding("job-1") is None
        assert store.prepare_direct_dispatch(job_id="job-1", expected_worker_epoch=1,
            expected_generation=1, expected_revision=2, request_id="terminal-start",
            payload_digest="1" * 64, controller_ready=False, now=datetime(2032, 1, 2, tzinfo=UTC)) == started


@pytest.mark.parametrize("damage", ("epoch", "revision", "generation", "receipt", "record", "capability", "capability-change", "missing-receipt", "marker-content", "malformed-identity", "owner", "reservation", "marker", "audit", "missing-audit", "paused", "removed", "inode", "mtime", "size", "symlink", "parent", "final", "evidence-inode", "evidence-mtime", "missing-identity", "wrong-plan"))
def test_stage_producer_binding_rejects_stale_evidence_atomically(tmp_path, damage):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        plan, staged, destination, _ = _stage_producer_fixture(store)
        sql = {
            "epoch": "UPDATE settings SET value = '2' WHERE key = 'worker_epoch'",
            "revision": "UPDATE jobs SET revision = 9",
            "generation": "UPDATE jobs SET generation = 2",
            "receipt": "UPDATE direct_dispatch_commands SET revision = 9",
            "record": "UPDATE engine_instances SET started_unix_us = started_unix_us + 1",
            "capability": "DELETE FROM direct_engine_recovery_capabilities",
            "capability-change": "UPDATE direct_engine_recovery_capabilities SET rpc_port = rpc_port + 1",
            "missing-receipt": "DELETE FROM direct_dispatch_commands",
            "owner": "UPDATE materialized_jobs SET partial_filename = 'changed.bin'",
            "reservation": "UPDATE publication_reservations SET final_filename = 'changed.bin'",
            "marker": "DELETE FROM publication_marker_bindings",
            "audit": "INSERT INTO events (kind, job_id, generation, revision) VALUES ('job_finalizing', 'job-1', 1, 5)",
            "missing-audit": "DELETE FROM events WHERE kind = 'job_finalizing'",
            "paused": "UPDATE jobs SET state = 'paused'",
            "removed": "UPDATE jobs SET state = 'removed'",
        }
        if damage in sql:
            store._connection.execute(sql[damage])
        elif damage in {"inode", "symlink"}:
            retained = destination.partial_path.with_name("retained")
            destination.partial_path.rename(retained)
            if damage == "inode":
                destination.partial_path.write_bytes(b"body")
            else:
                destination.partial_path.symlink_to(retained)
        elif damage == "marker-content":
            marker = destination.incomplete_dir / ".hermes-reservation"
            payload = marker.read_bytes()
            marker.write_bytes(b"x" + payload[1:])
        elif damage == "malformed-identity":
            plan = replace(plan, observed=replace(plan.observed, verified_identity=object()))
        elif damage == "mtime":
            details = destination.partial_path.stat()
            os.utime(destination.partial_path, ns=(details.st_atime_ns, details.st_mtime_ns + 1000000))
        elif damage == "size":
            destination.partial_path.write_bytes(b"wrong-size")
        elif damage == "parent":
            retained = destination.incomplete_dir.with_name("retained-dir")
            destination.incomplete_dir.rename(retained)
            destination.incomplete_dir.symlink_to(retained, target_is_directory=True)
        elif damage == "final":
            destination.final_path.write_bytes(b"unrelated")
        elif damage == "evidence-inode":
            staged = replace(staged, st_ino=staged.st_ino + 1)
        elif damage == "evidence-mtime":
            staged = replace(staged, mtime_ns=staged.mtime_ns + 1)
        elif damage == "missing-identity":
            plan = replace(plan, observed=replace(plan.observed, verified_identity=None))
        elif damage == "wrong-plan":
            plan = replace(plan, terminal=replace(plan.terminal, gid="ffffffffffffffff"))
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises((TypeError, ValueError, paths.PathValidationError)):
            store.bind_direct_staged_payload(plan, staged)
        assert tuple(store._connection.iterdump()) == snapshot
        assert store._connection.execute("SELECT COUNT(*) FROM staged_payload_bindings").fetchone()[0] == 0
        assert destination.partial_path.exists()


def test_stage_producer_bind_insert_failure_rolls_back_and_retains_files(tmp_path):
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        plan, staged, destination, _ = _stage_producer_fixture(store)
        store._connection.execute("CREATE TRIGGER fail_stage BEFORE INSERT ON staged_payload_bindings BEGIN SELECT RAISE(ABORT, 'stage failure'); END")
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(sqlite3.IntegrityError, match="stage failure"):
            store.bind_direct_staged_payload(plan, staged)
        assert tuple(store._connection.iterdump()) == snapshot
        assert destination.partial_path.read_bytes() == b"body"
        assert (destination.incomplete_dir / ".hermes-reservation").exists()


def test_stage_producer_failure_can_pause_contained_finalizing_revision(tmp_path):
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        plan, staged, destination, _ = _stage_producer_fixture(store)
        paused = store.pause_active_direct_job(job_id="job-1", generation=1, revision=5)
        assert (paused.state, paused.revision) == ("paused", 6)
        assert store._get_staged_payload_binding("job-1") is None
        with pytest.raises(ValueError):
            store.bind_direct_staged_payload(plan, staged)
        assert destination.partial_path.read_bytes() == b"body"


def test_stage_producer_prepare_rejects_intervening_same_cutpoint_audit(tmp_path):
    with closing(SQLiteStore(tmp_path / "state.db")) as store:
        plan, _, _, _ = _stage_producer_fixture(store)
        store._connection.execute("INSERT INTO events (kind, job_id, generation, revision) VALUES ('job_finalizing', 'job-1', 1, 5)")
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(ValueError):
            store.prepare_direct_stage(plan.terminal, plan.observed)
        assert tuple(store._connection.iterdump()) == snapshot


def test_exact_publication_attempt_schema_has_no_historical_backfill(tmp_path):
    store = SQLiteStore(tmp_path / 'attempt.db')
    try:
        assert store._connection.execute('PRAGMA user_version').fetchone()[0] == 16
        assert store._connection.execute('SELECT COUNT(*) FROM direct_publication_attempts').fetchone()[0] == 0
        assert store._connection.execute('SELECT COUNT(*) FROM closed_direct_publication_attempts').fetchone()[0] == 0
        columns = {row[1] for row in store._connection.execute('PRAGMA table_info(direct_publication_attempts)')}
        assert columns == {'job_id', 'attempt_id', 'original_request_id', 'proof', 'status', 'audit_id', 'generation', 'revision', 'state', 'worker_epoch', 'pending_request_id'}
    finally:
        store.close()


def _exact_attempt_fixture(store):
    from hermes_downloads import paths
    stage, staged, destination, started = _stage_producer_fixture(store)
    store.bind_direct_staged_payload(stage, staged)
    marker = paths.PublicationReservationMarker(destination.incomplete_dir / '.hermes-reservation',
        stage.terminal.marker.marker_device, stage.terminal.marker.marker_inode)
    prepared = paths.prepare_publication_payload(destination, stage.terminal.dispatch.reservation,
        marker, staged)
    attempt = store.reserve_direct_publication(stage, prepared)
    return stage, attempt, destination, started


def _fresh_stage_after_closed_attempt(store, stage, destination, *, action='resume', request_prefix='retire'):
    """A new explicit dispatch and actual path/store stage, with no old permit."""
    from hermes_downloads import paths
    current = store.get_job('job-1')
    assert store.apply_job_control(job_id='job-1', action='pause', request_id=request_prefix+'-pause',
        payload_digest='3'*64, expected_revision=current.revision,
        _contained_direct_transfer=True, _publication_recoverable=False).status == 'applied'
    closed = tuple(store._connection.execute('SELECT * FROM direct_publication_attempts').fetchone())
    current = store.get_job('job-1')
    assert store.apply_job_control(job_id='job-1', action=action, request_id=request_prefix+'-resume',
        payload_digest='4'*64, expected_revision=current.revision).state == 'queued'
    current = store.get_job('job-1')
    plan = store.prepare_direct_dispatch(job_id='job-1', expected_worker_epoch=1,
        expected_generation=current.generation, expected_revision=current.revision,
        request_id=request_prefix+'-new-dispatch', payload_digest='5'*64, controller_ready=True,
        now=datetime(2032,1,2,tzinfo=UTC))
    plan = store.advance_direct_dispatch_to_downloading(plan)
    assert store.finish_direct_dispatch(plan).status == 'started'
    terminal = replace(stage.terminal, dispatch=plan)
    observed = replace(stage.observed, generation=plan.generation)
    store.finalize_direct_terminal(terminal, observed)
    fresh_stage = store.prepare_direct_stage(terminal, observed)
    staged = paths.attest_staged_partial_payload(destination, plan.reservation)
    store.bind_direct_staged_payload(fresh_stage, staged)
    marker = paths.PublicationReservationMarker(destination.incomplete_dir / '.hermes-reservation',
        terminal.marker.marker_device, terminal.marker.marker_inode)
    prepared = paths.prepare_publication_payload(destination, plan.reservation, marker, staged)
    return fresh_stage, prepared, closed


@pytest.mark.parametrize('action', ('resume', 'start_now'))
def test_closed_attempt_allows_new_dispatch_publication_without_rearming(tmp_path, action):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, old, destination, _ = _exact_attempt_fixture(store)
        original_receipt = tuple(store._connection.execute(
            "SELECT * FROM direct_dispatch_commands WHERE request_id='terminal-start'").fetchone())
        original_events = tuple(tuple(row) for row in store._connection.execute('SELECT * FROM events'))
        fresh, prepared, closed = _fresh_stage_after_closed_attempt(store, stage, destination, action=action)
        attempt = store.reserve_direct_publication(fresh, prepared)
        assert attempt.attempt_id != old.attempt_id
        assert tuple(store._connection.execute('SELECT * FROM closed_direct_publication_attempts').fetchone()) == closed
        assert store._read_publication_attempt(store._connection, 'job-1') == attempt
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(ValueError):
            store.complete_direct_publication(old, None)
        assert tuple(store._connection.iterdump()) == snapshot
        retired_permit = paths.PublicationCreationPermit()
        retired_permit.revoke()
        with pytest.raises(paths.PathValidationError):
            paths.publish_staged_partial_payload(destination,prepared.reservation,
                prepared.staged_payload,prepared=prepared,creation_permit=retired_permit)
        assert not destination.final_path.exists()
        published = paths.publish_staged_partial_payload(destination, prepared.reservation,
            prepared.staged_payload, prepared=prepared,
            creation_permit=paths.PublicationCreationPermit())
        assert store.complete_direct_publication(attempt, published, initial_stage=fresh).state == 'completed'
        assert destination.final_path.stat().st_ino == destination.partial_path.stat().st_ino
        assert destination.final_path.read_bytes() == b'body'
        assert tuple(store._connection.execute(
            "SELECT * FROM direct_dispatch_commands WHERE request_id='terminal-start'").fetchone()) == original_receipt
        assert tuple(tuple(row) for row in store._connection.execute('SELECT * FROM events'))[:len(original_events)] == original_events
        assert sum(event.kind == 'job_completed' for event in store.list_events()) == 1
        assert tuple(store._connection.execute('SELECT * FROM closed_direct_publication_attempts').fetchone()) == closed


@pytest.mark.parametrize('action', ('pause', 'resume', 'start_now', 'remove'))
def test_legacy_control_guard_precedes_malformed_publication_attempt(tmp_path, monkeypatch, action):
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        _exact_attempt_fixture(store)
        store._connection.execute("UPDATE materialized_jobs SET source_kind='video' WHERE job_id='job-1'")
        store._connection.execute("UPDATE direct_publication_attempts SET proof='{}' WHERE job_id='job-1'")
        store._connection.commit()
        before = tuple(store._connection.iterdump())
        def forbidden(*args):
            pytest.fail('legacy control consulted publication predecessor authority')
        monkeypatch.setattr(store, '_publication_predecessor_matches', forbidden)
        result = store.apply_job_control(job_id='job-1', action=action,
            request_id='new-legacy-control', payload_digest='e' * 64,
            expected_revision=5, _contained_direct_transfer=True)
        assert result.status == 'blocked'
        assert tuple(store._connection.iterdump()) == before


@pytest.mark.parametrize('fault', ['binding', 'audit', 'finish', 'post-check'])
def test_exact_attempt_completion_rolls_back_all_effects(tmp_path, monkeypatch, fault):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, attempt, destination, _ = _exact_attempt_fixture(store)
        receipt = tuple(store._connection.execute('SELECT * FROM direct_dispatch_commands').fetchone())
        published = paths.publish_staged_partial_payload(destination, attempt.prepared.reservation,
            attempt.prepared.staged_payload, prepared=attempt.prepared,
            creation_permit=paths.PublicationCreationPermit())
        if fault != 'post-check':
            targets = {'binding': ('final_publication_bindings','INSERT',''),
                'audit': ('events','INSERT', "WHEN NEW.kind='job_completed'"),
                'finish': ('direct_publication_attempts','UPDATE', "WHEN NEW.status='finished'")}
            table, operation, condition = targets[fault]
            store._connection.execute(f"CREATE TRIGGER fail_complete BEFORE {operation} ON {table} {condition} BEGIN SELECT RAISE(ABORT,'fault'); END")
        else:
            original = paths.require_current_publication_payload
            calls = []
            def fail_post(*args):
                original(*args)
                calls.append(1)
                if len(calls) == 2:
                    raise paths.PathValidationError('post fence')
            monkeypatch.setattr(paths, 'require_current_publication_payload', fail_post)
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises((sqlite3.IntegrityError, paths.PathValidationError)):
            store.complete_direct_publication(attempt, published, initial_stage=stage)
        assert tuple(store._connection.iterdump()) == snapshot
        assert tuple(store._connection.execute('SELECT * FROM direct_dispatch_commands').fetchone()) == receipt
        assert destination.final_path.read_bytes() == b'body'
        assert store._get_final_publication_binding(stage.job.job_id) is None


@pytest.mark.parametrize('damage', ['digest','receipt','current','epoch','source','closed','audit','stage','marker','missing-attempt'])
def test_exact_attempt_recovery_never_falls_back_on_invalid_authority(tmp_path, monkeypatch, damage):
    import json
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, attempt, _, _ = _exact_attempt_fixture(store)
        if damage == 'digest':
            proof = json.loads(attempt.proof)
            proof['sha256'] = 'bad'
            store._connection.execute('UPDATE direct_publication_attempts SET proof=?', (json.dumps(proof),))
        else:
            sql = {'receipt': 'UPDATE direct_dispatch_commands SET revision=9',
                'current': 'UPDATE jobs SET revision=9',
                'epoch': "UPDATE settings SET value='2' WHERE key='worker_epoch'",
                'source': "UPDATE materialized_jobs SET source_kind='video'",
                'closed': "UPDATE direct_publication_attempts SET status='closed'",
                'audit': "INSERT INTO events(kind,job_id,generation,revision) VALUES('job_finalizing','job-1',1,5)",
                'stage': 'UPDATE staged_payload_bindings SET logical_size=7',
                'marker': 'UPDATE publication_marker_bindings SET marker_inode=marker_inode+1',
                'missing-attempt': 'DELETE FROM direct_publication_attempts'}[damage]
            store._connection.execute(sql)
        calls = []
        monkeypatch.setattr(store, '_prepare_direct_publication_reconciliation', lambda *a, **k: calls.append(1))
        try:
            store.prepare_direct_dispatch(job_id='job-1', expected_worker_epoch=store.worker_epoch(),
                expected_generation=store.get_job('job-1').generation,
                expected_revision=store.get_job('job-1').revision, request_id='exact-recovery',
                payload_digest='3'*64, controller_ready=False, now=datetime(2032,1,2,tzinfo=UTC))
        except (ValueError, TypeError):
            pass
        assert calls == []


def test_exact_attempt_repeated_cold_successors_and_explicit_zero_link_recovery(tmp_path, monkeypatch):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, attempt, destination, started = _exact_attempt_fixture(store)
        paths.publish_staged_partial_payload(destination, attempt.prepared.reservation,
            attempt.prepared.staged_payload, prepared=attempt.prepared,
            creation_permit=paths.PublicationCreationPermit())
        receipt = tuple(store._connection.execute('SELECT * FROM direct_dispatch_commands').fetchone())
        for _ in range(3):
            store.recover_cold_start()
            assert store.get_job('job-1').state == 'paused'
        current = store.get_job('job-1')
        recovery = store.prepare_direct_dispatch(job_id='job-1', expected_worker_epoch=store.worker_epoch(),
            expected_generation=current.generation, expected_revision=current.revision,
            request_id='exact-recovery', payload_digest='3'*64, controller_ready=False,
            now=datetime(2032,1,2,tzinfo=UTC))
        monkeypatch.setattr(paths.os,'link',lambda *a,**k: pytest.fail('recovery must never link'))
        p = recovery.attempt.prepared
        published = paths.publish_staged_partial_payload(p.destination,p.reservation,p.staged_payload,
            prepared=p,existing_only=True)
        result = store.complete_direct_publication(recovery.attempt,published)
        assert (result.status,result.state) == ('started','completed')
        assert tuple(store._connection.execute("SELECT * FROM direct_dispatch_commands WHERE request_id='terminal-start'").fetchone()) == receipt
        assert sum(e.kind=='job_completed' for e in store.list_events()) == 1


@pytest.mark.parametrize('field', ['request','digest','generation','downloading_revision',
    'finalizing_revision','finalizing_audit','epoch','ownership','marker','stage','chain','sha256'])
@pytest.mark.parametrize('value', [None, True, -1, 1 << 63, [], {}])
def test_exact_attempt_strict_immutable_proof_corruption_is_atomic(tmp_path, field, value):
    import json
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        _, attempt, _, _ = _exact_attempt_fixture(store)
        proof = json.loads(attempt.proof)
        proof[field] = value
        store._connection.execute('UPDATE direct_publication_attempts SET proof=?', (json.dumps(proof),))
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises((ValueError, TypeError)):
            store._read_publication_attempt(store._connection,'job-1')
        assert tuple(store._connection.iterdump()) == snapshot


def test_exact_attempt_queue_receipt_fault_rolls_back_gate_job_and_pointer(tmp_path):
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, attempt, _, _ = _exact_attempt_fixture(store)
        store._connection.execute("CREATE TRIGGER fail_queue BEFORE INSERT ON queue_commands BEGIN SELECT RAISE(ABORT,'queue receipt fault'); END")
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(sqlite3.IntegrityError):
            store.apply_queue_gate(gate='paused',request_id='queue-close',payload_digest='3'*64,
                expected_revision=store.queue_gate_snapshot()[1],
                _contained_direct_job=('job-1',attempt.generation,attempt.revision,True))
        assert tuple(store._connection.iterdump()) == snapshot


def test_exact_attempt_interrupted_pending_cold_settles_new_only(tmp_path):
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        _, attempt, _, _ = _exact_attempt_fixture(store)
        original = tuple(store._connection.execute('SELECT * FROM direct_dispatch_commands').fetchone())
        arguments = dict(job_id='job-1',expected_worker_epoch=1,expected_generation=1,
            expected_revision=5,request_id='recovery-pending',payload_digest='3'*64,
            controller_ready=False,now=datetime(2032,1,2,tzinfo=UTC))
        plan = store.prepare_direct_dispatch(**arguments)
        assert store.prepare_direct_dispatch(**arguments) == plan
        store.recover_cold_start()
        receipt = store._read_direct_dispatch_command(store._connection,'recovery-pending')
        assert receipt.status == 'blocked'
        assert tuple(store._connection.execute("SELECT * FROM direct_dispatch_commands WHERE request_id='terminal-start'").fetchone()) == original
        current = store._read_publication_attempt(store._connection,'job-1')
        assert current.state == 'paused' and current.pending_request_id is None


@pytest.mark.parametrize('failure_statement_prefix', ['CREATE TABLE direct_publication_attempts',
    'CREATE TABLE closed_direct_publication_attempts','PRAGMA user_version = 16'])
def test_exact_attempt_v16_migration_is_atomic_and_has_no_backfill(tmp_path, monkeypatch, failure_statement_prefix):
    database = tmp_path / 'attempt.db'
    _create_v14_database(database)
    with sqlite3.connect(database) as connection:
        connection.execute(store_module._FINAL_PUBLICATION_BINDINGS_SCHEMA)
        connection.execute('PRAGMA user_version = 15')
        original = tuple(connection.iterdump())
    connect = sqlite3.connect
    monkeypatch.setattr(store_module.sqlite3,'connect',lambda *a,**k: _MigrationFailureConnection(
        connect(*a,**k), failure_statement_prefix=failure_statement_prefix))
    with pytest.raises(sqlite3.OperationalError):
        SQLiteStore(database)
    with connect(database) as connection:
        assert tuple(connection.iterdump()) == original
        assert connection.execute('PRAGMA user_version').fetchone()[0] == 15
    monkeypatch.setattr(store_module.sqlite3,'connect',connect)
    with closing(SQLiteStore(database)) as store:
        assert store._connection.execute('PRAGMA user_version').fetchone()[0] == 16
        assert store._connection.execute('SELECT COUNT(*) FROM direct_publication_attempts').fetchone()[0] == 0


@pytest.mark.parametrize('action', ('pause', 'queue'))
def test_exact_attempt_already_paused_control_settles_pending_without_inventing_audit(tmp_path, action):
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        _, _, _, _ = _exact_attempt_fixture(store)
        store.recover_cold_start()
        current = store.get_job('job-1')
        pending = store.prepare_direct_dispatch(job_id='job-1', expected_worker_epoch=2,
            expected_generation=current.generation, expected_revision=current.revision,
            request_id='held-recovery', payload_digest='3'*64, controller_ready=False,
            now=datetime(2032,1,2,tzinfo=UTC))
        if action == 'pause':
            # First manual pause may change the hold; the next exact control is a no-op.
            store.apply_job_control(job_id='job-1', action='pause', request_id='first-pause',
                payload_digest='4'*64, expected_revision=current.revision,
                _contained_direct_transfer=True, _publication_recoverable=True)
            current = store.get_job('job-1')
            pending = store.prepare_direct_dispatch(job_id='job-1', expected_worker_epoch=2,
                expected_generation=current.generation, expected_revision=current.revision,
                request_id='held-second', payload_digest='5'*64, controller_ready=False,
                now=datetime(2032,1,2,tzinfo=UTC))
        events = store.list_events()
        if action == 'pause':
            store.apply_job_control(job_id='job-1', action='pause', request_id='noop-pause',
                payload_digest='6'*64, expected_revision=current.revision,
                _contained_direct_transfer=True, _publication_recoverable=True)
        else:
            store.apply_queue_gate(gate='paused', request_id='noop-queue', payload_digest='6'*64,
                expected_revision=store.queue_gate_snapshot()[1],
                _contained_direct_job=('job-1', current.generation, current.revision, True))
        assert store.list_events() == events
        attempt = store._read_publication_attempt(store._connection, 'job-1')
        assert attempt.pending_request_id is None
        assert store._read_direct_dispatch_command(store._connection, pending.request_id).status == 'blocked'


def test_exact_attempt_current_audit_kind_cannot_be_inherited(tmp_path):
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        _exact_attempt_fixture(store)
        store.recover_cold_start()
        store._connection.execute("UPDATE events SET kind='job_added' WHERE event_id=(SELECT audit_id FROM direct_publication_attempts)")
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(ValueError):
            store._read_publication_attempt(store._connection, 'job-1')
        assert tuple(store._connection.iterdump()) == snapshot


def test_exact_attempt_recovery_pending_digest_drift_cannot_complete(tmp_path):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        _, attempt, destination, _ = _exact_attempt_fixture(store)
        paths.publish_staged_partial_payload(destination, attempt.prepared.reservation,
            attempt.prepared.staged_payload, prepared=attempt.prepared,
            creation_permit=paths.PublicationCreationPermit())
        store.recover_cold_start()
        current = store.get_job('job-1')
        pending = store.prepare_direct_dispatch(job_id='job-1', expected_worker_epoch=2,
            expected_generation=current.generation, expected_revision=current.revision,
            request_id='held-recovery', payload_digest='3'*64, controller_ready=False,
            now=datetime(2032,1,2,tzinfo=UTC))
        p = pending.attempt.prepared
        published = paths.publish_staged_partial_payload(p.destination, p.reservation,
            p.staged_payload, prepared=p, existing_only=True)
        store._connection.execute("UPDATE direct_dispatch_commands SET payload_digest=? WHERE request_id='held-recovery'", ('4'*64,))
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(ValueError):
            store.complete_direct_publication(pending.attempt, published)
        assert tuple(store._connection.iterdump()) == snapshot


@pytest.mark.parametrize('fault', ('pointer', 'audit', 'job', 'receipt', 'epoch', 'original-receipt'))
def test_exact_attempt_completion_post_mutation_authority_fault_rolls_back(tmp_path, fault):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        _, attempt, destination, _ = _exact_attempt_fixture(store)
        paths.publish_staged_partial_payload(destination, attempt.prepared.reservation,
            attempt.prepared.staged_payload, prepared=attempt.prepared,
            creation_permit=paths.PublicationCreationPermit())
        store.recover_cold_start()
        current = store.get_job('job-1')
        pending = store.prepare_direct_dispatch(job_id='job-1', expected_worker_epoch=2,
            expected_generation=current.generation, expected_revision=current.revision,
            request_id='new-completion', payload_digest='3'*64, controller_ready=False,
            now=datetime(2032,1,2,tzinfo=UTC))
        p = pending.attempt.prepared
        published = paths.publish_staged_partial_payload(p.destination,p.reservation,p.staged_payload,
            prepared=p,existing_only=True)
        triggers = {
            'pointer': "AFTER UPDATE ON direct_publication_attempts WHEN NEW.status='finished' BEGIN UPDATE direct_publication_attempts SET audit_id=OLD.audit_id WHERE job_id=NEW.job_id; END",
            'audit': "AFTER INSERT ON events WHEN NEW.kind='job_completed' BEGIN UPDATE events SET kind='job_added' WHERE event_id=NEW.event_id; END",
            'job': "AFTER UPDATE ON jobs WHEN NEW.state='completed' BEGIN UPDATE jobs SET revision=NEW.revision+1 WHERE job_id=NEW.job_id; END",
            'receipt': "AFTER UPDATE ON direct_dispatch_commands WHEN NEW.request_id='new-completion' AND NEW.status='started' BEGIN UPDATE direct_dispatch_commands SET payload_digest='" + '4'*64 + "' WHERE request_id=NEW.request_id; END",
            'epoch': "AFTER INSERT ON events WHEN NEW.kind='job_completed' BEGIN UPDATE settings SET value='3' WHERE key='worker_epoch'; END",
            'original-receipt': "AFTER INSERT ON events WHEN NEW.kind='job_completed' BEGIN UPDATE direct_dispatch_commands SET revision=revision+1 WHERE request_id='terminal-start'; END",
        }
        store._connection.execute('CREATE TRIGGER corrupt_completion ' + triggers[fault])
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(ValueError):
            store.complete_direct_publication(pending.attempt, published)
        assert tuple(store._connection.iterdump()) == snapshot
        assert store._read_publication_attempt(store._connection,'job-1') == pending.attempt
        assert destination.final_path.read_bytes() == b'body'


@pytest.mark.parametrize('field', ('attempt_id', 'original_request_id', 'proof', 'status',
    'audit_id', 'generation', 'revision', 'state', 'worker_epoch', 'pending_request_id'))
def test_exact_attempt_schema_refuses_blob_authority_without_mutation(tmp_path, field):
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        _exact_attempt_fixture(store)
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(sqlite3.IntegrityError):
            store._connection.execute(f'UPDATE direct_publication_attempts SET {field}=?', (sqlite3.Binary(b'blob'),))
        assert tuple(store._connection.iterdump()) == snapshot


class _PublicationCommitFailure:
    def __init__(self, connection):
        self.connection = connection
    def __getattr__(self, name):
        return getattr(self.connection, name)
    def commit(self):
        raise sqlite3.OperationalError('injected publication commit fault')


@pytest.mark.parametrize('phase', ('reserve', 'initial-complete', 'recovery-complete', 'pending', 'abort', 'cold'))
def test_exact_attempt_commit_fault_is_all_or_none_and_retains_post_link_authority(tmp_path, phase):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, staged, destination, _ = _stage_producer_fixture(store)
        store.bind_direct_staged_payload(stage, staged)
        marker = paths.PublicationReservationMarker(destination.incomplete_dir / '.hermes-reservation',
            stage.terminal.marker.marker_device, stage.terminal.marker.marker_inode)
        prepared = paths.prepare_publication_payload(destination, stage.terminal.dispatch.reservation, marker, staged)
        attempt = None if phase == 'reserve' else store.reserve_direct_publication(stage, prepared)
        published = None
        if phase != 'reserve':
            published = paths.publish_staged_partial_payload(destination, prepared.reservation, staged,
                prepared=prepared,creation_permit=paths.PublicationCreationPermit())
        pending = None
        if phase in {'recovery-complete', 'pending', 'abort'}:
            store.recover_cold_start()
            current = store.get_job('job-1')
            arguments = dict(job_id='job-1',expected_worker_epoch=2,expected_generation=current.generation,
                expected_revision=current.revision,request_id='new-fault-recovery',payload_digest='3'*64,
                controller_ready=False,now=datetime(2032,1,2,tzinfo=UTC))
            if phase != 'pending':
                pending = store.prepare_direct_dispatch(**arguments)
        snapshot = tuple(store._connection.iterdump())
        connection = store._connection
        store._connection = _PublicationCommitFailure(connection)
        try:
            with pytest.raises(sqlite3.OperationalError, match='commit fault'):
                if phase == 'reserve':
                    store.reserve_direct_publication(stage, prepared)
                elif phase == 'initial-complete':
                    store.complete_direct_publication(attempt, published, initial_stage=stage)
                elif phase == 'recovery-complete':
                    store.complete_direct_publication(pending.attempt, published)
                elif phase == 'pending':
                    store.prepare_direct_dispatch(**arguments)
                elif phase == 'abort':
                    store.abort_exact_publication_recovery(pending)
                else:
                    store.recover_cold_start()
        finally:
            store._connection = connection
        assert tuple(connection.iterdump()) == snapshot
        assert destination.partial_path.read_bytes() == b'body'
        assert destination.final_path.exists() == (phase != 'reserve')
        if phase != 'reserve':
            assert store._read_publication_attempt(connection,'job-1') is not None


@pytest.mark.parametrize('phase', ('reserve', 'initial-complete', 'recovery-complete'))
@pytest.mark.parametrize('boundary', ('descriptor-pre', 'descriptor-post', 'rewrite-pre', 'rewrite-post'))
def test_exact_attempt_descriptor_and_restored_mtime_faults_roll_back_without_cleanup(tmp_path, monkeypatch, phase, boundary):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, staged, destination, _ = _stage_producer_fixture(store)
        store.bind_direct_staged_payload(stage, staged)
        marker = paths.PublicationReservationMarker(destination.incomplete_dir / '.hermes-reservation',
            stage.terminal.marker.marker_device, stage.terminal.marker.marker_inode)
        prepared = paths.prepare_publication_payload(destination, stage.terminal.dispatch.reservation, marker, staged)
        attempt = None if phase == 'reserve' else store.reserve_direct_publication(stage, prepared)
        published = None
        if attempt is not None:
            published = paths.publish_staged_partial_payload(destination, prepared.reservation, staged,
                prepared=prepared,creation_permit=paths.PublicationCreationPermit())
        if phase == 'recovery-complete':
            store.recover_cold_start()
            current = store.get_job('job-1')
            pending = store.prepare_direct_dispatch(job_id='job-1',expected_worker_epoch=2,
                expected_generation=current.generation,expected_revision=current.revision,
                request_id='new-fault-recovery',payload_digest='3'*64,controller_ready=False,
                now=datetime(2032,1,2,tzinfo=UTC))
            attempt = pending.attempt
        name = '_require_current_staged_payload' if phase == 'reserve' else 'require_current_publication_payload'
        original = getattr(paths,name)
        calls = []
        def damaged(*args, **kwargs):
            calls.append(1)
            target = 1 if boundary.endswith('pre') else 2
            if len(calls) == target:
                if boundary.startswith('descriptor'):
                    raise paths.PathValidationError('injected descriptor refusal')
                details = destination.partial_path.stat()
                destination.partial_path.write_bytes(b'BODY')
                os.utime(destination.partial_path, ns=(details.st_atime_ns,details.st_mtime_ns))
            return original(*args, **kwargs)
        monkeypatch.setattr(paths,name,damaged)
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(paths.PathValidationError):
            if phase == 'reserve':
                store.reserve_direct_publication(stage,prepared)
            else:
                store.complete_direct_publication(attempt,published,initial_stage=stage if phase=='initial-complete' else None)
        assert tuple(store._connection.iterdump()) == snapshot
        assert destination.partial_path.exists() and marker.path.exists()
        assert destination.final_path.exists() == (phase != 'reserve')


def test_exact_attempt_recovery_abort_requires_active_direct_and_cold_preserves_historical_video_literal(tmp_path):
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        _exact_attempt_fixture(store)
        pending = store.prepare_direct_dispatch(job_id='job-1',expected_worker_epoch=1,
            expected_generation=1,expected_revision=5,request_id='pending-before-source-change',
            payload_digest='3'*64,controller_ready=False,now=datetime(2032,1,2,tzinfo=UTC))
        store._connection.execute("UPDATE materialized_jobs SET source_kind='video'")
        attempt_before = tuple(store._connection.execute('SELECT * FROM direct_publication_attempts').fetchone())
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(ValueError):
            store.abort_exact_publication_recovery(pending)
        assert tuple(store._connection.iterdump()) == snapshot
        store.recover_cold_start()
        assert tuple(store._connection.execute('SELECT * FROM direct_publication_attempts').fetchone()) == attempt_before
        assert store.get_materialized_job('job-1').source_kind.value == 'video'


def test_exact_attempt_insert_fault_preserves_original_receipt_and_stage(tmp_path):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, staged, destination, _ = _stage_producer_fixture(store)
        store.bind_direct_staged_payload(stage,staged)
        marker = paths.PublicationReservationMarker(destination.incomplete_dir / '.hermes-reservation',
            stage.terminal.marker.marker_device,stage.terminal.marker.marker_inode)
        prepared = paths.prepare_publication_payload(destination,stage.terminal.dispatch.reservation,marker,staged)
        store._connection.execute("CREATE TRIGGER reject_attempt BEFORE INSERT ON direct_publication_attempts BEGIN SELECT RAISE(ABORT,'attempt insert fault'); END")
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(sqlite3.IntegrityError, match='attempt insert fault'):
            store.reserve_direct_publication(stage,prepared)
        assert tuple(store._connection.iterdump()) == snapshot
        assert not destination.final_path.exists()
        assert destination.partial_path.read_bytes() == b'body' and marker.path.exists()


@pytest.mark.parametrize('fault', ('archive', 'delete', 'insert', 'proof', 'receipt', 'audit', 'descriptor', 'commit',
    'archive-ignore', 'current-ignore', 'current-drop'))
def test_closed_attempt_retirement_fault_rolls_back_entire_transaction(tmp_path, monkeypatch, fault):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, _, destination, _ = _exact_attempt_fixture(store)
        fresh, prepared, closed = _fresh_stage_after_closed_attempt(store, stage, destination)
        triggers = {
            'archive': "BEFORE INSERT ON closed_direct_publication_attempts BEGIN SELECT RAISE(ABORT,'archive fault'); END",
            'delete': "BEFORE DELETE ON direct_publication_attempts BEGIN SELECT RAISE(ABORT,'delete fault'); END",
            'insert': "BEFORE INSERT ON direct_publication_attempts BEGIN SELECT RAISE(ABORT,'insert fault'); END",
            'proof': "AFTER INSERT ON direct_publication_attempts BEGIN UPDATE closed_direct_publication_attempts SET proof='{}'; END",
            'receipt': "AFTER INSERT ON direct_publication_attempts BEGIN UPDATE direct_dispatch_commands SET revision=revision+1 WHERE request_id='terminal-start'; END",
            'audit': "AFTER INSERT ON direct_publication_attempts BEGIN UPDATE events SET kind='job_added' WHERE event_id=(SELECT audit_id FROM closed_direct_publication_attempts); END",
            'archive-ignore': "BEFORE INSERT ON closed_direct_publication_attempts BEGIN SELECT RAISE(IGNORE); END",
            'current-ignore': "BEFORE INSERT ON direct_publication_attempts BEGIN SELECT RAISE(IGNORE); END",
            'current-drop': "AFTER INSERT ON direct_publication_attempts BEGIN DELETE FROM direct_publication_attempts WHERE job_id=NEW.job_id; END",
        }
        if fault in triggers:
            store._connection.execute('CREATE TRIGGER fail_retirement '+triggers[fault])
        elif fault == 'commit':
            store._connection = _PublicationCommitFailure(store._connection)
        elif fault == 'descriptor':
            original = paths._require_strict_publication_namespace
            calls = []
            def fail_after_retirement(*args, **kwargs):
                calls.append(1)
                if len(calls) == 2:
                    raise paths.PathValidationError('retirement post-check fault')
                return original(*args, **kwargs)
            monkeypatch.setattr(paths, '_require_strict_publication_namespace', fail_after_retirement)
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises((ValueError, sqlite3.Error, paths.PathValidationError)):
            store.reserve_direct_publication(fresh, prepared)
        assert tuple(store._connection.iterdump()) == snapshot
        assert tuple(store._connection.execute('SELECT * FROM direct_publication_attempts').fetchone()) == closed
        assert store._connection.execute('SELECT COUNT(*) FROM closed_direct_publication_attempts').fetchone()[0] == 0
        assert destination.partial_path.read_bytes() == b'body' and prepared.marker.path.exists()
        assert not destination.final_path.exists()


@pytest.mark.parametrize('damage', ('eligible', 'finished', 'proof', 'receipt', 'audit', 'pending', 'ambiguous'))
def test_closed_attempt_retirement_rejects_invalid_or_competing_authority(tmp_path, damage):
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, _, destination, _ = _exact_attempt_fixture(store)
        fresh, prepared, closed = _fresh_stage_after_closed_attempt(store, stage, destination)
        if damage in {'eligible','finished'}:
            state = 'paused' if damage == 'eligible' else 'completed'
            store._connection.execute('UPDATE direct_publication_attempts SET status=?,state=?', (damage,state))
        elif damage == 'proof':
            store._connection.execute("UPDATE direct_publication_attempts SET proof='{}'")
        elif damage == 'receipt':
            store._connection.execute("UPDATE direct_dispatch_commands SET revision=revision+1 WHERE request_id='terminal-start'")
        elif damage == 'audit':
            store._connection.execute("UPDATE events SET kind='job_added' WHERE event_id=(SELECT audit_id FROM direct_publication_attempts)")
        elif damage == 'pending':
            store._connection.execute("UPDATE direct_publication_attempts SET pending_request_id='retire-new-dispatch'")
        else:
            store._connection.execute('INSERT INTO closed_direct_publication_attempts VALUES (?,?,?,?,?,?,?,?,?,?,?)',closed)
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises((TypeError,ValueError,sqlite3.Error)):
            store.reserve_direct_publication(fresh,prepared)
        assert tuple(store._connection.iterdump()) == snapshot
        assert not destination.final_path.exists()


@pytest.mark.parametrize('damage', ('missing-current', 'invalid-current', 'missing-final'))
def test_closed_archive_never_supplies_recovery_authority(tmp_path, damage):
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, _, destination, _ = _exact_attempt_fixture(store)
        fresh, prepared, closed = _fresh_stage_after_closed_attempt(store,stage,destination)
        store.reserve_direct_publication(fresh,prepared)
        store.recover_cold_start()
        if damage == 'missing-current':
            store._connection.execute('DELETE FROM direct_publication_attempts')
        elif damage == 'invalid-current':
            store._connection.execute("UPDATE direct_publication_attempts SET proof='{}'")
        current = store.get_job('job-1')
        arguments = dict(job_id='job-1',expected_worker_epoch=2,
            expected_generation=current.generation,expected_revision=current.revision,
            request_id='archive-not-authority',payload_digest='6'*64,controller_ready=False,
            now=datetime(2032,1,2,tzinfo=UTC))
        if damage == 'invalid-current':
            snapshot = tuple(store._connection.iterdump())
            with pytest.raises(ValueError,match='publication attempt proof shape'):
                store.prepare_direct_dispatch(**arguments)
            assert tuple(store._connection.iterdump()) == snapshot
        elif damage == 'missing-final':
            # Valid current proof can request only existing-only verification.
            from hermes_downloads import paths
            result = store.prepare_direct_dispatch(**arguments)
            assert result.attempt.attempt_id != closed[1]
            with pytest.raises(paths.PathValidationError):
                paths.publish_staged_partial_payload(destination,prepared.reservation,
                    prepared.staged_payload,prepared=prepared,existing_only=True)
            store.abort_exact_publication_recovery(result)
        else:
            result = store.prepare_direct_dispatch(**arguments)
            assert result.status == 'blocked'
        assert tuple(store._connection.execute('SELECT * FROM closed_direct_publication_attempts').fetchone()) == closed
        assert not destination.final_path.exists()


@pytest.mark.parametrize('field', ('job_id','attempt_id','original_request_id','proof','status',
    'audit_id','generation','revision','state','worker_epoch','pending_request_id'))
def test_closed_archive_schema_rejects_blob_proof(tmp_path,field):
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, _, destination, _ = _exact_attempt_fixture(store)
        fresh, prepared, _ = _fresh_stage_after_closed_attempt(store,stage,destination)
        store.reserve_direct_publication(fresh,prepared)
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(sqlite3.IntegrityError):
            store._connection.execute(f'UPDATE closed_direct_publication_attempts SET {field}=?',(sqlite3.Binary(b'blob'),))
        assert tuple(store._connection.iterdump()) == snapshot


def test_closed_archive_retains_multiple_distinct_attempts_and_only_current_can_complete(tmp_path):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, first, destination, _ = _exact_attempt_fixture(store)
        next_stage, prepared, first_closed = _fresh_stage_after_closed_attempt(store,stage,destination)
        second = store.reserve_direct_publication(next_stage,prepared)
        final_stage, prepared, second_closed = _fresh_stage_after_closed_attempt(store,next_stage,
            destination,action='start_now',request_prefix='third')
        third = store.reserve_direct_publication(final_stage,prepared)
        assert len({first.attempt_id,second.attempt_id,third.attempt_id}) == 3
        archives = {row['attempt_id']:tuple(row) for row in store._connection.execute('SELECT * FROM closed_direct_publication_attempts')}
        assert archives == {first.attempt_id:first_closed,second.attempt_id:second_closed}
        for stale in (first,second):
            with pytest.raises(ValueError):
                store.complete_direct_publication(stale,None)
        published = paths.publish_staged_partial_payload(destination,prepared.reservation,
            prepared.staged_payload,prepared=prepared,creation_permit=paths.PublicationCreationPermit())
        assert store.complete_direct_publication(third,published,initial_stage=final_stage).state == 'completed'
        assert {row['attempt_id']:tuple(row) for row in store._connection.execute('SELECT * FROM closed_direct_publication_attempts')} == archives


def test_closed_retirement_never_takes_an_already_linked_final(tmp_path):
    from hermes_downloads import paths
    with closing(SQLiteStore(tmp_path / 'attempt.db')) as store:
        stage, _, destination, _ = _exact_attempt_fixture(store)
        fresh, prepared, closed = _fresh_stage_after_closed_attempt(store,stage,destination)
        os.link(destination.partial_path,destination.final_path)
        final = destination.final_path.stat()
        marker = prepared.marker.path.stat()
        snapshot = tuple(store._connection.iterdump())
        with pytest.raises(paths.PathValidationError):
            store.reserve_direct_publication(fresh,prepared)
        assert tuple(store._connection.iterdump()) == snapshot
        assert tuple(store._connection.execute('SELECT * FROM direct_publication_attempts').fetchone()) == closed
        assert store._connection.execute('SELECT COUNT(*) FROM closed_direct_publication_attempts').fetchone()[0] == 0
        assert destination.final_path.stat() == final and prepared.marker.path.stat() == marker
        assert destination.final_path.read_bytes() == destination.partial_path.read_bytes() == b'body'
