"""Transactional persistence and idempotency tests for the queue store."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
import sqlite3
from typing import Any

import pytest

import hermes_downloads.retry as retry_module
import hermes_downloads.store as store_module
from hermes_downloads.models import DownloadIntent, MaterializedJob, SourceKind
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
        "source_kind": SourceKind.VIDEO,
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


def _table_names(database_path: Path) -> frozenset[str]:
    with sqlite3.connect(database_path) as connection:
        return frozenset(
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
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

    def executescript(self, script: str) -> None:
        self._connection.execute("BEGIN EXCLUSIVE")
        raise sqlite3.OperationalError("injected bootstrap failure")

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
    initialized = SQLiteStore(database_path)
    try:
        initialized.apply_add(_intent())
        initialized._connection.execute(
            "UPDATE jobs SET state = 'downloading' WHERE job_id = 'job-1'"
        )
        initialized._connection.execute(
            """
            INSERT INTO settings (key, value, revision)
            VALUES ('queue_gate', 'running', 9)
            """
        )
        job_before = initialized.get_job("job-1")
        command_before = initialized.get_command("request-1")
        events_before = initialized.list_events()
    finally:
        initialized.close()
    _install_failing_insert_trigger(
        database_path,
        table="events",
        trigger_name="fail_recovery_event_insert",
        message="injected recovery event write failure",
    )

    store = SQLiteStore(database_path)
    try:
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


def test_v2_migrates_v1_database_without_changing_legacy_job_data(tmp_path: Path) -> None:
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
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        assert connection.execute(
            """
            SELECT job_id, source_url, generation, revision, state
            FROM jobs
            WHERE job_id = 'legacy-job'
            """
        ).fetchone() == expected_legacy_job
    v1_tables = {"settings", "jobs", "commands", "events"}
    v2_tables = _table_names(database_path)
    assert v1_tables <= v2_tables
    assert {"collection_holds", "job_retry", "job_retry_audit"} <= v2_tables
    assert len(v2_tables - v1_tables) >= 4


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


def test_v2_rejects_newer_schema_without_creating_legacy_tables(tmp_path: Path) -> None:
    database_path = tmp_path / "queue.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE future_jobs (job_id TEXT PRIMARY KEY)")
        connection.execute("PRAGMA user_version = 3")

    with pytest.raises(RuntimeError, match="newer than supported"):
        SQLiteStore(database_path)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
    assert _table_names(database_path) == {"future_jobs"}


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
