"""SQLite-backed transactional persistence for queue commands."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
import re
import sqlite3
from typing import Final

from hermes_downloads.models import DownloadIntent, MaterializedJob, SourceKind

__all__ = [
    "CommandRecord",
    "CommandResult",
    "EventRecord",
    "JobRecord",
    "RequestConflictError",
    "SQLiteStore",
]


_SCHEMA: Final = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    revision INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    source_url BLOB NOT NULL,
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    state TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS commands (
    request_id TEXT PRIMARY KEY,
    payload_digest TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL
);
"""

_MATERIALIZED_JOBS_SCHEMA: Final = """
CREATE TABLE materialized_jobs (
    job_id TEXT PRIMARY KEY REFERENCES jobs(job_id),
    source_kind TEXT NOT NULL,
    queue_collection_id TEXT,
    priority INTEGER NOT NULL,
    order_key INTEGER NOT NULL,
    scheduled_for_us INTEGER,
    authorized INTEGER NOT NULL,
    manual_hold INTEGER NOT NULL,
    start_now_requested INTEGER NOT NULL,
    category TEXT NOT NULL,
    destination_collection TEXT,
    partial_filename TEXT NOT NULL,
    selected_final_filename TEXT NOT NULL,
    expected_revision INTEGER
);
"""

_COLLECTION_HOLDS_SCHEMA: Final = """
CREATE TABLE collection_holds (
    collection_id TEXT PRIMARY KEY
);
"""

_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_PAGE_SIZE: Final = 100
_RECOVERABLE_COLD_START_STATES: Final = (
    "queued",
    "resolving",
    "downloading",
    "pausing",
    "paused",
    "retry_wait",
    "finalizing",
)


class RequestConflictError(ValueError):
    """Raised when a request ID is reused with a different payload digest."""


def _require_identifier(value: object, name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{name} must be a nonblank identifier")
    return value


def _require_sqlite_text(value: object, name: str) -> str:
    if type(value) is not str:
        raise ValueError(f"persisted {name} is not text")
    return value


def _require_optional_sqlite_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _require_sqlite_text(value, name)


def _require_sqlite_integer(value: object, name: str) -> int:
    if type(value) is not int:
        raise ValueError(f"persisted {name} is not an integer")
    return value


def _require_sqlite_boolean(value: object, name: str) -> bool:
    integer = _require_sqlite_integer(value, name)
    if integer not in {0, 1}:
        raise ValueError(f"persisted {name} is not a boolean")
    return bool(integer)


def _require_sqlite_blob(value: object, name: str) -> bytes:
    if type(value) is not bytes:
        raise ValueError(f"persisted {name} is not bytes")
    return value


@dataclass(frozen=True, slots=True)
class CommandResult:
    """The durable outcome of one add command delivery."""

    applied: bool
    job: str
    generation: int
    revision: int


@dataclass(frozen=True, slots=True)
class JobRecord:
    """Persisted immutable fields for a queued job."""

    job: str
    source_url: bytes
    generation: int
    revision: int
    state: str


@dataclass(frozen=True, slots=True)
class CommandRecord:
    """Persisted idempotency ledger entry."""

    request_id: str
    payload_digest: str
    job: str
    generation: int
    revision: int


@dataclass(frozen=True, slots=True)
class EventRecord:
    """A durable event emitted by a transactional queue mutation."""

    event_id: int
    kind: str
    job: str
    generation: int
    revision: int


class SQLiteStore:
    """A small single-writer SQLite store with command idempotency."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        connection = sqlite3.connect(self.database_path, isolation_level=None)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.executescript(_SCHEMA)
            self._migrate_schema_v2(connection)
        except BaseException:
            try:
                connection.rollback()
            except BaseException:
                pass
            try:
                connection.close()
            except BaseException:
                pass
            raise
        self._connection = connection

    @staticmethod
    def _migrate_schema_v2(connection: sqlite3.Connection) -> None:
        """Apply the additive v2 schema migration in one transaction."""

        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("PRAGMA user_version").fetchone()
            if row is None or type(row[0]) is not int:
                raise RuntimeError("database schema version is invalid")
            version = row[0]
            if version <= 1:
                connection.execute(_MATERIALIZED_JOBS_SCHEMA)
                connection.execute(_COLLECTION_HOLDS_SCHEMA)
                connection.execute("PRAGMA user_version = 2")
            elif version > 2:
                raise RuntimeError("database schema version is newer than supported")
            connection.commit()
        except BaseException:
            try:
                connection.rollback()
            except BaseException:
                pass
            raise

    def close(self) -> None:
        """Release the SQLite connection."""

        self._connection.close()

    def apply_add(
        self, intent: DownloadIntent, *, materialized: MaterializedJob | None = None
    ) -> CommandResult:
        """Atomically persist one queued job, command receipt, and event."""

        if not isinstance(intent, DownloadIntent):
            raise TypeError("intent must be a DownloadIntent")
        if materialized is not None:
            if type(materialized) is not MaterializedJob:
                raise TypeError("materialized must be a MaterializedJob")
            if materialized.intent != intent:
                raise ValueError("materialized.intent must equal intent")

        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = connection.execute(
                """
                SELECT payload_digest, job_id, generation, revision
                FROM commands
                WHERE request_id = ?
                """,
                (intent.request_id,),
            ).fetchone()
            if existing is not None:
                if existing["payload_digest"] != intent.payload_digest:
                    raise RequestConflictError(
                        "request_id is already bound to a different payload digest"
                    )
                if materialized is not None and not self._stored_projection_matches(
                    connection, existing["job_id"], intent, materialized
                ):
                    raise RequestConflictError(
                        "request_id is already bound to a different materialized projection"
                    )
                result = CommandResult(
                    applied=False,
                    job=existing["job_id"],
                    generation=existing["generation"],
                    revision=existing["revision"],
                )
            else:
                connection.execute(
                    """
                    INSERT INTO jobs (job_id, source_url, generation, revision, state)
                    VALUES (?, ?, ?, ?, 'queued')
                    """,
                    (
                        intent.job_id,
                        intent.source_url,
                        intent.generation,
                        intent.revision,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO commands (
                        request_id, payload_digest, job_id, generation, revision
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        intent.request_id,
                        intent.payload_digest,
                        intent.job_id,
                        intent.generation,
                        intent.revision,
                    ),
                )
                if materialized is not None:
                    self._insert_materialized_projection(connection, materialized)
                connection.execute(
                    """
                    INSERT INTO events (kind, job_id, generation, revision)
                    VALUES ('job_added', ?, ?, ?)
                    """,
                    (intent.job_id, intent.generation, intent.revision),
                )
                result = CommandResult(
                    applied=True,
                    job=intent.job_id,
                    generation=intent.generation,
                    revision=intent.revision,
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return result

    @staticmethod
    def _insert_materialized_projection(
        connection: sqlite3.Connection, materialized: MaterializedJob
    ) -> None:
        """Write one normalized, immutable materialized-job projection."""

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

    @staticmethod
    def _stored_projection_matches(
        connection: sqlite3.Connection,
        stored_job_id: object,
        intent: DownloadIntent,
        materialized: MaterializedJob,
    ) -> bool:
        """Compare a duplicate delivery against its persisted immutable projection."""

        if type(stored_job_id) is not str or stored_job_id != intent.job_id:
            return False
        job_rows = connection.execute(
            "SELECT source_url FROM jobs WHERE job_id = ?", (stored_job_id,)
        ).fetchall()
        domain_rows = connection.execute(
            """
            SELECT
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
            WHERE job_id = ?
            """,
            (stored_job_id,),
        ).fetchall()
        if len(job_rows) != 1 or len(domain_rows) != 1:
            return False
        if job_rows[0]["source_url"] != intent.source_url:
            return False
        return tuple(domain_rows[0]) == SQLiteStore._projection_values(materialized)

    @staticmethod
    def _projection_values(materialized: MaterializedJob) -> tuple[object, ...]:
        """Return SQLite-native values for the non-lifecycle domain projection."""

        scheduled_for_us = (
            None
            if materialized.scheduled_for is None
            else SQLiteStore._utc_microseconds(materialized.scheduled_for)
        )
        return (
            materialized.source_kind.value,
            materialized.queue_collection_id,
            materialized.priority,
            materialized.order_key,
            scheduled_for_us,
            1 if materialized.authorized else 0,
            1 if materialized.manual_hold else 0,
            1 if materialized.start_now_requested else 0,
            materialized.category,
            materialized.destination_collection,
            materialized.partial_filename,
            materialized.selected_final_filename,
            materialized.intent.expected_revision,
        )

    @staticmethod
    def _utc_microseconds(value: datetime) -> int:
        """Encode an aware UTC timestamp without a floating-point conversion."""

        delta = value.astimezone(UTC) - _EPOCH
        return (
            delta.days * 86_400_000_000
            + delta.seconds * 1_000_000
            + delta.microseconds
        )

    def initialize_cold_start(self) -> str:
        """Persist the global paused gate before any worker scheduling begins."""

        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            setting = connection.execute(
                "SELECT revision FROM settings WHERE key = 'queue_gate'"
            ).fetchone()
            if setting is None:
                connection.execute(
                    """
                    INSERT INTO settings (key, value, revision)
                    VALUES ('queue_gate', 'paused', 1)
                    """
                )
            else:
                connection.execute(
                    """
                    UPDATE settings
                    SET value = 'paused', revision = ?
                    WHERE key = 'queue_gate'
                    """,
                    (setting["revision"] + 1,),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return "paused"

    def recover_cold_start(self) -> int:
        """Atomically fence a cold worker epoch and pause incomplete jobs."""

        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            epoch_setting = connection.execute(
                "SELECT value, revision FROM settings WHERE key = 'worker_epoch'"
            ).fetchone()
            if epoch_setting is None:
                epoch = 1
                connection.execute(
                    """
                    INSERT INTO settings (key, value, revision)
                    VALUES ('worker_epoch', '1', 1)
                    """
                )
            else:
                epoch = int(epoch_setting["value"]) + 1
                connection.execute(
                    """
                    UPDATE settings
                    SET value = ?, revision = ?
                    WHERE key = 'worker_epoch'
                    """,
                    (str(epoch), epoch_setting["revision"] + 1),
                )

            gate_setting = connection.execute(
                "SELECT revision FROM settings WHERE key = 'queue_gate'"
            ).fetchone()
            if gate_setting is None:
                connection.execute(
                    """
                    INSERT INTO settings (key, value, revision)
                    VALUES ('queue_gate', 'paused', 1)
                    """
                )
            else:
                connection.execute(
                    """
                    UPDATE settings
                    SET value = 'paused', revision = ?
                    WHERE key = 'queue_gate'
                    """,
                    (gate_setting["revision"] + 1,),
                )

            jobs = connection.execute(
                """
                SELECT job_id, generation, revision
                FROM jobs
                WHERE state IN (?, ?, ?, ?, ?, ?, ?)
                ORDER BY job_id
                """,
                _RECOVERABLE_COLD_START_STATES,
            ).fetchall()
            for job in jobs:
                generation = job["generation"] + 1
                revision = job["revision"] + 1
                connection.execute(
                    """
                    UPDATE jobs
                    SET generation = ?, revision = ?, state = 'paused'
                    WHERE job_id = ?
                    """,
                    (generation, revision, job["job_id"]),
                )
                connection.execute(
                    """
                    INSERT INTO events (kind, job_id, generation, revision)
                    VALUES ('job_paused', ?, ?, ?)
                    """,
                    (job["job_id"], generation, revision),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return epoch

    def queue_gate(self) -> str | None:
        """Return the persisted global admission gate, if initialized."""

        row = self._connection.execute(
            "SELECT value FROM settings WHERE key = 'queue_gate'"
        ).fetchone()
        return None if row is None else row["value"]

    def collection_hold(self, collection_id: str) -> bool:
        """Return whether one exact collection gate is durably held."""

        collection_id = _require_identifier(collection_id, "collection_id")
        row = self._connection.execute(
            "SELECT 1 FROM collection_holds WHERE collection_id = ?", (collection_id,)
        ).fetchone()
        return row is not None

    def set_collection_hold(self, collection_id: str, *, held: bool) -> None:
        """Persist one collection gate as a presence row or remove it."""

        collection_id = _require_identifier(collection_id, "collection_id")
        if type(held) is not bool:
            raise TypeError("held must be a boolean")
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            if held:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO collection_holds (collection_id)
                    VALUES (?)
                    """,
                    (collection_id,),
                )
            else:
                connection.execute(
                    "DELETE FROM collection_holds WHERE collection_id = ?",
                    (collection_id,),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def worker_epoch(self) -> int | None:
        """Return the durable cold-worker epoch, if recovery has run."""

        row = self._connection.execute(
            "SELECT value FROM settings WHERE key = 'worker_epoch'"
        ).fetchone()
        return None if row is None else int(row["value"])

    def get_job(self, job_id: str) -> JobRecord | None:
        """Read one persisted job."""

        row = self._connection.execute(
            """
            SELECT job_id, source_url, generation, revision, state
            FROM jobs
            WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()
        return None if row is None else self._job_record(row)

    def get_materialized_job(self, job_id: str) -> MaterializedJob | None:
        """Read one materialized projection with the current job lifecycle values."""

        job_id = _require_identifier(job_id, "job_id")
        connection = self._connection
        domain_rows = connection.execute(
            "SELECT job_id FROM materialized_jobs WHERE job_id = ?", (job_id,)
        ).fetchall()
        if not domain_rows:
            return None
        if len(domain_rows) != 1:
            raise ValueError("materialized job has multiple domain rows")
        rows = connection.execute(
            """
            SELECT
                domain.job_id AS domain_job_id,
                job.job_id AS job_id,
                job.source_url AS source_url,
                job.generation AS generation,
                job.revision AS revision,
                command.request_id AS request_id,
                command.payload_digest AS payload_digest,
                domain.source_kind AS source_kind,
                domain.queue_collection_id AS queue_collection_id,
                domain.priority AS priority,
                domain.order_key AS order_key,
                domain.scheduled_for_us AS scheduled_for_us,
                domain.authorized AS authorized,
                domain.manual_hold AS manual_hold,
                domain.start_now_requested AS start_now_requested,
                domain.category AS category,
                domain.destination_collection AS destination_collection,
                domain.partial_filename AS partial_filename,
                domain.selected_final_filename AS selected_final_filename,
                domain.expected_revision AS expected_revision
            FROM materialized_jobs AS domain
            JOIN jobs AS job ON job.job_id = domain.job_id
            JOIN commands AS command ON command.job_id = job.job_id
            WHERE domain.job_id = ?
            """,
            (job_id,),
        ).fetchall()
        if len(rows) != 1:
            raise ValueError("materialized job must have exactly one command row")
        return self._materialized_job_from_row(rows[0])

    @staticmethod
    def _materialized_job_from_row(row: sqlite3.Row) -> MaterializedJob:
        domain_job_id = _require_sqlite_text(row["domain_job_id"], "domain job_id")
        job_id = _require_sqlite_text(row["job_id"], "job_id")
        if domain_job_id != job_id:
            raise ValueError("materialized domain job_id does not match its job")
        expected_revision_value = row["expected_revision"]
        expected_revision = (
            None
            if expected_revision_value is None
            else _require_sqlite_integer(expected_revision_value, "expected_revision")
        )
        scheduled_for_value = row["scheduled_for_us"]
        if scheduled_for_value is None:
            scheduled_for = None
        else:
            scheduled_for_us = _require_sqlite_integer(
                scheduled_for_value, "scheduled_for_us"
            )
            try:
                scheduled_for = _EPOCH + timedelta(microseconds=scheduled_for_us)
            except OverflowError as error:
                raise ValueError("persisted scheduled_for_us is out of range") from error
        try:
            source_kind = SourceKind(
                _require_sqlite_text(row["source_kind"], "source_kind")
            )
        except ValueError as error:
            raise ValueError("persisted source_kind is invalid") from error
        intent = DownloadIntent(
            job_id=job_id,
            request_id=_require_sqlite_text(row["request_id"], "request_id"),
            payload_digest=_require_sqlite_text(
                row["payload_digest"], "payload_digest"
            ),
            source_url=_require_sqlite_blob(row["source_url"], "source_url"),
            expected_revision=expected_revision,
            generation=_require_sqlite_integer(row["generation"], "generation"),
            revision=_require_sqlite_integer(row["revision"], "revision"),
        )
        return MaterializedJob(
            job_id=domain_job_id,
            intent=intent,
            source_kind=source_kind,
            queue_collection_id=_require_optional_sqlite_text(
                row["queue_collection_id"], "queue_collection_id"
            ),
            priority=_require_sqlite_integer(row["priority"], "priority"),
            order_key=_require_sqlite_integer(row["order_key"], "order_key"),
            scheduled_for=scheduled_for,
            authorized=_require_sqlite_boolean(row["authorized"], "authorized"),
            manual_hold=_require_sqlite_boolean(row["manual_hold"], "manual_hold"),
            start_now_requested=_require_sqlite_boolean(
                row["start_now_requested"], "start_now_requested"
            ),
            category=_require_sqlite_text(row["category"], "category"),
            destination_collection=_require_optional_sqlite_text(
                row["destination_collection"], "destination_collection"
            ),
            partial_filename=_require_sqlite_text(
                row["partial_filename"], "partial_filename"
            ),
            selected_final_filename=_require_sqlite_text(
                row["selected_final_filename"], "selected_final_filename"
            ),
        )

    def list_jobs(self, *, cursor: str | None = None) -> tuple[JobRecord, ...]:
        """Read one bounded page of jobs after a stable job-ID cursor."""

        if cursor is None:
            rows = self._connection.execute(
                """
                SELECT job_id, source_url, generation, revision, state
                FROM jobs
                ORDER BY job_id
                LIMIT ?
                """,
                (_PAGE_SIZE,),
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT job_id, source_url, generation, revision, state
                FROM jobs
                WHERE job_id > ?
                ORDER BY job_id
                LIMIT ?
                """,
                (cursor, _PAGE_SIZE),
            ).fetchall()
        return tuple(self._job_record(row) for row in rows)

    def get_command(self, request_id: str) -> CommandRecord | None:
        """Read one idempotency ledger entry."""

        row = self._connection.execute(
            """
            SELECT request_id, payload_digest, job_id, generation, revision
            FROM commands
            WHERE request_id = ?
            """,
            (request_id,),
        ).fetchone()
        if row is None:
            return None
        return CommandRecord(
            request_id=row["request_id"],
            payload_digest=row["payload_digest"],
            job=row["job_id"],
            generation=row["generation"],
            revision=row["revision"],
        )

    def list_events(self, *, cursor: int | None = None) -> tuple[EventRecord, ...]:
        """Read one bounded page of events after an insertion-order cursor."""

        if cursor is None:
            rows = self._connection.execute(
                """
                SELECT event_id, kind, job_id, generation, revision
                FROM events
                ORDER BY event_id
                LIMIT ?
                """,
                (_PAGE_SIZE,),
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT event_id, kind, job_id, generation, revision
                FROM events
                WHERE event_id > ?
                ORDER BY event_id
                LIMIT ?
                """,
                (cursor, _PAGE_SIZE),
            ).fetchall()
        return tuple(
            EventRecord(
                event_id=row["event_id"],
                kind=row["kind"],
                job=row["job_id"],
                generation=row["generation"],
                revision=row["revision"],
            )
            for row in rows
        )

    @staticmethod
    def _job_record(row: sqlite3.Row) -> JobRecord:
        return JobRecord(
            job=row["job_id"],
            source_url=row["source_url"],
            generation=row["generation"],
            revision=row["revision"],
            state=row["state"],
        )
