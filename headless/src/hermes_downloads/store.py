"""SQLite-backed transactional persistence for queue commands."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
import re
import sqlite3
from typing import Final

from hermes_downloads.models import DownloadIntent, MaterializedJob, SourceKind
from hermes_downloads.retry import (
    RetryAuditEvent,
    RetryAuditKind,
    RetryAuthority,
    RetryBudget,
    RetryPolicy,
)

__all__ = [
    "CommandRecord",
    "CommandResult",
    "EventRecord",
    "JobPageRecord",
    "JobRecord",
    "QueueGateResult",
    "RequestConflictError",
    "RevisionConflictError",
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

_JOB_RETRY_SCHEMA: Final = """
CREATE TABLE job_retry (
    job_id TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    budget_number INTEGER NOT NULL,
    ordinary_attempts INTEGER NOT NULL,
    paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
    exhausted INTEGER NOT NULL CHECK (exhausted IN (0, 1))
);
"""

_JOB_RETRY_AUDIT_SCHEMA: Final = """
CREATE TABLE job_retry_audit (
    job_id TEXT NOT NULL REFERENCES job_retry(job_id) ON DELETE CASCADE,
    audit_index INTEGER NOT NULL CHECK (audit_index >= 0 AND audit_index < 256),
    kind TEXT NOT NULL,
    generation INTEGER NOT NULL,
    budget_number INTEGER NOT NULL,
    ordinary_attempts INTEGER NOT NULL,
    PRIMARY KEY (job_id, audit_index)
);
"""

_QUEUE_COMMANDS_SCHEMA: Final = """
CREATE TABLE queue_commands (
    request_id TEXT PRIMARY KEY,
    payload_digest TEXT NOT NULL,
    gate TEXT NOT NULL CHECK (gate IN ('paused', 'running')),
    revision INTEGER NOT NULL
);
"""


def _normalize_table_schema(schema: str) -> str:
    """Canonicalize static SQLite DDL for exact current-version validation."""

    return " ".join(
        schema.replace("CREATE TABLE IF NOT EXISTS ", "CREATE TABLE ").split()
    ).upper()


def _expected_table_schemas(*schemas: str) -> dict[str, str]:
    """Index the known static table definitions by their normalized names."""

    expected: dict[str, str] = {}
    for schema in schemas:
        for statement in schema.split(";"):
            normalized = _normalize_table_schema(statement)
            if not normalized:
                continue
            prefix, _, _definition = normalized.partition("(")
            table_name = prefix.removeprefix("CREATE TABLE ").strip().lower()
            if not table_name:
                raise RuntimeError("static table schema is invalid")
            expected[table_name] = normalized
    return expected


_SUPPORTED_SCHEMA_VERSION: Final = 4
_RETRY_AUDIT_CAPACITY: Final = 256
_MAX_COUNTER: Final = (1 << 63) - 1
_V3_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
    _JOB_RETRY_SCHEMA,
    _JOB_RETRY_AUDIT_SCHEMA,
)
_V4_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
    _JOB_RETRY_SCHEMA,
    _JOB_RETRY_AUDIT_SCHEMA,
    _QUEUE_COMMANDS_SCHEMA,
)
_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SHA256_DIGEST: Final = re.compile(r"[0-9a-f]{64}\Z")
_QUEUE_GATES: Final = frozenset({"paused", "running"})
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
    """Raised when a request ID is reused for a different command."""


class RevisionConflictError(ValueError):
    """Raised when a command is fenced by a stale queue-gate revision."""


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


def _require_queue_gate(value: object) -> str:
    if type(value) is not str:
        raise TypeError("gate must be a string")
    if value not in _QUEUE_GATES:
        raise ValueError("gate must be 'paused' or 'running'")
    return value


def _require_counter(value: object, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= _MAX_COUNTER:
        raise ValueError(f"{name} must be a nonnegative persisted counter")
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
class QueueGateResult:
    """The durable outcome of one global queue-gate command delivery."""

    applied: bool
    gate: str
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
class JobPageRecord:
    """Lifecycle fields needed to render one bounded job page."""

    job: str
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
            self._reject_newer_schema_version(connection)
            connection.executescript(_SCHEMA)
            self._migrate_schema_v4(connection)
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
    def _reject_newer_schema_version(connection: sqlite3.Connection) -> None:
        """Fail before bootstrap can mutate an unsupported current schema."""

        row = connection.execute("PRAGMA user_version").fetchone()
        if row is None or type(row[0]) is not int:
            raise RuntimeError("database schema version is invalid")
        if row[0] > _SUPPORTED_SCHEMA_VERSION:
            raise RuntimeError("database schema version is newer than supported")
        expected_schemas = (
            _V4_TABLE_SCHEMAS
            if row[0] == _SUPPORTED_SCHEMA_VERSION
            else _V3_TABLE_SCHEMAS if row[0] == 3 else None
        )
        if expected_schemas is not None and not SQLiteStore._has_table_schemas(
            connection, expected_schemas
        ):
            raise RuntimeError("database schema version is incomplete")

    @staticmethod
    def _has_table_schemas(
        connection: sqlite3.Connection, expected_schemas: dict[str, str]
    ) -> bool:
        """Recognize an exact supported schema before a bootstrap write."""

        for table_name, expected_schema in expected_schemas.items():
            row = connection.execute(
                """
                SELECT sql
                FROM sqlite_master
                WHERE type = 'table' AND name = ?
                """,
                (table_name,),
            ).fetchone()
            if row is None or type(row["sql"]) is not str:
                return False
            if _normalize_table_schema(row["sql"]) != expected_schema:
                return False
        return True

    @staticmethod
    def _migrate_schema_v4(connection: sqlite3.Connection) -> None:
        """Apply additive v2 through v4 schema migrations in one transaction."""

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
                version = 2
            if version <= 2:
                connection.execute(_JOB_RETRY_SCHEMA)
                connection.execute(_JOB_RETRY_AUDIT_SCHEMA)
                connection.execute("PRAGMA user_version = 3")
                version = 3
            if version == 3:
                if not SQLiteStore._has_table_schemas(connection, _V3_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_QUEUE_COMMANDS_SCHEMA)
                connection.execute(f"PRAGMA user_version = {_SUPPORTED_SCHEMA_VERSION}")
            elif version == _SUPPORTED_SCHEMA_VERSION:
                if not SQLiteStore._has_table_schemas(connection, _V4_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
            elif version > _SUPPORTED_SCHEMA_VERSION:
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

    def apply_queue_gate(
        self,
        *,
        gate: str,
        request_id: str,
        payload_digest: str,
        expected_revision: int,
    ) -> QueueGateResult:
        """Atomically apply or replay one revision-fenced global gate command."""

        gate = _require_queue_gate(gate)
        request_id = _require_identifier(request_id, "request_id")
        payload_digest = _require_payload_digest(payload_digest)
        expected_revision = _require_counter(expected_revision, "expected_revision")

        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            receipt = connection.execute(
                """
                SELECT payload_digest, gate, revision
                FROM queue_commands
                WHERE request_id = ?
                """,
                (request_id,),
            ).fetchone()
            if receipt is not None:
                receipt_digest = _require_sqlite_text(
                    receipt["payload_digest"], "queue command payload_digest"
                )
                receipt_gate = _require_queue_gate(
                    _require_sqlite_text(receipt["gate"], "queue command gate")
                )
                receipt_revision = _require_counter(
                    _require_sqlite_integer(
                        receipt["revision"], "queue command revision"
                    ),
                    "queue command revision",
                )
                if receipt_digest != payload_digest or receipt_gate != gate:
                    raise RequestConflictError(
                        "request_id is already bound to a different queue-gate command"
                    )
                result = QueueGateResult(
                    applied=False, gate=receipt_gate, revision=receipt_revision
                )
            else:
                setting = connection.execute(
                    """
                    SELECT value, revision
                    FROM settings
                    WHERE key = 'queue_gate'
                    """
                ).fetchone()
                if setting is None:
                    raise RuntimeError("queue gate is not initialized")
                _require_queue_gate(
                    _require_sqlite_text(setting["value"], "queue gate value")
                )
                current_revision = _require_counter(
                    _require_sqlite_integer(
                        setting["revision"], "queue gate revision"
                    ),
                    "queue gate revision",
                )
                if current_revision != expected_revision:
                    raise RevisionConflictError("queue gate revision is stale")
                if current_revision == _MAX_COUNTER:
                    raise OverflowError("queue gate revision exceeds persisted counter range")
                next_revision = current_revision + 1
                connection.execute(
                    """
                    UPDATE settings
                    SET value = ?, revision = ?
                    WHERE key = 'queue_gate'
                    """,
                    (gate, next_revision),
                )
                connection.execute(
                    """
                    INSERT INTO queue_commands (request_id, payload_digest, gate, revision)
                    VALUES (?, ?, ?, ?)
                    """,
                    (request_id, payload_digest, gate, next_revision),
                )
                result = QueueGateResult(
                    applied=True, gate=gate, revision=next_revision
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return result

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
                job_id = _require_sqlite_text(job["job_id"], "job_id")
                generation = _require_sqlite_integer(job["generation"], "generation") + 1
                revision = _require_sqlite_integer(job["revision"], "revision") + 1
                retry_budget = self._read_retry_budget(connection, job_id)
                fenced_retry = None
                if retry_budget is not None:
                    authority = RetryAuthority.restore(
                        policy=RetryPolicy(), budget=retry_budget
                    )
                    fenced_retry = authority.fence_cold_start(
                        new_generation=generation
                    )
                connection.execute(
                    """
                    UPDATE jobs
                    SET generation = ?, revision = ?, state = 'paused'
                    WHERE job_id = ?
                    """,
                    (generation, revision, job_id),
                )
                if fenced_retry is not None:
                    self._replace_retry_budget(connection, fenced_retry)
                connection.execute(
                    """
                    INSERT INTO events (kind, job_id, generation, revision)
                    VALUES ('job_paused', ?, ?, ?)
                    """,
                    (job_id, generation, revision),
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

    def set_retry_budget(self, budget: RetryBudget) -> None:
        """Atomically replace one validated retry snapshot and its audit history."""

        budget = RetryAuthority.restore(policy=RetryPolicy(), budget=budget).budget
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            job_rows = connection.execute(
                """
                SELECT generation
                FROM jobs
                WHERE job_id = ?
                LIMIT 2
                """,
                (budget.job_id,),
            ).fetchall()
            if not job_rows:
                raise ValueError("retry budget job does not exist")
            if len(job_rows) != 1:
                raise ValueError("retry budget job record is not unique")
            generation = _require_sqlite_integer(
                job_rows[0]["generation"], "job generation"
            )
            if generation != budget.generation:
                raise ValueError(
                    "retry budget generation does not match the current job generation"
                )
            self._replace_retry_budget(connection, budget)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def get_retry_budget(self, job_id: str) -> RetryBudget | None:
        """Read one fully validated retry snapshot, if the job owns one."""

        job_id = _require_identifier(job_id, "job_id")
        return self._read_retry_budget(self._connection, job_id)

    @staticmethod
    def _read_retry_budget(
        connection: sqlite3.Connection, job_id: str
    ) -> RetryBudget | None:
        """Rebuild retry state from normalized rows without defaulting corruption."""

        snapshot_rows = connection.execute(
            """
            SELECT
                job_id,
                generation,
                budget_number,
                ordinary_attempts,
                paused,
                exhausted
            FROM job_retry
            WHERE job_id = ?
            LIMIT 2
            """,
            (job_id,),
        ).fetchall()
        if not snapshot_rows:
            orphan = connection.execute(
                """
                SELECT 1
                FROM job_retry_audit
                WHERE job_id = ?
                LIMIT 1
                """,
                (job_id,),
            ).fetchone()
            if orphan is not None:
                raise ValueError("retry audit exists without a retry snapshot")
            return None
        if len(snapshot_rows) != 1:
            raise ValueError("job has multiple retry snapshots")

        snapshot = snapshot_rows[0]
        persisted_job_id = _require_sqlite_text(snapshot["job_id"], "retry job_id")
        if persisted_job_id != job_id:
            raise ValueError("retry snapshot job_id does not match its lookup")
        job_rows = connection.execute(
            """
            SELECT generation
            FROM jobs
            WHERE job_id = ?
            LIMIT 2
            """,
            (job_id,),
        ).fetchall()
        if len(job_rows) != 1:
            raise ValueError("retry snapshot must belong to exactly one job")

        audit_rows = connection.execute(
            """
            SELECT audit_index, kind, generation, budget_number, ordinary_attempts
            FROM job_retry_audit
            WHERE job_id = ?
            ORDER BY audit_index
            LIMIT ?
            """,
            (job_id, _RETRY_AUDIT_CAPACITY + 1),
        ).fetchall()
        if len(audit_rows) > _RETRY_AUDIT_CAPACITY:
            raise ValueError("persisted retry audit exceeds capacity")
        audit = tuple(
            SQLiteStore._retry_audit_event_from_row(row, index)
            for index, row in enumerate(audit_rows)
        )
        budget = RetryBudget(
            job_id=persisted_job_id,
            generation=_require_sqlite_integer(
                snapshot["generation"], "retry generation"
            ),
            budget_number=_require_sqlite_integer(
                snapshot["budget_number"], "retry budget_number"
            ),
            ordinary_attempts=_require_sqlite_integer(
                snapshot["ordinary_attempts"], "retry ordinary_attempts"
            ),
            paused=_require_sqlite_boolean(snapshot["paused"], "retry paused"),
            exhausted=_require_sqlite_boolean(
                snapshot["exhausted"], "retry exhausted"
            ),
            audit=audit,
        )
        job_generation = _require_sqlite_integer(
            job_rows[0]["generation"], "job generation"
        )
        if budget.generation != job_generation:
            raise ValueError("retry budget generation does not match its job")
        return RetryAuthority.restore(policy=RetryPolicy(), budget=budget).budget

    @staticmethod
    def _retry_audit_event_from_row(
        row: sqlite3.Row, expected_index: int
    ) -> RetryAuditEvent:
        """Decode one ordered audit row without coercing malformed SQLite values."""

        audit_index = _require_sqlite_integer(row["audit_index"], "retry audit index")
        if audit_index != expected_index:
            raise ValueError("persisted retry audit is not contiguous")
        try:
            kind = RetryAuditKind(
                _require_sqlite_text(row["kind"], "retry audit kind")
            )
        except ValueError as error:
            raise ValueError("persisted retry audit kind is invalid") from error
        return RetryAuditEvent(
            kind=kind,
            generation=_require_sqlite_integer(
                row["generation"], "retry audit generation"
            ),
            budget_number=_require_sqlite_integer(
                row["budget_number"], "retry audit budget_number"
            ),
            ordinary_attempts=_require_sqlite_integer(
                row["ordinary_attempts"], "retry audit ordinary_attempts"
            ),
        )

    @staticmethod
    def _replace_retry_budget(
        connection: sqlite3.Connection, budget: RetryBudget
    ) -> None:
        """Replace one authoritative snapshot and its bounded normalized history."""

        budget = RetryAuthority.restore(policy=RetryPolicy(), budget=budget).budget
        connection.execute(
            """
            INSERT INTO job_retry (
                job_id,
                generation,
                budget_number,
                ordinary_attempts,
                paused,
                exhausted
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                generation = excluded.generation,
                budget_number = excluded.budget_number,
                ordinary_attempts = excluded.ordinary_attempts,
                paused = excluded.paused,
                exhausted = excluded.exhausted
            """,
            (
                budget.job_id,
                budget.generation,
                budget.budget_number,
                budget.ordinary_attempts,
                1 if budget.paused else 0,
                1 if budget.exhausted else 0,
            ),
        )
        connection.execute(
            "DELETE FROM job_retry_audit WHERE job_id = ?", (budget.job_id,)
        )
        connection.executemany(
            """
            INSERT INTO job_retry_audit (
                job_id,
                audit_index,
                kind,
                generation,
                budget_number,
                ordinary_attempts
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            tuple(
                (
                    budget.job_id,
                    index,
                    event.kind.value,
                    event.generation,
                    event.budget_number,
                    event.ordinary_attempts,
                )
                for index, event in enumerate(budget.audit)
            ),
        )

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

    def list_job_page(self, *, cursor: str | None = None) -> tuple[JobPageRecord, ...]:
        """Read one bounded lifecycle-only page after a stable job-ID cursor."""

        if cursor is None:
            rows = self._connection.execute(
                """
                SELECT job_id, generation, revision, state
                FROM jobs
                ORDER BY job_id
                LIMIT ?
                """,
                (_PAGE_SIZE,),
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT job_id, generation, revision, state
                FROM jobs
                WHERE job_id > ?
                ORDER BY job_id
                LIMIT ?
                """,
                (cursor, _PAGE_SIZE),
            ).fetchall()
        return tuple(
            JobPageRecord(
                job=row["job_id"],
                generation=row["generation"],
                revision=row["revision"],
                state=row["state"],
            )
            for row in rows
        )

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
