"""SQLite-backed transactional persistence for queue commands."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
import re
import secrets
import sqlite3
from typing import Final

from hermes_downloads.models import DownloadIntent, JobState, MaterializedJob, SourceKind
from hermes_downloads.processes import ProcessBirthIdentity
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
    "DirectEngineActivationFence",
    "DirectEngineRecord",
    "EventRecord",
    "JobControlResult",
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

_JOB_CONTROL_COMMANDS_SCHEMA: Final = """
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

_COMMAND_RECEIPTS_SCHEMA: Final = """
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

_ENGINE_INSTANCES_SCHEMA: Final = """
CREATE TABLE engine_instances (
    engine_kind TEXT PRIMARY KEY CHECK (engine_kind = 'direct'),
    worker_epoch INTEGER NOT NULL CHECK (worker_epoch > 0),
    leader_pid INTEGER NOT NULL CHECK (leader_pid > 0),
    process_group_id INTEGER NOT NULL CHECK (process_group_id = leader_pid),
    session_id INTEGER NOT NULL CHECK (session_id = leader_pid),
    owner_uid INTEGER NOT NULL CHECK (owner_uid >= 0),
    started_unix_us INTEGER NOT NULL CHECK (started_unix_us > 0),
    argv_sha256 TEXT NOT NULL CHECK (
        length(argv_sha256) = 64 AND argv_sha256 NOT GLOB '*[^0-9a-f]*'
    )
);
"""

_DIRECT_ENGINE_ACTIVATION_FENCES_SCHEMA: Final = """
CREATE TABLE direct_engine_activation_fences (
    engine_kind TEXT PRIMARY KEY NOT NULL CHECK (engine_kind = 'direct'),
    worker_epoch INTEGER NOT NULL CHECK (worker_epoch > 0),
    reservation_token TEXT NOT NULL CHECK (
        length(reservation_token) = 64
        AND reservation_token NOT GLOB '*[^0-9a-f]*'
    )
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


_SUPPORTED_SCHEMA_VERSION: Final = 8
_RETRY_AUDIT_CAPACITY: Final = 256
_MAX_COUNTER: Final = (1 << 63) - 1
_V1_TABLE_SCHEMAS: Final = _expected_table_schemas(_SCHEMA)
_V2_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
)
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
_V5_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
    _JOB_RETRY_SCHEMA,
    _JOB_RETRY_AUDIT_SCHEMA,
    _QUEUE_COMMANDS_SCHEMA,
    _ENGINE_INSTANCES_SCHEMA,
)
_V6_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
    _JOB_RETRY_SCHEMA,
    _JOB_RETRY_AUDIT_SCHEMA,
    _QUEUE_COMMANDS_SCHEMA,
    _ENGINE_INSTANCES_SCHEMA,
    _DIRECT_ENGINE_ACTIVATION_FENCES_SCHEMA,
)
_V7_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
    _JOB_RETRY_SCHEMA,
    _JOB_RETRY_AUDIT_SCHEMA,
    _QUEUE_COMMANDS_SCHEMA,
    _ENGINE_INSTANCES_SCHEMA,
    _DIRECT_ENGINE_ACTIVATION_FENCES_SCHEMA,
    _JOB_CONTROL_COMMANDS_SCHEMA,
)
_V8_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
    _JOB_RETRY_SCHEMA,
    _JOB_RETRY_AUDIT_SCHEMA,
    _QUEUE_COMMANDS_SCHEMA,
    _ENGINE_INSTANCES_SCHEMA,
    _DIRECT_ENGINE_ACTIVATION_FENCES_SCHEMA,
    _JOB_CONTROL_COMMANDS_SCHEMA,
    _COMMAND_RECEIPTS_SCHEMA,
)
_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SHA256_DIGEST: Final = re.compile(r"[0-9a-f]{64}\Z")
_QUEUE_GATES: Final = frozenset({"paused", "running"})
_JOB_CONTROL_ACTIONS: Final = frozenset({"pause", "resume", "start_now"})
_COMMAND_RECEIPT_SCOPES: Final = frozenset({"add", "queue_gate", "job_control"})
_ADD_COMMAND_SCOPE: Final = "add"
_ADD_COMMAND_ACTION: Final = "add"
_QUEUE_GATE_COMMAND_SCOPE: Final = "queue_gate"
_QUEUE_GATE_COMMAND_ACTION: Final = "queue_gate"
_JOB_CONTROL_COMMAND_SCOPE: Final = "job_control"
_JOB_CONTROL_STATUSES: Final = frozenset({"applied", "blocked", "stale"})
_TERMINAL_JOB_CONTROL_STATES: Final = frozenset(
    {"removed", "completed", "cancelled", "failed"}
)
_JOB_CONTROL_EVENT_KINDS: Final = {
    "pause": "job_paused",
    "resume": "job_resumed",
    "start_now": "job_start_now_requested",
}
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


def _require_job_control_action(value: object) -> str:
    if type(value) is not str:
        raise TypeError("action must be a string")
    if value not in _JOB_CONTROL_ACTIONS:
        raise ValueError("action is not a job-control action")
    return value


def _require_command_receipt_scope(value: object) -> str:
    if type(value) is not str or value not in _COMMAND_RECEIPT_SCOPES:
        raise ValueError("persisted command receipt scope is invalid")
    return value


def _require_command_receipt_action(scope: str, value: object) -> str:
    if type(value) is not str:
        raise ValueError("persisted command receipt action is invalid")
    if (
        (scope == _ADD_COMMAND_SCOPE and value == _ADD_COMMAND_ACTION)
        or (scope == _QUEUE_GATE_COMMAND_SCOPE and value == _QUEUE_GATE_COMMAND_ACTION)
        or (scope == _JOB_CONTROL_COMMAND_SCOPE and value in _JOB_CONTROL_ACTIONS)
    ):
        return value
    raise ValueError("persisted command receipt action is invalid")


def _require_job_control_status(value: object) -> str:
    if type(value) is not str:
        raise TypeError("status must be a string")
    if value not in _JOB_CONTROL_STATUSES:
        raise ValueError("status is not a job-control status")
    return value


def _require_public_job_state(value: object, name: str) -> str:
    state = _require_sqlite_text(value, name)
    try:
        JobState(state)
    except ValueError:
        raise ValueError(f"persisted {name} is not a public job state") from None
    return state


def _require_counter(value: object, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= _MAX_COUNTER:
        raise ValueError(f"{name} must be a nonnegative persisted counter")
    return value


def _require_worker_epoch(value: object, name: str) -> int:
    epoch = _require_counter(value, name)
    if epoch == 0:
        raise ValueError(f"{name} must be a positive worker epoch")
    return epoch


def _require_reservation_token(value: object) -> str:
    if type(value) is not str:
        raise TypeError("reservation_token must be a string")
    if _SHA256_DIGEST.fullmatch(value) is None:
        raise ValueError("reservation_token must be 64 lowercase hexadecimal characters")
    return value


def _require_process_birth_identity(value: object) -> ProcessBirthIdentity:
    if type(value) is not ProcessBirthIdentity:
        raise TypeError("identity must be a ProcessBirthIdentity")
    try:
        return ProcessBirthIdentity.from_record(value.to_record())
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise ValueError("identity must be a valid process-birth identity") from error


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
class JobControlResult:
    """The durable public readback for one materialized-job control command."""

    status: str
    job: str
    generation: int
    revision: int
    state: str
    authorized: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", _require_job_control_status(self.status))
        object.__setattr__(self, "job", _require_identifier(self.job, "job"))
        object.__setattr__(
            self, "generation", _require_counter(self.generation, "generation")
        )
        object.__setattr__(self, "revision", _require_counter(self.revision, "revision"))
        object.__setattr__(
            self, "state", _require_public_job_state(self.state, "job control state")
        )
        if type(self.authorized) is not bool:
            raise TypeError("authorized must be a boolean")


@dataclass(frozen=True, slots=True)
class _JobControlProjection:
    """The validated mutable fields needed for one serialized control transition."""

    job: str
    generation: int
    revision: int
    state: str
    authorized: bool
    manual_hold: bool
    start_now_requested: bool

    def to_result(self, status: str) -> JobControlResult:
        return JobControlResult(
            status=status,
            job=self.job,
            generation=self.generation,
            revision=self.revision,
            state=self.state,
            authorized=self.authorized,
        )


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


@dataclass(frozen=True, slots=True)
class DirectEngineActivationFence:
    """A durable direct-engine launch claim for one worker epoch."""

    worker_epoch: int
    reservation_token: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "worker_epoch", _require_worker_epoch(self.worker_epoch, "worker_epoch")
        )
        object.__setattr__(
            self, "reservation_token", _require_reservation_token(self.reservation_token)
        )


@dataclass(frozen=True, slots=True)
class DirectEngineRecord:
    """One direct-engine process identity bound to its owning worker epoch."""

    worker_epoch: int
    identity: ProcessBirthIdentity

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "worker_epoch", _require_worker_epoch(self.worker_epoch, "worker_epoch")
        )
        object.__setattr__(
            self, "identity", _require_process_birth_identity(self.identity)
        )


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
            self._migrate_schema_v8(connection)
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
        """Fail before bootstrap can mutate any nonempty unsupported schema."""

        row = connection.execute("PRAGMA user_version").fetchone()
        if row is None or type(row[0]) is not int or row[0] < 0:
            raise RuntimeError("database schema version is invalid")
        if row[0] > _SUPPORTED_SCHEMA_VERSION:
            raise RuntimeError("database schema version is newer than supported")
        expected_schemas = {
            0: {},
            1: _V1_TABLE_SCHEMAS,
            2: _V2_TABLE_SCHEMAS,
            3: _V3_TABLE_SCHEMAS,
            4: _V4_TABLE_SCHEMAS,
            5: _V5_TABLE_SCHEMAS,
            6: _V6_TABLE_SCHEMAS,
            7: _V7_TABLE_SCHEMAS,
            8: _V8_TABLE_SCHEMAS,
        }[row[0]]
        if not SQLiteStore._has_table_schemas(connection, expected_schemas):
            raise RuntimeError("database schema version is incomplete")

    @staticmethod
    def _has_table_schemas(
        connection: sqlite3.Connection, expected_schemas: dict[str, str]
    ) -> bool:
        """Recognize an exact supported schema before a bootstrap write."""

        actual_schemas: dict[str, str] = {}
        rows = connection.execute(
            """
            SELECT type, name, sql
            FROM sqlite_master
            WHERE substr(name, 1, 7) != 'sqlite_'
            """
        ).fetchall()
        for row in rows:
            if (
                type(row["type"]) is not str
                or row["type"] != "table"
                or type(row["name"]) is not str
                or type(row["sql"]) is not str
            ):
                return False
            actual_schemas[row["name"]] = _normalize_table_schema(row["sql"])
        return actual_schemas == expected_schemas

    @staticmethod
    def _migrate_schema_v8(connection: sqlite3.Connection) -> None:
        """Apply additive v2 through v8 schema migrations in one transaction."""

        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("PRAGMA user_version").fetchone()
            if row is None or type(row[0]) is not int or row[0] < 0:
                raise RuntimeError("database schema version is invalid")
            version = row[0]
            if version <= 1:
                connection.execute(_MATERIALIZED_JOBS_SCHEMA)
                connection.execute(_COLLECTION_HOLDS_SCHEMA)
                connection.execute("PRAGMA user_version = 2")
                version = 2
            if version == 2:
                if not SQLiteStore._has_table_schemas(connection, _V2_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_JOB_RETRY_SCHEMA)
                connection.execute(_JOB_RETRY_AUDIT_SCHEMA)
                connection.execute("PRAGMA user_version = 3")
                version = 3
            if version == 3:
                if not SQLiteStore._has_table_schemas(connection, _V3_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_QUEUE_COMMANDS_SCHEMA)
                connection.execute("PRAGMA user_version = 4")
                version = 4
            if version == 4:
                if not SQLiteStore._has_table_schemas(connection, _V4_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_ENGINE_INSTANCES_SCHEMA)
                connection.execute("PRAGMA user_version = 5")
                version = 5
            if version == 5:
                if not SQLiteStore._has_table_schemas(connection, _V5_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_DIRECT_ENGINE_ACTIVATION_FENCES_SCHEMA)
                connection.execute("PRAGMA user_version = 6")
                version = 6
            if version == 6:
                if not SQLiteStore._has_table_schemas(connection, _V6_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_JOB_CONTROL_COMMANDS_SCHEMA)
                connection.execute("PRAGMA user_version = 7")
                version = 7
            if version == 7:
                if not SQLiteStore._has_table_schemas(connection, _V7_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_COMMAND_RECEIPTS_SCHEMA)
                SQLiteStore._backfill_command_receipts(connection)
                connection.execute("PRAGMA user_version = 8")
                version = 8
            if version == _SUPPORTED_SCHEMA_VERSION:
                if not SQLiteStore._has_table_schemas(connection, _V8_TABLE_SCHEMAS):
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

    @staticmethod
    def _backfill_command_receipts(connection: sqlite3.Connection) -> None:
        """Backfill v7 receipts without choosing a cross-surface collision."""

        receipts: list[tuple[str, str, str, str]] = []
        request_ids: set[str] = set()

        def append_receipt(
            request_id_value: object,
            payload_digest_value: object,
            scope_value: str,
            action_value: object,
        ) -> None:
            request_id = _require_identifier(
                _require_sqlite_text(request_id_value, "command receipt request_id"),
                "command receipt request_id",
            )
            payload_digest = _require_payload_digest(payload_digest_value)
            scope = _require_command_receipt_scope(scope_value)
            action = _require_command_receipt_action(scope, action_value)
            if request_id in request_ids:
                raise RuntimeError(
                    "duplicate request_id across legacy command receipt tables"
                )
            request_ids.add(request_id)
            receipts.append((request_id, payload_digest, scope, action))

        for row in connection.execute("SELECT request_id, payload_digest FROM commands"):
            append_receipt(
                row["request_id"],
                row["payload_digest"],
                _ADD_COMMAND_SCOPE,
                _ADD_COMMAND_ACTION,
            )
        for row in connection.execute("SELECT request_id, payload_digest FROM queue_commands"):
            append_receipt(
                row["request_id"],
                row["payload_digest"],
                _QUEUE_GATE_COMMAND_SCOPE,
                _QUEUE_GATE_COMMAND_ACTION,
            )
        for row in connection.execute(
            "SELECT request_id, payload_digest, action FROM job_control_commands"
        ):
            append_receipt(
                row["request_id"],
                row["payload_digest"],
                _JOB_CONTROL_COMMAND_SCOPE,
                _require_job_control_action(
                    _require_sqlite_text(row["action"], "job control action")
                ),
            )
        connection.executemany(
            """
            INSERT INTO command_receipts (request_id, payload_digest, scope, action)
            VALUES (?, ?, ?, ?)
            """,
            tuple(receipts),
        )

    def close(self) -> None:
        """Release the SQLite connection."""

        self._connection.close()

    @staticmethod
    def _read_command_receipt(
        connection: sqlite3.Connection, request_id: str
    ) -> tuple[str, str, str] | None:
        """Read one exact shared receipt or fail closed on malformed state."""

        rows = connection.execute(
            """
            SELECT request_id, payload_digest, scope, action
            FROM command_receipts
            WHERE request_id = ?
            LIMIT 2
            """,
            (request_id,),
        ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError("command receipt is not unique")
        row = rows[0]
        stored_request_id = _require_identifier(
            _require_sqlite_text(row["request_id"], "command receipt request_id"),
            "command receipt request_id",
        )
        if stored_request_id != request_id:
            raise RuntimeError("command receipt request_id does not match its lookup")
        payload_digest = _require_payload_digest(
            _require_sqlite_text(row["payload_digest"], "command receipt payload_digest")
        )
        scope = _require_command_receipt_scope(
            _require_sqlite_text(row["scope"], "command receipt scope")
        )
        action = _require_command_receipt_action(
            scope, _require_sqlite_text(row["action"], "command receipt action")
        )
        return payload_digest, scope, action

    @staticmethod
    def _match_command_receipt(
        connection: sqlite3.Connection,
        *,
        request_id: str,
        payload_digest: str,
        scope: str,
        action: str,
    ) -> bool:
        """Match the global command identity before any surface-specific replay."""

        receipt = SQLiteStore._read_command_receipt(connection, request_id)
        if receipt is None:
            SQLiteStore._reject_unregistered_legacy_receipt(connection, request_id)
            return False
        if receipt != (payload_digest, scope, action):
            raise RequestConflictError(
                "request_id is already bound to a different command receipt"
            )
        return True

    @staticmethod
    def _reject_unregistered_legacy_receipt(
        connection: sqlite3.Connection, request_id: str
    ) -> None:
        """Prevent a damaged v8 registry from opening a cross-surface reuse hole."""

        row = connection.execute(
            """
            SELECT 1
            FROM (
                SELECT request_id FROM commands WHERE request_id = ?
                UNION ALL
                SELECT request_id FROM queue_commands WHERE request_id = ?
                UNION ALL
                SELECT request_id FROM job_control_commands WHERE request_id = ?
            )
            LIMIT 1
            """,
            (request_id, request_id, request_id),
        ).fetchone()
        if row is not None:
            raise RuntimeError("command receipt registry is incomplete")

    @staticmethod
    def _insert_command_receipt(
        connection: sqlite3.Connection,
        *,
        request_id: str,
        payload_digest: str,
        scope: str,
        action: str,
    ) -> None:
        """Persist one shared receipt in the caller's active transaction."""

        connection.execute(
            """
            INSERT INTO command_receipts (request_id, payload_digest, scope, action)
            VALUES (?, ?, ?, ?)
            """,
            (request_id, payload_digest, scope, action),
        )

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
            replay = self._match_command_receipt(
                connection,
                request_id=intent.request_id,
                payload_digest=intent.payload_digest,
                scope=_ADD_COMMAND_SCOPE,
                action=_ADD_COMMAND_ACTION,
            )
            existing = connection.execute(
                """
                SELECT payload_digest, job_id, generation, revision
                FROM commands
                WHERE request_id = ?
                """,
                (intent.request_id,),
            ).fetchone()
            if replay:
                if existing is None:
                    raise RuntimeError("command receipt is missing its add readback")
                receipt_digest = _require_payload_digest(
                    _require_sqlite_text(existing["payload_digest"], "command payload_digest")
                )
                if receipt_digest != intent.payload_digest:
                    raise RuntimeError("command receipt does not match its add readback")
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
                if existing is not None:
                    raise RuntimeError("command receipt registry is incomplete")
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
                self._insert_command_receipt(
                    connection,
                    request_id=intent.request_id,
                    payload_digest=intent.payload_digest,
                    scope=_ADD_COMMAND_SCOPE,
                    action=_ADD_COMMAND_ACTION,
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
            replay = self._match_command_receipt(
                connection,
                request_id=request_id,
                payload_digest=payload_digest,
                scope=_QUEUE_GATE_COMMAND_SCOPE,
                action=_QUEUE_GATE_COMMAND_ACTION,
            )
            receipt = connection.execute(
                """
                SELECT payload_digest, gate, revision
                FROM queue_commands
                WHERE request_id = ?
                """,
                (request_id,),
            ).fetchone()
            if replay:
                if receipt is None:
                    raise RuntimeError("command receipt is missing its queue-gate readback")
                receipt_digest = _require_payload_digest(
                    _require_sqlite_text(
                        receipt["payload_digest"], "queue command payload_digest"
                    )
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
                if receipt_digest != payload_digest:
                    raise RuntimeError(
                        "command receipt does not match its queue-gate readback"
                    )
                if receipt_gate != gate:
                    raise RequestConflictError(
                        "request_id is already bound to a different queue-gate command"
                    )
                result = QueueGateResult(
                    applied=False, gate=receipt_gate, revision=receipt_revision
                )
            else:
                if receipt is not None:
                    raise RuntimeError("command receipt registry is incomplete")
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
                self._insert_command_receipt(
                    connection,
                    request_id=request_id,
                    payload_digest=payload_digest,
                    scope=_QUEUE_GATE_COMMAND_SCOPE,
                    action=_QUEUE_GATE_COMMAND_ACTION,
                )
                result = QueueGateResult(
                    applied=True, gate=gate, revision=next_revision
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return result

    def apply_job_control(
        self,
        *,
        job_id: str,
        action: str,
        request_id: str,
        payload_digest: str,
        expected_revision: int,
    ) -> JobControlResult:
        """Atomically apply or replay one revision-fenced materialized-job command."""

        job_id = _require_identifier(job_id, "job_id")
        action = _require_job_control_action(action)
        request_id = _require_identifier(request_id, "request_id")
        payload_digest = _require_payload_digest(payload_digest)
        expected_revision = _require_counter(expected_revision, "expected_revision")

        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            replay = self._match_command_receipt(
                connection,
                request_id=request_id,
                payload_digest=payload_digest,
                scope=_JOB_CONTROL_COMMAND_SCOPE,
                action=action,
            )
            receipt = connection.execute(
                """
                SELECT
                    payload_digest,
                    job_id,
                    action,
                    status,
                    generation,
                    revision,
                    state,
                    authorized
                FROM job_control_commands
                WHERE request_id = ?
                """,
                (request_id,),
            ).fetchone()
            if replay:
                if receipt is None:
                    raise RuntimeError("command receipt is missing its job-control readback")
                result = self._job_control_result_from_receipt(receipt)
                receipt_digest = _require_payload_digest(
                    _require_sqlite_text(
                        receipt["payload_digest"], "job control payload_digest"
                    )
                )
                receipt_action = _require_job_control_action(
                    _require_sqlite_text(receipt["action"], "job control action")
                )
                if receipt_digest != payload_digest:
                    raise RuntimeError(
                        "command receipt does not match its job-control readback"
                    )
                if result.job != job_id or receipt_action != action:
                    raise RequestConflictError(
                        "request_id is already bound to a different job-control command"
                    )
            else:
                if receipt is not None:
                    raise RuntimeError("command receipt registry is incomplete")
                current = self._read_job_control_projection(connection, job_id)
                if current.revision != expected_revision:
                    result = current.to_result("stale")
                elif current.state in _TERMINAL_JOB_CONTROL_STATES:
                    result = current.to_result("blocked")
                elif action == "start_now" and self._current_queue_gate(connection) != "running":
                    result = current.to_result("blocked")
                else:
                    next_state = current.state
                    next_authorized = current.authorized
                    next_manual_hold = current.manual_hold
                    next_start_now_requested = current.start_now_requested
                    if action == "pause":
                        next_manual_hold = True
                        next_state = "paused"
                    elif action == "resume":
                        next_manual_hold = False
                        if current.state == "paused":
                            next_state = "queued"
                    else:
                        next_manual_hold = False
                        next_authorized = True
                        next_start_now_requested = True
                        next_state = "queued"

                    if (
                        next_state == current.state
                        and next_authorized == current.authorized
                        and next_manual_hold == current.manual_hold
                        and next_start_now_requested == current.start_now_requested
                    ):
                        result = current.to_result("applied")
                    else:
                        if current.revision == _MAX_COUNTER:
                            raise OverflowError(
                                "job revision exceeds persisted counter range"
                            )
                        updated = _JobControlProjection(
                            job=current.job,
                            generation=current.generation,
                            revision=current.revision + 1,
                            state=next_state,
                            authorized=next_authorized,
                            manual_hold=next_manual_hold,
                            start_now_requested=next_start_now_requested,
                        )
                        self._persist_job_control_mutation(
                            connection, action=action, updated=updated
                        )
                        result = updated.to_result("applied")
                self._insert_job_control_receipt(
                    connection,
                    request_id=request_id,
                    payload_digest=payload_digest,
                    action=action,
                    result=result,
                )
                self._insert_command_receipt(
                    connection,
                    request_id=request_id,
                    payload_digest=payload_digest,
                    scope=_JOB_CONTROL_COMMAND_SCOPE,
                    action=action,
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return result

    @staticmethod
    def _read_job_control_projection(
        connection: sqlite3.Connection, job_id: str
    ) -> _JobControlProjection:
        """Read only a complete materialized-job control target or fail closed."""

        rows = connection.execute(
            """
            SELECT
                job.job_id AS job_id,
                job.generation AS generation,
                job.revision AS revision,
                job.state AS state,
                domain.authorized AS authorized,
                domain.manual_hold AS manual_hold,
                domain.start_now_requested AS start_now_requested
            FROM jobs AS job
            JOIN materialized_jobs AS domain ON domain.job_id = job.job_id
            WHERE job.job_id = ?
            LIMIT 2
            """,
            (job_id,),
        ).fetchall()
        if len(rows) != 1:
            raise ValueError("job control target is not a materialized job")
        row = rows[0]
        stored_job_id = _require_identifier(
            _require_sqlite_text(row["job_id"], "job control job_id"), "job_id"
        )
        if stored_job_id != job_id:
            raise ValueError("job control target does not match its lookup")
        return _JobControlProjection(
            job=stored_job_id,
            generation=_require_counter(
                _require_sqlite_integer(row["generation"], "job control generation"),
                "job control generation",
            ),
            revision=_require_counter(
                _require_sqlite_integer(row["revision"], "job control revision"),
                "job control revision",
            ),
            state=_require_public_job_state(row["state"], "job control state"),
            authorized=_require_sqlite_boolean(
                row["authorized"], "job control authorized"
            ),
            manual_hold=_require_sqlite_boolean(
                row["manual_hold"], "job control manual_hold"
            ),
            start_now_requested=_require_sqlite_boolean(
                row["start_now_requested"], "job control start_now_requested"
            ),
        )

    @staticmethod
    def _current_queue_gate(connection: sqlite3.Connection) -> str:
        """Read the single durable queue gate required by start-now admission."""

        rows = connection.execute(
            "SELECT value FROM settings WHERE key = 'queue_gate' LIMIT 2"
        ).fetchall()
        if len(rows) != 1:
            raise ValueError("queue gate is not initialized")
        return _require_queue_gate(
            _require_sqlite_text(rows[0]["value"], "queue gate value")
        )

    @staticmethod
    def _persist_job_control_mutation(
        connection: sqlite3.Connection,
        *,
        action: str,
        updated: _JobControlProjection,
    ) -> None:
        """Persist exactly one lifecycle/domain transition and its audit event."""

        connection.execute(
            """
            UPDATE jobs
            SET revision = ?, state = ?
            WHERE job_id = ?
            """,
            (updated.revision, updated.state, updated.job),
        )
        SQLiteStore._require_one_changed_row(connection, "job control lifecycle update")
        connection.execute(
            """
            UPDATE materialized_jobs
            SET authorized = ?, manual_hold = ?, start_now_requested = ?
            WHERE job_id = ?
            """,
            (
                1 if updated.authorized else 0,
                1 if updated.manual_hold else 0,
                1 if updated.start_now_requested else 0,
                updated.job,
            ),
        )
        SQLiteStore._require_one_changed_row(connection, "job control projection update")
        connection.execute(
            """
            INSERT INTO events (kind, job_id, generation, revision)
            VALUES (?, ?, ?, ?)
            """,
            (
                _JOB_CONTROL_EVENT_KINDS[action],
                updated.job,
                updated.generation,
                updated.revision,
            ),
        )

    @staticmethod
    def _require_one_changed_row(connection: sqlite3.Connection, operation: str) -> None:
        row = connection.execute("SELECT changes()").fetchone()
        if row is None or type(row[0]) is not int or row[0] != 1:
            raise RuntimeError(f"{operation} did not affect exactly one row")

    @staticmethod
    def _insert_job_control_receipt(
        connection: sqlite3.Connection,
        *,
        request_id: str,
        payload_digest: str,
        action: str,
        result: JobControlResult,
    ) -> None:
        """Persist the exact original public result for idempotent replay."""

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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                payload_digest,
                result.job,
                action,
                result.status,
                result.generation,
                result.revision,
                result.state,
                1 if result.authorized else 0,
            ),
        )

    @staticmethod
    def _job_control_result_from_receipt(row: sqlite3.Row) -> JobControlResult:
        """Decode a stored command readback without coercing malformed values."""

        return JobControlResult(
            status=_require_job_control_status(
                _require_sqlite_text(row["status"], "job control status")
            ),
            job=_require_identifier(
                _require_sqlite_text(row["job_id"], "job control job_id"), "job_id"
            ),
            generation=_require_counter(
                _require_sqlite_integer(row["generation"], "job control generation"),
                "job control generation",
            ),
            revision=_require_counter(
                _require_sqlite_integer(row["revision"], "job control revision"),
                "job control revision",
            ),
            state=_require_public_job_state(row["state"], "job control state"),
            authorized=_require_sqlite_boolean(
                row["authorized"], "job control authorized"
            ),
        )

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

    @staticmethod
    def _current_worker_epoch(connection: sqlite3.Connection) -> int:
        rows = connection.execute(
            """
            SELECT value
            FROM settings
            WHERE key = 'worker_epoch'
            LIMIT 2
            """
        ).fetchall()
        if len(rows) != 1:
            raise ValueError("current worker epoch is not initialized")
        value = _require_sqlite_text(rows[0]["value"], "worker epoch")
        try:
            epoch = int(value)
        except ValueError as error:
            raise ValueError("persisted worker epoch is invalid") from error
        if str(epoch) != value:
            raise ValueError("persisted worker epoch is invalid")
        return _require_worker_epoch(epoch, "worker epoch")

    def reserve_direct_engine_activation(
        self, *, worker_epoch: int
    ) -> DirectEngineActivationFence | None:
        """Durably claim one current epoch before any direct-engine launch.

        ``None`` is a non-throwing conflict: another direct record or launch fence
        already exists, so callers must treat direct activation as blocked.
        """

        worker_epoch = _require_worker_epoch(worker_epoch, "worker_epoch")
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            if self._current_worker_epoch(connection) != worker_epoch:
                raise ValueError("direct engine activation worker epoch is not current")
            direct_record = connection.execute(
                "SELECT 1 FROM engine_instances WHERE engine_kind = 'direct' LIMIT 1"
            ).fetchone()
            existing_fence = connection.execute(
                """
                SELECT 1
                FROM direct_engine_activation_fences
                WHERE engine_kind = 'direct'
                LIMIT 1
                """
            ).fetchone()
            if direct_record is not None or existing_fence is not None:
                result = None
            else:
                result = DirectEngineActivationFence(
                    worker_epoch=worker_epoch,
                    reservation_token=secrets.token_hex(32),
                )
                connection.execute(
                    """
                    INSERT INTO direct_engine_activation_fences (
                        engine_kind, worker_epoch, reservation_token
                    )
                    VALUES ('direct', ?, ?)
                    """,
                    (worker_epoch, result.reservation_token),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return result

    def get_direct_engine_activation_fence(self) -> DirectEngineActivationFence | None:
        """Read the exact durable direct-engine launch fence, if one exists."""

        rows = self._connection.execute(
            """
            SELECT engine_kind, worker_epoch, reservation_token
            FROM direct_engine_activation_fences
            LIMIT 2
            """
        ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise ValueError("direct engine activation fence is not unique")
        row = rows[0]
        if _require_sqlite_text(row["engine_kind"], "engine kind") != "direct":
            raise ValueError("persisted direct engine activation fence kind is invalid")
        try:
            return DirectEngineActivationFence(
                worker_epoch=_require_worker_epoch(
                    _require_sqlite_integer(row["worker_epoch"], "worker epoch"),
                    "worker epoch",
                ),
                reservation_token=_require_reservation_token(
                    _require_sqlite_text(row["reservation_token"], "reservation token")
                ),
            )
        except (TypeError, ValueError) as error:
            raise ValueError("persisted direct engine activation fence is invalid") from error

    def clear_direct_engine_activation_fence(
        self, fence: DirectEngineActivationFence
    ) -> bool:
        """Compare and clear only the exact direct-engine activation fence."""

        if type(fence) is not DirectEngineActivationFence:
            raise TypeError("fence must be a DirectEngineActivationFence")
        fence = DirectEngineActivationFence(
            worker_epoch=fence.worker_epoch,
            reservation_token=fence.reservation_token,
        )
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                """
                DELETE FROM direct_engine_activation_fences
                WHERE engine_kind = 'direct'
                  AND worker_epoch = ?
                  AND reservation_token = ?
                """,
                (fence.worker_epoch, fence.reservation_token),
            )
            changed = connection.execute("SELECT changes()").fetchone()
            if changed is None or type(changed[0]) is not int or changed[0] not in {0, 1}:
                raise RuntimeError("direct engine activation fence delete is invalid")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return bool(changed[0])

    def bind_direct_engine_activation_fence(
        self, fence: DirectEngineActivationFence, record: DirectEngineRecord
    ) -> None:
        """Atomically replace one reserved fence with its direct-engine identity."""

        if type(fence) is not DirectEngineActivationFence:
            raise TypeError("fence must be a DirectEngineActivationFence")
        if type(record) is not DirectEngineRecord:
            raise TypeError("record must be a DirectEngineRecord")
        fence = DirectEngineActivationFence(
            worker_epoch=fence.worker_epoch,
            reservation_token=fence.reservation_token,
        )
        record = DirectEngineRecord(
            worker_epoch=record.worker_epoch,
            identity=record.identity,
        )
        if record.worker_epoch != fence.worker_epoch:
            raise ValueError(
                "direct engine record worker epoch does not match activation fence"
            )

        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            if self._current_worker_epoch(connection) != fence.worker_epoch:
                raise ValueError("direct engine activation fence worker epoch is not current")
            fence_rows = connection.execute(
                """
                SELECT engine_kind, worker_epoch, reservation_token
                FROM direct_engine_activation_fences
                LIMIT 2
                """
            ).fetchall()
            if len(fence_rows) != 1:
                raise ValueError("direct engine activation fence is not present")
            persisted_fence = fence_rows[0]
            try:
                current_fence = DirectEngineActivationFence(
                    worker_epoch=_require_worker_epoch(
                        _require_sqlite_integer(
                            persisted_fence["worker_epoch"], "worker epoch"
                        ),
                        "worker epoch",
                    ),
                    reservation_token=_require_reservation_token(
                        _require_sqlite_text(
                            persisted_fence["reservation_token"], "reservation token"
                        )
                    ),
                )
            except (TypeError, ValueError) as error:
                raise ValueError("persisted direct engine activation fence is invalid") from error
            if (
                _require_sqlite_text(persisted_fence["engine_kind"], "engine kind")
                != "direct"
                or current_fence != fence
            ):
                raise ValueError("direct engine activation fence does not match")
            direct_record = connection.execute(
                "SELECT 1 FROM engine_instances WHERE engine_kind = 'direct' LIMIT 1"
            ).fetchone()
            if direct_record is not None:
                raise ValueError("direct engine record is already present")

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
            connection.execute(
                """
                DELETE FROM direct_engine_activation_fences
                WHERE engine_kind = 'direct'
                  AND worker_epoch = ?
                  AND reservation_token = ?
                """,
                (fence.worker_epoch, fence.reservation_token),
            )
            changed = connection.execute("SELECT changes()").fetchone()
            if changed is None or type(changed[0]) is not int or changed[0] != 1:
                raise RuntimeError("direct engine activation fence bind delete is invalid")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def set_direct_engine_record(self, record: DirectEngineRecord) -> None:
        """Create or replace the direct engine record for the current worker epoch."""

        if type(record) is not DirectEngineRecord:
            raise TypeError("record must be a DirectEngineRecord")
        record = DirectEngineRecord(
            worker_epoch=record.worker_epoch,
            identity=record.identity,
        )
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            if self._current_worker_epoch(connection) != record.worker_epoch:
                raise ValueError("direct engine worker epoch is not current")
            activation_fence = connection.execute(
                """
                SELECT 1
                FROM direct_engine_activation_fences
                WHERE engine_kind = 'direct'
                LIMIT 1
                """
            ).fetchone()
            if activation_fence is not None:
                raise ValueError("direct engine record conflicts with an activation fence")
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
                ON CONFLICT(engine_kind) DO UPDATE SET
                    worker_epoch = excluded.worker_epoch,
                    leader_pid = excluded.leader_pid,
                    process_group_id = excluded.process_group_id,
                    session_id = excluded.session_id,
                    owner_uid = excluded.owner_uid,
                    started_unix_us = excluded.started_unix_us,
                    argv_sha256 = excluded.argv_sha256
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
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def get_direct_engine_record(self) -> DirectEngineRecord | None:
        """Read the one exact durable direct-engine record, if present."""

        rows = self._connection.execute(
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
            LIMIT 2
            """
        ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise ValueError("direct engine record is not unique")
        row = rows[0]
        if _require_sqlite_text(row["engine_kind"], "engine kind") != "direct":
            raise ValueError("persisted engine kind is invalid")
        try:
            return DirectEngineRecord(
                worker_epoch=_require_worker_epoch(
                    _require_sqlite_integer(row["worker_epoch"], "worker epoch"),
                    "worker epoch",
                ),
                identity=ProcessBirthIdentity.from_record(
                    {
                        "leader_pid": _require_sqlite_integer(
                            row["leader_pid"], "leader_pid"
                        ),
                        "process_group_id": _require_sqlite_integer(
                            row["process_group_id"], "process_group_id"
                        ),
                        "session_id": _require_sqlite_integer(
                            row["session_id"], "session_id"
                        ),
                        "owner_uid": _require_sqlite_integer(
                            row["owner_uid"], "owner_uid"
                        ),
                        "started_unix_us": _require_sqlite_integer(
                            row["started_unix_us"], "started_unix_us"
                        ),
                        "argv_sha256": _require_sqlite_text(
                            row["argv_sha256"], "argv_sha256"
                        ),
                    }
                ),
            )
        except (TypeError, ValueError) as error:
            raise ValueError("persisted direct engine record is invalid") from error

    def clear_direct_engine_record(self, record: DirectEngineRecord) -> bool:
        """Delete only the exact direct-engine record supplied by the caller."""

        if type(record) is not DirectEngineRecord:
            raise TypeError("record must be a DirectEngineRecord")
        record = DirectEngineRecord(
            worker_epoch=record.worker_epoch,
            identity=record.identity,
        )
        identity = record.identity
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                """
                DELETE FROM engine_instances
                WHERE engine_kind = 'direct'
                  AND worker_epoch = ?
                  AND leader_pid = ?
                  AND process_group_id = ?
                  AND session_id = ?
                  AND owner_uid = ?
                  AND started_unix_us = ?
                  AND argv_sha256 = ?
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
            changed = connection.execute("SELECT changes()").fetchone()
            if changed is None or type(changed[0]) is not int or changed[0] not in {0, 1}:
                raise RuntimeError("direct engine record delete is invalid")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return bool(changed[0])

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
