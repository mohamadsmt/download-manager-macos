"""SQLite-backed transactional persistence for queue commands."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Final

from hermes_downloads.models import DownloadIntent

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


class RequestConflictError(ValueError):
    """Raised when a request ID is reused with a different payload digest."""


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
        self._connection = sqlite3.connect(self.database_path, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.executescript(_SCHEMA)

    def close(self) -> None:
        """Release the SQLite connection."""

        self._connection.close()

    def apply_add(self, intent: DownloadIntent) -> CommandResult:
        """Atomically persist one queued job, command receipt, and event."""

        if not isinstance(intent, DownloadIntent):
            raise TypeError("intent must be a DownloadIntent")

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

    def queue_gate(self) -> str | None:
        """Return the persisted global admission gate, if initialized."""

        row = self._connection.execute(
            "SELECT value FROM settings WHERE key = 'queue_gate'"
        ).fetchone()
        return None if row is None else row["value"]

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

    def list_jobs(self) -> tuple[JobRecord, ...]:
        """Read all persisted jobs in stable job-ID order."""

        rows = self._connection.execute(
            """
            SELECT job_id, source_url, generation, revision, state
            FROM jobs
            ORDER BY job_id
            """
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

    def list_events(self) -> tuple[EventRecord, ...]:
        """Read durable events in insertion order."""

        rows = self._connection.execute(
            """
            SELECT event_id, kind, job_id, generation, revision
            FROM events
            ORDER BY event_id
            """
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
