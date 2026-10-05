"""SQLite-backed transactional persistence for queue commands."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
import re
import hashlib
import json
import stat
import secrets
import sqlite3
import time
from typing import TYPE_CHECKING, Final

from hermes_downloads.models import (
    Admission,
    DownloadIntent,
    JobState,
    MaterializedJob,
    PublicationReservation,
    SourceKind,
)
from hermes_downloads.processes import ProcessBirthIdentity
from hermes_downloads.ipc import (AddBatchCommand, AddBatchResult, AddBatchEntryResult,
    JobControlCommand, _batch_entry, _batch_canonical, _batch_decode, _batch_collection_id,
    _batch_component, _MAX_BATCH_REPLY)
from hermes_downloads.network import validate_source_url, SourcePolicyError
from hermes_downloads.retry import (
    CompletionVerification,
    RetryAuditEvent,
    RetryAuditKind,
    RetryAuthority,
    RetryBudget,
    RetryPolicy,
)

if TYPE_CHECKING:
    from hermes_downloads.direct import DirectTransfer
    from hermes_downloads.paths import StagedPartialPayload

__all__ = [
    "CommandRecord",
    "CommandResult",
    "DirectDispatchResult",
    "DirectEngineActivationFence",
    "DirectEngineRecord",
    "EventRecord",
    "JobControlResult",
    "JobPageRecord",
    "JobRecord",
    "PublicationMarkerBinding",
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

_PUBLICATION_RESERVATIONS_SCHEMA: Final = """
CREATE TABLE publication_reservations (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES materialized_jobs(job_id) ON DELETE CASCADE CHECK (
        typeof(job_id) = 'text'
        AND length(job_id) BETWEEN 1 AND 128
        AND substr(job_id, 1, 1) GLOB '[A-Za-z0-9]'
        AND job_id NOT GLOB '*[^A-Za-z0-9._:-]*'
    ),
    target_component TEXT NOT NULL CHECK (
        typeof(target_component) = 'text'
        AND length(target_component) BETWEEN 1 AND 255
        AND target_component NOT IN ('.', '..')
        AND substr(target_component, 1, 1) != '.'
        AND instr(target_component, '/') = 0
        AND instr(target_component, char(92)) = 0
        AND instr(target_component, char(0)) = 0
        AND target_component NOT GLOB (
            '*[' || char(1) || '-' || char(31) || char(127) || '-' || char(159) || ']*'
        )
    ),
    final_filename TEXT NOT NULL CHECK (
        typeof(final_filename) = 'text'
        AND length(final_filename) BETWEEN 1 AND 255
        AND final_filename NOT IN ('.', '..')
        AND substr(final_filename, 1, 1) != '.'
        AND instr(final_filename, '/') = 0
        AND instr(final_filename, char(92)) = 0
        AND instr(final_filename, char(0)) = 0
        AND final_filename NOT GLOB (
            '*[' || char(1) || '-' || char(31) || char(127) || '-' || char(159) || ']*'
        )
    ),
    claim_token TEXT NOT NULL CHECK (
        typeof(claim_token) = 'text'
        AND length(claim_token) = 64
        AND claim_token NOT GLOB '*[^0-9a-f]*'
    ),
    UNIQUE(target_component, final_filename)
);
"""

_PUBLICATION_MARKER_BINDINGS_SCHEMA: Final = """
CREATE TABLE publication_marker_bindings (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES publication_reservations(job_id) ON DELETE CASCADE CHECK (
        typeof(job_id) = 'text'
        AND length(job_id) BETWEEN 1 AND 128
        AND substr(job_id, 1, 1) GLOB '[A-Za-z0-9]'
        AND job_id NOT GLOB '*[^A-Za-z0-9._:-]*'
    ),
    marker_device INTEGER NOT NULL CHECK (
        typeof(marker_device) = 'integer'
        AND marker_device >= 0
        AND marker_device <= 9223372036854775807
    ),
    marker_inode INTEGER NOT NULL CHECK (
        typeof(marker_inode) = 'integer'
        AND marker_inode >= 0
        AND marker_inode <= 9223372036854775807
    )
);
"""

_STAGED_PAYLOAD_BINDINGS_SCHEMA: Final = """
CREATE TABLE staged_payload_bindings (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES publication_marker_bindings(job_id) ON DELETE CASCADE CHECK (
        typeof(job_id) = 'text'
        AND length(job_id) BETWEEN 1 AND 128
        AND substr(job_id, 1, 1) GLOB '[A-Za-z0-9]'
        AND job_id NOT GLOB '*[^A-Za-z0-9._:-]*'
    ),
    partial_device INTEGER NOT NULL CHECK (
        typeof(partial_device) = 'integer'
        AND partial_device >= 0
        AND partial_device <= 9223372036854775807
    ),
    partial_inode INTEGER NOT NULL CHECK (
        typeof(partial_inode) = 'integer'
        AND partial_inode >= 0
        AND partial_inode <= 9223372036854775807
    ),
    logical_size INTEGER NOT NULL CHECK (
        typeof(logical_size) = 'integer'
        AND logical_size >= 0
        AND logical_size <= 9223372036854775807
    )
);
"""

_FINAL_PUBLICATION_BINDINGS_SCHEMA: Final = """
CREATE TABLE final_publication_bindings (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES staged_payload_bindings(job_id) ON DELETE CASCADE CHECK (
        typeof(job_id) = 'text'
        AND length(job_id) BETWEEN 1 AND 128
        AND substr(job_id, 1, 1) GLOB '[A-Za-z0-9]'
        AND job_id NOT GLOB '*[^A-Za-z0-9._:-]*'
    ),
    final_device INTEGER NOT NULL CHECK (
        typeof(final_device) = 'integer'
        AND final_device >= 0
        AND final_device <= 9223372036854775807
    ),
    final_inode INTEGER NOT NULL CHECK (
        typeof(final_inode) = 'integer'
        AND final_inode >= 0
        AND final_inode <= 9223372036854775807
    ),
    logical_size INTEGER NOT NULL CHECK (
        typeof(logical_size) = 'integer'
        AND logical_size >= 0
        AND logical_size <= 9223372036854775807
    )
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

_V7_JOB_CONTROL_COMMANDS_SCHEMA: Final = """
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

_JOB_CONTROL_COMMANDS_SCHEMA: Final = """
CREATE TABLE job_control_commands (
    request_id TEXT PRIMARY KEY,
    payload_digest TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    action TEXT NOT NULL CHECK (action IN ('pause', 'resume', 'start_now', 'remove')),
    status TEXT NOT NULL CHECK (status IN ('applied', 'blocked', 'stale')),
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    state TEXT NOT NULL,
    authorized INTEGER NOT NULL CHECK (authorized IN (0, 1))
);
"""

_V8_COMMAND_RECEIPTS_SCHEMA: Final = """
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
        OR (scope = 'job_control' AND action IN ('pause', 'resume', 'start_now', 'remove'))
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

_DIRECT_ENGINE_RECOVERY_CAPABILITIES_SCHEMA: Final = """
CREATE TABLE direct_engine_recovery_capabilities (
    engine_kind TEXT PRIMARY KEY NOT NULL CHECK (engine_kind = 'direct'),
    worker_epoch INTEGER NOT NULL CHECK (
        typeof(worker_epoch) = 'integer' AND worker_epoch > 0
    ),
    leader_pid INTEGER NOT NULL CHECK (
        typeof(leader_pid) = 'integer' AND leader_pid > 0
    ),
    process_group_id INTEGER NOT NULL CHECK (
        typeof(process_group_id) = 'integer' AND process_group_id = leader_pid
    ),
    session_id INTEGER NOT NULL CHECK (
        typeof(session_id) = 'integer' AND session_id = leader_pid
    ),
    owner_uid INTEGER NOT NULL CHECK (
        typeof(owner_uid) = 'integer' AND owner_uid >= 0
    ),
    started_unix_us INTEGER NOT NULL CHECK (
        typeof(started_unix_us) = 'integer' AND started_unix_us > 0
    ),
    argv_sha256 TEXT NOT NULL CHECK (
        typeof(argv_sha256) = 'text'
        AND length(argv_sha256) = 64
        AND argv_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    rpc_port INTEGER NOT NULL CHECK (
        typeof(rpc_port) = 'integer' AND rpc_port BETWEEN 1024 AND 65535
    ),
    rpc_secret TEXT NOT NULL CHECK (
        typeof(rpc_secret) = 'text'
        AND length(rpc_secret) = 43
        AND rpc_secret NOT GLOB '*[^A-Za-z0-9_-]*'
    ),
    FOREIGN KEY (engine_kind) REFERENCES engine_instances(engine_kind)
);
"""

_DIRECT_DISPATCH_COMMANDS_SCHEMA: Final = """
CREATE TABLE direct_dispatch_commands (
    request_id TEXT PRIMARY KEY NOT NULL CHECK (
        typeof(request_id) = 'text'
        AND length(request_id) BETWEEN 1 AND 128
        AND substr(request_id, 1, 1) GLOB '[A-Za-z0-9]'
        AND request_id NOT GLOB '*[^A-Za-z0-9._:-]*'
    ),
    payload_digest TEXT NOT NULL CHECK (
        typeof(payload_digest) = 'text'
        AND length(payload_digest) = 64
        AND payload_digest NOT GLOB '*[^0-9a-f]*'
    ),
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    status TEXT NOT NULL CHECK (status IN ('pending', 'started', 'blocked', 'stale')),
    generation INTEGER NOT NULL CHECK (
        typeof(generation) = 'integer'
        AND generation >= 0
        AND generation <= 9223372036854775807
    ),
    revision INTEGER NOT NULL CHECK (
        typeof(revision) = 'integer'
        AND revision >= 0
        AND revision <= 9223372036854775807
    ),
    state TEXT NOT NULL
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


_DIRECT_PUBLICATION_ATTEMPTS_SCHEMA: Final = """
CREATE TABLE direct_publication_attempts (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES jobs(job_id) CHECK (typeof(job_id) = 'text' AND length(job_id) BETWEEN 1 AND 128),
    attempt_id TEXT NOT NULL UNIQUE CHECK (typeof(attempt_id) = 'text' AND length(attempt_id) BETWEEN 1 AND 128),
    original_request_id TEXT NOT NULL UNIQUE REFERENCES direct_dispatch_commands(request_id) CHECK (typeof(original_request_id) = 'text'),
    proof TEXT NOT NULL CHECK (typeof(proof) = 'text' AND length(proof) BETWEEN 1 AND 4096),
    status TEXT NOT NULL CHECK (typeof(status) = 'text' AND status IN ('eligible', 'finished', 'closed')),
    audit_id INTEGER NOT NULL REFERENCES events(event_id) CHECK (typeof(audit_id) = 'integer' AND audit_id > 0),
    generation INTEGER NOT NULL CHECK (typeof(generation) = 'integer' AND generation BETWEEN 0 AND 9223372036854775807),
    revision INTEGER NOT NULL CHECK (typeof(revision) = 'integer' AND revision BETWEEN 0 AND 9223372036854775807),
    state TEXT NOT NULL CHECK (typeof(state) = 'text' AND state IN ('finalizing', 'paused', 'completed', 'removed', 'queued')),
    worker_epoch INTEGER NOT NULL CHECK (typeof(worker_epoch) = 'integer' AND worker_epoch BETWEEN 1 AND 9223372036854775807),
    pending_request_id TEXT UNIQUE REFERENCES direct_dispatch_commands(request_id) CHECK (pending_request_id IS NULL OR typeof(pending_request_id) = 'text'),
    CHECK (status != 'eligible' OR state IN ('finalizing', 'paused')),
    CHECK (status != 'finished' OR (state = 'completed' AND pending_request_id IS NULL))
);
"""
_CLOSED_DIRECT_PUBLICATION_ATTEMPTS_SCHEMA: Final = """
CREATE TABLE closed_direct_publication_attempts (
    job_id TEXT NOT NULL REFERENCES jobs(job_id) CHECK (typeof(job_id) = 'text' AND length(job_id) BETWEEN 1 AND 128),
    attempt_id TEXT PRIMARY KEY NOT NULL CHECK (typeof(attempt_id) = 'text' AND length(attempt_id) BETWEEN 1 AND 128),
    original_request_id TEXT NOT NULL UNIQUE REFERENCES direct_dispatch_commands(request_id) CHECK (typeof(original_request_id) = 'text'),
    proof TEXT NOT NULL CHECK (typeof(proof) = 'text' AND length(proof) BETWEEN 1 AND 4096),
    status TEXT NOT NULL CHECK (typeof(status) = 'text' AND status = 'closed'),
    audit_id INTEGER NOT NULL REFERENCES events(event_id) CHECK (typeof(audit_id) = 'integer' AND audit_id > 0),
    generation INTEGER NOT NULL CHECK (typeof(generation) = 'integer' AND generation BETWEEN 0 AND 9223372036854775807),
    revision INTEGER NOT NULL CHECK (typeof(revision) = 'integer' AND revision BETWEEN 0 AND 9223372036854775807),
    state TEXT NOT NULL CHECK (typeof(state) = 'text' AND state IN ('paused', 'removed', 'queued')),
    worker_epoch INTEGER NOT NULL CHECK (typeof(worker_epoch) = 'integer' AND worker_epoch BETWEEN 1 AND 9223372036854775807),
    pending_request_id TEXT CHECK (pending_request_id IS NULL)
);
"""
_SUPPORTED_SCHEMA_VERSION: Final = 17
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
    _V7_JOB_CONTROL_COMMANDS_SCHEMA,
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
    _V7_JOB_CONTROL_COMMANDS_SCHEMA,
    _V8_COMMAND_RECEIPTS_SCHEMA,
)
_V9_TABLE_SCHEMAS: Final = _expected_table_schemas(
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
_V10_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _PUBLICATION_RESERVATIONS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
    _JOB_RETRY_SCHEMA,
    _JOB_RETRY_AUDIT_SCHEMA,
    _QUEUE_COMMANDS_SCHEMA,
    _ENGINE_INSTANCES_SCHEMA,
    _DIRECT_ENGINE_ACTIVATION_FENCES_SCHEMA,
    _JOB_CONTROL_COMMANDS_SCHEMA,
    _COMMAND_RECEIPTS_SCHEMA,
)
_V11_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _PUBLICATION_RESERVATIONS_SCHEMA,
    _PUBLICATION_MARKER_BINDINGS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
    _JOB_RETRY_SCHEMA,
    _JOB_RETRY_AUDIT_SCHEMA,
    _QUEUE_COMMANDS_SCHEMA,
    _ENGINE_INSTANCES_SCHEMA,
    _DIRECT_ENGINE_ACTIVATION_FENCES_SCHEMA,
    _JOB_CONTROL_COMMANDS_SCHEMA,
    _COMMAND_RECEIPTS_SCHEMA,
)
_V12_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _PUBLICATION_RESERVATIONS_SCHEMA,
    _PUBLICATION_MARKER_BINDINGS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
    _JOB_RETRY_SCHEMA,
    _JOB_RETRY_AUDIT_SCHEMA,
    _QUEUE_COMMANDS_SCHEMA,
    _ENGINE_INSTANCES_SCHEMA,
    _DIRECT_ENGINE_ACTIVATION_FENCES_SCHEMA,
    _DIRECT_ENGINE_RECOVERY_CAPABILITIES_SCHEMA,
    _JOB_CONTROL_COMMANDS_SCHEMA,
    _COMMAND_RECEIPTS_SCHEMA,
)
_V13_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _PUBLICATION_RESERVATIONS_SCHEMA,
    _PUBLICATION_MARKER_BINDINGS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
    _JOB_RETRY_SCHEMA,
    _JOB_RETRY_AUDIT_SCHEMA,
    _QUEUE_COMMANDS_SCHEMA,
    _ENGINE_INSTANCES_SCHEMA,
    _DIRECT_ENGINE_ACTIVATION_FENCES_SCHEMA,
    _DIRECT_ENGINE_RECOVERY_CAPABILITIES_SCHEMA,
    _DIRECT_DISPATCH_COMMANDS_SCHEMA,
    _JOB_CONTROL_COMMANDS_SCHEMA,
    _COMMAND_RECEIPTS_SCHEMA,
)
_V14_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _PUBLICATION_RESERVATIONS_SCHEMA,
    _PUBLICATION_MARKER_BINDINGS_SCHEMA,
    _STAGED_PAYLOAD_BINDINGS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
    _JOB_RETRY_SCHEMA,
    _JOB_RETRY_AUDIT_SCHEMA,
    _QUEUE_COMMANDS_SCHEMA,
    _ENGINE_INSTANCES_SCHEMA,
    _DIRECT_ENGINE_ACTIVATION_FENCES_SCHEMA,
    _DIRECT_ENGINE_RECOVERY_CAPABILITIES_SCHEMA,
    _DIRECT_DISPATCH_COMMANDS_SCHEMA,
    _JOB_CONTROL_COMMANDS_SCHEMA,
    _COMMAND_RECEIPTS_SCHEMA,
)
_V15_TABLE_SCHEMAS: Final = _expected_table_schemas(
    _SCHEMA,
    _MATERIALIZED_JOBS_SCHEMA,
    _PUBLICATION_RESERVATIONS_SCHEMA,
    _PUBLICATION_MARKER_BINDINGS_SCHEMA,
    _STAGED_PAYLOAD_BINDINGS_SCHEMA,
    _FINAL_PUBLICATION_BINDINGS_SCHEMA,
    _COLLECTION_HOLDS_SCHEMA,
    _JOB_RETRY_SCHEMA,
    _JOB_RETRY_AUDIT_SCHEMA,
    _QUEUE_COMMANDS_SCHEMA,
    _ENGINE_INSTANCES_SCHEMA,
    _DIRECT_ENGINE_ACTIVATION_FENCES_SCHEMA,
    _DIRECT_ENGINE_RECOVERY_CAPABILITIES_SCHEMA,
    _DIRECT_DISPATCH_COMMANDS_SCHEMA,
    _JOB_CONTROL_COMMANDS_SCHEMA,
    _COMMAND_RECEIPTS_SCHEMA,
)
_V16_TABLE_SCHEMAS: Final = dict(_V15_TABLE_SCHEMAS,
    **_expected_table_schemas(_DIRECT_PUBLICATION_ATTEMPTS_SCHEMA,
        _CLOSED_DIRECT_PUBLICATION_ATTEMPTS_SCHEMA))
_V17_COMMAND_RECEIPTS_SCHEMA: Final = """
CREATE TABLE command_receipts (
    request_id TEXT PRIMARY KEY,
    payload_digest TEXT NOT NULL CHECK (
        length(payload_digest) = 64 AND payload_digest NOT GLOB '*[^0-9a-f]*'
    ),
    scope TEXT NOT NULL CHECK (scope IN ('add', 'queue_gate', 'job_control', 'add_batch', 'add_batch_entry')),
    action TEXT NOT NULL CHECK (
        (scope = 'add' AND action = 'add')
        OR (scope = 'queue_gate' AND action = 'queue_gate')
        OR (scope = 'job_control' AND action IN ('pause', 'resume', 'start_now', 'remove'))
        OR (scope = 'add_batch' AND action = 'add_batch')
        OR (scope = 'add_batch_entry' AND action = 'add_batch_entry')
    )
);
"""
_ADD_BATCH_COMMANDS_SCHEMA: Final = """
CREATE TABLE add_batch_commands (
    request_id TEXT PRIMARY KEY NOT NULL REFERENCES command_receipts(request_id),
    payload_digest TEXT NOT NULL CHECK (length(payload_digest)=64 AND payload_digest NOT GLOB '*[^0-9a-f]*'),
    entry_count INTEGER NOT NULL CHECK (entry_count BETWEEN 1 AND 500),
    receipt BLOB NOT NULL CHECK (length(receipt) BETWEEN 1 AND 262144)
) STRICT;
"""
_ADD_BATCH_ENTRIES_SCHEMA: Final = """
CREATE TABLE add_batch_entries (
    parent_request_id TEXT NOT NULL REFERENCES add_batch_commands(request_id) DEFERRABLE INITIALLY DEFERRED,
    entry_index INTEGER NOT NULL CHECK (entry_index BETWEEN 0 AND 499),
    status TEXT NOT NULL CHECK (status IN ('applied','blocked')),
    reason TEXT,
    job_id TEXT UNIQUE REFERENCES jobs(job_id),
    child_request_id TEXT UNIQUE REFERENCES commands(request_id),
    generation INTEGER,
    revision INTEGER,
    order_key INTEGER,
    creation_intent_blob BLOB,
    creation_intent_digest TEXT,
    PRIMARY KEY (parent_request_id, entry_index),
    CHECK (
        (status='applied' AND reason IS NULL AND job_id IS NOT NULL AND child_request_id IS NOT NULL
         AND generation IS NOT NULL AND generation=0 AND revision IS NOT NULL AND revision=0
         AND order_key IS NOT NULL AND order_key>=0 AND creation_intent_blob IS NOT NULL
         AND length(creation_intent_blob) BETWEEN 1 AND 40960 AND creation_intent_digest IS NOT NULL
         AND length(creation_intent_digest)=64 AND creation_intent_digest NOT GLOB '*[^0-9a-f]*')
        OR
        (status='blocked' AND reason IS NOT NULL
         AND reason IN ('invalid_entry','invalid_source','invalid_destination','job_conflict','destination_conflict','request_conflict')
         AND job_id IS NULL AND child_request_id IS NULL AND generation IS NULL AND revision IS NULL
         AND order_key IS NULL AND creation_intent_blob IS NULL AND creation_intent_digest IS NULL)
    )
) STRICT;
"""
_V17_TABLE_SCHEMAS: Final = dict(_V16_TABLE_SCHEMAS,
    **_expected_table_schemas(_V17_COMMAND_RECEIPTS_SCHEMA,
        _ADD_BATCH_COMMANDS_SCHEMA, _ADD_BATCH_ENTRIES_SCHEMA))
_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SHA256_DIGEST: Final = re.compile(r"[0-9a-f]{64}\Z")
_DIRECT_ENGINE_RECOVERY_SECRET: Final = re.compile(r"[A-Za-z0-9_-]{43}\Z")
_QUEUE_GATES: Final = frozenset({"paused", "running"})
_JOB_CONTROL_ACTIONS: Final = frozenset({"pause", "resume", "start_now", "remove"})
_COMMAND_RECEIPT_SCOPES: Final = frozenset({"add", "queue_gate", "job_control", "add_batch", "add_batch_entry"})
_ADD_COMMAND_SCOPE: Final = "add"
_ADD_COMMAND_ACTION: Final = "add"
_QUEUE_GATE_COMMAND_SCOPE: Final = "queue_gate"
_QUEUE_GATE_COMMAND_ACTION: Final = "queue_gate"
_JOB_CONTROL_COMMAND_SCOPE: Final = "job_control"
_JOB_CONTROL_STATUSES: Final = frozenset({"applied", "blocked", "stale"})
_DIRECT_DISPATCH_STATUSES: Final = frozenset({"started", "blocked", "stale"})
_TERMINAL_JOB_CONTROL_STATES: Final = frozenset(
    {"removed", "completed", "cancelled", "failed"}
)
_ACTIVE_JOB_CONTROL_STATES: Final = frozenset(
    {"resolving", "downloading", "pausing", "finalizing"}
)
_JOB_CONTROL_EVENT_KINDS: Final = {
    "pause": "job_paused",
    "resume": "job_resumed",
    "start_now": "job_start_now_requested",
    "remove": "job_removed",
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
        or (scope in {'add_batch', 'add_batch_entry'} and value == scope)
    ):
        return value
    raise ValueError("persisted command receipt action is invalid")


def _require_job_control_status(value: object) -> str:
    if type(value) is not str:
        raise TypeError("status must be a string")
    if value not in _JOB_CONTROL_STATUSES:
        raise ValueError("status is not a job-control status")
    return value


def _require_direct_dispatch_status(value: object) -> str:
    if type(value) is not str:
        raise TypeError("status must be a string")
    if value not in _DIRECT_DISPATCH_STATUSES:
        raise ValueError("status is not a direct-dispatch status")
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


def _require_direct_engine_recovery_port(value: object) -> int:
    if type(value) is not int or not 1024 <= value <= 65535:
        raise ValueError("rpc_port must be a loopback TCP port")
    return value


def _require_direct_engine_recovery_secret(value: object) -> str:
    if type(value) is not str or _DIRECT_ENGINE_RECOVERY_SECRET.fullmatch(value) is None:
        raise ValueError("rpc_secret is invalid")
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
class DirectDispatchResult:
    """A durable, redacted direct-dispatch readback."""

    status: str
    job: str
    generation: int
    revision: int
    state: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", _require_direct_dispatch_status(self.status))
        object.__setattr__(self, "job", _require_identifier(self.job, "job"))
        object.__setattr__(
            self, "generation", _require_counter(self.generation, "generation")
        )
        object.__setattr__(self, "revision", _require_counter(self.revision, "revision"))
        object.__setattr__(
            self, "state", _require_public_job_state(self.state, "direct dispatch state")
        )


@dataclass(frozen=True, slots=True, repr=False)
class _DirectDispatchPlan:
    """Private persisted inputs for one still-paused engine admission."""

    job: MaterializedJob
    reservation: PublicationReservation
    admission: Admission
    generation: int
    revision: int
    request_id: str
    payload_digest: str


@dataclass(frozen=True, slots=True, repr=False)
class _DirectTerminalPlan:
    """Exact started dispatch and its owned engine/marker/GID observation fence."""

    dispatch: _DirectDispatchPlan
    record: DirectEngineRecord
    marker: PublicationMarkerBinding
    gid: str
    partial_path: Path
    capability: _DirectEngineRecoveryCapability | None = None


@dataclass(frozen=True, slots=True, repr=False)
class _DirectStagePlan:
    """One live finalizing cutpoint, retaining its originating STARTED receipt."""

    terminal: _DirectTerminalPlan
    observed: DirectTransfer
    job: MaterializedJob
    revision: int
    audit_id: int
    receipt: _DirectDispatchCommand
    capability: _DirectEngineRecoveryCapability


@dataclass(frozen=True, slots=True, repr=False)
class _DirectPublicationAttempt:
    attempt_id: str
    job: MaterializedJob
    prepared: object
    proof: str
    audit_id: int
    generation: int
    revision: int
    state: str
    worker_epoch: int
    pending_request_id: str | None = None
    pending_payload_digest: str | None = None


@dataclass(frozen=True, slots=True, repr=False)
class _DirectAttemptRecoveryPlan:
    attempt: _DirectPublicationAttempt
    request_id: str
    payload_digest: str


@dataclass(frozen=True, slots=True, repr=False)
class _DirectPublicationReconciliationPlan:
    """Private evidence required to finish an already-published final payload."""

    job: MaterializedJob
    reservation: PublicationReservation
    marker: PublicationMarkerBinding
    staged: _StagedPayloadBinding
    generation: int
    revision: int
    request_id: str
    payload_digest: str


@dataclass(frozen=True, slots=True)
class _DirectDispatchCommand:
    """Internal direct-dispatch idempotency receipt."""

    request_id: str
    payload_digest: str
    job: str
    status: str
    generation: int
    revision: int
    state: str

    def to_result(self) -> DirectDispatchResult:
        if self.status == "pending":
            raise ValueError("pending direct dispatch has no public result")
        return DirectDispatchResult(
            status=self.status,
            job=self.job,
            generation=self.generation,
            revision=self.revision,
            state=self.state,
        )


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
class PublicationMarkerBinding:
    """The durable filesystem identity of one job-local publication marker."""

    job_id: str
    marker_device: int
    marker_inode: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "job_id", _require_identifier(self.job_id, "job_id"))
        object.__setattr__(
            self,
            "marker_device",
            _require_counter(self.marker_device, "marker_device"),
        )
        object.__setattr__(
            self,
            "marker_inode",
            _require_counter(self.marker_inode, "marker_inode"),
        )


@dataclass(frozen=True, slots=True)
class _StagedPayloadBinding:
    """Private durable identity for one verified staged partial payload."""

    job_id: str
    partial_device: int
    partial_inode: int
    logical_size: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "job_id", _require_identifier(self.job_id, "job_id"))
        object.__setattr__(
            self,
            "partial_device",
            _require_counter(self.partial_device, "partial_device"),
        )
        object.__setattr__(
            self,
            "partial_inode",
            _require_counter(self.partial_inode, "partial_inode"),
        )
        object.__setattr__(
            self,
            "logical_size",
            _require_counter(self.logical_size, "logical_size"),
        )


@dataclass(frozen=True, slots=True)
class _FinalPublicationBinding:
    """Private durable identity of a verified same-inode final publication."""

    job_id: str
    final_device: int
    final_inode: int
    logical_size: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "job_id", _require_identifier(self.job_id, "job_id"))
        object.__setattr__(
            self,
            "final_device",
            _require_counter(self.final_device, "final_device"),
        )
        object.__setattr__(
            self,
            "final_inode",
            _require_counter(self.final_inode, "final_inode"),
        )
        object.__setattr__(
            self,
            "logical_size",
            _require_counter(self.logical_size, "logical_size"),
        )


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


@dataclass(frozen=True, slots=True, repr=False)
class _DirectEngineRecoveryCapability:
    """Private loopback shutdown authority paired to one direct-engine record."""

    rpc_port: int
    rpc_secret: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "rpc_port",
            _require_direct_engine_recovery_port(self.rpc_port),
        )
        object.__setattr__(
            self,
            "rpc_secret",
            _require_direct_engine_recovery_secret(self.rpc_secret),
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(rpc_port={self.rpc_port}, rpc_secret=<redacted>)"


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


@dataclass(frozen=True, slots=True, repr=False)
class _BatchCreationIntent:
    parent_request_id: str
    parent_payload_digest: str
    index: int
    audit_id: int
    seal: str
    original_job: MaterializedJob = field(repr=False)
    reservation: PublicationReservation = field(repr=False)
    expected_sha256: str | None = field(repr=False)


class _BatchEntryBlocked(ValueError):
    """Only these explicitly detected domain outcomes may survive a savepoint."""


def _batch_child_id(parent: str, index: int) -> str:
    return 'batch-entry:' + hashlib.sha256(parent.encode('utf-8') + b'\0' + str(index).encode('ascii')).hexdigest()


def _batch_child_digest(key: dict) -> str:
    return hashlib.sha256(b'hermes-downloads:batch-entry:v1\0' + _batch_canonical(key, 40960)).hexdigest()


def _batch_creation_seal(blob: bytes) -> str:
    return hashlib.sha256(b'hermes-downloads:batch-creation-intent:v1\0' + blob).hexdigest()


def _batch_original_job(entry, collection, creation, child_id, digest):
    intent = DownloadIntent(entry['job'], child_id, digest, entry['source_url'].encode('utf-8'),
        expected_revision=None, generation=0, revision=0)
    return MaterializedJob(entry['job'], intent, SourceKind.DIRECT,
        creation['queue_collection_id'], entry['priority'], creation['order_key'], None,
        False, False, False, entry['category'], collection,
        entry['partial_filename'], entry['selected_final_filename'])


class SQLiteStore:
    """A small single-writer SQLite store with command idempotency."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        connection = sqlite3.connect(self.database_path, isolation_level=None)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            self._reject_newer_schema_version(connection)
            self._migrate_schema_v15(connection)
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
            9: _V9_TABLE_SCHEMAS,
            10: _V10_TABLE_SCHEMAS,
            11: _V11_TABLE_SCHEMAS,
            12: _V12_TABLE_SCHEMAS,
            13: _V13_TABLE_SCHEMAS,
            14: _V14_TABLE_SCHEMAS,
            15: _V15_TABLE_SCHEMAS,
            16: _V16_TABLE_SCHEMAS,
            17: _V17_TABLE_SCHEMAS,
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
    def _migrate_schema_v15(connection: sqlite3.Connection) -> None:
        """Bootstrap v1 then apply additive v2 through v15 migrations atomically."""

        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("PRAGMA user_version").fetchone()
            if row is None or type(row[0]) is not int or row[0] < 0:
                raise RuntimeError("database schema version is invalid")
            version = row[0]
            if version == 0:
                for statement in _SCHEMA.split(";"):
                    if statement.strip():
                        connection.execute(statement)
                connection.execute("PRAGMA user_version = 1")
                version = 1
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
                connection.execute(_V7_JOB_CONTROL_COMMANDS_SCHEMA)
                connection.execute("PRAGMA user_version = 7")
                version = 7
            if version == 7:
                if not SQLiteStore._has_table_schemas(connection, _V7_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_V8_COMMAND_RECEIPTS_SCHEMA)
                SQLiteStore._backfill_command_receipts(connection)
                connection.execute("PRAGMA user_version = 8")
                version = 8
            if version == 8:
                if not SQLiteStore._has_table_schemas(connection, _V8_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(
                    "ALTER TABLE job_control_commands RENAME TO job_control_commands_v8"
                )
                connection.execute(_JOB_CONTROL_COMMANDS_SCHEMA)
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
                connection.execute(
                    "ALTER TABLE command_receipts RENAME TO command_receipts_v8"
                )
                connection.execute(_COMMAND_RECEIPTS_SCHEMA)
                connection.execute(
                    """
                    INSERT INTO command_receipts (request_id, payload_digest, scope, action)
                    SELECT request_id, payload_digest, scope, action
                    FROM command_receipts_v8
                    """
                )
                connection.execute("DROP TABLE command_receipts_v8")
                connection.execute("PRAGMA user_version = 9")
                version = 9
            if version == 9:
                if not SQLiteStore._has_table_schemas(connection, _V9_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_PUBLICATION_RESERVATIONS_SCHEMA)
                connection.execute("PRAGMA user_version = 10")
                version = 10
            if version == 10:
                if not SQLiteStore._has_table_schemas(connection, _V10_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_PUBLICATION_MARKER_BINDINGS_SCHEMA)
                connection.execute("PRAGMA user_version = 11")
                version = 11
            if version == 11:
                if not SQLiteStore._has_table_schemas(connection, _V11_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_DIRECT_ENGINE_RECOVERY_CAPABILITIES_SCHEMA)
                connection.execute("PRAGMA user_version = 12")
                version = 12
            if version == 12:
                if not SQLiteStore._has_table_schemas(connection, _V12_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_DIRECT_DISPATCH_COMMANDS_SCHEMA)
                connection.execute("PRAGMA user_version = 13")
                version = 13
            if version == 13:
                if not SQLiteStore._has_table_schemas(connection, _V13_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_STAGED_PAYLOAD_BINDINGS_SCHEMA)
                connection.execute("PRAGMA user_version = 14")
                version = 14
            if version == 14:
                if not SQLiteStore._has_table_schemas(connection, _V14_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_FINAL_PUBLICATION_BINDINGS_SCHEMA)
                connection.execute("PRAGMA user_version = 15")
                version = 15
            if version == 15:
                if not SQLiteStore._has_table_schemas(connection, _V15_TABLE_SCHEMAS):
                    raise RuntimeError("database schema version is incomplete")
                connection.execute(_DIRECT_PUBLICATION_ATTEMPTS_SCHEMA)
                connection.execute(_CLOSED_DIRECT_PUBLICATION_ATTEMPTS_SCHEMA)
                connection.execute("PRAGMA user_version = 16")
                version = 16
            if version == 16:
                if not SQLiteStore._has_table_schemas(connection, _V16_TABLE_SCHEMAS):
                    raise RuntimeError('database schema version is incomplete')
                rows = connection.execute('SELECT request_id FROM command_receipts').fetchall()
                for row in rows:
                    SQLiteStore._read_command_receipt(connection, row[0])
                connection.execute('ALTER TABLE command_receipts RENAME TO command_receipts_v16')
                connection.execute(_V17_COMMAND_RECEIPTS_SCHEMA)
                connection.execute('INSERT INTO command_receipts SELECT * FROM command_receipts_v16')
                connection.execute('DROP TABLE command_receipts_v16')
                connection.execute(_ADD_BATCH_COMMANDS_SCHEMA)
                connection.execute(_ADD_BATCH_ENTRIES_SCHEMA)
                connection.execute('PRAGMA user_version = 17')
                version = 17
            if version == _SUPPORTED_SCHEMA_VERSION:
                if not SQLiteStore._has_table_schemas(connection, _V17_TABLE_SCHEMAS):
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
                UNION ALL
                SELECT request_id FROM direct_dispatch_commands WHERE request_id = ?
                UNION ALL
                SELECT request_id FROM add_batch_commands WHERE request_id = ?
                UNION ALL
                SELECT child_request_id FROM add_batch_entries WHERE child_request_id = ?
            )
            LIMIT 1
            """,
            (request_id, request_id, request_id, request_id, request_id, request_id),
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

    @staticmethod
    def _read_source_kind(
        connection: sqlite3.Connection, job_id: str
    ) -> SourceKind | None:
        """Decode persisted compatibility without treating it as work authority."""

        row = connection.execute(
            "SELECT source_kind FROM materialized_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if row is None:
            return None
        try:
            return SourceKind(_require_sqlite_text(row["source_kind"], "source_kind"))
        except ValueError as error:
            raise ValueError("persisted source_kind is invalid") from error

    @staticmethod
    def _require_mutable_source(
        connection: sqlite3.Connection, job_id: str, *, direct_only: bool = False
    ) -> None:
        """Keep legacy targets inert, including private lifecycle/binding seams."""

        kind = SQLiteStore._read_source_kind(connection, job_id)
        if kind is SourceKind.LEGACY_VIDEO or (
            direct_only and kind is not SourceKind.DIRECT
        ):
            raise ValueError("unsupported source kind")

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
            result = self._apply_add_in_transaction(connection, intent, materialized,
                scope=_ADD_COMMAND_SCOPE, action=_ADD_COMMAND_ACTION)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return result

    @contextmanager
    def _batch_budget(self):
        connection = self._connection
        deadline = time.monotonic() + 2.0
        previous = connection.execute('PRAGMA busy_timeout').fetchone()[0]
        connection.execute('PRAGMA busy_timeout = 2000')
        connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        try:
            yield deadline
            if time.monotonic() >= deadline:
                raise TimeoutError('batch database deadline')
        finally:
            connection.set_progress_handler(None, 0)
            connection.execute(f'PRAGMA busy_timeout = {previous}')

    def apply_add_batch(self, command: AddBatchCommand) -> AddBatchResult:
        """One owner transaction captures inactive jobs and their original sealed intent."""
        if type(command) is not AddBatchCommand:
            raise TypeError('invalid batch command')
        connection = self._connection
        try:
            with self._batch_budget() as deadline:
                connection.execute('BEGIN IMMEDIATE')
                try:
                    try:
                        replay = self._match_command_receipt(connection, request_id=command.request_id,
                            payload_digest=command.payload_digest, scope='add_batch', action='add_batch')
                    except RuntimeError:
                        # Dispatch has its own accepted registry. It has no fabricated global scope.
                        direct = self._read_direct_dispatch_command(connection, command.request_id)
                        other = connection.execute('''SELECT 1 FROM commands WHERE request_id=?
                            UNION ALL SELECT 1 FROM queue_commands WHERE request_id=?
                            UNION ALL SELECT 1 FROM job_control_commands WHERE request_id=?
                            UNION ALL SELECT 1 FROM add_batch_commands WHERE request_id=?
                            UNION ALL SELECT 1 FROM add_batch_entries WHERE child_request_id=? LIMIT 1''',
                            (command.request_id,) * 5).fetchone()
                        if direct is not None and other is None:
                            raise RequestConflictError('request_id is bound to direct dispatch') from None
                        raise
                    if replay:
                        result, _ = self._read_batch(command.request_id)
                        # Read-only replay releases the snapshot without a commit or any writes.
                        connection.rollback()
                        return result
                    queue_id = _batch_collection_id(command.collection)
                    if queue_id is not None and connection.execute(
                            'SELECT 1 FROM materialized_jobs WHERE queue_collection_id=? AND (destination_collection IS NULL OR destination_collection != ?) LIMIT 1',
                            (queue_id, command.collection)).fetchone() is not None:
                        raise RuntimeError('batch collection identity changed')
                    maximum = connection.execute('SELECT MAX(order_key) FROM materialized_jobs').fetchone()[0]
                    # SQLite MAX hides invalid lower storage values, so validate all existing counters too.
                    if connection.execute("SELECT 1 FROM materialized_jobs WHERE typeof(order_key) != 'integer' OR order_key < 0 LIMIT 1").fetchone() is not None:
                        raise RuntimeError('batch order is invalid')
                    base = 0 if maximum is None else _require_counter(maximum, 'batch order') + 1
                    if base + len(command.entries) - 1 > _MAX_COUNTER:
                        raise OverflowError('batch order overflow')
                    results = []
                    for index, raw in enumerate(command.entries):
                        if time.monotonic() >= deadline: raise TimeoutError('batch database deadline')
                        connection.execute('SAVEPOINT batch_entry')
                        blob = seal = None
                        try:
                            try: entry = _batch_entry(_batch_decode(raw))
                            except (TypeError, ValueError): raise _BatchEntryBlocked('invalid_entry') from None
                            try: source = validate_source_url(entry['source_url'])
                            except SourcePolicyError: raise _BatchEntryBlocked('invalid_source') from None
                            child_id = _batch_child_id(command.request_id, index)
                            if child_id == command.request_id or self._read_command_receipt(connection, child_id) is not None:
                                raise _BatchEntryBlocked('request_conflict')
                            if self._read_direct_dispatch_command(connection, child_id) is not None:
                                raise _BatchEntryBlocked('request_conflict')
                            self._reject_unregistered_legacy_receipt(connection, child_id)
                            if connection.execute('SELECT 1 FROM jobs WHERE job_id=?', (entry['job'],)).fetchone() is not None:
                                raise _BatchEntryBlocked('job_conflict')
                            creation = dict(generation=0, revision=0, state='queued', expected_revision=None,
                                order_key=base + index, queue_collection_id=queue_id, scheduled_for_us=None,
                                authorized=False, manual_hold=False, start_now_requested=False)
                            key = dict(v=1, parent_request_id=command.request_id,
                                parent_payload_digest=command.payload_digest, index=index,
                                collection=command.collection, entry=entry, creation=creation)
                            child_digest = _batch_child_digest(key)
                            job = _batch_original_job(entry, command.collection, creation, child_id, child_digest)
                            if job.intent.source_url != source.raw_url: raise RuntimeError('batch source snapshot changed')
                            try: self._ensure_publication_target_is_available(connection, job)
                            except sqlite3.IntegrityError as error:
                                if str(error) != 'publication target is already reserved': raise
                                raise _BatchEntryBlocked('destination_conflict') from None
                            applied = self._apply_add_in_transaction(connection, job.intent, job,
                                scope='add_batch_entry', action='add_batch_entry')
                            if not applied.applied: raise RuntimeError('unexpected child replay')
                            events = connection.execute("SELECT event_id,generation,revision FROM events WHERE job_id=? AND kind='job_added' LIMIT 2", (job.job_id,)).fetchall()
                            if len(events) != 1 or tuple(events[0])[1:] != (0, 0): raise RuntimeError('batch creation audit invalid')
                            audit_id = _require_counter(events[0][0], 'batch audit')
                            if audit_id < 1: raise RuntimeError('batch creation audit invalid')
                            reservation = self._read_publication_reservation(connection, job.job_id)
                            if reservation is None: raise RuntimeError('batch reservation missing')
                            record = {**key, 'child_request_id': child_id, 'child_payload_digest': child_digest,
                                'creation': {**creation, 'audit_id': audit_id}, 'reservation': dict(
                                    target_component=reservation.target_component, final_filename=reservation.final_filename,
                                    claim_token=reservation.claim_token)}
                            blob = _batch_canonical(record, 40960); seal = _batch_creation_seal(blob)
                            item = AddBatchEntryResult(index, 'applied', None, job.job_id, child_id, 0, 0, 'queued', base + index)
                        except _BatchEntryBlocked as error:
                            connection.execute('ROLLBACK TO batch_entry')
                            item = AddBatchEntryResult(index, 'blocked', str(error), None, None, None, None, None, None)
                        connection.execute('RELEASE batch_entry')
                        connection.execute('''INSERT INTO add_batch_entries
                            (parent_request_id,entry_index,status,reason,job_id,child_request_id,generation,revision,order_key,creation_intent_blob,creation_intent_digest)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)''', (command.request_id, index, item.status, item.reason,
                                item.job, item.child_request_id, item.generation, item.revision, item.order_key, blob, seal))
                        results.append(item)
                    result = AddBatchResult(command.request_id, False, tuple(results))
                    receipt = _batch_canonical(result.to_record(), _MAX_BATCH_REPLY)
                    self._insert_command_receipt(connection, request_id=command.request_id,
                        payload_digest=command.payload_digest, scope='add_batch', action='add_batch')
                    connection.execute('INSERT INTO add_batch_commands VALUES (?,?,?,?)',
                        (command.request_id, command.payload_digest, len(results), receipt))
                    if time.monotonic() >= deadline: raise TimeoutError('batch database deadline')
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
                return result
        except RequestConflictError:
            raise
        except Exception:
            raise RuntimeError('batch_state_invalid') from None

    def _read_batch(self, parent_id):
        connection = self._connection
        rows = connection.execute('SELECT * FROM add_batch_commands WHERE request_id=? LIMIT 2', (parent_id,)).fetchall()
        if len(rows) != 1: raise ValueError('batch parent missing')
        parent = rows[0]
        _require_identifier(parent['request_id'], 'batch parent')
        digest = _require_payload_digest(parent['payload_digest'])
        count = parent['entry_count']
        if type(count) is not int or not 1 <= count <= 500: raise ValueError('batch parent count invalid')
        if self._read_command_receipt(connection, parent_id) != (digest, 'add_batch', 'add_batch'):
            raise ValueError('batch parent registry invalid')
        raw = parent['receipt']
        if type(raw) is not bytes or not 1 <= len(raw) <= _MAX_BATCH_REPLY: raise ValueError('batch receipt invalid')
        record = _batch_decode(raw)
        if _batch_canonical(record, _MAX_BATCH_REPLY) != raw: raise ValueError('batch receipt noncanonical')
        original = AddBatchResult.from_record(record)
        if original.replayed or original.request_id != parent_id or len(original.results) != count:
            raise ValueError('batch parent receipt invalid')
        entries = connection.execute('SELECT * FROM add_batch_entries WHERE parent_request_id=? ORDER BY entry_index', (parent_id,)).fetchall()
        if len(entries) != count: raise ValueError('batch indexed count invalid')
        intents = {}
        for index, row in enumerate(entries):
            if type(row['entry_index']) is not int or row['entry_index'] != index or row['parent_request_id'] != parent_id:
                raise ValueError('batch index invalid')
            item = AddBatchEntryResult(index, row['status'], row['reason'], row['job_id'], row['child_request_id'],
                row['generation'], row['revision'], 'queued' if row['status'] == 'applied' else None, row['order_key'])
            if item != original.results[index]: raise ValueError('batch indexed receipt changed')
            if item.status == 'blocked':
                if row['creation_intent_blob'] is not None or row['creation_intent_digest'] is not None:
                    raise ValueError('batch rejection has intent authority')
            else:
                intents[item.job] = self._read_batch_creation(row, parent_id, digest)
        return AddBatchResult(parent_id, True, original.results), intents

    def _read_batch_creation(self, row, parent_id, parent_digest):
        connection = self._connection
        blob = row['creation_intent_blob']; seal = row['creation_intent_digest']
        if type(blob) is not bytes or not 1 <= len(blob) <= 40960: raise ValueError('batch intent invalid')
        _require_payload_digest(seal)
        if _batch_creation_seal(blob) != seal: raise ValueError('batch intent seal changed')
        record = _batch_decode(blob)
        if type(record) is not dict or set(record) != {'v','parent_request_id','parent_payload_digest','index','child_request_id','child_payload_digest','collection','entry','creation','reservation'}:
            raise ValueError('batch intent shape invalid')
        if _batch_canonical(record, 40960) != blob: raise ValueError('batch intent noncanonical')
        if (type(record['v']) is not int or record['v'] != 1 or record['parent_request_id'] != parent_id
                or record['parent_payload_digest'] != parent_digest or type(record['index']) is not int
                or record['index'] != row['entry_index']): raise ValueError('batch intent parent changed')
        collection = record['collection']
        if collection is not None: _batch_component(collection)
        entry = record['entry']
        if type(entry) is not dict or set(entry) != {'job','source_kind','source_url','priority','category','partial_filename','selected_final_filename','expected_sha256'}:
            raise ValueError('batch normalized intent missing')
        if _batch_entry(entry) != entry: raise ValueError('batch normalized intent changed')
        creation = record['creation']
        if type(creation) is not dict or set(creation) != {'audit_id','generation','revision','state','expected_revision','order_key','queue_collection_id','scheduled_for_us','authorized','manual_hold','start_now_requested'}:
            raise ValueError('batch creation shape invalid')
        if (type(creation['generation']) is not int or creation['generation'] != 0
                or type(creation['revision']) is not int or creation['revision'] != 0
                or creation['state'] != 'queued' or creation['expected_revision'] is not None
                or creation['scheduled_for_us'] is not None
                or any(creation[key] is not False for key in ('authorized','manual_hold','start_now_requested'))
                or creation['queue_collection_id'] != _batch_collection_id(collection)):
            raise ValueError('batch creation projection changed')
        _require_counter(creation['order_key'], 'batch order')
        audit_id = _require_counter(creation['audit_id'], 'batch audit')
        if audit_id < 1: raise ValueError('batch audit invalid')
        child_id = _batch_child_id(parent_id, record['index'])
        if record['child_request_id'] != child_id or row['child_request_id'] != child_id or row['job_id'] != entry['job'] or row['order_key'] != creation['order_key']:
            raise ValueError('batch indexed original changed')
        key = {name: record[name] for name in ('v','parent_request_id','parent_payload_digest','index','collection','entry')}
        key['creation'] = {name: value for name, value in creation.items() if name != 'audit_id'}
        digest = _require_payload_digest(record['child_payload_digest'])
        if _batch_child_digest(key) != digest: raise ValueError('batch child digest changed')
        commands = connection.execute('SELECT request_id,payload_digest,job_id,generation,revision FROM commands WHERE job_id=? LIMIT 2', (entry['job'],)).fetchall()
        if len(commands) != 1 or tuple(commands[0]) != (child_id, digest, entry['job'], 0, 0):
            raise ValueError('batch child command changed')
        if any(type(commands[0][k]) is not int for k in ('generation','revision')):
            raise ValueError('batch child counters invalid')
        if self._read_command_receipt(connection, child_id) != (digest, 'add_batch_entry', 'add_batch_entry'):
            raise ValueError('batch child registry changed')
        original = _batch_original_job(entry, collection, creation, child_id, digest)
        reserved = record['reservation']
        if type(reserved) is not dict or set(reserved) != {'target_component','final_filename','claim_token'}:
            raise ValueError('batch reservation shape invalid')
        reservation = PublicationReservation(entry['job'], **reserved)
        if (reservation.target_component != (collection if collection is not None else entry['category'])
                or reservation.final_filename != entry['selected_final_filename']):
            raise ValueError('batch captured reservation changed')
        # Build original first. Current data is only a separately checked successor.
        current = self.get_materialized_job(original.job_id)
        if current is None or current.intent.source_url != original.intent.source_url or current.intent.request_id != child_id or current.intent.payload_digest != digest or self._immutable_projection_values(current) != self._immutable_projection_values(original):
            raise ValueError('batch immutable projection changed')
        if self._read_publication_reservation(connection, original.job_id) != reservation:
            raise ValueError('batch reservation changed')
        captured = _BatchCreationIntent(parent_id, parent_digest, row['entry_index'], audit_id, seal,
            original, reservation, entry['expected_sha256'])
        self._validate_batch_lifecycle(captured, current)
        return captured

    def get_batch_creation_intent(self, job_id: str) -> _BatchCreationIntent | None:
        """Readonly original metadata; never reconstruct missing intent or grant work."""
        _require_identifier(job_id, 'job_id')
        connection = self._connection
        try:
            with self._batch_budget():
                connection.execute('BEGIN')
                try:
                    links = connection.execute('SELECT parent_request_id FROM add_batch_entries WHERE job_id=? LIMIT 2', (job_id,)).fetchall()
                    commands = connection.execute('SELECT request_id,payload_digest,job_id,generation,revision FROM commands WHERE job_id=? LIMIT 2', (job_id,)).fetchall()
                    if links:
                        if len(links) != 1: raise ValueError('batch job link invalid')
                        _, intents = self._read_batch(links[0][0])
                        if job_id not in intents: raise ValueError('batch accepted link missing')
                        return intents[job_id]
                    if not commands:
                        if connection.execute('SELECT 1 FROM jobs WHERE job_id=?', (job_id,)).fetchone() is not None or connection.execute('SELECT 1 FROM events WHERE job_id=?', (job_id,)).fetchone() is not None:
                            raise ValueError('creation provenance missing')
                        return None
                    if len(commands) != 1: raise ValueError('creation provenance not unique')
                    command = commands[0]
                    request = _require_identifier(command['request_id'], 'legacy request')
                    digest = _require_payload_digest(command['payload_digest'])
                    if self._read_command_receipt(connection, request) != (digest, 'add', 'add'):
                        raise ValueError('batch child link missing')
                    generation = _require_counter(command['generation'], 'legacy generation')
                    revision = _require_counter(command['revision'], 'legacy revision')
                    added = connection.execute("SELECT event_id,generation,revision FROM events WHERE job_id=? AND kind='job_added' LIMIT 2", (job_id,)).fetchall()
                    if len(added) != 1 or tuple(added[0])[1:] != (generation, revision) or type(added[0][0]) is not int or added[0][0] < 1 or self.get_job(job_id) is None:
                        raise ValueError('legacy creation provenance invalid')
                    return None
                finally:
                    connection.rollback()
        except Exception:
            raise RuntimeError('batch_state_invalid') from None

    def _batch_control_corroborated(self, job_id, action, generation, revision, state, authorized):
        connection = self._connection
        # Include all compatible receipts. Later no-ops are distinguished by their exact input digest.
        cursor = connection.execute('SELECT * FROM job_control_commands WHERE job_id=? AND action=? AND generation=? AND revision=?',
            (job_id, action, generation, revision))
        for row in cursor:
            result = self._job_control_result_from_receipt(row)
            request = _require_identifier(row['request_id'], 'control request')
            digest = _require_payload_digest(row['payload_digest'])
            if self._read_command_receipt(connection, request) != (digest, 'job_control', action):
                raise ValueError('batch control registry invalid')
            original = JobControlCommand(job=job_id, action=action, request_id=request,
                expected_revision=revision - 1)
            if (result.status == 'applied' and result.job == job_id and result.generation == generation
                    and result.revision == revision and result.state == state and result.authorized is authorized
                    and original.payload_digest == digest):
                return True
        return False

    def _batch_dispatch_corroborated(self, job_id, generation, revision, kind):
        connection = self._connection
        cursor = connection.execute('SELECT request_id FROM direct_dispatch_commands WHERE job_id=? AND generation=? AND revision BETWEEN ? AND ?',
            (job_id, generation, revision, revision + (2 if kind == 'job_resolving' else 1)))
        for row in cursor:
            receipt = self._read_direct_dispatch_command(connection, row[0])
            if receipt is None or receipt.status not in {'pending','started','blocked'}:
                raise ValueError('batch dispatch receipt invalid')
            if receipt.job != job_id or receipt.generation != generation: raise ValueError('batch dispatch target invalid')
            expected = 'resolving' if kind == 'job_resolving' else 'downloading'
            if receipt.revision == revision and receipt.state == expected and receipt.status in {'pending','started'}:
                return True
            # A resolving receipt may already be at its exact downloading successor.
            if kind == 'job_resolving' and receipt.revision == revision + 1 and receipt.state == 'downloading' and receipt.status in {'pending','started'}:
                event = connection.execute('SELECT kind FROM events WHERE job_id=? AND generation=? AND revision=?', (job_id,generation,revision+1)).fetchall()
                if len(event) == 1 and event[0][0] == 'job_downloading': return True
            # Contained pending dispatch records its actual immediately paused successor.
            if receipt.status == 'blocked' and receipt.state == 'paused' and receipt.revision > revision:
                events = connection.execute('SELECT kind,revision FROM events WHERE job_id=? AND generation=? AND revision>? AND revision<=? ORDER BY event_id',
                    (job_id, generation, revision, receipt.revision)).fetchall()
                wanted = [('job_paused', revision + 1)] if receipt.revision == revision + 1 else [('job_downloading', revision + 1), ('job_paused', revision + 2)]
                if [tuple(e) for e in events] == wanted: return True
        return False

    def _publication_closure_corroborated(self, connection, row):
        event = connection.execute('SELECT kind,job_id,generation,revision FROM events WHERE event_id=?',
            (row['audit_id'],)).fetchone()
        if event is None or tuple(event)[1:] != (row['job_id'],row['generation'],row['revision']):
            return False
        if row['state'] in {'paused','removed'}:
            return event['kind'] == 'job_' + row['state']
        if row['state'] != 'queued' or event['kind'] not in {'job_resumed','job_start_now_requested'}:
            return False
        action = 'resume' if event['kind'] == 'job_resumed' else 'start_now'
        predecessor = connection.execute('SELECT kind,generation,revision FROM events WHERE job_id=? AND event_id<? ORDER BY event_id DESC LIMIT 1',
            (row['job_id'],row['audit_id'])).fetchone()
        if (predecessor is None or predecessor['kind'] not in ({'job_paused'} if action == 'resume' else {'job_paused','job_finalizing'})
                or tuple(predecessor)[1:] != (row['generation'],row['revision'] - 1)):
            return False
        return any(self._batch_control_corroborated(row['job_id'],action,row['generation'],row['revision'],'queued',authorized)
            for authorized in ((False,True) if action == 'resume' else (True,)))

    def _batch_publication_record(self, row, captured, *, archived):
        """Strict database-only qualification of one explicitly named attempt, never a permit."""
        connection = self._connection
        keys = ('job_id','attempt_id','original_request_id','proof','status','audit_id','generation','revision','state','worker_epoch','pending_request_id')
        if tuple(row.keys()) != keys: raise ValueError('batch publication columns invalid')
        for key in ('job_id','attempt_id','original_request_id'):
            _require_identifier(row[key], 'publication identity')
        if row['job_id'] != captured.original_job.job_id: raise ValueError('publication job changed')
        status = row['status']; state = row['state']
        if type(status) is not str or type(state) is not str: raise ValueError('publication scalar invalid')
        if status not in {'eligible','finished','closed'} or state not in {'finalizing','paused','completed','removed','queued'}:
            raise ValueError('publication status invalid')
        if archived and (status != 'closed' or state not in {'paused','removed','queued'} or row['pending_request_id'] is not None):
            raise ValueError('publication archive invalid')
        counters = tuple(_require_counter(row[key], key) for key in ('audit_id','generation','revision','worker_epoch'))
        audit, generation, revision, epoch = counters
        if audit < 1 or epoch < 1: raise ValueError('publication pointer invalid')
        raw = row['proof']
        if type(raw) is not str or not 1 <= len(raw.encode('utf-8')) <= 4096: raise ValueError('publication proof invalid')
        proof = _batch_decode(raw.encode('utf-8'))
        if type(proof) is not dict or set(proof) != {'request','digest','generation','downloading_revision','finalizing_revision','finalizing_audit','epoch','ownership','marker','stage','chain','sha256'}:
            raise ValueError('publication proof shape invalid')
        _require_identifier(proof['request'], 'publication request')
        for name in ('digest','ownership','sha256'): _require_payload_digest(proof[name])
        for name in ('generation','downloading_revision','finalizing_revision','finalizing_audit','epoch'): _require_counter(proof[name], name)
        if (proof['request'] != row['original_request_id'] or proof['finalizing_revision'] != proof['downloading_revision'] + 1
                or proof['epoch'] < 1 or proof['finalizing_audit'] < 1 or generation < proof['generation']
                or revision < proof['finalizing_revision'] or epoch < proof['epoch']):
            raise ValueError('publication original fence invalid')
        for name, length in (('marker',2),('stage',7)):
            if type(proof[name]) is not list or len(proof[name]) != length: raise ValueError('publication proof counters invalid')
            for value in proof[name]: _require_counter(value, 'publication metadata')
        if not stat.S_ISREG(proof['stage'][3]) or proof['stage'][4] != 1: raise ValueError('publication stage invalid')
        if type(proof['chain']) is not list or len(proof['chain']) != 4: raise ValueError('publication chain invalid')
        for pair in proof['chain']:
            if type(pair) is not list or len(pair) != 2: raise ValueError('publication chain invalid')
            for value in pair: _require_counter(value, 'publication directory')
        if self._publication_ownership(captured.original_job, captured.reservation) != proof['ownership']:
            raise ValueError('publication original ownership changed')
        if captured.expected_sha256 is not None and captured.expected_sha256 != proof['sha256']:
            raise ValueError('publication expected checksum metadata changed')
        receipt = self._read_direct_dispatch_command(connection, proof['request'])
        if receipt != _DirectDispatchCommand(proof['request'], proof['digest'], captured.original_job.job_id,
                'started', proof['generation'], proof['downloading_revision'], 'downloading'):
            raise ValueError('publication original dispatch changed')
        event = connection.execute('SELECT kind,job_id,generation,revision FROM events WHERE event_id=?', (proof['finalizing_audit'],)).fetchone()
        if event is None or tuple(event) != ('job_finalizing',captured.original_job.job_id,proof['generation'],proof['finalizing_revision']):
            raise ValueError('publication original audit changed')
        opposite = 'direct_publication_attempts' if archived else 'closed_direct_publication_attempts'
        if connection.execute(f'SELECT 1 FROM {opposite} WHERE attempt_id=? OR original_request_id=? LIMIT 1', (row['attempt_id'],row['original_request_id'])).fetchone() is not None:
            raise ValueError('publication identity overlaps archive')
        if status == 'closed':
            if state not in {'paused','removed','queued'} or row['pending_request_id'] is not None or audit <= proof['finalizing_audit']:
                raise ValueError('publication closed pointer invalid')
            if not self._publication_closure_corroborated(connection,row):
                raise ValueError('publication closure audit changed')
            current = self._read_job_control_projection(connection, captured.original_job.job_id)
            if generation > current.generation or epoch > self._current_worker_epoch(connection):
                raise ValueError('publication closure chronology changed')
            if archived:
                fresh = connection.execute('SELECT proof FROM direct_publication_attempts WHERE job_id=?', (current.job,)).fetchone()
                if fresh is None: raise ValueError('publication archive has no successor')
                newer = _batch_decode(fresh[0].encode('utf-8'))
                if revision >= newer['finalizing_revision']: raise ValueError('publication retirement chronology changed')
        else:
            if epoch - proof['epoch'] != generation - proof['generation']:
                raise ValueError('publication cold epoch relation changed')
            current = self._read_job_control_projection(connection, captured.original_job.job_id)
            latest = connection.execute('SELECT event_id,generation,revision,kind FROM events WHERE job_id=? ORDER BY event_id DESC LIMIT 1', (current.job,)).fetchone()
            if latest is None or tuple(latest) != (audit,generation,revision,'job_'+state) or (current.generation,current.revision,current.state) != (generation,revision,state):
                raise ValueError('publication current pointer changed')
            if status == 'eligible':
                if state not in {'finalizing','paused'} or self._current_worker_epoch(connection) != epoch:
                    raise ValueError('publication eligible epoch changed')
                self._read_publication_attempt(connection, current.job)
            else:
                if state != 'completed' or row['pending_request_id'] is not None or self._current_worker_epoch(connection) < epoch:
                    raise ValueError('publication finished pointer invalid')
                self._read_publication_attempt(connection, current.job, require_current=False)
                binding = self._read_final_publication_binding(connection, current.job)
                if binding is None or (binding.final_device,binding.final_inode,binding.logical_size) != tuple(proof['stage'][:3]):
                    raise ValueError('publication final binding changed')
        return proof

    def _validate_batch_lifecycle(self, captured, current):
        connection = self._connection
        job_id = captured.original_job.job_id
        generation = revision = 0; state = 'queued'
        flags = {(False,False,False)}
        first = True; last_audit = None; direct_lineage = False; finalizing_event = None
        cursor = connection.execute('SELECT event_id,kind,generation,revision FROM events WHERE job_id=? ORDER BY event_id', (job_id,))
        for row in cursor:
            audit = _require_counter(row['event_id'], 'batch lifecycle audit')
            kind = _require_sqlite_text(row['kind'], 'batch lifecycle kind')
            next_generation = _require_counter(row['generation'], 'batch lifecycle generation')
            next_revision = _require_counter(row['revision'], 'batch lifecycle revision')
            if first:
                if (audit,kind,next_generation,next_revision) != (captured.audit_id,'job_added',0,0):
                    raise ValueError('batch original creation audit changed')
                first = False; last_audit = audit; continue
            if audit <= last_audit or next_revision != revision + 1:
                raise ValueError('batch lifecycle revision gap')
            successors = set()
            if kind == 'job_paused' and next_generation == generation + 1:
                if state not in _RECOVERABLE_COLD_START_STATES: raise ValueError('batch cold predecessor invalid')
                successors = flags; next_state = 'paused'; direct_lineage = False
            elif next_generation != generation:
                raise ValueError('batch unsupported generation jump')
            elif kind == 'job_paused':
                next_state = 'paused'
                for authorized, hold, start in flags:
                    candidate = (authorized,True,start)
                    if (state != 'paused' or candidate != (authorized,hold,start)) and self._batch_control_corroborated(job_id,'pause',generation,next_revision,'paused',authorized):
                        successors.add(candidate)
                if state in {'resolving','downloading','finalizing'} and direct_lineage:
                    successors.update(flags)
            elif kind in {'job_resumed','job_start_now_requested','job_removed'}:
                action = {'job_resumed':'resume','job_start_now_requested':'start_now','job_removed':'remove'}[kind]
                next_state = ('queued' if state == 'paused' else state) if action == 'resume' else ('queued' if action == 'start_now' else 'removed')
                for authorized, hold, start in flags:
                    candidate = (authorized,False,start) if action == 'resume' else ((True,False,True) if action == 'start_now' else (False,True,False))
                    if (next_state,candidate) != (state,(authorized,hold,start)) and self._batch_control_corroborated(job_id,action,generation,next_revision,next_state,candidate[0]):
                        successors.add(candidate)
                direct_lineage = False
            elif kind in {'job_resolving','job_downloading'}:
                expected = 'queued' if kind == 'job_resolving' else 'resolving'
                if state != expected or not self._batch_dispatch_corroborated(job_id,generation,next_revision,kind):
                    raise ValueError('batch dispatch lifecycle invalid')
                next_state = 'resolving' if kind == 'job_resolving' else 'downloading'
                successors = flags; direct_lineage = True
            elif kind == 'job_finalizing':
                if state != 'downloading' or not direct_lineage: raise ValueError('batch finalizing predecessor invalid')
                receipts = connection.execute("SELECT request_id FROM direct_dispatch_commands WHERE job_id=? AND generation=? AND revision=? AND status='started' AND state='downloading'", (job_id,generation,revision)).fetchall()
                if not receipts: raise ValueError('batch finalizing dispatch missing')
                for receipt in receipts: self._read_direct_dispatch_command(connection,receipt[0])
                finalizing_event = (audit,generation,next_revision)
                next_state = 'finalizing'; successors = flags
            elif kind == 'job_completed':
                if state not in {'finalizing','paused'} or finalizing_event is None:
                    raise ValueError('batch completion predecessor invalid')
                attempt = connection.execute('SELECT * FROM direct_publication_attempts WHERE job_id=?', (job_id,)).fetchone()
                if attempt is None or attempt['status'] != 'finished': raise ValueError('batch completed attempt missing')
                proof = self._batch_publication_record(attempt,captured,archived=False)
                if proof['finalizing_audit'] != finalizing_event[0]: raise ValueError('batch completed original audit changed')
                if (attempt['audit_id'],attempt['generation'],attempt['revision']) != (audit,generation,next_revision):
                    raise ValueError('batch completed audit pointer changed')
                next_state = 'completed'; successors = flags
            else:
                raise ValueError('batch unsupported lifecycle mutation')
            if not successors or len(successors) > 8: raise ValueError('batch lifecycle flags unsupported')
            flags = successors; generation = next_generation; revision = next_revision; state = next_state; last_audit = audit
        if first or (current.intent.generation,current.intent.revision) != (generation,revision):
            raise ValueError('batch lifecycle terminal counters changed')
        actual = self.get_job(job_id)
        if actual is None or actual.state != state or (current.authorized,current.manual_hold,current.start_now_requested) not in flags:
            raise ValueError('batch lifecycle terminal tuple changed')
        attempt = connection.execute('SELECT * FROM direct_publication_attempts WHERE job_id=?', (job_id,)).fetchone()
        if attempt is not None:
            self._batch_publication_record(attempt,captured,archived=False)
        # Qualify only named archived proofs associated with actual finalizing audits in this lineage.
        audits = connection.execute("SELECT event_id,generation,revision FROM events WHERE job_id=? AND kind='job_finalizing' ORDER BY event_id", (job_id,))
        for event in audits:
            receipts = connection.execute("SELECT request_id FROM direct_dispatch_commands WHERE job_id=? AND generation=? AND revision=? AND status='started' AND state='downloading'", (job_id,event['generation'],event['revision']-1))
            for receipt in receipts:
                archived = connection.execute('SELECT * FROM closed_direct_publication_attempts WHERE original_request_id=?', (receipt[0],)).fetchone()
                if archived is not None:
                    proof = self._batch_publication_record(archived,captured,archived=True)
                    if proof['finalizing_audit'] != event['event_id']: raise ValueError('batch archive original audit changed')

    def _apply_add_in_transaction(self, connection, intent, materialized, *, scope, action):
        """Apply one validated add in its owner transaction; never begin or commit."""
        if (scope, action) not in {("add", "add"), ("add_batch_entry", "add_batch_entry")}:
            raise ValueError("invalid add scope")
        replay = self._match_command_receipt(
            connection,
            request_id=intent.request_id,
            payload_digest=intent.payload_digest,
            scope=scope,
            action=action,
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
            stored_job_id = _require_identifier(
                _require_sqlite_text(existing["job_id"], "command job_id"),
                "command job_id",
            )
            reservation = self._read_publication_reservation(connection, stored_job_id)
            has_stored_projection = (
                connection.execute(
                    "SELECT 1 FROM materialized_jobs WHERE job_id = ?", (stored_job_id,)
                ).fetchone()
                is not None
            )
            if (materialized is not None or has_stored_projection) and reservation is None:
                raise ValueError("materialized add replay is missing publication reservation")
            if materialized is not None and not self._stored_projection_matches(
                connection, stored_job_id, intent, materialized
            ):
                raise RequestConflictError(
                    "request_id is already bound to a different materialized projection"
                )
            result = CommandResult(
                applied=False,
                job=stored_job_id,
                generation=existing["generation"],
                revision=existing["revision"],
            )
        else:
            if existing is not None:
                raise RuntimeError("command receipt registry is incomplete")
            if materialized is not None:
                if materialized.source_kind is not SourceKind.DIRECT:
                    raise ValueError("unsupported source kind")
                self._ensure_publication_target_is_available(connection, materialized)
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
                scope=scope,
                action=action,
            )
            if materialized is not None:
                self._insert_materialized_projection(connection, materialized)
                self._insert_publication_reservation(connection, materialized)
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
    def _insert_publication_reservation(
        connection: sqlite3.Connection, materialized: MaterializedJob
    ) -> None:
        """Persist one opaque publication receipt in the caller's add transaction."""

        reservation = PublicationReservation(
            job_id=materialized.job_id,
            target_component=materialized.destination_collection or materialized.category,
            final_filename=materialized.selected_final_filename,
            claim_token=secrets.token_hex(32),
        )
        connection.execute(
            """
            INSERT INTO publication_reservations (
                job_id, target_component, final_filename, claim_token
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                reservation.job_id,
                reservation.target_component,
                reservation.final_filename,
                reservation.claim_token,
            ),
        )

    @staticmethod
    def _ensure_publication_target_is_available(
        connection: sqlite3.Connection, materialized: MaterializedJob
    ) -> None:
        """Reject a destination already owned by any materialized projection."""

        target_component = materialized.destination_collection or materialized.category
        conflict = connection.execute(
            """
            SELECT 1
            FROM materialized_jobs
            WHERE job_id != ?
              AND COALESCE(destination_collection, category) = ?
              AND selected_final_filename = ?
            LIMIT 1
            """,
            (
                materialized.job_id,
                target_component,
                materialized.selected_final_filename,
            ),
        ).fetchone()
        if conflict is not None:
            raise sqlite3.IntegrityError("publication target is already reserved")

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
        return tuple(domain_rows[0]) == SQLiteStore._immutable_projection_values(
            materialized
        )

    @staticmethod
    def _immutable_projection_values(
        materialized: MaterializedJob,
    ) -> tuple[object, ...]:
        """Return the add-time domain fields that controls cannot mutate."""

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
            materialized.category,
            materialized.destination_collection,
            materialized.partial_filename,
            materialized.selected_final_filename,
            materialized.intent.expected_revision,
        )

    @staticmethod
    def _projection_values(materialized: MaterializedJob) -> tuple[object, ...]:
        """Return SQLite-native values for the complete materialized projection."""

        immutable = SQLiteStore._immutable_projection_values(materialized)
        return (
            *immutable[:5],
            1 if materialized.authorized else 0,
            1 if materialized.manual_hold else 0,
            1 if materialized.start_now_requested else 0,
            *immutable[5:],
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

    def publication_control_is_current(self, *, job_id, action, request_id, payload_digest, expected_revision):
        connection = self._connection
        if self._match_command_receipt(connection,request_id=request_id,payload_digest=payload_digest,
                scope=_JOB_CONTROL_COMMAND_SCOPE,action=action):
            return False
        current = self._read_job_control_projection(connection,job_id)
        return current.revision == expected_revision and current.state not in _TERMINAL_JOB_CONTROL_STATES

    def publication_queue_control_is_current(self, *, request_id, payload_digest, expected_revision):
        if self._match_command_receipt(self._connection,request_id=request_id,payload_digest=payload_digest,
                scope=_QUEUE_GATE_COMMAND_SCOPE,action=_QUEUE_GATE_COMMAND_ACTION):
            return False
        return self.queue_gate_snapshot()[1] == expected_revision

    def apply_queue_gate(
        self,
        *,
        gate: str,
        request_id: str,
        payload_digest: str,
        expected_revision: int,
        _contained_direct_job: tuple[str, int, int, bool] | None = None,
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
                if _contained_direct_job is not None:
                    if gate != "paused" or type(_contained_direct_job) is not tuple or len(_contained_direct_job) != 4:
                        raise ValueError("contained queue publication input is invalid")
                    job_id, generation, revision, recoverable = _contained_direct_job
                    if type(recoverable) is not bool:
                        raise TypeError("publication recovery flag is invalid")
                    job = self.get_materialized_job(job_id)
                    if job is None or job.source_kind is not SourceKind.DIRECT:
                        raise ValueError('contained queue job must be direct')
                    current = self._read_job_control_projection(connection,job_id)
                    if (current.generation,current.revision) != (generation,revision) or current.state not in {'downloading','finalizing','paused'}:
                        raise ValueError("contained queue job is stale")
                    predecessor = self._publication_predecessor_matches(connection,current)
                    if current.state != "paused":
                        self._persist_direct_dispatch_lifecycle(connection,current=current,state="paused",event_kind="job_paused")
                    self._advance_publication_pointer(connection,current,preserve=predecessor and recoverable)
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
        _contained_direct_transfer: bool = False,
        _publication_recoverable: bool = False,
    ) -> JobControlResult:
        """Atomically apply or replay one revision-fenced materialized-job command."""

        job_id = _require_identifier(job_id, "job_id")
        action = _require_job_control_action(action)
        request_id = _require_identifier(request_id, "request_id")
        payload_digest = _require_payload_digest(payload_digest)
        expected_revision = _require_counter(expected_revision, "expected_revision")
        if type(_contained_direct_transfer) is not bool:
            raise TypeError("_contained_direct_transfer must be a boolean")
        if type(_publication_recoverable) is not bool:
            raise TypeError("_publication_recoverable must be a boolean")

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
                if self._read_source_kind(connection, job_id) is SourceKind.LEGACY_VIDEO:
                    connection.commit()
                    return current.to_result("blocked")
                publication_predecessor = self._publication_predecessor_matches(connection,current)
                if current.revision != expected_revision:
                    result = current.to_result("stale")
                elif current.state in _TERMINAL_JOB_CONTROL_STATES:
                    result = current.to_result("blocked")
                elif (
                    action == "remove"
                    and current.state in _ACTIVE_JOB_CONTROL_STATES
                    and not _contained_direct_transfer
                ):
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
                    elif action == "remove":
                        next_state = "removed"
                        next_authorized = False
                        next_manual_hold = True
                        next_start_now_requested = False
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
                        if _contained_direct_transfer and action == 'pause':
                            self._advance_publication_pointer(connection,current,
                                preserve=publication_predecessor and _publication_recoverable)
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
                        self._advance_publication_pointer(connection,current,
                            preserve=(publication_predecessor and action == "pause" and _publication_recoverable))
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
        """Persist exactly one supported lifecycle/domain transition and audit."""

        SQLiteStore._require_mutable_source(connection, updated.job, direct_only=True)
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

    def prepare_direct_dispatch(
        self,
        *,
        job_id: str,
        expected_worker_epoch: int,
        expected_generation: int,
        expected_revision: int,
        request_id: str,
        payload_digest: str,
        controller_ready: bool,
        now: datetime,
    ) -> _DirectDispatchPlan | _DirectPublicationReconciliationPlan | DirectDispatchResult:
        """Fence and durably enter ``resolving`` before any engine admission.

        The returned plan contains only immutable persisted job data and the
        already-computed durable admission gates.  Pending engine receipts remain
        fail-closed after a crash; a terminal-publication receipt may only replay
        its no-transfer inode reconciliation.
        """

        job_id = _require_identifier(job_id, "job_id")
        expected_worker_epoch = _require_worker_epoch(
            expected_worker_epoch, "expected_worker_epoch"
        )
        expected_generation = _require_counter(expected_generation, "expected_generation")
        expected_revision = _require_counter(expected_revision, "expected_revision")
        request_id = _require_identifier(request_id, "request_id")
        payload_digest = _require_payload_digest(payload_digest)
        if type(controller_ready) is not bool:
            raise TypeError("controller_ready must be a boolean")
        if type(now) is not datetime or now.tzinfo is None or now.utcoffset() is None:
            raise TypeError("now must be a timezone-aware datetime")
        now = now.astimezone(UTC)

        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            if self._read_command_receipt(connection, request_id) is not None:
                raise RequestConflictError(
                    "request_id is already bound to a different command receipt"
                )
            replay = self._read_direct_dispatch_command(connection, request_id)
            if replay is not None:
                if replay.payload_digest != payload_digest or replay.job != job_id:
                    raise RequestConflictError(
                        "request_id is already bound to a different direct dispatch"
                    )
                if replay.status == "pending":
                    current = self._read_job_control_projection(connection, job_id)
                    exact = self._prepare_exact_publication_recovery(connection,current,request_id,payload_digest,persist=False)
                    if exact is not None:
                        connection.commit()
                        return exact
                    materialized = self.get_materialized_job(job_id)
                    if (
                        materialized is not None
                        and materialized.source_kind is SourceKind.LEGACY_VIDEO
                    ):
                        connection.commit()
                        return self._direct_dispatch_result_from_current(current, "blocked")
                    reconciliation = (
                        None
                        if (
                            materialized is None
                            or connection.execute('SELECT 1 FROM direct_publication_attempts WHERE job_id=?', (job_id,)).fetchone() is not None
                            or self._has_started_direct_publication(connection, job_id)
                            or materialized.source_kind is not SourceKind.DIRECT
                            or replay.generation != current.generation
                            or replay.revision != current.revision
                            or replay.state != current.state
                        )
                        else self._prepare_direct_publication_reconciliation(
                            connection,
                            current=current,
                            materialized=materialized,
                            request_id=request_id,
                            payload_digest=payload_digest,
                            persist_receipt=False,
                        )
                    )
                    if reconciliation is not None:
                        connection.commit()
                        return reconciliation
                    result = DirectDispatchResult(
                        status="blocked",
                        job=current.job,
                        generation=current.generation,
                        revision=current.revision,
                        state=current.state,
                    )
                    self._update_direct_dispatch_command(
                        connection, request_id=request_id, result=result
                    )
                else:
                    result = replay.to_result()
                connection.commit()
                return result

            self._reject_unregistered_legacy_receipt(connection, request_id)
            current_epoch = self._current_worker_epoch(connection)
            current = self._read_job_control_projection(connection, job_id)
            materialized = self.get_materialized_job(job_id)
            if materialized is None:
                raise ValueError("direct dispatch target is not materialized")
            if materialized.source_kind is SourceKind.LEGACY_VIDEO:
                connection.commit()
                return self._direct_dispatch_result_from_current(current, "blocked")

            if expected_worker_epoch != current_epoch:
                result = self._direct_dispatch_result_from_current(current, "stale")
            elif (
                expected_generation != current.generation
                or expected_revision != current.revision
            ):
                result = self._direct_dispatch_result_from_current(current, "stale")
            elif materialized.source_kind is not SourceKind.DIRECT:
                result = self._direct_dispatch_result_from_current(current, "blocked")
            elif (current.state in (JobState.FINALIZING.value, JobState.PAUSED.value)
                and connection.execute('SELECT 1 FROM direct_publication_attempts WHERE job_id=?', (job_id,)).fetchone() is None
                and self._has_started_direct_publication(connection, job_id)):
                # A real STARTED producer cannot inherit metadata-only legacy
                # authority if its attempt was never committed or is missing.
                result = self._direct_dispatch_result_from_current(current, "blocked")
            elif current.state in (JobState.FINALIZING.value, JobState.PAUSED.value):
                exact_row = connection.execute('SELECT status FROM direct_publication_attempts WHERE job_id=?', (job_id,)).fetchone()
                if exact_row is not None:
                    exact = self._prepare_exact_publication_recovery(connection,current,request_id,payload_digest)
                    if exact is not None:
                        connection.commit()
                        return exact
                    result = self._direct_dispatch_result_from_current(current, "blocked")
                    self._insert_direct_dispatch_command(connection,request_id=request_id,payload_digest=payload_digest,
                        job=job_id,status=result.status,generation=result.generation,revision=result.revision,state=result.state)
                    connection.commit()
                    return result
                reconciliation = self._prepare_direct_publication_reconciliation(
                    connection,
                    current=current,
                    materialized=materialized,
                    request_id=request_id,
                    payload_digest=payload_digest,
                )
                if reconciliation is not None:
                    connection.commit()
                    return reconciliation
                result = self._direct_dispatch_result_from_current(current, "blocked")
            elif self._has_other_active_direct_dispatch(connection, job_id):
                result = self._direct_dispatch_result_from_current(current, "blocked")
            elif current.state != JobState.QUEUED.value:
                result = self._direct_dispatch_result_from_current(current, "blocked")
            else:
                admission = self._direct_dispatch_admission(
                    connection, materialized=materialized, now=now
                )
                if not controller_ready or not admission.allowed:
                    result = self._direct_dispatch_result_from_current(current, "blocked")
                else:
                    reservation = self._read_publication_reservation(connection, job_id)
                    if reservation is None:
                        raise ValueError("direct dispatch target has no publication reservation")
                    resolving = self._persist_direct_dispatch_lifecycle(
                        connection,
                        current=current,
                        state=JobState.RESOLVING.value,
                        event_kind="job_resolving",
                    )
                    self._insert_direct_dispatch_command(
                        connection,
                        request_id=request_id,
                        payload_digest=payload_digest,
                        job=job_id,
                        status="pending",
                        generation=resolving.generation,
                        revision=resolving.revision,
                        state=resolving.state,
                    )
                    updated_intent = replace(
                        materialized.intent,
                        generation=resolving.generation,
                        revision=resolving.revision,
                    )
                    plan = _DirectDispatchPlan(
                        job=replace(materialized, intent=updated_intent),
                        reservation=reservation,
                        admission=admission,
                        generation=resolving.generation,
                        revision=resolving.revision,
                        request_id=request_id,
                        payload_digest=payload_digest,
                    )
                    connection.commit()
                    return plan

            self._insert_direct_dispatch_command(
                connection,
                request_id=request_id,
                payload_digest=payload_digest,
                job=job_id,
                status=result.status,
                generation=result.generation,
                revision=result.revision,
                state=result.state,
            )
            connection.commit()
            return result
        except BaseException:
            connection.rollback()
            raise

    def advance_direct_dispatch_to_downloading(
        self, plan: _DirectDispatchPlan
    ) -> _DirectDispatchPlan:
        """Persist ``resolving`` to ``downloading`` before engine unpause."""

        if type(plan) is not _DirectDispatchPlan:
            raise TypeError("plan must be a direct-dispatch plan")
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            command = self._require_pending_direct_dispatch_command(connection, plan)
            current = self._read_job_control_projection(connection, plan.job.job_id)
            if (
                current.generation != plan.generation
                or current.revision != plan.revision
                or current.state != JobState.RESOLVING.value
                or command.generation != plan.generation
                or command.revision != plan.revision
                or command.state != JobState.RESOLVING.value
            ):
                raise ValueError("direct dispatch resolving state is stale")
            downloading = self._persist_direct_dispatch_lifecycle(
                connection,
                current=current,
                state=JobState.DOWNLOADING.value,
                event_kind="job_downloading",
            )
            self._update_pending_direct_dispatch_command(
                connection,
                request_id=plan.request_id,
                generation=downloading.generation,
                revision=downloading.revision,
                state=downloading.state,
            )
            connection.commit()
            return replace(
                plan,
                job=replace(
                    plan.job,
                    intent=replace(
                        plan.job.intent,
                        generation=downloading.generation,
                        revision=downloading.revision,
                    ),
                ),
                generation=downloading.generation,
                revision=downloading.revision,
            )
        except BaseException:
            connection.rollback()
            raise

    def finish_direct_dispatch(self, plan: _DirectDispatchPlan) -> DirectDispatchResult:
        """Record a started receipt only after aria2 accepted the unpause."""

        if type(plan) is not _DirectDispatchPlan:
            raise TypeError("plan must be a direct-dispatch plan")
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            command = self._require_pending_direct_dispatch_command(connection, plan)
            current = self._read_job_control_projection(connection, plan.job.job_id)
            if (
                current.generation != plan.generation
                or current.revision != plan.revision
                or current.state != JobState.DOWNLOADING.value
                or command.generation != plan.generation
                or command.revision != plan.revision
                or command.state != JobState.DOWNLOADING.value
            ):
                raise ValueError("direct dispatch downloading state is stale")
            result = self._direct_dispatch_result_from_current(current, "started")
            self._update_direct_dispatch_command(
                connection, request_id=plan.request_id, result=result
            )
            connection.commit()
            return result
        except BaseException:
            connection.rollback()
            raise

    def finalize_direct_terminal(
        self, terminal: _DirectTerminalPlan, observed: object
    ) -> DirectDispatchResult:
        """Atomically audit verified current completion without changing its receipt.

        No staged/final inode is bound here and no filesystem publication occurs.
        The engine import is confined to this explicitly dispatched terminal path.
        """

        if (
            type(terminal) is _DirectTerminalPlan
            and type(terminal.dispatch) is _DirectDispatchPlan
        ):
            self._require_mutable_source(
                self._connection, terminal.dispatch.job.job_id, direct_only=True
            )
        from hermes_downloads.direct import DirectTransfer

        if type(terminal) is not _DirectTerminalPlan:
            raise TypeError("terminal must be a direct-terminal plan")
        plan = terminal.dispatch
        if (
            type(plan) is not _DirectDispatchPlan
            or type(terminal.record) is not DirectEngineRecord
            or type(terminal.marker) is not PublicationMarkerBinding
            or type(observed) is not DirectTransfer
        ):
            raise TypeError("direct terminal evidence is invalid")
        if (
            observed.job_id != plan.job.job_id
            or observed.generation != plan.generation
            or observed.gid != terminal.gid
            or type(terminal.gid) is not str
            or re.fullmatch(r"[0-9a-fA-F]{16}", terminal.gid) is None
            or observed.partial_path != terminal.partial_path
            or terminal.partial_path.name != plan.job.partial_filename
            or terminal.partial_path.parent.name != plan.job.job_id
            or observed.status != "complete"
            or type(observed.verification) is not CompletionVerification
            or type(observed.hash_verified) is not bool
            or observed.hash_verified
            != (observed.verification is CompletionVerification.CHECKSUM_VERIFIED)
            or _require_counter(observed.total_length, "total_length")
            != _require_counter(observed.completed_length, "completed_length")
        ):
            raise ValueError("direct terminal evidence does not match its owner")
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            current = self._read_job_control_projection(connection, plan.job.job_id)
            command = self._read_direct_dispatch_command(connection, plan.request_id)
            if (
                self._current_worker_epoch(connection) != terminal.record.worker_epoch
                or self.get_direct_engine_record() != terminal.record
                or self.get_direct_engine_activation_fence() is not None
                or self._get_direct_engine_recovery_capability(terminal.record) is None
                or (terminal.capability is not None and
                    self._get_direct_engine_recovery_capability(terminal.record) != terminal.capability)
                or current.generation != plan.generation
                or current.revision != plan.revision
                or current.state != JobState.DOWNLOADING.value
                or self.get_materialized_job(current.job) != plan.job
                or plan.job.source_kind is not SourceKind.DIRECT
                or command != _DirectDispatchCommand(
                    request_id=plan.request_id,
                    payload_digest=plan.payload_digest,
                    job=current.job,
                    status="started",
                    generation=plan.generation,
                    revision=plan.revision,
                    state=JobState.DOWNLOADING.value,
                )
                or self._read_publication_reservation(connection, current.job) != plan.reservation
                or self._read_publication_marker_binding(connection, current.job) != terminal.marker
            ):
                raise ValueError("direct terminal state is stale")
            finalizing = self._persist_direct_dispatch_lifecycle(
                connection,
                current=current,
                state=JobState.FINALIZING.value,
                event_kind="job_finalizing",
            )
            connection.commit()
            return self._direct_dispatch_result_from_current(finalizing, "started")
        except BaseException:
            connection.rollback()
            raise

    def prepare_direct_stage(
        self, terminal: _DirectTerminalPlan, observed: DirectTransfer
    ) -> _DirectStagePlan:
        """Capture live authority after the real terminal producer commits."""

        if type(terminal) is not _DirectTerminalPlan:
            raise TypeError("terminal must be a direct-terminal plan")
        dispatch = terminal.dispatch
        if type(dispatch) is not _DirectDispatchPlan:
            raise TypeError("stage dispatch is invalid")
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            job = self.get_materialized_job(dispatch.job.job_id)
            receipt = self._read_direct_dispatch_command(connection, dispatch.request_id)
            capability = self._get_direct_engine_recovery_capability(terminal.record)
            latest = connection.execute(
                "SELECT event_id FROM events WHERE job_id = ? ORDER BY event_id DESC LIMIT 1",
                (dispatch.job.job_id,),
            ).fetchone()
            if job is None or receipt is None or capability is None or latest is None:
                raise ValueError("direct stage authority is absent")
            stage = _DirectStagePlan(terminal, observed, job, dispatch.revision + 1,
                                     latest["event_id"], receipt, capability)
            self._require_direct_stage_authority(connection, stage)
            connection.commit()
            return stage
        except BaseException:
            connection.rollback()
            raise

    def _require_direct_stage_authority(
        self, connection: sqlite3.Connection, stage: _DirectStagePlan
    ) -> None:
        if (
            type(stage) is _DirectStagePlan
            and type(stage.terminal) is _DirectTerminalPlan
            and type(stage.terminal.dispatch) is _DirectDispatchPlan
        ):
            self._require_mutable_source(
                connection, stage.terminal.dispatch.job.job_id, direct_only=True
            )
        from hermes_downloads.direct import DirectTransfer, _VerifiedPayloadIdentity

        if type(stage) is not _DirectStagePlan:
            raise TypeError("stage must be a direct-stage plan")
        terminal, observed = stage.terminal, stage.observed
        if (type(terminal) is not _DirectTerminalPlan
                or type(terminal.dispatch) is not _DirectDispatchPlan
                or type(terminal.record) is not DirectEngineRecord
                or type(terminal.marker) is not PublicationMarkerBinding
                or type(stage.capability) is not _DirectEngineRecoveryCapability
                or type(stage.receipt) is not _DirectDispatchCommand
                or not isinstance(terminal.partial_path, Path)
                or type(observed) is not DirectTransfer
                or type(observed.verified_identity) is not _VerifiedPayloadIdentity):
            raise TypeError("direct stage evidence is invalid")
        dispatch = terminal.dispatch
        identity = observed.verified_identity
        # Reconstruct to reject malformed evidence even at this private seam.
        _VerifiedPayloadIdentity(identity.st_dev, identity.st_ino,
                                 identity.logical_size, identity.mtime_ns,
                                 identity.st_mode, identity.st_nlink, identity.ctime_ns)
        current = self._read_job_control_projection(connection, dispatch.job.job_id)
        latest = connection.execute(
            "SELECT event_id, kind, generation, revision FROM events "
            "WHERE job_id = ? ORDER BY event_id DESC LIMIT 1", (current.job,),
        ).fetchone()
        audited_count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE job_id = ? AND kind = 'job_finalizing' "
            "AND generation = ? AND revision = ?",
            (current.job, dispatch.generation, stage.revision),
        ).fetchone()[0]
        _require_counter(dispatch.generation, "stage generation")
        _require_counter(dispatch.revision, "stage revision")
        expected_job = replace(dispatch.job, intent=replace(dispatch.job.intent,
                                                           revision=dispatch.revision + 1))
        expected_receipt = _DirectDispatchCommand(
            dispatch.request_id, dispatch.payload_digest, dispatch.job.job_id,
            "started", dispatch.generation, dispatch.revision, JobState.DOWNLOADING.value,
        )
        if (
            type(stage.revision) is not int or stage.revision != dispatch.revision + 1
            or type(stage.audit_id) is not int or stage.audit_id < 1
            or current.generation != dispatch.generation
            or current.revision != stage.revision or current.state != JobState.FINALIZING.value
            or latest is None or audited_count != 1
            or tuple(latest) != (stage.audit_id, "job_finalizing", dispatch.generation, stage.revision)
            or stage.job != expected_job or self.get_materialized_job(current.job) != stage.job
            or dispatch.job.source_kind is not SourceKind.DIRECT
            or stage.receipt != expected_receipt
            or self._read_direct_dispatch_command(connection, dispatch.request_id) != stage.receipt
            or self._current_worker_epoch(connection) != terminal.record.worker_epoch
            or self.get_direct_engine_record() != terminal.record
            or self.get_direct_engine_activation_fence() is not None
            or terminal.capability != stage.capability
            or self._get_direct_engine_recovery_capability(terminal.record) != stage.capability
            or self._read_publication_reservation(connection, current.job) != dispatch.reservation
            or self._read_publication_marker_binding(connection, current.job) != terminal.marker
            or observed.job_id != current.job or observed.generation != dispatch.generation
            or type(terminal.gid) is not str or re.fullmatch(r"[0-9a-fA-F]{16}", terminal.gid) is None
            or observed.gid != terminal.gid or observed.partial_path != terminal.partial_path
            or terminal.partial_path.name != dispatch.job.partial_filename
            or terminal.partial_path.parent.name != current.job
            or observed.status != "complete"
            or type(observed.verification) is not CompletionVerification
            or type(observed.hash_verified) is not bool
            or observed.hash_verified != (observed.verification is CompletionVerification.CHECKSUM_VERIFIED)
            or _require_counter(observed.total_length, "total_length") != identity.logical_size
            or _require_counter(observed.completed_length, "completed_length") != identity.logical_size
        ):
            raise ValueError("direct stage authority is stale")

    def bind_direct_staged_payload(
        self, stage: _DirectStagePlan, staged: StagedPartialPayload
    ) -> _StagedPayloadBinding:
        """Fence all live authority and bind atomically, retaining finalizing."""

        from hermes_downloads.paths import (
            PublicationReservationMarker, StagedPartialPayload,
            _require_current_staged_payload, rehydrate_destination,
        )

        if type(stage) is not _DirectStagePlan or type(staged) is not StagedPartialPayload:
            raise TypeError("direct stage binding evidence is invalid")
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            self._require_direct_stage_authority(connection, stage)
            identity = stage.observed.verified_identity
            if (staged.st_dev, staged.st_ino, staged.logical_size, staged.mtime_ns,
                staged.st_mode, staged.st_nlink, staged.ctime_ns) != (
                identity.st_dev, identity.st_ino, identity.logical_size, identity.mtime_ns,
                identity.st_mode, identity.st_nlink, identity.ctime_ns
            ):
                raise ValueError("attested payload differs from original verification")
            job = stage.job
            destination = rehydrate_destination(
                category=job.category, collection=job.destination_collection,
                partial_filename=job.partial_filename,
                selected_final_filename=job.selected_final_filename, job_id=job.job_id,
            )
            if destination.partial_path != stage.terminal.partial_path:
                raise ValueError("stage path differs from original verification")
            marker = PublicationReservationMarker(
                destination.incomplete_dir / ".hermes-reservation",
                stage.terminal.marker.marker_device, stage.terminal.marker.marker_inode,
            )
            _require_current_staged_payload(destination, stage.terminal.dispatch.reservation,
                                            marker, staged)
            requested = _StagedPayloadBinding(job.job_id, staged.st_dev, staged.st_ino,
                                              staged.logical_size)
            result = self._bind_staged_payload_in_transaction(
                connection, requested=requested,
                claim_token=stage.terminal.dispatch.reservation.claim_token,
            )
            # Recheck before commit; an insert failure or namespace change rolls
            # back without altering receipt, lifecycle, files or marker.
            _require_current_staged_payload(destination, stage.terminal.dispatch.reservation,
                                            marker, staged)
            connection.commit()
            return result
        except BaseException:
            connection.rollback()
            raise

    @staticmethod
    def _has_started_direct_publication(connection, job_id):
        return connection.execute("SELECT 1 FROM direct_dispatch_commands WHERE job_id=? AND status='started' AND state='downloading' LIMIT 1",
            (job_id,)).fetchone() is not None

    @staticmethod
    def _publication_ownership(job, reservation) -> str:
        # Lifecycle and hold flags are fenced by the current pointer instead.
        values = [job.job_id, job.intent.request_id, job.intent.payload_digest,
            job.intent.source_url.hex(), job.source_kind.value, job.queue_collection_id,
            job.priority, job.order_key, None if job.scheduled_for is None else job.scheduled_for.isoformat(),
            job.category, job.destination_collection, job.partial_filename, job.selected_final_filename,
            reservation.job_id, reservation.target_component, reservation.final_filename, reservation.claim_token]
        return hashlib.sha256(json.dumps(values, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()

    def _read_publication_attempt(self, connection, job_id, *, require_current=True, _retiring_closed=False):
        from hermes_downloads.paths import (PreparedPublicationPayload, PublicationReservationMarker,
            StagedPartialPayload, rehydrate_destination)
        _require_identifier(job_id, 'publication job')
        row = connection.execute('SELECT * FROM direct_publication_attempts WHERE job_id = ?', (job_id,)).fetchone()
        if row is None:
            return None
        for key in ('job_id', 'attempt_id', 'original_request_id', 'proof', 'status', 'state'):
            _require_sqlite_text(row[key], 'publication ' + key)
        if row['job_id'] != job_id:
            raise ValueError('publication attempt job changed')
        _require_identifier(row['attempt_id'], 'attempt_id')
        if row['status'] not in {'eligible','finished','closed'} or row['state'] not in {'finalizing','paused','completed','removed','queued'}:
            raise ValueError('publication attempt state is invalid')
        if connection.execute('SELECT 1 FROM closed_direct_publication_attempts WHERE attempt_id=? OR original_request_id=?',
                (row['attempt_id'],row['original_request_id'])).fetchone() is not None:
            raise ValueError('publication attempt is also retired')
        counters = tuple(_require_counter(_require_sqlite_integer(row[k], k), k)
            for k in ('audit_id','generation','revision','worker_epoch'))
        if counters[0] < 1 or counters[3] < 1:
            raise ValueError('publication attempt pointer is invalid')
        if type(row['proof']) is not str or len(row['proof']) > 4096:
            raise ValueError('publication attempt proof is invalid')
        proof = json.loads(row['proof'])
        if type(proof) is not dict or set(proof) != {'request','digest','generation','downloading_revision',
                'finalizing_revision','finalizing_audit','epoch','ownership','marker','stage','chain','sha256'}:
            raise ValueError('publication attempt proof shape is invalid')
        _require_identifier(proof['request'], 'publication request')
        for key in ('digest','ownership','sha256'):
            _require_payload_digest(proof[key])
        for key in ('generation','downloading_revision','finalizing_revision','finalizing_audit','epoch'):
            _require_counter(proof[key], key)
        if (proof['finalizing_revision'] != proof['downloading_revision'] + 1
                or proof['finalizing_audit'] < 1 or proof['epoch'] < 1
                or proof['request'] != row['original_request_id']):
            raise ValueError('publication original fence is invalid')
        if (counters[1] < proof['generation'] or counters[2] < proof['finalizing_revision']
            or counters[3] < proof['epoch']
            or (row['status'] == 'eligible' and row['state'] not in {'finalizing', 'paused'})
            or (row['status'] == 'finished' and (row['state'] != 'completed' or row['pending_request_id'] is not None))
            or (row['state'] == 'finalizing' and counters != (proof['finalizing_audit'],
                proof['generation'], proof['finalizing_revision'], proof['epoch']))):
            raise ValueError('publication successor fence is invalid')
        for key, length in (('marker',2),('stage',7)):
            values = proof[key]
            if type(values) is not list or len(values) != length:
                raise ValueError('publication metadata shape is invalid')
            for value in values:
                _require_counter(value, 'publication metadata')
        if not stat.S_ISREG(proof['stage'][3]) or proof['stage'][4] != 1:
            raise ValueError('publication original stage is invalid')
        if type(proof['chain']) is not list or len(proof['chain']) != 4:
            raise ValueError('publication chain is invalid')
        for pair in proof['chain']:
            if type(pair) is not list or len(pair) != 2:
                raise ValueError('publication chain is invalid')
            for value in pair:
                _require_counter(value, 'publication directory')
        receipt = self._read_direct_dispatch_command(connection, proof['request'])
        expected = _DirectDispatchCommand(proof['request'], proof['digest'], job_id, 'started',
            proof['generation'], proof['downloading_revision'], 'downloading')
        job = self.get_materialized_job(job_id)
        reservation = self._read_publication_reservation(connection, job_id)
        marker = self._read_publication_marker_binding(connection, job_id)
        staged = self._read_staged_payload_binding(connection, job_id)
        if (receipt != expected or job is None or job.source_kind is not SourceKind.DIRECT
            or reservation is None or marker is None or staged is None
            or self._publication_ownership(job,reservation) != proof['ownership']
            or (marker.marker_device,marker.marker_inode) != tuple(proof['marker'])
            or (not _retiring_closed and (staged.partial_device,staged.partial_inode,staged.logical_size) != tuple(proof['stage'][:3]))):
            raise ValueError('publication immutable authority changed')
        original_event = connection.execute('SELECT kind,job_id,generation,revision FROM events WHERE event_id = ?',
            (proof['finalizing_audit'],)).fetchone()
        if original_event is None or tuple(original_event) != ('job_finalizing',job_id,proof['generation'],proof['finalizing_revision']):
            raise ValueError('publication original audit changed')
        if _retiring_closed:
            current = self._read_job_control_projection(connection,job_id)
            if (require_current or row['status'] != 'closed' or row['pending_request_id'] is not None
                or not self._publication_closure_corroborated(connection,row)
                or counters[0] <= proof['finalizing_audit']
                or counters[1] > current.generation or counters[2] >= current.revision
                or counters[3] > self._current_worker_epoch(connection)):
                raise ValueError('closed publication attempt authority is invalid')
        destination = rehydrate_destination(category=job.category, collection=job.destination_collection,
            partial_filename=job.partial_filename, selected_final_filename=job.selected_final_filename, job_id=job_id)
        dev,ino,size,mode,nlink,mtime,ctime = proof['stage']
        prepared = PreparedPublicationPayload(destination,reservation,
            PublicationReservationMarker(destination.incomplete_dir / '.hermes-reservation', *proof['marker']),
            StagedPartialPayload(destination.partial_path,dev,ino,size,mtime,mode,nlink,ctime),
            proof['sha256'],tuple(map(tuple,proof['chain'])))
        pending = row['pending_request_id']
        pending_digest = None
        if pending is not None:
            _require_identifier(_require_sqlite_text(pending,'publication pending'), 'publication pending')
            command = self._read_direct_dispatch_command(connection, pending)
            if (pending == proof['request'] or command is None or command.status != 'pending'
                or (command.job, command.generation, command.revision, command.state) !=
                (job_id, counters[1], counters[2], row['state'])):
                raise ValueError('publication pending receipt is stale')
            pending_digest = command.payload_digest
        attempt = _DirectPublicationAttempt(row['attempt_id'],job,prepared,row['proof'],*counters[:3],
            row['state'],counters[3],pending,pending_digest)
        if require_current:
            current = self._read_job_control_projection(connection,job_id)
            latest = connection.execute('SELECT event_id,generation,revision,kind FROM events WHERE job_id = ? ORDER BY event_id DESC LIMIT 1', (job_id,)).fetchone()
            if (row['status'] != 'eligible' or latest is None
                or tuple(latest) != (*counters[:3], 'job_' + row['state'])
                or (current.generation,current.revision,current.state) != (attempt.generation,attempt.revision,attempt.state)
                or self._current_worker_epoch(connection) != attempt.worker_epoch):
                raise ValueError('publication current pointer is stale')
        return attempt

    def reserve_direct_publication(self, stage, prepared):
        from hermes_downloads.paths import (PreparedPublicationPayload, _require_current_staged_payload,
            _require_strict_publication_namespace, _open_visible_publication_chain,
            _open_staged_partial_payload, _strict_staged_metadata)
        import os
        if type(stage) is not _DirectStagePlan or stage.job.source_kind is not SourceKind.DIRECT:
            raise ValueError('publication requires direct stage')
        if type(prepared) is not PreparedPublicationPayload:
            raise TypeError('publication preparation is invalid')
        connection = self._connection
        connection.execute('BEGIN IMMEDIATE')
        try:
            self._require_direct_stage_authority(connection,stage)
            if (prepared.reservation != stage.terminal.dispatch.reservation
                or (prepared.marker.st_dev,prepared.marker.st_ino) != (stage.terminal.marker.marker_device,stage.terminal.marker.marker_inode)):
                raise ValueError('publication preparation authority changed')
            identity = stage.observed.verified_identity
            original = (identity.st_dev,identity.st_ino,identity.logical_size,identity.st_mode,identity.st_nlink,identity.mtime_ns,identity.ctime_ns)
            if _strict_staged_metadata(prepared.destination,prepared.staged_payload) != original:
                raise ValueError('publication stage metadata changed')
            def check():
                _require_current_staged_payload(prepared.destination,prepared.reservation,prepared.marker,prepared.staged_payload)
                directories = _open_visible_publication_chain(prepared.destination.root,
                    prepared.reservation.target_component,stage.job.job_id,prepared.directory_identities)
                try:
                    fd = _open_staged_partial_payload(directories[3],prepared.destination.partial_path.name)
                    try:
                        _require_strict_publication_namespace(prepared,fd,original,published=False)
                    finally:
                        os.close(fd)
                finally:
                    for fd in reversed(directories): os.close(fd)
            check()
            dispatch = stage.terminal.dispatch
            proof = json.dumps(dict(request=dispatch.request_id,digest=dispatch.payload_digest,
                generation=dispatch.generation,downloading_revision=dispatch.revision,
                finalizing_revision=stage.revision,finalizing_audit=stage.audit_id,
                epoch=stage.terminal.record.worker_epoch,ownership=self._publication_ownership(stage.job,prepared.reservation),
                marker=[prepared.marker.st_dev,prepared.marker.st_ino],stage=original,
                chain=prepared.directory_identities,sha256=prepared.sha256),sort_keys=True,separators=(',',':'))
            closed = connection.execute('SELECT * FROM direct_publication_attempts WHERE job_id=?',
                (stage.job.job_id,)).fetchone()
            if closed is not None:
                # A retired attempt is proof only. It never authorizes a new permit
                # or supplies the current pointer, even if its files still exist.
                retired = self._read_publication_attempt(connection,stage.job.job_id,
                    require_current=False,_retiring_closed=True)
                retired_proof = json.loads(retired.proof)
                retired_receipt = self._read_direct_dispatch_command(connection,closed['original_request_id'])
                retired_audits = tuple(tuple(connection.execute('SELECT * FROM events WHERE event_id=?',
                    (event_id,)).fetchone()) for event_id in (retired_proof['finalizing_audit'],closed['audit_id']))
                connection.execute('INSERT INTO closed_direct_publication_attempts VALUES (?,?,?,?,?,?,?,?,?,?,?)',tuple(closed))
                connection.execute("DELETE FROM direct_publication_attempts WHERE job_id=? AND attempt_id=? AND status='closed'",
                    (stage.job.job_id,closed['attempt_id']))
                self._require_one_changed_row(connection,'closed publication retirement')
            connection.execute('INSERT INTO direct_publication_attempts VALUES (?,?,?,?,?,?,?,?,?,?,NULL)',
                (stage.job.job_id,secrets.token_hex(32),dispatch.request_id,proof,'eligible',stage.audit_id,
                 dispatch.generation,stage.revision,'finalizing',stage.terminal.record.worker_epoch))
            attempt = self._read_publication_attempt(connection,stage.job.job_id)
            if attempt is None:
                raise ValueError('publication attempt insert did not persist')
            if closed is not None:
                archived = connection.execute('SELECT * FROM closed_direct_publication_attempts WHERE attempt_id=?',
                    (closed['attempt_id'],)).fetchone()
                if archived is None or tuple(archived) != tuple(closed):
                    raise ValueError('closed publication proof changed')
                if (self._read_direct_dispatch_command(connection,closed['original_request_id']) != retired_receipt
                    or tuple(tuple(row) if row is not None else () for row in
                        (connection.execute('SELECT * FROM events WHERE event_id=?',(event_id,)).fetchone()
                         for event_id in (retired_proof['finalizing_audit'],closed['audit_id']))) != retired_audits):
                    raise ValueError('closed publication receipt or audit changed')
            check()
            connection.commit()
            return attempt
        except BaseException:
            connection.rollback()
            raise

    def complete_direct_publication(self, attempt, published, *, initial_stage=None):
        from hermes_downloads.paths import require_current_publication_payload
        if type(attempt) is not _DirectPublicationAttempt:
            raise TypeError('publication attempt is invalid')
        connection = self._connection
        connection.execute('BEGIN IMMEDIATE')
        try:
            current_attempt = self._read_publication_attempt(connection,attempt.job.job_id)
            if current_attempt != attempt:
                raise ValueError('publication completion plan is stale')
            if initial_stage is not None:
                self._require_direct_stage_authority(connection,initial_stage)
            if attempt.pending_request_id is not None:
                command = self._read_direct_dispatch_command(connection,attempt.pending_request_id)
                if command is None or command.status != 'pending' or (command.job,command.generation,command.revision,command.state) != (attempt.job.job_id,attempt.generation,attempt.revision,attempt.state):
                    raise ValueError('publication recovery receipt is stale')
            require_current_publication_payload(attempt.prepared,published)
            self._bind_final_publication_in_transaction(connection,
                requested=_FinalPublicationBinding(attempt.job.job_id,published.st_dev,published.st_ino,published.logical_size),
                claim_token=attempt.prepared.reservation.claim_token)
            current = self._read_job_control_projection(connection,attempt.job.job_id)
            completed = self._persist_direct_dispatch_lifecycle(connection,current=current,state='completed',event_kind='job_completed')
            audit = connection.execute('SELECT MAX(event_id) FROM events WHERE job_id = ?', (completed.job,)).fetchone()[0]
            connection.execute("UPDATE direct_publication_attempts SET status='finished',audit_id=?,revision=?,state='completed',pending_request_id=NULL WHERE job_id=? AND attempt_id=? AND status='eligible'",
                (audit,completed.revision,completed.job,attempt.attempt_id))
            self._require_one_changed_row(connection,'publication finish')
            result = self._direct_dispatch_result_from_current(completed,'started')
            if attempt.pending_request_id is not None:
                self._update_direct_dispatch_command(connection,request_id=attempt.pending_request_id,result=result)
            # Original receipt, reservation and all immutable proof are checked again.
            finished = self._read_publication_attempt(connection,completed.job,require_current=False)
            latest = connection.execute('SELECT event_id,kind,generation,revision FROM events WHERE job_id=? ORDER BY event_id DESC LIMIT 1',
                (completed.job,)).fetchone()
            status = connection.execute('SELECT status FROM direct_publication_attempts WHERE job_id=?', (completed.job,)).fetchone()
            if (finished is None or status['status'] != 'finished'
                or finished.attempt_id != attempt.attempt_id or finished.proof != attempt.proof
                or finished.prepared != attempt.prepared
                or (finished.audit_id,finished.generation,finished.revision,finished.state,finished.worker_epoch) !=
                    (audit,completed.generation,completed.revision,'completed',attempt.worker_epoch)
                or self._current_worker_epoch(connection) != attempt.worker_epoch
                or self._read_job_control_projection(connection,completed.job) != completed
                or latest is None or tuple(latest) != (audit,'job_completed',completed.generation,completed.revision)):
                raise ValueError('publication completed authority changed')
            if attempt.pending_request_id is not None:
                expected_receipt = _DirectDispatchCommand(attempt.pending_request_id,
                    attempt.pending_payload_digest,completed.job,'started',completed.generation,
                    completed.revision,'completed')
                if self._read_direct_dispatch_command(connection,attempt.pending_request_id) != expected_receipt:
                    raise ValueError('publication completed receipt changed')
            require_current_publication_payload(attempt.prepared,published)
            connection.commit()
            return result
        except BaseException:
            connection.rollback()
            raise

    def _prepare_exact_publication_recovery(self, connection, current, request_id, payload_digest, *, persist=True):
        job = self.get_materialized_job(current.job)
        if job is None or job.source_kind is not SourceKind.DIRECT:
            return None
        row = connection.execute('SELECT status FROM direct_publication_attempts WHERE job_id=?', (current.job,)).fetchone()
        if row is None or row['status'] != 'eligible':
            return None
        attempt = self._read_publication_attempt(connection,current.job)
        if attempt.pending_request_id not in (None,request_id):
            raise ValueError('publication recovery already pending')
        if persist:
            self._insert_direct_dispatch_command(connection,request_id=request_id,payload_digest=payload_digest,
                job=current.job,status='pending',generation=current.generation,revision=current.revision,state=current.state)
            connection.execute('UPDATE direct_publication_attempts SET pending_request_id=? WHERE job_id=?', (request_id,current.job))
            attempt = self._read_publication_attempt(connection,current.job)
        else:
            command = self._read_direct_dispatch_command(connection,request_id)
            if attempt.pending_request_id != request_id or command is None or command.payload_digest != payload_digest or command.status != 'pending':
                return None
        return _DirectAttemptRecoveryPlan(attempt,request_id,payload_digest)

    def abort_exact_publication_recovery(self, plan):
        if type(plan) is not _DirectAttemptRecoveryPlan:
            raise TypeError('publication recovery plan is invalid')
        connection = self._connection
        connection.execute('BEGIN IMMEDIATE')
        try:
            job = self.get_materialized_job(plan.attempt.job.job_id)
            if job is None or job.source_kind is not SourceKind.DIRECT:
                raise ValueError('publication recovery requires direct source')
            current = self._read_job_control_projection(connection,plan.attempt.job.job_id)
            result = self._direct_dispatch_result_from_current(current,'blocked')
            command = self._read_direct_dispatch_command(connection,plan.request_id)
            if command is not None and command.status == 'pending' and command.payload_digest == plan.payload_digest:
                self._update_direct_dispatch_command(connection,request_id=plan.request_id,result=result)
            connection.execute('UPDATE direct_publication_attempts SET pending_request_id=NULL WHERE job_id=? AND pending_request_id=?', (current.job,plan.request_id))
            connection.commit()
            return result
        except BaseException:
            connection.rollback()
            raise

    def _advance_publication_pointer(self, connection, predecessor, *, preserve, old_epoch=None):
        job = self.get_materialized_job(predecessor.job)
        if job is None or job.source_kind is not SourceKind.DIRECT:
            return
        row = connection.execute('SELECT * FROM direct_publication_attempts WHERE job_id=?', (predecessor.job,)).fetchone()
        if row is None or row['status'] != 'eligible':
            return
        latest = connection.execute('SELECT event_id,generation,revision FROM events WHERE job_id=? ORDER BY event_id DESC LIMIT 1', (predecessor.job,)).fetchone()
        current = self._read_job_control_projection(connection,predecessor.job)
        epoch = self._current_worker_epoch(connection)
        old = (predecessor.generation,predecessor.revision,predecessor.state,
            epoch if old_epoch is None else old_epoch)
        matches = (row['generation'],row['revision'],row['state'],row['worker_epoch']) == old
        # Caller checked predecessor's latest audit before its own mutation.
        eligible = preserve and matches and current.state == 'paused'
        if row['pending_request_id'] is not None:
            command = self._read_direct_dispatch_command(connection,row['pending_request_id'])
            if command is not None and command.status == 'pending':
                self._update_direct_dispatch_command(connection,request_id=command.request_id,
                    result=self._direct_dispatch_result_from_current(current,'blocked'))
        connection.execute('UPDATE direct_publication_attempts SET status=?,audit_id=?,generation=?,revision=?,state=?,worker_epoch=?,pending_request_id=NULL WHERE job_id=?',
            ('eligible' if eligible else 'closed',latest['event_id'],current.generation,current.revision,current.state,epoch,current.job))

    def _publication_predecessor_matches(self, connection, current):
        job = self.get_materialized_job(current.job)
        if job is None or job.source_kind is not SourceKind.DIRECT:
            return False
        row = connection.execute('SELECT status FROM direct_publication_attempts WHERE job_id=?', (current.job,)).fetchone()
        if row is None or row['status'] != 'eligible':
            return False
        try:
            return self._read_publication_attempt(connection, current.job) is not None
        except (TypeError, ValueError):
            return False

    def abort_direct_dispatch(self, plan: _DirectDispatchPlan) -> DirectDispatchResult:
        """Durably pause a contained uncertain dispatch without retrying it."""

        if type(plan) is not _DirectDispatchPlan:
            raise TypeError("plan must be a direct-dispatch plan")
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            command = self._require_pending_direct_dispatch_command(connection, plan)
            current = self._read_job_control_projection(connection, plan.job.job_id)
            if current.generation != plan.generation:
                raise ValueError("direct dispatch generation is stale")
            if current.state in {JobState.RESOLVING.value, JobState.DOWNLOADING.value}:
                current = self._persist_direct_dispatch_lifecycle(
                    connection,
                    current=current,
                    state=JobState.PAUSED.value,
                    event_kind="job_paused",
                )
            if current.state != JobState.PAUSED.value:
                raise ValueError("direct dispatch cannot be safely aborted")
            if command.job != current.job:
                raise ValueError("direct dispatch command job does not match")
            result = self._direct_dispatch_result_from_current(current, "blocked")
            self._update_direct_dispatch_command(
                connection, request_id=plan.request_id, result=result
            )
            connection.commit()
            return result
        except BaseException:
            connection.rollback()
            raise

    def complete_direct_publication_reconciliation(
        self,
        plan: _DirectPublicationReconciliationPlan,
        *,
        final_device: object,
        final_inode: object,
        logical_size: object,
    ) -> DirectDispatchResult:
        """Bind an already-published final payload and complete it atomically."""

        if type(plan) is not _DirectPublicationReconciliationPlan:
            raise TypeError("plan must be a direct-publication reconciliation plan")
        requested = _FinalPublicationBinding(
            job_id=plan.job.job_id,
            final_device=_require_counter(final_device, "final_device"),
            final_inode=_require_counter(final_inode, "final_inode"),
            logical_size=_require_counter(logical_size, "logical_size"),
        )
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            command = self._require_pending_direct_dispatch_command(connection, plan)
            current = self._read_job_control_projection(connection, plan.job.job_id)
            if (
                current.generation != plan.generation
                or current.revision != plan.revision
                or not self._has_audited_finalization_cutpoint(connection, current)
                or command.generation != plan.generation
                or command.revision != plan.revision
                or command.state != current.state
                or self._read_publication_reservation(connection, current.job)
                != plan.reservation
                or self._read_publication_marker_binding(connection, current.job)
                != plan.marker
                or self._read_staged_payload_binding(connection, current.job) != plan.staged
            ):
                raise ValueError("direct publication reconciliation state is stale")
            self._bind_final_publication_in_transaction(
                connection,
                requested=requested,
                claim_token=plan.reservation.claim_token,
            )
            completed = self._persist_direct_dispatch_lifecycle(
                connection,
                current=current,
                state=JobState.COMPLETED.value,
                event_kind="job_completed",
            )
            result = self._direct_dispatch_result_from_current(completed, "started")
            self._update_direct_dispatch_command(
                connection, request_id=plan.request_id, result=result
            )
            connection.commit()
            return result
        except BaseException:
            connection.rollback()
            raise

    def abort_direct_publication_reconciliation(
        self, plan: _DirectPublicationReconciliationPlan
    ) -> DirectDispatchResult:
        """Close a failed final-publication receipt without retrying its transfer."""

        if type(plan) is not _DirectPublicationReconciliationPlan:
            raise TypeError("plan must be a direct-publication reconciliation plan")
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            command = self._require_pending_direct_dispatch_command(connection, plan)
            current = self._read_job_control_projection(connection, plan.job.job_id)
            if (
                current.generation != plan.generation
                or current.revision != plan.revision
                or not self._has_audited_finalization_cutpoint(connection, current)
                or command.generation != plan.generation
                or command.revision != plan.revision
                or command.state != current.state
            ):
                raise ValueError("direct publication reconciliation state is stale")
            result = self._direct_dispatch_result_from_current(current, "blocked")
            self._update_direct_dispatch_command(
                connection, request_id=plan.request_id, result=result
            )
            connection.commit()
            return result
        except BaseException:
            connection.rollback()
            raise

    def pause_active_direct_job(
        self, *, job_id: str, generation: int, revision: int, _publication_recoverable: bool = False
    ) -> JobControlResult:
        """Persist a contained active direct job as paused without a new request."""

        job_id = _require_identifier(job_id, "job_id")
        generation = _require_counter(generation, "generation")
        revision = _require_counter(revision, "revision")
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            current = self._read_job_control_projection(connection, job_id)
            if (
                current.generation != generation
                or current.revision != revision
                or current.state not in {JobState.DOWNLOADING.value, JobState.FINALIZING.value}
            ):
                raise ValueError("active direct job is stale")
            predecessor = self._publication_predecessor_matches(connection,current)
            paused = self._persist_direct_dispatch_lifecycle(
                connection, current=current, state=JobState.PAUSED.value, event_kind="job_paused")
            self._advance_publication_pointer(connection,current,
                preserve=predecessor and _publication_recoverable)
            connection.commit()
            return paused.to_result("applied")
        except BaseException:
            connection.rollback()
            raise

    @staticmethod
    def _direct_dispatch_result_from_current(
        current: _JobControlProjection, status: str
    ) -> DirectDispatchResult:
        return DirectDispatchResult(
            status=status,
            job=current.job,
            generation=current.generation,
            revision=current.revision,
            state=current.state,
        )

    def _prepare_direct_publication_reconciliation(
        self,
        connection: sqlite3.Connection,
        *,
        current: _JobControlProjection,
        materialized: MaterializedJob,
        request_id: str,
        payload_digest: str,
        persist_receipt: bool = True,
    ) -> _DirectPublicationReconciliationPlan | None:
        """Return a plan only for a supported durable finalization cutpoint."""

        self._require_mutable_source(connection, current.job, direct_only=True)
        if materialized.source_kind is not SourceKind.DIRECT:
            raise ValueError("unsupported source kind")
        if not self._has_audited_finalization_cutpoint(connection, current):
            return None
        try:
            reservation = self._read_publication_reservation(connection, current.job)
            marker = self._read_publication_marker_binding(connection, current.job)
            staged = self._read_staged_payload_binding(connection, current.job)
            if (
                reservation is None
                or marker is None
                or staged is None
            ):
                return None
            # Reject a corrupt pre-existing final binding before filesystem work.
            self._read_final_publication_binding(connection, current.job)
        except (TypeError, ValueError):
            return None
        if persist_receipt:
            self._insert_direct_dispatch_command(
                connection,
                request_id=request_id,
                payload_digest=payload_digest,
                job=current.job,
                status="pending",
                generation=current.generation,
                revision=current.revision,
                state=current.state,
            )
        updated_intent = replace(
            materialized.intent,
            generation=current.generation,
            revision=current.revision,
        )
        return _DirectPublicationReconciliationPlan(
            job=replace(materialized, intent=updated_intent),
            reservation=reservation,
            marker=marker,
            staged=staged,
            generation=current.generation,
            revision=current.revision,
            request_id=request_id,
            payload_digest=payload_digest,
        )

    @staticmethod
    def _has_other_active_direct_dispatch(
        connection: sqlite3.Connection, job_id: str
    ) -> bool:
        """Allow at most one body-capable direct lifecycle per worker."""

        return (
            connection.execute(
                """
                SELECT 1
                FROM jobs AS job
                JOIN materialized_jobs AS domain ON domain.job_id = job.job_id
                WHERE job.job_id != ?
                  AND domain.source_kind = 'direct'
                  AND job.state IN ('resolving', 'downloading', 'pausing')
                LIMIT 1
                """,
                (job_id,),
            ).fetchone()
            is not None
        )

    @staticmethod
    def _direct_dispatch_admission(
        connection: sqlite3.Connection,
        *,
        materialized: MaterializedJob,
        now: datetime,
    ) -> Admission:
        SQLiteStore._require_mutable_source(
            connection, materialized.job_id, direct_only=True
        )
        if materialized.source_kind is not SourceKind.DIRECT:
            raise ValueError("unsupported source kind")
        collection_held = (
            materialized.queue_collection_id is not None
            and connection.execute(
                "SELECT 1 FROM collection_holds WHERE collection_id = ? LIMIT 1",
                (materialized.queue_collection_id,),
            ).fetchone()
            is not None
        )
        due = materialized.start_now_requested or (
            materialized.scheduled_for is None or materialized.scheduled_for <= now
        )
        return Admission(
            queue_running=SQLiteStore._current_queue_gate(connection) == "running",
            collection_held=collection_held,
            authorized=materialized.authorized,
            item_held=materialized.manual_hold,
            due=due,
        )

    @staticmethod
    def _persist_direct_dispatch_lifecycle(
        connection: sqlite3.Connection,
        *,
        current: _JobControlProjection,
        state: str,
        event_kind: str,
    ) -> _JobControlProjection:
        SQLiteStore._require_mutable_source(connection, current.job, direct_only=True)
        if current.revision == _MAX_COUNTER:
            raise OverflowError("job revision exceeds persisted counter range")
        next_state = _require_public_job_state(state, "direct dispatch state")
        updated = replace(current, revision=current.revision + 1, state=next_state)
        connection.execute(
            "UPDATE jobs SET revision = ?, state = ? WHERE job_id = ?",
            (updated.revision, updated.state, updated.job),
        )
        SQLiteStore._require_one_changed_row(connection, "direct dispatch lifecycle update")
        connection.execute(
            """
            INSERT INTO events (kind, job_id, generation, revision)
            VALUES (?, ?, ?, ?)
            """,
            (event_kind, updated.job, updated.generation, updated.revision),
        )
        return updated

    @staticmethod
    def _insert_direct_dispatch_command(
        connection: sqlite3.Connection,
        *,
        request_id: str,
        payload_digest: str,
        job: str,
        status: str,
        generation: int,
        revision: int,
        state: str,
    ) -> None:
        if status not in {"pending", *_DIRECT_DISPATCH_STATUSES}:
            raise ValueError("direct dispatch persistence status is invalid")
        connection.execute(
            """
            INSERT INTO direct_dispatch_commands (
                request_id, payload_digest, job_id, status, generation, revision, state
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (request_id, payload_digest, job, status, generation, revision, state),
        )

    @staticmethod
    def _update_pending_direct_dispatch_command(
        connection: sqlite3.Connection,
        *,
        request_id: str,
        generation: int,
        revision: int,
        state: str,
    ) -> None:
        connection.execute(
            """
            UPDATE direct_dispatch_commands
            SET generation = ?, revision = ?, state = ?
            WHERE request_id = ? AND status = 'pending'
            """,
            (generation, revision, state, request_id),
        )
        SQLiteStore._require_one_changed_row(connection, "pending direct dispatch update")

    @staticmethod
    def _update_direct_dispatch_command(
        connection: sqlite3.Connection, *, request_id: str, result: DirectDispatchResult
    ) -> None:
        connection.execute(
            """
            UPDATE direct_dispatch_commands
            SET status = ?, generation = ?, revision = ?, state = ?
            WHERE request_id = ? AND status = 'pending'
            """,
            (
                result.status,
                result.generation,
                result.revision,
                result.state,
                request_id,
            ),
        )
        SQLiteStore._require_one_changed_row(connection, "direct dispatch receipt update")

    @staticmethod
    def _read_direct_dispatch_command(
        connection: sqlite3.Connection, request_id: str
    ) -> _DirectDispatchCommand | None:
        rows = connection.execute(
            """
            SELECT request_id, payload_digest, job_id, status, generation, revision, state
            FROM direct_dispatch_commands
            WHERE request_id = ?
            LIMIT 2
            """,
            (request_id,),
        ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError("direct dispatch receipt is not unique")
        row = rows[0]
        return _DirectDispatchCommand(
            request_id=_require_identifier(
                _require_sqlite_text(row["request_id"], "direct dispatch request_id"),
                "direct dispatch request_id",
            ),
            payload_digest=_require_payload_digest(
                _require_sqlite_text(
                    row["payload_digest"], "direct dispatch payload_digest"
                )
            ),
            job=_require_identifier(
                _require_sqlite_text(row["job_id"], "direct dispatch job_id"),
                "direct dispatch job_id",
            ),
            status=_require_sqlite_text(row["status"], "direct dispatch status"),
            generation=_require_counter(
                _require_sqlite_integer(row["generation"], "direct dispatch generation"),
                "direct dispatch generation",
            ),
            revision=_require_counter(
                _require_sqlite_integer(row["revision"], "direct dispatch revision"),
                "direct dispatch revision",
            ),
            state=_require_public_job_state(row["state"], "direct dispatch state"),
        )

    @staticmethod
    def _require_pending_direct_dispatch_command(
        connection: sqlite3.Connection,
        plan: _DirectDispatchPlan | _DirectPublicationReconciliationPlan,
    ) -> _DirectDispatchCommand:
        SQLiteStore._require_mutable_source(connection, plan.job.job_id, direct_only=True)
        command = SQLiteStore._read_direct_dispatch_command(connection, plan.request_id)
        if (
            command is None
            or command.status != "pending"
            or command.payload_digest != plan.payload_digest
            or command.job != plan.job.job_id
        ):
            raise ValueError("direct dispatch receipt is not pending")
        return command

    @staticmethod
    def _has_audited_finalization_cutpoint(
        connection: sqlite3.Connection, current: _JobControlProjection
    ) -> bool:
        """Prove the current cutpoint, or its immediate cold-paused successor.

        Cold recovery advances both fences exactly once. Ordinary pause only
        advances revision; repeated recovery and intervening job audits cannot
        inherit an earlier finalization authority.
        """

        events = connection.execute(
            """
            SELECT kind, generation, revision FROM events
            WHERE job_id = ? ORDER BY event_id DESC LIMIT 2
            """,
            (current.job,),
        ).fetchall()
        if not events:
            return False
        latest = events[0]
        if current.state == JobState.FINALIZING.value:
            return (
                latest["kind"] == "job_finalizing"
                and latest["generation"] == current.generation
                and latest["revision"] == current.revision
            )
        return (
            current.state == JobState.PAUSED.value
            and len(events) == 2
            and latest["kind"] == "job_paused"
            and latest["generation"] == current.generation
            and latest["revision"] == current.revision
            and events[1]["kind"] == "job_finalizing"
            and events[1]["generation"] == current.generation - 1
            and events[1]["revision"] == current.revision - 1
        )

    def recover_cold_start(self) -> int:
        """Atomically fence a cold worker epoch and pause incomplete jobs."""

        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            old_epoch = self.worker_epoch()
            eligible_predecessors = {}
            for row in connection.execute("SELECT job_id FROM direct_publication_attempts WHERE status='eligible'").fetchall():
                job = self.get_materialized_job(row['job_id'])
                if job is None or job.source_kind is not SourceKind.DIRECT:
                    continue
                current = self._read_job_control_projection(connection,row['job_id'])
                eligible_predecessors[current.job] = (current,self._publication_predecessor_matches(connection,current))
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
                SELECT job_id, generation, revision, state
                FROM jobs
                WHERE state IN (?, ?, ?, ?, ?, ?, ?)
                  AND NOT EXISTS (
                      SELECT 1 FROM materialized_jobs AS domain
                      WHERE domain.job_id = jobs.job_id AND domain.source_kind != 'direct'
                  )
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
            for predecessor, matches in eligible_predecessors.values():
                self._advance_publication_pointer(connection,predecessor,preserve=matches,old_epoch=old_epoch)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return epoch

    def queue_gate_snapshot(self) -> tuple[str, int] | None:
        """Return the current durable queue gate and its fencing revision."""

        row = self._connection.execute(
            "SELECT value, revision FROM settings WHERE key = 'queue_gate'"
        ).fetchone()
        if row is None:
            return None
        return (
            _require_queue_gate(_require_sqlite_text(row["value"], "queue gate value")),
            _require_counter(
                _require_sqlite_integer(row["revision"], "queue gate revision"),
                "queue gate revision",
            ),
        )

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
        SQLiteStore._require_mutable_source(connection, budget.job_id)
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
            recovery_capability = connection.execute(
                """
                SELECT 1
                FROM direct_engine_recovery_capabilities
                WHERE engine_kind = 'direct'
                LIMIT 1
                """
            ).fetchone()
            if (
                direct_record is not None
                or existing_fence is not None
                or recovery_capability is not None
            ):
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
            recovery_capability = connection.execute(
                """
                SELECT 1
                FROM direct_engine_recovery_capabilities
                WHERE engine_kind = 'direct'
                LIMIT 1
                """
            ).fetchone()
            if recovery_capability is not None:
                raise ValueError("direct engine recovery capability is already present")

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
            recovery_capability = connection.execute(
                """
                SELECT 1
                FROM direct_engine_recovery_capabilities
                WHERE engine_kind = 'direct'
                LIMIT 1
                """
            ).fetchone()
            if recovery_capability is not None:
                raise ValueError("direct engine record conflicts with a recovery capability")
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

    def _get_direct_engine_recovery_capability(
        self, record: DirectEngineRecord
    ) -> _DirectEngineRecoveryCapability | None:
        """Read only the private shutdown authority exactly paired to ``record``."""

        if type(record) is not DirectEngineRecord:
            raise TypeError("record must be a DirectEngineRecord")
        record = DirectEngineRecord(
            worker_epoch=record.worker_epoch,
            identity=record.identity,
        )
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
                argv_sha256,
                rpc_port,
                rpc_secret
            FROM direct_engine_recovery_capabilities
            LIMIT 2
            """
        ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise ValueError("persisted direct engine recovery capability is invalid")
        row = rows[0]
        try:
            if _require_sqlite_text(row["engine_kind"], "engine kind") != "direct":
                raise ValueError
            persisted_record = DirectEngineRecord(
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
            capability = _DirectEngineRecoveryCapability(
                rpc_port=_require_direct_engine_recovery_port(
                    _require_sqlite_integer(row["rpc_port"], "rpc_port")
                ),
                rpc_secret=_require_direct_engine_recovery_secret(
                    _require_sqlite_text(row["rpc_secret"], "rpc_secret")
                ),
            )
        except (TypeError, ValueError):
            raise ValueError("persisted direct engine recovery capability is invalid") from None
        if persisted_record != record:
            raise ValueError(
                "persisted direct engine recovery capability does not match direct record"
            )
        return capability

    def _bind_direct_engine_recovery_capability(
        self,
        record: DirectEngineRecord,
        capability: _DirectEngineRecoveryCapability,
    ) -> None:
        """Durably attach one private shutdown capability to an exact record."""

        if type(record) is not DirectEngineRecord:
            raise TypeError("record must be a DirectEngineRecord")
        if type(capability) is not _DirectEngineRecoveryCapability:
            raise TypeError("capability must be a direct engine recovery capability")
        record = DirectEngineRecord(
            worker_epoch=record.worker_epoch,
            identity=record.identity,
        )
        capability = _DirectEngineRecoveryCapability(
            rpc_port=capability.rpc_port,
            rpc_secret=capability.rpc_secret,
        )
        identity = record.identity
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            if self._current_worker_epoch(connection) != record.worker_epoch:
                raise ValueError("direct engine recovery capability worker epoch is not current")
            exact_record = connection.execute(
                """
                SELECT 1
                FROM engine_instances
                WHERE engine_kind = 'direct'
                  AND worker_epoch = ?
                  AND leader_pid = ?
                  AND process_group_id = ?
                  AND session_id = ?
                  AND owner_uid = ?
                  AND started_unix_us = ?
                  AND argv_sha256 = ?
                LIMIT 1
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
            ).fetchone()
            if exact_record is None:
                raise ValueError("direct engine recovery capability record is not present")
            activation_fence = connection.execute(
                """
                SELECT 1
                FROM direct_engine_activation_fences
                WHERE engine_kind = 'direct'
                LIMIT 1
                """
            ).fetchone()
            if activation_fence is not None:
                raise ValueError("direct engine recovery capability conflicts with an activation fence")
            existing_capability = connection.execute(
                """
                SELECT 1
                FROM direct_engine_recovery_capabilities
                WHERE engine_kind = 'direct'
                LIMIT 1
                """
            ).fetchone()
            if existing_capability is not None:
                raise ValueError("direct engine recovery capability is already present")
            connection.execute(
                """
                INSERT INTO direct_engine_recovery_capabilities (
                    engine_kind,
                    worker_epoch,
                    leader_pid,
                    process_group_id,
                    session_id,
                    owner_uid,
                    started_unix_us,
                    argv_sha256,
                    rpc_port,
                    rpc_secret
                )
                VALUES ('direct', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.worker_epoch,
                    identity.leader_pid,
                    identity.process_group_id,
                    identity.session_id,
                    identity.owner_uid,
                    identity.started_unix_us,
                    identity.argv_sha256,
                    capability.rpc_port,
                    capability.rpc_secret,
                ),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def _clear_direct_engine_record_and_recovery_capability(
        self,
        record: DirectEngineRecord,
        capability: _DirectEngineRecoveryCapability,
    ) -> bool:
        """Compare-clear one exact record and its private shutdown authority."""

        if type(record) is not DirectEngineRecord:
            raise TypeError("record must be a DirectEngineRecord")
        if type(capability) is not _DirectEngineRecoveryCapability:
            raise TypeError("capability must be a direct engine recovery capability")
        record = DirectEngineRecord(
            worker_epoch=record.worker_epoch,
            identity=record.identity,
        )
        capability = _DirectEngineRecoveryCapability(
            rpc_port=capability.rpc_port,
            rpc_secret=capability.rpc_secret,
        )
        identity = record.identity
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                """
                DELETE FROM direct_engine_recovery_capabilities
                WHERE engine_kind = 'direct'
                  AND worker_epoch = ?
                  AND leader_pid = ?
                  AND process_group_id = ?
                  AND session_id = ?
                  AND owner_uid = ?
                  AND started_unix_us = ?
                  AND argv_sha256 = ?
                  AND rpc_port = ?
                  AND rpc_secret = ?
                """,
                (
                    record.worker_epoch,
                    identity.leader_pid,
                    identity.process_group_id,
                    identity.session_id,
                    identity.owner_uid,
                    identity.started_unix_us,
                    identity.argv_sha256,
                    capability.rpc_port,
                    capability.rpc_secret,
                ),
            )
            capability_changed = connection.execute("SELECT changes()").fetchone()
            if (
                capability_changed is None
                or type(capability_changed[0]) is not int
                or capability_changed[0] not in {0, 1}
            ):
                raise RuntimeError("direct engine recovery capability delete is invalid")
            if capability_changed[0] == 0:
                connection.commit()
                return False
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
            record_changed = connection.execute("SELECT changes()").fetchone()
            if (
                record_changed is None
                or type(record_changed[0]) is not int
                or record_changed[0] not in {0, 1}
            ):
                raise RuntimeError("direct engine record delete is invalid")
            if record_changed[0] == 0:
                connection.rollback()
                return False
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return True

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
            recovery_capability = connection.execute(
                """
                SELECT 1
                FROM direct_engine_recovery_capabilities
                WHERE engine_kind = 'direct'
                LIMIT 1
                """
            ).fetchone()
            if recovery_capability is not None:
                connection.commit()
                return False
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

    def get_publication_reservation(
        self, job_id: str
    ) -> PublicationReservation | None:
        """Read one exact materialized-job publication receipt, if present."""

        job_id = _require_identifier(job_id, "job_id")
        return self._read_publication_reservation(self._connection, job_id)

    def get_publication_marker_binding(
        self, job_id: str
    ) -> PublicationMarkerBinding | None:
        """Read one exact job-local marker identity, if the receipt owns one."""

        job_id = _require_identifier(job_id, "job_id")
        return self._read_publication_marker_binding(self._connection, job_id)

    def _get_staged_payload_binding(self, job_id: str) -> _StagedPayloadBinding | None:
        """Read one private staged-payload identity through its owner chain."""

        job_id = _require_identifier(job_id, "job_id")
        return self._read_staged_payload_binding(self._connection, job_id)

    def _get_final_publication_binding(
        self, job_id: str
    ) -> _FinalPublicationBinding | None:
        """Read one private final identity through its verified owner chain."""

        job_id = _require_identifier(job_id, "job_id")
        return self._read_final_publication_binding(self._connection, job_id)

    @staticmethod
    def _read_publication_reservation(
        connection: sqlite3.Connection, job_id: str
    ) -> PublicationReservation | None:
        """Read one receipt only from a jobs-to-projection-to-receipt chain."""

        projection_rows = connection.execute(
            """
            SELECT job_id, category, destination_collection, selected_final_filename
            FROM materialized_jobs
            WHERE job_id = ?
            LIMIT 2
            """,
            (job_id,),
        ).fetchall()
        reservation_rows = connection.execute(
            """
            SELECT job_id, target_component, final_filename, claim_token
            FROM publication_reservations
            WHERE job_id = ?
            LIMIT 2
            """,
            (job_id,),
        ).fetchall()
        if not projection_rows and not reservation_rows:
            return None
        job_rows = connection.execute(
            """
            SELECT job_id
            FROM jobs
            WHERE job_id = ?
            LIMIT 2
            """,
            (job_id,),
        ).fetchall()
        if len(job_rows) != 1:
            raise ValueError("publication reservation owner is not a unique job")
        owner_job_id = _require_identifier(
            _require_sqlite_text(job_rows[0]["job_id"], "publication reservation owner job_id"),
            "publication reservation owner job_id",
        )
        if owner_job_id != job_id:
            raise ValueError("publication reservation owner job_id does not match its lookup")
        if len(projection_rows) != 1:
            raise ValueError("publication reservation owner is not a unique materialized job")
        if not reservation_rows:
            return None
        if len(reservation_rows) != 1:
            raise ValueError("materialized job must have exactly one publication reservation")

        projection = projection_rows[0]
        projection_job_id = _require_identifier(
            _require_sqlite_text(
                projection["job_id"], "publication reservation materialized job_id"
            ),
            "publication reservation materialized job_id",
        )
        if projection_job_id != job_id:
            raise ValueError(
                "publication reservation materialized job_id does not match its lookup"
            )
        destination_collection = _require_optional_sqlite_text(
            projection["destination_collection"],
            "publication reservation destination_collection",
        )
        target_component = (
            _require_sqlite_text(
                projection["category"], "publication reservation category"
            )
            if destination_collection is None
            else destination_collection
        )
        final_filename = _require_sqlite_text(
            projection["selected_final_filename"],
            "publication reservation selected_final_filename",
        )

        row = reservation_rows[0]
        persisted_job_id = _require_identifier(
            _require_sqlite_text(row["job_id"], "publication reservation job_id"),
            "publication reservation job_id",
        )
        if persisted_job_id != job_id:
            raise ValueError("publication reservation job_id does not match its lookup")
        try:
            reservation = PublicationReservation(
                job_id=persisted_job_id,
                target_component=_require_sqlite_text(
                    row["target_component"], "publication reservation target_component"
                ),
                final_filename=_require_sqlite_text(
                    row["final_filename"], "publication reservation final_filename"
                ),
                claim_token=_require_sqlite_text(
                    row["claim_token"], "publication reservation claim_token"
                ),
            )
            expected = PublicationReservation(
                job_id=projection_job_id,
                target_component=target_component,
                final_filename=final_filename,
                claim_token=reservation.claim_token,
            )
        except (TypeError, ValueError) as error:
            raise ValueError("persisted publication reservation is invalid") from error
        if (
            reservation.target_component != expected.target_component
            or reservation.final_filename != expected.final_filename
        ):
            raise ValueError(
                "publication reservation does not match its materialized destination"
            )
        return reservation

    @staticmethod
    def _read_publication_marker_binding(
        connection: sqlite3.Connection, job_id: str
    ) -> PublicationMarkerBinding | None:
        """Read a marker identity only through its current reservation owner chain."""

        reservation = SQLiteStore._read_publication_reservation(connection, job_id)
        rows = connection.execute(
            """
            SELECT job_id, marker_device, marker_inode
            FROM publication_marker_bindings
            WHERE job_id = ?
            LIMIT 2
            """,
            (job_id,),
        ).fetchall()
        if reservation is None:
            if rows:
                raise ValueError(
                    "publication marker binding is missing its publication reservation"
                )
            return None
        if not rows:
            return None
        if len(rows) != 1:
            raise ValueError("publication reservation has multiple marker bindings")
        row = rows[0]
        try:
            binding = PublicationMarkerBinding(
                job_id=_require_sqlite_text(
                    row["job_id"], "publication marker binding job_id"
                ),
                marker_device=_require_counter(
                    _require_sqlite_integer(
                        row["marker_device"], "publication marker binding marker_device"
                    ),
                    "publication marker binding marker_device",
                ),
                marker_inode=_require_counter(
                    _require_sqlite_integer(
                        row["marker_inode"], "publication marker binding marker_inode"
                    ),
                    "publication marker binding marker_inode",
                ),
            )
        except (TypeError, ValueError) as error:
            raise ValueError("persisted publication marker binding is invalid") from error
        if binding.job_id != job_id or binding.job_id != reservation.job_id:
            raise ValueError("publication marker binding job_id does not match its owner")
        return binding

    @staticmethod
    def _read_staged_payload_binding(
        connection: sqlite3.Connection, job_id: str
    ) -> _StagedPayloadBinding | None:
        """Read staged identity only through complete current owner chains."""

        requested: _StagedPayloadBinding | None = None
        rows = connection.execute(
            """
            SELECT job_id, partial_device, partial_inode, logical_size
            FROM staged_payload_bindings
            """
        ).fetchall()
        for row in rows:
            try:
                binding = _StagedPayloadBinding(
                    job_id=_require_sqlite_text(
                        row["job_id"], "staged payload binding job_id"
                    ),
                    partial_device=_require_counter(
                        _require_sqlite_integer(
                            row["partial_device"],
                            "staged payload binding partial_device",
                        ),
                        "staged payload binding partial_device",
                    ),
                    partial_inode=_require_counter(
                        _require_sqlite_integer(
                            row["partial_inode"],
                            "staged payload binding partial_inode",
                        ),
                        "staged payload binding partial_inode",
                    ),
                    logical_size=_require_counter(
                        _require_sqlite_integer(
                            row["logical_size"],
                            "staged payload binding logical_size",
                        ),
                        "staged payload binding logical_size",
                    ),
                )
            except (TypeError, ValueError) as error:
                raise ValueError("persisted staged payload binding is invalid") from error
            marker = SQLiteStore._read_publication_marker_binding(
                connection, binding.job_id
            )
            if marker is None:
                raise ValueError("staged payload binding is missing its publication marker")
            if marker.job_id != binding.job_id:
                raise ValueError("staged payload binding job_id does not match its owner")
            if binding.job_id == job_id:
                if requested is not None:
                    raise ValueError("materialized job has multiple staged payload bindings")
                requested = binding
        return requested

    @staticmethod
    def _read_final_publication_binding(
        connection: sqlite3.Connection, job_id: str
    ) -> _FinalPublicationBinding | None:
        """Read final identity only when it exactly matches a staged owner chain."""

        requested: _FinalPublicationBinding | None = None
        rows = connection.execute(
            """
            SELECT job_id, final_device, final_inode, logical_size
            FROM final_publication_bindings
            """
        ).fetchall()
        for row in rows:
            try:
                binding = _FinalPublicationBinding(
                    job_id=_require_sqlite_text(
                        row["job_id"], "final publication binding job_id"
                    ),
                    final_device=_require_counter(
                        _require_sqlite_integer(
                            row["final_device"],
                            "final publication binding final_device",
                        ),
                        "final publication binding final_device",
                    ),
                    final_inode=_require_counter(
                        _require_sqlite_integer(
                            row["final_inode"],
                            "final publication binding final_inode",
                        ),
                        "final publication binding final_inode",
                    ),
                    logical_size=_require_counter(
                        _require_sqlite_integer(
                            row["logical_size"],
                            "final publication binding logical_size",
                        ),
                        "final publication binding logical_size",
                    ),
                )
            except (TypeError, ValueError) as error:
                raise ValueError("persisted final publication binding is invalid") from error
            staged = SQLiteStore._read_staged_payload_binding(connection, binding.job_id)
            if staged is None:
                raise ValueError("final publication binding is missing its staged payload")
            if staged.job_id != binding.job_id:
                raise ValueError("final publication binding job_id does not match its owner")
            if (
                binding.final_device != staged.partial_device
                or binding.final_inode != staged.partial_inode
                or binding.logical_size != staged.logical_size
            ):
                raise ValueError("final publication binding does not match staged payload")
            if binding.job_id == job_id:
                if requested is not None:
                    raise ValueError("materialized job has multiple final publication bindings")
                requested = binding
        return requested

    def bind_publication_marker(
        self,
        job_id: str,
        *,
        claim_token: str,
        marker_device: object,
        marker_inode: object,
    ) -> PublicationMarkerBinding:
        """Persist or exactly replay the marker identity bound to one reservation."""

        job_id = _require_identifier(job_id, "job_id")
        claim_token = _require_reservation_token(claim_token)
        marker_device = _require_counter(marker_device, "marker_device")
        marker_inode = _require_counter(marker_inode, "marker_inode")
        requested = PublicationMarkerBinding(
            job_id=job_id,
            marker_device=marker_device,
            marker_inode=marker_inode,
        )
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            reservation = self._read_publication_reservation(connection, job_id)
            if reservation is None:
                raise ValueError(
                    "publication marker binding requires a current publication reservation"
                )
            if reservation.claim_token != claim_token:
                raise ValueError("publication marker claim token does not match")
            existing = self._read_publication_marker_binding(connection, job_id)
            self._read_source_kind(connection, job_id)
            if existing is None:
                self._require_mutable_source(connection, job_id)
                connection.execute(
                    """
                    INSERT INTO publication_marker_bindings (
                        job_id, marker_device, marker_inode
                    )
                    VALUES (?, ?, ?)
                    """,
                    (
                        requested.job_id,
                        requested.marker_device,
                        requested.marker_inode,
                    ),
                )
                result = requested
            else:
                if existing != requested:
                    raise ValueError("publication marker binding does not match")
                result = existing
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return result

    def _bind_staged_payload(
        self,
        job_id: str,
        *,
        claim_token: str,
        partial_device: object,
        partial_inode: object,
        logical_size: object,
    ) -> _StagedPayloadBinding:
        """Persist or exactly replay staged inode identity under its receipt chain."""

        job_id = _require_identifier(job_id, "job_id")
        claim_token = _require_reservation_token(claim_token)
        requested = _StagedPayloadBinding(
            job_id=job_id,
            partial_device=_require_counter(partial_device, "partial_device"),
            partial_inode=_require_counter(partial_inode, "partial_inode"),
            logical_size=_require_counter(logical_size, "logical_size"),
        )
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            result = self._bind_staged_payload_in_transaction(
                connection, requested=requested, claim_token=claim_token
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return result

    def _bind_staged_payload_in_transaction(
        self, connection: sqlite3.Connection, *, requested: _StagedPayloadBinding,
        claim_token: str,
    ) -> _StagedPayloadBinding:
        reservation = self._read_publication_reservation(connection, requested.job_id)
        if reservation is None:
            raise ValueError(
                "staged payload binding requires a current publication reservation"
            )
        if reservation.claim_token != claim_token:
            raise ValueError("staged payload claim token does not match")
        marker = self._read_publication_marker_binding(connection, requested.job_id)
        if marker is None:
            raise ValueError(
                "staged payload binding requires a current publication marker"
            )
        if marker.job_id != reservation.job_id:
            raise ValueError("publication marker binding does not match its owner")
        existing = self._read_staged_payload_binding(connection, requested.job_id)
        self._read_source_kind(connection, requested.job_id)
        if existing is None:
            self._require_mutable_source(connection, requested.job_id)
            connection.execute(
                """
                INSERT INTO staged_payload_bindings (
                    job_id, partial_device, partial_inode, logical_size
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    requested.job_id,
                    requested.partial_device,
                    requested.partial_inode,
                    requested.logical_size,
                ),
            )
            result = requested
        else:
            if existing != requested:
                raise ValueError("staged payload binding does not match")
            result = existing
        return result

    def _bind_final_publication(
        self,
        job_id: str,
        *,
        claim_token: str,
        final_device: object,
        final_inode: object,
        logical_size: object,
    ) -> _FinalPublicationBinding:
        """Persist or exactly replay a verified same-inode final publication."""

        job_id = _require_identifier(job_id, "job_id")
        requested = _FinalPublicationBinding(
            job_id=job_id,
            final_device=_require_counter(final_device, "final_device"),
            final_inode=_require_counter(final_inode, "final_inode"),
            logical_size=_require_counter(logical_size, "logical_size"),
        )
        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            result = self._bind_final_publication_in_transaction(
                connection,
                requested=requested,
                claim_token=claim_token,
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return result

    def _bind_final_publication_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        requested: _FinalPublicationBinding,
        claim_token: str,
    ) -> _FinalPublicationBinding:
        """Bind final identity using an already-open atomic lifecycle transaction."""

        if type(requested) is not _FinalPublicationBinding:
            raise TypeError("requested must be a final publication binding")
        claim_token = _require_reservation_token(claim_token)
        reservation = self._read_publication_reservation(connection, requested.job_id)
        if reservation is None:
            raise ValueError("final publication binding requires a current publication reservation")
        if reservation.claim_token != claim_token:
            raise ValueError("final publication claim token does not match")
        marker = self._read_publication_marker_binding(connection, requested.job_id)
        if marker is None:
            raise ValueError("final publication binding requires a current publication marker")
        if marker.job_id != reservation.job_id:
            raise ValueError("publication marker binding does not match its owner")
        staged = self._read_staged_payload_binding(connection, requested.job_id)
        if staged is None:
            raise ValueError("final publication binding requires a current staged payload")
        if staged.job_id != marker.job_id:
            raise ValueError("staged payload binding does not match its owner")
        if (
            requested.final_device != staged.partial_device
            or requested.final_inode != staged.partial_inode
            or requested.logical_size != staged.logical_size
        ):
            raise ValueError("final publication binding does not match staged payload")
        existing = self._read_final_publication_binding(connection, requested.job_id)
        self._read_source_kind(connection, requested.job_id)
        if existing is None:
            self._require_mutable_source(connection, requested.job_id)
            connection.execute(
                """
                INSERT INTO final_publication_bindings (
                    job_id, final_device, final_inode, logical_size
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    requested.job_id,
                    requested.final_device,
                    requested.final_inode,
                    requested.logical_size,
                ),
            )
            return requested
        if existing != requested:
            raise ValueError("final publication binding does not match")
        return existing

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
