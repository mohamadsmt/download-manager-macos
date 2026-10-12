"""SQLite-backed transactional persistence for queue commands."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
import re
import hashlib
import json
import stat
import secrets
import sqlite3
import time
import math
import os
import uuid
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
    JobControlCommand, JobAddCommand, TargetAuthorizeCommand, TargetAuthorizeResult, _batch_entry, _batch_canonical, _batch_decode, _batch_collection_id,
    _batch_component, _MAX_BATCH_REPLY, QueryCursor, QueryListPage, QueryStatus,
    QueryEventsPage, QueryEventRecord, PublicJobRecord, validate_query_cursor, _query_counter,
    NotificationPendingCommand, NotificationClaimCommand, NotificationPresentedCommand,
    NotificationReleaseCommand, NotificationResult, validate_notification_event, _notification_uuid)
from hermes_downloads.network import validate_source_url, SourcePolicyError
from hermes_downloads.retry import (
    CompletionVerification,
    RetryAuditEvent,
    RetryAuditKind,
    RetryAuthority,
    RetryBudget,
    RetryPolicy, FailureKind, RetryFailure, RetryAction, failure_from_http_status,
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
_SUPPORTED_SCHEMA_VERSION: Final = 24
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
_V18_COMMAND_RECEIPTS_SCHEMA: Final = _V17_COMMAND_RECEIPTS_SCHEMA.replace(
    "'add_batch_entry'))", "'add_batch_entry', 'target_authorize'))").replace(
    "OR (scope = 'add_batch_entry' AND action = 'add_batch_entry')",
    "OR (scope = 'add_batch_entry' AND action = 'add_batch_entry')\n        OR (scope = 'target_authorize' AND action = 'target_authorize')")
_TARGET_COMMANDS_SCHEMA: Final = """
CREATE TABLE target_commands (
    request_id TEXT PRIMARY KEY NOT NULL REFERENCES command_receipts(request_id),
    payload_digest TEXT NOT NULL CHECK (length(payload_digest)=64 AND payload_digest NOT GLOB '*[^0-9a-f]*'),
    action TEXT NOT NULL CHECK (action IN ('start','resume','start_now')),
    worker_epoch INTEGER NOT NULL CHECK (worker_epoch>0),
    captured_at_us INTEGER NOT NULL CHECK (captured_at_us>=0),
    member_count INTEGER NOT NULL CHECK (member_count BETWEEN 1 AND 500),
    command_blob BLOB NOT NULL CHECK (length(command_blob) BETWEEN 1 AND 131072),
    receipt_blob BLOB NOT NULL CHECK (length(receipt_blob) BETWEEN 1 AND 262144),
    vector_digest TEXT NOT NULL CHECK (length(vector_digest)=64 AND vector_digest NOT GLOB '*[^0-9a-f]*')
) STRICT;
"""
_TARGET_MEMBERS_SCHEMA: Final = """
CREATE TABLE target_members (
    request_id TEXT NOT NULL REFERENCES target_commands(request_id) DEFERRABLE INITIALLY DEFERRED,
    target_index INTEGER NOT NULL CHECK (target_index BETWEEN 0 AND 499),
    job_id TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('new_authority','existing_authority','blocked','stale')),
    reason TEXT CHECK (reason IN ('unknown_job','legacy_kind','terminal','busy','incompatible_authority','stale_revision')),
    creation_kind TEXT CHECK (creation_kind IN ('single','batch')),
    creation_request_id TEXT,
    creation_payload_digest TEXT,
    creation_index INTEGER CHECK (creation_index BETWEEN 0 AND 499),
    creation_child_request_id TEXT,
    creation_child_payload_digest TEXT,
    creation_audit_id INTEGER REFERENCES events(event_id),
    creation_generation INTEGER CHECK (creation_generation>=0),
    creation_revision INTEGER CHECK (creation_revision>=0),
    creation_seal TEXT,
    reservation_claim_token TEXT,
    immutable_identity_digest TEXT,
    captured_audit_id INTEGER REFERENCES events(event_id),
    captured_generation INTEGER CHECK (captured_generation>=0),
    captured_revision INTEGER CHECK (captured_revision>=0),
    captured_state TEXT,
    captured_authorized INTEGER CHECK (captured_authorized IN (0,1)),
    captured_manual_hold INTEGER CHECK (captured_manual_hold IN (0,1)),
    captured_start_now_requested INTEGER CHECK (captured_start_now_requested IN (0,1)),
    round_generation INTEGER CHECK (round_generation>0),
    owner_request_id TEXT,
    owner_target_index INTEGER CHECK (owner_target_index BETWEEN 0 AND 499),
    intent_audit_id INTEGER UNIQUE REFERENCES events(event_id),
    intent_generation INTEGER CHECK (intent_generation>=0),
    intent_revision INTEGER CHECK (intent_revision>=0),
    intent_state TEXT,
    intent_authorized INTEGER CHECK (intent_authorized IN (0,1)),
    intent_manual_hold INTEGER CHECK (intent_manual_hold IN (0,1)),
    intent_start_now_requested INTEGER CHECK (intent_start_now_requested IN (0,1)),
    preserved_publication_attempt_id TEXT,
    held_mask INTEGER CHECK (held_mask BETWEEN 0 AND 15),
    PRIMARY KEY (request_id,target_index),
    UNIQUE (request_id,job_id),
    FOREIGN KEY (creation_kind,creation_request_id,round_generation)
        REFERENCES authorization_rounds(cohort_kind,cohort_request_id,round_generation) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (owner_request_id,owner_target_index)
        REFERENCES target_members(request_id,target_index) DEFERRABLE INITIALLY DEFERRED,
    CHECK ((outcome IN ('new_authority','existing_authority') AND reason IS NULL AND round_generation IS NOT NULL
            AND owner_request_id IS NOT NULL AND owner_target_index IS NOT NULL AND creation_kind IS NOT NULL
            AND captured_audit_id IS NOT NULL AND held_mask IS NOT NULL)
        OR (outcome IN ('blocked','stale') AND reason IS NOT NULL AND round_generation IS NULL
            AND owner_request_id IS NULL AND owner_target_index IS NULL AND intent_audit_id IS NULL)),
    CHECK ((intent_audit_id IS NULL AND intent_generation IS NULL AND intent_revision IS NULL AND intent_state IS NULL
            AND intent_authorized IS NULL AND intent_manual_hold IS NULL AND intent_start_now_requested IS NULL)
        OR (intent_audit_id IS NOT NULL AND intent_generation IS NOT NULL AND intent_revision IS NOT NULL AND intent_state IS NOT NULL
            AND intent_authorized IS NOT NULL AND intent_manual_hold IS NOT NULL AND intent_start_now_requested IS NOT NULL))
) STRICT;
"""
_AUTHORIZATION_ROUNDS_SCHEMA: Final = """
CREATE TABLE authorization_rounds (
    cohort_kind TEXT NOT NULL CHECK (cohort_kind IN ('single','batch')),
    cohort_request_id TEXT NOT NULL,
    round_generation INTEGER NOT NULL CHECK (round_generation>0),
    origin_request_id TEXT NOT NULL REFERENCES target_commands(request_id) DEFERRABLE INITIALLY DEFERRED,
    member_count INTEGER NOT NULL CHECK (member_count BETWEEN 1 AND 500),
    vector_digest TEXT NOT NULL CHECK (length(vector_digest)=64 AND vector_digest NOT GLOB '*[^0-9a-f]*'),
    status TEXT NOT NULL CHECK (status IN ('open','terminal')),
    PRIMARY KEY (cohort_kind,cohort_request_id,round_generation)
) STRICT;
"""
_JOB_AUTHORIZATION_HEADS_SCHEMA: Final = """
CREATE TABLE job_authorization_heads (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES jobs(job_id),
    owner_request_id TEXT NOT NULL,
    owner_target_index INTEGER NOT NULL CHECK (owner_target_index BETWEEN 0 AND 499),
    activation_request_id TEXT NOT NULL,
    cohort_kind TEXT NOT NULL CHECK (cohort_kind IN ('single','batch')),
    cohort_request_id TEXT NOT NULL,
    round_generation INTEGER NOT NULL CHECK (round_generation>0),
    current_audit_id INTEGER NOT NULL REFERENCES events(event_id),
    current_generation INTEGER NOT NULL CHECK (current_generation>=0),
    current_revision INTEGER NOT NULL CHECK (current_revision>=0),
    current_state TEXT NOT NULL,
    current_worker_epoch INTEGER NOT NULL CHECK (current_worker_epoch>0),
    intent_status TEXT NOT NULL CHECK (intent_status IN ('current','inactive','terminal')),
    last_admission_serial INTEGER NOT NULL CHECK (last_admission_serial>=0),
    FOREIGN KEY (owner_request_id,owner_target_index) REFERENCES target_members(request_id,target_index) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (activation_request_id,job_id) REFERENCES target_members(request_id,job_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (cohort_kind,cohort_request_id,round_generation)
        REFERENCES authorization_rounds(cohort_kind,cohort_request_id,round_generation) DEFERRABLE INITIALLY DEFERRED
) STRICT;
"""
_V18_TABLE_SCHEMAS: Final = dict(_V17_TABLE_SCHEMAS,
    **_expected_table_schemas(_V18_COMMAND_RECEIPTS_SCHEMA, _TARGET_COMMANDS_SCHEMA,
        _TARGET_MEMBERS_SCHEMA, _AUTHORIZATION_ROUNDS_SCHEMA, _JOB_AUTHORIZATION_HEADS_SCHEMA))
# SQLite preserves the final name quoted after the FK-safe temporary-table rename.
_V18_TABLE_SCHEMAS['command_receipts'] = _V18_TABLE_SCHEMAS['command_receipts'].replace(
    'CREATE TABLE COMMAND_RECEIPTS', 'CREATE TABLE "COMMAND_RECEIPTS"', 1)

_DIRECT_CLEANUP_CLAIMS_SCHEMA: Final = """
CREATE TABLE direct_cleanup_claims (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES jobs(job_id)
        CHECK(typeof(job_id)='text' AND length(job_id) BETWEEN 1 AND 128
              AND substr(job_id,1,1) GLOB '[A-Za-z0-9]'
              AND job_id NOT GLOB '*[^A-Za-z0-9._:-]*'),
    attempt_id TEXT NOT NULL UNIQUE REFERENCES direct_publication_attempts(attempt_id)
        CHECK(typeof(attempt_id)='text' AND length(attempt_id) BETWEEN 1 AND 128
              AND substr(attempt_id,1,1) GLOB '[A-Za-z0-9]'
              AND attempt_id NOT GLOB '*[^A-Za-z0-9._:-]*'),
    completion_audit_id INTEGER NOT NULL REFERENCES events(event_id)
        CHECK(typeof(completion_audit_id)='integer' AND completion_audit_id BETWEEN 1 AND 9223372036854775807),
    completion_generation INTEGER NOT NULL
        CHECK(typeof(completion_generation)='integer' AND completion_generation BETWEEN 0 AND 9223372036854775807),
    completion_revision INTEGER NOT NULL
        CHECK(typeof(completion_revision)='integer' AND completion_revision BETWEEN 0 AND 9223372036854775807),
    completion_epoch INTEGER NOT NULL
        CHECK(typeof(completion_epoch)='integer' AND completion_epoch BETWEEN 1 AND 9223372036854775807),
    proof_sha256 TEXT NOT NULL
        CHECK(typeof(proof_sha256)='text' AND length(proof_sha256)=64 AND proof_sha256 NOT GLOB '*[^0-9a-f]*'),
    published_stat TEXT NOT NULL
        CHECK(typeof(published_stat)='text' AND length(published_stat) BETWEEN 15 AND 141),
    phase TEXT NOT NULL CHECK(typeof(phase)='text' AND phase IN ('pending','certified','finished')),
    activation_epoch INTEGER
        CHECK(activation_epoch IS NULL OR (typeof(activation_epoch)='integer' AND activation_epoch BETWEEN 1 AND 9223372036854775807)),
    activation_request_id TEXT REFERENCES direct_dispatch_commands(request_id)
        CHECK(activation_request_id IS NULL OR (typeof(activation_request_id)='text'
              AND length(activation_request_id) BETWEEN 1 AND 128
              AND substr(activation_request_id,1,1) GLOB '[A-Za-z0-9]'
              AND activation_request_id NOT GLOB '*[^A-Za-z0-9._:-]*')),
    certified_stat TEXT
        CHECK(certified_stat IS NULL OR (typeof(certified_stat)='text' AND length(certified_stat) BETWEEN 15 AND 141)),
    CHECK((phase='pending' AND certified_stat IS NULL) OR
          (phase IN ('certified','finished') AND certified_stat IS NOT NULL)),
    CHECK(activation_request_id IS NULL OR activation_epoch IS NOT NULL)
) STRICT;
"""
_V19_TABLE_SCHEMAS: Final = dict(_V18_TABLE_SCHEMAS,
    **_expected_table_schemas(_DIRECT_CLEANUP_CLAIMS_SCHEMA))

_TARGET_DISPATCH_MARKER: Final = """producer_kind TEXT NOT NULL DEFAULT 'legacy'
    CHECK (typeof(producer_kind)='text' AND producer_kind IN ('legacy','target_body','target_publication'))"""
_TARGET_DISPATCH_CAUSES_SCHEMA: Final = """
CREATE TABLE target_dispatch_causes (
    request_id TEXT PRIMARY KEY NOT NULL REFERENCES direct_dispatch_commands(request_id),
    kind TEXT NOT NULL CHECK (kind IN ('body','publication')),
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    owner_request_id TEXT NOT NULL,
    owner_target_index INTEGER NOT NULL CHECK (owner_target_index BETWEEN 0 AND 499),
    activation_request_id TEXT NOT NULL,
    admission_serial INTEGER CHECK (admission_serial BETWEEN 1 AND 9223372036854775807),
    predecessor_audit_id INTEGER NOT NULL REFERENCES events(event_id)
        CHECK (predecessor_audit_id BETWEEN 1 AND 9223372036854775807),
    resolving_audit_id INTEGER UNIQUE REFERENCES events(event_id)
        CHECK (resolving_audit_id BETWEEN 1 AND 9223372036854775807),
    attempt_id TEXT CHECK (attempt_id IS NULL OR (
        length(attempt_id) BETWEEN 1 AND 128
        AND substr(attempt_id,1,1) GLOB '[A-Za-z0-9]'
        AND attempt_id NOT GLOB '*[^A-Za-z0-9._:-]*')),
    original_body_request_id TEXT REFERENCES direct_dispatch_commands(request_id),
    FOREIGN KEY (owner_request_id,owner_target_index)
        REFERENCES target_members(request_id,target_index) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (activation_request_id,job_id)
        REFERENCES target_members(request_id,job_id) DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (owner_request_id,owner_target_index,admission_serial),
    UNIQUE (kind,activation_request_id,job_id,predecessor_audit_id,attempt_id),
    CHECK ((kind='body' AND admission_serial IS NOT NULL AND resolving_audit_id IS NOT NULL
            AND attempt_id IS NULL AND original_body_request_id IS NULL)
        OR (kind='publication' AND admission_serial IS NULL AND resolving_audit_id IS NULL
            AND attempt_id IS NOT NULL AND original_body_request_id IS NOT NULL))
) STRICT;
"""
_V20_TABLE_SCHEMAS: Final = dict(_V19_TABLE_SCHEMAS,
    **_expected_table_schemas(_TARGET_DISPATCH_CAUSES_SCHEMA))
# ALTER appends inside the original table's final parenthesis, preserving its spelling.
_V20_TABLE_SCHEMAS['direct_dispatch_commands'] = (
    _V19_TABLE_SCHEMAS['direct_dispatch_commands'][:-1] + ', '
    + _normalize_table_schema(_TARGET_DISPATCH_MARKER) + ')')

_NOTIFICATION_OUTBOX_SCHEMA: Final = """
CREATE TABLE notification_outbox (
    sequence INTEGER PRIMARY KEY CHECK(sequence BETWEEN 1 AND 9223372036854775807),
    store_uuid TEXT NOT NULL CHECK(length(store_uuid)=36),
    semantic_key TEXT NOT NULL UNIQUE CHECK(length(semantic_key) BETWEEN 1 AND 384),
    type TEXT NOT NULL CHECK(type IN ('completion','needs_link','needs_login','needs_space','persistent_error')),
    job_id TEXT REFERENCES jobs(job_id),
    collection_id TEXT,
    generation INTEGER NOT NULL CHECK(generation BETWEEN 0 AND 9223372036854775807),
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms BETWEEN 0 AND 9223372036854775807),
    count INTEGER NOT NULL CHECK(count BETWEEN 1 AND 500),
    reason_code TEXT CHECK(reason_code IN ('link_expired','login_required','insufficient_space','retry_exhausted','download_failed')),
    provenance TEXT NOT NULL CHECK(length(provenance) BETWEEN 1 AND 524288),
    presentation_state TEXT NOT NULL DEFAULT 'pending' CHECK(presentation_state IN ('pending','claimed','presented')),
    consumer_id TEXT,
    claim_token TEXT,
    lease_expires_at_ms INTEGER CHECK(lease_expires_at_ms BETWEEN 0 AND 9223372036854775807),
    deliverable INTEGER GENERATED ALWAYS AS (presentation_state!='presented') STORED,
    delivery_priority INTEGER GENERATED ALWAYS AS (type='completion') STORED,
    UNIQUE(deliverable,delivery_priority,sequence),
    CHECK((consumer_id IS NULL AND claim_token IS NULL AND lease_expires_at_ms IS NULL AND presentation_state='pending')
        OR (consumer_id IS NOT NULL AND length(consumer_id)=36 AND claim_token IS NOT NULL
            AND length(claim_token)=64 AND lease_expires_at_ms IS NOT NULL AND presentation_state IN ('claimed','presented')))
) STRICT;
"""
_NOTIFICATION_CLAIMS_SCHEMA: Final = """
CREATE TABLE notification_claims (
    sequence INTEGER NOT NULL REFERENCES notification_outbox(sequence),
    claim_token TEXT NOT NULL CHECK(length(claim_token)=64 AND claim_token NOT GLOB '*[^0-9a-f]*'),
    consumer_id TEXT NOT NULL CHECK(length(consumer_id)=36),
    lease_expires_at_ms INTEGER NOT NULL CHECK(lease_expires_at_ms BETWEEN 0 AND 9223372036854775807),
    outcome TEXT NOT NULL CHECK(outcome IN ('claimed','released','presented')),
    notification_id TEXT CHECK(length(notification_id) BETWEEN 1 AND 256),
    CHECK((outcome='presented' AND notification_id IS NOT NULL) OR (outcome!='presented' AND notification_id IS NULL)),
    PRIMARY KEY(sequence,claim_token)
) STRICT;
"""
_V21_TABLE_SCHEMAS: Final = dict(_V20_TABLE_SCHEMAS,
    **_expected_table_schemas(_NOTIFICATION_OUTBOX_SCHEMA, _NOTIFICATION_CLAIMS_SCHEMA))

_STOPPED_BODIES_SCHEMA = """
CREATE TABLE stopped_direct_bodies (
 job_id TEXT PRIMARY KEY NOT NULL REFERENCES jobs(job_id),
 source_request_id TEXT NOT NULL UNIQUE REFERENCES direct_dispatch_commands(request_id),
 pause_request_id TEXT NOT NULL REFERENCES job_control_commands(request_id),
 pause_audit_id INTEGER NOT NULL REFERENCES events(event_id),
 worker_epoch INTEGER NOT NULL CHECK(worker_epoch>=1),
 generation INTEGER NOT NULL CHECK(generation>=0),
 revision INTEGER NOT NULL CHECK(revision>=0),
 checkpoint_digest TEXT NOT NULL CHECK(length(checkpoint_digest)=64),
 facts TEXT NOT NULL,
 closure TEXT NOT NULL
) STRICT;
"""
_SAVED_CONTINUATIONS_SCHEMA = """
CREATE TABLE saved_direct_continuations (
 job_id TEXT PRIMARY KEY NOT NULL REFERENCES stopped_direct_bodies(job_id),
 request_id TEXT NOT NULL UNIQUE REFERENCES target_commands(request_id),
 payload_digest TEXT NOT NULL CHECK(length(payload_digest)=64),
 checkpoint_digest TEXT NOT NULL CHECK(length(checkpoint_digest)=64),
 worker_epoch INTEGER NOT NULL CHECK(worker_epoch>=1),
 generation INTEGER NOT NULL CHECK(generation>=0),
 revision INTEGER NOT NULL CHECK(revision>=0),
 phase TEXT NOT NULL CHECK(phase IN ('intent','linked','retired','ready','admitted','failed')),
 facts TEXT NOT NULL,
 body_request_id TEXT UNIQUE REFERENCES direct_dispatch_commands(request_id)
) STRICT;
"""
_V22_TABLE_SCHEMAS = dict(_V21_TABLE_SCHEMAS,
 **_expected_table_schemas(_STOPPED_BODIES_SCHEMA, _SAVED_CONTINUATIONS_SCHEMA))

_STOPPED_CYCLES_SCHEMA = _STOPPED_BODIES_SCHEMA.replace('stopped_direct_bodies','stopped_direct_cycles').replace(
 'job_id TEXT PRIMARY KEY NOT NULL','job_id TEXT NOT NULL').replace(
 'source_request_id TEXT NOT NULL UNIQUE','source_request_id TEXT PRIMARY KEY NOT NULL').replace(
 'pause_request_id TEXT NOT NULL REFERENCES job_control_commands(request_id)',
 'pause_request_id TEXT NOT NULL').replace(' closure TEXT NOT NULL',
 " closure TEXT NOT NULL,\n stop_kind TEXT NOT NULL CHECK(stop_kind IN ('pause','queue','shutdown','cold'))")
_SAVED_CYCLES_SCHEMA = _SAVED_CONTINUATIONS_SCHEMA.replace('saved_direct_continuations','saved_direct_cycles').replace(
 'job_id TEXT PRIMARY KEY NOT NULL REFERENCES stopped_direct_bodies(job_id)',
 'job_id TEXT NOT NULL REFERENCES jobs(job_id)').replace('request_id TEXT NOT NULL UNIQUE',
 'request_id TEXT PRIMARY KEY NOT NULL',1)
_SAVED_BINDINGS_SCHEMA = '''CREATE TABLE saved_resume_bindings (
 request_id TEXT PRIMARY KEY NOT NULL REFERENCES target_commands(request_id),
 source_request_id TEXT NOT NULL REFERENCES direct_dispatch_commands(request_id),
 retained_names TEXT NOT NULL,
 drained INTEGER NOT NULL CHECK(drained IN (0,1)),
 prepared_from TEXT REFERENCES target_commands(request_id)
) STRICT;'''
_SAVED_BODY_OWNERS_SCHEMA = '''CREATE TABLE saved_body_owners (
 request_id TEXT PRIMARY KEY NOT NULL REFERENCES direct_dispatch_commands(request_id),
 process TEXT NOT NULL, gid TEXT NOT NULL, worker_epoch INTEGER NOT NULL
) STRICT;'''
_SAVED_CLEANUP_REQUESTS_SCHEMA = '''CREATE TABLE saved_cleanup_requests (
 request_id TEXT PRIMARY KEY NOT NULL REFERENCES target_commands(request_id),
 job_id TEXT NOT NULL REFERENCES jobs(job_id), worker_epoch INTEGER NOT NULL,
 armed INTEGER NOT NULL CHECK(armed IN (0,1))
) STRICT;'''
_COLD_FENCES_SCHEMA = '''CREATE TABLE saved_cold_fences (
 audit_id INTEGER PRIMARY KEY NOT NULL REFERENCES events(event_id),
 job_id TEXT NOT NULL REFERENCES jobs(job_id),
 old_epoch INTEGER NOT NULL, new_epoch INTEGER NOT NULL,
 old_generation INTEGER NOT NULL, old_revision INTEGER NOT NULL, old_state TEXT NOT NULL,
 generation INTEGER NOT NULL, revision INTEGER NOT NULL,
 UNIQUE(job_id,new_epoch)
) STRICT;'''
_RETAINED_CLEANUP_SCHEMA = '''CREATE TABLE saved_retained_cleanup (
 request_id TEXT NOT NULL REFERENCES target_commands(request_id),
 role INTEGER NOT NULL CHECK(role IN (0,1)),
 attempt_id TEXT NOT NULL REFERENCES direct_publication_attempts(attempt_id),
 basename TEXT NOT NULL, facts TEXT NOT NULL,
 phase TEXT NOT NULL CHECK(phase IN ('authorized','absent')),
 PRIMARY KEY(request_id,role)
) STRICT;'''
_V23_DDLS = (_STOPPED_CYCLES_SCHEMA,_SAVED_CYCLES_SCHEMA,_SAVED_BINDINGS_SCHEMA,
 _COLD_FENCES_SCHEMA,_RETAINED_CLEANUP_SCHEMA,_SAVED_BODY_OWNERS_SCHEMA,_SAVED_CLEANUP_REQUESTS_SCHEMA)
_V23_TABLE_SCHEMAS = dict(_V22_TABLE_SCHEMAS,**_expected_table_schemas(*_V23_DDLS))

_DIRECT_FAILURES_SCHEMA = """CREATE TABLE direct_failures (
 body_request_id TEXT PRIMARY KEY REFERENCES direct_dispatch_commands(request_id),
 job_id TEXT NOT NULL REFERENCES jobs(job_id), worker_epoch INTEGER NOT NULL,
 generation INTEGER NOT NULL, revision INTEGER NOT NULL, audit_id INTEGER NOT NULL UNIQUE REFERENCES events(event_id),
 http_status INTEGER, error_code INTEGER NOT NULL CHECK(error_code BETWEEN 1 AND 32),
 observation_known INTEGER NOT NULL CHECK(observation_known IN (0,1)),
 action TEXT NOT NULL, state TEXT NOT NULL, observed_at_ms INTEGER NOT NULL,
 retry_due_ms INTEGER, observation TEXT NOT NULL, closure TEXT NOT NULL, budget TEXT NOT NULL, seal TEXT NOT NULL
) STRICT;"""
_DIRECT_RETRY_FRESH_SCHEMA = """CREATE TABLE direct_retry_fresh (
 request_id TEXT PRIMARY KEY, source_request_id TEXT NOT NULL REFERENCES direct_failures(body_request_id),
 job_id TEXT NOT NULL REFERENCES jobs(job_id), worker_epoch INTEGER NOT NULL,
 generation INTEGER NOT NULL, revision INTEGER NOT NULL, authorized_at_ms INTEGER NOT NULL,
 chain TEXT NOT NULL, marker TEXT NOT NULL, body_request_id TEXT UNIQUE REFERENCES direct_dispatch_commands(request_id),
 payload_digest TEXT NOT NULL, seal TEXT NOT NULL
) STRICT;"""
_DIRECT_STOCK_BINDINGS_SCHEMA = """CREATE TABLE direct_stock_bindings (
 body_request_id TEXT PRIMARY KEY REFERENCES direct_dispatch_commands(request_id),
 owner TEXT NOT NULL
) STRICT;"""
_STOPPED_FAILED_SCHEMA = _STOPPED_CYCLES_SCHEMA.replace('stopped_direct_cycles','stopped_failed_bodies').replace(
 "stop_kind IN ('pause','queue','shutdown','cold')","stop_kind='failure'")
_STOPPED_CONTROL_SCHEMA = _STOPPED_CYCLES_SCHEMA.replace('stopped_direct_cycles','stopped_control_bodies').replace(
 "stop_kind IN ('pause','queue','shutdown','cold')","stop_kind IN ('configure','replace_source','edit','purge','collection','preempt')")
_CONFIGURATION_SCHEMA = '''CREATE TABLE direct_configurations (
 request_id TEXT PRIMARY KEY, payload_digest TEXT NOT NULL, wire BLOB NOT NULL,
 receipt BLOB NOT NULL, revision INTEGER NOT NULL UNIQUE, worker_epoch INTEGER NOT NULL
) STRICT;'''
_JOB_ENGINES_SCHEMA = '''CREATE TABLE direct_job_engines (
 job_id TEXT PRIMARY KEY REFERENCES jobs(job_id),
 dispatch_request_id TEXT NOT NULL UNIQUE REFERENCES direct_dispatch_commands(request_id),
 worker_epoch INTEGER NOT NULL, reservation_token TEXT NOT NULL UNIQUE,
 phase TEXT NOT NULL CHECK(phase IN ('reserved','bound','ready')),
 process TEXT, rpc_port INTEGER, rpc_secret TEXT, owner_digest TEXT NOT NULL,
 CHECK((phase='reserved' AND process IS NULL AND rpc_port IS NULL AND rpc_secret IS NULL)
 OR (phase='bound' AND process IS NOT NULL AND rpc_port IS NULL AND rpc_secret IS NULL)
 OR (phase='ready' AND process IS NOT NULL AND rpc_port IS NOT NULL AND rpc_secret IS NOT NULL))
) STRICT;'''
_SOURCE_REQUESTS_SCHEMA = "CREATE TABLE direct_source_requests (\n request_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(job_id),\n worker_epoch INTEGER NOT NULL, generation INTEGER NOT NULL, revision INTEGER NOT NULL,\n wire BLOB NOT NULL, receipt BLOB NOT NULL, seal TEXT NOT NULL,\n status TEXT NOT NULL CHECK(status IN ('pending','applied','blocked'))\n) STRICT;"
_SOURCES_SCHEMA = 'CREATE TABLE direct_sources (\n request_id TEXT PRIMARY KEY REFERENCES direct_source_requests(request_id), job_id TEXT NOT NULL REFERENCES jobs(job_id),\n generation INTEGER NOT NULL, revision INTEGER NOT NULL, source_request_id TEXT,\n checkpoint_digest TEXT, expected_sha256 TEXT NOT NULL, seal TEXT NOT NULL\n) STRICT;'
_EDIT_REQUESTS_SCHEMA = 'CREATE TABLE direct_edit_requests (\n request_id TEXT PRIMARY KEY, worker_epoch INTEGER NOT NULL, wire BLOB NOT NULL,\n capture TEXT NOT NULL, receipt BLOB NOT NULL, seal TEXT NOT NULL\n) STRICT;'
_EDIT_OUTCOMES_SCHEMA = "CREATE TABLE direct_edit_outcomes (\n request_id TEXT NOT NULL REFERENCES direct_edit_requests(request_id), job_id TEXT NOT NULL REFERENCES jobs(job_id),\n status TEXT NOT NULL CHECK(status IN ('pending','applied','blocked')), reason TEXT,\n PRIMARY KEY(request_id,job_id)\n) STRICT;"
_JOB_EDITS_SCHEMA = 'CREATE TABLE direct_job_edits (\n request_id TEXT NOT NULL REFERENCES direct_edit_requests(request_id), job_id TEXT NOT NULL REFERENCES jobs(job_id),\n generation INTEGER NOT NULL, revision INTEGER NOT NULL, audit_id INTEGER NOT NULL UNIQUE REFERENCES events(event_id),\n previous_revision INTEGER NOT NULL, source_request_id TEXT, settings TEXT NOT NULL, prior_request_id TEXT, seal TEXT NOT NULL,\n PRIMARY KEY(request_id,job_id)\n) STRICT;'
_DESTINATIONS_SCHEMA = 'CREATE TABLE direct_destinations (\n request_id TEXT NOT NULL REFERENCES direct_edit_requests(request_id), job_id TEXT NOT NULL REFERENCES jobs(job_id),\n category TEXT NOT NULL, collection TEXT, claim_token TEXT NOT NULL UNIQUE, seal TEXT NOT NULL,\n PRIMARY KEY(request_id,job_id)\n) STRICT;'
_DESTINATION_MARKERS_SCHEMA = 'CREATE TABLE direct_destination_markers (\n request_id TEXT NOT NULL, job_id TEXT NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL,\n PRIMARY KEY(request_id,job_id), FOREIGN KEY(request_id,job_id) REFERENCES direct_destinations(request_id,job_id)\n) STRICT;'
_ATTEMPT_DESTINATIONS_SCHEMA = 'CREATE TABLE direct_attempt_destinations (\n attempt_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, job_id TEXT NOT NULL,\n FOREIGN KEY(request_id,job_id) REFERENCES direct_destinations(request_id,job_id)\n) STRICT;'
_DESTINATION_STAGED_SCHEMA = 'CREATE TABLE direct_destination_staged (\n request_id TEXT NOT NULL, job_id TEXT NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL, size INTEGER NOT NULL,\n PRIMARY KEY(request_id,job_id), FOREIGN KEY(request_id,job_id) REFERENCES direct_destination_markers(request_id,job_id)\n) STRICT;'
_DESTINATION_FINAL_SCHEMA = 'CREATE TABLE direct_destination_final (\n request_id TEXT NOT NULL, job_id TEXT NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL, size INTEGER NOT NULL,\n PRIMARY KEY(request_id,job_id), FOREIGN KEY(request_id,job_id) REFERENCES direct_destination_staged(request_id,job_id)\n) STRICT;'
_FILE_REQUESTS_SCHEMA = """CREATE TABLE direct_file_requests (
 request_id TEXT PRIMARY KEY, worker_epoch INTEGER NOT NULL, wire BLOB NOT NULL,
 receipt BLOB NOT NULL, seal TEXT NOT NULL
) STRICT;"""
_FILE_OUTCOMES_SCHEMA = """CREATE TABLE direct_file_outcomes (
 request_id TEXT NOT NULL REFERENCES direct_file_requests(request_id), job_id TEXT NOT NULL REFERENCES jobs(job_id),
 status TEXT NOT NULL CHECK(status IN ('pending','blocked','reveal_pending','reveal_offered','reveal_issued','revealed','reveal_failed','purge_authorized','saved_purge_authorized','purged')),
 reason TEXT, token TEXT, presentation TEXT,
 PRIMARY KEY(request_id,job_id)
) STRICT;"""
_FILE_ARTIFACTS_SCHEMA = """CREATE TABLE direct_file_artifacts (
 request_id TEXT NOT NULL, job_id TEXT NOT NULL, attempt_id TEXT NOT NULL REFERENCES direct_publication_attempts(attempt_id),
 facts TEXT NOT NULL, seal TEXT NOT NULL, absence_observed INTEGER NOT NULL CHECK(absence_observed IN (0,1)),
 FOREIGN KEY(request_id,job_id) REFERENCES direct_file_outcomes(request_id,job_id), PRIMARY KEY(request_id,job_id)
) STRICT;"""
_SAVED_PURGE_SCHEMA = """CREATE TABLE direct_saved_purge (
 request_id TEXT NOT NULL, job_id TEXT NOT NULL, source_request_id TEXT NOT NULL REFERENCES direct_dispatch_commands(request_id),
 checkpoint_digest TEXT NOT NULL, records TEXT NOT NULL, seal TEXT NOT NULL,
 FOREIGN KEY(request_id,job_id) REFERENCES direct_file_outcomes(request_id,job_id), PRIMARY KEY(request_id,job_id)
) STRICT;"""
_SAVED_PURGE_SUCCESSORS_SCHEMA = """CREATE TABLE direct_saved_purge_successors (
 request_id TEXT NOT NULL, job_id TEXT NOT NULL, prior_request_id TEXT NOT NULL,
 PRIMARY KEY(request_id,job_id), UNIQUE(prior_request_id,job_id), CHECK(request_id!=prior_request_id),
 FOREIGN KEY(request_id,job_id) REFERENCES direct_saved_purge(request_id,job_id),
 FOREIGN KEY(prior_request_id,job_id) REFERENCES direct_saved_purge(request_id,job_id)
) STRICT;"""
_SAVED_PURGE_ARTIFACTS_SCHEMA = """CREATE TABLE direct_saved_purge_artifacts (
 request_id TEXT NOT NULL, job_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
 phase TEXT NOT NULL CHECK(phase IN ('authorized','absent')), observation TEXT,
 FOREIGN KEY(request_id,job_id) REFERENCES direct_saved_purge(request_id,job_id), PRIMARY KEY(request_id,job_id,ordinal)
) STRICT;"""
_CONTROL_REQUESTS_SCHEMA = """CREATE TABLE direct_control_requests (
 request_id TEXT PRIMARY KEY, worker_epoch INTEGER NOT NULL, wire BLOB NOT NULL, receipt BLOB NOT NULL,
 membership_digest TEXT NOT NULL, seal TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('pending','applied','blocked')),
 completion_closed INTEGER NOT NULL CHECK(completion_closed IN (0,1))
) STRICT;"""
_CONTROL_COHORTS_SCHEMA = """CREATE TABLE direct_control_cohorts (
 request_id TEXT NOT NULL REFERENCES direct_control_requests(request_id), cohort_index INTEGER NOT NULL,
 child_request_id TEXT NOT NULL UNIQUE, members TEXT NOT NULL, digest TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('pending','applied','blocked')), receipt BLOB,
 PRIMARY KEY(request_id,cohort_index)
) STRICT;"""
_CONTROL_MEMBERS_SCHEMA = """CREATE TABLE direct_control_members (
 request_id TEXT NOT NULL, job_id TEXT NOT NULL REFERENCES jobs(job_id), cohort_index INTEGER NOT NULL,
 target_index INTEGER NOT NULL CHECK(target_index BETWEEN 0 AND 499), generation INTEGER NOT NULL, revision INTEGER NOT NULL,
 PRIMARY KEY(request_id,job_id), UNIQUE(request_id,cohort_index,target_index),
 FOREIGN KEY(request_id,cohort_index) REFERENCES direct_control_cohorts(request_id,cohort_index)
) STRICT;"""
_RETRY_REOPEN_SCHEMA = """CREATE TABLE direct_retry_reopens (
 request_id TEXT PRIMARY KEY REFERENCES direct_control_requests(request_id), job_id TEXT NOT NULL REFERENCES jobs(job_id),
 worker_epoch INTEGER NOT NULL, old_generation INTEGER NOT NULL, old_revision INTEGER NOT NULL,
 generation INTEGER NOT NULL, revision INTEGER NOT NULL, audit_id INTEGER NOT NULL UNIQUE REFERENCES events(event_id),
 source_request_id TEXT NOT NULL, resume_request_id TEXT NOT NULL UNIQUE, before_budget TEXT NOT NULL, after_budget TEXT NOT NULL,
 seal TEXT NOT NULL
) STRICT;"""
_PREEMPTIONS_SCHEMA = """CREATE TABLE direct_preemptions (
 request_id TEXT PRIMARY KEY REFERENCES direct_control_requests(request_id), job_id TEXT NOT NULL REFERENCES jobs(job_id),
 source_request_id TEXT NOT NULL, worker_epoch INTEGER NOT NULL, generation INTEGER NOT NULL, revision INTEGER NOT NULL,
 process TEXT NOT NULL, seal TEXT NOT NULL
) STRICT;"""
_STOPPED_ABSENT_SCHEMA = _STOPPED_CYCLES_SCHEMA.replace('stopped_direct_cycles','stopped_absent_bodies').replace(
 "stop_kind IN ('pause','queue','shutdown','cold')","stop_kind='cold_absent'")
_CLEANUP_ACTIVATIONS_SCHEMA = """CREATE TABLE saved_cleanup_activations (
 job_id TEXT NOT NULL, attempt_id TEXT NOT NULL, worker_epoch INTEGER NOT NULL CHECK(worker_epoch>0),
 request_id TEXT NOT NULL REFERENCES saved_cleanup_requests(request_id),
 PRIMARY KEY(job_id,attempt_id,worker_epoch), UNIQUE(request_id)
) STRICT;"""
_V24_DDLS = (_SAVED_PURGE_SUCCESSORS_SCHEMA,_STOPPED_ABSENT_SCHEMA,_CLEANUP_ACTIVATIONS_SCHEMA,_DIRECT_FAILURES_SCHEMA,_DIRECT_RETRY_FRESH_SCHEMA,_DIRECT_STOCK_BINDINGS_SCHEMA,_STOPPED_FAILED_SCHEMA,_STOPPED_CONTROL_SCHEMA,_CONFIGURATION_SCHEMA,_JOB_ENGINES_SCHEMA,_SOURCE_REQUESTS_SCHEMA,_SOURCES_SCHEMA,_EDIT_REQUESTS_SCHEMA,_EDIT_OUTCOMES_SCHEMA,_JOB_EDITS_SCHEMA,_DESTINATIONS_SCHEMA,_DESTINATION_MARKERS_SCHEMA,_ATTEMPT_DESTINATIONS_SCHEMA,_DESTINATION_STAGED_SCHEMA,_DESTINATION_FINAL_SCHEMA,_FILE_REQUESTS_SCHEMA,_FILE_OUTCOMES_SCHEMA,_FILE_ARTIFACTS_SCHEMA,_SAVED_PURGE_SCHEMA,_SAVED_PURGE_ARTIFACTS_SCHEMA,_CONTROL_REQUESTS_SCHEMA,_CONTROL_COHORTS_SCHEMA,_CONTROL_MEMBERS_SCHEMA,_RETRY_REOPEN_SCHEMA,_PREEMPTIONS_SCHEMA)
_V24_TABLE_SCHEMAS = dict(_V23_TABLE_SCHEMAS,**_expected_table_schemas(*_V24_DDLS))


_IDENTIFIER: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SHA256_DIGEST: Final = re.compile(r"[0-9a-f]{64}\Z")
_DIRECT_ENGINE_RECOVERY_SECRET: Final = re.compile(r"[A-Za-z0-9_-]{43}\Z")
_QUEUE_GATES: Final = frozenset({"paused", "running"})
_JOB_CONTROL_ACTIONS: Final = frozenset({"pause", "resume", "start_now", "remove"})
_COMMAND_RECEIPT_SCOPES: Final = frozenset({"add", "queue_gate", "job_control", "add_batch", "add_batch_entry", "target_authorize"})
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
    "blocked",
    "needs_auth",
    "needs_link",
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
        or (scope in {'add_batch', 'add_batch_entry', 'target_authorize'} and value == scope)
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
class _SavedResumePlan:
    job_id: str
    request_id: str
    checkpoint_digest: str
    worker_epoch: int
    generation: int
    revision: int
    source_request_id: str
    retained_names: tuple[str, str]
    source_serial: int


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
class _DirectCleanupClaim:
    job_id: str
    attempt_id: str
    completion_audit_id: int
    completion_generation: int
    completion_revision: int
    completion_epoch: int
    proof_sha256: str
    published_stat: tuple[int, ...]
    phase: str
    activation_epoch: int | None
    activation_request_id: str | None
    certified_stat: tuple[int, ...] | None
    attempt: _DirectPublicationAttempt


@dataclass(frozen=True, slots=True, repr=False)
class _DirectCleanupPlan:
    download_result: DirectDispatchResult
    claim: _DirectCleanupClaim
    worker_epoch: int
    request_id: str | None
    payload_digest: str | None


class _PublicationChecksumMismatch(ValueError):
    """A qualified immutable expected digest differs from the prepared bytes."""


_CLEANUP_COLUMNS = tuple('job_id attempt_id completion_audit_id completion_generation '
    'completion_revision completion_epoch proof_sha256 published_stat phase '
    'activation_epoch activation_request_id certified_stat'.split())


def _cleanup_stat(value, links):
    if type(value) is not str or not 15 <= len(value) <= 141:
        raise ValueError('cleanup stat encoding invalid')
    values = json.loads(value)
    if type(values) is not list or len(values) != 7:
        raise ValueError('cleanup stat shape invalid')
    for item in values:
        _require_counter(item, 'cleanup stat')
    if (json.dumps(values, separators=(',', ':')) != value
        or not stat.S_ISREG(values[3]) or values[4] != links):
        raise ValueError('cleanup stat invalid')
    return tuple(values)


def _published_cleanup_stat(published):
    from hermes_downloads.paths import PublishedFinalPayload
    if type(published) is not PublishedFinalPayload:
        raise TypeError('cleanup payload invalid')
    return (published.st_dev, published.st_ino, published.logical_size, published.st_mode,
        published.st_nlink, published.mtime_ns, published.ctime_ns)


def _cleanup_row_values(claim):
    return tuple(json.dumps(list(value), separators=(',', ':')) if name in
        {'published_stat','certified_stat'} and value is not None else value
        for name in _CLEANUP_COLUMNS for value in (getattr(claim,name),))


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


@dataclass(frozen=True, slots=True, repr=False)
class _TargetCreationOrigin:
    kind: str
    job: MaterializedJob | None = None
    reservation: PublicationReservation | None = None
    request_id: str | None = None
    payload_digest: str | None = None
    index: int | None = None
    audit_id: int | None = None
    generation: int | None = None
    revision: int | None = None
    seal: str | None = None
    batch: _BatchCreationIntent | None = None


@dataclass(frozen=True, slots=True, repr=False)
class _TargetAuthorization:
    job_id: str
    owner_request_id: str
    owner_target_index: int
    activation_request_id: str
    cohort_kind: str
    cohort_request_id: str
    round_generation: int
    current_audit_id: int
    current_generation: int
    current_revision: int
    current_state: str
    current_worker_epoch: int
    intent_status: str
    last_admission_serial: int


_TARGET_MEMBER_COLUMNS: Final = tuple("""request_id target_index job_id outcome reason creation_kind creation_request_id
creation_payload_digest creation_index creation_child_request_id creation_child_payload_digest creation_audit_id
creation_generation creation_revision creation_seal reservation_claim_token immutable_identity_digest
captured_audit_id captured_generation captured_revision captured_state captured_authorized captured_manual_hold
captured_start_now_requested round_generation owner_request_id owner_target_index intent_audit_id intent_generation
intent_revision intent_state intent_authorized intent_manual_hold intent_start_now_requested
preserved_publication_attempt_id held_mask""".split())
_TARGET_CREATION_COLUMNS: Final = _TARGET_MEMBER_COLUMNS[5:17]
_TARGET_CAPTURE_COLUMNS: Final = _TARGET_MEMBER_COLUMNS[17:24]
_TARGET_INTENT_COLUMNS: Final = _TARGET_MEMBER_COLUMNS[27:34]


def _target_vector_digest(members, columns=_TARGET_MEMBER_COLUMNS):
    digest = hashlib.sha256(b'[')
    for index, member in enumerate(members):
        if index: digest.update(b',')
        digest.update(_batch_canonical([member[name] for name in columns], 16384))
    digest.update(b']')
    return digest.hexdigest()


def _target_result(member):
    held = member['held_mask']
    return dict(index=member['target_index'], job=member['job_id'], outcome=member['outcome'],
        reason=member['reason'], round_generation=member['round_generation'],
        captured_generation=member['captured_generation'], captured_revision=member['captured_revision'],
        held_by=None if held is None else [gate for bit, gate in enumerate(('global','collection','manual','not_due')) if held & (1 << bit)])


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
            18: _V18_TABLE_SCHEMAS,
            19: _V19_TABLE_SCHEMAS,
            20: _V20_TABLE_SCHEMAS,
            21: _V21_TABLE_SCHEMAS,
            22: _V22_TABLE_SCHEMAS,
            23: _V23_TABLE_SCHEMAS,
            24: _V24_TABLE_SCHEMAS,
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

        if connection.execute('PRAGMA foreign_keys').fetchone()[0] != 1:
            raise RuntimeError('migration requires enabled foreign keys')
        connection.execute('PRAGMA foreign_keys = OFF')
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
            if version == 17:
                if not SQLiteStore._has_table_schemas(connection, _V17_TABLE_SCHEMAS):
                    raise RuntimeError('database schema version is incomplete')
                for row in connection.execute('SELECT request_id FROM command_receipts').fetchall():
                    SQLiteStore._read_command_receipt(connection, row[0])
                connection.execute(_V18_COMMAND_RECEIPTS_SCHEMA.replace('CREATE TABLE command_receipts', 'CREATE TABLE target_registry_v18', 1))
                connection.execute('INSERT INTO target_registry_v18 SELECT * FROM command_receipts')
                connection.execute('DROP TABLE command_receipts')
                connection.execute('ALTER TABLE target_registry_v18 RENAME TO command_receipts')
                for ddl in (_TARGET_COMMANDS_SCHEMA, _TARGET_MEMBERS_SCHEMA, _AUTHORIZATION_ROUNDS_SCHEMA, _JOB_AUTHORIZATION_HEADS_SCHEMA):
                    connection.execute(ddl)
                if connection.execute('PRAGMA foreign_key_check').fetchone() is not None:
                    raise RuntimeError('migration foreign key violation')
                connection.execute('PRAGMA user_version = 18')
                version = 18
            if version == 18:
                if not SQLiteStore._has_table_schemas(connection, _V18_TABLE_SCHEMAS):
                    raise RuntimeError('database schema version is incomplete')
                connection.execute(_DIRECT_CLEANUP_CLAIMS_SCHEMA)
                connection.execute('PRAGMA user_version = 19')
                version = 19
            if version == 19:
                if not SQLiteStore._has_table_schemas(connection, _V19_TABLE_SCHEMAS):
                    raise RuntimeError('database schema version is incomplete')
                connection.execute('ALTER TABLE direct_dispatch_commands ADD COLUMN ' + _TARGET_DISPATCH_MARKER)
                connection.execute(_TARGET_DISPATCH_CAUSES_SCHEMA)
                if connection.execute('PRAGMA foreign_key_check').fetchone() is not None:
                    raise RuntimeError('migration foreign key violation')
                connection.execute('PRAGMA user_version = 20')
                version = 20
            if version == 20:
                if not SQLiteStore._has_table_schemas(connection, _V20_TABLE_SCHEMAS):
                    raise RuntimeError('database schema version is incomplete')
                connection.execute(_NOTIFICATION_OUTBOX_SCHEMA)
                connection.execute(_NOTIFICATION_CLAIMS_SCHEMA)
                connection.execute('PRAGMA user_version = 21')
                version = 21
            if version == 21:
                if not SQLiteStore._has_table_schemas(connection, _V21_TABLE_SCHEMAS):
                    raise RuntimeError('database schema version is incomplete')
                connection.execute(_STOPPED_BODIES_SCHEMA)
                connection.execute(_SAVED_CONTINUATIONS_SCHEMA)
                connection.execute('PRAGMA user_version = 22')
                version = 22
            if version == 22:
                if not SQLiteStore._has_table_schemas(connection, _V22_TABLE_SCHEMAS):
                    raise RuntimeError('database schema version is incomplete')
                if connection.execute('PRAGMA foreign_key_check').fetchone() is not None:
                    raise RuntimeError('migration foreign key violation')
                for ddl in _V23_DDLS: connection.execute(ddl)
                connection.execute('PRAGMA user_version = 23')
                version = 23
            if version == 23:
                if not SQLiteStore._has_table_schemas(connection, _V23_TABLE_SCHEMAS):
                    raise RuntimeError('database schema version is incomplete')
                if connection.execute('PRAGMA foreign_key_check').fetchone() is not None:
                    raise RuntimeError('migration foreign key violation')
                for ddl in _V24_DDLS: connection.execute(ddl)
                connection.execute('PRAGMA user_version = 24')
                version = 24
            if version == _SUPPORTED_SCHEMA_VERSION:
                if not SQLiteStore._has_table_schemas(connection, _V24_TABLE_SCHEMAS):
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

        finally:
            connection.execute('PRAGMA foreign_keys = ON')
            if connection.execute('PRAGMA foreign_keys').fetchone()[0] != 1:
                raise RuntimeError('migration foreign key restoration failed')

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

        for table in ('direct_configurations','direct_source_requests','direct_edit_requests','direct_file_requests','direct_control_requests'):
            if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone() is not None and connection.execute(f'SELECT 1 FROM {table} WHERE request_id=?',(request_id,)).fetchone() is not None:
                raise RequestConflictError('request belongs to another typed command')
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
                UNION ALL
                SELECT request_id FROM target_commands WHERE request_id = ?
                UNION ALL
                SELECT request_id FROM target_dispatch_causes WHERE request_id = ?
            )
            LIMIT 1
            """,
            (request_id, request_id, request_id, request_id, request_id, request_id, request_id, request_id),
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
    def _batch_budget(self, *, deadline=None):
        connection = self._connection
        now = time.monotonic()
        if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline) or deadline <= now):
            raise TimeoutError('batch database deadline')
        deadline = now + 2.0 if deadline is None else min(now + 2.0, deadline)
        previous = connection.execute('PRAGMA busy_timeout').fetchone()[0]
        connection.execute(f'PRAGMA busy_timeout = {min(2000, max(0, int((deadline - now) * 1000)))}')
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

    def _read_batch(self, parent_id, parent_cache=None):
        if parent_cache is None: parent_cache = {}
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
                intents[item.job] = self._read_batch_creation(row, parent_id, digest, parent_cache)
        return AddBatchResult(parent_id, True, original.results), intents

    def _read_batch_creation(self, row, parent_id, parent_digest, parent_cache=None):
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
        self._validate_batch_lifecycle(captured, current, parent_cache)
        return captured

    def get_batch_creation_intent(self, job_id: str) -> _BatchCreationIntent | None:
        """Readonly original metadata; never reconstruct missing intent or grant work."""
        _require_identifier(job_id, 'job_id')
        connection = self._connection
        try:
            with self._batch_budget():
                connection.execute('BEGIN')
                try:
                    origin = self._read_creation_origin(connection, job_id, {}, target=False)
                    return origin.batch if origin.kind == 'batch' else None
                finally:
                    connection.rollback()
        except Exception:
            raise RuntimeError('batch_state_invalid') from None

    def _read_creation_origin(self, connection, job_id, parent_cache, *, target=True):
        """Qualify original creation inside the caller's transaction and budget."""
        links = connection.execute('SELECT parent_request_id FROM add_batch_entries WHERE job_id=? LIMIT 2', (job_id,)).fetchall()
        commands = connection.execute('SELECT request_id,payload_digest,job_id,generation,revision FROM commands WHERE job_id=? LIMIT 2', (job_id,)).fetchall()
        if links:
            if len(links) != 1: raise ValueError('batch job link invalid')
            parent = links[0][0]
            if parent not in parent_cache: parent_cache[parent] = self._read_batch(parent, parent_cache)
            _, intents = parent_cache[parent]
            if job_id not in intents: raise ValueError('batch accepted link missing')
            captured = intents[job_id]
            return _TargetCreationOrigin('batch', captured.original_job, captured.reservation,
                captured.parent_request_id, captured.parent_payload_digest, captured.index,
                captured.audit_id, captured.original_job.intent.generation, captured.original_job.intent.revision,
                captured.seal, captured)
        if not commands:
            for table in ('jobs','events','materialized_jobs','publication_reservations','job_authorization_heads'):
                if connection.execute(f'SELECT 1 FROM {table} WHERE job_id=? LIMIT 1', (job_id,)).fetchone() is not None:
                    raise ValueError('creation provenance missing')
            for row in connection.execute('SELECT request_id,target_index FROM target_members WHERE job_id=?', (job_id,)):
                command, _, members = self._read_target_command(connection, row['request_id'], parent_cache)
                member = members[row['target_index']]
                # Sealed unknown results are negative history, never creation or authority.
                if (command.to_record()['selector']['kind'] != 'jobs' or member['job_id'] != job_id
                        or (member['outcome'], member['reason']) != ('blocked', 'unknown_job')
                        or any(member[name] is not None for name in _TARGET_MEMBER_COLUMNS[5:])):
                    raise ValueError('creation provenance missing')
            return _TargetCreationOrigin('unknown')
        if len(commands) != 1: raise ValueError('creation provenance not unique')
        command = commands[0]
        request = _require_identifier(command['request_id'], 'single request')
        digest = _require_payload_digest(command['payload_digest'])
        generation = _require_counter(command['generation'], 'single generation')
        revision = _require_counter(command['revision'], 'single revision')
        if self._read_command_receipt(connection, request) != (digest, 'add', 'add'):
            raise ValueError('single registry changed')
        added = connection.execute("SELECT event_id,generation,revision FROM events WHERE job_id=? AND kind='job_added' LIMIT 2", (job_id,)).fetchall()
        if (len(added) != 1 or tuple(added[0])[1:] != (generation,revision)
                or type(added[0][0]) is not int or added[0][0] < 1 or self.get_job(job_id) is None):
            raise ValueError('single creation audit changed')
        if not target:
            return _TargetCreationOrigin('unsupported_single',request_id=request,payload_digest=digest,
                index=0,audit_id=added[0][0],generation=generation,revision=revision)
        job = self.get_materialized_job(job_id)
        if job is None:
            return _TargetCreationOrigin('unsupported_single', request_id=request, payload_digest=digest,
                index=0, audit_id=added[0][0], generation=generation, revision=revision)
        reservation = self._read_publication_reservation(connection, job_id)
        if reservation is None: raise ValueError('single reservation missing')
        origin = _TargetCreationOrigin('unsupported_single', job, reservation, request, digest, 0,
            added[0][0], generation, revision)
        if job.source_kind is not SourceKind.DIRECT: return origin
        source = validate_source_url(job.intent.source_url.decode('utf-8'))
        if source.raw_url != job.intent.source_url: raise ValueError('single source changed')
        if (job.queue_collection_id is not None or job.destination_collection is not None
                or job.scheduled_for is not None or job.intent.expected_revision is not None):
            return origin
        typed = JobAddCommand(job_id, request, job.intent.source_url.decode('utf-8'), job.priority,
            job.order_key, job.category, job.partial_filename, job.selected_final_filename)
        if typed.payload_digest != digest: raise ValueError('single typed digest changed')
        latest = self._target_latest(connection, job_id)
        current = self._read_job_control_projection(connection, job_id)
        if (latest['generation'],latest['revision']) != (current.generation,current.revision):
            raise ValueError('single current audit changed')
        if current.generation < generation or current.revision < revision: raise ValueError('single chronology changed')
        return replace(origin, kind='supported_single')

    @staticmethod
    def _target_latest(connection, job_id):
        row = connection.execute('SELECT event_id,kind,generation,revision FROM events WHERE job_id=? ORDER BY event_id DESC LIMIT 1', (job_id,)).fetchone()
        if row is None: raise ValueError('target audit missing')
        for name in ('event_id','generation','revision'): _require_counter(row[name], name)
        if row['event_id'] < 1: raise ValueError('target audit invalid')
        _require_sqlite_text(row['kind'], 'target audit kind')
        return row

    def _target_creation_values(self, origin):
        if origin.kind not in {'batch','supported_single','unsupported_single'} or origin.job is None or origin.reservation is None: return {}
        job = origin.job
        return dict(creation_kind='batch' if origin.kind == 'batch' else 'single',
            creation_request_id=origin.request_id, creation_payload_digest=origin.payload_digest,
            creation_index=origin.index, creation_child_request_id=job.intent.request_id,
            creation_child_payload_digest=job.intent.payload_digest, creation_audit_id=origin.audit_id,
            creation_generation=origin.generation, creation_revision=origin.revision, creation_seal=origin.seal,
            reservation_claim_token=origin.reservation.claim_token,
            immutable_identity_digest=self._publication_ownership(job, origin.reservation))

    @staticmethod
    def _target_transform(action, current, preserved):
        state = current.state
        if action in {'resume','start_now'} and state in {'paused','retry_wait','blocked','needs_auth','needs_link'} and not preserved: state = 'queued'
        return replace(current, state=state, authorized=True,
            manual_hold=current.manual_hold if action == 'start' else False,
            start_now_requested=True if action == 'start_now' else current.start_now_requested)

    def _read_target_command(self, connection, request_id, cache):
        key = ('target', request_id)
        if key in cache: return cache[key]
        row = connection.execute('SELECT * FROM target_commands WHERE request_id=?', (request_id,)).fetchone()
        if row is None: raise ValueError('target command missing')
        for name in ('command_blob','receipt_blob'): _require_sqlite_blob(row[name], name)
        command = TargetAuthorizeCommand.from_record(_batch_decode(row['command_blob']))
        if (command._wire_request != row['command_blob'] or command.request_id != request_id
                or command.payload_digest != row['payload_digest'] or command.action != row['action']
                or command.expected_worker_epoch != row['worker_epoch']
                or self._read_command_receipt(connection, request_id) != (command.payload_digest,'target_authorize','target_authorize')):
            raise ValueError('target command seal changed')
        _require_counter(row['captured_at_us'], 'target owner UTC')
        result = TargetAuthorizeResult.from_record(_batch_decode(row['receipt_blob']))
        if result._wire_reply != row['receipt_blob'] or result.replayed or result.request_id != request_id or result.worker_epoch != command.expected_worker_epoch:
            raise ValueError('target original receipt changed')
        members = [dict(m) for m in connection.execute('SELECT * FROM target_members WHERE request_id=? ORDER BY target_index', (request_id,))]
        if type(row['member_count']) is not int or len(members) != row['member_count'] or not 1 <= len(members) <= 500:
            raise ValueError('target member count changed')
        if _target_vector_digest(members) != _require_payload_digest(row['vector_digest']): raise ValueError('target member seal changed')
        selector = command.to_record()['selector']
        if selector['kind'] == 'jobs' and len(selector['targets']) != len(members): raise ValueError('target selection changed')
        for index, member in enumerate(members):
            if tuple(member) != _TARGET_MEMBER_COLUMNS or member['request_id'] != request_id or type(member['target_index']) is not int or member['target_index'] != index:
                raise ValueError('target indexed relation changed')
            _require_identifier(member['job_id'], 'target job')
            for name in ('creation_request_id','creation_child_request_id','owner_request_id','preserved_publication_attempt_id'):
                if member[name] is not None: _require_identifier(member[name], name)
            for name in ('creation_payload_digest','creation_child_payload_digest','creation_seal','reservation_claim_token','immutable_identity_digest'):
                if member[name] is not None: _require_payload_digest(member[name])
            for name in ('creation_index','creation_audit_id','creation_generation','creation_revision','captured_audit_id',
                    'captured_generation','captured_revision','round_generation','owner_target_index','intent_audit_id','intent_generation','intent_revision','held_mask'):
                if member[name] is not None: _require_counter(member[name], name)
            for name in ('captured_authorized','captured_manual_hold','captured_start_now_requested','intent_authorized','intent_manual_hold','intent_start_now_requested'):
                if member[name] is not None: _require_sqlite_boolean(member[name], name)
            for name in ('captured_state','intent_state'):
                if member[name] is not None: _require_public_job_state(member[name], name)
            if member['held_mask'] is not None and member['held_mask'] > 15: raise ValueError('target gate mask invalid')
            for columns in (_TARGET_CAPTURE_COLUMNS, _TARGET_INTENT_COLUMNS):
                values = [member[n] for n in columns]
                if any(v is None for v in values) and not all(v is None for v in values): raise ValueError('target partial scalar group')
            creation = [member[n] for n in _TARGET_CREATION_COLUMNS if n != 'creation_seal']
            if any(v is None for v in creation) and not all(v is None for v in creation): raise ValueError('target partial creation')
            if member['creation_kind'] is None and member['creation_seal'] is not None:
                raise ValueError('target orphan creation seal')
            if member['creation_kind'] is not None:
                if member['creation_kind'] not in {'single','batch'} or member['creation_index'] > 499 or member['creation_audit_id'] < 1:
                    raise ValueError('target creation invalid')
                if (member['creation_kind'] == 'single' and (member['creation_index'] != 0 or member['creation_seal'] is not None)) or (member['creation_kind'] == 'batch' and member['creation_seal'] is None):
                    raise ValueError('target creation seal invalid')
                audit = connection.execute('SELECT kind,job_id,generation,revision FROM events WHERE event_id=?', (member['creation_audit_id'],)).fetchone()
                if audit is None or tuple(audit) != ('job_added',member['job_id'],member['creation_generation'],member['creation_revision']):
                    raise ValueError('target original audit changed')
                original = connection.execute('SELECT request_id,payload_digest,job_id,generation,revision FROM commands WHERE request_id=?',
                    (member['creation_child_request_id'],)).fetchone()
                if original is None or tuple(original) != (member['creation_child_request_id'],member['creation_child_payload_digest'],
                        member['job_id'],member['creation_generation'],member['creation_revision']):
                    raise ValueError('target original command changed')
                scope = 'add_batch_entry' if member['creation_kind']=='batch' else 'add'
                if self._read_command_receipt(connection,member['creation_child_request_id']) != (member['creation_child_payload_digest'],scope,scope):
                    raise ValueError('target original registry changed')
                if member['creation_kind']=='batch':
                    if self._read_command_receipt(connection,member['creation_request_id']) != (member['creation_payload_digest'],'add_batch','add_batch'):
                        raise ValueError('target original parent registry changed')
                    sealed = connection.execute('SELECT job_id,child_request_id,creation_intent_blob,creation_intent_digest FROM add_batch_entries WHERE parent_request_id=? AND entry_index=? AND status="applied"',
                        (member['creation_request_id'],member['creation_index'])).fetchone()
                    if (sealed is None or tuple(sealed)[:2] != (member['job_id'],member['creation_child_request_id'])
                            or sealed['creation_intent_digest'] != member['creation_seal']
                            or _batch_creation_seal(_require_sqlite_blob(sealed['creation_intent_blob'],'original target creation')) != member['creation_seal']):
                        raise ValueError('target original batch seal changed')
                elif (member['creation_request_id'],member['creation_payload_digest']) != (member['creation_child_request_id'],member['creation_child_payload_digest']):
                    raise ValueError('target original single relation changed')
                materialized = self.get_materialized_job(member['job_id'])
                reservation = self._read_publication_reservation(connection,member['job_id'])
                if (materialized is None or reservation is None or reservation.claim_token != member['reservation_claim_token']
                        or self._publication_ownership(materialized,reservation) != member['immutable_identity_digest']):
                    raise ValueError('target immutable creation changed')
            if member['captured_audit_id'] is not None:
                audit = connection.execute('SELECT job_id,generation,revision FROM events WHERE event_id=?', (member['captured_audit_id'],)).fetchone()
                if audit is None or tuple(audit) != (member['job_id'],member['captured_generation'],member['captured_revision']):
                    raise ValueError('target captured audit changed')
            positive = member['outcome'] in {'new_authority','existing_authority'}
            if positive:
                if any(member[n] is None for n in (*_TARGET_CAPTURE_COLUMNS,'creation_kind','round_generation','owner_request_id','owner_target_index','held_mask')):
                    raise ValueError('target authority incomplete')
                if member['round_generation'] < 1 or member['owner_target_index'] > 499: raise ValueError('target round invalid')
                owner = connection.execute('SELECT * FROM target_members WHERE request_id=? AND target_index=?', (member['owner_request_id'],member['owner_target_index'])).fetchone()
                if owner is None or owner['outcome'] != 'new_authority' or owner['job_id'] != member['job_id'] or owner['round_generation'] != member['round_generation'] or any(owner[n] != member[n] for n in _TARGET_CREATION_COLUMNS):
                    raise ValueError('target original owner changed')
                if (owner['owner_request_id'],owner['owner_target_index']) != (owner['request_id'],owner['target_index']): raise ValueError('target owner cycle')
                if member['outcome'] == 'new_authority' and (member['owner_request_id'],member['owner_target_index']) != (request_id,index):
                    raise ValueError('target new owner changed')
                current = _JobControlProjection(member['job_id'],member['captured_generation'],member['captured_revision'],member['captured_state'],
                    bool(member['captured_authorized']),bool(member['captured_manual_hold']),bool(member['captured_start_now_requested']))
                wanted = self._target_transform(command.action,current,member['preserved_publication_attempt_id'] is not None)
                if member['intent_audit_id'] is None:
                    if wanted != current: raise ValueError('target missing intent audit')
                else:
                    if member['intent_generation'] != current.generation or member['intent_revision'] != current.revision + 1:
                        raise ValueError('target intent counters changed')
                    if (member['intent_state'],member['intent_authorized'],member['intent_manual_hold'],member['intent_start_now_requested']) != (wanted.state,int(wanted.authorized),int(wanted.manual_hold),int(wanted.start_now_requested)):
                        raise ValueError('target intent transform changed')
                    if (current.state not in {'queued','paused'} and not (command.action=='resume' and (self._fresh_retry(connection,current.job,command.request_id) or self._resumable_failure_capture(connection,member)))) or wanted == current: raise ValueError('target unsafe intent')
                    audit = connection.execute('SELECT kind,job_id,generation,revision FROM events WHERE event_id=?', (member['intent_audit_id'],)).fetchone()
                    if audit is None or tuple(audit) != ('job_target_authorized',member['job_id'],member['intent_generation'],member['intent_revision']):
                        raise ValueError('target intent audit changed')
                    predecessor = connection.execute('SELECT event_id FROM events WHERE job_id=? AND event_id<? ORDER BY event_id DESC LIMIT 1', (member['job_id'],member['intent_audit_id'])).fetchone()
                    if predecessor is None or predecessor[0] != member['captured_audit_id']: raise ValueError('target predecessor changed')
            elif any(member[n] is not None for n in ('round_generation','owner_request_id','owner_target_index',*_TARGET_INTENT_COLUMNS,'preserved_publication_attempt_id')):
                raise ValueError('blocked target has authority')
            if selector['kind'] == 'jobs':
                expected = selector['targets'][index]
                if expected['job'] != member['job_id']: raise ValueError('target ordered selector changed')
                if positive and expected['expected_revision'] not in (None,member['captured_revision']): raise ValueError('target selected fence changed')
            else:
                creation = selector['creation']; indices = selector['indices']
                if member['creation_kind'] is not None and (member['creation_kind'],member['creation_request_id']) != (creation['kind'],creation['request_id']):
                    raise ValueError('target cohort selector changed')
                if indices is not None and (len(indices) != len(members) or member['creation_index'] != indices[index]): raise ValueError('target selected positions changed')
        if result.to_record()['results'] != [_target_result(m) for m in members]: raise ValueError('target receipt vector changed')
        cache[key] = (command,result,members)
        try:
            for member in members:
                if member['outcome'] in {'new_authority','existing_authority'}:
                    self._target_round(connection,member,cache)
                    if member['owner_request_id'] != request_id:
                        self._read_target_command(connection,member['owner_request_id'],cache)
        except Exception:
            cache.pop(key,None)
            raise
        return cache[key]

    def _target_round(self, connection, member, cache):
        cohort = (member['creation_kind'],member['creation_request_id'])
        generation = member['round_generation']; key = ('round',*cohort,generation)
        if key in cache: return cache[key]
        row = connection.execute('SELECT * FROM authorization_rounds WHERE cohort_kind=? AND cohort_request_id=? AND round_generation=?', (*cohort,generation)).fetchone()
        if row is None or row['status'] not in {'open','terminal'}: raise ValueError('target round missing')
        _require_identifier(row['origin_request_id'], 'round origin')
        if row['origin_request_id'] != member['owner_request_id']: raise ValueError('target round owner changed')
        selected = [dict(m) for m in connection.execute("SELECT * FROM target_members WHERE request_id=? AND creation_kind=? AND creation_request_id=? AND round_generation=? AND outcome='new_authority' ORDER BY target_index", (row['origin_request_id'],*cohort,generation))]
        if type(row['member_count']) is not int or not 1 <= row['member_count'] <= 500 or len(selected) != row['member_count']:
            raise ValueError('target round count changed')
        if _target_vector_digest(selected,('request_id','target_index','job_id')) != _require_payload_digest(row['vector_digest']): raise ValueError('target round seal changed')
        counter = self._target_counter(connection,cohort,cache)
        if counter < generation: raise ValueError('target round exceeds cohort counter')
        cache[key] = row
        return row

    def _target_counter(self, connection, cohort, cache):
        key = ('counter',*cohort)
        if key in cache: return cache[key]
        name = 'authorization_round:' + hashlib.sha256(_batch_canonical(dict(kind=cohort[0],request_id=cohort[1]),512)).hexdigest()
        row = connection.execute('SELECT key,value,revision FROM settings WHERE key=?', (name,)).fetchone()
        if row is None:
            if connection.execute('SELECT 1 FROM authorization_rounds WHERE cohort_kind=? AND cohort_request_id=? LIMIT 1',cohort).fetchone() is not None:
                raise ValueError('target counter missing')
            counter = 0
        else:
            counter = _require_counter(row['revision'],'cohort counter')
            if counter < 1 or type(row['value']) is not str or row['value'] != str(counter) or row['key'] != name:
                raise ValueError('target counter invalid')
            last = connection.execute('SELECT origin_request_id FROM authorization_rounds WHERE cohort_kind=? AND cohort_request_id=? AND round_generation=?',(*cohort,counter)).fetchone()
            if last is None: raise ValueError('target counter round missing')
        cache[key] = counter
        if counter:
            try:
                _,_,members = self._read_target_command(connection,last['origin_request_id'],cache)
                selected = [m for m in members if m['outcome']=='new_authority'
                    and (m['creation_kind'],m['creation_request_id'],m['round_generation']) == (*cohort,counter)]
                if not selected: raise ValueError('target counter causal members missing')
                self._target_round(connection,selected[0],cache)
            except Exception:
                cache.pop(key,None)
                raise
        return counter

    def _target_managed(self, connection, job_id, cache):
        member = connection.execute("SELECT request_id FROM target_members WHERE job_id=? AND outcome IN ('new_authority','existing_authority') LIMIT 1", (job_id,)).fetchone()
        if member is None:
            if connection.execute('SELECT 1 FROM job_authorization_heads WHERE job_id=?', (job_id,)).fetchone() is not None:
                raise ValueError('target head without membership')
            return False
        self._read_target_command(connection,member[0],cache)
        return True

    def _read_target_head(self, connection, job_id, origin, cache, *, epoch=None):
        managed = self._target_managed(connection,job_id,cache)
        row = connection.execute('SELECT * FROM job_authorization_heads WHERE job_id=?', (job_id,)).fetchone()
        if not managed: return None
        if row is None: raise ValueError('upgraded target head missing')
        head = _TargetAuthorization(**dict(row))
        for name in ('job_id','owner_request_id','activation_request_id','cohort_request_id'): _require_identifier(getattr(head,name), name)
        for name in ('owner_target_index','round_generation','current_audit_id','current_generation','current_revision','current_worker_epoch','last_admission_serial'):
            _require_counter(getattr(head,name), name)
        if head.owner_target_index > 499 or head.round_generation < 1 or head.current_audit_id < 1 or head.current_worker_epoch < 1:
            raise ValueError('target head counter invalid')
        if head.intent_status not in {'current','inactive','terminal'}: raise ValueError('target head status invalid')
        current = self._read_job_control_projection(connection,job_id); latest = self._target_latest(connection,job_id)
        epoch = self._current_worker_epoch(connection) if epoch is None else epoch
        if (head.current_generation,head.current_revision,head.current_state,head.current_audit_id,head.current_worker_epoch) != (current.generation,current.revision,current.state,latest['event_id'],epoch):
            raise ValueError('target head current fence changed')
        owner_command,_,owners = self._read_target_command(connection,head.owner_request_id,cache)
        if head.owner_target_index >= len(owners): raise ValueError('target owner missing')
        owner = owners[head.owner_target_index]
        activation_command,_,activations = self._read_target_command(connection,head.activation_request_id,cache)
        activated = [m for m in activations if m['job_id'] == job_id]
        if len(activated) != 1: raise ValueError('target activation missing')
        activation = activated[0]
        expected_creation = self._target_creation_values(origin)
        if (not expected_creation or owner['outcome'] != 'new_authority' or activation['outcome'] not in {'new_authority','existing_authority'}
                or owner['job_id'] != job_id or any(owner[n] != v or activation[n] != v for n,v in expected_creation.items())
                or (head.cohort_kind,head.cohort_request_id,head.round_generation) != (owner['creation_kind'],owner['creation_request_id'],owner['round_generation'])
                or (activation['owner_request_id'],activation['owner_target_index'],activation['round_generation']) != (head.owner_request_id,head.owner_target_index,head.round_generation)):
            raise ValueError('target head original authority changed')
        if head.intent_status == 'current' and activation_command.expected_worker_epoch != epoch:
            raise ValueError('target current activation epoch changed')
        prefix = 'intent' if activation['intent_audit_id'] is not None else 'captured'
        generation,revision,state = (activation[prefix+'_'+n] for n in ('generation','revision','state'))
        flags = tuple(bool(activation[prefix+'_'+n]) for n in ('authorized','manual_hold','start_now_requested'))
        last = activation[prefix+'_audit_id']
        cold_successors = 0
        for audit in connection.execute('SELECT event_id,kind,generation,revision FROM events WHERE job_id=? AND event_id>? ORDER BY event_id', (job_id,last)):
            if audit['revision'] != revision + 1: raise ValueError('target head audit gap')
            if audit['generation'] == generation + 1 and audit['kind'] == 'job_paused':
                if state not in _RECOVERABLE_COLD_START_STATES: raise ValueError('target cold predecessor changed')
                state = 'paused'; cold_successors += 1
            elif audit['kind']=='job_resume_blocked' and audit['generation']==generation:
                state=self._saved_decision_for_audit(connection,job_id,generation,audit['revision'])
            elif audit['kind']=='job_retry_opened' and audit['generation']==generation+1:
                reopened=self._retry_reopen(connection,audit['event_id'])
                if (reopened['old_generation'],reopened['old_revision'])!=(generation,revision) or state not in {'blocked','paused'}:raise ValueError('retry head predecessor changed')
                state='paused'
            elif audit['generation'] != generation: raise ValueError('target head generation changed')
            elif audit['kind'] in {'job_resolving','job_downloading'}:
                expected = 'queued' if audit['kind']=='job_resolving' else 'resolving'
                if state != expected or not self._batch_dispatch_corroborated(job_id,generation,audit['revision'],audit['kind'],cache):
                    raise ValueError('target body lifecycle changed')
                state = 'resolving' if expected=='queued' else 'downloading'
            elif audit['kind'] == 'job_finalizing':
                if state != 'downloading': raise ValueError('target finalizing predecessor changed')
                producers = connection.execute("SELECT request_id FROM direct_dispatch_commands WHERE job_id=? AND generation=? AND revision=? AND status='started' AND state='downloading'",
                    (job_id,generation,revision)).fetchall()
                if len(producers) != 1 or self._read_target_dispatch_cause(connection,producers[0][0],cache) is None:
                    raise ValueError('target finalizing producer missing')
                state = 'finalizing'
            elif audit['kind']=='job_edited':
                self._effective_settings(connection,self.get_materialized_job(job_id))
            elif audit['kind'] == 'job_failure':
                if state != 'downloading': raise ValueError('failure head predecessor changed')
                state = self._failure_state_for_audit(connection,job_id,generation,audit['revision'],audit['event_id'])
            elif audit['kind'] == 'job_paused':
                if self._batch_control_corroborated(job_id,'pause',generation,audit['revision'],'paused',flags[0]):
                    flags = (flags[0],True,flags[2])
                elif state not in {'resolving','downloading','finalizing'}:
                    raise ValueError('target pause receipt missing')
                state = 'paused'
            elif audit['kind'] == 'job_removed':
                if not self._batch_control_corroborated(job_id,'remove',generation,audit['revision'],'removed',False):
                    raise ValueError('target removal receipt missing')
                state = 'removed'; flags = (False,True,False)
            elif audit['kind'] == 'job_completed':
                if state not in {'paused','finalizing'} or self._read_final_publication_binding(connection,job_id) is None:
                    raise ValueError('target completion proof missing')
                state = 'completed'
            else: raise ValueError('target unsupported head successor')
            generation,revision,last = audit['generation'],audit['revision'],audit['event_id']
        if (generation,revision,state,last,flags) != (current.generation,current.revision,current.state,latest['event_id'],(current.authorized,current.manual_hold,current.start_now_requested)):
            raise ValueError('target head flags changed')
        if epoch < activation_command.expected_worker_epoch or cold_successors > epoch - activation_command.expected_worker_epoch:
            raise ValueError('target head cold epoch changed')
        if head.intent_status == 'current' and cold_successors:
            raise ValueError('target cold head still active')
        if (head.intent_status == 'terminal') != (current.state in _TERMINAL_JOB_CONTROL_STATES): raise ValueError('target terminal status changed')
        serials = connection.execute("SELECT request_id,admission_serial FROM target_dispatch_causes WHERE kind='body' AND owner_request_id=? AND owner_target_index=? ORDER BY admission_serial",
            (head.owner_request_id,head.owner_target_index)).fetchall()
        if len(serials) != head.last_admission_serial or any(r['admission_serial'] != index for index,r in enumerate(serials,1)):
            raise ValueError('target admission serial history changed')
        for producer in serials: self._read_target_dispatch_cause(connection,producer['request_id'],cache)
        return head

    def _target_audit_member(self, connection, audit_id, cache):
        row = connection.execute('SELECT request_id,target_index FROM target_members WHERE intent_audit_id=?', (audit_id,)).fetchone()
        if row is None: raise ValueError('target audit member missing')
        _,_,members = self._read_target_command(connection,row['request_id'],cache)
        member = members[row['target_index']]
        if member['intent_audit_id'] != audit_id: raise ValueError('target audit relation changed')
        return member

    def _target_publication_audit(self, connection, row, cache=None):
        member = self._target_audit_member(connection,row['audit_id'],{} if cache is None else cache)
        if ((member['job_id'],member['intent_generation'],member['intent_revision'],member['intent_state'])
                != (row['job_id'],row['generation'],row['revision'],row['state'])
                or member['preserved_publication_attempt_id'] != row['attempt_id'] or row['state'] != 'paused'):
            raise ValueError('target publication cutpoint changed')
        return True

    def _capture_target_head(self, connection, job_id, cache=None, *, epoch=None):
        cache = {} if cache is None else cache
        if not self._target_managed(connection,job_id,cache): return None
        origin = self._read_creation_origin(connection,job_id,cache)
        return self._read_target_head(connection,job_id,origin,cache,epoch=epoch)

    def _advance_target_head(self, connection, head, *, inactive=False):
        if head is None: return
        current = self._read_job_control_projection(connection,head.job_id)
        audit = self._target_latest(connection,head.job_id)
        status = 'terminal' if current.state in _TERMINAL_JOB_CONTROL_STATES else ('inactive' if inactive else head.intent_status)
        connection.execute('''UPDATE job_authorization_heads SET current_audit_id=?,current_generation=?,current_revision=?,
            current_state=?,current_worker_epoch=?,intent_status=? WHERE job_id=? AND owner_request_id=? AND owner_target_index=?
            AND activation_request_id=? AND cohort_kind=? AND cohort_request_id=? AND round_generation=?
            AND current_audit_id=? AND current_generation=? AND current_revision=? AND current_state=?
            AND current_worker_epoch=? AND intent_status=? AND last_admission_serial=?''',
            (audit['event_id'],current.generation,current.revision,current.state,self._current_worker_epoch(connection),status,
             head.job_id,head.owner_request_id,head.owner_target_index,head.activation_request_id,head.cohort_kind,head.cohort_request_id,
             head.round_generation,head.current_audit_id,head.current_generation,head.current_revision,head.current_state,
             head.current_worker_epoch,head.intent_status,head.last_admission_serial))
        self._require_one_changed_row(connection,'target successor CAS')

    def _close_target_round(self, connection, job_id):
        cache = {}; head = self._capture_target_head(connection,job_id,cache)
        if head is None or head.intent_status != 'terminal': return
        _,_,members = self._read_target_command(connection,head.owner_request_id,cache)
        selected = [m for m in members if m['outcome']=='new_authority' and
            (m['creation_kind'],m['creation_request_id'],m['round_generation']) ==
            (head.cohort_kind,head.cohort_request_id,head.round_generation)]
        for member in selected:
            row = connection.execute('SELECT intent_status FROM job_authorization_heads WHERE job_id=?', (member['job_id'],)).fetchone()
            if row is None: raise ValueError('target terminal member head missing')
            if row[0] != 'terminal': return
        for member in selected:
            terminal = self._capture_target_head(connection,member['job_id'],cache)
            if terminal is None or terminal.intent_status != 'terminal': raise ValueError('target terminal authority changed')
        closed = connection.execute("UPDATE authorization_rounds SET status='terminal' WHERE cohort_kind=? AND cohort_request_id=? AND round_generation=? AND status='open'",
            (head.cohort_kind,head.cohort_request_id,head.round_generation))
        if closed.rowcount != 1:
            return
        captured=connection.execute('SELECT request_id FROM direct_control_cohorts WHERE child_request_id=?',(head.owner_request_id,)).fetchone()
        if captured is not None:
            self._close_captured_completion(connection,captured[0]);return
        facts = [self._notification_completion_fact(connection, m['job_id']) for m in selected]
        completed = [fact for fact in facts if fact is not None]
        if not completed:
            return
        collections = {self.get_materialized_job(m['job_id']).queue_collection_id for m in selected}
        collection = next(iter(collections)) if len(collections) == 1 else None
        provenance = dict(kind='round', cohort_kind=head.cohort_kind,
            cohort_request_id=head.cohort_request_id, round_generation=head.round_generation,
            owner_request_id=head.owner_request_id,
            members=[dict(job_id=m['job_id'], creation_kind=m['creation_kind'],
                creation_request_id=m['creation_request_id'], creation_index=m['creation_index'],
                completion=fact) for m, fact in zip(selected, facts)])
        self._enqueue_completion(connection,
            semantic_key=f'round:{head.cohort_kind}:{head.cohort_request_id}:{head.round_generation}',
            job_id=completed[0]['job_id'] if len(completed) == 1 else None,
            collection_id=collection, generation=head.round_generation, count=len(completed), provenance=provenance)

    def _close_captured_completion(self,connection,request):
        row,command,cohorts=self._control_request(request)
        if command.action!='start' or row['completion_closed'] or row['status']!='applied':return
        groups=[];cache={}
        for cohort in cohorts:
            _,_,members=self._read_target_command(connection,cohort['child_request_id'],cache)
            selected=[member for member in members if member['outcome']=='new_authority']
            for member in selected:
                head=self._capture_target_head(connection,member['job_id'],cache)
                if head is None or (head.owner_request_id,head.owner_target_index)!=(member['owner_request_id'],member['owner_target_index']):raise ValueError('captured completion owner changed')
                if head.intent_status!='terminal':return
            groups.append((cohort,selected))
        connection.execute("UPDATE direct_control_requests SET completion_closed=1 WHERE request_id=? AND status='applied' AND completion_closed=0",(request,))
        self._require_one_changed_row(connection,'captured completion CAS')
        for cohort,members in groups:
            facts=[self._notification_completion_fact(connection,member['job_id']) for member in members]
            completed=[fact for fact in facts if fact is not None]
            if not completed:continue
            collections={self.get_materialized_job(member['job_id']).queue_collection_id for member in members}
            provenance=dict(kind='captured',parent_request_id=request,parent_seal=row['seal'],child_request_id=cohort['child_request_id'],cohort_index=cohort['cohort_index'],
                members=[dict(job_id=member['job_id'],creation_kind=member['creation_kind'],creation_request_id=member['creation_request_id'],creation_index=member['creation_index'],completion=fact) for member,fact in zip(members,facts)])
            self._enqueue_completion(connection,semantic_key='captured:'+request+':'+str(cohort['cohort_index']),job_id=completed[0]['job_id'] if len(completed)==1 else None,
                collection_id=next(iter(collections)) if len(collections)==1 else None,generation=command.expected_worker_epoch,count=len(completed),provenance=provenance)

    def _notification_completion_fact(self, connection, job_id):
        current = self._read_job_control_projection(connection, job_id)
        if current.state != 'completed':
            return None
        job = self.get_materialized_job(job_id)
        audit = self._target_latest(connection, job_id)
        binding = self._read_final_publication_binding(connection, job_id)
        if (job is None or job.source_kind is not SourceKind.DIRECT or binding is None
            or (audit['kind'], audit['generation'], audit['revision']) != ('job_completed', current.generation, current.revision)):
            return None
        creation = connection.execute("SELECT event_id FROM events WHERE job_id=? AND kind='job_added' ORDER BY event_id LIMIT 1", (job_id,)).fetchone()
        if creation is None:
            raise ValueError('notification creation audit missing')
        return dict(job_id=job_id, generation=current.generation, revision=current.revision,
            audit_id=audit['event_id'], creation_audit_id=creation[0],
            final_device=binding.final_device, final_inode=binding.final_inode, logical_size=binding.logical_size)

    def _notification_store_uuid(self, connection, *, create=False):
        row = connection.execute("SELECT value,revision FROM settings WHERE key='notification_store_uuid'").fetchone()
        if row is None:
            if not create:
                return None
            value = str(uuid.uuid4())
            connection.execute("INSERT INTO settings VALUES ('notification_store_uuid',?,0)", (value,))
            return value
        _notification_uuid(row['value'])
        if type(row['revision']) is not int or row['revision'] != 0:
            raise ValueError('notification identity changed')
        return row['value']

    def _enqueue_completion(self, connection, *, semantic_key, job_id, collection_id, generation, count, provenance):
        """Only the publication/round transaction owns completion production."""
        if not connection.in_transaction:
            raise ValueError('notification producer requires owner transaction')
        store_uuid = self._notification_store_uuid(connection, create=True)
        encoded = json.dumps(provenance, separators=(',', ':'), sort_keys=True, allow_nan=False)
        old = connection.execute('SELECT * FROM notification_outbox WHERE semantic_key=?', (semantic_key,)).fetchone()
        creation = time.time_ns() // 1_000_000 if old is None else old['created_at_ms']
        values = (store_uuid, semantic_key, 'completion', job_id, collection_id,
            generation, creation, count, None, encoded)
        fields = 'store_uuid semantic_key type job_id collection_id generation created_at_ms count reason_code provenance'.split()
        if old is not None:
            if tuple(old[key] for key in fields) != values:
                raise ValueError('notification immutable payload changed')
            return
        connection.execute('INSERT INTO notification_outbox (' + ','.join(fields) + ') VALUES (?,?,?,?,?,?,?,?,?,?)', values)

    def _enqueue_standalone_completion(self, connection, job_id):
        if self._capture_target_head(connection, job_id) is not None:
            return
        fact = self._notification_completion_fact(connection, job_id)
        if fact is None:
            raise ValueError('notification completion not corroborated')
        self._enqueue_completion(connection, semantic_key=f"job:{job_id}:{fact['generation']}:{fact['audit_id']}",
            job_id=job_id, collection_id=self.get_materialized_job(job_id).queue_collection_id,
            generation=fact['generation'], count=1, provenance=dict(kind='job', completion=fact))

    def apply_target_authorize(self, command: TargetAuthorizeCommand, *, _queue_transaction=False) -> TargetAuthorizeResult:
        """Seal explicit metadata intent; no execution or filesystem entry point."""
        if type(command) is not TargetAuthorizeCommand: raise ValueError('target_request_invalid')
        connection = self._connection; cache = {}
        if _queue_transaction and not connection.in_transaction:raise ValueError('queue transaction missing')
        def commit():
            if _queue_transaction:connection.execute('RELEASE queue_saved_resume')
            else:connection.commit()
        def rollback():
            if _queue_transaction:
                connection.execute('ROLLBACK TO queue_saved_resume');connection.execute('RELEASE queue_saved_resume')
            else:connection.rollback()
        try:
            with self._batch_budget() as deadline:
                connection.execute('SAVEPOINT queue_saved_resume' if _queue_transaction else 'BEGIN IMMEDIATE')
                try:
                    try:
                        replay = self._match_command_receipt(connection,request_id=command.request_id,
                            payload_digest=command.payload_digest,scope='target_authorize',action='target_authorize')
                    except RuntimeError:
                        if self._read_direct_dispatch_command(connection,command.request_id) is not None:
                            raise RequestConflictError('request_id already belongs to direct dispatch') from None
                        raise
                    if replay:
                        _,result,_ = self._read_target_command(connection,command.request_id,cache)
                        rollback()
                        return TargetAuthorizeResult.from_record({**result.to_record(),'replayed':True})
                    if command.expected_worker_epoch != self._current_worker_epoch(connection): raise ValueError('target_epoch_stale')
                    selector = command.to_record()['selector']
                    if selector['kind'] == 'jobs':
                        selection = [(t['job'],t['expected_revision']) for t in selector['targets']]
                    elif selector['creation']['kind'] == 'batch':
                        parent = selector['creation']['request_id']
                        if connection.execute('SELECT 1 FROM add_batch_commands WHERE request_id=?',(parent,)).fetchone() is None: raise ValueError('target_request_invalid')
                        result,intents = self._read_batch(parent,cache); cache[parent] = (result,intents)
                        accepted = {c.index: c.original_job.job_id for c in intents.values()}
                        indices = list(accepted) if selector['indices'] is None else selector['indices']
                        if not indices or any(i not in accepted for i in indices): raise ValueError('target_request_invalid')
                        selection = [(accepted[i],None) for i in indices]
                    else:
                        row = connection.execute('SELECT job_id FROM commands WHERE request_id=?', (selector['creation']['request_id'],)).fetchone()
                        if row is None: raise ValueError('target_request_invalid')
                        selection = [(row[0],None)]
                    now = datetime.now(UTC); captured_at = self._utc_microseconds(now)
                    members = []; captures = []; new_groups = {}
                    for index,(job_id,expected_revision) in enumerate(selection):
                        origin = self._read_creation_origin(connection,job_id,cache)
                        member = dict.fromkeys(_TARGET_MEMBER_COLUMNS)
                        member.update(request_id=command.request_id,target_index=index,job_id=job_id,outcome='blocked',reason='unknown_job')
                        head = None; current = None; preserved = None; wanted = None
                        if origin.kind != 'unknown':
                            if origin.job is not None:
                                current = self._read_job_control_projection(connection,job_id)
                                latest = self._target_latest(connection,job_id)
                                member.update(captured_audit_id=latest['event_id'],captured_generation=current.generation,
                                    captured_revision=current.revision,captured_state=current.state,captured_authorized=int(current.authorized),
                                    captured_manual_hold=int(current.manual_hold),captured_start_now_requested=int(current.start_now_requested))
                            member.update(self._target_creation_values(origin))
                            if current is None: member['reason'] = 'incompatible_authority'
                            elif origin.job.source_kind is not SourceKind.DIRECT: member['reason'] = 'legacy_kind'
                            else:
                                head = self._read_target_head(connection,job_id,origin,cache)
                                if expected_revision is not None and expected_revision != current.revision:
                                    member.update(outcome='stale',reason='stale_revision')
                                elif (current.state=='completed' and command.action=='resume' and head is not None
                                    and len(selection)==1 and selector['kind']=='jobs'):
                                    claim=self._read_direct_cleanup_claim(connection,job_id,{})
                                    if claim is None or claim.phase=='finished': member['reason']='terminal'
                                    else:
                                        wanted=current
                                        member.update(outcome='existing_authority',reason=None,round_generation=head.round_generation,
                                            owner_request_id=head.owner_request_id,owner_target_index=head.owner_target_index)
                                elif current.state in _TERMINAL_JOB_CONTROL_STATES: member['reason'] = 'terminal'
                                elif origin.kind == 'unsupported_single': member['reason'] = 'incompatible_authority'
                                elif command.action == 'resume' and head is None: member['reason'] = 'incompatible_authority'
                                else:
                                    attempt = connection.execute("SELECT attempt_id FROM direct_publication_attempts WHERE job_id=? AND status='eligible'", (job_id,)).fetchone()
                                    if attempt is not None:
                                        if not self._publication_predecessor_matches(connection,current): raise ValueError('target publication predecessor changed')
                                        if current.state == 'paused': preserved = attempt[0]
                                    saved_ok = (command.action not in {'resume','start_now'} or preserved is not None or (head is None and current.state=='queued')
                                        or (head is not None and (head.last_admission_serial==0 or (len(selection)==1 and selector['kind']=='jobs'))
                                            and self._saved_resume_eligible(connection,origin,current,head,command)))
                                    wanted = self._target_transform(command.action,current,preserved is not None)
                                    if not saved_ok: member['reason'] = 'incompatible_authority'
                                    elif current.state not in {'queued','paused','retry_wait','blocked','needs_auth','needs_link'} and (head is None or wanted != current): member['reason'] = 'busy'
                                    elif current.state not in {'queued','paused','retry_wait','blocked','needs_auth','needs_link',*_ACTIVE_JOB_CONTROL_STATES}: member['reason'] = 'busy'
                                    else:
                                        member.update(outcome='existing_authority' if head else 'new_authority',reason=None,
                                            preserved_publication_attempt_id=preserved)
                                        if head:
                                            member.update(round_generation=head.round_generation,owner_request_id=head.owner_request_id,owner_target_index=head.owner_target_index)
                                        else:
                                            member.update(owner_request_id=command.request_id,owner_target_index=index)
                                            cohort = (member['creation_kind'],member['creation_request_id'])
                                            new_groups.setdefault(cohort,[]).append(member)
                        if current is not None:
                            after = wanted if member['outcome'] in {'new_authority','existing_authority'} else current
                            due = (after.start_now_requested or origin.job.scheduled_for is None or origin.job.scheduled_for <= now) and not self._retry_not_due(connection,job_id,now)
                            collection = origin.job.queue_collection_id
                            held = (int(self._current_queue_gate(connection) != 'running')
                                | (2 if collection is not None and self.collection_hold(collection) else 0)
                                | (4 if after.manual_hold else 0) | (8 if not due else 0))
                            member['held_mask'] = held
                        members.append(member); captures.append((origin,current,head,wanted))
                    for cohort,group in new_groups.items():
                        counter = self._target_counter(connection,cohort,cache)
                        if counter == _MAX_COUNTER: raise OverflowError('cohort round overflow')
                        name = 'authorization_round:' + hashlib.sha256(_batch_canonical(dict(kind=cohort[0],request_id=cohort[1]),512)).hexdigest()
                        if counter == 0: connection.execute('INSERT INTO settings VALUES (?,?,?)',(name,'1',1))
                        else:
                            connection.execute('UPDATE settings SET value=?,revision=? WHERE key=? AND value=? AND revision=?', (str(counter+1),counter+1,name,str(counter),counter))
                            self._require_one_changed_row(connection,'target cohort CAS')
                        cache[('counter',*cohort)] = counter + 1
                        for member in group: member['round_generation'] = counter + 1
                        connection.execute('INSERT INTO authorization_rounds VALUES (?,?,?,?,?,?,?)', (*cohort,counter+1,command.request_id,len(group),
                            _target_vector_digest(group,('request_id','target_index','job_id')),'open'))
                    for member,(origin,current,head,wanted) in zip(members,captures):
                        if member['outcome'] not in {'new_authority','existing_authority'}: continue
                        if current.state=='completed': continue
                        if wanted != current:
                            if current.revision == _MAX_COUNTER: raise OverflowError('target revision overflow')
                            wanted = replace(wanted,revision=current.revision+1)
                            connection.execute('UPDATE jobs SET revision=?,state=? WHERE job_id=? AND generation=? AND revision=? AND state=?',
                                (wanted.revision,wanted.state,current.job,current.generation,current.revision,current.state))
                            self._require_one_changed_row(connection,'target projection CAS')
                            connection.execute('UPDATE materialized_jobs SET authorized=?,manual_hold=?,start_now_requested=? WHERE job_id=?',
                                (int(wanted.authorized),int(wanted.manual_hold),int(wanted.start_now_requested),current.job))
                            self._require_one_changed_row(connection,'target flags')
                            audit = connection.execute("INSERT INTO events(kind,job_id,generation,revision) VALUES ('job_target_authorized',?,?,?)", (current.job,wanted.generation,wanted.revision)).lastrowid
                            member.update(intent_audit_id=audit,intent_generation=wanted.generation,intent_revision=wanted.revision,
                                intent_state=wanted.state,intent_authorized=int(wanted.authorized),intent_manual_hold=int(wanted.manual_hold),intent_start_now_requested=int(wanted.start_now_requested))
                            self._advance_publication_pointer(connection,current,preserve=member['preserved_publication_attempt_id'] is not None)
                        else: audit = member['captured_audit_id']
                        if head is None:
                            connection.execute('INSERT INTO job_authorization_heads VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                (current.job,command.request_id,member['target_index'],command.request_id,member['creation_kind'],member['creation_request_id'],
                                member['round_generation'],audit,wanted.generation,wanted.revision,wanted.state,command.expected_worker_epoch,'current',0))
                        else:
                            connection.execute("""UPDATE job_authorization_heads SET activation_request_id=?,current_audit_id=?,current_generation=?,current_revision=?,current_state=?,intent_status='current'
                                WHERE job_id=? AND owner_request_id=? AND owner_target_index=? AND activation_request_id=?
                                AND cohort_kind=? AND cohort_request_id=? AND round_generation=? AND current_audit_id=?
                                AND current_generation=? AND current_revision=? AND current_state=? AND current_worker_epoch=?
                                AND intent_status=? AND last_admission_serial=?""",
                                (command.request_id,audit,wanted.generation,wanted.revision,wanted.state,current.job,
                                 head.owner_request_id,head.owner_target_index,head.activation_request_id,head.cohort_kind,head.cohort_request_id,head.round_generation,
                                 head.current_audit_id,head.current_generation,head.current_revision,head.current_state,head.current_worker_epoch,head.intent_status,head.last_admission_serial))
                            self._require_one_changed_row(connection,'target activation CAS')
                    result = TargetAuthorizeResult(command.request_id,command.expected_worker_epoch,False,[_target_result(m) for m in members])
                    self._insert_command_receipt(connection,request_id=command.request_id,payload_digest=command.payload_digest,scope='target_authorize',action='target_authorize')
                    connection.execute('INSERT INTO target_commands VALUES (?,?,?,?,?,?,?,?,?)',
                        (command.request_id,command.payload_digest,command.action,command.expected_worker_epoch,captured_at,len(members),command._wire_request,result._wire_reply,_target_vector_digest(members)))
                    placeholders = ','.join('?' for _ in _TARGET_MEMBER_COLUMNS)
                    connection.executemany(f'INSERT INTO target_members VALUES ({placeholders})', [tuple(m[n] for n in _TARGET_MEMBER_COLUMNS) for m in members])
                    self._read_target_command(connection,command.request_id,cache)
                    if command.action in {'resume','start_now'}:
                        for member in members:
                            if (member['outcome'] == 'existing_authority' and member['preserved_publication_attempt_id'] is None
                                and member['captured_state']!='completed'
                                and self._capture_target_head(connection,member['job_id']).last_admission_serial>0):
                                self._create_saved_continuation(connection,command,member['job_id'])
                            elif member['outcome']=='existing_authority' and member['captured_state']=='completed':
                                connection.execute('INSERT INTO saved_cleanup_requests VALUES (?,?,?,1)',(command.request_id,member['job_id'],command.expected_worker_epoch))
                    if time.monotonic() >= deadline: raise TimeoutError('target deadline')
                    commit()
                    return result
                except BaseException:
                    rollback(); raise
        except RequestConflictError: raise
        except (TimeoutError,sqlite3.OperationalError) as error:
            if isinstance(error,TimeoutError) or 'interrupt' in str(error) or 'locked' in str(error): raise RuntimeError('target_deadline') from None
            raise RuntimeError('target_authority_corrupt') from None
        except Exception as error:
            if str(error) in {'target_epoch_stale','target_request_invalid','unsupported_selection_size'}: raise RuntimeError(str(error)) from None
            raise RuntimeError('target_authority_corrupt') from None

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

    def _batch_dispatch_corroborated(self, job_id, generation, revision, kind, cache=None):
        connection = self._connection
        cursor = connection.execute('SELECT request_id FROM direct_dispatch_commands WHERE job_id=? AND generation=? AND revision BETWEEN ? AND ?',
            (job_id, generation, revision, revision + (2 if kind == 'job_resolving' else 1)))
        for row in cursor:
            receipt = self._read_direct_dispatch_command(connection, row[0])
            self._read_target_dispatch_cause(connection,row[0],{} if cache is None else cache)
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
        if event['kind'] == 'job_target_authorized':
            member = self._target_audit_member(connection,row['audit_id'],{})
            command,_,_ = self._read_target_command(connection,member['request_id'],{})
            return (row['state'] == 'queued' and command.action in {'resume','start_now'}
                and member['preserved_publication_attempt_id'] is None and member['captured_state']=='paused'
                and (member['job_id'],member['intent_generation'],member['intent_revision'],member['intent_state'])
                    == (row['job_id'],row['generation'],row['revision'],row['state'])
                and member['immutable_identity_digest'] == _batch_decode(row['proof'].encode('utf-8'))['ownership'])
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

    def _batch_publication_record(self, row, captured, *, archived, cache):
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
        self._read_target_dispatch_cause(connection,proof['request'],cache)
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
            audit_kind = 'job_'+state
            if latest is not None and latest['kind']=='job_target_authorized' and state=='paused':
                self._target_publication_audit(connection,row)
                audit_kind = 'job_target_authorized'
            if latest is None or tuple(latest) != (audit,generation,revision,audit_kind) or (current.generation,current.revision,current.state) != (generation,revision,state):
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

    def _validate_batch_lifecycle(self, captured, current, parent_cache=None):
        if parent_cache is None: parent_cache = {}
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
            elif kind=='job_resume_blocked' and next_generation==generation:
                next_state=self._saved_decision_for_audit(connection,job_id,generation,next_revision);successors=flags
            elif kind=='job_retry_opened' and next_generation==generation+1:
                reopened=self._retry_reopen(connection,audit)
                if (reopened['old_generation'],reopened['old_revision'])!=(generation,revision) or state not in {'blocked','paused'}:raise ValueError('retry lifecycle predecessor changed')
                next_state='paused';successors=flags
            elif next_generation != generation:
                raise ValueError('batch unsupported generation jump')
            elif kind == 'job_target_authorized':
                member = self._target_audit_member(connection,audit,parent_cache)
                origin = _TargetCreationOrigin('batch',captured.original_job,captured.reservation,
                    captured.parent_request_id,captured.parent_payload_digest,captured.index,captured.audit_id,
                    captured.original_job.intent.generation,captured.original_job.intent.revision,captured.seal,captured)
                if (any(member[n] != value for n,value in self._target_creation_values(origin).items())
                        or (member['captured_audit_id'],member['captured_generation'],member['captured_revision'],member['captured_state'])
                            != (last_audit,generation,revision,state)
                        or tuple(bool(member['captured_'+n]) for n in ('authorized','manual_hold','start_now_requested')) not in flags):
                    raise ValueError('batch target predecessor changed')
                next_state = member['intent_state']
                successors = {tuple(bool(member['intent_'+n]) for n in ('authorized','manual_hold','start_now_requested'))}
            elif kind=='job_edited':
                self._effective_settings(connection,current)
                next_state=state;successors=flags
            elif kind == 'job_failure':
                if state != 'downloading' or not direct_lineage: raise ValueError('failure predecessor changed')
                next_state = self._failure_state_for_audit(connection,job_id,generation,next_revision,audit)
                successors = flags
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
                if state != expected or not self._batch_dispatch_corroborated(job_id,generation,next_revision,kind,parent_cache):
                    raise ValueError('batch dispatch lifecycle invalid')
                next_state = 'resolving' if kind == 'job_resolving' else 'downloading'
                successors = flags; direct_lineage = True
            elif kind == 'job_finalizing':
                if state != 'downloading' or not direct_lineage: raise ValueError('batch finalizing predecessor invalid')
                receipts = connection.execute("SELECT request_id FROM direct_dispatch_commands WHERE job_id=? AND generation=? AND revision=? AND status='started' AND state='downloading'", (job_id,generation,revision)).fetchall()
                if not receipts: raise ValueError('batch finalizing dispatch missing')
                for receipt in receipts:
                    self._read_direct_dispatch_command(connection,receipt[0])
                    self._read_target_dispatch_cause(connection,receipt[0],parent_cache)
                finalizing_event = (audit,generation,next_revision)
                next_state = 'finalizing'; successors = flags
            elif kind == 'job_completed':
                if state not in {'finalizing','paused'} or finalizing_event is None:
                    raise ValueError('batch completion predecessor invalid')
                attempt = connection.execute('SELECT * FROM direct_publication_attempts WHERE job_id=?', (job_id,)).fetchone()
                if attempt is None or attempt['status'] != 'finished': raise ValueError('batch completed attempt missing')
                proof = self._batch_publication_record(attempt,captured,archived=False,cache=parent_cache)
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
            self._batch_publication_record(attempt,captured,archived=False,cache=parent_cache)
        # Qualify only named archived proofs associated with actual finalizing audits in this lineage.
        audits = connection.execute("SELECT event_id,generation,revision FROM events WHERE job_id=? AND kind='job_finalizing' ORDER BY event_id", (job_id,))
        for event in audits:
            receipts = connection.execute("SELECT request_id FROM direct_dispatch_commands WHERE job_id=? AND generation=? AND revision=? AND status='started' AND state='downloading'", (job_id,event['generation'],event['revision']-1))
            for receipt in receipts:
                archived = connection.execute('SELECT * FROM closed_direct_publication_attempts WHERE original_request_id=?', (receipt[0],)).fetchone()
                if archived is not None:
                    proof = self._batch_publication_record(archived,captured,archived=True,cache=parent_cache)
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
            receipt = self._connection.execute('SELECT gate,revision FROM queue_commands WHERE request_id=?', (request_id,)).fetchone()
            return receipt is not None and tuple(receipt)==self.queue_gate_snapshot() and receipt['gate']=='paused'
        return self.queue_gate_snapshot()[1] == expected_revision

    def cleanup_control_is_current(self, *, job_id, action, request_id, payload_digest, expected_revision):
        if self._match_command_receipt(self._connection,request_id=request_id,payload_digest=payload_digest,
                scope=_JOB_CONTROL_COMMAND_SCOPE,action=action):
            return False
        current = self._read_job_control_projection(self._connection,job_id)
        return current.revision == expected_revision and current.state == 'completed'

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
                if gate=='running':
                    captures=[]
                    for candidate in connection.execute("SELECT job_id FROM jobs WHERE state='paused' ORDER BY job_id").fetchall():
                        job_id=candidate[0]
                        head=self._capture_target_head(connection,job_id)
                        if head is None:continue
                        current=self._read_job_control_projection(connection,job_id)
                        job=self.get_materialized_job(job_id)
                        if job is None or job.source_kind is not SourceKind.DIRECT or not current.authorized or current.manual_hold:continue
                        if job.queue_collection_id is not None and connection.execute('SELECT 1 FROM collection_holds WHERE collection_id=?',(job.queue_collection_id,)).fetchone():continue
                        checkpoint=self._saved_checkpoint(connection,job_id)
                        if checkpoint is None:
                            if head.last_admission_serial!=0 or head.intent_status!='inactive':continue
                        elif checkpoint[0]['stop_kind'] not in {'queue','shutdown','cold','cold_absent'}:continue
                        budget=self.get_retry_budget(job_id)
                        if budget is not None and (budget.paused or budget.exhausted):continue
                        origin=self._read_creation_origin(connection,job_id,{},target=False)
                        if not self._saved_resume_eligible(connection,origin,current,head):
                            self._record_saved_decision(connection,job_id)
                            continue
                        captures.append((job_id,current.revision))
                    for job_id,revision in captures:
                        request='queue-resume:'+hashlib.sha256((request_id+':'+job_id).encode()).hexdigest()
                        command=TargetAuthorizeCommand(request,self._current_worker_epoch(connection),'resume',dict(kind='jobs',targets=[dict(job=job_id,expected_revision=revision)]))
                        self.apply_target_authorize(command,_queue_transaction=True)
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
                managed_control = action in {'resume','start_now'} and self._target_managed(connection,job_id,{})
                if action in {'pause','remove'}:
                    self._capture_target_head(connection,job_id)
                publication_predecessor = False if managed_control else self._publication_predecessor_matches(connection,current)
                if managed_control:
                    result = current.to_result('blocked')
                elif current.revision != expected_revision:
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
                if result.status == 'applied' and result.state in _TERMINAL_JOB_CONTROL_STATES:
                    self._close_target_round(connection,job_id)
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

    def _persist_job_control_mutation(
        self,
        connection: sqlite3.Connection,
        *,
        action: str,
        updated: _JobControlProjection,
    ) -> None:
        """Persist exactly one supported lifecycle/domain transition and audit."""

        SQLiteStore._require_mutable_source(connection, updated.job, direct_only=True)
        head = self._capture_target_head(connection,updated.job)
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
        self._advance_target_head(connection,head)

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

    @staticmethod
    def _target_dispatch_identity(kind, scalars):
        key = ([kind, scalars['owner_request_id'], scalars['owner_target_index'], scalars['admission_serial']]
            if kind == 'body' else [kind, scalars['activation_request_id'], scalars['job_id'],
                scalars['predecessor_audit_id'], scalars['attempt_id']])
        values = [kind, *(scalars[n] for n in ('job_id','owner_request_id','owner_target_index',
            'activation_request_id','admission_serial','predecessor_audit_id','attempt_id','original_body_request_id'))]
        request = 'target-dispatch:' + hashlib.sha256(b'hermes-downloads:target-dispatch-key:v1\0'
            + _batch_canonical(key,1024)).hexdigest()
        digest = hashlib.sha256(b'hermes-downloads:target-dispatch-cause:v1\0'
            + _batch_canonical(values,1024)).hexdigest()
        return request, digest

    def _read_target_dispatch_cause(self, connection, request_id, cache):
        """Static producer qualification; never infer authority from a current head."""
        key = ('dispatch-cause',request_id)
        if key in cache: return cache[key]
        receipt = self._read_direct_dispatch_command(connection,request_id)
        marker = connection.execute('SELECT producer_kind FROM direct_dispatch_commands WHERE request_id=?', (request_id,)).fetchone()
        row = connection.execute('SELECT * FROM target_dispatch_causes WHERE request_id=?', (request_id,)).fetchone()
        if marker is None:
            if row is not None: raise ValueError('target cause without producer')
            return None
        if marker[0] == 'legacy':
            if row is not None: raise ValueError('legacy producer has conflicting cause')
            return None
        if row is None or marker[0] not in {'target_body','target_publication'}:
            raise ValueError('private producer cause missing')
        cause = dict(row)
        kind = cause['kind']
        if marker[0] != 'target_' + str(kind) or kind not in {'body','publication'}:
            raise ValueError('private producer kind changed')
        for name in ('request_id','job_id','owner_request_id','activation_request_id'):
            _require_identifier(cause[name],name)
        for name in ('owner_target_index','predecessor_audit_id'):
            _require_counter(cause[name],name)
        if cause['owner_target_index'] > 499 or cause['predecessor_audit_id'] < 1:
            raise ValueError('private producer scalar invalid')
        if kind == 'body':
            for name in ('admission_serial','resolving_audit_id'):
                if _require_counter(cause[name],name) < 1: raise ValueError('private body counter invalid')
            if cause['admission_serial'] > 1:
                continuation = connection.execute('SELECT * FROM saved_direct_continuations WHERE body_request_id=? UNION ALL SELECT * FROM saved_direct_cycles WHERE body_request_id=?',(request_id,request_id)).fetchone()
                fresh=self._fresh_retry(connection,cause['job_id'],cause['activation_request_id'])
                if continuation is None:
                    if fresh is None or fresh['body_request_id']!=request_id: raise ValueError('retry BODY lineage changed')
                    source=connection.execute('SELECT admission_serial FROM target_dispatch_causes WHERE request_id=?',(fresh['source_request_id'],)).fetchone()
                    if source is None or cause['admission_serial']!=source[0]+1: raise ValueError('retry serial changed')
                else:
                    if fresh is not None or continuation['phase'] != 'admitted' or (continuation['job_id'],continuation['request_id']) != (cause['job_id'],cause['activation_request_id']):
                        raise ValueError('saved BODY lineage changed')
                    self._saved_continuation(connection,cause['job_id'],continuation['request_id'])
            if cause['attempt_id'] is not None or cause['original_body_request_id'] is not None:
                raise ValueError('private body null shape changed')
        else:
            if cause['admission_serial'] is not None or cause['resolving_audit_id'] is not None:
                raise ValueError('private publication null shape changed')
            for name in ('attempt_id','original_body_request_id'): _require_identifier(cause[name],name)
        request, digest = self._target_dispatch_identity(kind,cause)
        if receipt is None or request != request_id or receipt.payload_digest != digest or receipt.job != cause['job_id']:
            raise ValueError('private producer identity changed')
        if self._read_command_receipt(connection,request_id) is not None:
            raise ValueError('private producer overlaps registry')
        _,_,owners = self._read_target_command(connection,cause['owner_request_id'],cache)
        activation_command,_,members = self._read_target_command(connection,cause['activation_request_id'],cache)
        if cause['owner_target_index'] >= len(owners): raise ValueError('private owner missing')
        owner = owners[cause['owner_target_index']]
        selected = [m for m in members if m['job_id'] == cause['job_id']]
        if len(selected) != 1: raise ValueError('private activation missing')
        activation = selected[0]
        if (owner['outcome'] != 'new_authority' or owner['job_id'] != cause['job_id']
            or activation['outcome'] not in {'new_authority','existing_authority'}
            or (activation['owner_request_id'],activation['owner_target_index'],activation['round_generation']) !=
                (cause['owner_request_id'],cause['owner_target_index'],owner['round_generation'])
            or any(owner[n] != activation[n] for n in _TARGET_CREATION_COLUMNS)):
            raise ValueError('private original authority changed')
        prefix = 'intent' if activation['intent_audit_id'] is not None else 'captured'
        predecessor = connection.execute('SELECT event_id,kind,job_id,generation,revision FROM events WHERE event_id=?',
            (cause['predecessor_audit_id'],)).fetchone()
        if (predecessor is None or predecessor['job_id'] != cause['job_id']
            or predecessor['event_id'] < activation[prefix+'_audit_id']
            or predecessor['generation'] != activation[prefix+'_generation']):
            raise ValueError('private predecessor changed')
        if kind == 'body':
            if cause['admission_serial']>1 and continuation is not None and (continuation['generation'],continuation['revision'],continuation['worker_epoch']) != (
                predecessor['generation'],predecessor['revision'],activation_command.expected_worker_epoch):
                raise ValueError('saved BODY predecessor changed')
            successor = connection.execute('SELECT event_id,kind,generation,revision FROM events WHERE job_id=? AND event_id>? ORDER BY event_id LIMIT 1',
                (cause['job_id'],cause['predecessor_audit_id'])).fetchone()
            if successor is None or tuple(successor) != (cause['resolving_audit_id'],'job_resolving',predecessor['generation'],predecessor['revision']+1):
                raise ValueError('private resolving successor changed')
            delta = receipt.revision - successor['revision']
            expected = {('pending','resolving'):0, ('pending','downloading'):1,
                ('started','downloading'):1, ('blocked','paused'):None}.get((receipt.status,receipt.state),-1)
            if (receipt.generation != successor['generation'] or expected == -1
                or (expected is not None and delta != expected) or (expected is None and delta not in (1,2))):
                raise ValueError('private body receipt chronology changed')
            events = [tuple(e) for e in connection.execute('SELECT kind,revision FROM events WHERE job_id=? AND generation=? AND revision>? AND revision<=? ORDER BY event_id',
                (cause['job_id'],receipt.generation,successor['revision'],receipt.revision))]
            wanted = ([('job_downloading',successor['revision']+1)] if receipt.state == 'downloading'
                else ([('job_downloading',successor['revision']+1),('job_paused',successor['revision']+2)]
                    if delta == 2 else ([('job_paused',successor['revision']+1)] if delta == 1 else [])))
            if events != wanted: raise ValueError('private body receipt audits changed')
        else:
            attempts = connection.execute('SELECT * FROM direct_publication_attempts WHERE attempt_id=? UNION ALL SELECT * FROM closed_direct_publication_attempts WHERE attempt_id=?',
                (cause['attempt_id'],cause['attempt_id'])).fetchall()
            if len(attempts) != 1: raise ValueError('private named attempt missing')
            attempt = attempts[0]
            proof = _batch_decode(attempt['proof'].encode('utf-8'))
            original = self._read_direct_dispatch_command(connection,cause['original_body_request_id'])
            original_cause = self._read_target_dispatch_cause(connection,cause['original_body_request_id'],cache)
            if (attempt['job_id'] != cause['job_id'] or attempt['original_request_id'] != cause['original_body_request_id']
                or proof['request'] != cause['original_body_request_id']
                or original != _DirectDispatchCommand(proof['request'],proof['digest'],cause['job_id'],
                    'started',proof['generation'],proof['downloading_revision'],'downloading')
                or proof['ownership'] != owner['immutable_identity_digest']
                or proof['finalizing_audit'] > predecessor['event_id']):
                raise ValueError('private original publication producer changed')
            if original_cause is None:
                if activation['preserved_publication_attempt_id'] != cause['attempt_id']:
                    raise ValueError('legacy publication lacks sealed preserved attempt')
            elif (original_cause['kind'] != 'body'
                or any(original_cause[n] != cause[n] for n in ('job_id','owner_request_id','owner_target_index'))
                or proof['epoch'] != self._read_target_command(connection,original_cause['activation_request_id'],cache)[0].expected_worker_epoch):
                raise ValueError('private publication body cause changed')
            if (receipt.generation != predecessor['generation'] or receipt.revision < predecessor['revision']
                or (receipt.status == 'pending' and (receipt.revision,receipt.state) != (predecessor['revision'],'paused'))
                or (receipt.status == 'started' and receipt.state != 'completed')
                or receipt.status not in {'pending','blocked','stale','started'}):
                raise ValueError('private publication receipt chronology changed')
        cache[key] = cause
        return cause

    def _prepare_target_dispatch(self, *, expected_worker_epoch, owner_slot_ready, now, _saved_qualifications=None):
        """Select and commit one exact private operation before any engine exists."""
        _require_worker_epoch(expected_worker_epoch,'expected_worker_epoch')
        if type(owner_slot_ready) is not bool: raise TypeError('owner_slot_ready must be boolean')
        if type(now) is not datetime or now.tzinfo is None or now.utcoffset() is None:
            raise TypeError('now must be a timezone-aware datetime')
        connection = self._connection
        connection.execute('BEGIN IMMEDIATE')
        try:
            if owner_slot_ready and expected_worker_epoch==self._current_worker_epoch(connection):
                carrier=connection.execute('SELECT * FROM saved_cleanup_requests WHERE worker_epoch=? AND armed=1 ORDER BY request_id LIMIT 1',(expected_worker_epoch,)).fetchone()
                if carrier is not None:
                    command=self._qualify_cleanup_target(connection,carrier['request_id'],carrier['job_id'],expected_worker_epoch)
                    claim=self._read_direct_cleanup_claim(connection,carrier['job_id'],{})
                    connection.execute('UPDATE saved_cleanup_requests SET armed=0 WHERE request_id=? AND armed=1',(carrier['request_id'],))
                    self._require_one_changed_row(connection,'cleanup target consume')
                    result=self._direct_dispatch_result_from_current(self._read_job_control_projection(connection,carrier['job_id']),'blocked')
                    connection.commit()
                    return _DirectCleanupPlan(result,claim,expected_worker_epoch,command.request_id,command.payload_digest)
            if (not owner_slot_ready or expected_worker_epoch != self._current_worker_epoch(connection)
                or self._current_queue_gate(connection) != 'running'
                or self.configuration_snapshot()['desired']['global_limit_bps']=='0'
                or self.get_direct_engine_record() is not None or self.get_direct_engine_activation_fence() is not None):
                connection.commit(); return None
            if (connection.execute("SELECT 1 FROM jobs JOIN materialized_jobs USING(job_id) WHERE source_kind='direct' AND state='resolving' LIMIT 1").fetchone() is not None
                or connection.execute("SELECT count(*) FROM (SELECT job_id FROM direct_job_engines UNION SELECT job_id FROM direct_cleanup_claims WHERE phase!='finished')").fetchone()[0]>=int(self.configuration_snapshot()['desired']['concurrency'])):
                connection.commit(); return None
            cache = {}
            with self._batch_budget():
                candidates = list(connection.execute('''SELECT job.job_id FROM jobs AS job
                    JOIN job_authorization_heads AS head ON head.job_id=job.job_id
                    JOIN materialized_jobs AS domain ON domain.job_id=job.job_id
                    WHERE head.intent_status='current' AND job.state IN ('queued','paused')
                    ORDER BY domain.start_now_requested DESC,domain.priority DESC,domain.order_key,job.job_id'''))
                ordering={}
                for candidate in candidates:
                    item=self.get_materialized_job(candidate[0]);effective=self._effective_settings(connection,item)
                    ordering[candidate[0]]=(-int(item.start_now_requested),-effective['priority'],int(effective['order_key']),candidate[0])
                candidates.sort(key=lambda row:ordering[row[0]])
                for candidate in candidates:
                    job_id = candidate[0]
                    if connection.execute("SELECT 1 FROM direct_source_requests WHERE job_id=? AND status='pending' UNION ALL SELECT 1 FROM direct_edit_outcomes WHERE job_id=? AND status='pending'",(job_id,job_id)).fetchone():continue
                    origin = self._read_creation_origin(connection,job_id,cache)
                    head = self._read_target_head(connection,job_id,origin,cache)
                    if origin.kind not in {'supported_single','batch'}: raise ValueError('private creation invalid')
                    current = self._read_job_control_projection(connection,job_id)
                    materialized = self.get_materialized_job(job_id)
                    admission = self._direct_dispatch_admission(connection,materialized=materialized,now=now.astimezone(UTC))
                    owner = self._read_target_command(connection,head.owner_request_id,cache)[2][head.owner_target_index]
                    if self._target_round(connection,owner,cache)['status'] != 'open' or not admission.allowed: continue
                    scalars = dict(job_id=job_id,owner_request_id=head.owner_request_id,owner_target_index=head.owner_target_index,
                        activation_request_id=head.activation_request_id,admission_serial=None,
                        predecessor_audit_id=head.current_audit_id,resolving_audit_id=None,attempt_id=None,original_body_request_id=None)
                    if current.state == 'paused':
                        row = connection.execute('SELECT * FROM direct_publication_attempts WHERE job_id=?', (job_id,)).fetchone()
                        if row is None or row['status'] != 'eligible': continue
                        attempt = self._read_publication_attempt(connection,job_id)
                        activation = next(m for m in self._read_target_command(connection,head.activation_request_id,cache)[2] if m['job_id']==job_id)
                        producer = self._read_target_dispatch_cause(connection,row['original_request_id'],cache)
                        if activation['preserved_publication_attempt_id'] != attempt.attempt_id:
                            if (producer is None or producer['kind'] != 'body'
                                or (producer['owner_request_id'],producer['owner_target_index'],producer['admission_serial']) !=
                                    (head.owner_request_id,head.owner_target_index,head.last_admission_serial)):
                                raise ValueError('private paused publication provenance changed')
                        scalars.update(attempt_id=attempt.attempt_id,original_body_request_id=row['original_request_id'])
                        kind = 'publication'
                    else:
                        continuation = None
                        if head.last_admission_serial != 0:
                            continuation = self._saved_continuation(connection,job_id)
                            if continuation is None:
                                fresh=self._fresh_retry(connection,job_id,head.activation_request_id)
                                if fresh is None or fresh['body_request_id'] is not None or (fresh['worker_epoch'],fresh['generation'],fresh['revision']+1)!=(expected_worker_epoch,current.generation,current.revision): continue
                            else:
                                if (continuation.request_id,continuation.worker_epoch,continuation.generation,continuation.revision) != (head.activation_request_id,expected_worker_epoch,current.generation,current.revision): continue
                                phase = self._continuation_row(connection,job_id,continuation.request_id)['phase']
                                if phase == 'intent':
                                    connection.commit(); return continuation
                                if phase != 'ready': continue
                                drain=connection.execute('SELECT drained FROM saved_resume_bindings WHERE request_id=?',(continuation.request_id,)).fetchone()
                                if drain is None or drain[0]!=1:continue
                                self._require_ready_saved_pair(connection,job_id,continuation.request_id)
                                if origin.kind=='batch' and origin.batch.expected_sha256 is None:
                                    qualified=None if _saved_qualifications is None else _saved_qualifications.get(job_id)
                                    if qualified is None or qualified[:2]!=(continuation,self.effective_source(job_id)):
                                        connection.commit();return continuation
                        else:
                            continuation = None
                        scalars['admission_serial'] = head.last_admission_serial + 1
                        kind = 'body'
                    request,digest = self._target_dispatch_identity(kind,scalars)
                    if connection.execute("SELECT 1 FROM direct_dispatch_commands JOIN materialized_jobs USING(job_id) WHERE source_kind='direct' AND status='pending' AND request_id!=? LIMIT 1", (request,)).fetchone() is not None:
                        connection.commit(); return None
                    if self._read_command_receipt(connection,request) is not None: raise RequestConflictError('private dispatch overlaps registered ID')
                    replay = self._read_direct_dispatch_command(connection,request)
                    if replay is not None:
                        cause = self._read_target_dispatch_cause(connection,request,cache)
                        if cause != dict(request_id=request,kind=kind,**scalars): raise ValueError('private replay scalar mismatch')
                        result = (self._prepare_exact_publication_recovery(connection,current,request,digest,persist=False)
                            if kind=='publication' and replay.status=='pending' else replay.to_result())
                        if result is None: raise ValueError('private pending recovery changed')
                        connection.commit(); return result
                    self._reject_unregistered_legacy_receipt(connection,request)
                    if kind == 'body':
                        current = self._persist_direct_dispatch_lifecycle(connection,current=current,state='resolving',
                            event_kind='job_resolving',_qualified_head=head,_defer_target_head=True)
                        scalars['resolving_audit_id'] = self._target_latest(connection,job_id)['event_id']
                    self._insert_direct_dispatch_command(connection,request_id=request,payload_digest=digest,job=job_id,
                        status='pending',generation=current.generation,revision=current.revision,state=current.state,producer_kind='target_'+kind)
                    connection.execute('INSERT INTO target_dispatch_causes VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                        (request,kind,*(scalars[n] for n in ('job_id','owner_request_id','owner_target_index','activation_request_id',
                            'admission_serial','predecessor_audit_id','resolving_audit_id','attempt_id','original_body_request_id'))))
                    if kind == 'body' and scalars['admission_serial'] > 1 and continuation is None:
                        connection.execute('UPDATE direct_retry_fresh SET body_request_id=? WHERE request_id=? AND body_request_id IS NULL',(request,head.activation_request_id))
                        self._require_one_changed_row(connection,'retry BODY CAS')
                    if kind == 'body' and scalars['admission_serial'] > 1 and continuation is not None:
                        table = self._continuation_table(connection,continuation.request_id)
                        connection.execute(f"UPDATE {table} SET phase='admitted',body_request_id=? WHERE request_id=? AND phase='ready' AND body_request_id IS NULL",(request,continuation.request_id))
                        self._require_one_changed_row(connection,'saved BODY admission CAS')
                    self._read_target_dispatch_cause(connection,request,cache)
                    if kind == 'body':
                        self._advance_target_head(connection,head)
                        connection.execute('UPDATE job_authorization_heads SET last_admission_serial=? WHERE job_id=? AND last_admission_serial=?',
                            (scalars['admission_serial'],job_id,head.last_admission_serial))
                        self._require_one_changed_row(connection,'target admission serial CAS')
                        result = _DirectDispatchPlan(replace(materialized,intent=replace(materialized.intent,
                            generation=current.generation,revision=current.revision)),self._effective_reservation(connection,job_id),admission,
                            current.generation,current.revision,request,digest)
                    else:
                        connection.execute('UPDATE direct_publication_attempts SET pending_request_id=? WHERE job_id=? AND attempt_id=? AND pending_request_id IS NULL',
                            (request,job_id,scalars['attempt_id']))
                        self._require_one_changed_row(connection,'private publication pointer CAS')
                        result = self._prepare_exact_publication_recovery(connection,current,request,digest,persist=False)
                        if result is None: raise ValueError('private publication preparation failed')
                    connection.commit(); return result
            connection.commit(); return None
        except BaseException:
            connection.rollback(); raise

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
                if self._read_target_dispatch_cause(connection,request_id,{}) is not None:
                    connection.commit()
                    return replay.to_result() if replay.status != 'pending' else DirectDispatchResult(
                        'blocked',replay.job,replay.generation,replay.revision,replay.state)
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

            if (materialized.source_kind is SourceKind.DIRECT and current.state == 'completed'
                and expected_worker_epoch == current_epoch
                and (expected_generation,expected_revision) == (current.generation,current.revision)):
                result = self._direct_dispatch_result_from_current(current,'blocked')
                with self._batch_budget():
                    try:
                        claim = self._read_direct_cleanup_claim(connection,job_id,{})
                    except (TypeError,ValueError):
                        claim = None
                self._insert_direct_dispatch_command(connection,request_id=request_id,payload_digest=payload_digest,
                    job=job_id,status=result.status,generation=result.generation,revision=result.revision,state=result.state)
                connection.commit()
                if claim is not None and claim.phase in {'pending','certified'}:
                    return _DirectCleanupPlan(result,claim,current_epoch,request_id,payload_digest)
                return result

            if self._target_managed(connection,job_id,{}):
                result = self._direct_dispatch_result_from_current(current,'blocked')
            elif expected_worker_epoch != current_epoch:
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
            command = self._require_pending_direct_dispatch_command(connection, plan, _effect=True)
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
            command = self._require_pending_direct_dispatch_command(connection, plan, _effect=True)
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

    def _require_terminal_owner(self, connection, terminal):
        if type(terminal) is not _DirectTerminalPlan or type(terminal.dispatch) is not _DirectDispatchPlan:
            raise ValueError('terminal owner invalid')
        plan=terminal.dispatch;current=self._read_job_control_projection(connection,plan.job.job_id)
        command=self._read_direct_dispatch_command(connection,plan.request_id)
        cause=self._read_target_dispatch_cause(connection,plan.request_id,{})
        if cause is not None:
            head=self._capture_target_head(connection,current.job)
            if head.intent_status!='current' or head.last_admission_serial!=cause['admission_serial'] or not self._direct_dispatch_admission(connection,materialized=self.get_materialized_job(current.job),now=datetime.now(UTC)).allowed:
                raise ValueError('private terminal authority changed')
        if (type(terminal.record) is not DirectEngineRecord or type(terminal.marker) is not PublicationMarkerBinding
            or self._current_worker_epoch(connection)!=terminal.record.worker_epoch or self._engine_record_for_dispatch(plan)!=terminal.record
            or self.get_direct_engine_activation_fence() is not None or self._get_direct_engine_recovery_capability(terminal.record) is None
            or (terminal.capability is not None and self._get_direct_engine_recovery_capability(terminal.record)!=terminal.capability)
            or (current.generation,current.revision,current.state)!=(plan.generation,plan.revision,'downloading')
            or self.get_materialized_job(current.job)!=plan.job or plan.job.source_kind is not SourceKind.DIRECT
            or command!=_DirectDispatchCommand(plan.request_id,plan.payload_digest,current.job,'started',plan.generation,plan.revision,'downloading')
            or self._effective_reservation(connection,current.job)!=plan.reservation
            or self._read_publication_marker_binding(connection,current.job)!=terminal.marker
            or not isinstance(terminal.partial_path,Path) or terminal.partial_path.name!=plan.job.partial_filename
            or terminal.partial_path.parent.name!=current.job
            or type(terminal.gid) is not str or re.fullmatch('[0-9a-fA-F]{16}',terminal.gid) is None):
            raise ValueError('terminal owner stale')
        return current

    def bind_stock_owner(self, plan, record, gid, owner):
        expected=(plan.job.job_id,plan.generation,gid,record.worker_epoch,plan.request_id,
            hashlib.sha256(json.dumps(record.identity.to_record(),sort_keys=True,separators=(',',':')).encode()).hexdigest())
        if type(owner) is not tuple or len(owner)!=7 or owner[:6]!=expected or _SHA256_DIGEST.fullmatch(owner[6]) is None:
            raise ValueError('stock owner producer changed')
        connection=self._connection;connection.execute('BEGIN IMMEDIATE')
        try:
            self._require_pending_direct_dispatch_command(connection,plan,_effect=True)
            if self._engine_record_for_dispatch(plan)!=record or self.get_direct_engine_activation_fence() is not None:
                raise ValueError('stock owner process changed')
            connection.execute('INSERT INTO direct_stock_bindings VALUES (?,?)',(plan.request_id,json.dumps(owner,separators=(',',':'))))
            connection.commit()
        except BaseException:connection.rollback();raise

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
            current=self._require_terminal_owner(connection,terminal)
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
        cause = self._read_target_dispatch_cause(connection,dispatch.request_id,{})
        if cause is not None:
            with self._batch_budget():
                head = self._capture_target_head(connection,dispatch.job.job_id)
            if (head.intent_status != 'current' or head.last_admission_serial != cause['admission_serial']
                or not self._direct_dispatch_admission(connection,materialized=self.get_materialized_job(dispatch.job.job_id),now=datetime.now(UTC)).allowed):
                raise ValueError('private stage authority changed')
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
            or self._engine_record_for_dispatch(dispatch) != terminal.record
            or self.get_direct_engine_activation_fence() is not None
            or terminal.capability != stage.capability
            or self._get_direct_engine_recovery_capability(terminal.record) != stage.capability
            or self._effective_reservation(connection, current.job) != dispatch.reservation
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
            destination = self.runtime_destination(job.job_id)
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
        self._read_target_dispatch_cause(connection,proof['request'],{})
        expected = _DirectDispatchCommand(proof['request'], proof['digest'], job_id, 'started',
            proof['generation'], proof['downloading_revision'], 'downloading')
        job = self.get_materialized_job(job_id)
        destination_link=connection.execute('SELECT request_id,job_id FROM direct_attempt_destinations WHERE attempt_id=?',(row['attempt_id'],)).fetchone()
        version=None if destination_link is None else self._destination_version(connection,job_id,destination_link[0])
        if destination_link is not None and (destination_link[1]!=job_id or version is None):raise ValueError('attempt destination changed')
        if destination_link is None and self._destination_version(connection,job_id) is not None:raise ValueError('attempt destination receipt missing')
        reservation=self._read_publication_reservation(connection,job_id) if version is None else self._effective_reservation(connection,job_id,version['request_id'])
        marker = self._read_publication_marker_binding(connection, job_id)
        staged = self._read_staged_payload_binding(connection, job_id)
        if (receipt != expected or job is None or job.source_kind is not SourceKind.DIRECT
            or reservation is None or marker is None or staged is None
            or self._publication_ownership(job,self._read_publication_reservation(connection,job_id)) != proof['ownership']
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
        destination = self.runtime_destination(job_id,None if version is None else version['request_id'])
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
            self._read_target_dispatch_cause(connection,pending,{})
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
            audit_kind = 'job_' + row['state']
            if latest is not None and latest['kind']=='job_target_authorized' and row['state']=='paused':
                self._target_publication_audit(connection,row)
                audit_kind = 'job_target_authorized'
            if (row['status'] != 'eligible' or latest is None
                or tuple(latest) != (*counters[:3], audit_kind)
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
            with self._batch_budget():
                self._qualify_publication_checksum(connection,stage.job.job_id,prepared.sha256,{})
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
                epoch=stage.terminal.record.worker_epoch,ownership=self._publication_ownership(stage.job,self._read_publication_reservation(connection,stage.job.job_id)),
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
            version=self._destination_version(connection,stage.job.job_id)
            if version is not None:
                attempt_id=connection.execute('SELECT attempt_id FROM direct_publication_attempts WHERE job_id=?',(stage.job.job_id,)).fetchone()[0]
                connection.execute('INSERT INTO direct_attempt_destinations VALUES (?,?,?)',(attempt_id,version['request_id'],stage.job.job_id))
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

    def _qualify_publication_checksum(self, connection, job_id, actual_sha256, cache):
        _require_payload_digest(actual_sha256)
        origin = self._read_creation_origin(connection,job_id,cache,target=False)
        if origin.kind == 'batch':
            expected = origin.batch.expected_sha256
        elif origin.kind == 'unsupported_single':
            expected = None
        else:
            raise ValueError('publication creation provenance invalid')
        if expected is not None:
            _require_payload_digest(expected)
            if expected != actual_sha256:
                raise _PublicationChecksumMismatch('publication checksum mismatch')
            return CompletionVerification.CHECKSUM_VERIFIED
        return CompletionVerification.TRANSPORT_VERIFIED

    def _read_direct_cleanup_claim(self, connection, job_id, cache):
        _require_identifier(job_id,'cleanup job')
        row = connection.execute('SELECT * FROM direct_cleanup_claims WHERE job_id=?',(job_id,)).fetchone()
        if row is None:
            return None
        if tuple(row.keys()) != _CLEANUP_COLUMNS:
            raise ValueError('cleanup columns invalid')
        for key in ('job_id','attempt_id'):
            _require_identifier(_require_sqlite_text(row[key],key),key)
        for key in ('completion_audit_id','completion_generation','completion_revision','completion_epoch'):
            _require_counter(_require_sqlite_integer(row[key],key),key)
        if row['completion_audit_id'] < 1 or row['completion_epoch'] < 1:
            raise ValueError('cleanup completion fence invalid')
        _require_payload_digest(row['proof_sha256'])
        published = _cleanup_stat(row['published_stat'],2)
        phase = row['phase']
        if type(phase) is not str or phase not in {'pending','certified','finished'}:
            raise ValueError('cleanup phase invalid')
        certified = None if row['certified_stat'] is None else _cleanup_stat(row['certified_stat'],1)
        if (phase == 'pending') != (certified is None):
            raise ValueError('cleanup certificate phase invalid')
        activation = row['activation_epoch']
        request = row['activation_request_id']
        epoch = self._current_worker_epoch(connection)
        if activation is not None:
            _require_worker_epoch(activation,'cleanup activation epoch')
            if not row['completion_epoch'] <= activation <= epoch:
                raise ValueError('cleanup activation epoch invalid')
        if phase != 'pending' and activation is None:
            raise ValueError('cleanup activation missing')
        if request is not None:
            _require_identifier(request,'cleanup activation request')
            receipt = self._read_direct_dispatch_command(connection,request)
            if receipt is None:
                self._qualify_cleanup_target(connection,request,job_id,activation)
            elif activation is None or (receipt.job,receipt.status,
                    receipt.generation,receipt.revision,receipt.state) != (job_id,'blocked',
                    row['completion_generation'],row['completion_revision'],'completed'):
                raise ValueError('cleanup activation receipt changed')
        if activation is not None and request is None and activation>row['completion_epoch']:
            target=connection.execute('SELECT request_id FROM saved_cleanup_activations WHERE job_id=? AND attempt_id=? AND worker_epoch=?',(job_id,row['attempt_id'],activation)).fetchone()
            if target is None:raise ValueError('cleanup target activation missing')
            self._qualify_cleanup_target(connection,target[0],job_id,activation)
        attempt = self._read_publication_attempt(connection,job_id,require_current=False)
        status = connection.execute('SELECT status FROM direct_publication_attempts WHERE job_id=?',(job_id,)).fetchone()
        current = self._read_job_control_projection(connection,job_id)
        latest = connection.execute('SELECT event_id,kind,generation,revision FROM events WHERE job_id=? ORDER BY event_id DESC LIMIT 1',(job_id,)).fetchone()
        if (row['job_id'] != job_id or attempt is None or status[0] != 'finished'
            or attempt.attempt_id != row['attempt_id']
            or hashlib.sha256(attempt.proof.encode('utf-8')).hexdigest() != row['proof_sha256']
            or (attempt.audit_id,attempt.generation,attempt.revision,attempt.state,attempt.worker_epoch) !=
                (row['completion_audit_id'],row['completion_generation'],row['completion_revision'],'completed',row['completion_epoch'])
            or (current.generation,current.revision,current.state) !=
                (row['completion_generation'],row['completion_revision'],'completed')
            or latest is None or tuple(latest) != (row['completion_audit_id'],'job_completed',
                row['completion_generation'],row['completion_revision']) or row['completion_epoch'] > epoch):
            raise ValueError('cleanup completed authority changed')
        binding = self._read_final_publication_binding(connection,job_id)
        stage = json.loads(attempt.proof)['stage']
        if (binding is None or published[:3] != (binding.final_device,binding.final_inode,binding.logical_size)
            or published[:4] != tuple(stage[:4]) or published[5] != stage[5]
            or certified is not None and (certified[:4] != published[:4] or certified[5] != published[5])):
            raise ValueError('cleanup payload binding changed')
        self._qualify_publication_checksum(connection,job_id,attempt.prepared.sha256,cache)
        self._capture_target_head(connection,job_id,cache)
        return _DirectCleanupClaim(row['job_id'],row['attempt_id'],row['completion_audit_id'],
            row['completion_generation'],row['completion_revision'],row['completion_epoch'],
            row['proof_sha256'],published,phase,activation,request,certified,attempt)

    def _mint_direct_cleanup_claim(self, connection, finished, published, verification, cache):
        from hermes_downloads.paths import require_current_publication_payload
        if type(verification) is not CompletionVerification:
            raise TypeError('cleanup verification invalid')
        cache.clear()
        if self._qualify_publication_checksum(connection,finished.job.job_id,published.sha256,cache) is not verification:
            raise ValueError('cleanup verification changed')
        require_current_publication_payload(finished.prepared,published)
        values = _published_cleanup_stat(published)
        encoded = json.dumps(list(values),separators=(',',':'))
        _cleanup_stat(encoded,2)
        connection.execute('INSERT INTO direct_cleanup_claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            (finished.job.job_id,finished.attempt_id,finished.audit_id,finished.generation,
             finished.revision,finished.worker_epoch,hashlib.sha256(finished.proof.encode('utf-8')).hexdigest(),
             encoded,'pending',None,None,None))
        self._require_one_changed_row(connection,'cleanup claim mint')
        claim = self._read_direct_cleanup_claim(connection,finished.job.job_id,cache)
        if claim is None or claim.attempt != finished or claim.published_stat != values:
            raise ValueError('cleanup claim mint readback changed')
        require_current_publication_payload(finished.prepared,published)

    def prepare_live_direct_cleanup(self, attempt, published):
        if type(attempt) is not _DirectPublicationAttempt:
            raise TypeError('cleanup live attempt invalid')
        connection = self._connection
        with self._batch_budget():
            connection.execute('BEGIN')
            try:
                claim = self._read_direct_cleanup_claim(connection,attempt.job.job_id,{})
                if claim is None or claim.phase != 'pending':
                    return None
                if (claim.attempt_id != attempt.attempt_id or claim.attempt.proof != attempt.proof
                    or claim.published_stat != _published_cleanup_stat(published)
                    or claim.completion_epoch != self._current_worker_epoch(connection)
                    or claim.activation_epoch is not None):
                    raise ValueError('cleanup live completion changed')
                result = self._direct_dispatch_result_from_current(
                    self._read_job_control_projection(connection,claim.job_id),'blocked')
                return _DirectCleanupPlan(result,claim,claim.completion_epoch,None,None)
            finally:
                connection.rollback()

    def _require_cleanup_plan(self, connection, plan, cache):
        if type(plan) is not _DirectCleanupPlan:
            raise TypeError('cleanup plan invalid')
        claim = self._read_direct_cleanup_claim(connection,plan.claim.job_id,cache)
        if (claim != plan.claim or self._current_worker_epoch(connection) != plan.worker_epoch
            or self.get_direct_engine_record() is not None or self.get_direct_engine_activation_fence() is not None
            or connection.execute('SELECT 1 FROM direct_job_engines WHERE job_id=?',(plan.claim.job_id,)).fetchone() is not None):
            raise ValueError('cleanup plan stale')
        if plan.request_id is None:
            if plan.payload_digest is not None or plan.worker_epoch != claim.completion_epoch:
                raise ValueError('cleanup automatic epoch stale')
        else:
            receipt = self._read_direct_dispatch_command(connection,plan.request_id)
            if receipt is None:
                command=self._qualify_cleanup_target(connection,plan.request_id,claim.job_id,plan.worker_epoch)
                if command.payload_digest!=plan.payload_digest or plan.download_result!=self._direct_dispatch_result_from_current(
                    self._read_job_control_projection(connection,claim.job_id),'blocked'):
                    raise ValueError('cleanup target receipt stale')
            elif (receipt.payload_digest != plan.payload_digest
                or receipt.to_result() != plan.download_result or receipt.status != 'blocked'
                or (receipt.job,receipt.generation,receipt.revision,receipt.state) !=
                    (claim.job_id,claim.completion_generation,claim.completion_revision,'completed')):
                raise ValueError('cleanup explicit receipt stale')
        return claim

    def _cas_direct_cleanup(self, connection, old, new, cache):
        connection.execute('UPDATE direct_cleanup_claims SET phase=?,activation_epoch=?,activation_request_id=?,certified_stat=? WHERE '+
            ' AND '.join(name+' IS ?' for name in _CLEANUP_COLUMNS),
            (new.phase,new.activation_epoch,new.activation_request_id,
             None if new.certified_stat is None else json.dumps(list(new.certified_stat),separators=(',',':')),
             *_cleanup_row_values(old)))
        self._require_one_changed_row(connection,'cleanup claim CAS')
        # This CAS changes no creation, target or lifecycle facts cached in this transaction.
        if self._read_direct_cleanup_claim(connection,old.job_id,cache) != new:
            raise ValueError('cleanup claim CAS readback changed')

    def activate_direct_cleanup(self, plan):
        from hermes_downloads.paths import require_direct_cleanup_namespace
        connection = self._connection
        with self._batch_budget():
            connection.execute('BEGIN IMMEDIATE')
            try:
                cache = {}; claim = self._require_cleanup_plan(connection,plan,cache)
                if claim.phase not in {'pending','certified'}:
                    raise ValueError('cleanup ownership not quiescent')
                evidence = claim.published_stat if claim.phase == 'pending' else claim.certified_stat
                require_direct_cleanup_namespace(claim.attempt.prepared,evidence,phase=claim.phase)
                request=plan.request_id
                if request is not None and self._read_direct_dispatch_command(connection,request) is None:
                    self._qualify_cleanup_target(connection,request,claim.job_id,plan.worker_epoch)
                    old=connection.execute('SELECT request_id FROM saved_cleanup_activations WHERE job_id=? AND attempt_id=? AND worker_epoch=?',(claim.job_id,claim.attempt_id,plan.worker_epoch)).fetchone()
                    if old is None:connection.execute('INSERT INTO saved_cleanup_activations VALUES (?,?,?,?)',(claim.job_id,claim.attempt_id,plan.worker_epoch,request))
                    elif old[0]!=request:raise ValueError('cleanup activation already owned')
                    request=None
                activated = replace(claim,activation_epoch=plan.worker_epoch,activation_request_id=request)
                self._cas_direct_cleanup(connection,claim,activated,cache)
                require_direct_cleanup_namespace(claim.attempt.prepared,evidence,phase=claim.phase)
                connection.commit()
                return replace(plan,claim=activated)
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _cleanup_activation_request(connection,claim):
        if claim.activation_request_id is not None:return claim.activation_request_id
        row=connection.execute('SELECT request_id FROM saved_cleanup_activations WHERE job_id=? AND attempt_id=? AND worker_epoch=?',(claim.job_id,claim.attempt_id,claim.activation_epoch)).fetchone()
        return None if row is None else row[0]

    def certify_direct_cleanup(self, plan, published):
        from hermes_downloads.paths import require_current_direct_cleanup_payload
        connection = self._connection
        with self._batch_budget():
            connection.execute('BEGIN IMMEDIATE')
            try:
                cache = {}; claim = self._require_cleanup_plan(connection,plan,cache)
                if (claim.phase != 'pending' or claim.activation_epoch != plan.worker_epoch
                    or self._cleanup_activation_request(connection,claim) != plan.request_id):
                    raise ValueError('cleanup certification not activated')
                require_current_direct_cleanup_payload(claim.attempt.prepared,published,marker_present=True)
                certified = replace(claim,phase='certified',certified_stat=_cleanup_stat(
                    json.dumps(list(_published_cleanup_stat(published)),separators=(',',':')),1))
                self._cas_direct_cleanup(connection,claim,certified,cache)
                require_current_direct_cleanup_payload(claim.attempt.prepared,published,marker_present=True)
                connection.commit()
                return replace(plan,claim=certified)
            except BaseException:
                connection.rollback()
                raise

    def finish_direct_cleanup(self, plan, published):
        from hermes_downloads.paths import require_current_direct_cleanup_payload
        connection = self._connection
        with self._batch_budget():
            connection.execute('BEGIN IMMEDIATE')
            try:
                cache = {}; claim = self._require_cleanup_plan(connection,plan,cache)
                if (claim.phase != 'certified' or claim.activation_epoch != plan.worker_epoch
                    or self._cleanup_activation_request(connection,claim) != plan.request_id
                    or _published_cleanup_stat(published) != claim.certified_stat):
                    raise ValueError('cleanup finish not certified')
                require_current_direct_cleanup_payload(claim.attempt.prepared,published,marker_present=False)
                from hermes_downloads.paths import _require_retained_absence
                aliases=self.retained_cleanup_records(plan)
                if any(row['phase']!='absent' for row in aliases): raise ValueError('retained cleanup not acknowledged')
                _require_retained_absence(claim.attempt.prepared,aliases,marker_present=False)
                finished = replace(claim,phase='finished')
                self._cas_direct_cleanup(connection,claim,finished,cache)
                require_current_direct_cleanup_payload(claim.attempt.prepared,published,marker_present=False)
                connection.commit()
                return replace(plan,claim=finished)
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
            original = json.loads(attempt.proof)['request']
            cause = self._read_target_dispatch_cause(connection,attempt.pending_request_id or original,{})
            if cause is not None:
                with self._batch_budget():
                    head = self._capture_target_head(connection,attempt.job.job_id)
                if (head.intent_status != 'current'
                    or (head.owner_request_id,head.owner_target_index) != (cause['owner_request_id'],cause['owner_target_index'])
                    or not self._direct_dispatch_admission(connection,materialized=attempt.job,now=datetime.now(UTC)).allowed):
                    raise ValueError('private publication completion authority changed')
            cache = {}
            with self._batch_budget():
                verification = self._qualify_publication_checksum(connection,attempt.job.job_id,
                    attempt.prepared.sha256,cache)
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
            self._close_target_round(connection,completed.job)
            self._enqueue_standalone_completion(connection,completed.job)
            with self._batch_budget():
                self._mint_direct_cleanup_claim(connection,finished,published,verification,cache)
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
        with self._batch_budget():
            self._qualify_publication_checksum(connection,current.job,attempt.prepared.sha256,{})
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
            self._close_target_round(connection,completed.job)
            self._enqueue_standalone_completion(connection,completed.job)
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

    def _resumable_failure_capture(self, connection, member):
        failure=connection.execute('SELECT * FROM direct_failures WHERE audit_id=?',(member['captured_audit_id'],)).fetchone()
        if failure is None or (failure['job_id'],failure['generation'],failure['revision'],failure['state'])!=(member['job_id'],member['captured_generation'],member['captured_revision'],member['captured_state']): return False
        return self._saved_checkpoint(connection,member['job_id'],failure['body_request_id']) is not None

    @staticmethod
    def _retry_not_due(connection, job_id, now):
        row=connection.execute("SELECT retry_due_ms FROM direct_failures WHERE body_request_id=(SELECT request_id FROM target_dispatch_causes WHERE job_id=? AND kind='body' ORDER BY admission_serial DESC LIMIT 1) AND action='retry_wait'",(job_id,)).fetchone()
        return row is not None and row[0] is not None and row[0]>SQLiteStore._utc_microseconds(now.astimezone(UTC))//1000

    def _fresh_retry(self, connection, job_id, request):
        row=connection.execute('SELECT * FROM direct_retry_fresh WHERE request_id=?',(request,)).fetchone()
        if row is None: return None
        failure=connection.execute('SELECT * FROM direct_failures WHERE body_request_id=?',(row['source_request_id'],)).fetchone()
        explicit=connection.execute('SELECT audit_id FROM direct_retry_reopens WHERE resume_request_id=?',(request,)).fetchone()
        if explicit is not None:
            reopened=self._retry_reopen(connection,explicit[0])
            if failure is None or failure['body_request_id']!=reopened['source_request_id'] or failure['action']!='exhausted' or (row['job_id'],row['worker_epoch'],row['generation'],row['revision'])!=(job_id,reopened['worker_epoch'],reopened['generation'],reopened['revision']):raise ValueError('explicit retry namespace changed')
            self._failure_state_for_audit(connection,job_id,failure['generation'],failure['revision'],failure['audit_id'])
        else:
            if (row['job_id']!=job_id or failure is None or
                (failure['job_id'],failure['worker_epoch'],failure['generation'],failure['revision'],failure['action'])!=(job_id,row['worker_epoch'],row['generation'],row['revision'],'retry_wait')
                or failure['retry_due_ms'] is None or row['authorized_at_ms']<failure['retry_due_ms']):raise ValueError('fresh retry failure fence changed')
            self._failure_state_for_audit(connection,job_id,row['generation'],row['revision'],failure['audit_id'])
        value=dict(row);seal=value.pop('seal');value.pop('body_request_id')
        if seal!=hashlib.sha256(b'hermes-downloads:fresh-retry:v1\0'+failure['seal'].encode()+_batch_canonical(value,16384)).hexdigest(): raise ValueError('retry namespace seal changed')
        chain=json.loads(row['chain']);marker=json.loads(row['marker'])
        if len(chain)!=4 or any(type(pair) is not list or len(pair)!=2 or any(type(x) is not int or x<0 for x in pair) for pair in chain) or len(marker)!=2 or any(type(x) is not int or x<0 for x in marker): raise ValueError('retry namespace proof changed')
        command=connection.execute('SELECT * FROM target_commands WHERE request_id=?',(request,)).fetchone()
        if command is not None:
            member=connection.execute('SELECT * FROM target_members WHERE request_id=? AND job_id=?',(request,job_id)).fetchall()
            if (command['action']!='resume' or command['worker_epoch']!=row['worker_epoch'] or command['payload_digest']!=row['payload_digest'] or len(member)!=1
                or (member[0]['captured_generation'],member[0]['captured_revision'],member[0]['captured_state'])!=(row['generation'],row['revision'],'paused' if explicit is not None else 'retry_wait')): raise ValueError('retry resume carrier changed')
        return row

    def _insert_fresh_retry(self, connection, command, failure, chain, marker, authorized_at_ms):
        if not connection.in_transaction: raise ValueError('retry intent requires owner transaction')
        value=dict(request_id=command.request_id,source_request_id=failure['body_request_id'],job_id=failure['job_id'],
            worker_epoch=command.expected_worker_epoch,generation=failure['generation'],revision=failure['revision'],
            authorized_at_ms=authorized_at_ms,chain=json.dumps(chain),marker=json.dumps(marker),payload_digest=command.payload_digest)
        seal=hashlib.sha256(b'hermes-downloads:fresh-retry:v1\0'+failure['seal'].encode()+_batch_canonical(value,16384)).hexdigest()
        fields=tuple(value)
        connection.execute('INSERT INTO direct_retry_fresh ('+','.join(fields)+',seal) VALUES (?,?,?,?,?,?,?,?,?,?,?)',(*value.values(),seal))

    def require_fresh_retry_absence(self, plan):
        from hermes_downloads.paths import _saved_open_chain
        cause=self._read_target_dispatch_cause(self._connection,plan.request_id,{})
        row=self._fresh_retry(self._connection,plan.job.job_id,cause['activation_request_id'])
        if row is None or row['body_request_id']!=plan.request_id: raise ValueError('retry absence carrier missing')
        destination=self._saved_destination(plan.job.job_id)
        descriptors,chain,marker=_saved_open_chain(destination,plan.reservation)
        try:
            if json.loads(row['chain'])!=[list(pair) for pair in chain] or json.loads(row['marker'])!=list(marker): raise ValueError('retry namespace rebound')
            for name in (destination.partial_path.name,destination.partial_path.name+'.aria2'):
                try: os.stat(name,dir_fd=descriptors[3],follow_symlinks=False)
                except FileNotFoundError: continue
                raise ValueError('retry requires exact payload absence')
        finally:
            for fd in reversed(descriptors): os.close(fd)

    def prepare_due_retry(self, *, now):
        now_ms=self._utc_microseconds(now.astimezone(UTC))//1000
        connection=self._connection
        rows=connection.execute("SELECT f.* FROM direct_failures f JOIN jobs j USING(job_id) WHERE j.state='retry_wait' AND f.generation=j.generation AND f.revision=j.revision AND f.retry_due_ms<=? ORDER BY f.retry_due_ms,f.job_id",(now_ms,)).fetchall()
        for failure in rows:
            job=failure['job_id'];epoch=self.worker_epoch()
            if failure['worker_epoch']!=epoch or self._latest_body_source(connection,job)!=failure['body_request_id']: continue
            head=self._capture_target_head(connection,job)
            if head is None or head.intent_status!='current': continue
            budget=self.get_retry_budget(job)
            if budget is None or budget.paused or budget.exhausted: continue
            if not self._direct_dispatch_admission(connection,materialized=self.get_materialized_job(job),now=now).allowed: continue
            request='retry:'+hashlib.sha256((failure['body_request_id']+':'+str(failure['audit_id'])).encode()).hexdigest()
            if connection.execute('SELECT 1 FROM target_commands WHERE request_id=?',(request,)).fetchone(): continue
            command=TargetAuthorizeCommand(request,epoch,'resume',dict(kind='jobs',targets=[dict(job=job,expected_revision=failure['revision'])]))
            if self._saved_checkpoint(connection,job,failure['body_request_id']) is not None:
                receipt=self.apply_target_authorize(command)
                if receipt.to_record()['results'][0]['outcome']=='blocked':self._block_saved_decision(job)
                return receipt
            from hermes_downloads.paths import _saved_open_chain
            destination=self._saved_destination(job);reservation=self._effective_reservation(connection,job)
            try:
                descriptors,chain,marker=_saved_open_chain(destination,reservation)
                try:
                    for name in (destination.partial_path.name,destination.partial_path.name+'.aria2'):
                        try: os.stat(name,dir_fd=descriptors[3],follow_symlinks=False)
                        except FileNotFoundError: continue
                        raise ValueError('saved bytes require checkpoint')
                finally:
                    for fd in reversed(descriptors):os.close(fd)
            except (OSError,ValueError):
                self._block_saved_decision(job)
                continue
            command=TargetAuthorizeCommand(request,epoch,'resume',dict(kind='jobs',targets=[dict(job=job,expected_revision=failure['revision'])]))
            connection.execute('BEGIN IMMEDIATE')
            try:
                old=connection.execute('SELECT * FROM direct_retry_fresh WHERE request_id=?',(request,)).fetchone()
                if old is None:
                    self._insert_fresh_retry(connection,command,failure,chain,marker,now_ms)
                self._fresh_retry(connection,job,request);connection.commit()
            except BaseException: connection.rollback();raise
            return self.apply_target_authorize(command)
        return None

    def _failure_state_for_audit(self, connection, job, generation, revision, audit):
        row=connection.execute('SELECT * FROM direct_failures WHERE audit_id=?',(audit,)).fetchone()
        if row is None or (row['job_id'],row['generation'],row['revision'])!=(job,generation,revision):
            raise ValueError('failure audit missing')
        receipt=connection.execute('SELECT * FROM direct_dispatch_commands WHERE request_id=?',(row['body_request_id'],)).fetchone()
        if receipt is None or (receipt['job_id'],receipt['generation'],receipt['revision'],receipt['state'],receipt['status'])!=(job,generation,revision-1,'downloading','started'):
            raise ValueError('failure BODY changed')
        value=dict(row);seal=value.pop('seal')
        if seal!=hashlib.sha256(b'hermes-downloads:typed-failure:v1\0'+_batch_canonical(value,32768)).hexdigest(): raise ValueError('failure immutable seal changed')
        observation=json.loads(row['observation']);closure=json.loads(row['closure'])
        if (type(observation) is not dict or observation.get('known') is not bool(row['observation_known'])
            or observation.get('http_status')!=row['http_status'] or row['error_code'] not in range(1,33)
            or closure.get('group_absent') is not True or closure.get('observer_joined') is not True
            or closure.get('reader_joined') is not True or closure.get('eof') is not True
            or type(closure.get('returncode')) is not int): raise ValueError('failure observation changed')
        ProcessBirthIdentity.from_record(closure['process'])
        binding=connection.execute('SELECT owner FROM direct_stock_bindings WHERE body_request_id=?',(row['body_request_id'],)).fetchone()
        if binding is None or json.loads(binding[0])!=observation.get('owner'): raise ValueError('failure binding changed')
        owner=tuple(observation['owner'])
        if owner[:6]!=(job,generation,closure['gid'],row['worker_epoch'],row['body_request_id'],hashlib.sha256(json.dumps(closure['process'],sort_keys=True,separators=(',',':')).encode()).hexdigest()): raise ValueError('failure origin changed')
        action=RetryAction(row['action'])
        expected={RetryAction.RETRY_WAIT:'retry_wait',RetryAction.OFFLINE_WAIT:'retry_wait',
            RetryAction.NEEDS_AUTH:'needs_auth',RetryAction.NEEDS_LINK:'needs_link'}.get(action,'blocked')
        cause=self._read_target_dispatch_cause(connection,row['body_request_id'],{})
        if row['error_code']==8 and action is RetryAction.NEEDS_DECISION and cause is not None and cause['admission_serial']>1:
            expected='paused'
        if expected!=row['state'] or (row['retry_due_ms'] is not None and
            (action not in {RetryAction.RETRY_WAIT,RetryAction.OFFLINE_WAIT} or row['retry_due_ms']<row['observed_at_ms'])):
            raise ValueError('failure disposition changed')
        return expected

    def _enqueue_actionable(self, connection, failure, *, kind, reason):
        if not connection.in_transaction: raise ValueError('actionable producer requires owner transaction')
        key='failure:'+failure['body_request_id']+':'+str(failure['audit_id'])
        provenance=json.dumps(dict(kind='failure',body_request_id=failure['body_request_id'],audit_id=failure['audit_id'],
            action=failure['action'],observation_sha256=hashlib.sha256(failure['observation'].encode()).hexdigest()),sort_keys=True,separators=(',',':'))
        values=(self._notification_store_uuid(connection,create=True),key,kind,failure['job_id'],
            self.get_materialized_job(failure['job_id']).queue_collection_id,failure['generation'],failure['observed_at_ms'],1,reason,provenance)
        fields='store_uuid semantic_key type job_id collection_id generation created_at_ms count reason_code provenance'.split()
        old=connection.execute('SELECT * FROM notification_outbox WHERE semantic_key=?',(key,)).fetchone()
        if old is not None:
            if tuple(old[k] for k in fields)!=values: raise ValueError('actionable immutable payload changed')
            return
        connection.execute('INSERT INTO notification_outbox ('+','.join(fields)+') VALUES (?,?,?,?,?,?,?,?,?,?)',values)

    def fail_direct_terminal(self, terminal, *, error_code, stock, closure, now):
        from hermes_downloads.direct import _StockResult
        if type(terminal) is not _DirectTerminalPlan or type(stock) is not _StockResult:
            raise ValueError('invalid typed terminal failure')
        if (type(error_code) is not int or error_code not in range(1,33) or now.tzinfo is None
            or closure.get('process')!=terminal.record.identity.to_record() or closure.get('gid')!=terminal.gid
            or any(closure.get(k) is not True for k in ('group_absent','observer_joined','reader_joined','eof'))
            or type(closure.get('returncode')) is not int): raise ValueError('terminal failure drain incomplete')
        connection=self._connection;connection.execute('BEGIN IMMEDIATE')
        try:
            plan=terminal.dispatch;current=self._require_terminal_owner(connection,terminal)
            binding=connection.execute('SELECT owner FROM direct_stock_bindings WHERE body_request_id=?',(plan.request_id,)).fetchone()
            expected=(current.job,current.generation,terminal.gid,terminal.record.worker_epoch,plan.request_id,
                hashlib.sha256(json.dumps(terminal.record.identity.to_record(),sort_keys=True,separators=(',',':')).encode()).hexdigest())
            if (binding is None or type(stock.owner) is not tuple or len(stock.owner)!=7 or stock.owner[:6]!=expected
                or tuple(json.loads(binding[0]))!=stock.owner): raise ValueError('terminal stock observation unbound')
            if connection.execute('SELECT 1 FROM direct_failures WHERE body_request_id=?',(plan.request_id,)).fetchone():
                raise ValueError('terminal failure already settled')
            if stock.known and stock.http_status is not None and stock.http_status>=400:
                failure=failure_from_http_status(stock.http_status,retry_after_seconds=stock.retry_after_seconds)
            elif error_code==9: failure=RetryFailure(FailureKind.DISK_FULL)
            elif error_code in {2,5,6,19,29}: failure=RetryFailure(FailureKind.TRANSIENT_HOST)
            elif error_code in {10,11,12,13,14,15,16,17,18,20,21,23,25,26,27,28,30,31}: failure=RetryFailure(FailureKind.PERMANENT_HOST)
            else: failure=RetryFailure(FailureKind.FORBIDDEN)
            budget=self.get_retry_budget(current.job)
            authority=RetryAuthority.open(policy=RetryPolicy(),job_id=current.job,generation=current.generation) if budget is None else RetryAuthority.restore(policy=RetryPolicy(),budget=budget)
            policy=RetryPolicy()
            jitter=policy._backoff_seconds(authority.budget.ordinary_attempts+1)*policy.max_jitter_fraction*(secrets.randbelow(1001)/1000)
            decision=authority.decide(failure,generation=current.generation,jitter_seconds=jitter)
            action=decision.action
            state={RetryAction.RETRY_WAIT:'retry_wait',RetryAction.OFFLINE_WAIT:'retry_wait',
                RetryAction.NEEDS_AUTH:'needs_auth',RetryAction.NEEDS_LINK:'needs_link'}.get(action,'blocked')
            cause=self._read_target_dispatch_cause(connection,plan.request_id,{})
            if error_code==8 and action is RetryAction.NEEDS_DECISION and cause is not None and cause['admission_serial']>1:
                state='paused'
            observed=self._utc_microseconds(now.astimezone(UTC))//1000
            due=None if decision.delay_seconds is None else observed+math.ceil(decision.delay_seconds*1000)
            if due is not None and due>_MAX_COUNTER:
                action=RetryAction.NEEDS_DECISION;state='blocked';due=None
            observation=json.dumps(dict(known=stock.known,http_status=stock.http_status,retry_after_seconds=stock.retry_after_seconds,
                resource_digest=stock.resource_digest,strong_validator=stock.strong_validator,total_length=stock.total_length,contributors=stock.contributors,owner=stock.owner),sort_keys=True,separators=(',',':'))
            head=self._capture_target_head(connection,current.job)
            updated=self._persist_direct_dispatch_lifecycle(connection,current=current,state=state,event_kind='job_failure',_qualified_head=head)
            audit=self._target_latest(connection,current.job)['event_id']
            encoded_budget=json.dumps(dict(generation=authority.budget.generation,budget_number=authority.budget.budget_number,
                ordinary_attempts=authority.budget.ordinary_attempts,exhausted=authority.budget.exhausted),sort_keys=True,separators=(',',':'))
            values=(plan.request_id,current.job,terminal.record.worker_epoch,current.generation,updated.revision,audit,stock.http_status,error_code,
                 int(stock.known),action.value,state,observed,due,observation,json.dumps(closure,sort_keys=True,separators=(',',':')),encoded_budget)
            fields='body_request_id job_id worker_epoch generation revision audit_id http_status error_code observation_known action state observed_at_ms retry_due_ms observation closure budget'.split()
            seal=hashlib.sha256(b'hermes-downloads:typed-failure:v1\0'+_batch_canonical(dict(zip(fields,values)),32768)).hexdigest()
            connection.execute('INSERT INTO direct_failures VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(*values,seal))
            self._replace_retry_budget(connection,authority.budget)
            fact=dict(connection.execute('SELECT * FROM direct_failures WHERE body_request_id=?',(plan.request_id,)).fetchone())
            event={RetryAction.NEEDS_AUTH:('needs_login','login_required'),RetryAction.NEEDS_LINK:('needs_link','link_expired'),
                RetryAction.EXHAUSTED:('persistent_error','retry_exhausted')}.get(action)
            if failure.kind is FailureKind.DISK_FULL:event=('needs_space','insufficient_space')
            elif failure.kind is FailureKind.PERMANENT_HOST:event=('persistent_error','download_failed')
            if event:self._enqueue_actionable(connection,fact,kind=event[0],reason=event[1])
            self._failure_state_for_audit(connection,current.job,current.generation,updated.revision,audit)
            connection.commit();return updated.to_result('applied')
        except BaseException:
            connection.rollback();raise

    def pause_active_direct_job(
        self, *, job_id: str, generation: int, revision: int, _publication_recoverable: bool = False, _filesystem_errno: int | None = None
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
            if _filesystem_errno is not None:
                import errno
                if type(_filesystem_errno) is not int or _filesystem_errno<1:raise ValueError('invalid filesystem errno')
                if _filesystem_errno in {errno.ENOSPC,errno.EDQUOT,errno.EACCES,errno.EPERM,errno.EROFS,errno.EIO}:
                    body=self._latest_body_source(connection,job_id)
                    if body is None:raise ValueError('filesystem failure BODY missing')
                    fact=dict(body_request_id=body,audit_id=self._target_latest(connection,job_id)['event_id'],action='filesystem_failure',
                        observation=json.dumps(dict(errno=_filesystem_errno),sort_keys=True,separators=(',',':')),job_id=job_id,generation=generation,observed_at_ms=time.time_ns()//1_000_000)
                    self._enqueue_actionable(connection,fact,kind='needs_space' if _filesystem_errno in {errno.ENOSPC,errno.EDQUOT} else 'persistent_error',
                        reason='insufficient_space' if _filesystem_errno in {errno.ENOSPC,errno.EDQUOT} else 'download_failed')
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
        effective=SQLiteStore._effective_settings(connection,materialized)
        scheduled=None if effective['scheduled_for'] is None else datetime.fromisoformat(effective['scheduled_for'])
        due = (materialized.start_now_requested or scheduled is None or scheduled<=now) and effective['limit_bps']!='0'
        due = due and not connection.execute("SELECT 1 FROM direct_file_outcomes JOIN direct_file_requests USING(request_id) WHERE job_id=? AND status IN ('pending','purge_authorized','saved_purge_authorized','purged') LIMIT 1",(materialized.job_id,)).fetchone()
        due = due and not SQLiteStore._retry_not_due(connection,materialized.job_id,now)
        return Admission(
            queue_running=SQLiteStore._current_queue_gate(connection) == "running",
            collection_held=collection_held,
            authorized=materialized.authorized,
            item_held=materialized.manual_hold,
            due=due,
        )

    def _persist_direct_dispatch_lifecycle(
        self,
        connection: sqlite3.Connection,
        *,
        current: _JobControlProjection,
        state: str,
        event_kind: str,
        _qualified_head=None,
        _defer_target_head=False,
    ) -> _JobControlProjection:
        SQLiteStore._require_mutable_source(connection, current.job, direct_only=True)
        head = _qualified_head if _qualified_head is not None else self._capture_target_head(connection,current.job)
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
        if not _defer_target_head:
            self._advance_target_head(connection,head)
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
        producer_kind: str = 'legacy',
    ) -> None:
        if status not in {"pending", *_DIRECT_DISPATCH_STATUSES}:
            raise ValueError("direct dispatch persistence status is invalid")
        connection.execute(
            """
            INSERT INTO direct_dispatch_commands (
                request_id, payload_digest, job_id, status, generation, revision, state, producer_kind
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (request_id, payload_digest, job, status, generation, revision, state, producer_kind),
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

    def _require_pending_direct_dispatch_command(
        self,
        connection: sqlite3.Connection,
        plan: _DirectDispatchPlan | _DirectPublicationReconciliationPlan,
        *, _effect=False,
    ) -> _DirectDispatchCommand:
        SQLiteStore._require_mutable_source(connection, plan.job.job_id, direct_only=True)
        command = SQLiteStore._read_direct_dispatch_command(connection, plan.request_id)
        cause = self._read_target_dispatch_cause(connection,plan.request_id,{})
        if cause is not None:
            with self._batch_budget():
                head = self._capture_target_head(connection,plan.job.job_id)
            if (head is None or head.intent_status != 'current'
                or (head.owner_request_id,head.owner_target_index,head.activation_request_id,head.last_admission_serial) !=
                    (cause['owner_request_id'],cause['owner_target_index'],cause['activation_request_id'],cause['admission_serial'])
                or (_effect and ((head.current_generation,head.current_revision,head.current_state) !=
                        (plan.generation,plan.revision,command.state)
                    or self.get_materialized_job(plan.job.job_id) != plan.job
                    or not self._direct_dispatch_admission(connection,materialized=self.get_materialized_job(plan.job.job_id),now=datetime.now(UTC)).allowed))):
                raise ValueError('private body effect authority changed')
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
            target_cache = {}
            target_heads = [self._capture_target_head(connection,row[0],target_cache,epoch=old_epoch)
                for row in connection.execute("SELECT job_id FROM job_authorization_heads UNION SELECT job_id FROM target_members WHERE outcome IN ('new_authority','existing_authority')").fetchall()]
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
                WHERE state IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                if job['state'] in {'blocked','needs_auth','needs_link'}:
                    failure=connection.execute('SELECT audit_id FROM direct_failures WHERE job_id=? AND generation=? AND revision=? AND state=?',(job_id,job['generation'],job['revision'],job['state'])).fetchone()
                    budget=self._read_retry_budget(connection,job_id)
                    if failure is None and budget is None:continue
                    if budget is not None and budget.generation!=job['generation']:raise ValueError('cold failure budget generation changed')
                    if failure is not None:self._failure_state_for_audit(connection,job_id,job['generation'],job['revision'],failure[0])
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
                audit = connection.execute(
                    "INSERT INTO events(kind,job_id,generation,revision) VALUES ('job_paused',?,?,?)",
                    (job_id,generation,revision)).lastrowid
                if self._latest_body_source(connection,job_id) is not None:
                    connection.execute('INSERT INTO saved_cold_fences VALUES (?,?,?,?,?,?,?,?,?)',
                        (audit,job_id,old_epoch,epoch,job['generation'],job['revision'],job['state'],generation,revision))
            for predecessor, matches in eligible_predecessors.values():
                self._advance_publication_pointer(connection,predecessor,preserve=matches,old_epoch=old_epoch)
            for head in target_heads:
                self._advance_target_head(connection,head,inactive=True)
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

    @staticmethod
    def _edit_request(connection, request):
        from hermes_downloads.ipc import EditCommand
        row=connection.execute('SELECT * FROM direct_edit_requests WHERE request_id=?',(request,)).fetchone()
        if row is None:raise ValueError('edit authority missing')
        command=EditCommand.from_record(_batch_decode(row['wire']))
        if command.request_id!=request or row['seal']!=hashlib.sha256(row['wire']+row['capture'].encode()+str(row['worker_epoch']).encode()+row['receipt']).hexdigest():raise ValueError('edit authority changed')
        if _batch_decode(row['receipt'])!=dict(request_id=request,status='pending',replayed=False,ids=list(command.ids)):raise ValueError('edit receipt changed')
        capture=json.loads(row['capture'])
        if type(capture) is not dict or set(capture)!=set(command.ids):raise ValueError('edit capture changed')
        return row,command,capture

    @staticmethod
    def _effective_settings(connection, job):
        current=dict(priority=job.priority,order_key=str(job.order_key),scheduled_for=None if job.scheduled_for is None else job.scheduled_for.astimezone(UTC).isoformat(),limit_bps=None,destination=dict(category=job.category,collection=job.destination_collection))
        prior=None
        for row in connection.execute('SELECT * FROM direct_job_edits WHERE job_id=? ORDER BY rowid',(job.job_id,)):
            _,command,capture=SQLiteStore._edit_request(connection,row['request_id'])
            values=[row[name] for name in ('request_id','job_id','generation','revision','audit_id','previous_revision','source_request_id','settings','prior_request_id')]
            if row['seal']!=hashlib.sha256(_batch_canonical(values,16384)).hexdigest() or row['prior_request_id']!=prior or row['job_id'] not in command.ids:raise ValueError('effective edit changed')
            patch=command.to_record()['patch']
            if 'scheduled_for' in patch and patch['scheduled_for'] is not None:patch['scheduled_for']=datetime.fromisoformat(patch['scheduled_for']).astimezone(UTC).isoformat()
            current=current|patch
            if json.loads(row['settings'])!=current:raise ValueError('edit projection changed')
            audit=connection.execute('SELECT kind,job_id,generation,revision FROM events WHERE event_id=?',(row['audit_id'],)).fetchone()
            if audit is None or tuple(audit)!=('job_edited',job.job_id,row['generation'],row['revision']) or row['revision']!=row['previous_revision']+1:raise ValueError('edit audit changed')
            outcome=connection.execute('SELECT status FROM direct_edit_outcomes WHERE request_id=? AND job_id=?',(row['request_id'],job.job_id)).fetchone()
            if outcome is None or outcome[0]!='applied':raise ValueError('edit outcome changed')
            prior=row['request_id']
        return current

    def accept_edit(self, command, *, deadline):
        connection=self._connection
        with self._batch_budget(deadline=deadline):
            connection.execute('BEGIN IMMEDIATE')
            try:
                if connection.execute('SELECT 1 FROM direct_edit_requests WHERE request_id=?',(command.request_id,)).fetchone():
                    row,old,_=self._edit_request(connection,command.request_id)
                    if old._wire_request!=command._wire_request:raise RequestConflictError('edit request conflict')
                    connection.rollback();return _batch_decode(row['receipt'])|dict(replayed=True)
                if self._read_command_receipt(connection,command.request_id) is not None:raise RequestConflictError('edit request conflict')
                capture={}
                for job in command.ids:
                    origin=self._read_creation_origin(connection,job,{})
                    current=self._read_job_control_projection(connection,job)
                    if origin.job is None or origin.job.source_kind is not SourceKind.DIRECT or current.state in _TERMINAL_JOB_CONTROL_STATES:raise ValueError('edit job unavailable')
                    if command.expected_revision is not None and current.revision!=command.expected_revision:raise ValueError('edit revision stale')
                    if connection.execute("SELECT 1 FROM direct_edit_outcomes WHERE job_id=? AND status='pending' UNION ALL SELECT 1 FROM direct_source_requests WHERE job_id=? AND status='pending'",(job,job)).fetchone():raise ValueError('job change pending')
                    capture[job]=dict(generation=current.generation,revision=current.revision,state=current.state)
                receipt=dict(request_id=command.request_id,status='pending',replayed=False,ids=list(command.ids))
                encoded=_batch_canonical(receipt,131072);captured=json.dumps(capture,sort_keys=True,separators=(',',':'));epoch=self.worker_epoch()
                seal=hashlib.sha256(command._wire_request+captured.encode()+str(epoch).encode()+encoded).hexdigest()
                connection.execute('INSERT INTO direct_edit_requests VALUES (?,?,?,?,?,?)',(command.request_id,epoch,command._wire_request,captured,encoded,seal))
                connection.executemany("INSERT INTO direct_edit_outcomes VALUES (?,?,'pending',NULL)",[(command.request_id,job) for job in command.ids])
                if time.monotonic()>=deadline:raise TimeoutError('edit deadline')
                connection.commit();return receipt
            except BaseException:connection.rollback();raise

    def apply_edit(self, request, job):
        connection=self._connection;row,command,capture=self._edit_request(connection,request)
        current=self._read_job_control_projection(connection,job);original=self.get_materialized_job(job)
        if current.state in {'resolving','downloading','finalizing'} or any(owner[0]==job for owner in self._job_engine_owners()):return
        old=capture[job];patch=command.to_record()['patch'];reason=None
        if row['worker_epoch']!=self.worker_epoch() or old['generation']!=current.generation or current.revision not in {old['revision'],old['revision']+1}:reason='stale_fence'
        source=self._latest_body_source(connection,job)
        if 'destination' in patch and source is not None and reason is None:
            from hermes_downloads.paths import _require_saved_pair
            origin=self._read_creation_origin(connection,job,{})
            checkpoint=self._saved_checkpoint(connection,job,source)
            try:
                if origin.kind!='batch' or origin.batch.expected_sha256 is None or checkpoint is None or self._checkpoint_fence(connection,checkpoint[0])!=(self.worker_epoch(),current.generation,current.revision):raise ValueError('saved destination authority unavailable')
                _require_saved_pair(self._saved_destination(job),self._effective_reservation(connection,job),checkpoint[1])
            except (OSError,ValueError):reason='saved_destination_requires_decision'
        settings=self._effective_settings(connection,original)|patch
        if settings['scheduled_for'] is not None:settings['scheduled_for']=datetime.fromisoformat(settings['scheduled_for']).astimezone(UTC).isoformat()
        head=self._capture_target_head(connection,job)
        connection.execute('BEGIN IMMEDIATE')
        try:
            if reason is None:
                updated=self._persist_direct_dispatch_lifecycle(connection,current=current,state=current.state,event_kind='job_edited',_qualified_head=head,_defer_target_head=True)
                audit=self._target_latest(connection,job)['event_id']
                prior=connection.execute('SELECT request_id FROM direct_job_edits WHERE job_id=? ORDER BY rowid DESC LIMIT 1',(job,)).fetchone()
                values=(request,job,updated.generation,updated.revision,audit,current.revision,source,json.dumps(settings,sort_keys=True,separators=(',',':')),None if prior is None else prior[0])
                connection.execute('INSERT INTO direct_job_edits VALUES (?,?,?,?,?,?,?,?,?,?)',(*values,hashlib.sha256(_batch_canonical(list(values),16384)).hexdigest()))
                if 'destination' in patch:
                    dest=patch['destination'];token=secrets.token_hex(32);authority=(request,job,dest['category'],dest['collection'],token)
                    connection.execute('INSERT INTO direct_destinations VALUES (?,?,?,?,?,?)',(*authority,hashlib.sha256(_batch_canonical(list(authority),4096)).hexdigest()))
                self._advance_target_head(connection,head)
            connection.execute("UPDATE direct_edit_outcomes SET status=?,reason=? WHERE request_id=? AND job_id=? AND status='pending'",('applied' if reason is None else 'blocked',reason,request,job))
            self._require_one_changed_row(connection,'edit outcome CAS');connection.commit()
        except BaseException:connection.rollback();raise
        if reason is None and source is not None and settings['limit_bps']!='0' and current.authorized and not current.manual_hold and head is not None and head.intent_status=='current':
            resume='edit-resume:'+hashlib.sha256((request+':'+job).encode()).hexdigest()
            self.apply_target_authorize(TargetAuthorizeCommand(resume,self.worker_epoch(),'resume',dict(kind='jobs',targets=[dict(job=job,expected_revision=updated.revision)])))

    @staticmethod
    def _destination_version(connection,job,request=None):
        row=connection.execute('SELECT * FROM direct_destinations WHERE job_id=?'+(' ORDER BY rowid DESC LIMIT 1' if request is None else ' AND request_id=?'),(job,) if request is None else (job,request)).fetchone()
        if row is None:return None
        values=[row[name] for name in ('request_id','job_id','category','collection','claim_token')]
        _,command,_=SQLiteStore._edit_request(connection,row['request_id'])
        if row['seal']!=hashlib.sha256(_batch_canonical(values,4096)).hexdigest() or command.to_record()['patch'].get('destination')!=dict(category=row['category'],collection=row['collection']):raise ValueError('destination authority changed')
        outcome=connection.execute('SELECT status FROM direct_edit_outcomes WHERE request_id=? AND job_id=?',(row['request_id'],job)).fetchone()
        if outcome is None or outcome[0]!='applied':raise ValueError('destination not applied')
        return row

    @staticmethod
    def _effective_reservation(connection,job,request=None):
        original=SQLiteStore._read_publication_reservation(connection,job)
        row=SQLiteStore._destination_version(connection,job,request)
        return original if row is None else PublicationReservation(job,row['collection'] or row['category'],original.final_filename,row['claim_token'])

    def runtime_destination(self,job,request=None):
        from hermes_downloads.paths import rehydrate_destination
        original=self.get_materialized_job(job);row=self._destination_version(self._connection,job,request)
        destination=rehydrate_destination(category=original.category if row is None else row['category'],collection=original.destination_collection if row is None else row['collection'],partial_filename=original.partial_filename,selected_final_filename=original.selected_final_filename,job_id=job)
        return destination if row is None else replace(destination,workspace_reservation=self._read_publication_reservation(self._connection,job))

    def _source_request(self, request):
        from hermes_downloads.ipc import ReplaceSourceCommand
        row=self._connection.execute('SELECT * FROM direct_source_requests WHERE request_id=?',(request,)).fetchone()
        if row is None:raise ValueError('source request missing')
        command=ReplaceSourceCommand.from_record(_batch_decode(row['wire']))
        capture=[row['job_id'],row['worker_epoch'],row['generation'],row['revision']]
        if (command.id,command.request_id)!=(row['job_id'],request) or row['seal']!=hashlib.sha256(command._wire_request+_batch_canonical(capture,4096)+row['receipt']).hexdigest():raise ValueError('source request changed')
        expected=dict(request_id=request,id=command.id,revision=str(row['revision']),status='pending',replayed=False)
        if _batch_decode(row['receipt'])!=expected or row['status'] not in {'pending','applied','blocked'}:raise ValueError('source receipt changed')
        return row,command

    def accept_source_replacement(self, command, *, deadline):
        connection=self._connection
        with self._batch_budget(deadline=deadline):
            connection.execute('BEGIN IMMEDIATE')
            try:
                if connection.execute('SELECT 1 FROM direct_source_requests WHERE request_id=?',(command.request_id,)).fetchone():
                    row,old=self._source_request(command.request_id)
                    if old._wire_request!=command._wire_request:raise RequestConflictError('source request conflict')
                    connection.rollback();return _batch_decode(row['receipt'])|dict(replayed=True)
                if self._read_command_receipt(connection,command.request_id) is not None:raise RequestConflictError('source request conflict')
                source=validate_source_url(command.url)
                if source.raw_url!=command.url.encode():raise ValueError('source bytes changed')
                origin=self._read_creation_origin(connection,command.id,{})
                current=self._read_job_control_projection(connection,command.id)
                if origin.job is None or origin.job.source_kind is not SourceKind.DIRECT or current.state in _TERMINAL_JOB_CONTROL_STATES:raise ValueError('source job unavailable')
                if command.expected_revision is not None and command.expected_revision!=current.revision:raise ValueError('source revision stale')
                if connection.execute("SELECT 1 FROM direct_source_requests WHERE job_id=? AND status='pending' UNION ALL SELECT 1 FROM direct_edit_outcomes WHERE job_id=? AND status='pending'",(command.id,command.id)).fetchone():raise ValueError('source change pending')
                receipt=dict(request_id=command.request_id,id=command.id,revision=str(current.revision),status='pending',replayed=False)
                encoded=_batch_canonical(receipt,4096);epoch=self.worker_epoch()
                capture=[command.id,epoch,current.generation,current.revision]
                seal=hashlib.sha256(command._wire_request+_batch_canonical(capture,4096)+encoded).hexdigest()
                connection.execute("INSERT INTO direct_source_requests VALUES (?,?,?,?,?,?,?,?,'pending')",(command.request_id,command.id,epoch,current.generation,current.revision,command._wire_request,encoded,seal))
                if time.monotonic()>=deadline:raise TimeoutError('source deadline')
                connection.commit();return receipt
            except BaseException:connection.rollback();raise

    def _control_request(self,request):
        from hermes_downloads.ipc import ControlIntentCommand
        connection=self._connection;row=connection.execute('SELECT * FROM direct_control_requests WHERE request_id=?',(request,)).fetchone()
        if row is None:raise ValueError('captured control missing')
        command=ControlIntentCommand.from_record(_batch_decode(row['wire']));receipt=_batch_decode(row['receipt'])
        if command.request_id!=request:raise ValueError('captured request identity changed')
        cohorts=connection.execute('SELECT * FROM direct_control_cohorts WHERE request_id=? ORDER BY cohort_index',(request,)).fetchall();total=0;digests=[]
        for index,cohort in enumerate(cohorts):
            members=json.loads(cohort['members']);digest=hashlib.sha256(cohort['members'].encode()).hexdigest()
            if cohort['cohort_index']!=index or not 1<=len(members)<=500 or digest!=cohort['digest'] or cohort['child_request_id']!='capture:'+hashlib.sha256(request.encode()).hexdigest()+':'+str(index):raise ValueError('captured cohort changed')
            captured=connection.execute('SELECT * FROM direct_control_members WHERE request_id=? AND cohort_index=? ORDER BY target_index',(request,index)).fetchall()
            if [member['target_index'] for member in captured]!=list(range(len(members))):raise ValueError('captured target indices changed')
            for member in members:
                if type(member) is not dict or set(member)!={'job_id','generation','revision'}:raise ValueError('captured member shape changed')
                _require_identifier(member['job_id'],'captured job');_require_counter(member['generation'],'captured generation');_require_counter(member['revision'],'captured revision')
            stored=[dict(job_id=m['job_id'],generation=m['generation'],revision=m['revision']) for m in captured]
            if stored!=members:raise ValueError('captured members changed')
            total+=len(members);digests.append(digest)
            if command.action=='start' and cohort['status']=='applied':
                child,original,_=self._read_target_command(connection,cohort['child_request_id'],{})
                if child._wire_request!=TargetAuthorizeCommand(cohort['child_request_id'],command.expected_worker_epoch,'start',dict(kind='jobs',targets=[dict(job=m['job_id'],expected_revision=m['revision']) for m in members]))._wire_request or original._wire_reply!=cohort['receipt']:raise ValueError('captured child receipt changed')
        digest=hashlib.sha256(_batch_canonical(digests,131072)).hexdigest()
        if row['membership_digest']!=digest or row['worker_epoch']!=command.expected_worker_epoch or row['seal']!=hashlib.sha256(row['wire']+row['receipt']+digest.encode()).hexdigest():raise ValueError('captured control seal changed')
        if receipt!=dict(request_id=request,status='pending',replayed=False,member_count=str(total),cohort_count=str(len(cohorts)),revision=receipt.get('revision')) or type(receipt['revision']) is not str or re.fullmatch(r'0|[1-9][0-9]{0,18}',receipt['revision']) is None:raise ValueError('captured control receipt changed')
        _require_counter(int(receipt['revision']),'captured revision')
        if row['status']=='applied' and command.action=='start' and any(c['status']!='applied' for c in cohorts):raise ValueError('captured control settlement changed')
        return row,command,cohorts

    def accept_control_intent(self,command,*,deadline):
        connection=self._connection
        with self._batch_budget(deadline=deadline):
            connection.execute('BEGIN IMMEDIATE')
            try:
                if connection.execute('SELECT 1 FROM direct_control_requests WHERE request_id=?',(command.request_id,)).fetchone():
                    row,old,_=self._control_request(command.request_id)
                    if old._wire_request!=command._wire_request:raise RequestConflictError('captured control conflict')
                    connection.rollback();return _batch_decode(row['receipt'])|dict(replayed=True)
                if self._read_command_receipt(connection,command.request_id) is not None:raise RequestConflictError('captured control conflict')
                if command.expected_worker_epoch!=self.worker_epoch():raise ValueError('control epoch stale')
                revision=0
                if command.scope=='collection' and not connection.execute('SELECT 1 FROM materialized_jobs WHERE queue_collection_id=?',(command.collection,)).fetchone():raise ValueError('unknown collection')
                if command.scope=='collection' and command.action in {'pause','resume'}:
                    key='collection_gate:'+hashlib.sha256(command.collection.encode()).hexdigest()
                    old=connection.execute('SELECT revision FROM settings WHERE key=?',(key,)).fetchone();revision=0 if old is None else old[0]
                    if command.expected_revision!=revision:raise ValueError('collection revision stale')
                    if connection.execute("SELECT 1 FROM direct_control_requests WHERE status='pending' AND wire!=?",(command._wire_request,)).fetchone():
                        for pending in connection.execute("SELECT request_id FROM direct_control_requests WHERE status='pending'"):
                            _,other,_=self._control_request(pending[0])
                            if other.scope=='collection' and other.collection==command.collection and other.action in {'pause','resume'}:raise ValueError('collection change pending')
                    revision+=1;_require_counter(revision,'collection revision')
                    connection.execute('INSERT INTO settings VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,revision=excluded.revision',(key,command.action,revision))
                    if command.action=='pause':connection.execute('INSERT OR IGNORE INTO collection_holds VALUES (?)',(command.collection,))
                    else:connection.execute('DELETE FROM collection_holds WHERE collection_id=?',(command.collection,))
                selected=connection.execute("SELECT jobs.job_id,jobs.generation,jobs.revision FROM jobs JOIN materialized_jobs USING(job_id) WHERE source_kind='direct' AND jobs.state NOT IN ('completed','removed','cancelled')"+(' AND queue_collection_id=?' if command.scope=='collection' else ' AND jobs.job_id=?' if command.scope=='job' else '')+' ORDER BY jobs.job_id', (command.collection,) if command.scope=='collection' else (command.job,) if command.scope=='job' else ()).fetchall()
                if command.scope=='job' and (len(selected)!=1 or selected[0]['revision']!=command.expected_revision):raise ValueError('job control fence stale')
                groups=[selected[index:index+500] for index in range(0,len(selected),500)];cohorts=[]
                for index,group in enumerate(groups):
                    members=[dict(job_id=m[0],generation=m[1],revision=m[2]) for m in group];encoded=json.dumps(members,sort_keys=True,separators=(',',':'));digest=hashlib.sha256(encoded.encode()).hexdigest()
                    cohorts.append((index,'capture:'+hashlib.sha256(command.request_id.encode()).hexdigest()+':'+str(index),encoded,digest,members))
                digest=hashlib.sha256(_batch_canonical([c[3] for c in cohorts],131072)).hexdigest();receipt=dict(request_id=command.request_id,status='pending',replayed=False,member_count=str(len(selected)),cohort_count=str(len(cohorts)),revision=str(revision));encoded=_batch_canonical(receipt,4096)
                connection.execute("INSERT INTO direct_control_requests VALUES (?,?,?,?,?,?,'pending',0)",(command.request_id,command.expected_worker_epoch,command._wire_request,encoded,digest,hashlib.sha256(command._wire_request+encoded+digest.encode()).hexdigest()))
                for index,child,members,digest,values in cohorts:
                    connection.execute("INSERT INTO direct_control_cohorts VALUES (?,?,?,?,?,'pending',NULL)",(command.request_id,index,child,members,digest))
                    connection.executemany('INSERT INTO direct_control_members VALUES (?,?,?,?,?,?)',[(command.request_id,m['job_id'],index,target,m['generation'],m['revision']) for target,m in enumerate(values)])
                if time.monotonic()>=deadline:raise TimeoutError('captured control deadline')
                connection.commit();return receipt
            except BaseException:connection.rollback();raise

    def control_snapshot(self,request,*,deadline=None):
        with self._batch_budget(deadline=deadline):
            row,_,_=self._control_request(request);receipt=_batch_decode(row['receipt']);receipt.pop('replayed')
            return receipt|dict(status=row['status'],completion_closed=bool(row['completion_closed']))

    def _preemption(self,request):
        row=self._connection.execute('SELECT * FROM direct_preemptions WHERE request_id=?',(request,)).fetchone()
        if row is None:return None
        _,command,_=self._control_request(request)
        values=[row[name] for name in ('request_id','job_id','source_request_id','worker_epoch','generation','revision','process')]
        body=self._read_direct_dispatch_command(self._connection,row['source_request_id'])
        owner=self._connection.execute('SELECT process FROM saved_body_owners WHERE request_id=?',(row['source_request_id'],)).fetchone()
        if command.scope!='job' or command.action!='preempt' or command.job==row['job_id'] or command.expected_worker_epoch!=row['worker_epoch'] or row['seal']!=hashlib.sha256(_batch_canonical(values,16384)).hexdigest():raise ValueError('preemption authority changed')
        if body is None or (body.job,body.generation,body.revision,body.state,body.status)!=(row['job_id'],row['generation'],row['revision'],'downloading','started') or owner is None or json.loads(owner[0])!=json.loads(row['process']):raise ValueError('preemption BODY changed')
        return row

    def reserve_preemption(self,request,plan,record):
        old=self._preemption(request)
        if old is not None:return old
        _,command,_=self._control_request(request)
        current=self._read_job_control_projection(self._connection,plan.job.job_id)
        if command.expected_worker_epoch!=self.worker_epoch() or current.state!='downloading' or self._engine_record_for_dispatch(plan)!=record or (current.generation,current.revision)!=(plan.generation,plan.revision):raise ValueError('preemption owner stale')
        process=json.dumps(record.identity.to_record(),sort_keys=True,separators=(',',':'));values=(request,current.job,plan.request_id,self.worker_epoch(),current.generation,current.revision,process)
        self._connection.execute('INSERT INTO direct_preemptions VALUES (?,?,?,?,?,?,?,?)',(*values,hashlib.sha256(_batch_canonical(list(values),16384)).hexdigest()))
        return self._preemption(request)

    def finish_preemption(self,request):
        intent,command,_=self._control_request(request)
        selected=self._read_job_control_projection(self._connection,command.job)
        target='preempt-selected:'+hashlib.sha256(request.encode()).hexdigest()
        result=self.apply_target_authorize(TargetAuthorizeCommand(target,self.worker_epoch(),'start_now',dict(kind='jobs',targets=[dict(job=command.job,expected_revision=command.expected_revision)])))
        if result.to_record()['results'][0]['outcome'] not in {'new_authority','existing_authority'}:raise ValueError('preemption selected target blocked')
        victim=self._preemption(request)
        if victim is not None:
            current=self._read_job_control_projection(self._connection,victim['job_id'])
            if current.state=='paused' and current.authorized and not current.manual_hold:
                resume='preempt-victim:'+hashlib.sha256(request.encode()).hexdigest()
                self.apply_target_authorize(TargetAuthorizeCommand(resume,self.worker_epoch(),'resume',dict(kind='jobs',targets=[dict(job=current.job,expected_revision=current.revision)])))
        self._connection.execute("UPDATE direct_control_requests SET status='applied' WHERE request_id=? AND status='pending'",(request,))
        self._connection.execute("UPDATE direct_control_cohorts SET status='applied' WHERE request_id=? AND status='pending'",(request,))

    def _retry_reopen(self,connection,audit):
        row=connection.execute('SELECT * FROM direct_retry_reopens WHERE audit_id=?',(audit,)).fetchone()
        if row is None:raise ValueError('retry budget fence missing')
        values=[row[name] for name in ('request_id','job_id','worker_epoch','old_generation','old_revision','generation','revision','audit_id','source_request_id','resume_request_id','before_budget','after_budget')]
        if row['seal']!=hashlib.sha256(_batch_canonical(values,32768)).hexdigest():raise ValueError('retry budget fence changed')
        intent,command,_=self._control_request(row['request_id'])
        captured=connection.execute('SELECT generation,revision FROM direct_control_members WHERE request_id=? AND job_id=?',(row['request_id'],row['job_id'])).fetchone()
        event=connection.execute('SELECT kind,job_id,generation,revision FROM events WHERE event_id=?',(audit,)).fetchone()
        before=json.loads(row['before_budget']);after=json.loads(row['after_budget'])
        if (command.scope,command.action,command.job,command.expected_worker_epoch,command.expected_revision)!=('job','retry',row['job_id'],row['worker_epoch'],row['old_revision']) or captured is None or tuple(captured)!=(row['old_generation'],row['old_revision']):raise ValueError('retry budget consent changed')
        if (row['generation'],row['revision'])!=(row['old_generation']+1,row['old_revision']+1) or event is None or tuple(event)!=('job_retry_opened',row['job_id'],row['generation'],row['revision']):raise ValueError('retry budget audit changed')
        if set(before)!={'generation','budget_number','ordinary_attempts','exhausted'} or not before['exhausted'] or before['generation']!=row['old_generation'] or after!=dict(generation=row['generation'],budget_number=before['budget_number']+1,ordinary_attempts=0,exhausted=False):raise ValueError('retry budget transition changed')
        return row

    def apply_explicit_retry(self,request):
        from hermes_downloads.paths import _saved_open_chain
        connection=self._connection;intent,command,_=self._control_request(request)
        if intent['status']!='pending':return
        existing=connection.execute('SELECT audit_id FROM direct_retry_reopens WHERE request_id=?',(request,)).fetchone()
        if existing is None:
            current=self._read_job_control_projection(connection,command.job);budget=self.get_retry_budget(command.job)
            if intent['worker_epoch']!=self.worker_epoch() or (current.revision!=command.expected_revision or current.state not in {'blocked','paused'}) or budget is None or budget.generation!=current.generation or not budget.exhausted or any(owner[0]==current.job for owner in self._job_engine_owners()):raise ValueError('exhausted retry unavailable')
            member=connection.execute('SELECT generation,revision FROM direct_control_members WHERE request_id=? AND job_id=?',(request,current.job)).fetchone()
            if member is None or tuple(member)!=(current.generation,current.revision):raise ValueError('retry capture stale')
            source=self._latest_body_source(connection,current.job);failure=connection.execute('SELECT * FROM direct_failures WHERE body_request_id=?',(source,)).fetchone()
            if failure is None or failure['action']!='exhausted':raise ValueError('exhausted typed failure missing')
            self._failure_state_for_audit(connection,current.job,failure['generation'],failure['revision'],failure['audit_id'])
            checkpoint=self._saved_checkpoint(connection,current.job,source);chain=marker=None
            if checkpoint is None:
                destination=self._saved_destination(current.job);reservation=self._effective_reservation(connection,current.job)
                descriptors,chain,marker=_saved_open_chain(destination,reservation)
                try:
                    for name in (destination.partial_path.name,destination.partial_path.name+'.aria2'):
                        try:os.stat(name,dir_fd=descriptors[3],follow_symlinks=False)
                        except FileNotFoundError:continue
                        raise ValueError('unqualified bytes preserved')
                finally:
                    for descriptor in reversed(descriptors):os.close(descriptor)
            elif self._checkpoint_fence(connection,checkpoint[0])!=(self.worker_epoch(),current.generation,current.revision):raise ValueError('retry checkpoint fence stale')
            head=self._capture_target_head(connection,current.job)
            authority=RetryAuthority.restore(policy=RetryPolicy(),budget=budget);after=authority.resume_after_exhaustion(new_generation=current.generation+1)
            if current.generation==_MAX_COUNTER or current.revision==_MAX_COUNTER:raise ValueError('retry counters exhausted')
            resume='explicit-retry:'+hashlib.sha256(request.encode()).hexdigest()
            connection.execute('BEGIN IMMEDIATE')
            try:
                connection.execute("UPDATE jobs SET generation=generation+1,revision=revision+1,state='paused' WHERE job_id=? AND generation=? AND revision=? AND state IN ('blocked','paused')",(current.job,current.generation,current.revision));self._require_one_changed_row(connection,'retry generation CAS')
                audit=connection.execute("INSERT INTO events(kind,job_id,generation,revision) VALUES ('job_retry_opened',?,?,?)",(current.job,current.generation+1,current.revision+1)).lastrowid
                encode=lambda value:json.dumps(dict(generation=value.generation,budget_number=value.budget_number,ordinary_attempts=value.ordinary_attempts,exhausted=value.exhausted),sort_keys=True,separators=(',',':'))
                values=(request,current.job,self.worker_epoch(),current.generation,current.revision,current.generation+1,current.revision+1,audit,source,resume,encode(budget),encode(after))
                connection.execute('INSERT INTO direct_retry_reopens VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',(*values,hashlib.sha256(_batch_canonical(list(values),32768)).hexdigest()))
                self._replace_retry_budget(connection,after);self._advance_target_head(connection,head)
                if checkpoint is None:
                    carrier=TargetAuthorizeCommand(resume,self.worker_epoch(),'resume',dict(kind='jobs',targets=[dict(job=current.job,expected_revision=current.revision+1)]))
                    self._insert_fresh_retry(connection,carrier,dict(failure)|dict(generation=current.generation+1,revision=current.revision+1),chain,marker,time.time_ns()//1_000_000)
                connection.commit()
            except BaseException:connection.rollback();raise
            existing=(audit,)
        reopened=self._retry_reopen(connection,existing[0]);current=self._read_job_control_projection(connection,reopened['job_id'])
        result=self.apply_target_authorize(TargetAuthorizeCommand(reopened['resume_request_id'],self.worker_epoch(),'resume',dict(kind='jobs',targets=[dict(job=current.job,expected_revision=reopened['revision'])])))
        if result.to_record()['results'][0]['outcome']!='existing_authority':raise ValueError('retry resume blocked')
        connection.execute("UPDATE direct_control_requests SET status='applied' WHERE request_id=? AND status='pending'",(request,));connection.execute("UPDATE direct_control_cohorts SET status='applied' WHERE request_id=? AND status='pending'",(request,))

    def apply_captured_cohort(self,request):
        row,command,cohorts=self._control_request(request)
        if row['status']!='pending':return
        if command.expected_worker_epoch!=self.worker_epoch():
            self._connection.execute("UPDATE direct_control_requests SET status='blocked' WHERE request_id=? AND status='pending'",(request,));self._connection.execute("UPDATE direct_control_cohorts SET status='blocked' WHERE request_id=? AND status='pending'",(request,));return
        cohort=next((c for c in cohorts if c['status']=='pending'),None)
        if cohort is not None:
            members=json.loads(cohort['members'])
            result=self.apply_target_authorize(TargetAuthorizeCommand(cohort['child_request_id'],command.expected_worker_epoch,'start',dict(kind='jobs',targets=[dict(job=m['job_id'],expected_revision=m['revision']) for m in members])))
            self._connection.execute("UPDATE direct_control_cohorts SET status='applied',receipt=? WHERE request_id=? AND cohort_index=? AND status='pending'",(result._wire_reply,request,cohort['cohort_index']));self._require_one_changed_row(self._connection,'captured child CAS')
        if not self._connection.execute("SELECT 1 FROM direct_control_cohorts WHERE request_id=? AND status='pending'",(request,)).fetchone():
            self._connection.execute('BEGIN IMMEDIATE')
            try:
                self._connection.execute("UPDATE direct_control_requests SET status='applied' WHERE request_id=? AND status='pending'",(request,))
                self._close_captured_completion(self._connection,request);self._connection.commit()
            except BaseException:self._connection.rollback();raise

    @staticmethod
    def _file_request(connection,request):
        from hermes_downloads.ipc import FilesCommand
        row=connection.execute('SELECT * FROM direct_file_requests WHERE request_id=?',(request,)).fetchone()
        if row is None:raise ValueError('file authority missing')
        command=FilesCommand.from_record(_batch_decode(row['wire']))
        if command.request_id!=request or row['seal']!=hashlib.sha256(row['wire']+str(row['worker_epoch']).encode()+row['receipt']).hexdigest():raise ValueError('file authority changed')
        if _batch_decode(row['receipt'])!=dict(request_id=request,status='pending',replayed=False,ids=list(command.ids)):raise ValueError('file receipt changed')
        return row,command

    def accept_files(self,command,*,deadline):
        connection=self._connection
        with self._batch_budget(deadline=deadline):
            connection.execute('BEGIN IMMEDIATE')
            try:
                if connection.execute('SELECT 1 FROM direct_file_requests WHERE request_id=?',(command.request_id,)).fetchone():
                    row,old=self._file_request(connection,command.request_id)
                    if old._wire_request!=command._wire_request:raise RequestConflictError('file request conflict')
                    connection.rollback();return _batch_decode(row['receipt'])|dict(replayed=True)
                if self._read_command_receipt(connection,command.request_id) is not None:raise RequestConflictError('file request conflict')
                for job in command.ids:
                    origin=self._read_creation_origin(connection,job,{})
                    if origin.job is None or origin.job.source_kind is not SourceKind.DIRECT:raise ValueError('direct file owner required')
                receipt=dict(request_id=command.request_id,status='pending',replayed=False,ids=list(command.ids));encoded=_batch_canonical(receipt,131072);epoch=self.worker_epoch()
                seal=hashlib.sha256(command._wire_request+str(epoch).encode()+encoded).hexdigest()
                connection.execute('INSERT INTO direct_file_requests VALUES (?,?,?,?,?)',(command.request_id,epoch,command._wire_request,encoded,seal))
                connection.executemany("INSERT INTO direct_file_outcomes(request_id,job_id,status) VALUES (?,?,'pending')",[(command.request_id,job) for job in command.ids])
                if time.monotonic()>=deadline:raise TimeoutError('file deadline')
                connection.commit();return receipt
            except BaseException:connection.rollback();raise

    def certified_file(self,job):
        from hermes_downloads.paths import _direct_cleanup_payload,require_current_direct_cleanup_payload
        claim=self._read_direct_cleanup_claim(self._connection,job,{})
        if claim is None or claim.phase!='finished':raise ValueError('finished file proof unavailable')
        payload=_direct_cleanup_payload(claim.attempt.prepared,claim.certified_stat)
        require_current_direct_cleanup_payload(claim.attempt.prepared,payload,marker_present=False)
        return claim,payload

    def _saved_purge_records(self,job,source):
        connection=self._connection;checkpoint,pair=self._saved_checkpoint(connection,job,source)
        destination=self._saved_destination(job);fence=checkpoint
        records=[dict(basename=name,facts=list(facts),source_request_id=source,continuation_request_id=None,role=role) for role,(name,facts) in enumerate(((destination.partial_path.name,pair.payload),(destination.partial_path.name+'.aria2',pair.control)))]
        rows=connection.execute('SELECT * FROM saved_direct_continuations WHERE job_id=? UNION ALL SELECT * FROM saved_direct_cycles WHERE job_id=?',(job,job)).fetchall()
        prepared=False
        for row in rows:
            continuation=self._saved_continuation(connection,job,row['request_id'])
            binding=connection.execute('SELECT drained FROM saved_resume_bindings WHERE request_id=?',(row['request_id'],)).fetchone()
            prior,old=self._saved_checkpoint(connection,job,continuation.source_request_id)
            history=json.loads(row['facts'])
            if row['phase'] not in {'ready','admitted'} or binding is None or binding[0]!=1 or old.chain!=pair.chain or old.marker!=pair.marker or set(history)!={'linked','retired','ready'} or set(history['ready'])!={'retained','candidates'} or history['retired']!=dict(retained=history['ready']['retained']):raise ValueError('saved purge original drain changed')
            for group in (history['ready']['retained'],history['ready']['candidates']):
                if type(group) is not list or len(group)!=2 or any(type(fact) is not list or len(fact)!=9 or any(type(value) is not int or value<0 for value in fact) for fact in group):raise ValueError('saved purge finite pair changed')
            if len({tuple(fact[:2]) for fact in (*history['ready']['retained'],*history['ready']['candidates'])})!=4:raise ValueError('saved purge candidate identity changed')
            children=connection.execute('SELECT request_id FROM saved_resume_bindings WHERE prepared_from=?',(row['request_id'],)).fetchall()
            if children:
                if len(children)!=1 or row['phase']!='ready':raise ValueError('saved purge prepared lineage ambiguous')
                self._saved_continuation(connection,job,children[0][0])
                continue
            if row['phase']=='ready':
                if prepared or continuation.source_request_id!=source or row['worker_epoch']>=self.worker_epoch():raise ValueError('saved purge prepared owner changed')
                prepared=True;fence=row
                for record,facts in zip(records[:2],history['ready']['candidates']):
                    record['facts']=facts;record['continuation_request_id']=continuation.request_id
            records.extend(dict(basename=name,facts=facts,source_request_id=continuation.source_request_id,continuation_request_id=continuation.request_id,role=role) for role,(name,facts) in enumerate(zip(continuation.retained_names,history['ready']['retained'])))
        if len({record['basename'] for record in records})!=len(records):raise ValueError('saved purge names collide')
        return checkpoint,pair,records,fence

    def prepare_saved_file_purge(self,request,job):
        from hermes_downloads.paths import _qualify_saved_purge
        connection=self._connection;intent,command=self._file_request(connection,request)
        if command.action!='purge' or job not in command.ids:raise ValueError('saved purge request changed')
        old=connection.execute('SELECT * FROM direct_saved_purge WHERE request_id=? AND job_id=?',(request,job)).fetchone()
        source=self._latest_body_source(connection,job) if old is None else old['source_request_id']
        checkpoint,pair,records,fence=self._saved_purge_records(job,source)
        if old is None:
            current=self._read_job_control_projection(connection,job)
            epoch,generation,revision=self._checkpoint_fence(connection,fence)
            exact=(epoch,generation,revision)==(self.worker_epoch(),current.generation,current.revision)
            if not exact and current.state=='removed' and epoch<=self.worker_epoch() and (generation,revision+1)==(current.generation,current.revision):
                head=self._capture_target_head(connection,job)
                exact=head is not None and head.intent_status=='terminal' and self._batch_control_corroborated(job,'remove',current.generation,current.revision,'removed',False)
            if intent['worker_epoch']!=self.worker_epoch() or not exact:raise ValueError('fresh saved purge fence required')
            prior_rows=connection.execute('SELECT p.* FROM direct_saved_purge p WHERE p.job_id=? AND p.source_request_id=? AND NOT EXISTS (SELECT 1 FROM direct_saved_purge_successors s WHERE s.prior_request_id=p.request_id AND s.job_id=p.job_id)',(job,source)).fetchall()
            if len(prior_rows)>1:raise ValueError('saved purge predecessor ambiguous')
            prior=None if not prior_rows else prior_rows[0]
            prior_records=None if prior is None else self.prepare_saved_file_purge(prior['request_id'],job)[2]
            marker=_qualify_saved_purge(self._saved_destination(job),self._effective_reservation(connection,job),pair,records,prior=prior_records)
            records.append(dict(basename='.hermes-reservation',facts=list(marker),source_request_id=source,continuation_request_id=None,role=2))
            encoded=json.dumps(records,sort_keys=True,separators=(',',':'))
            if prior is not None and encoded!=prior['records']:raise ValueError('saved purge predecessor artifacts changed')
            values=[request,job,source,checkpoint['checkpoint_digest'],encoded]
            connection.execute('BEGIN IMMEDIATE')
            try:
                connection.execute('INSERT INTO direct_saved_purge VALUES (?,?,?,?,?,?)',(*values,hashlib.sha256(_batch_canonical(values,131072)).hexdigest()))
                if prior is not None:connection.execute('INSERT INTO direct_saved_purge_successors VALUES (?,?,?)',(request,job,prior['request_id']))
                connection.executemany('INSERT INTO direct_saved_purge_artifacts VALUES (?,?,?,?,NULL)',[(request,job,index,'authorized') for index in range(len(records))])
                connection.execute("UPDATE direct_file_outcomes SET status='saved_purge_authorized' WHERE request_id=? AND job_id=? AND status='pending'",(request,job));self._require_one_changed_row(connection,'saved purge authorize CAS');connection.commit()
            except BaseException:connection.rollback();raise
        else:
            stored=json.loads(old['records']);values=[request,job,source,old['checkpoint_digest'],old['records']]
            if old['seal']!=hashlib.sha256(_batch_canonical(values,131072)).hexdigest() or old['checkpoint_digest']!=checkpoint['checkpoint_digest'] or stored[:-1]!=records or stored[-1]['basename']!='.hermes-reservation' or tuple(stored[-1]['facts'][:2])!=pair.marker:raise ValueError('saved purge authority changed')
            link=connection.execute('SELECT prior_request_id FROM direct_saved_purge_successors WHERE request_id=? AND job_id=?',(request,job)).fetchone()
            if link is not None:
                prior=connection.execute('SELECT rowid,* FROM direct_saved_purge WHERE request_id=? AND job_id=?',(link[0],job)).fetchone()
                self._file_request(connection,link[0])
                old_rowid=connection.execute('SELECT rowid FROM direct_saved_purge WHERE request_id=? AND job_id=?',(request,job)).fetchone()[0]
                if prior is None or prior['rowid']>=old_rowid or (prior['source_request_id'],prior['checkpoint_digest'],prior['records'])!=(source,old['checkpoint_digest'],old['records']) or prior['seal']!=hashlib.sha256(_batch_canonical([link[0],job,source,prior['checkpoint_digest'],prior['records']],131072)).hexdigest():raise ValueError('saved purge predecessor authority changed')
            records=stored
        phases=connection.execute('SELECT ordinal,phase,observation FROM direct_saved_purge_artifacts WHERE request_id=? AND job_id=? ORDER BY ordinal',(request,job)).fetchall()
        if [row[0] for row in phases]!=list(range(len(records))):raise ValueError('saved purge finite artifacts changed')
        for artifact in phases:
            if artifact['phase']=='authorized' and artifact['observation'] is not None or artifact['phase']=='absent' and (artifact['observation'] is None or json.loads(artifact['observation']) not in [dict(absence_observed=True,kind=kind) for kind in ('unlink_observed','qualified_present_absence')]):raise ValueError('saved purge absence certificate changed')
        inherited=set()
        link=connection.execute('SELECT prior_request_id FROM direct_saved_purge_successors WHERE request_id=? AND job_id=?',(request,job)).fetchone()
        if link is not None:inherited={item[0] for item in connection.execute("SELECT ordinal FROM direct_saved_purge_artifacts WHERE request_id=? AND job_id=? AND phase='absent'",(link[0],job))}
        # Prior absence fences rebound; the new request still acknowledges its own observation.
        return 'saved',pair,[record|dict(phase='absent' if index in inherited else phase[1]) for index,(record,phase) in enumerate(zip(records,phases))],intent['worker_epoch']==self.worker_epoch()

    def acknowledge_saved_file_artifact(self,request,job,index,fact):
        if type(index) is not int or type(fact) is not dict or fact not in [dict(absence_observed=True,kind=kind) for kind in ('unlink_observed','qualified_present_absence')]:raise ValueError('saved purge observation changed')
        self._file_request(self._connection,request)
        row=self._connection.execute('SELECT phase FROM direct_saved_purge_artifacts WHERE request_id=? AND job_id=? AND ordinal=?',(request,job,index)).fetchone()
        if row is None:raise ValueError('saved purge artifact missing')
        if row[0]=='authorized':
            self._connection.execute("UPDATE direct_saved_purge_artifacts SET phase='absent',observation=? WHERE request_id=? AND job_id=? AND ordinal=? AND phase='authorized'",(json.dumps(fact,sort_keys=True,separators=(',',':')),request,job,index));self._require_one_changed_row(self._connection,'saved purge absence CAS')

    def prepare_file_action(self,request,job):
        connection=self._connection;intent,command=self._file_request(connection,request)
        row=connection.execute('SELECT * FROM direct_file_outcomes WHERE request_id=? AND job_id=?',(request,job)).fetchone()
        if row is None or job not in command.ids:raise ValueError('file member changed')
        if row['status'] not in {'pending','purge_authorized','saved_purge_authorized'}:return None
        if row['status']=='saved_purge_authorized':return self.prepare_saved_file_purge(request,job)
        if command.action=='purge' and self._latest_body_source(connection,job) is not None and self._read_job_control_projection(connection,job).state!='completed':
            try:return self.prepare_saved_file_purge(request,job)
            except (ValueError,OSError):
                connection.execute("UPDATE direct_file_outcomes SET status='blocked',reason='saved_artifact_not_qualified' WHERE request_id=? AND job_id=? AND status='pending'",(request,job));return None
        artifact=connection.execute('SELECT * FROM direct_file_artifacts WHERE request_id=? AND job_id=?',(request,job)).fetchone()
        if artifact is not None:
            claim=self._read_direct_cleanup_claim(connection,job,{})
            facts=json.loads(artifact['facts'])
            values=[request,job,artifact['attempt_id'],artifact['facts']]
            if (claim is None or claim.phase!='finished' or claim.attempt.attempt_id!=artifact['attempt_id'] or list(claim.certified_stat)!=facts
                or artifact['seal']!=hashlib.sha256(_batch_canonical(values,16384)).hexdigest()):raise ValueError('purge authority changed')
            return claim,intent['worker_epoch']==self.worker_epoch()
        try:
            if intent['worker_epoch']!=self.worker_epoch():raise ValueError('fresh file consent required')
            claim,_=self.certified_file(job)
        except (ValueError,OSError):
            connection.execute("UPDATE direct_file_outcomes SET status='blocked',reason='qualified_finished_file_required' WHERE request_id=? AND job_id=? AND status='pending'",(request,job));return None
        if command.action=='reveal':
            connection.execute("UPDATE direct_file_outcomes SET status='reveal_pending' WHERE request_id=? AND job_id=? AND status='pending'",(request,job));return None
        facts=json.dumps(list(claim.certified_stat),separators=(',',':'));values=[request,job,claim.attempt.attempt_id,facts]
        connection.execute('BEGIN IMMEDIATE')
        try:
            connection.execute('INSERT INTO direct_file_artifacts VALUES (?,?,?,?,?,0)',(*values,hashlib.sha256(_batch_canonical(values,16384)).hexdigest()))
            connection.execute("UPDATE direct_file_outcomes SET status='purge_authorized' WHERE request_id=? AND job_id=? AND status='pending'",(request,job));self._require_one_changed_row(connection,'purge authorize CAS');connection.commit()
        except BaseException:connection.rollback();raise
        return claim,True

    def acknowledge_file_purge(self,request,job,*,failed):
        self._file_request(self._connection,request)
        self._connection.execute('BEGIN IMMEDIATE')
        try:
            saved=self._connection.execute('SELECT count(*) FROM direct_saved_purge_artifacts WHERE request_id=? AND job_id=?',(request,job)).fetchone()[0]
            if not failed and saved:
                if self._connection.execute("SELECT count(*) FROM direct_saved_purge_artifacts WHERE request_id=? AND job_id=? AND phase='absent'",(request,job)).fetchone()[0]!=saved:raise ValueError('saved purge acknowledgement incomplete')
            if not failed and not saved:
                self._connection.execute('UPDATE direct_file_artifacts SET absence_observed=1 WHERE request_id=? AND job_id=? AND absence_observed=0',(request,job));self._require_one_changed_row(self._connection,'purge absence CAS')
            self._connection.execute("UPDATE direct_file_outcomes SET status=?,reason=? WHERE request_id=? AND job_id=? AND status IN ('purge_authorized','saved_purge_authorized')",('blocked' if failed else 'purged','artifact_changed_or_cancelled' if failed else None,request,job));self._require_one_changed_row(self._connection,'purge outcome CAS');self._connection.commit()
        except BaseException:self._connection.rollback();raise

    def file_bridge(self,record,*,deadline):
        op=record.get('op');base={'op','protocol_version'}
        if record.get('protocol_version')!=1 or type(record.get('protocol_version')) is not int:raise ValueError('reveal version')
        if op=='reveal_pending':
            if set(record)!=base:raise ValueError('reveal shape')
        else:
            fields=base|{'request_id','job_id'}|({'token'} if op in {'reveal_presented','reveal_click','reveal_settle'} else set())|({'notification_id'} if op=='reveal_presented' else set())|({'success'} if op=='reveal_settle' else set())
            if set(record)!=fields:raise ValueError('reveal shape')
            for name in ('request_id','job_id'):_require_identifier(record[name],name)
            if 'token' in record and (type(record['token']) is not str or re.fullmatch('[0-9a-f]{64}',record['token']) is None):raise ValueError('reveal token')
            if op=='reveal_settle' and type(record['success']) is not bool:raise ValueError('reveal result')
            if op=='reveal_presented' and (type(record['notification_id']) is not str or not 1<=len(record['notification_id'])<=256 or not record['notification_id'].strip() or any(ord(c)<32 or 127<=ord(c)<=159 for c in record['notification_id'])):raise ValueError('reveal presentation')
        with self._notification_transaction(deadline=deadline,write=op!='reveal_pending') as connection:
            if op=='reveal_pending':
                row=connection.execute("SELECT request_id,job_id FROM direct_file_outcomes JOIN direct_file_requests USING(request_id) WHERE status='reveal_pending' AND worker_epoch=? ORDER BY direct_file_outcomes.rowid LIMIT 1",(self.worker_epoch(),)).fetchone()
                return dict(request=None if row is None else dict(request_id=row[0],job_id=row[1]))
            intent,command=self._file_request(connection,record['request_id']);job=record['job_id']
            row=connection.execute('SELECT * FROM direct_file_outcomes WHERE request_id=? AND job_id=?',(command.request_id,job)).fetchone()
            if row is None or job not in command.ids or command.action!='reveal' or intent['worker_epoch']!=self.worker_epoch():return dict(status='stale',path=None,token=None)
            if op=='reveal_offer':
                if row['status']!='reveal_pending':return dict(status='unknown',path=None,token=None)
                _,payload=self.certified_file(job);token=secrets.token_hex(32)
                connection.execute("UPDATE direct_file_outcomes SET status='reveal_offered',token=? WHERE request_id=? AND job_id=? AND status='reveal_pending'",(token,command.request_id,job));self._require_one_changed_row(connection,'reveal offer CAS')
                return dict(status='offered',path=str(payload.path),token=token)
            if row['token']!=record['token']:return dict(status='stale',path=None,token=None)
            if op=='reveal_presented':
                if row['presentation'] is not None and row['presentation']!=record['notification_id']:raise ValueError('presentation changed')
                connection.execute('UPDATE direct_file_outcomes SET presentation=? WHERE request_id=? AND job_id=?',(record['notification_id'],command.request_id,job));return dict(status='presented',path=None,token=None)
            if op=='reveal_click':
                if row['status']!='reveal_offered':return dict(status='unknown',path=None,token=None)
                _,payload=self.certified_file(job)
                connection.execute("UPDATE direct_file_outcomes SET status='reveal_issued' WHERE request_id=? AND job_id=? AND status='reveal_offered'",(command.request_id,job));self._require_one_changed_row(connection,'reveal click CAS')
                return dict(status='issued',path=str(payload.path),token=record['token'])
            if op!='reveal_settle':raise ValueError('reveal operation')
            status='revealed' if record['success'] else 'reveal_failed'
            if row['status']==status:return dict(status=status,path=None,token=None)
            if row['status']!='reveal_issued':return dict(status='unknown',path=None,token=None)
            connection.execute('UPDATE direct_file_outcomes SET status=? WHERE request_id=? AND job_id=? AND status=?',(status,command.request_id,job,'reveal_issued'));self._require_one_changed_row(connection,'reveal SDK result CAS')
            return dict(status=status,path=None,token=None)

    def source_detail(self, job, *, deadline=None):
        from zoneinfo import ZoneInfo
        _require_identifier(job,'job')
        with self._batch_budget(deadline=deadline):
            original=self.get_materialized_job(job)
            if original is None:raise ValueError('unknown job')
            effective=self._effective_settings(self._connection,original)
            if effective['scheduled_for'] is not None:effective=effective|dict(scheduled_for=datetime.fromisoformat(effective['scheduled_for']).astimezone(ZoneInfo('Asia/Tehran')).isoformat())
            source=None;edit=None
            row=self._connection.execute('SELECT request_id FROM direct_source_requests WHERE job_id=? ORDER BY rowid DESC LIMIT 1',(job,)).fetchone()
            if row is not None:
                value,_=self._source_request(row[0]);status=value['status']
                source=dict(request_id=row[0],status=status,alternative='separate_download' if status=='blocked' else None)
            row=self._connection.execute('SELECT request_id,status,reason FROM direct_edit_outcomes WHERE job_id=? ORDER BY rowid DESC LIMIT 1',(job,)).fetchone()
            if row is not None:
                self._edit_request(self._connection,row[0]);edit=dict(request_id=row[0],status=row[1],reason=row[2])
            files=None
            file=self._connection.execute('SELECT request_id,status,reason FROM direct_file_outcomes WHERE job_id=? ORDER BY rowid DESC LIMIT 1',(job,)).fetchone()
            if file is not None:
                self._file_request(self._connection,file[0]);path=None
                if file[1] in {'reveal_pending','reveal_offered','reveal_issued','revealed','reveal_failed'}:
                    try:path=str(self.certified_file(job)[1].path)
                    except (ValueError,OSError):pass
                files=dict(request_id=file[0],status=file[1],reason=file[2],path=path)
            collection=None
            if original.queue_collection_id is not None:
                identifier=original.queue_collection_id
                gate=self._connection.execute('SELECT value,revision FROM settings WHERE key=?',('collection_gate:'+hashlib.sha256(identifier.encode()).hexdigest(),)).fetchone()
                held=self._connection.execute('SELECT 1 FROM collection_holds WHERE collection_id=?',(identifier,)).fetchone() is not None
                revision=0 if gate is None else _require_counter(gate['revision'],'collection revision')
                if gate is not None and (gate['value'] not in {'pause','resume'} or held!=(gate['value']=='pause')):raise ValueError('collection gate changed')
                collection=dict(id=identifier,held=held,revision=str(revision))
            budget=self.get_retry_budget(job);retry=None
            if budget is not None:
                due=None
                failure=self._connection.execute("SELECT retry_due_ms FROM direct_failures WHERE job_id=? AND generation=? AND state='retry_wait' ORDER BY audit_id DESC LIMIT 1",(job,budget.generation)).fetchone()
                if failure is not None and self._read_job_control_projection(self._connection,job).state=='retry_wait':
                    due=datetime.fromtimestamp(_require_counter(failure[0],'retry due')/1000,UTC).isoformat()
                retry=dict(generation=str(budget.generation),budget_number=str(budget.budget_number),ordinary_attempts=str(budget.ordinary_attempts),paused=budget.paused,exhausted=budget.exhausted,due_at=due)
            detail=dict(id=job,source_replacement=source,edit=edit,effective=effective,files=files,collection=collection,retry=retry)
            latest=self._target_latest(self._connection,job)
            if latest['kind']=='job_resume_blocked':
                self._saved_decision_for_audit(self._connection,job,latest['generation'],latest['revision'])
                detail['resume_decision']=dict(status='blocked',reason='source_identity_changed_or_unknown',alternative='separate_download')
            return detail

    def apply_source_replacement(self, request, *, _verified_stock=None):
        connection=self._connection;row,command=self._source_request(request)
        current=self._read_job_control_projection(connection,command.id)
        origin=self._read_creation_origin(connection,command.id,{})
        if current.state in {'resolving','downloading','finalizing'}:return False
        if any(owner[0]==command.id for owner in self._job_engine_owners()):return False
        checkpoint=self._saved_checkpoint(connection,command.id)
        expected=None if origin.kind!='batch' else origin.batch.expected_sha256
        head=self._capture_target_head(connection,command.id)
        # Strong ETags are resource scoped. An unchanged URL can be freshly
        # verified; a different URL still needs the trusted original digest.
        stock=None if checkpoint is None else self._checkpoint_stock(connection,checkpoint[0])
        unchanged=(expected is None and stock is not None and stock.known and stock.strong_validator is not None
            and stock.total_length is not None and stock.resource_digest is not None and _verified_stock==stock
            and command.url.encode()==self.effective_source(command.id))
        qualified=((expected is not None or unchanged) and row['worker_epoch']==self.worker_epoch() and row['generation']==current.generation
            and current.state in {'paused','queued','blocked','needs_link','needs_auth'} and head is not None
            and (head.last_admission_serial==0 or checkpoint is not None))
        connection.execute('BEGIN IMMEDIATE')
        try:
            if qualified and expected is not None:
                source=None if checkpoint is None else checkpoint[0]['source_request_id'];digest=None if checkpoint is None else checkpoint[0]['checkpoint_digest']
                values=(request,command.id,current.generation,current.revision,source,digest,expected)
                connection.execute('INSERT INTO direct_sources VALUES (?,?,?,?,?,?,?,?)',(*values,hashlib.sha256(_batch_canonical(list(values),4096)).hexdigest()))
            connection.execute("UPDATE direct_source_requests SET status=? WHERE request_id=? AND status='pending'",('applied' if qualified else 'blocked',request))
            self._require_one_changed_row(connection,'source outcome CAS');connection.commit()
        except BaseException:connection.rollback();raise
        if qualified and current.authorized and not current.manual_hold and head.intent_status=='current':
            resume='source-resume:'+hashlib.sha256(request.encode()).hexdigest()
            self.apply_target_authorize(TargetAuthorizeCommand(resume,self.worker_epoch(),'resume',dict(kind='jobs',targets=[dict(job=command.id,expected_revision=current.revision)])))
        return True

    def effective_source(self, job):
        rows=self._connection.execute('SELECT * FROM direct_sources WHERE job_id=? ORDER BY rowid',(job,)).fetchall()
        source=self.get_job(job).source_url
        for row in rows:
            values=tuple(row[name] for name in ('request_id','job_id','generation','revision','source_request_id','checkpoint_digest','expected_sha256'))
            intent,command=self._source_request(row['request_id'])
            origin=self._read_creation_origin(self._connection,job,{},target=False)
            if (row['seal']!=hashlib.sha256(_batch_canonical(list(values),4096)).hexdigest() or intent['status']!='applied'
                or row['job_id']!=command.id or origin.kind!='batch' or origin.batch.expected_sha256!=row['expected_sha256']):raise ValueError('effective source binding changed')
            if row['source_request_id'] is not None:
                checkpoint=self._saved_checkpoint(self._connection,job,row['source_request_id'])
                if checkpoint is None or checkpoint[0]['checkpoint_digest']!=row['checkpoint_digest']:raise ValueError('source checkpoint changed')
            source=command.url.encode()
        return source

    def configuration_command(self, *, revision=None, request_id=None):
        from hermes_downloads.ipc import ConfigureCommand
        row=self._connection.execute('SELECT * FROM direct_configurations WHERE '+('revision=?' if revision is not None else 'request_id=?'),(revision if revision is not None else request_id,)).fetchone()
        if row is None:raise ValueError('configuration authority missing')
        command=ConfigureCommand.from_record(_batch_decode(row['wire']))
        if (row['request_id'],row['payload_digest'],row['wire'])!=(command.request_id,command.payload_digest,command._wire_request):raise ValueError('configuration authority changed')
        _require_worker_epoch(row['worker_epoch'],'configuration epoch')
        if _batch_decode(row['receipt'])!=dict(request_id=command.request_id,revision=str(row['revision']),status='pending',replayed=False):raise ValueError('configuration receipt changed')
        return command,row['worker_epoch']

    def configuration_snapshot(self, *, deadline=None):
        from hermes_downloads.ipc import ConfigureCommand
        with (nullcontext() if self._connection.in_transaction else self._batch_budget(deadline=deadline)):
            values=[]
            default=dict(concurrency='1',connections='4',global_limit_bps=None)
            for key in ('direct_desired','direct_applied'):
                row=self._connection.execute('SELECT value,revision FROM settings WHERE key=?',(key,)).fetchone()
                value=default if row is None else json.loads(row['value'])
                ConfigureCommand('settings-validation',value)
                if set(value)!=set(default):raise ValueError('incomplete settings')
                values.append((value,0 if row is None else _require_counter(row['revision'],'settings revision')))
            desired,applied=values
            if applied[1]>desired[1]:raise ValueError('applied settings exceed authority')
            current=default;qualified={0:default}
            for revision in range(1,desired[1]+1):
                command,_=self.configuration_command(revision=revision)
                current=current|command.to_record()['patch']
                if revision in {desired[1],applied[1]}:qualified[revision]=current
            if desired[0]!=qualified[desired[1]] or applied[0]!=qualified[applied[1]]:raise ValueError('settings projection changed')
            return dict(desired=desired[0],applied=applied[0],revision=str(desired[1]),applied_revision=str(applied[1]),
                status='applied' if desired==applied else 'pending',speed_units='bytes per second')

    def accept_configuration(self, command, *, deadline):
        from hermes_downloads.ipc import ConfigureCommand
        if type(command) is not ConfigureCommand: raise ValueError('invalid configuration')
        connection=self._connection
        with self._batch_budget(deadline=deadline):
            connection.execute('BEGIN IMMEDIATE')
            try:
                row=connection.execute('SELECT * FROM direct_configurations WHERE request_id=?',(command.request_id,)).fetchone()
                if row is not None:
                    if (row['payload_digest'],row['wire'])!=(command.payload_digest,command._wire_request): raise RequestConflictError('configuration request conflict')
                    receipt=_batch_decode(row['receipt'])
                    if receipt!=dict(request_id=command.request_id,revision=str(row['revision']),status='pending',replayed=False): raise ValueError('configuration receipt changed')
                    connection.rollback();return receipt|dict(replayed=True)
                if self._read_command_receipt(connection,command.request_id) is not None or self._read_direct_dispatch_command(connection,command.request_id) is not None:
                    raise RequestConflictError('configuration request conflict')
                current=self.configuration_snapshot();revision=int(current['revision'])+1
                _require_counter(revision,'configuration revision')
                desired=current['desired']|command.to_record()['patch']
                encoded=json.dumps(desired,sort_keys=True,separators=(',',':'))
                receipt=dict(request_id=command.request_id,revision=str(revision),status='pending',replayed=False)
                connection.execute("INSERT INTO settings VALUES ('direct_desired',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,revision=excluded.revision",(encoded,revision))
                connection.execute('INSERT INTO direct_configurations VALUES (?,?,?,?,?,?)',(command.request_id,command.payload_digest,command._wire_request,_batch_canonical(receipt,4096),revision,self._current_worker_epoch(connection)))
                if time.monotonic()>=deadline: raise TimeoutError('configuration deadline')
                connection.commit();return receipt
            except BaseException:connection.rollback();raise

    def applied_configuration(self, snapshot):
        connection=self._connection
        with self._batch_budget():
            connection.execute('BEGIN IMMEDIATE')
            try:
                current=self.configuration_snapshot()
                if current!=snapshot:raise ValueError('settings changed during application')
                desired=json.dumps(snapshot['desired'],sort_keys=True,separators=(',',':'))
                connection.execute("INSERT INTO settings VALUES ('direct_applied',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,revision=excluded.revision",(desired,int(snapshot['revision'])))
                connection.commit()
            except BaseException:connection.rollback();raise

    @staticmethod
    def _job_engine_seal(values):
        return hashlib.sha256(_batch_canonical(list(values),4096)).hexdigest()

    def _decode_job_engine(self, row):
        if row['owner_digest']!=self._job_engine_seal(row[name] for name in ('job_id','dispatch_request_id','worker_epoch','reservation_token','phase','process','rpc_port','rpc_secret')):raise ValueError('engine owner binding changed')
        _require_identifier(row['job_id'],'engine job');_require_identifier(row['dispatch_request_id'],'engine dispatch')
        _require_worker_epoch(row['worker_epoch'],'engine epoch');_require_reservation_token(row['reservation_token'])
        if row['phase'] not in {'reserved','bound','ready'}:raise ValueError('engine phase changed')
        record=None if row['process'] is None else DirectEngineRecord(row['worker_epoch'],ProcessBirthIdentity.from_record(json.loads(row['process'])))
        capability=None if row['rpc_port'] is None else _DirectEngineRecoveryCapability(row['rpc_port'],row['rpc_secret'])
        if (row['phase']=='reserved')!=(record is None) or (row['phase']=='ready')!=(capability is not None):raise ValueError('engine owner incomplete')
        receipt=self._read_direct_dispatch_command(self._connection,row['dispatch_request_id'])
        cause=self._read_target_dispatch_cause(self._connection,row['dispatch_request_id'],{})
        if receipt is None or receipt.job!=row['job_id'] or cause is None or cause['kind']!='body':raise ValueError('engine dispatch changed')
        command=self._read_target_command(self._connection,cause['activation_request_id'],{})[0]
        if command.expected_worker_epoch!=row['worker_epoch']:raise ValueError('engine dispatch epoch changed')
        return row['job_id'],record,row['dispatch_request_id'],row['reservation_token'],capability

    def _job_engine_owners(self):
        return [self._decode_job_engine(row) for row in self._connection.execute('SELECT * FROM direct_job_engines ORDER BY job_id')]

    def _job_engine_for_record(self, record):
        if type(record) is not DirectEngineRecord:raise ValueError('engine record invalid')
        found=[row for row in self._job_engine_owners() if row[1]==record]
        if len(found)>1:raise ValueError('ambiguous process owner')
        return found[0] if found else None

    def _engine_record_for_dispatch(self, plan):
        row=self._connection.execute('SELECT * FROM direct_job_engines WHERE job_id=?',(plan.job.job_id,)).fetchone()
        if row is None:return self.get_direct_engine_record()
        owner=self._decode_job_engine(row)
        if owner[2]!=plan.request_id:raise ValueError('engine belongs to another dispatch')
        return owner[1]

    def reserve_job_engine(self, plan):
        connection=self._connection;connection.execute('BEGIN IMMEDIATE')
        try:
            self._require_pending_direct_dispatch_command(connection,plan,_effect=True)
            if (self.get_direct_engine_record() is not None or self.get_direct_engine_activation_fence() is not None
                or len(self._job_engine_owners())>=int(self.configuration_snapshot()['desired']['concurrency'])):raise ValueError('engine capacity unavailable')
            fence=DirectEngineActivationFence(self.worker_epoch(),secrets.token_hex(32))
            values=(plan.job.job_id,plan.request_id,fence.worker_epoch,fence.reservation_token,'reserved',None,None,None)
            connection.execute('INSERT INTO direct_job_engines VALUES (?,?,?,?,?,?,?,?,?)',(*values,self._job_engine_seal(values)))
            connection.commit();return fence
        except BaseException:connection.rollback();raise

    def bind_job_engine(self, plan, fence, record):
        connection=self._connection;connection.execute('BEGIN IMMEDIATE')
        try:
            self._require_pending_direct_dispatch_command(connection,plan,_effect=True)
            if record.worker_epoch!=fence.worker_epoch or fence.worker_epoch!=self.worker_epoch():raise ValueError('engine epoch changed')
            encoded=json.dumps(record.identity.to_record(),sort_keys=True,separators=(',',':'))
            row=connection.execute('SELECT * FROM direct_job_engines WHERE job_id=?',(plan.job.job_id,)).fetchone();self._decode_job_engine(row)
            sealed=self._job_engine_seal((plan.job.job_id,plan.request_id,fence.worker_epoch,fence.reservation_token,'bound',encoded,None,None))
            connection.execute("UPDATE direct_job_engines SET phase='bound',process=?,owner_digest=? WHERE job_id=? AND dispatch_request_id=? AND worker_epoch=? AND reservation_token=? AND phase='reserved'",(encoded,sealed,plan.job.job_id,plan.request_id,fence.worker_epoch,fence.reservation_token))
            self._require_one_changed_row(connection,'job engine bind');connection.commit()
        except BaseException:connection.rollback();raise

    def clear_job_engine_fence(self, plan, fence):
        connection=self._connection;connection.execute('BEGIN IMMEDIATE')
        try:
            connection.execute("DELETE FROM direct_job_engines WHERE job_id=? AND dispatch_request_id=? AND worker_epoch=? AND reservation_token=? AND phase='reserved'",(plan.job.job_id,plan.request_id,fence.worker_epoch,fence.reservation_token))
            changed=connection.execute('SELECT changes()').fetchone()[0];connection.commit();return changed==1
        except BaseException:connection.rollback();raise

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
            if self._job_engine_owners():
                connection.commit();return None

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
        owner=self._job_engine_for_record(record)
        if owner is not None:return owner[4]
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
        owner=self._job_engine_for_record(record)
        if owner is not None:
            connection=self._connection;connection.execute('BEGIN IMMEDIATE')
            try:
                if self.worker_epoch()!=record.worker_epoch:raise ValueError('engine capability epoch changed')
                encoded=json.dumps(record.identity.to_record(),sort_keys=True,separators=(',',':'))
                sealed=self._job_engine_seal((owner[0],owner[2],record.worker_epoch,owner[3],'ready',encoded,capability.rpc_port,capability.rpc_secret))
                connection.execute("UPDATE direct_job_engines SET phase='ready',rpc_port=?,rpc_secret=?,owner_digest=? WHERE job_id=? AND dispatch_request_id=? AND worker_epoch=? AND reservation_token=? AND process=? AND phase='bound'",(capability.rpc_port,capability.rpc_secret,sealed,owner[0],owner[2],record.worker_epoch,owner[3],encoded))
                self._require_one_changed_row(connection,'job capability bind');connection.commit();return
            except BaseException:connection.rollback();raise
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
        owner=self._job_engine_for_record(record)
        if owner is not None:
            if owner[4]!=capability:return False
            connection=self._connection;connection.execute('BEGIN IMMEDIATE')
            try:
                encoded=json.dumps(record.identity.to_record(),sort_keys=True,separators=(',',':'))
                connection.execute("DELETE FROM direct_job_engines WHERE job_id=? AND dispatch_request_id=? AND worker_epoch=? AND reservation_token=? AND process=? AND phase='ready' AND rpc_port=? AND rpc_secret=?",(owner[0],owner[2],record.worker_epoch,owner[3],encoded,capability.rpc_port,capability.rpc_secret))
                changed=connection.execute('SELECT changes()').fetchone()[0];connection.commit();return changed==1
            except BaseException:connection.rollback();raise
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
        owner=self._job_engine_for_record(record)
        if owner is not None:
            if owner[4] is not None:return False
            connection=self._connection;connection.execute('BEGIN IMMEDIATE')
            try:
                encoded=json.dumps(record.identity.to_record(),sort_keys=True,separators=(',',':'))
                connection.execute("DELETE FROM direct_job_engines WHERE job_id=? AND dispatch_request_id=? AND worker_epoch=? AND reservation_token=? AND process=? AND phase='bound'",(owner[0],owner[2],record.worker_epoch,owner[3],encoded))
                changed=connection.execute('SELECT changes()').fetchone()[0];connection.commit();return changed==1
            except BaseException:connection.rollback();raise
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

        version=SQLiteStore._destination_version(connection,job_id)
        if version is not None:
            row=connection.execute('SELECT device,inode FROM direct_destination_markers WHERE request_id=? AND job_id=?',(version['request_id'],job_id)).fetchone()
            original=connection.execute('SELECT marker_device,marker_inode FROM publication_marker_bindings WHERE job_id=?',(job_id,)).fetchone()
            if original is not None and row is not None and tuple(original)!=tuple(row):raise ValueError('destination marker changed original workspace')
            return None if row is None else PublicationMarkerBinding(job_id,*tuple(row))
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

        version=SQLiteStore._destination_version(connection,job_id)
        if version is not None:
            row=connection.execute('SELECT device,inode,size FROM direct_destination_staged WHERE request_id=? AND job_id=?',(version['request_id'],job_id)).fetchone()
            if row is None:return None
            binding=_StagedPayloadBinding(job_id,*tuple(row))
            if SQLiteStore._read_publication_marker_binding(connection,job_id) is None:raise ValueError('destination stage lacks marker')
            return binding
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

        version=SQLiteStore._destination_version(connection,job_id)
        if version is not None:
            row=connection.execute('SELECT device,inode,size FROM direct_destination_final WHERE request_id=? AND job_id=?',(version['request_id'],job_id)).fetchone()
            if row is None:return None
            binding=_FinalPublicationBinding(job_id,*tuple(row))
            stage=SQLiteStore._read_staged_payload_binding(connection,job_id)
            if stage is None or (stage.partial_device,stage.partial_inode,stage.logical_size)!=(binding.final_device,binding.final_inode,binding.logical_size):raise ValueError('destination final lacks stage')
            return binding
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
            reservation = self._effective_reservation(connection, job_id)
            if reservation is None:
                raise ValueError(
                    "publication marker binding requires a current publication reservation"
                )
            if reservation.claim_token != claim_token:
                raise ValueError("publication marker claim token does not match")
            version=self._destination_version(connection,job_id)
            if version is not None:
                existing=self._read_publication_marker_binding(connection,job_id)
                if existing is None:connection.execute('INSERT INTO direct_destination_markers VALUES (?,?,?,?)',(version['request_id'],job_id,marker_device,marker_inode))
                elif existing!=requested:raise ValueError('destination marker rebound')
                connection.commit();return requested
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
        reservation = self._effective_reservation(connection, requested.job_id)
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
        version=self._destination_version(connection,requested.job_id)
        if version is not None:
            existing=self._read_staged_payload_binding(connection,requested.job_id)
            if existing is None:connection.execute('INSERT INTO direct_destination_staged VALUES (?,?,?,?,?)',(version['request_id'],requested.job_id,requested.partial_device,requested.partial_inode,requested.logical_size))
            elif existing!=requested:raise ValueError('destination payload rebound')
            return requested
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
        reservation = self._effective_reservation(connection, requested.job_id)
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
        version=self._destination_version(connection,requested.job_id)
        if version is not None:
            existing=self._read_final_publication_binding(connection,requested.job_id)
            if existing is None:connection.execute('INSERT INTO direct_destination_final VALUES (?,?,?,?,?)',(version['request_id'],requested.job_id,requested.final_device,requested.final_inode,requested.logical_size))
            elif existing!=requested:raise ValueError('destination payload rebound')
            return requested
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

    def _notification_projection(self, connection, row, now):
        identity = self._notification_store_uuid(connection)
        if identity is None or row['store_uuid'] != identity:
            raise ValueError('notification identity unavailable')
        if _require_counter(row['sequence'], 'notification sequence') < 1:
            raise ValueError('notification sequence invalid')
        _require_counter(row['generation'], 'notification generation')
        _require_counter(row['created_at_ms'], 'notification creation')
        path = None
        if row['type']=='completion' and row['count'] == 1 and row['job_id'] is not None:
            from hermes_downloads.paths import _direct_cleanup_payload, require_current_direct_cleanup_payload
            try:
                claim = self._read_direct_cleanup_claim(connection, row['job_id'], {})
                if claim is not None and claim.phase == 'finished':
                    provenance = json.loads(row['provenance'])
                    fact = (provenance['completion'] if provenance['kind'] == 'job' else
                        next(m['completion'] for m in provenance['members'] if m['completion'] is not None))
                    if (fact['job_id'], fact['audit_id'], fact['generation'], fact['revision']) != (
                            claim.job_id, claim.completion_audit_id, claim.completion_generation, claim.completion_revision):
                        raise ValueError('notification cleanup identity changed')
                    payload = _direct_cleanup_payload(claim.attempt.prepared, claim.certified_stat)
                    require_current_direct_cleanup_payload(claim.attempt.prepared, payload, marker_present=False)
                    path = str(payload.path)
            except (ValueError, TypeError, KeyError, StopIteration, OSError):
                # An event survives incomplete cleanup or a changed final namespace.
                path = None
        state = row['presentation_state']
        if state == 'claimed' and row['lease_expires_at_ms'] <= now:
            state = 'pending'
        event = dict(event_id=f"{identity}:{row['sequence']}", type=row['type'], job_id=row['job_id'],
            collection_id=row['collection_id'], generation=row['generation'], created_at_ms=row['created_at_ms'],
            count=row['count'], reason_code=row['reason_code'], reveal_path=path, presentation_state=state)
        # A legitimate native signed64 completion survives a renderer projection refusal.
        validate_notification_event({**event, 'reveal_path': None})
        if path is not None:
            try:
                validate_notification_event(event)
            except (ValueError, TypeError):
                event['reveal_path'] = None
        return event

    @contextmanager
    def _notification_transaction(self, *, deadline, write):
        connection = self._connection
        if connection.in_transaction:
            raise ValueError('notification cannot borrow a transaction')
        with self._batch_budget(deadline=deadline) as end:
            connection.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
            try:
                yield connection
                if time.monotonic() >= end:
                    raise TimeoutError('notification database deadline')
                if write:
                    remaining_ms = int((end - time.monotonic()) * 1000)
                    if remaining_ms <= 0:
                        raise TimeoutError('notification database deadline')
                    connection.execute(f'PRAGMA busy_timeout = {remaining_ms}')
                    connection.commit()
            finally:
                connection.rollback()

    def read_notification_pending(self, command, *, deadline=None):
        if type(command) is not NotificationPendingCommand:
            raise ValueError('invalid notification pending')
        with self._notification_transaction(deadline=deadline, write=False) as connection:
            now = time.time_ns() // 1_000_000
            rows = connection.execute('''SELECT * FROM notification_outbox WHERE deliverable=1
                ORDER BY delivery_priority,sequence LIMIT 2''').fetchall()
            events = [] if not rows else [self._notification_projection(connection, rows[0], now)]
            return NotificationResult(command.op, dict(schema_version=1, profile='default', source='local',
                events=events, next_cursor=events[0]['event_id'] if len(rows) > 1 else None, has_more=len(rows) > 1))

    def claim_notification(self, command, *, deadline=None):
        if type(command) is not NotificationClaimCommand:
            raise ValueError('invalid notification claim')
        with self._notification_transaction(deadline=deadline, write=True) as connection:
            now = time.time_ns() // 1_000_000
            row = connection.execute('''SELECT * FROM notification_outbox WHERE deliverable=1
                AND presentation_state='claimed' AND consumer_id=? AND lease_expires_at_ms>?
                ORDER BY delivery_priority,sequence LIMIT 1''', (command.consumer_id, now)).fetchone()
            if row is None:
                row = connection.execute('''SELECT * FROM notification_outbox WHERE deliverable=1
                    AND (presentation_state='pending' OR lease_expires_at_ms<=?)
                    ORDER BY delivery_priority,sequence LIMIT 1''', (now,)).fetchone()
            claims = []
            if row is not None:
                event = self._notification_projection(connection, row, now)
                if row['presentation_state'] == 'claimed' and row['lease_expires_at_ms'] > now:
                    history = connection.execute('SELECT * FROM notification_claims WHERE sequence=? AND claim_token=?',
                        (row['sequence'], row['claim_token'])).fetchone()
                    if history is None or (history['consumer_id'], history['lease_expires_at_ms'], history['outcome']) != (
                            command.consumer_id, row['lease_expires_at_ms'], 'claimed'):
                        raise ValueError('notification claim history changed')
                    token, expiry = row['claim_token'], row['lease_expires_at_ms']
                else:
                    token, expiry = secrets.token_hex(32), now + 30_000
                    connection.execute('''UPDATE notification_outbox SET presentation_state='claimed',
                        consumer_id=?,claim_token=?,lease_expires_at_ms=? WHERE sequence=?''',
                        (command.consumer_id, token, expiry, row['sequence']))
                    connection.execute('INSERT INTO notification_claims VALUES (?,?,?,?,?,NULL)',
                        (row['sequence'], token, command.consumer_id, expiry, 'claimed'))
                event['presentation_state'] = 'claimed'
                claims.append(dict(event=event, claim_token=token, lease_expires_at_ms=expiry))
            return NotificationResult(command.op, dict(schema_version=1, profile='default', source='local',
                claims=claims, server_now_ms=now))

    def _settle_notification(self, command, *, deadline):
        presented = type(command) is NotificationPresentedCommand
        with self._notification_transaction(deadline=deadline, write=True) as connection:
            identity, sequence = command.event_id.rsplit(':', 1)
            row = connection.execute('SELECT * FROM notification_outbox WHERE sequence=? AND store_uuid=?',
                (int(sequence), identity)).fetchone()
            status = 'stale_claim'
            if row is not None and identity == self._notification_store_uuid(connection):
                history = connection.execute('SELECT * FROM notification_claims WHERE sequence=? AND claim_token=?',
                    (row['sequence'], command.claim_token)).fetchone()
                outcome = 'presented' if presented else 'released'
                receipt = command.notification_id if presented else None
                exact = history is not None and history['consumer_id'] == command.consumer_id
                if exact and (history['outcome'], history['notification_id']) == (outcome, receipt):
                    status = 'already_' + outcome
                elif (exact and history['outcome'] == 'claimed' and row['presentation_state'] == 'claimed'
                    and (row['consumer_id'], row['claim_token'], row['lease_expires_at_ms']) ==
                        (command.consumer_id, command.claim_token, history['lease_expires_at_ms'])
                    and row['lease_expires_at_ms'] > time.time_ns() // 1_000_000):
                    connection.execute('UPDATE notification_claims SET outcome=?,notification_id=? WHERE sequence=? AND claim_token=?',
                        (outcome, receipt, row['sequence'], command.claim_token))
                    if presented:
                        connection.execute("UPDATE notification_outbox SET presentation_state='presented' WHERE sequence=?", (row['sequence'],))
                    else:
                        connection.execute("UPDATE notification_outbox SET presentation_state='pending',consumer_id=NULL,claim_token=NULL,lease_expires_at_ms=NULL WHERE sequence=?", (row['sequence'],))
                    status = outcome
            return NotificationResult(command.op, dict(status=status))

    def present_notification(self, command, *, deadline=None):
        if type(command) is not NotificationPresentedCommand:
            raise ValueError('invalid notification presentation')
        return self._settle_notification(command, deadline=deadline)

    def release_notification(self, command, *, deadline=None):
        if type(command) is not NotificationReleaseCommand:
            raise ValueError('invalid notification release')
        return self._settle_notification(command, deadline=deadline)

    def read_query_list_page(self, *, cursor=None, deadline=None) -> QueryListPage:
        """Finite live keyset, bounded by the first page's maximal ID and epoch."""
        connection = self._connection
        if connection.in_transaction:
            raise ValueError('query cannot borrow a transaction')
        selector = validate_query_cursor(cursor, 'list')
        with self._batch_budget(deadline=deadline):
            connection.execute('BEGIN')
            try:
                epoch = _query_counter(self._current_worker_epoch(connection), positive=True)
                if type(selector) is QueryCursor:
                    if selector.epoch != epoch:
                        raise ValueError('query epoch changed')
                    after, end = selector.after, selector.end
                else:
                    after = selector or ''
                    end = connection.execute('SELECT MAX(job_id) FROM jobs').fetchone()[0]
                    if end is not None:
                        QueryCursor('list', epoch, end, end)
                rows = connection.execute('''SELECT job_id, generation, revision, state FROM jobs
                    WHERE job_id > ? AND job_id <= ? ORDER BY job_id LIMIT 101''', (after, end)).fetchall()
                jobs = tuple(PublicJobRecord(row['job_id'],
                    _query_counter(row['generation']), _query_counter(row['revision']), row['state']) for row in rows)
                has_more = len(jobs) > 100
                next_cursor = QueryCursor('list', epoch, jobs[99].job, end).encode() if has_more else None
                return QueryListPage(jobs[:100], next_cursor, has_more)
            finally:
                connection.rollback()

    def read_query_status(self, *, id, deadline=None) -> QueryStatus:
        connection = self._connection
        if connection.in_transaction:
            raise ValueError('query cannot borrow a transaction')
        QueryStatus(id, None)
        with self._batch_budget(deadline=deadline):
            connection.execute('BEGIN')
            try:
                _query_counter(self._current_worker_epoch(connection), positive=True)
                row = connection.execute('''SELECT job_id, generation, revision, state
                    FROM jobs WHERE job_id = ?''', (id,)).fetchone()
                record = None if row is None else PublicJobRecord(row['job_id'],
                    _query_counter(row['generation']), _query_counter(row['revision']), row['state'])
                return QueryStatus(id, record)
            finally:
                connection.rollback()

    def read_query_events_page(self, *, cursor=None, deadline=None) -> QueryEventsPage:
        connection = self._connection
        if connection.in_transaction:
            raise ValueError('query cannot borrow a transaction')
        selector = validate_query_cursor(cursor, 'events')
        with self._batch_budget(deadline=deadline):
            connection.execute('BEGIN')
            try:
                epoch = _query_counter(self._current_worker_epoch(connection), positive=True)
                if selector is not None:
                    if selector.epoch != epoch:
                        raise ValueError('query epoch changed')
                    after, end = selector.after, selector.end
                else:
                    after = 0
                    end = connection.execute('SELECT MAX(event_id) FROM events').fetchone()[0]
                    if end is not None:
                        _query_counter(end, positive=True)
                rows = connection.execute('''SELECT event_id, kind, job_id, generation, revision FROM events
                    WHERE event_id > ? AND event_id <= ? ORDER BY event_id LIMIT 101''', (after, end)).fetchall()
                events = tuple(QueryEventRecord(row['event_id'], row['kind'], row['job_id'],
                    row['generation'], row['revision']) for row in rows)
                has_more = len(events) > 100
                next_cursor = QueryCursor('events', epoch, events[99].event_id, end).encode() if has_more else None
                return QueryEventsPage(events[:100], next_cursor, has_more)
            finally:
                connection.rollback()

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

    def _saved_destination(self, job_id):
        return self.runtime_destination(job_id)

    def _latest_body_source(self, connection, job_id):
        row = connection.execute("SELECT request_id FROM target_dispatch_causes WHERE job_id=? AND kind='body' ORDER BY admission_serial DESC LIMIT 1",(job_id,)).fetchone()
        return None if row is None else row[0]

    def _continuation_table(self, connection, request):
        tables = [table for table in ('saved_direct_continuations','saved_direct_cycles')
            if connection.execute(f'SELECT 1 FROM {table} WHERE request_id=?',(request,)).fetchone()]
        if len(tables)!=1: raise ValueError('continuation identity ambiguous')
        return tables[0]

    def _continuation_row(self, connection, job_id, request=None):
        if request is None:
            head = connection.execute('SELECT activation_request_id FROM job_authorization_heads WHERE job_id=?',(job_id,)).fetchone()
            if head is None: return None
            request = head[0]
        rows = connection.execute('SELECT * FROM saved_direct_continuations WHERE request_id=? UNION ALL SELECT * FROM saved_direct_cycles WHERE request_id=?',(request,request)).fetchall()
        if not rows: return None
        if len(rows)!=1 or rows[0]['job_id']!=job_id: raise ValueError('continuation identity changed')
        return rows[0]

    def _saved_checkpoint(self, connection, job_id, source_request_id=None):
        from hermes_downloads.paths import _StoppedPair
        source = source_request_id or self._latest_body_source(connection,job_id)
        rows = connection.execute("SELECT *, 'pause' AS stop_kind,22 AS version FROM stopped_direct_bodies WHERE source_request_id=? UNION ALL SELECT *,23 AS version FROM stopped_direct_cycles WHERE source_request_id=? UNION ALL SELECT *,24 AS version FROM stopped_failed_bodies WHERE source_request_id=? UNION ALL SELECT *,24 AS version FROM stopped_control_bodies WHERE source_request_id=? UNION ALL SELECT *,24 AS version FROM stopped_absent_bodies WHERE source_request_id=?",(source,source,source,source,source)).fetchall()
        if not rows: return None
        if len(rows)!=1 or rows[0]['job_id']!=job_id: raise ValueError('checkpoint source ambiguous')
        row = rows[0]
        facts = _StoppedPair.from_record(json.loads(row['facts']))
        closure = json.loads(row['closure'])
        value = dict(source=row['source_request_id'],pause=row['pause_request_id'],audit=row['pause_audit_id'],
            epoch=row['worker_epoch'],generation=row['generation'],revision=row['revision'],facts=facts.to_record(),closure=closure)
        if row['version']>=23: value['stop_kind']=row['stop_kind']
        digest = hashlib.sha256(_batch_canonical(value,16384)).hexdigest()
        if digest!=row['checkpoint_digest']: raise ValueError('stopped checkpoint digest changed')
        receipt = self._read_direct_dispatch_command(connection,source)
        cause = self._read_target_dispatch_cause(connection,source,{})
        audit = connection.execute('SELECT * FROM events WHERE event_id=?',(row['pause_audit_id'],)).fetchone()
        if (receipt is None or receipt.status!='started' or receipt.state!='downloading' or receipt.job!=job_id
            or cause is None or cause['kind']!='body'
            or audit is None or (audit['kind'],audit['job_id'],audit['generation'],audit['revision'])!=(('job_failure' if row['stop_kind']=='failure' else 'job_paused'),job_id,row['generation'],row['revision'])
            or set(closure)-{'stock'}!={'process','returncode','group_absent','observer_joined','gid','completed_length'}
            or closure['group_absent'] is not True
            or (closure['completed_length'] is not None if row['stop_kind']=='cold_absent' else type(closure['completed_length']) is not int or not 0<closure['completed_length']<=facts.payload[2])
            or type(closure['gid']) is not str or re.fullmatch('[0-9a-fA-F]{16}',closure['gid']) is None):
            raise ValueError('stopped BODY provenance changed')
        ProcessBirthIdentity.from_record(closure['process'])
        if row['stop_kind'] in {'cold','cold_absent'}:
            owner = connection.execute('SELECT * FROM saved_body_owners WHERE request_id=?',(source,)).fetchone()
            if (owner is None or json.loads(owner['process'])!=closure['process'] or owner['gid']!=closure['gid']
                or closure['observer_joined'] is not False or closure['returncode'] is not None):
                raise ValueError('cold containment provenance changed')
            fences=connection.execute('SELECT * FROM saved_cold_fences WHERE audit_id=?',(row['pause_audit_id'],)).fetchone()
            if fences is None or (fences['old_generation'],fences['generation'])!=(receipt.generation,row['generation']):
                raise ValueError('cold stopped fence missing')
        else:
            if receipt.generation!=row['generation'] or closure['observer_joined'] is not True or type(closure['returncode']) is not int:
                raise ValueError('original owner drain missing')
            if row['stop_kind']=='failure':
                failure=connection.execute('SELECT * FROM direct_failures WHERE body_request_id=?',(source,)).fetchone()
                if (failure is None or (failure['job_id'],failure['worker_epoch'],failure['generation'],failure['revision'],failure['audit_id'])!=(job_id,row['worker_epoch'],row['generation'],row['revision'],row['pause_audit_id'])
                    or any(json.loads(failure['closure'])[k]!=closure[k] for k in ('process','returncode','group_absent','observer_joined','gid'))): raise ValueError('failure stop provenance changed')
                self._failure_state_for_audit(connection,job_id,row['generation'],row['revision'],row['pause_audit_id'])
            elif row['stop_kind']=='pause':
                pause=connection.execute('SELECT * FROM job_control_commands WHERE request_id=?',(row['pause_request_id'],)).fetchone()
                if pause is None or (pause['job_id'],pause['action'],pause['status'],pause['generation'],pause['revision'],pause['state'])!=(job_id,'pause','applied',row['generation'],row['revision'],'paused'):
                    raise ValueError('pause provenance changed')
            elif row['stop_kind']=='queue':
                pause=connection.execute('SELECT gate FROM queue_commands WHERE request_id=?',(row['pause_request_id'],)).fetchone()
                if pause is None or pause[0]!='paused': raise ValueError('queue stop provenance changed')
            elif row['stop_kind']=='configure':
                self.configuration_command(request_id=row['pause_request_id'])
            elif row['stop_kind']=='replace_source':
                self._source_request(row['pause_request_id'])
            elif row['stop_kind']=='preempt':
                victim=self._preemption(row['pause_request_id'])
                if victim is None or (victim['job_id'],victim['source_request_id'],victim['worker_epoch'],victim['generation'],victim['revision'])!=(job_id,source,row['worker_epoch'],row['generation'],row['revision']-1) or json.loads(victim['process'])!=closure['process']:raise ValueError('preemption stop changed')
            elif row['stop_kind']=='collection':
                _,control,_=self._control_request(row['pause_request_id'])
                member=connection.execute('SELECT generation,revision FROM direct_control_members WHERE request_id=? AND job_id=?',(row['pause_request_id'],job_id)).fetchone()
                if control.scope!='collection' or control.action!='pause' or control.collection!=self.get_materialized_job(job_id).queue_collection_id or member is None or tuple(member)!=(row['generation'],row['revision']-1):raise ValueError('collection stopped member changed')
            elif row['stop_kind']=='purge':
                self._file_request(connection,row['pause_request_id'])
            elif row['stop_kind']=='edit':
                self._edit_request(connection,row['pause_request_id'])
            elif row['stop_kind']!='shutdown': raise ValueError('unknown stop provenance')
        self._checkpoint_stock(connection,row)
        return row,facts

    def _checkpoint_stock(self, connection, row):
        from hermes_downloads.direct import _StockResult
        closure=json.loads(row['closure']);value=closure.get('stock')
        if value is None:return None
        value=dict(value);value['owner']=tuple(value['owner']) if value.get('owner') is not None else None
        stock=_StockResult(**value)
        binding=connection.execute('SELECT owner FROM direct_stock_bindings WHERE body_request_id=?',(row['source_request_id'],)).fetchone()
        receipt=self._read_direct_dispatch_command(connection,row['source_request_id'])
        if (binding is None or stock.owner is None or tuple(json.loads(binding[0]))!=stock.owner
            or stock.owner[:5]!=(row['job_id'],receipt.generation,closure['gid'],row['worker_epoch'],row['source_request_id'])
            or stock.owner[5]!=hashlib.sha256(json.dumps(closure['process'],sort_keys=True,separators=(',',':')).encode()).hexdigest()):
            raise ValueError('saved stock identity binding changed')
        return stock

    def seal_stopped_direct_body(self, plan, pause, result, facts, closure, *, stop_kind='pause'):
        connection=self._connection
        connection.execute('BEGIN IMMEDIATE')
        with connection:
            current=self._read_job_control_projection(connection,plan.job.job_id)
            if (current.generation,current.revision,current.state)!=(result.generation,result.revision,result.state) or (current.state!='paused' and stop_kind!='failure'):
                raise ValueError('stopped checkpoint stale')
            origin=self._read_creation_origin(connection,current.job,{},target=False)
            if origin.kind!='batch': return
            if self._saved_checkpoint(connection,current.job,plan.request_id) is not None: return
            audit=self._target_latest(connection,current.job)['event_id'];epoch=self._current_worker_epoch(connection)
            request=pause.request_id if pause is not None else ('cold:'+str(audit) if stop_kind in {'cold','cold_absent'} else ('failure:'+plan.request_id if stop_kind=='failure' else 'shutdown:'+plan.request_id))
            cause=self._read_target_dispatch_cause(connection,plan.request_id,{})
            legacy = stop_kind=='pause' and cause['admission_serial']==1 and not connection.execute('SELECT 1 FROM stopped_direct_bodies WHERE job_id=?',(current.job,)).fetchone()
            value=dict(source=plan.request_id,pause=request,audit=audit,epoch=epoch,generation=current.generation,
                revision=current.revision,facts=facts.to_record(),closure=closure)
            if not legacy: value['stop_kind']=stop_kind
            digest=hashlib.sha256(_batch_canonical(value,16384)).hexdigest()
            values=(current.job,plan.request_id,request,audit,epoch,current.generation,current.revision,digest,
                json.dumps(facts.to_record(),separators=(',',':'),sort_keys=True),json.dumps(closure,separators=(',',':'),sort_keys=True))
            if legacy: connection.execute('INSERT INTO stopped_direct_bodies VALUES (?,?,?,?,?,?,?,?,?,?)',values)
            else:
                table='stopped_absent_bodies' if stop_kind=='cold_absent' else 'stopped_failed_bodies' if stop_kind=='failure' else 'stopped_control_bodies' if stop_kind in {'configure','replace_source','edit','purge','collection','preempt'} else 'stopped_direct_cycles'
                connection.execute(f'INSERT INTO {table} VALUES (?,?,?,?,?,?,?,?,?,?,?)',(*values,stop_kind))
            self._saved_checkpoint(connection,current.job,plan.request_id)

    def _checkpoint_fence(self, connection, row, *, through=None):
        epoch,generation,revision=row['worker_epoch'],row['generation'],row['revision']
        source=row['source_request_id'] if 'source_request_id' in row.keys() else connection.execute('SELECT source_request_id FROM saved_resume_bindings WHERE request_id=?',(row['request_id'],)).fetchone()[0]
        actions=[(fence['audit_id'],'cold',fence) for fence in connection.execute('SELECT * FROM saved_cold_fences WHERE job_id=? AND new_epoch>?',(row['job_id'],epoch))]
        actions.extend((edit['audit_id'],'edit',edit) for edit in connection.execute('SELECT * FROM direct_job_edits WHERE job_id=? AND (generation>? OR (generation=? AND revision>?))',(row['job_id'],generation,generation,revision)))
        actions.extend((reopened['audit_id'],'retry',reopened) for reopened in connection.execute('SELECT * FROM direct_retry_reopens WHERE job_id=? AND generation>?',(row['job_id'],generation)))
        for _,kind,fence in sorted(actions,key=lambda action:action[0]):
            if through is not None and (fence['generation'],fence['revision'])>(through[1],through[2]):continue
            if kind=='retry':
                reopened=self._retry_reopen(connection,fence['audit_id'])
                if (reopened['worker_epoch'],reopened['old_generation'],reopened['old_revision'],reopened['source_request_id'])!=(epoch,generation,revision,source):raise ValueError('retry checkpoint chain changed')
                generation,revision=reopened['generation'],reopened['revision'];continue
            if kind=='edit':
                intent,_,_=self._edit_request(connection,fence['request_id'])
                self._effective_settings(connection,self.get_materialized_job(row['job_id']))
                if (intent['worker_epoch'],fence['generation'],fence['previous_revision'],fence['source_request_id'])!=(epoch,generation,revision,source):raise ValueError('edited checkpoint chain changed')
                revision=fence['revision']
                continue
            if (fence['old_epoch'],fence['old_generation'],fence['old_revision'])!=(epoch,generation,revision):raise ValueError('cold checkpoint chain changed')
            if (fence['new_epoch'],fence['generation'],fence['revision'])!=(epoch+1,generation+1,revision+1):raise ValueError('cold checkpoint counters changed')
            audit=connection.execute('SELECT kind,job_id,generation,revision FROM events WHERE event_id=?',(fence['audit_id'],)).fetchone()
            if audit is None or tuple(audit)!=('job_paused',row['job_id'],fence['generation'],fence['revision']):raise ValueError('cold checkpoint audit changed')
            epoch,generation,revision=fence['new_epoch'],fence['generation'],fence['revision']
        return epoch,generation,revision

    def _reusable_prepared(self, connection, job_id, source):
        bindings=connection.execute('SELECT * FROM saved_resume_bindings WHERE source_request_id=?',(source,)).fetchall()
        if not bindings: return None
        leaves=[binding for binding in bindings if not connection.execute('SELECT 1 FROM saved_resume_bindings WHERE prepared_from=?',(binding['request_id'],)).fetchone()]
        if len(leaves)!=1: raise ValueError('prepared lineage ambiguous')
        binding=leaves[0];row=self._continuation_row(connection,job_id,binding['request_id'])
        if row is None or row['phase']!='ready' or row['worker_epoch']>=self._current_worker_epoch(connection) or binding['drained']!=1:
            raise ValueError('prepared owner drain unavailable')
        self._require_ready_saved_pair(connection,job_id,row['request_id'])
        return row,binding

    def _saved_resume_eligible(self, connection, origin, current, head, command=None):
        from hermes_downloads.paths import _require_saved_pair
        if command is not None and self._fresh_retry(connection,current.job,command.request_id): return True
        if head is not None and head.last_admission_serial==0: return True
        if origin.kind!='batch' or head is None or current.state not in {'paused','retry_wait','blocked','needs_auth','needs_link'}: return False
        checkpoint=self._saved_checkpoint(connection,current.job)
        if checkpoint is None: return False
        row,facts=checkpoint
        if origin.batch.expected_sha256 is None:
            stock=self._checkpoint_stock(connection,row)
            if stock is None or not stock.known or stock.strong_validator is None or stock.resource_digest is None or stock.total_length is None:return False
        budget=self.get_retry_budget(current.job)
        if budget is not None and (budget.generation!=current.generation or budget.exhausted): return False
        prior=connection.execute('SELECT 1 FROM saved_direct_continuations WHERE checkpoint_digest=? UNION ALL SELECT 1 FROM saved_direct_cycles WHERE checkpoint_digest=?',(row['checkpoint_digest'],row['checkpoint_digest'])).fetchone()
        if prior:
            try:
                prepared=self._reusable_prepared(connection,current.job,row['source_request_id'])
                return prepared is not None and self._checkpoint_fence(connection,prepared[0])==(
                    self._current_worker_epoch(connection),current.generation,current.revision)
            except (OSError,ValueError): return False
        if self._checkpoint_fence(connection,row)!=(self._current_worker_epoch(connection),current.generation,current.revision): return False
        try: _require_saved_pair(self._saved_destination(current.job),self._effective_reservation(connection,current.job),facts)
        except (OSError,ValueError): return False
        return True

    def _create_saved_continuation(self, connection, command, job_id):
        from hermes_downloads.paths import _SAVED_NAMES
        if self._fresh_retry(connection,job_id,command.request_id): return
        row,_=self._saved_checkpoint(connection,job_id)
        current=self._read_job_control_projection(connection,job_id)
        prepared=self._reusable_prepared(connection,job_id,row['source_request_id'])
        legacy=prepared is None and not connection.execute('SELECT 1 FROM saved_direct_continuations WHERE job_id=?',(job_id,)).fetchone() and row['version']==22
        names=_SAVED_NAMES if legacy else tuple('.hermes-resume-'+hashlib.sha256(row['source_request_id'].encode()).hexdigest()+'-'+role for role in ('payload','control'))
        if prepared is not None: names=tuple(json.loads(prepared[1]['retained_names']))
        table='saved_direct_continuations' if legacy else 'saved_direct_cycles'
        connection.execute(f'INSERT INTO {table} VALUES (?,?,?,?,?,?,?,?,?,NULL)',(job_id,command.request_id,command.payload_digest,
            row['checkpoint_digest'],command.expected_worker_epoch,current.generation,current.revision,
            'intent' if prepared is None else 'ready','{}' if prepared is None else prepared[0]['facts']))
        connection.execute('INSERT INTO saved_resume_bindings VALUES (?,?,?,?,?)',(command.request_id,row['source_request_id'],
            json.dumps(names,separators=(',',':')),int(prepared is not None),None if prepared is None else prepared[0]['request_id']))
        budget=self.get_retry_budget(job_id)
        if budget is not None and budget.paused:
            authority=RetryAuthority.restore(policy=RetryPolicy(),budget=budget);authority.reopen_ordinary(generation=current.generation)
            self._replace_retry_budget(connection,authority.budget)

    def _saved_continuation(self, connection, job_id, request=None):
        from hermes_downloads.paths import _SAVED_NAMES
        row=self._continuation_row(connection,job_id,request)
        if row is None: return None
        binding=connection.execute('SELECT * FROM saved_resume_bindings WHERE request_id=?',(row['request_id'],)).fetchone()
        if binding is None:
            source=connection.execute('SELECT source_request_id FROM stopped_direct_bodies WHERE job_id=? AND checkpoint_digest=?',(job_id,row['checkpoint_digest'])).fetchone()
            if source is None: raise ValueError('legacy continuation checkpoint missing')
            source=source[0];names=_SAVED_NAMES
        else:
            source=binding['source_request_id'];names=tuple(json.loads(binding['retained_names']))
            fixed=tuple('.hermes-resume-'+hashlib.sha256(source.encode()).hexdigest()+'-'+role for role in ('payload','control'))
            if names not in (_SAVED_NAMES,fixed): raise ValueError('retained names changed')
            if binding['prepared_from'] is not None:
                previous=connection.execute('SELECT * FROM saved_resume_bindings WHERE request_id=?',(binding['prepared_from'],)).fetchone()
                prior=self._continuation_row(connection,job_id,binding['prepared_from'])
                if (previous is None or previous['source_request_id']!=source or previous['retained_names']!=binding['retained_names']
                    or previous['drained']!=1 or prior is None or prior['phase']!='ready' or prior['facts']!=row['facts']
                    or prior['worker_epoch']>=row['worker_epoch']): raise ValueError('prepared drain lineage changed')
            elif names==_SAVED_NAMES and self._continuation_table(connection,row['request_id'])!='saved_direct_continuations':
                raise ValueError('legacy retained names changed')
        checkpoint=self._saved_checkpoint(connection,job_id,source)
        if checkpoint is None or checkpoint[0]['checkpoint_digest']!=row['checkpoint_digest']: raise ValueError('continuation checkpoint changed')
        command,_,members=self._read_target_command(connection,row['request_id'],{})
        selected=[m for m in members if m['job_id']==job_id and m['outcome']=='existing_authority']
        if len(selected)!=1: raise ValueError('continuation member missing')
        member=selected[0];prefix='intent' if member['intent_audit_id'] is not None else 'captured'
        if ((row['generation'],row['revision'])!=(member[prefix+'_generation'],member[prefix+'_revision'])
            or command.action not in {'resume','start_now'} or command.payload_digest!=row['payload_digest'] or command.expected_worker_epoch!=row['worker_epoch']):
            raise ValueError('continuation intent changed')
        # Validate the captured cold chain as it existed at consent, not a later current projection.
        predecessor=prior if binding is not None and binding['prepared_from'] is not None else checkpoint[0]
        epoch,generation,revision=self._checkpoint_fence(connection,predecessor,through=(row['worker_epoch'],row['generation'],row['revision']-1))
        if (row['worker_epoch'],row['generation'],row['revision'])!=(epoch,generation,revision+1): raise ValueError('continuation intent fences changed')
        cause=self._read_target_dispatch_cause(connection,source,{})
        if row['body_request_id'] is not None:
            body=connection.execute('SELECT job_id,activation_request_id,admission_serial FROM target_dispatch_causes WHERE request_id=?',(row['body_request_id'],)).fetchone()
            if body is None or tuple(body)!=(job_id,row['request_id'],cause['admission_serial']+1): raise ValueError('continuation serial changed')
        return _SavedResumePlan(job_id,row['request_id'],row['checkpoint_digest'],row['worker_epoch'],row['generation'],row['revision'],source,names,cause['admission_serial'])

    def record_saved_phase(self, plan, phase, facts):
        connection=self._connection;connection.execute('BEGIN IMMEDIATE')
        with connection:
            current=self._read_job_control_projection(connection,plan.job_id);head=self._capture_target_head(connection,plan.job_id)
            if (self._saved_continuation(connection,plan.job_id)!=plan or self._current_worker_epoch(connection)!=plan.worker_epoch
                or (current.generation,current.revision,current.state)!=(plan.generation,plan.revision,'queued')
                or head.activation_request_id!=plan.request_id or head.last_admission_serial!=plan.source_serial
                or not self._direct_dispatch_admission(connection,materialized=self.get_materialized_job(plan.job_id),now=datetime.now(UTC)).allowed):
                raise ValueError('saved preparation revoked')
            row=self._continuation_row(connection,plan.job_id,plan.request_id);old=row['phase']
            if (old,phase) not in {('intent','linked'),('linked','retired'),('retired','ready')}: raise ValueError('saved phase changed')
            history=json.loads(row['facts']);expected={'intent':set(),'linked':{'linked'},'retired':{'linked','retired'}}[old]
            if set(history)!=expected: raise ValueError('saved phase facts changed')
            history[phase]=facts;table=self._continuation_table(connection,plan.request_id)
            connection.execute(f'UPDATE {table} SET phase=?,facts=? WHERE request_id=? AND phase=?',(phase,json.dumps(history,separators=(',',':'),sort_keys=True),plan.request_id,old))
            self._require_one_changed_row(connection,'saved phase CAS')
            if phase=='ready': self._require_ready_saved_pair(connection,plan.job_id,plan.request_id,require_drain=False)

    def qualify_saved_drain(self, plan, *, _existing=False):
        current=self._read_job_control_projection(self._connection,plan.job_id)
        if (self._saved_continuation(self._connection,plan.job_id,plan.request_id)!=plan
            or self.worker_epoch()!=plan.worker_epoch
            or (current.generation,current.revision,current.state)!=(plan.generation,plan.revision,'queued')):raise ValueError('saved drain changed')
        self._require_ready_saved_pair(self._connection,plan.job_id,plan.request_id,require_drain=_existing)
        if _existing:return
        self._connection.execute('UPDATE saved_resume_bindings SET drained=1 WHERE request_id=? AND drained=0',(plan.request_id,))
        self._require_one_changed_row(self._connection,'saved original observation JOIN')

    def _require_ready_saved_pair(self, connection, job_id, request=None, *, require_drain=True):
        from hermes_downloads.paths import _require_saved_pair
        plan=self._saved_continuation(connection,job_id,request)
        if plan is None: raise ValueError('saved continuation missing')
        _,facts=self._saved_checkpoint(connection,job_id,plan.source_request_id)
        row=self._continuation_row(connection,job_id,plan.request_id)
        if row['phase'] not in {'ready','admitted'}: raise ValueError('saved pair not ready')
        binding=connection.execute('SELECT drained FROM saved_resume_bindings WHERE request_id=?',(plan.request_id,)).fetchone()
        if require_drain and (binding is None or binding[0]!=1): raise ValueError('saved original drain missing')
        history=json.loads(row['facts'])
        if set(history)!={'linked','retired','ready'}: raise ValueError('saved phase history incomplete')
        evidence=history['ready']
        if history['retired']!=dict(retained=evidence['retained']) or set(evidence)!={'retained','candidates'}: raise ValueError('retained phase facts changed')
        _require_saved_pair(self._saved_destination(job_id),self._effective_reservation(connection,job_id),facts,
            retained=evidence['retained'],candidates=evidence['candidates'],retained_names=plan.retained_names)
        return row

    def _block_saved_decision(self, job_id):
        connection=self._connection;connection.execute('BEGIN IMMEDIATE')
        with connection:
            self._record_saved_decision(connection,job_id)

    def _record_saved_decision(self, connection, job_id):
        if not connection.in_transaction:raise ValueError('saved decision transaction missing')
        current=self._read_job_control_projection(connection,job_id)
        if current.state not in {'queued','retry_wait','paused'}:return
        self._persist_direct_dispatch_lifecycle(connection,current=current,state='blocked',event_kind='job_resume_blocked')

    def _saved_decision_for_audit(self, connection, job_id, generation, revision):
        # The decision is bound either to a failed saved preparation or the
        # original typed retry failure. Both carriers are immutable and fenced.
        for table in ('saved_direct_continuations','saved_direct_cycles'):
            if connection.execute(f"SELECT 1 FROM {table} WHERE job_id=? AND generation=? AND revision=? AND phase='failed'",(job_id,generation,revision-1)).fetchone():return 'blocked'
        if connection.execute("SELECT 1 FROM direct_failures WHERE job_id=? AND generation=? AND revision=? AND state='retry_wait'",(job_id,generation,revision-1)).fetchone():return 'blocked'
        for table in ('stopped_direct_bodies','stopped_direct_cycles','stopped_control_bodies','stopped_failed_bodies','stopped_absent_bodies'):
            if connection.execute(f'SELECT 1 FROM {table} WHERE job_id=? AND generation=? AND revision=?',(job_id,generation,revision-1)).fetchone():return 'blocked'
        if connection.execute('SELECT 1 FROM saved_cold_fences WHERE job_id=? AND generation=? AND revision=?',(job_id,generation,revision-1)).fetchone():return 'blocked'
        raise ValueError('saved source decision provenance changed')

    def fail_saved_preparation(self, plan, *, pause=True, _identity_decision=False):
        table=self._continuation_table(self._connection,plan.request_id)
        self._connection.execute(f"UPDATE {table} SET phase='failed' WHERE request_id=? AND phase!='admitted'",(plan.request_id,))
        current=self.get_job(plan.job_id)
        if pause and current.state=='queued':
            if _identity_decision:self._block_saved_decision(plan.job_id)
            else:self.apply_job_control(job_id=plan.job_id,action='pause',request_id='saved-failed:'+plan.checkpoint_digest,payload_digest=plan.checkpoint_digest,expected_revision=current.revision)

    def cold_body_owner(self, record):
        rows=self._connection.execute('SELECT * FROM saved_body_owners WHERE worker_epoch=?',(record.worker_epoch,)).fetchall()
        selected=[]
        for owner in rows:
            if json.loads(owner['process'])!=record.identity.to_record(): continue
            receipt=self._read_direct_dispatch_command(self._connection,owner['request_id'])
            cause=self._read_target_dispatch_cause(self._connection,owner['request_id'],{})
            if receipt is None or cause is None or cause['kind']!='body' or receipt.status!='started': continue
            fence=self._connection.execute('SELECT * FROM saved_cold_fences WHERE job_id=? AND new_epoch=?',(receipt.job,self.worker_epoch())).fetchone()
            if (fence is not None and fence['old_state']=='downloading' and fence['old_epoch']==record.worker_epoch
                and fence['old_generation']==receipt.generation and self._latest_body_source(self._connection,receipt.job)==owner['request_id']
                and self._saved_checkpoint(self._connection,receipt.job,owner['request_id']) is None):
                selected.append((owner,receipt))
        if len(selected)>1: raise ValueError('cold BODY ownership ambiguous')
        return selected[0] if selected else None

    def seal_cold_body(self, record, owned, completed, facts):
        owner,receipt=owned
        if self.cold_body_owner(record)!=owned: raise ValueError('cold BODY ownership changed')
        job=self.get_materialized_job(receipt.job)
        current=self.get_job(receipt.job)
        plan=_DirectDispatchPlan(job,self._effective_reservation(self._connection,receipt.job),
            self._direct_dispatch_admission(self._connection,materialized=job,now=datetime.now(UTC)),
            receipt.generation,receipt.revision,receipt.request_id,receipt.payload_digest)
        closure=dict(process=record.identity.to_record(),returncode=None,group_absent=True,observer_joined=False,
            gid=owner['gid'],completed_length=completed)
        self.seal_stopped_direct_body(plan,None,current,facts,closure,stop_kind='cold_absent' if completed is None else 'cold')

    def retained_cleanup_records(self, plan, *, authorize=False):
        from hermes_downloads.paths import _saved_open_chain,_saved_visible,_STAGED_PARTIAL_READ_FLAGS
        import os
        connection=self._connection
        if authorize: connection.execute('BEGIN IMMEDIATE')
        try:
            claim=self._require_cleanup_plan(connection,plan,{})
            records=[]
            rows=connection.execute("SELECT * FROM saved_direct_continuations WHERE job_id=? AND phase='admitted' UNION ALL SELECT * FROM saved_direct_cycles WHERE job_id=? AND phase='admitted'",(claim.job_id,claim.job_id)).fetchall()
            for row in rows:
                continuation=self._saved_continuation(connection,claim.job_id,row['request_id'])
                checkpoint,pair=self._saved_checkpoint(connection,claim.job_id,continuation.source_request_id)
                history=json.loads(row['facts'])
                if set(history)!={'linked','retired','ready'} or history['retired']!=dict(retained=history['ready']['retained']):
                    raise ValueError('retained cleanup history changed')
                for role,(name,facts) in enumerate(zip(continuation.retained_names,history['ready']['retained'])):
                    encoded=json.dumps(facts,separators=(',',':'))
                    previous=connection.execute('SELECT * FROM saved_retained_cleanup WHERE request_id=? AND role=?',(row['request_id'],role)).fetchone()
                    if previous is None:
                        if not authorize: raise ValueError('retained cleanup authority missing')
                        descriptors,_,_=_saved_open_chain(claim.attempt.prepared.destination,claim.attempt.prepared.reservation,pair)
                        fd=None
                        try:
                            fd=os.open(name,_STAGED_PARTIAL_READ_FLAGS,dir_fd=descriptors[3])
                            if _saved_visible(descriptors[3],name,fd)!=tuple(facts): raise ValueError('retained cleanup source changed')
                        finally:
                            if fd is not None: os.close(fd)
                            for descriptor in reversed(descriptors): os.close(descriptor)
                        connection.execute("INSERT INTO saved_retained_cleanup VALUES (?,?,?,?,?,'authorized')",(row['request_id'],role,claim.attempt_id,name,encoded))
                        phase='authorized'
                    else:
                        if (previous['attempt_id'],previous['basename'],previous['facts'])!=(claim.attempt_id,name,encoded):
                            raise ValueError('retained cleanup authority changed')
                        phase=previous['phase']
                    records.append(dict(request_id=row['request_id'],source_request_id=continuation.source_request_id,role=role,
                        basename=name,facts=facts,phase=phase,checkpoint=pair.to_record()))
            known=connection.execute('SELECT count(*) FROM saved_retained_cleanup WHERE attempt_id=?',(claim.attempt_id,)).fetchone()[0]
            if known!=len(records): raise ValueError('retained cleanup aliases changed')
            if authorize: connection.commit()
            return records
        except BaseException:
            if authorize: connection.rollback()
            raise

    def acknowledge_retained_cleanup(self, plan):
        from hermes_downloads.paths import _require_retained_absence
        records=self.retained_cleanup_records(plan)
        _require_retained_absence(plan.claim.attempt.prepared,records,marker_present=True)
        for record in records:
            self._connection.execute("UPDATE saved_retained_cleanup SET phase='absent' WHERE request_id=? AND role=? AND phase='authorized'",(record['request_id'],record['role']))
        return records

    def _qualify_cleanup_target(self, connection, request, job_id, epoch):
        command,_,members=self._read_target_command(connection,request,{})
        carrier=connection.execute('SELECT * FROM saved_cleanup_requests WHERE request_id=?',(request,)).fetchone()
        current=self._read_job_control_projection(connection,job_id)
        if (command.action!='resume' or command.expected_worker_epoch!=epoch or carrier is None
            or (carrier['job_id'],carrier['worker_epoch'])!=(job_id,epoch) or len(members)!=1
            or members[0]['outcome']!='existing_authority'
            or (members[0]['job_id'],members[0]['captured_generation'],members[0]['captured_revision'],members[0]['captured_state'])!=
                (job_id,current.generation,current.revision,'completed')):
            raise ValueError('cleanup target authority changed')
        return command
