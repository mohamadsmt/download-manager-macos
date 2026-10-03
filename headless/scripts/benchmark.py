#!/usr/bin/env python3
"""Allocate and summarize local downloader benchmark evidence.

Version 1 preserves T22a diagnostic allocation/summary behavior. Version 2 adds
a bounded loopback-only engine baseline, not worker or live-source acceptance.

A trial is a JSON object with the required fields listed in ``TRIAL_FIELDS``.
Failed records add the conditional ``failure_classification`` field and use
``null`` for every unobserved metric.  ``configuration`` is a closed JSON object
that records the engine, settings, version inventory, fixture-only source label,
and repetition number; raw URLs and raw external input are not accepted.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
import hashlib
import http.server
import inspect
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import threading
import time
import stat
import sys
from typing import Any, Final


SCHEMA_VERSION: Final = 1
REPORT_STATUS: Final = "pending_real_execution"
FIXTURE_PAYLOAD_BYTES: Final = 64 * 1024 * 1024

_IDENTIFIER_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_TIMESTAMP_RE: Final = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)
_IDENTIFIER_SEGMENT_RE: Final = re.compile(
    r"[A-Z]+s(?=$)|[A-Z]+(?=[A-Z][a-z]|[0-9]|$)|[A-Z]?[a-z]+|[0-9]+"
)
_VERSION_METADATA_VALUE_RE: Final = re.compile(
    r"^(?:[A-Za-z][A-Za-z0-9]{0,31}[-_])?[vV]?\d+(?:\.\d+){0,7}"
    r"(?:[-+._][A-Za-z0-9]{1,16}){0,4}$"
)
_CREDENTIAL_SHAPED_VALUE_RE: Final = re.compile(
    r"^(?:gh[pousr]_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+|"
    r"sk-[A-Za-z0-9_-]+|AKIA[A-Z0-9]{16}|xox[baprs]-[A-Za-z0-9-]+)$",
    re.IGNORECASE,
)
_MAX_METADATA_ENTRIES: Final = 32
_MAX_METADATA_STRING_LENGTH: Final = 64
_SAFE_SETTING_LABELS: Final = frozenset({"none", "parallel"})
_SENSITIVE_METADATA_KEY_TOKENS: Final = frozenset(
    {
        "url",
        "uri",
        "cookie",
        "token",
        "credential",
        "authorization",
        "password",
        "secret",
        "apikey",
        "auth",
        "header",
        "input",
    }
)

TRIAL_FIELDS: Final = frozenset(
    {
        "schema_version",
        "run_id",
        "trial_id",
        "configuration",
        "outcome",
        "fixture_sha256",
        "observed_at_utc",
        "payload_bytes",
        "elapsed_seconds",
        "cpu_seconds",
        "max_rss_bytes",
        "server_payload_bytes",
        "retransmitted_bytes",
        "client_payload_bytes",
        "allocated_disk_bytes",
        "pause_latency_seconds",
        "completion_sha256",
    }
)
CONFIGURATION_FIELDS: Final = frozenset(
    {"id", "repetition", "engine", "source", "versions", "settings"}
)
MEASURED_METRICS: Final = (
    "elapsed_seconds",
    "cpu_seconds",
    "max_rss_bytes",
    "server_payload_bytes",
    "retransmitted_bytes",
    "client_payload_bytes",
    "allocated_disk_bytes",
    "pause_latency_seconds",
)
_INTEGER_METRICS: Final = frozenset(
    {
        "max_rss_bytes",
        "server_payload_bytes",
        "retransmitted_bytes",
        "client_payload_bytes",
        "allocated_disk_bytes",
    }
)
FAILURE_CLASSIFICATIONS: Final = frozenset(
    {
        "accounting_error",
        "allocation_error",
        "engine_error",
        "environment_error",
        "integrity_error",
        "pause_error",
        "setup_error",
        "timeout",
    }
)


class BenchmarkValidationError(ValueError):
    """A trial record or local evidence input violates the closed contract."""


class ArtifactAllocationError(BenchmarkValidationError):
    """A fresh evidence directory cannot be allocated without clobbering state."""


class SummaryValidationError(BenchmarkValidationError):
    """A report cannot be formed without a complete compatible trial matrix."""


def _validate_identifier(value: object, label: str) -> str:
    if type(value) is not str or not _IDENTIFIER_RE.fullmatch(value):
        raise BenchmarkValidationError(f"{label} must be a portable identifier")
    return value


def validate_run_id(value: object) -> str:
    """Validate a portable artifact-run identifier without accepting path syntax."""

    return _validate_identifier(value, "run_id")


def validate_trial_id(value: object) -> str:
    """Validate a portable trial identifier without accepting path syntax."""

    return _validate_identifier(value, "trial_id")


def _require_nonnegative_integer(value: object, label: str, *, positive: bool = False) -> int:
    if type(value) is not int or value < 0 or (positive and value == 0):
        qualifier = "positive" if positive else "nonnegative"
        raise BenchmarkValidationError(f"{label} must be a finite {qualifier} integer")
    return value


def _require_nonnegative_number(value: object, label: str, *, positive: bool = False) -> int | float:
    if type(value) is int:
        number: int | float = value
    elif type(value) is float and math.isfinite(value):
        number = value
    else:
        raise BenchmarkValidationError(f"{label} must be a finite number")
    if number < 0 or (positive and number == 0):
        qualifier = "positive" if positive else "nonnegative"
        raise BenchmarkValidationError(f"{label} must be {qualifier}")
    return number


def _validate_sha256(value: object, label: str) -> str:
    if type(value) is not str or not _SHA256_RE.fullmatch(value):
        raise BenchmarkValidationError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _validate_observed_at_utc(value: object) -> str:
    if type(value) is not str or not _TIMESTAMP_RE.fullmatch(value):
        raise BenchmarkValidationError("observed_at_utc must be an ISO-8601 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise BenchmarkValidationError(
            "observed_at_utc must be an ISO-8601 UTC timestamp"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise BenchmarkValidationError("observed_at_utc must be UTC")
    return value


def _contains_external_source(value: str) -> bool:
    return value.startswith("//") or "://" in value or value.startswith(("file:", "data:"))


def _metadata_key_tokens(key_name: str) -> tuple[str, ...]:
    return tuple(
        segment.casefold()
        for part in re.split(r"[_-]+", key_name)
        for segment in _IDENTIFIER_SEGMENT_RE.findall(part)
    )


def _metadata_key_semantic_tokens(key_name: str) -> tuple[str, ...]:
    return tuple(
        token[:-1] if len(token) > 1 and token.endswith("s") else token
        for token in _metadata_key_tokens(key_name)
    )


def _validate_metadata_string(
    value: object, label: str, *, allow_setting_label: bool = False
) -> str:
    if type(value) is not str or not value or len(value) > _MAX_METADATA_STRING_LENGTH:
        raise BenchmarkValidationError(f"{label} must contain bounded metadata strings")
    if _contains_external_source(value) or _CREDENTIAL_SHAPED_VALUE_RE.fullmatch(value):
        raise BenchmarkValidationError(f"{label} must not contain raw external input")
    if _VERSION_METADATA_VALUE_RE.fullmatch(value):
        return value
    if allow_setting_label and value in _SAFE_SETTING_LABELS:
        return value
    raise BenchmarkValidationError(f"{label} must contain bounded metadata strings")


def _validate_metadata_key(value: object, key_label: str, container_label: str) -> str:
    key_name = _validate_identifier(value, key_label)
    tokens = _metadata_key_semantic_tokens(key_name)
    if (
        any(token in _SENSITIVE_METADATA_KEY_TOKENS for token in tokens)
        or ("api", "key") in zip(tokens, tokens[1:])
    ):
        raise BenchmarkValidationError(f"{container_label} must not contain raw external input")
    return key_name


def _validate_setting_value(value: object, label: str, depth: int = 0) -> None:
    if depth > 4:
        raise BenchmarkValidationError(f"{label} exceeds the supported JSON nesting")
    if value is None or type(value) in (bool, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise BenchmarkValidationError(f"{label} must not contain a nonfinite number")
        return
    if type(value) is str:
        _validate_metadata_string(value, label, allow_setting_label=True)
        return
    if type(value) is list:
        if len(value) > _MAX_METADATA_ENTRIES:
            raise BenchmarkValidationError(f"{label} has too many values")
        for index, item in enumerate(value):
            _validate_setting_value(item, f"{label}[{index}]", depth + 1)
        return
    if type(value) is dict:
        if len(value) > _MAX_METADATA_ENTRIES:
            raise BenchmarkValidationError(f"{label} has too many keys")
        for key, item in value.items():
            key_name = _validate_metadata_key(key, f"{label} key", label)
            _validate_setting_value(item, f"{label}.{key_name}", depth + 1)
        return
    raise BenchmarkValidationError(f"{label} must contain JSON values")


def _validate_configuration(value: object) -> dict[str, Any]:
    if type(value) is not dict or set(value) != CONFIGURATION_FIELDS:
        raise BenchmarkValidationError("configuration must have the documented closed fields")

    _validate_identifier(value["id"], "configuration.id")
    _require_nonnegative_integer(value["repetition"], "configuration.repetition", positive=True)
    if value["engine"] not in {"curl", "aria2"}:
        raise BenchmarkValidationError("configuration.engine must be curl or aria2")
    if value["source"] != "deterministic_local_fixture":
        raise BenchmarkValidationError(
            "configuration.source must identify the deterministic local fixture"
        )

    versions = value["versions"]
    if type(versions) is not dict or not versions:
        raise BenchmarkValidationError("configuration.versions must be a nonempty object")
    if len(versions) > _MAX_METADATA_ENTRIES:
        raise BenchmarkValidationError("configuration.versions has too many keys")
    for name, version in versions.items():
        _validate_metadata_key(
            name,
            "configuration.versions key",
            "configuration.versions",
        )
        _validate_metadata_string(version, "configuration.versions")

    settings = value["settings"]
    if type(settings) is not dict or not settings:
        raise BenchmarkValidationError("configuration.settings must be a nonempty object")
    if "connections" not in settings:
        raise BenchmarkValidationError("configuration.settings must record connections")
    _require_nonnegative_integer(settings["connections"], "settings.connections", positive=True)
    _validate_setting_value(settings, "configuration.settings")
    return value


def _validate_metric(value: object, name: str) -> int | float:
    if name in _INTEGER_METRICS:
        return _require_nonnegative_integer(value, name)
    return _require_nonnegative_number(value, name)


def _validate_accounting(record: Mapping[str, Any]) -> None:
    payload = record["payload_bytes"]
    client = record["client_payload_bytes"]
    retransmitted = record["retransmitted_bytes"]
    server = record["server_payload_bytes"]
    if client != payload:
        raise BenchmarkValidationError("incompatible accounting: client payload differs from fixture")
    if server != client + retransmitted:
        raise BenchmarkValidationError("incompatible accounting: server payload ledger is inconsistent")


def validate_trial_record(record: object) -> dict[str, Any]:
    """Validate one diagnostic trial record and return it without modifying it.

    Passed trials must carry every measurement and an end-to-end completion hash.
    Failed trials must identify one bounded failure class and leave at least one
    unobserved measurement as ``null``; this prevents all-zero fabricated metrics.
    """

    if type(record) is not dict:
        raise BenchmarkValidationError("trial record must be a JSON object")
    if type(record.get("schema_version")) is int and record["schema_version"] == 2:
        return _validate_baseline_trial(record)
    outcome = record.get("outcome")
    if outcome not in {"passed", "failed"}:
        raise BenchmarkValidationError("outcome must be passed or failed")

    expected_fields = set(TRIAL_FIELDS)
    if outcome == "failed":
        expected_fields.add("failure_classification")
    if set(record) != expected_fields:
        raise BenchmarkValidationError("trial record fields do not match the schema")

    if record["schema_version"] != SCHEMA_VERSION or type(record["schema_version"]) is not int:
        raise BenchmarkValidationError("unsupported schema_version")
    validate_run_id(record["run_id"])
    validate_trial_id(record["trial_id"])
    _validate_configuration(record["configuration"])
    fixture_sha256 = _validate_sha256(record["fixture_sha256"], "fixture_sha256")
    _validate_observed_at_utc(record["observed_at_utc"])
    _require_nonnegative_integer(record["payload_bytes"], "payload_bytes", positive=True)

    if outcome == "passed":
        for metric in MEASURED_METRICS:
            _validate_metric(record[metric], metric)
        _require_nonnegative_number(record["elapsed_seconds"], "elapsed_seconds", positive=True)
        _validate_accounting(record)
        completion = _validate_sha256(record["completion_sha256"], "completion_sha256")
        if completion != fixture_sha256:
            raise BenchmarkValidationError("completion_sha256 must equal fixture_sha256")
        return record

    if record["failure_classification"] not in FAILURE_CLASSIFICATIONS:
        raise BenchmarkValidationError("failure_classification is not in the bounded taxonomy")
    if record["completion_sha256"] is not None:
        raise BenchmarkValidationError("failed trial completion_sha256 must be null")

    observed_metrics = 0
    for metric in MEASURED_METRICS:
        if record[metric] is None:
            continue
        _validate_metric(record[metric], metric)
        observed_metrics += 1
    if observed_metrics == len(MEASURED_METRICS):
        raise BenchmarkValidationError("failed trials must use null for unobserved metrics")
    return record


def canonical_json(record: object) -> str:
    """Return a canonical, newline-terminated serialization of a valid trial."""

    validated = validate_trial_record(record)
    return json.dumps(
        validated,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ) + "\n"


def _ensure_artifact_root(artifact_root: Path) -> None:
    if not artifact_root.is_absolute():
        raise ArtifactAllocationError("artifact_root must be an absolute supplied path")

    current = Path(artifact_root.anchor)
    for component in artifact_root.parts[1:]:
        current = current / component
        created = False
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            try:
                os.mkdir(current, 0o700)
            except FileExistsError:
                pass
            except OSError as error:
                raise ArtifactAllocationError("artifact root could not be created") from error
            else:
                created = True
            try:
                metadata = os.lstat(current)
            except OSError as error:
                raise ArtifactAllocationError("artifact root could not be inspected") from error
        except OSError as error:
            raise ArtifactAllocationError("artifact root could not be inspected") from error

        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise ArtifactAllocationError("artifact root must be a real directory chain")
        if created and os.name == "posix" and stat.S_IMODE(metadata.st_mode) & 0o077:
            raise ArtifactAllocationError("new artifact directories must be owner-only")


def allocate_run_directory(artifact_root: str | Path, run_id: object) -> Path:
    """Atomically allocate ``artifact_root / run_id`` without clobbering any entry.

    The caller must explicitly supply the intended ``.artifacts/download-manager``
    root.  Existing directories, files, and symlinks all fail closed; no merging or
    overwrite is attempted.
    """

    validated_run_id = validate_run_id(run_id)
    try:
        root = Path(artifact_root)
    except (TypeError, ValueError) as error:
        raise ArtifactAllocationError("artifact_root must be a valid path") from error
    _ensure_artifact_root(root)

    target = root / validated_run_id
    try:
        os.mkdir(target, 0o700)
    except FileExistsError as error:
        raise ArtifactAllocationError("run directory already exists") from error
    except OSError as error:
        raise ArtifactAllocationError("run directory could not be allocated") from error

    try:
        metadata = os.lstat(target)
    except OSError as error:
        raise ArtifactAllocationError("allocated run directory could not be inspected") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ArtifactAllocationError("allocated run directory is unsafe")
    if os.name == "posix" and stat.S_IMODE(metadata.st_mode) & 0o077:
        raise ArtifactAllocationError("allocated run directory is not owner-only")
    return target


def _configuration_signature(configuration: Mapping[str, Any]) -> str:
    base_configuration = {
        key: value for key, value in configuration.items() if key != "repetition"
    }
    return json.dumps(
        base_configuration,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _copy_configuration_without_repetition(configuration: Mapping[str, Any]) -> dict[str, Any]:
    serialized = _configuration_signature(configuration)
    copied = json.loads(serialized)
    assert type(copied) is dict
    return copied


def _validate_expected_configurations(value: object) -> frozenset[str]:
    if not isinstance(value, (set, frozenset)) or not value:
        raise SummaryValidationError("expected configurations must be a nonempty explicit set")
    validated = {_validate_identifier(item, "expected configuration") for item in value}
    if len(validated) != len(value):
        raise SummaryValidationError("expected configurations must be unique")
    return frozenset(validated)


def _p50(values: list[int | float]) -> int | float:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2


def _metric_summary(records: list[dict[str, Any]], metric: str) -> dict[str, Any]:
    values = [record[metric] for record in records]
    return {
        "p50": _p50(values),
        "range": {"min": min(values), "max": max(values)},
    }


def summarize_trials(
    trials: Iterable[object],
    expected_configurations: set[str] | frozenset[str],
    *,
    repetitions: int,
) -> dict[str, Any]:
    """Fail closed unless an exact compatible matrix of passed trials is present.

    The returned report intentionally remains ``pending_real_execution``.  It is a
    computed diagnostic summary, never acceptance evidence by itself.
    """

    expected = _validate_expected_configurations(expected_configurations)
    if type(repetitions) is not int or repetitions <= 0:
        raise SummaryValidationError("repetitions must be a positive exact integer")

    validated_trials: list[dict[str, Any]] = []
    for trial in trials:
        try:
            validated = validate_trial_record(trial)
        except BenchmarkValidationError as error:
            raise SummaryValidationError(f"invalid trial record: {error}") from error
        if validated["outcome"] != "passed":
            raise SummaryValidationError("failed trial records cannot be summarized")
        validated_trials.append(validated)

    trial_ids: set[str] = set()
    for trial in validated_trials:
        trial_id = trial["trial_id"]
        if trial_id in trial_ids:
            raise SummaryValidationError("duplicate trial_id in evidence")
        trial_ids.add(trial_id)

    run_ids = {trial["run_id"] for trial in validated_trials}
    if len(run_ids) != 1:
        raise SummaryValidationError("inconsistent run IDs in evidence")
    fixture_hashes = {trial["fixture_sha256"] for trial in validated_trials}
    if len(fixture_hashes) != 1:
        raise SummaryValidationError("inconsistent fixture hashes in evidence")
    payload_sizes = {trial["payload_bytes"] for trial in validated_trials}
    if len(payload_sizes) != 1:
        raise SummaryValidationError("incompatible payload accounting in evidence")
    payload_bytes = next(iter(payload_sizes))
    if payload_bytes != FIXTURE_PAYLOAD_BYTES:
        raise SummaryValidationError("payload_bytes must be the 64 MiB controlled fixture")

    grouped: dict[str, list[dict[str, Any]]] = {}
    for trial in validated_trials:
        configuration_id = trial["configuration"]["id"]
        grouped.setdefault(configuration_id, []).append(trial)
    actual = frozenset(grouped)
    missing = expected - actual
    unexpected = actual - expected
    if missing:
        raise SummaryValidationError("missing expected configuration in evidence")
    if unexpected:
        raise SummaryValidationError("unexpected configuration in evidence")

    if len(validated_trials) != len(expected) * repetitions:
        raise SummaryValidationError("evidence does not contain the exact trial count")

    configuration_summaries: dict[str, Any] = {}
    expected_repetitions = set(range(1, repetitions + 1))
    for configuration_id in sorted(expected):
        records = grouped[configuration_id]
        repetition_records: dict[int, dict[str, Any]] = {}
        signature: str | None = None
        for record in records:
            configuration = record["configuration"]
            repetition = configuration["repetition"]
            if repetition in repetition_records:
                raise SummaryValidationError("duplicate repetition in configuration evidence")
            repetition_records[repetition] = record
            current_signature = _configuration_signature(configuration)
            if signature is None:
                signature = current_signature
            elif signature != current_signature:
                raise SummaryValidationError("inconsistent configuration settings in evidence")
        if set(repetition_records) != expected_repetitions:
            raise SummaryValidationError("missing or unexpected repetition in configuration evidence")

        ordered_records = [repetition_records[index] for index in sorted(repetition_records)]
        configuration_summaries[configuration_id] = {
            "configuration": _copy_configuration_without_repetition(
                ordered_records[0]["configuration"]
            ),
            "metrics": {
                metric: _metric_summary(ordered_records, metric)
                for metric in MEASURED_METRICS
            },
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "status": REPORT_STATUS,
        "run_id": next(iter(run_ids)),
        "fixture": {
            "sha256": next(iter(fixture_hashes)),
            "payload_bytes": payload_bytes,
        },
        "repetitions": repetitions,
        "configurations": configuration_summaries,
    }


# Version 2 is a local engine baseline, with a separately measured pause probe.
BASELINE_SCHEMA_VERSION: Final = 2
BASELINE_PURPOSE: Final = "local_engine_baseline"
PAUSE_SCOPE: Final = "separate_graceful_engine_containment"
ORIGIN_MODELS: Final = ("unrestricted", "per-connection")
BASELINE_ENGINES: Final = ("curl-single", "aria2-single", "aria2-multi")
ORIGIN_RATE: Final = 4 * 1024 * 1024
PROBE_RATE: Final = 1024 * 1024
PROBE_BYTE_LIMIT: Final = 4 * 1024 * 1024
BASELINE_FIELDS: Final = frozenset(
    {"purpose", "runner_sha256", "source_sha256", "pause_probe"}
)
PROBE_FIELDS: Final = frozenset({
    "scope", "elapsed_seconds", "cpu_seconds", "max_rss_bytes",
    "server_payload_bytes", "client_logical_bytes", "allocated_disk_bytes",
    "server_unique_payload_bytes", "retransmitted_bytes", "latency_seconds", "forced", "contained",
})


class BudgetExceeded(RuntimeError):
    """A finite aggregate transfer or monotonic wall budget was exhausted."""


class RunBudget:
    def __init__(self, wall_seconds: float, byte_limit: int, *, clock=time.monotonic):
        _require_nonnegative_number(wall_seconds, "wall_seconds", positive=True)
        _require_nonnegative_integer(byte_limit, "byte_limit", positive=True)
        self.clock = clock
        self.deadline = clock() + wall_seconds
        self.byte_limit = byte_limit
        self.transferred = 0
        self.lock = threading.Lock()

    def check(self) -> None:
        if self.clock() >= self.deadline:
            raise BudgetExceeded("wall budget exhausted")

    def remaining(self) -> float:
        self.check()
        return self.deadline - self.clock()

    def consume(self, count: int) -> None:
        _require_nonnegative_integer(count, "transfer bytes")
        with self.lock:
            self.check()
            if self.transferred + count > self.byte_limit:
                raise BudgetExceeded("payload budget exhausted")
            self.transferred += count

    def send(self, connection, data: bytes) -> int:
        # One short socket send under the lock prevents concurrent overspending;
        # only bytes actually accepted by the socket enter the ledger.
        with self.lock:
            self.check()
            if self.transferred + len(data) > self.byte_limit:
                raise BudgetExceeded("payload budget exhausted")
            sent = connection.send(data)
            self.transferred += sent
            return sent


def baseline_sequence() -> list[tuple[str, str, int]]:
    return [(model, engine, repetition)
            for repetition in (1, 2, 3)
            for model in ORIGIN_MODELS
            for engine in (BASELINE_ENGINES[repetition-1:] + BASELINE_ENGINES[:repetition-1])]


def baseline_settings(model: str, engine: str) -> dict[str, Any]:
    if model not in ORIGIN_MODELS or engine not in BASELINE_ENGINES:
        raise BenchmarkValidationError("unsupported baseline configuration")
    return {
        "connections": 16 if engine == "aria2-multi" else 1,
        "file_allocation": "none", "verify_tls": True,
        "origin_bytes_per_connection_second": ORIGIN_RATE if model == "per-connection" else 0,
        "probe_bytes_per_connection_second": PROBE_RATE,
        "probe_byte_limit": PROBE_BYTE_LIMIT, "retry_count": 0,
        "minimum_split_bytes": 1024 * 1024,
    }


def engine_environment(directory: Path) -> dict[str, str]:
    # Allowlist, rather than removing only known proxy/credential variables.
    return {"PATH": "/usr/bin:/bin:/opt/homebrew/bin", "HOME": str(directory),
            "LC_ALL": "C", "TMPDIR": str(directory)}


def engine_argv(engine: str, executable: str, port: int, directory: Path,
                timeout: float) -> list[str]:
    if engine not in BASELINE_ENGINES or type(port) is not int or not 1 <= port <= 65535:
        raise BenchmarkValidationError("unsupported engine or local port")
    _require_nonnegative_number(timeout, "timeout", positive=True)
    destination = "http://127.0.0.1:" + str(port) + "/payload"
    seconds = str(max(1, math.ceil(timeout)))
    if engine == "curl-single":
        return [executable, "-q", "--no-netrc", "--proxy", "", "--noproxy", "*",
                "--proto", "=http", "--max-redirs", "0", "--retry", "0",
                "--connect-timeout", "2", "--max-time", seconds,
                "--fail", "--silent", "--show-error", "--no-clobber",
                "--output", str(directory / "payload.bin"), destination]
    connections = "16" if engine == "aria2-multi" else "1"
    return [executable, "--no-conf=true", "--no-netrc=true", "--all-proxy=",
            "--http-proxy=", "--https-proxy=", "--ftp-proxy=", "--check-certificate=true",
            "--file-allocation=none", "--allow-overwrite=false", "--auto-file-renaming=false",
            "--continue=false", "--max-tries=1", "--retry-wait=0", "--connect-timeout=2",
            "--timeout=" + seconds, "--split=" + connections,
            "--max-connection-per-server=" + connections, "--min-split-size=1M",
            "--max-concurrent-downloads=1", "--enable-color=false", "--console-log-level=error",
            "--summary-interval=0", "--download-result=hide", "--dir=" + str(directory),
            "--out=payload.bin", destination]


def _private_write(path: Path, content: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(content)


def _json_write(path: Path, record: object) -> None:
    _private_write(path, json.dumps(record, allow_nan=False, sort_keys=True,
                                   separators=(",", ":")) + "\n")


def generate_fixture(path: Path, budget: RunBudget) -> str:
    budget.check()
    digest = hashlib.sha256()
    block = bytes(range(256)) * 256
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        for _ in range(FIXTURE_PAYLOAD_BYTES // len(block)):
            budget.check()
            stream.write(block)
            digest.update(block)
    budget.check()
    return digest.hexdigest()


def normalize_rss(value: int, platform: str = sys.platform) -> int:
    return int(value) if platform == "darwin" else int(value) * 1024


def _group_exists(pid: int) -> bool:
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return False
    return True


def run_child(argv: list[str], directory: Path, budget: RunBudget, *, timeout: float,
              log_name: str, pause_ready=None, signal_observer=None) -> dict[str, Any]:
    """Own one new session; wait4 measures its actual engine leader, never the runner.

    Up to ten seconds (half the remaining budget for short calls) are reserved
    inside the aggregate wall budget for termination.
    These engines spawn no helper processes for this local HTTP fixture. Any
    surviving process-group member is killed and containment checked before return.
    """
    remaining = budget.remaining()
    if remaining <= 0.1:
        raise BudgetExceeded("insufficient containment reserve")
    containment_reserve = min(10, remaining / 2)
    _validate_identifier(log_name, "log_name")
    started = time.monotonic()
    deadline = min(started + timeout, budget.deadline - containment_reserve)
    descriptor = os.open(directory / (log_name + ".log"),
                         os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    process = None
    status = usage = None
    classification = None
    pause_started = None
    forced = False
    try:
        with os.fdopen(descriptor, "wb") as log:
            process = subprocess.Popen(argv, cwd=directory, env=engine_environment(directory),
                                       stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                       start_new_session=True, shell=False, umask=0o077)
            while True:
                waited, observed_status, observed_usage = os.wait4(process.pid, os.WNOHANG)
                if waited:
                    status, usage = observed_status, observed_usage
                    break
                now = time.monotonic()
                if pause_ready is not None and pause_ready():
                    pause_started = now
                    break
                if now >= deadline:
                    classification = "timeout"
                    break
                try:
                    budget.check()
                except BudgetExceeded:
                    classification = "timeout"
                    break
                time.sleep(0.01)
    finally:
        if process is not None:
            if status is None or _group_exists(process.pid):
                signaled = time.monotonic()
                os.killpg(process.pid, signal.SIGTERM) if _group_exists(process.pid) else None
                if signal_observer is not None:
                    signal_observer(signal.SIGTERM)
                grace_deadline = min(signaled + 5, budget.deadline - 1)
                while time.monotonic() < grace_deadline:
                    if status is None:
                        waited, observed_status, observed_usage = os.wait4(process.pid, os.WNOHANG)
                        if waited:
                            status, usage = observed_status, observed_usage
                    if status is not None and not _group_exists(process.pid):
                        break
                    time.sleep(0.01)
                if _group_exists(process.pid):
                    forced = True
                    os.killpg(process.pid, signal.SIGKILL)
                    if signal_observer is not None:
                        signal_observer(signal.SIGKILL)
                if status is None:
                    _, status, usage = os.wait4(process.pid, 0)
                while _group_exists(process.pid) and time.monotonic() < budget.deadline:
                    time.sleep(0.01)
                if _group_exists(process.pid):
                    raise RuntimeError("engine group containment could not be verified")
            process.returncode = os.waitstatus_to_exitcode(status)
    assert process is not None and usage is not None
    elapsed = time.monotonic() - started
    result = {"pid": process.pid, "exit_code": process.returncode,
              "elapsed_seconds": elapsed,
              "cpu_seconds": usage.ru_utime + usage.ru_stime,
              "max_rss_bytes": normalize_rss(usage.ru_maxrss),
              "contained": not _group_exists(process.pid), "forced": forced,
              "pause_latency_seconds": time.monotonic() - pause_started if pause_started else None}
    if classification:
        result["failure_classification"] = classification
    elif pause_ready is not None and (pause_started is None or forced):
        result["failure_classification"] = "pause_error"
    elif pause_ready is None and process.returncode != 0:
        result["failure_classification"] = "engine_error"
    return result


class LocalOrigin:
    """Loopback-only range origin; payload ledger counts successful socket sends.

    Retransmission means repeated HTTP payload ranges, not TCP retransmission.
    HEAD/header bytes do not enter the payload ledger. No URL/input is accepted.
    """
    def __init__(self, fixture: Path, budget: RunBudget, rate: int, byte_limit: int):
        self.fixture, self.budget, self.rate, self.byte_limit = fixture, budget, rate, byte_limit
        self.transferred = 0
        self.requests: list[dict[str, int]] = []
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.error: str | None = None
        origin = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self):
                self.request.settimeout(0.25)  # bound idle/header reads too
                super().setup()

            def log_message(self, *args):
                pass

            def do_HEAD(self):
                self.handle_payload(head=True)

            def do_GET(self):
                self.handle_payload(head=False)

            def handle_payload(self, head: bool):
                self.connection.settimeout(0.25)
                self.close_connection = True
                if self.path != "/payload":
                    self.send_error(404)
                    return
                start, end = 0, FIXTURE_PAYLOAD_BYTES - 1
                range_value = self.headers.get("Range")
                if range_value:
                    match = re.fullmatch(r"bytes=(\d+)-(\d*)", range_value)
                    if not match:
                        self.send_error(416)
                        return
                    start = int(match[1])
                    end = int(match[2]) if match[2] else end
                    if not 0 <= start <= end < FIXTURE_PAYLOAD_BYTES:
                        self.send_error(416)
                        return
                self.send_response(206 if range_value else 200)
                self.send_header("Content-Length", str(end - start + 1))
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Connection", "close")
                if range_value:
                    self.send_header("Content-Range", f"bytes {start}-{end}/{FIXTURE_PAYLOAD_BYTES}")
                self.end_headers()
                if head:
                    return
                row = {"start": start, "end": end, "sent": 0}
                with origin.lock:
                    origin.requests.append(row)
                began = time.monotonic()
                try:
                    with origin.fixture.open("rb") as payload:
                        payload.seek(start)
                        while row["sent"] < end - start + 1 and not origin.stop.is_set():
                            origin.budget.check()
                            if origin.rate:
                                target = began + row["sent"] / origin.rate
                                if origin.stop.wait(max(0, target - time.monotonic())):
                                    break
                            data = payload.read(min(65536, end - start + 1 - row["sent"]))
                            offset = 0
                            while offset < len(data) and not origin.stop.is_set():
                                with origin.lock:
                                    if origin.transferred + len(data) - offset > origin.byte_limit:
                                        raise BudgetExceeded("origin payload budget exhausted")
                                    sent = origin.budget.send(self.connection, data[offset:])
                                    row["sent"] += sent
                                    origin.transferred += sent
                                if sent == 0:
                                    raise ConnectionError("closed socket")
                                offset += sent
                except BudgetExceeded:
                    origin.error = "timeout"
                    origin.stop.set()
                except (OSError, ConnectionError):
                    pass  # Partial sends remain in the ledger, including pause.

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = False
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.05})

    @property
    def port(self) -> int:
        return self.server.server_address[1]

    def __enter__(self):
        try:
            self.budget.check()
        except BudgetExceeded:
            self.server.server_close()
            raise
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.server.shutdown()
        self.server.server_close()  # joins request threads, bounded socket timeout
        self.thread.join(timeout=1)
        if self.thread.is_alive():
            raise RuntimeError("origin containment failed")

    def ledger(self) -> dict[str, Any]:
        intervals = sorted((r["start"], r["start"] + r["sent"])
                           for r in self.requests if r["sent"])
        unique, cursor = 0, 0
        for start, end in intervals:
            unique += max(0, end - max(cursor, start))
            cursor = max(cursor, end)
        return {"server_payload_bytes": self.transferred, "unique_payload_bytes": unique,
                "retransmitted_bytes": self.transferred - unique,
                "requests": self.requests}


def inventory_engines(directory: Path, budget: RunBudget) -> dict[str, tuple[str, str]]:
    inventory = {}
    for engine, name, prefix in (("curl", "curl", ["-q"]),
                                  ("aria2", "aria2c", ["--no-conf=true"])):
        executable = shutil.which(name, path=engine_environment(directory)["PATH"])
        if executable is None:
            raise BenchmarkValidationError("required local engine is missing")
        result = run_child([executable, *prefix, "--version"], directory, budget,
                           timeout=5, log_name=engine + "-version")
        if "failure_classification" in result:
            raise BenchmarkValidationError("engine inventory failed")
        text = (directory / (engine + "-version.log")).read_text()[:65536]
        match = re.search(r"(?:curl |aria2 version )(\d+(?:\.\d+)+)", text)
        if not match:
            raise BenchmarkValidationError("engine version was not recognized")
        inventory[engine] = (executable, match[1])
        _json_write(directory / (engine + "-inventory.json"),
                    {"version": match[1], "binary_sha256": hashlib.sha256(Path(executable).read_bytes()).hexdigest(),
                     "process_accounting": result})
    return inventory


def _measure_file(path: Path, budget: RunBudget, *, verify: bool) -> dict[str, Any]:
    budget.check()
    if not path.exists():
        return {"client_payload_bytes": None, "allocated_disk_bytes": None, "completion_sha256": None}
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode):
        raise BenchmarkValidationError("engine output is not a regular file")
    digest = hashlib.sha256()
    if verify:
        with path.open("rb") as stream:
            while block := stream.read(1024 * 1024):
                budget.check()
                digest.update(block)
    budget.check()
    return {"client_payload_bytes": metadata.st_size,
            "allocated_disk_bytes": metadata.st_blocks * 512,
            "completion_sha256": digest.hexdigest() if verify else None}


def execute_baseline_trial(record: dict[str, Any], directory: Path, fixture: Path,
                           budget: RunBudget, inventory: dict[str, tuple[str, str]],
                           trial_seconds: float) -> dict[str, Any]:
    settings = record["configuration"]["settings"]
    engine = record["configuration"]["id"].split("-", 1)[1]
    if record["configuration"]["id"].startswith("per-connection-"):
        engine = record["configuration"]["id"][len("per-connection-"):]
    executable = inventory[record["configuration"]["engine"]][0]
    completion = directory / "completion"
    completion.mkdir(mode=0o700)
    with LocalOrigin(fixture, budget, settings["origin_bytes_per_connection_second"],
                     2 * FIXTURE_PAYLOAD_BYTES) as origin:
        result = run_child(engine_argv(engine, executable, origin.port, completion, trial_seconds),
                           completion, budget, timeout=trial_seconds, log_name="engine")
    ledger = origin.ledger()
    _json_write(completion / "ledger.json", ledger)
    _json_write(completion / "process.json", result)
    for metric in ("elapsed_seconds", "cpu_seconds", "max_rss_bytes"):
        record[metric] = result[metric]
    record["server_payload_bytes"] = ledger["server_payload_bytes"]
    record["retransmitted_bytes"] = ledger["retransmitted_bytes"]
    measured = _measure_file(completion / "payload.bin", budget, verify=True)
    record.update(measured)
    failure = result.get("failure_classification") or origin.error
    if not failure and (measured["client_payload_bytes"] != FIXTURE_PAYLOAD_BYTES or
                        measured["completion_sha256"] != record["fixture_sha256"]):
        failure = "integrity_error"
    if not failure and ledger["unique_payload_bytes"] != FIXTURE_PAYLOAD_BYTES:
        failure = "accounting_error"
    if failure:
        record.update(failure_classification=failure, completion_sha256=None)
        return record

    probe = directory / "pause"
    probe.mkdir(mode=0o700)
    with LocalOrigin(fixture, budget, PROBE_RATE, PROBE_BYTE_LIMIT) as origin:
        result = run_child(engine_argv(engine, executable, origin.port, probe, 10), probe,
                           budget, timeout=10, log_name="engine",
                           pause_ready=lambda: origin.transferred >= 65536)
    ledger = origin.ledger()
    _json_write(probe / "ledger.json", ledger)
    _json_write(probe / "process.json", result)
    measured = _measure_file(probe / "payload.bin", budget, verify=False)
    pause = {"scope": PAUSE_SCOPE,
             **{metric: result[metric] for metric in ("elapsed_seconds", "cpu_seconds", "max_rss_bytes")},
             "server_payload_bytes": ledger["server_payload_bytes"],
             "client_logical_bytes": measured["client_payload_bytes"],
             "server_unique_payload_bytes": ledger["unique_payload_bytes"],
             "retransmitted_bytes": ledger["retransmitted_bytes"],
             "allocated_disk_bytes": measured["allocated_disk_bytes"],
             "latency_seconds": result["pause_latency_seconds"],
             "forced": result["forced"], "contained": result["contained"]}
    record["pause_probe"] = pause
    failure = result.get("failure_classification") or origin.error
    if not failure and (pause["latency_seconds"] is None or pause["latency_seconds"] > 5):
        failure = "pause_error"
    if not failure and (not pause["server_payload_bytes"] or pause["client_logical_bytes"] is None):
        failure = "pause_error"
    if failure:
        record.update(failure_classification=failure, completion_sha256=None)
        return record
    record.update(outcome="passed", pause_latency_seconds=pause["latency_seconds"])
    record.pop("failure_classification", None)
    return record


def _validate_baseline_trial(record: dict[str, Any]) -> dict[str, Any]:
    expected = set(TRIAL_FIELDS) | set(BASELINE_FIELDS)
    if record.get("outcome") == "failed":
        expected.add("failure_classification")
    if set(record) != expected or record.get("purpose") != BASELINE_PURPOSE:
        raise BenchmarkValidationError("baseline fields or purpose mismatch")
    for key in ("runner_sha256", "source_sha256"):
        _validate_sha256(record[key], key)
    legacy = {key: value for key, value in record.items() if key not in BASELINE_FIELDS}
    legacy["schema_version"] = 1
    validate_trial_record(legacy)
    config = record["configuration"]
    matched = [(model, engine) for model in ORIGIN_MODELS for engine in BASELINE_ENGINES
               if config["id"] == f"{model}-{engine}"]
    if len(matched) != 1:
        raise BenchmarkValidationError("unknown baseline configuration")
    model, engine = matched[0]
    if record["trial_id"] != f"{config['id']}-{config['repetition']}":
        raise BenchmarkValidationError("baseline trial identity mismatch")
    if config["engine"] != ("curl" if engine == "curl-single" else "aria2"):
        raise BenchmarkValidationError("baseline engine mismatch")
    if config["settings"] != baseline_settings(model, engine):
        raise BenchmarkValidationError("baseline settings mismatch")
    if record["payload_bytes"] != FIXTURE_PAYLOAD_BYTES:
        raise BenchmarkValidationError("baseline fixture size mismatch")
    probe = record["pause_probe"]
    if probe is None and record["outcome"] == "failed":
        return record
    if type(probe) is not dict or set(probe) != PROBE_FIELDS or probe["scope"] != PAUSE_SCOPE:
        raise BenchmarkValidationError("pause probe fields or scope mismatch")
    for metric in PROBE_FIELDS - {"scope", "forced", "contained"}:
        if probe[metric] is None and record["outcome"] == "failed":
            continue
        if metric.endswith("bytes"):
            _require_nonnegative_integer(probe[metric], "probe." + metric)
        else:
            _require_nonnegative_number(probe[metric], "probe." + metric)
    if type(probe["forced"]) is not bool or type(probe["contained"]) is not bool:
        raise BenchmarkValidationError("pause containment must be explicit booleans")
    if record["outcome"] == "passed":
        if (not probe["contained"] or probe["forced"] or not 0 < probe["latency_seconds"] <= 5 or
            probe["latency_seconds"] != record["pause_latency_seconds"] or
            not 0 < probe["server_payload_bytes"] <= PROBE_BYTE_LIMIT or
            probe["server_payload_bytes"] != probe["server_unique_payload_bytes"] + probe["retransmitted_bytes"] or
            probe["server_unique_payload_bytes"] > FIXTURE_PAYLOAD_BYTES or
            probe["elapsed_seconds"] < probe["latency_seconds"] or
            not probe["max_rss_bytes"]):
            raise BenchmarkValidationError("incompatible pause probe measurement")
        if record["cpu_seconds"] <= 0 or record["max_rss_bytes"] <= 0:
            raise BenchmarkValidationError("baseline process accounting must be observed")
    return record


def summarize_baseline_trials(trials: Iterable[object]) -> dict[str, Any]:
    records = list(trials)
    try:
        for record in records:
            if type(record) is not dict or record.get("schema_version") != 2:
                raise BenchmarkValidationError("baseline requires schema version 2")
            validate_trial_record(record)
    except BenchmarkValidationError as error:
        raise SummaryValidationError(str(error)) from error
    for key in ("runner_sha256", "source_sha256", "purpose"):
        if len({record[key] for record in records}) != 1:
            raise SummaryValidationError("inconsistent baseline " + key)
    if len({json.dumps(record["configuration"]["versions"], sort_keys=True) for record in records}) != 1:
        raise SummaryValidationError("inconsistent baseline versions")
    legacy = [{**{key: value for key, value in record.items() if key not in BASELINE_FIELDS},
               "schema_version": 1} for record in records]
    summary = summarize_trials(legacy, {f"{model}-{engine}" for model in ORIGIN_MODELS
                                       for engine in BASELINE_ENGINES}, repetitions=3)
    summary.update(schema_version=2, status="measured_local_baseline", trial_count=18,
                   purpose=BASELINE_PURPOSE, runner_sha256=records[0]["runner_sha256"],
                   source_sha256=records[0]["source_sha256"], pause_scope=PAUSE_SCOPE,
                   probe_totals={metric: sum(r["pause_probe"][metric] for r in records)
                                 for metric in ("server_payload_bytes", "server_unique_payload_bytes", "retransmitted_bytes", "client_logical_bytes", "cpu_seconds")})
    return summary


def run_baselines(artifact_root: Path, run_id: str, *, mode: str = "full",
                  wall_seconds: float = 1200, byte_budget: int = 2 * 1024**3,
                  trial_seconds: float = 60) -> dict[str, Any]:
    if mode not in {"full", "smoke"}:
        raise BenchmarkValidationError("unsupported execution mode")
    _require_nonnegative_number(wall_seconds, "wall_seconds", positive=True)
    _require_nonnegative_number(trial_seconds, "trial_seconds", positive=True)
    _require_nonnegative_integer(byte_budget, "byte_budget", positive=True)
    if wall_seconds > (120 if mode == "smoke" else 1800) or trial_seconds > 60:
        raise BenchmarkValidationError("wall or trial deadline exceeds bounded maximum")
    if byte_budget > (2 * FIXTURE_PAYLOAD_BYTES if mode == "smoke" else 3 * 1024**3):
        raise BenchmarkValidationError("payload budget exceeds bounded maximum")
    budget = RunBudget(wall_seconds, byte_budget)
    directory = allocate_run_directory(artifact_root, run_id)
    (directory / "trials").mkdir(mode=0o700)
    sequence = baseline_sequence() if mode == "full" else [("unrestricted", "curl-single", 1)]
    runner_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    source_hash = hashlib.sha256(inspect.getsource(generate_fixture).encode()).hexdigest()
    fixture_hash = None
    inventory = {}
    setup_failure = None
    started = time.monotonic()
    setup_budget = RunBudget(min(30, wall_seconds), byte_budget)
    try:
        fixture_hash = generate_fixture(directory / "fixture.bin", setup_budget)
        inventory = inventory_engines(directory, setup_budget)
        setup_budget.check()
        budget.check()
    except (OSError, BenchmarkValidationError, BudgetExceeded):
        setup_failure = "setup_error"
    if fixture_hash is None:
        # Hash the deterministic source bytes, not a success claim about setup.
        digest = hashlib.sha256()
        for _ in range(1024):
            digest.update(bytes(range(256)) * 256)
        fixture_hash = digest.hexdigest()
    _json_write(directory / "manifest.json", {
        "schema_version": 2, "purpose": BASELINE_PURPOSE, "mode": mode,
        "runner_sha256": runner_hash, "source_sha256": source_hash,
        "fixture_sha256": fixture_hash, "payload_bytes": FIXTURE_PAYLOAD_BYTES,
        "wall_seconds": wall_seconds, "trial_seconds": trial_seconds,
        "byte_budget": byte_budget, "setup_elapsed_seconds": time.monotonic() - started,
        "setup_failure": setup_failure, "sequence": sequence,
        "pause_scope": PAUSE_SCOPE, "process_accounting_scope": "wait4_engine_leader",
        "ledger_scope": "socket_accepted_http_payload_ranges",
    })
    records = []
    for model, engine, repetition in sequence:
        trial_id = f"{model}-{engine}-{repetition}"
        record = {metric: None for metric in MEASURED_METRICS}
        record.update(schema_version=2, purpose=BASELINE_PURPOSE, run_id=run_id,
                      trial_id=trial_id, runner_sha256=runner_hash, source_sha256=source_hash,
                      outcome="failed", failure_classification=setup_failure or "timeout",
                      fixture_sha256=fixture_hash, payload_bytes=FIXTURE_PAYLOAD_BYTES,
                      observed_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                      completion_sha256=None, pause_probe=None,
                      configuration={"id": f"{model}-{engine}", "repetition": repetition,
                                     "engine": "curl" if engine == "curl-single" else "aria2",
                                     "source": "deterministic_local_fixture",
                                     "versions": {"python": sys.version.split()[0],
                                                  **{key: item[1] for key, item in inventory.items()}},
                                     "settings": baseline_settings(model, engine)})
        if not setup_failure:
            try:
                budget.check()
                trial_directory = directory / trial_id
                trial_directory.mkdir(mode=0o700)
                record = execute_baseline_trial(record, trial_directory, directory / "fixture.bin",
                                                budget, inventory, trial_seconds)
            except BudgetExceeded:
                record.update(outcome="failed", failure_classification="timeout",
                              completion_sha256=None, pause_latency_seconds=None)
            except (OSError, BenchmarkValidationError):
                record.update(outcome="failed", failure_classification="environment_error",
                              completion_sha256=None, pause_latency_seconds=None)
        _private_write(directory / "trials" / (trial_id + ".json"), canonical_json(record))
        records.append(record)
    summary = None
    try:
        budget.check()
        if mode == "full":
            summary = summarize_baseline_trials(records)
        elif len(records) == 1 and records[0]["outcome"] == "passed":
            validate_trial_record(records[0])
            summary = {"schema_version": 2, "status": "measured_local_smoke", "trial_count": 1,
                       "run_id": run_id, "purpose": BASELINE_PURPOSE, "pause_scope": PAUSE_SCOPE}
    except (SummaryValidationError, BudgetExceeded):
        pass
    report = {"schema_version": 2, "status": summary["status"] if summary else "failed_local_baseline",
              "mode": mode, "run_id": run_id, "trial_count": len(records),
              "failed_trials": [r["trial_id"] for r in records if r["outcome"] == "failed"],
              "aggregate_server_payload_bytes": budget.transferred,
              "elapsed_seconds": time.monotonic() - started,
              "wall_seconds": wall_seconds, "byte_budget": byte_budget}
    if summary:
        _json_write(directory / "summary.json", summary)
    _json_write(directory / "report.json", report)
    return report


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Allocate evidence or run bounded loopback-only engine baselines."
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)
    allocate = commands.add_parser(
        "allocate", help="atomically allocate a pending evidence directory"
    )
    allocate.add_argument("--artifact-root", required=True)
    allocate.add_argument("--run-id", required=True)
    run = commands.add_parser("run", help="measure local engine baseline or one smoke")
    run.add_argument("--artifact-root", required=True)
    run.add_argument("--run-id", required=True)
    run.add_argument("--mode", choices=("full", "smoke"), default="full")
    run.add_argument("--wall-seconds", type=float, default=None)
    run.add_argument("--byte-budget", type=int, default=None)
    run.add_argument("--trial-seconds", type=float, default=60)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Allocate evidence or explicitly run a bounded local-only baseline."""

    parser = _build_argument_parser()
    arguments = parser.parse_args(argv)
    if arguments.command == "run":
        try:
            report = run_baselines(Path(arguments.artifact_root), arguments.run_id,
                                   mode=arguments.mode,
                                   wall_seconds=arguments.wall_seconds if arguments.wall_seconds is not None else (120 if arguments.mode == "smoke" else 1200),
                                   byte_budget=arguments.byte_budget if arguments.byte_budget is not None else (2 * FIXTURE_PAYLOAD_BYTES if arguments.mode == "smoke" else 2 * 1024**3),
                                   trial_seconds=arguments.trial_seconds)
        except (BenchmarkValidationError, BudgetExceeded) as error:
            parser.error(str(error))
        print(json.dumps(report, sort_keys=True))
        return 0 if report["status"] in {"measured_local_baseline", "measured_local_smoke"} else 1
    if arguments.command == "allocate":
        try:
            run_directory = allocate_run_directory(arguments.artifact_root, arguments.run_id)
        except ArtifactAllocationError as error:
            parser.error(str(error))
        print(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "status": REPORT_STATUS,
                    "run_directory": str(run_directory),
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    parser.error("unsupported command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
