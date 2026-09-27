#!/usr/bin/env python3
"""Allocate and summarize local downloader benchmark evidence.

This T22a helper deliberately does not generate a payload, launch an engine, open a
network connection, or claim benchmark acceptance.  It only establishes the
filesystem and JSON evidence rules that a later controlled runner must follow.

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
import json
import math
import os
from pathlib import Path
import re
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
    return "://" in value or value.startswith(("file:", "data:"))


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
        if not value or len(value) > 256 or _contains_external_source(value):
            raise BenchmarkValidationError(f"{label} must not contain raw external input")
        return
    if type(value) is list:
        if len(value) > 32:
            raise BenchmarkValidationError(f"{label} has too many values")
        for index, item in enumerate(value):
            _validate_setting_value(item, f"{label}[{index}]", depth + 1)
        return
    if type(value) is dict:
        if len(value) > 32:
            raise BenchmarkValidationError(f"{label} has too many keys")
        for key, item in value.items():
            key_name = _validate_identifier(key, f"{label} key")
            normalized = key_name.casefold().replace("-", "_")
            if any(
                token in normalized
                for token in (
                    "url",
                    "uri",
                    "cookie",
                    "token",
                    "credential",
                    "authorization",
                    "header",
                    "input",
                )
            ):
                raise BenchmarkValidationError(f"{label} must not contain raw external input")
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
    for name, version in versions.items():
        _validate_identifier(name, "configuration.versions key")
        if (
            type(version) is not str
            or not version
            or len(version) > 128
            or _contains_external_source(version)
        ):
            raise BenchmarkValidationError("configuration.versions must contain bounded strings")

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


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Allocate pending local benchmark evidence directories; this command "
            "does not run a benchmark."
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)
    allocate = commands.add_parser(
        "allocate", help="atomically allocate a pending evidence directory"
    )
    allocate.add_argument("--artifact-root", required=True)
    allocate.add_argument("--run-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the narrow allocation CLI without launching a download or benchmark."""

    parser = _build_argument_parser()
    arguments = parser.parse_args(argv)
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
