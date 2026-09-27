"""Behavioral contract for local downloader benchmark evidence artifacts."""

from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
from typing import Any

import pytest


HEADLESS_ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_PATH = HEADLESS_ROOT / "scripts" / "benchmark.py"
FIXTURE_SHA256 = "a" * 64
PAYLOAD_BYTES = 64 * 1024 * 1024
EXPECTED_CONFIGURATIONS = {"curl-single", "aria2-single", "aria2-multi"}


def _benchmark():
    assert BENCHMARK_PATH.is_file(), (
        "headless/scripts/benchmark.py must provide local benchmark evidence helpers"
    )
    module_name = "_download_manager_benchmark_under_test"
    spec = importlib.util.spec_from_file_location(module_name, BENCHMARK_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _configuration(config_id: str, repetition: int) -> dict[str, Any]:
    engine, connections = {
        "curl-single": ("curl", 1),
        "aria2-single": ("aria2", 1),
        "aria2-multi": ("aria2", 16),
    }[config_id]
    return {
        "id": config_id,
        "repetition": repetition,
        "engine": engine,
        "source": "deterministic_local_fixture",
        "versions": {"engine": "test-1.0", "python": "3.12"},
        "settings": {"connections": connections, "file_allocation": "none"},
    }


def _passed_record(
    config_id: str,
    repetition: int,
    *,
    elapsed_seconds: float = 2.0,
    run_id: str = "matrix-20260927",
    fixture_sha256: str = FIXTURE_SHA256,
) -> dict[str, Any]:
    retransmitted_bytes = repetition
    return {
        "schema_version": 1,
        "run_id": run_id,
        "trial_id": f"{config_id}-trial-{repetition}",
        "configuration": _configuration(config_id, repetition),
        "outcome": "passed",
        "fixture_sha256": fixture_sha256,
        "observed_at_utc": "2026-09-27T00:00:00Z",
        "payload_bytes": PAYLOAD_BYTES,
        "elapsed_seconds": elapsed_seconds,
        "cpu_seconds": elapsed_seconds / 2,
        "max_rss_bytes": 1024 * repetition,
        "server_payload_bytes": PAYLOAD_BYTES + retransmitted_bytes,
        "retransmitted_bytes": retransmitted_bytes,
        "client_payload_bytes": PAYLOAD_BYTES,
        "allocated_disk_bytes": PAYLOAD_BYTES + (4096 * repetition),
        "pause_latency_seconds": elapsed_seconds / 10,
        "completion_sha256": fixture_sha256,
    }


def _failed_record() -> dict[str, Any]:
    record = _passed_record("curl-single", 1)
    record.update(
        {
            "outcome": "failed",
            "failure_classification": "engine_error",
            "elapsed_seconds": None,
            "cpu_seconds": None,
            "max_rss_bytes": None,
            "server_payload_bytes": None,
            "retransmitted_bytes": None,
            "client_payload_bytes": None,
            "allocated_disk_bytes": None,
            "pause_latency_seconds": None,
            "completion_sha256": None,
        }
    )
    return record


def _complete_records() -> list[dict[str, Any]]:
    elapsed_by_configuration = {
        "curl-single": (3.0, 1.0, 2.0),
        "aria2-single": (6.0, 4.0, 5.0),
        "aria2-multi": (9.0, 7.0, 8.0),
    }
    return [
        _passed_record(config_id, repetition, elapsed_seconds=elapsed_seconds)
        for config_id, elapsed_values in elapsed_by_configuration.items()
        for repetition, elapsed_seconds in enumerate(elapsed_values, start=1)
    ]


def test_allocates_owner_only_run_directory_and_never_clobbers_a_symlink(
    tmp_path: Path,
) -> None:
    benchmark = _benchmark()
    artifact_root = tmp_path / ".artifacts" / "download-manager"

    created = benchmark.allocate_run_directory(artifact_root, "matrix-20260927")

    assert created == artifact_root / "matrix-20260927"
    assert created.is_dir()
    assert stat.S_IMODE(created.stat().st_mode) == 0o700
    with pytest.raises(benchmark.ArtifactAllocationError):
        benchmark.allocate_run_directory(artifact_root, "matrix-20260927")

    outside = tmp_path / "outside"
    outside.mkdir()
    symlink_target = artifact_root / "matrix-20260928"
    symlink_target.symlink_to(outside, target_is_directory=True)

    with pytest.raises(benchmark.ArtifactAllocationError):
        benchmark.allocate_run_directory(artifact_root, "matrix-20260928")

    assert symlink_target.is_symlink()
    assert outside.is_dir()


@pytest.mark.parametrize(
    "bad_identifier",
    ("", ".", "..", "../escape", "nested/run", "nested\\run", "has space", "\x00", "x" * 65),
)
def test_rejects_nonportable_run_and_trial_identifiers(bad_identifier: str) -> None:
    benchmark = _benchmark()

    with pytest.raises(benchmark.BenchmarkValidationError):
        benchmark.validate_run_id(bad_identifier)
    with pytest.raises(benchmark.BenchmarkValidationError):
        benchmark.validate_trial_id(bad_identifier)


def test_validates_complete_records_and_rejects_fake_or_nonfinite_measurements() -> None:
    benchmark = _benchmark()
    record = _passed_record("curl-single", 1)

    validated = benchmark.validate_trial_record(record)

    assert validated == record
    for field, value in (
        ("elapsed_seconds", float("nan")),
        ("cpu_seconds", float("inf")),
        ("payload_bytes", 0),
        ("completion_sha256", "b" * 64),
    ):
        malformed = deepcopy(record)
        malformed[field] = value
        with pytest.raises(benchmark.BenchmarkValidationError):
            benchmark.validate_trial_record(malformed)

    with_url = deepcopy(record)
    with_url["configuration"]["source"] = "https://example.test/payload.bin"
    with pytest.raises(benchmark.BenchmarkValidationError):
        benchmark.validate_trial_record(with_url)


@pytest.mark.parametrize(
    "credential_key",
    (
        "password",
        "secret",
        "api_key",
        "api-key",
        "apiKey",
        "APIKey",
        "auth",
        "authToken",
    ),
)
def test_rejects_raw_credential_like_setting_keys_at_all_depths(credential_key: str) -> None:
    benchmark = _benchmark()

    direct = _passed_record("curl-single", 1)
    direct["configuration"]["settings"][credential_key] = "raw-secret"
    with pytest.raises(benchmark.BenchmarkValidationError, match="raw external input"):
        benchmark.validate_trial_record(direct)

    nested = _passed_record("curl-single", 1)
    nested["configuration"]["settings"]["transport"] = {credential_key: "raw-secret"}
    with pytest.raises(benchmark.BenchmarkValidationError, match="raw external input"):
        benchmark.validate_trial_record(nested)


@pytest.mark.parametrize(
    "credential_key",
    (
        "password",
        "secret",
        "api_key",
        "api-key",
        "apiKey",
        "APIKey",
        "auth",
        "authToken",
    ),
)
def test_rejects_raw_credential_like_version_metadata_keys(credential_key: str) -> None:
    benchmark = _benchmark()

    normal = _passed_record("curl-single", 1)
    normal["configuration"]["versions"] = {
        "engine": "aria2-1.37.0",
        "Python": "3.12.12",
    }
    assert benchmark.validate_trial_record(normal) == normal

    malformed = deepcopy(normal)
    malformed["configuration"]["versions"] = {credential_key: "raw-secret"}
    with pytest.raises(benchmark.BenchmarkValidationError, match="raw external input"):
        benchmark.validate_trial_record(malformed)


def test_accepts_ordinary_bounded_benchmark_settings() -> None:
    benchmark = _benchmark()
    record = _passed_record("curl-single", 1)
    record["configuration"]["settings"].update(
        {
            "retry_count": 3,
            "transport": {"mode": "parallel", "verify_tls": True},
        }
    )

    assert benchmark.validate_trial_record(record) == record


def test_failed_record_requires_a_bounded_classification_and_null_unobserved_metrics() -> None:
    benchmark = _benchmark()
    failed = _failed_record()

    assert benchmark.validate_trial_record(failed) == failed

    missing_classification = deepcopy(failed)
    del missing_classification["failure_classification"]
    with pytest.raises(benchmark.BenchmarkValidationError):
        benchmark.validate_trial_record(missing_classification)

    unbounded_classification = deepcopy(failed)
    unbounded_classification["failure_classification"] = "raw engine stderr"
    with pytest.raises(benchmark.BenchmarkValidationError):
        benchmark.validate_trial_record(unbounded_classification)

    fake_zero_metrics = deepcopy(failed)
    for field in benchmark.MEASURED_METRICS:
        fake_zero_metrics[field] = 0
    with pytest.raises(benchmark.BenchmarkValidationError):
        benchmark.validate_trial_record(fake_zero_metrics)


def test_canonical_json_is_deterministic_and_rejects_unknown_record_fields() -> None:
    benchmark = _benchmark()
    record = _passed_record("curl-single", 1)
    reordered = {key: record[key] for key in reversed(tuple(record))}

    encoded = benchmark.canonical_json(record)

    assert encoded == benchmark.canonical_json(reordered)
    assert encoded.endswith("\n")
    assert ", " not in encoded
    assert json.loads(encoded) == record

    unknown = deepcopy(record)
    unknown["source_url"] = "https://example.test/not-recorded"
    with pytest.raises(benchmark.BenchmarkValidationError):
        benchmark.validate_trial_record(unknown)


def test_summarizer_fails_closed_for_missing_failed_duplicate_and_inconsistent_trials() -> None:
    benchmark = _benchmark()
    records = _complete_records()

    with pytest.raises(benchmark.SummaryValidationError, match="missing expected configuration"):
        benchmark.summarize_trials(
            [record for record in records if record["configuration"]["id"] != "aria2-multi"],
            EXPECTED_CONFIGURATIONS,
            repetitions=3,
        )

    failed = deepcopy(records)
    failed[0] = _failed_record()
    with pytest.raises(benchmark.SummaryValidationError, match="failed trial"):
        benchmark.summarize_trials(failed, EXPECTED_CONFIGURATIONS, repetitions=3)

    duplicate_trial = deepcopy(records)
    duplicate_trial.append(deepcopy(records[0]))
    with pytest.raises(benchmark.SummaryValidationError, match="duplicate trial_id"):
        benchmark.summarize_trials(duplicate_trial, EXPECTED_CONFIGURATIONS, repetitions=3)

    inconsistent_fixture = deepcopy(records)
    inconsistent_fixture[1]["fixture_sha256"] = "b" * 64
    inconsistent_fixture[1]["completion_sha256"] = "b" * 64
    with pytest.raises(benchmark.SummaryValidationError, match="fixture hashes"):
        benchmark.summarize_trials(
            inconsistent_fixture, EXPECTED_CONFIGURATIONS, repetitions=3
        )

    inconsistent_run = deepcopy(records)
    inconsistent_run[1]["run_id"] = "other-matrix"
    with pytest.raises(benchmark.SummaryValidationError, match="run IDs"):
        benchmark.summarize_trials(inconsistent_run, EXPECTED_CONFIGURATIONS, repetitions=3)


def test_summarizer_rejects_duplicate_repetition_and_incompatible_accounting() -> None:
    benchmark = _benchmark()
    records = _complete_records()

    duplicate_repetition = deepcopy(records)
    duplicate_repetition[1]["configuration"]["repetition"] = 1
    with pytest.raises(benchmark.SummaryValidationError, match="duplicate repetition"):
        benchmark.summarize_trials(
            duplicate_repetition, EXPECTED_CONFIGURATIONS, repetitions=3
        )

    incompatible_accounting = deepcopy(records)
    incompatible_accounting[0]["server_payload_bytes"] += 1
    with pytest.raises(benchmark.SummaryValidationError, match="accounting"):
        benchmark.summarize_trials(
            incompatible_accounting, EXPECTED_CONFIGURATIONS, repetitions=3
        )


def test_cli_allocate_command_only_allocates_pending_evidence_directory(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    benchmark = _benchmark()

    exit_code = benchmark.main(
        [
            "allocate",
            "--artifact-root",
            str(tmp_path / ".artifacts" / "download-manager"),
            "--run-id",
            "matrix-20260927",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["status"] == "pending_real_execution"
    assert output["run_directory"].endswith("matrix-20260927")


def test_summarizer_calculates_p50_and_inclusive_ranges_with_pending_status() -> None:
    benchmark = _benchmark()

    report = benchmark.summarize_trials(
        _complete_records(), EXPECTED_CONFIGURATIONS, repetitions=3
    )

    assert report["status"] == "pending_real_execution"
    assert set(report["configurations"]) == EXPECTED_CONFIGURATIONS
    assert report["configurations"]["curl-single"]["metrics"]["elapsed_seconds"] == {
        "p50": 2.0,
        "range": {"min": 1.0, "max": 3.0},
    }
    assert report["configurations"]["aria2-single"]["metrics"]["elapsed_seconds"] == {
        "p50": 5.0,
        "range": {"min": 4.0, "max": 6.0},
    }
    assert report["fixture"]["payload_bytes"] == PAYLOAD_BYTES
    assert report["repetitions"] == 3
