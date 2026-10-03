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
SENSITIVE_METADATA_KEYS = (
    "password",
    "passwords",
    "secret",
    "secrets",
    "api_key",
    "api-key",
    "apiKey",
    "APIKey",
    "api_keys",
    "api-keys",
    "apiKeys",
    "APIKeys",
    "auth",
    "authToken",
    "authTokens",
    "url",
    "URL",
    "urls",
    "uri",
    "uris",
    "source_url",
    "source-url",
    "sourceUrl",
    "sourceUrls",
    "sourceURI",
    "cookie",
    "cookies",
    "token",
    "tokens",
    "credential",
    "credentials",
    "header",
    "headers",
    "authorization",
    "authorizations",
    "input",
    "inputs",
)
TRAILING_ACRONYM_PLURAL_METADATA_KEYS = (
    "URLs",
    "URIs",
    "sourceURLs",
    "sourceURIs",
)


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
    "sensitive_key",
    SENSITIVE_METADATA_KEYS,
)
def test_rejects_sensitive_setting_keys_at_all_depths(sensitive_key: str) -> None:
    benchmark = _benchmark()

    direct = _passed_record("curl-single", 1)
    direct["configuration"]["settings"][sensitive_key] = (
        {"XTest": "1.0"} if sensitive_key == "headers" else "parallel"
    )
    with pytest.raises(benchmark.BenchmarkValidationError, match="raw external input"):
        benchmark.validate_trial_record(direct)

    nested = _passed_record("curl-single", 1)
    nested["configuration"]["settings"]["transport"] = {sensitive_key: "parallel"}
    with pytest.raises(benchmark.BenchmarkValidationError, match="raw external input"):
        benchmark.validate_trial_record(nested)


@pytest.mark.parametrize(
    "sensitive_key",
    SENSITIVE_METADATA_KEYS,
)
def test_rejects_sensitive_version_metadata_keys(sensitive_key: str) -> None:
    benchmark = _benchmark()

    normal = _passed_record("curl-single", 1)
    normal["configuration"]["versions"] = {
        "engine": "aria2-1.37.0",
        "Python": "3.12.12",
    }
    assert benchmark.validate_trial_record(normal) == normal

    malformed = deepcopy(normal)
    malformed["configuration"]["versions"] = {sensitive_key: "1.0"}
    with pytest.raises(benchmark.BenchmarkValidationError, match="raw external input"):
        benchmark.validate_trial_record(malformed)


@pytest.mark.parametrize(
    "sensitive_key",
    TRAILING_ACRONYM_PLURAL_METADATA_KEYS,
)
def test_rejects_trailing_acronym_plural_version_metadata_keys(
    sensitive_key: str,
) -> None:
    benchmark = _benchmark()
    record = _passed_record("curl-single", 1)
    record["configuration"]["versions"] = {sensitive_key: "1.0"}

    with pytest.raises(benchmark.BenchmarkValidationError, match="raw external input"):
        benchmark.validate_trial_record(record)


@pytest.mark.parametrize(
    "sensitive_key",
    TRAILING_ACRONYM_PLURAL_METADATA_KEYS,
)
def test_rejects_trailing_acronym_plural_setting_keys_recursively(
    sensitive_key: str,
) -> None:
    benchmark = _benchmark()
    record = _passed_record("curl-single", 1)
    record["configuration"]["settings"]["transport"] = {
        "metadata": {sensitive_key: "parallel"}
    }

    with pytest.raises(benchmark.BenchmarkValidationError, match="raw external input"):
        benchmark.validate_trial_record(record)


@pytest.mark.parametrize(
    ("key_name", "expected_tokens"),
    (
        ("URLs", ("urls",)),
        ("URIs", ("uris",)),
        ("sourceURLs", ("source", "urls")),
        ("sourceURIs", ("source", "uris")),
        ("APIKeys", ("api", "keys")),
        ("apiKeys", ("api", "keys")),
        ("sourceUrl", ("source", "url")),
        ("source_url", ("source", "url")),
        ("source-url", ("source", "url")),
        ("curl", ("curl",)),
    ),
)
def test_tokenizes_camel_snake_and_kebab_metadata_keys(
    key_name: str, expected_tokens: tuple[str, ...]
) -> None:
    benchmark = _benchmark()

    assert benchmark._metadata_key_tokens(key_name) == expected_tokens


def test_accepts_safe_version_inventory_metadata() -> None:
    benchmark = _benchmark()
    record = _passed_record("curl-single", 1)
    record["configuration"]["versions"] = {
        "curl": "8.7.1",
        "engine": "aria2-1.37.0",
        "Python": "3.12.12",
    }

    assert benchmark.validate_trial_record(record) == record


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


@pytest.mark.parametrize(
    ("container", "value"),
    (
        ("versions", "ghp_" + "0123456789012345678901234567890123456789"),
        ("settings", "private raw user content"),
        ("versions", "//private.example/path"),
        ("settings", "https://private.example/path"),
    ),
)
def test_rejects_raw_metadata_string_values(container: str, value: str) -> None:
    benchmark = _benchmark()
    record = _passed_record("curl-single", 1)
    if container == "versions":
        record["configuration"]["versions"] = {"engine": value}
    else:
        record["configuration"]["settings"]["note"] = value

    with pytest.raises(benchmark.BenchmarkValidationError):
        benchmark.validate_trial_record(record)


def test_rejects_more_than_32_version_metadata_entries() -> None:
    benchmark = _benchmark()
    record = _passed_record("curl-single", 1)
    record["configuration"]["versions"] = {
        f"tool{index}": "1.0" for index in range(33)
    }

    with pytest.raises(benchmark.BenchmarkValidationError):
        benchmark.validate_trial_record(record)


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


# T22b: injected records below test contracts, never measured benchmark evidence.
def _baseline_record(model: str, config_id: str, repetition: int) -> dict[str, Any]:
    benchmark = _benchmark()
    record = _passed_record(config_id, repetition)
    record.update(schema_version=2, purpose="local_engine_baseline",
                  runner_sha256="b" * 64, source_sha256="c" * 64)
    record["configuration"]["id"] = f"{model}-{config_id}"
    record["trial_id"] = f"{model}-{config_id}-{repetition}"
    record["configuration"]["settings"] = benchmark.baseline_settings(model, config_id)
    record["pause_probe"] = {
        "scope": "separate_graceful_engine_containment",
        "elapsed_seconds": 0.5, "cpu_seconds": 0.02, "max_rss_bytes": 4096,
        "server_payload_bytes": 65536, "client_logical_bytes": 65536,
        "server_unique_payload_bytes": 65536, "retransmitted_bytes": 0,
        "allocated_disk_bytes": 65536, "latency_seconds": record["pause_latency_seconds"],
        "forced": False, "contained": True,
    }
    return record


def test_real_baseline_sequence_is_three_balanced_rotations_under_two_models() -> None:
    benchmark = _benchmark()
    sequence = benchmark.baseline_sequence()
    assert len(sequence) == 18
    for model in ("unrestricted", "per-connection"):
        rows = [(config, rep) for origin, config, rep in sequence if origin == model]
        assert {row for row in rows} == {(config, rep) for config in EXPECTED_CONFIGURATIONS
                                           for rep in (1, 2, 3)}
        for rep in (1, 2, 3):
            assert [config for config, repetition in rows if repetition == rep] == (
                ["curl-single", "aria2-single", "aria2-multi"][rep-1:]
                + ["curl-single", "aria2-single", "aria2-multi"][:rep-1]
            )


def test_engine_argv_is_closed_loopback_no_configuration_or_credentials(tmp_path: Path) -> None:
    benchmark = _benchmark()
    curl = benchmark.engine_argv("curl-single", "/usr/bin/curl", 12345, tmp_path, 10)
    aria = benchmark.engine_argv("aria2-multi", "/opt/homebrew/bin/aria2c", 12345, tmp_path, 10)
    assert curl[1] == "-q"
    assert "--no-netrc" in curl and "--proxy" in curl and "--noproxy" in curl
    assert "--file-allocation=none" in aria and "--no-conf=true" in aria
    assert "--no-netrc=true" in aria and "--check-certificate=true" in aria
    assert "--allow-overwrite=false" in aria and "--auto-file-renaming=false" in aria
    assert "--split=16" in aria and "--max-connection-per-server=16" in aria
    assert curl[-1] == aria[-1] == "http://127.0.0.1:12345/payload"
    with pytest.raises(benchmark.BenchmarkValidationError):
        benchmark.engine_argv("curl-single --insecure", "/usr/bin/curl", 12345, tmp_path, 10)
    clean = benchmark.engine_environment(tmp_path)
    assert set(clean) == {"PATH", "HOME", "LC_ALL", "TMPDIR"}
    assert not any("proxy" in key.lower() for key in clean)


def test_monotonic_aggregate_budget_rejects_excess_before_transfer() -> None:
    benchmark = _benchmark()
    now = [1.0]
    budget = benchmark.RunBudget(10, 100, clock=lambda: now[0])
    budget.consume(70)
    with pytest.raises(benchmark.BudgetExceeded):
        budget.consume(31)
    assert budget.transferred == 70
    now[0] = 12
    with pytest.raises(benchmark.BudgetExceeded):
        budget.check()


def test_fixture_is_generated_deterministically_exclusive_and_budgeted(tmp_path: Path) -> None:
    benchmark = _benchmark()
    budget = benchmark.RunBudget(10, PAYLOAD_BYTES)
    first = tmp_path / "fixture"
    digest = benchmark.generate_fixture(first, budget)
    import hashlib
    assert first.stat().st_size == PAYLOAD_BYTES
    assert hashlib.sha256(first.read_bytes()).hexdigest() == digest
    assert first.read_bytes()[:512] == bytes(range(256)) * 2
    assert stat.S_IMODE(first.stat().st_mode) == 0o600
    with pytest.raises(FileExistsError):
        benchmark.generate_fixture(first, budget)
    assert budget.transferred == 0  # generation is setup, never network ledger
    expired = benchmark.RunBudget(1, 100, clock=lambda: 0)
    expired.clock = lambda: 2
    with pytest.raises(benchmark.BudgetExceeded):
        benchmark.generate_fixture(tmp_path / "expired", expired)


def test_actual_wait4_metrics_and_timeout_group_containment(tmp_path: Path) -> None:
    benchmark = _benchmark()
    # Fault control only: no engine evidence or extra public runner CLI arguments.
    result = benchmark.run_child(
        [sys.executable, "-c", "sum(i*i for i in range(100000))"], tmp_path,
        benchmark.RunBudget(5, 100), timeout=2, log_name="accounting")
    assert result["exit_code"] == 0 and result["contained"] is True
    assert result["cpu_seconds"] > 0 and result["max_rss_bytes"] > 0
    timed = benchmark.run_child(
        [sys.executable, "-c", "import time; time.sleep(10)"], tmp_path,
        benchmark.RunBudget(5, 100), timeout=0.05, log_name="timeout")
    assert timed["failure_classification"] == "timeout" and timed["contained"] is True
    with pytest.raises(ProcessLookupError):
        os.killpg(timed["pid"], 0)
    assert benchmark.normalize_rss(10, "darwin") == 10
    assert benchmark.normalize_rss(10, "linux") == 10240


def test_pause_probe_signals_active_child_and_accounts_separately(tmp_path: Path) -> None:
    benchmark = _benchmark()
    signals = []
    result = benchmark.run_child(
        [sys.executable, "-c", "import time; time.sleep(10)"], tmp_path,
        benchmark.RunBudget(5, 100), timeout=2, log_name="pause",
        pause_ready=lambda: True, signal_observer=signals.append)
    assert signals[0] == benchmark.signal.SIGTERM
    assert result["pause_latency_seconds"] > 0
    assert result["forced"] is False and result["contained"] is True


def test_v2_complete_summary_rejects_missing_extra_failed_scope_and_hash_mismatch() -> None:
    benchmark = _benchmark()
    records = [_baseline_record(model, config, rep)
               for model, config, rep in benchmark.baseline_sequence()]
    summary = benchmark.summarize_baseline_trials(records)
    assert summary["status"] == "measured_local_baseline"
    assert summary["trial_count"] == 18 and summary["schema_version"] == 2
    assert summary["pause_scope"] == "separate_graceful_engine_containment"
    assert summary["probe_totals"]["server_payload_bytes"] == 18 * 65536
    variants = [records[:-1], records + [records[0]]]
    for mutate in (lambda r: r.update(outcome="failed", failure_classification="timeout",
                                      completion_sha256=None, pause_latency_seconds=None,
                                      pause_probe=None),
                   lambda r: r.update(runner_sha256="d" * 64),
                   lambda r: r["pause_probe"].update(scope="worker_pause"),
                   lambda r: r["pause_probe"].update(server_payload_bytes=0),
                   lambda r: r["configuration"]["settings"].update(connections=3)):
        variant = deepcopy(records)
        mutate(variant[0])
        variants.append(variant)
    for variant in variants:
        with pytest.raises(benchmark.SummaryValidationError):
            benchmark.summarize_baseline_trials(variant)
    assert json.loads(benchmark.canonical_json(records[0])) == records[0]


def test_run_preserves_every_failed_trial_without_manufacturing_summary(tmp_path: Path,
                                                                      monkeypatch) -> None:
    benchmark = _benchmark()
    monkeypatch.setattr(benchmark, "inventory_engines", lambda *a: {"curl": ("/usr/bin/curl", "8.0"),
                                                                  "aria2": ("/missing/aria2", "1.0")})
    def fail(*args, **kwargs):
        raise benchmark.BudgetExceeded("bounded fault")
    monkeypatch.setattr(benchmark, "execute_baseline_trial", fail)
    report = benchmark.run_baselines(tmp_path, "failures", mode="full", wall_seconds=10,
                                     byte_budget=2 * 1024**3, trial_seconds=1)
    records = [json.loads(path.read_text()) for path in (tmp_path / "failures" / "trials").glob("*.json")]
    assert len(records) == 18 and all(r["outcome"] == "failed" for r in records)
    assert report["status"] == "failed_local_baseline" and not (tmp_path / "failures" / "summary.json").exists()
    for path in (tmp_path / "failures").rglob("*"):
        assert stat.S_IMODE(path.stat().st_mode) == (0o700 if path.is_dir() else 0o600)


def test_local_ledger_counts_partial_sends_and_repeated_ranges(tmp_path: Path) -> None:
    benchmark = _benchmark()
    origin = benchmark.LocalOrigin(tmp_path / "fixture", benchmark.RunBudget(5, 100), 0, 100)
    try:
        origin.transferred = 25
        origin.requests = [{"start": 0, "end": 19, "sent": 20},
                           {"start": 10, "end": 19, "sent": 5}]
        ledger = origin.ledger()
        assert ledger["server_payload_bytes"] == 25
        assert ledger["unique_payload_bytes"] == 20
        assert ledger["retransmitted_bytes"] == 5
        assert origin.server.server_address[0] == "127.0.0.1"
    finally:
        origin.server.server_close()
    class Socket:
        def send(self, data):
            return 3
    budget = benchmark.RunBudget(5, 10)
    assert budget.send(Socket(), b"abcdefgh") == 3
    assert budget.transferred == 3


def test_v2_probe_records_sparse_logical_size_and_its_own_retransmission() -> None:
    benchmark = _benchmark()
    record = _baseline_record("per-connection", "aria2-multi", 1)
    record["pause_probe"]["client_logical_bytes"] = PAYLOAD_BYTES
    record["pause_probe"]["server_unique_payload_bytes"] = 60000
    record["pause_probe"]["retransmitted_bytes"] = 5536
    assert benchmark.validate_trial_record(record) == record
    broken = deepcopy(record)
    broken["pause_probe"]["retransmitted_bytes"] += 1
    with pytest.raises(benchmark.BenchmarkValidationError):
        benchmark.validate_trial_record(broken)


def test_cli_rejects_user_sources_and_arbitrary_engine_arguments() -> None:
    benchmark = _benchmark()
    for extra in (['--url', 'http://127.0.0.1/private'], ['--engine-args', '--insecure']):
        with pytest.raises(SystemExit) as error:
            benchmark.main(['run', '--artifact-root', '/tmp/evidence', '--run-id', 'unused', *extra])
        assert error.value.code == 2


def test_ignored_term_is_forced_contained_and_never_a_successful_pause(tmp_path: Path) -> None:
    benchmark = _benchmark()
    result = benchmark.run_child(
        [sys.executable, "-c", "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(10)"],
        tmp_path, benchmark.RunBudget(2, 100), timeout=0.3, log_name="forced")
    assert result["forced"] is True and result["contained"] is True
    assert result["failure_classification"] == "timeout"
    assert result["elapsed_seconds"] < 2
    with pytest.raises(ProcessLookupError):
        os.killpg(result["pid"], 0)


def test_completion_file_is_verified_and_allocated_blocks_are_measured(tmp_path: Path) -> None:
    benchmark = _benchmark()
    payload = tmp_path / "payload"
    with payload.open("wb") as stream:
        stream.seek(1024 * 1024 - 1)
        stream.write(b"x")
    measured = benchmark._measure_file(payload, benchmark.RunBudget(5, 100), verify=True)
    import hashlib
    assert measured["client_payload_bytes"] == 1024 * 1024
    assert measured["allocated_disk_bytes"] == payload.stat().st_blocks * 512
    assert measured["completion_sha256"] == hashlib.sha256(payload.read_bytes()).hexdigest()
    absent = benchmark._measure_file(tmp_path / "absent", benchmark.RunBudget(5, 100), verify=False)
    assert absent == {"client_payload_bytes": None, "allocated_disk_bytes": None, "completion_sha256": None}


def test_failed_setup_still_retains_the_entire_requested_matrix(tmp_path: Path, monkeypatch) -> None:
    benchmark = _benchmark()
    def missing(*args):
        raise benchmark.BenchmarkValidationError("local engine missing")
    monkeypatch.setattr(benchmark, "inventory_engines", missing)
    report = benchmark.run_baselines(tmp_path, "setup-failed", mode="full", wall_seconds=10,
                                     byte_budget=2 * 1024**3, trial_seconds=1)
    records = [json.loads(path.read_text()) for path in (tmp_path / "setup-failed" / "trials").glob("*.json")]
    assert len(records) == 18
    assert all(r["failure_classification"] == "setup_error" for r in records)
    assert all(r["cpu_seconds"] is None and r["server_payload_bytes"] is None for r in records)
    assert report["status"] == "failed_local_baseline"
    assert not (tmp_path / "setup-failed" / "summary.json").exists()


def test_origin_setup_bounds_idle_headers_and_closes_on_expired_entry(tmp_path: Path,
                                                                    monkeypatch) -> None:
    benchmark = _benchmark()
    budget = benchmark.RunBudget(1, 100, clock=lambda: 0)
    origin = benchmark.LocalOrigin(tmp_path / "fixture", budget, 0, 100)
    calls = []
    class Socket:
        def settimeout(self, seconds):
            calls.append(seconds)
    monkeypatch.setattr(benchmark.http.server.BaseHTTPRequestHandler, "setup",
                        lambda self: calls.append("base"))
    handler = object.__new__(origin.server.RequestHandlerClass)
    handler.request = Socket()
    handler.setup()
    assert calls == [0.25, "base"]
    budget.clock = lambda: 2
    with pytest.raises(benchmark.BudgetExceeded):
        origin.__enter__()
    assert origin.server.fileno() == -1


def test_v2_summary_rejects_cross_configuration_version_inventory_mismatch() -> None:
    benchmark = _benchmark()
    records = [_baseline_record(model, config, rep)
               for model, config, rep in benchmark.baseline_sequence()]
    for record in records:
        if record["configuration"]["id"] == "unrestricted-curl-single":
            record["configuration"]["versions"]["python"] = "3.12.99"
    with pytest.raises(benchmark.SummaryValidationError, match="versions"):
        benchmark.summarize_baseline_trials(records)


def test_v2_trial_identity_must_correspond_to_configuration_and_repetition() -> None:
    benchmark = _benchmark()
    record = _baseline_record("unrestricted", "curl-single", 1)
    record["trial_id"] = "unrestricted-aria2-single-1"
    with pytest.raises(benchmark.BenchmarkValidationError, match="identity"):
        benchmark.validate_trial_record(record)


@pytest.mark.parametrize("versions", [(1, 2, 1), (2, 2, 2)])
def test_public_v1_summary_rejects_mixed_and_all_v2_records(versions) -> None:
    benchmark = _benchmark()
    records = [_baseline_record("unrestricted", "curl-single", rep) for rep in (1, 2, 3)]
    for record, version in zip(records, versions):
        if version == 1:
            for key in benchmark.BASELINE_FIELDS:
                record.pop(key)
            record["schema_version"] = 1
    with pytest.raises(benchmark.SummaryValidationError, match="schema"):
        benchmark.summarize_trials(records, {"unrestricted-curl-single"}, repetitions=3)


def test_v2_summary_validates_then_explicitly_converts_for_v1_reuse(monkeypatch) -> None:
    benchmark = _benchmark()
    records = [_baseline_record(model, config, rep)
               for model, config, rep in benchmark.baseline_sequence()]
    original = deepcopy(records)
    legacy_summary = benchmark.summarize_trials
    converted = []
    def capture(trials, expected, *, repetitions):
        converted.extend(deepcopy(trials))
        return legacy_summary(trials, expected, repetitions=repetitions)
    monkeypatch.setattr(benchmark, "summarize_trials", capture)
    summary = benchmark.summarize_baseline_trials(records)
    assert len(converted) == 18 and all(r["schema_version"] == 1 for r in converted)
    assert all(not (set(r) & benchmark.BASELINE_FIELDS) for r in converted)
    assert records == original
    assert summary["schema_version"] == 2 and summary["status"] == "measured_local_baseline"
    assert legacy_summary(converted, {r["configuration"]["id"] for r in converted},
                          repetitions=3)["status"] == "pending_real_execution"
    converted.clear()
    records[0]["pause_probe"]["contained"] = False
    with pytest.raises(benchmark.SummaryValidationError):
        benchmark.summarize_baseline_trials(records)
    assert converted == []


def _injected_containment_failure(benchmark, component="engine"):
    # No real process authority is invented by these boundary injections.
    failure_type = getattr(benchmark, "ContainmentFailure", None)
    if failure_type is None:
        return RuntimeError("engine group containment could not be verified")
    return failure_type(component, authority=None,
                        observations={"group_absent": None} if component == "engine"
                        else {"thread_stopped": False})


@pytest.mark.parametrize("boundary", ["setup", "inventory", "trial"])
def test_uncertain_containment_retains_matrix_and_stops_engine_admission(
        tmp_path: Path, monkeypatch, boundary) -> None:
    benchmark = _benchmark()
    launches = []
    def fixture(path, budget):
        if boundary == "setup":
            raise _injected_containment_failure(benchmark, "origin")
        path.write_bytes(b"test-only-fixture")
        return FIXTURE_SHA256
    monkeypatch.setattr(benchmark, "generate_fixture", fixture)
    monkeypatch.setattr(benchmark.shutil, "which", lambda *a, **k: "/usr/bin/true")
    def child(*args, **kwargs):
        launches.append(kwargs["log_name"])
        raise _injected_containment_failure(benchmark)
    monkeypatch.setattr(benchmark, "run_child", child)
    if boundary == "trial":
        monkeypatch.setattr(benchmark, "inventory_engines", lambda *a: {
            "curl": ("/usr/bin/true", "8.0"), "aria2": ("/usr/bin/true", "1.0")})
    report = benchmark.run_baselines(tmp_path, boundary, wall_seconds=10, trial_seconds=1)
    directory = tmp_path / boundary
    records = [json.loads((directory / "trials" / f"{m}-{e}-{r}.json").read_text())
               for m, e, r in benchmark.baseline_sequence()]
    assert len(records) == report["trial_count"] == 18
    assert launches == ([] if boundary == "setup" else
                        ["curl-version"] if boundary == "inventory" else ["engine"])
    assert all(r["outcome"] == "failed" and r["failure_classification"] == "containment_error"
               for r in records)
    assert all(all(r[m] is None for m in benchmark.MEASURED_METRICS) for r in records[1:])
    assert all(r["completion_sha256"] is None and r["pause_probe"] is None for r in records)
    if boundary != "trial":
        assert all(r["configuration"]["versions"] == {"python": sys.version.split()[0]}
                   for r in records)
    evidence = json.loads((directory / "containment-failure.json").read_text())
    assert evidence["authority"] is None
    assert evidence["component"] == ("origin" if boundary == "setup" else "engine")
    assert evidence["status"] == "uncertain" and evidence["message"]
    assert report["status"] == "failed_local_baseline" and report["containment_failure"] == evidence
    assert json.loads((directory / "report.json").read_text()) == report
    assert not (directory / "summary.json").exists()
    for record in records:
        benchmark.validate_trial_record(record)
    for path in directory.rglob("*"):
        assert stat.S_IMODE(path.stat().st_mode) == (0o700 if path.is_dir() else 0o600)


@pytest.mark.parametrize("component", ["engine", "origin"])
def test_probe_containment_failure_preserves_completion_and_partial_accounting(
        tmp_path: Path, monkeypatch, component) -> None:
    benchmark = _benchmark()
    launches = []
    monkeypatch.setattr(benchmark, "generate_fixture", lambda *a: FIXTURE_SHA256)
    monkeypatch.setattr(benchmark, "inventory_engines", lambda *a: {
        "curl": ("/usr/bin/true", "8.0"), "aria2": ("/usr/bin/true", "1.0")})
    class Origin:
        def __init__(self, fixture, budget, rate, byte_limit):
            self.probe = byte_limit == 4 * 1024**2
            self.port, self.error, self.transferred = 12345, None, 65536
        def __enter__(self):
            return self
        def __exit__(self, *args):
            if self.probe and component == "origin":
                raise _injected_containment_failure(benchmark, "origin")
        def ledger(self):
            size = 65536 if self.probe else PAYLOAD_BYTES
            return {"server_payload_bytes": size, "unique_payload_bytes": size,
                    "retransmitted_bytes": 0, "requests": []}
    monkeypatch.setattr(benchmark, "LocalOrigin", Origin)
    def child(argv, directory, budget, **kwargs):
        launches.append(directory.name)
        if directory.name == "pause" and component == "engine":
            raise _injected_containment_failure(benchmark)
        return {"pid": 123, "exit_code": 0, "elapsed_seconds": 0.5, "cpu_seconds": 0.1,
                "max_rss_bytes": 4096, "contained": True, "forced": False,
                "pause_latency_seconds": 0.1 if directory.name == "pause" else None}
    monkeypatch.setattr(benchmark, "run_child", child)
    monkeypatch.setattr(benchmark, "_measure_file", lambda *a, **k: {
        "client_payload_bytes": PAYLOAD_BYTES, "allocated_disk_bytes": PAYLOAD_BYTES,
        "completion_sha256": FIXTURE_SHA256})
    report = benchmark.run_baselines(tmp_path, component, wall_seconds=10, trial_seconds=1)
    directory = tmp_path / component
    records = [json.loads((directory / "trials" / f"{m}-{e}-{r}.json").read_text())
               for m, e, r in benchmark.baseline_sequence()]
    assert launches == ["completion", "pause"]
    assert len(records) == 18 and len(report["failed_trials"]) == 18
    first = records[0]
    assert first["elapsed_seconds"] == 0.5 and first["cpu_seconds"] == 0.1
    assert first["server_payload_bytes"] == first["client_payload_bytes"] == PAYLOAD_BYTES
    assert first["pause_latency_seconds"] is None and first["completion_sha256"] is None
    assert first["pause_probe"]["server_payload_bytes"] == 65536
    assert first["pause_probe"]["client_logical_bytes"] is None
    assert first["pause_probe"]["contained"] is False
    assert first["pause_probe"]["latency_seconds"] is None
    assert first["pause_probe"]["cpu_seconds"] == (None if component == "engine" else 0.1)
    assert all(all(r[m] is None for m in benchmark.MEASURED_METRICS) for r in records[1:])
    assert report["status"] == "failed_local_baseline" and not (directory / "summary.json").exists()
    for record in records:
        benchmark.validate_trial_record(record)


def test_run_child_uncertain_group_raises_typed_failure_with_owned_authority(
        tmp_path: Path, monkeypatch) -> None:
    benchmark = _benchmark()
    from types import SimpleNamespace
    now = [0.0]
    def clock():
        now[0] += 0.2
        return now[0]
    monkeypatch.setattr(benchmark.time, "monotonic", clock)
    monkeypatch.setattr(benchmark.time, "sleep", lambda *a: None)
    process = SimpleNamespace(pid=123456, returncode=None)
    monkeypatch.setattr(benchmark.subprocess, "Popen", lambda *a, **k: process)
    monkeypatch.setattr(benchmark.os, "wait4", lambda pid, flags: (
        pid, 0, SimpleNamespace(ru_utime=0.1, ru_stime=0.2, ru_maxrss=4096)))
    monkeypatch.setattr(benchmark, "_group_exists", lambda pid: True)
    signals = []
    monkeypatch.setattr(benchmark.os, "killpg", lambda pid, sig: signals.append((pid, sig)))
    with pytest.raises(RuntimeError) as caught:
        benchmark.run_child(["test-only"], tmp_path, benchmark.RunBudget(4, 100, clock=clock),
                            timeout=1, log_name="uncertain")
    error = caught.value
    assert type(error).__name__ == "ContainmentFailure"
    assert error.evidence["authority"] == {
        "pid": 123456, "process_group_id": 123456, "session_id": 123456}
    assert error.evidence["observations"]["group_absent"] is False
    assert error.evidence["observations"]["leader_reaped"] is True
    assert error.accounting["contained"] is False and error.accounting["cpu_seconds"] > 0
    assert signals == [(123456, benchmark.signal.SIGTERM), (123456, benchmark.signal.SIGKILL)]


def test_origin_cleanup_uncertainty_is_typed_after_real_threads_are_joined(
        tmp_path: Path, monkeypatch) -> None:
    benchmark = _benchmark()
    origin = benchmark.LocalOrigin(tmp_path / "fixture", benchmark.RunBudget(5, 100), 0, 100)
    origin.__enter__()
    thread = origin.thread
    # Join the actual thread; inject only the final observation, never an orphan.
    class Observation:
        def join(self, timeout):
            thread.join(timeout=timeout)
        def is_alive(self):
            return True
    origin.thread = Observation()
    try:
        with pytest.raises(RuntimeError) as caught:
            origin.__exit__()
        assert type(caught.value).__name__ == "ContainmentFailure"
        assert caught.value.evidence["component"] == "origin"
        assert caught.value.evidence["authority"] is None
        assert caught.value.evidence["observations"]["thread_stopped"] is False
    finally:
        thread.join(timeout=1)
        origin.server.server_close()
    assert not thread.is_alive() and origin.server.fileno() == -1


def test_unrelated_runtime_error_is_not_classified_as_containment(tmp_path: Path, monkeypatch) -> None:
    benchmark = _benchmark()
    def unrelated(*args):
        raise RuntimeError("unrelated programming fault")
    monkeypatch.setattr(benchmark, "generate_fixture", unrelated)
    with pytest.raises(RuntimeError, match="unrelated programming fault"):
        benchmark.run_baselines(tmp_path, "unrelated", wall_seconds=10)


def test_origin_cleanup_error_preserves_prior_owned_engine_authority(tmp_path: Path, monkeypatch) -> None:
    benchmark = _benchmark()
    failure = benchmark.ContainmentFailure("engine", authority={
        "pid": 123456, "process_group_id": 123456, "session_id": 123456},
        observations={"group_absent": None, "leader_reaped": False})
    origin = benchmark.LocalOrigin(tmp_path / "fixture", benchmark.RunBudget(5, 100), 0, 100)
    def denied():
        raise OSError("test-only-sensitive-detail")
    monkeypatch.setattr(origin.server, "shutdown", denied)
    try:
        with pytest.raises(benchmark.ContainmentFailure) as caught:
            origin.__exit__(type(failure), failure, None)
        assert caught.value is failure
        assert caught.value.evidence["authority"]["pid"] == 123456
        assert caught.value.evidence["origin_failure"]["component"] == "origin"
        assert "test-only-sensitive-detail" not in json.dumps(caught.value.evidence)
    finally:
        origin.server.server_close()
