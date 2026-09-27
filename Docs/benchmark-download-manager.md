# Download Manager benchmark evidence

## Status and scope

**No benchmark result exists yet.** T22a adds only local evidence allocation, strict
trial validation, and deterministic summary mechanics. It does not generate the
fixture, start curl or aria2, transfer a payload, open a network connection, or
make a numerical performance, recovery, A08, A09, or A11 claim.

`headless/scripts/benchmark.py` is standard-library-only. Its `allocate` command
creates a pending evidence directory; it is not a benchmark runner. Any generated
summary has the literal status `pending_real_execution`, never a passing acceptance
status.

## Artifact location and allocation

Canonical future evidence is kept outside Git at:

```text
.artifacts/download-manager/<run-id>/
```

The reserved run layout is:

```text
<run-id>/
  trials/<trial-id>.json   # canonical diagnostic record per future trial
  summary.json             # derived pending_real_execution report
```

T22a allocates only `<run-id>` and does not fabricate either trial or summary
artifact. A later controlled runner may populate the reserved paths only after it
has observed real measurements.

The repository ignores only `.artifacts/download-manager/`; it does not blanket
ignore `.artifacts/`. Callers must explicitly supply the absolute
`.artifacts/download-manager` root to `allocate_run_directory` or to the CLI:

```text
headless/scripts/benchmark.py allocate \
  --artifact-root /absolute/path/to/.artifacts/download-manager \
  --run-id matrix-20260927
```

`run_id` and `trial_id` are portable 1–64-character identifiers containing only
ASCII letters, digits, `_`, and `-`, beginning with a letter or digit. They cannot
contain a separator, dot path, space, NUL, or traversal component.

Allocation uses an exclusive directory creation for `<run-id>`. A pre-existing
file, directory, dangling symlink, or directory symlink with that name is rejected;
the allocator never merges with or overwrites it. New managed directories are mode
`0700` on POSIX, and the supplied root chain must contain real directories rather
than symlinks. A future runner writes its immutable trial JSON records and derived
summary below that freshly allocated run directory.

## Trial JSON contract

Each trial record is a canonical UTF-8 JSON object serialized with sorted keys,
compact separators, no nonfinite numbers, and a trailing newline. The base object
has exactly these fields:

```text
schema_version
run_id
trial_id
configuration
outcome
fixture_sha256
observed_at_utc
payload_bytes
elapsed_seconds
cpu_seconds
max_rss_bytes
server_payload_bytes
retransmitted_bytes
client_payload_bytes
allocated_disk_bytes
pause_latency_seconds
completion_sha256
```

`schema_version` is currently `1`. `outcome` is exactly `passed` or `failed`.
Passed records have precisely the base fields and all measurement fields are finite,
nonnegative numbers; payload bytes and elapsed time must be positive. Their
completion SHA-256 is lowercase hexadecimal and must equal the fixture SHA-256.
The accounting identity is enforced:

```text
client_payload_bytes == payload_bytes
server_payload_bytes == client_payload_bytes + retransmitted_bytes
```

The configuration is a closed object with these fields:

```text
id, repetition, engine, source, versions, settings
```

`source` is exactly `deterministic_local_fixture`, `engine` is `curl` or `aria2`,
`versions` and `settings` are bounded JSON objects, and settings records a positive
`connections` value. This captures the version inventory, engine settings, fixture
source label, and trial order without recording a source URL, credentials, headers,
or other raw external input.

A failed record is intentionally diagnostic rather than a zero-valued successful
measurement. It has the base fields plus required `failure_classification`, one of:

```text
accounting_error, allocation_error, engine_error, environment_error,
integrity_error, pause_error, setup_error, timeout
```

Its `completion_sha256` is `null`, and at least one unobserved measurement is
`null`. Any observed failure metric remains finite and nonnegative. Failed artifacts
are retained for diagnosis but cannot become canonical acceptance evidence.

## Fail-closed summary

`summarize_trials(trials, expected_configurations, repetitions=...)` is pure: it
makes no filesystem, network, or engine calls. The caller must provide a nonempty
explicit set of expected configuration IDs and an exact positive repetition count.
For every expected configuration, the summarizer requires every repetition number
from `1` through that count, with identical configuration settings apart from the
repetition number.

It rejects rather than filters or repairs any of the following:

- missing or unexpected configurations, trials, or repetitions;
- duplicate trial IDs or duplicate configuration repetitions;
- failed trial records;
- different run IDs, fixture hashes, payload sizes, schema versions, or settings;
- a payload size other than the controlled 64 MiB fixture;
- incompatible client/server/retransmission accounting or malformed measurements.

On a complete matrix it computes each configuration's p50 in code and returns an
inclusive `min`/`max` range for elapsed time, CPU time, maximum RSS, server payload,
retransmitted bytes, client payload, allocated disk bytes, and pause latency. The
report remains `pending_real_execution` even when synthetic unit-test records are
complete.

## Deferred controlled matrix and acceptance thresholds

The later controlled local matrix must use a deterministic **64 MiB** synthetic
payload and three balanced repetitions for each of:

1. `curl-single`
2. `aria2-single`
3. `aria2-multi`

All configurations use the same fixture and record versions, settings, hash, CPU,
RSS, server payload ledger, client accounting, actual allocated disk blocks, pause
latency, and completion hash. The fixture must separately model unrestricted and
per-connection throttling. This local matrix consumes no WAN budget. Native Swift
may be compared only using a disposable independent harness; an unsafe or
non-comparable comparison must be explicitly excluded rather than silently omitted.

Approved future acceptance criteria include:

- Use at least 30-second steady measurement windows after 10 seconds of settling;
  server-ledger payload accounting is authoritative and retransmitted bytes are
  separate.
- The certified steady cap is at most 105% of the configured cap. Short-burst
  magnitude and stop time must be reported separately, never hidden by an average.
- Exercise global/per-job caps, job joins/leaves, unlimited mode, stall/retry, and
  cap reduction.
- Target graceful fixture pause within 5 seconds; if force cleanup is required,
  contain it within 10 seconds and do not report success before containment.
- Track logical bytes separately from physical allocated blocks, including merge
  peak and cleanup behavior; do not infer physical space from logical file length.
- A fresh failure requires a new canonical evidence run after the fix. Preserve the
  failed artifact as diagnostic evidence and never drop it to make a report clean.

The local fixture does not substitute for an authorized live-source test. Actual
YouTube output/hash/`ffprobe` acceptance remains deferred and is required for A11.
