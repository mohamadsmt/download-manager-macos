# Download Manager benchmark evidence

## Status and scope

T22a's version-1 allocation, strict metadata validation, and pure summary remain
available. Version-1 summaries retain `pending_real_execution`; synthetic unit
records are contract tests, never runtime evidence.

T22b adds an explicit `run` command for **local engine baselines**. It generates a
real deterministic 64 MiB payload and serves only `127.0.0.1` using Python's
standard library. It invokes the existing curl/aria2 engines with argv and no
shell. It accepts neither user URLs nor additional engine arguments. No DNS, WAN,
worker, Swift application, user queue/store, profile, or Hermes settings are used.

A full run requests 18 measured completion trials: three balanced rotations of
`curl-single`, `aria2-single`, and `aria2-multi` under both unrestricted and
per-connection origins. Single configurations use one connection; multi uses 16.
The throttled origin allows 4 MiB/s per connection, including an initial 64 KiB
chunk. This models an origin constraint; it does not certify a worker/global cap.

Native Swift is excluded because its controller depends on AppKit, Combine, and
the real store, and there is no isolated comparable engine harness. The runner
never launches the old application against user state.

## Artifact location and allocation

Evidence must be owner-only and outside Git (or beneath this narrowly ignored root):

```text
.artifacts/download-manager/<run-id>/
```

The reserved run layout is:

```text
<run-id>/
  trials/<trial-id>.json   # immutable diagnostic record per requested trial
  manifest.json            # source hashes, sequence, budgets, setup
  <trial-id>/completion/   # real output, process accounting, origin ledger
  <trial-id>/pause/        # separate containment-probe output/accounting/ledger
  summary.json             # only when the requested evidence is complete
  report.json              # success or failure; never filters failed trials
```

T22a allocates only `<run-id>` and does not fabricate either trial or summary
artifact. The explicit T22b `run` command populates these paths from observed
measurements and retains failed records for diagnosis.

The repository ignores only `.artifacts/download-manager/`; it does not blanket
ignore `.artifacts/`. Callers explicitly supply an absolute private root to
`allocate_run_directory` or to the CLI, for example:

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
than symlinks. The runner writes immutable trial JSON records and its derived
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

The frozen T22a contract below uses `schema_version: 1`. `outcome` is exactly `passed` or `failed`.
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
This public version-1 entry point requires exact integer `schema_version: 1` on
every record; mixed versions and all-version-2 inputs are rejected.
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

## Running the local baseline

First install the isolated, noneditable Python 3.12 package and verify installed
source parity before collecting evidence:

```sh
env -u PYTHONPATH -u PYTHONHOME uv sync --locked --project headless \
  --python 3.12 --no-editable --all-groups --reinstall-package hermes-downloads
headless/scripts/run-tests tests/unit/test_benchmark.py
```

Invoke the explicit interpreter from a fresh scratch working directory, with an
absolute private artifact root and a new run ID. For the complete local matrix:

```sh
env -u PYTHONPATH -u PYTHONHOME /absolute/repo/headless/.venv/bin/python -I \
  /absolute/repo/headless/scripts/benchmark.py run \
  --artifact-root /absolute/private/benchmark-evidence --run-id reviewed-matrix \
  --mode full --wall-seconds 1200 --trial-seconds 60 --byte-budget 2147483648
```

For one unrestricted curl completion and its containment probe, use `--mode smoke
--wall-seconds 120 --trial-seconds 60 --byte-budget 134217728`. A smoke is never a
complete matrix. No transfer occurs with the retained `allocate` command.

Setup has a 30-second monotonic budget within the aggregate run budget. Completion
children have at most 60 seconds each, probes at most 10 seconds, and termination
reserves up to 10 seconds inside the aggregate deadline (half the remaining budget
for short injected fault tests). SIGTERM has at most five seconds of grace, then
SIGKILL and group-containment verification. Forced cleanup fails a pause probe.
The CLI rejects aggregate deadlines over 1800 seconds (120 for smoke), and byte
budgets over 3 GiB (128 MiB for smoke). Each completion origin is additionally
bounded to 128 MiB, and each probe to 4 MiB. Setup, child waits, hashes, transfers,
and the trial sequence all check monotonic budgets. Exhaustion retains failed
records for all requested slots, rather than skipping them for a clean summary.

Unverified engine-group or origin shutdown raises a typed `ContainmentFailure`,
with fixed redacted messages and the nonpassing `containment_error` classification.
Setup, inventory and trial boundaries retain the current failed slot and every
remaining requested slot, write `report.json`, and omit `summary.json`. Admission
stops immediately: no later inventory engine, completion or probe is launched,
including after a verified completion whose separate probe fails containment.
Unrelated `RuntimeError` exceptions are not converted into containment failures.

After the existing SIGKILL, an owned-group zero probe returning EPERM leaves
group absence unknown. The runner continues only read-only owned-leader `wait4`
and zero-probe observations within the original aggregate deadline, sends no
further signals, and requires both positive leader reaping and positive group
absence (ProcessLookupError) before declaring containment. Persistent uncertainty
exhausts that same deadline and raises `ContainmentFailure`; other signal, probe,
and wait errors still fail closed immediately. Known reaped process accounting is
retained, while unobserved accounting remains null.

Private `containment-failure.json` evidence and the report retain only known owned
PID/process-group/session authority and observed shutdown/accounting facts. Missing
authority stays null; group absence is false when observed present and null when
unverifiable. This does not claim successful cleanup or authorize signaling an
unknown system group. A concurrent origin failure preserves prior engine authority.
Already observed process/ledger accounting is retained; unobserved metrics in the
current and unlaunched slots stay null. Mutable output size/hash is unavailable
until engine and origin shutdown are both verified. A previously verified
completion keeps its measured metrics if its probe fails, but the failed trial's
completion hash and pause latency remain null. Unknown engine versions are omitted,
never replaced with placeholder metadata.

New run directories are `0700`; evidence files are exclusively created `0600`.
Engine outputs live in fresh private trial directories; curl disables config and
netrc, aria2 disables config/netrc and explicitly sets `file-allocation=none`,
no overwrite, and no automatic renaming. The minimal child environment omits
ambient proxy/config/credential variables. TLS verification remains enabled;
the synthetic origin itself uses HTTP on numeric loopback, without redirects.
Engine version stdout and executable hashes are retained privately.

## Version-2 measurement contract

Version 2 adds the closed fields `purpose: local_engine_baseline`, `runner_sha256`
(the exact runner file), `source_sha256` (fixture-generator source), and
`pause_probe`. It keeps the original strict metadata redaction and all completion
accounting requirements. Exact settings, versions, deterministic fixture hash,
client size/hash, and actual `st_blocks * 512` allocations are recorded.

CPU seconds and normalized maximum RSS come from `wait4` for the actual engine
leader: macOS RSS is bytes; Linux RSS is KiB converted to bytes. This is engine
process accounting, not whole-run CPU, server CPU, or a sampled machine metric.
Completion elapsed time spans launch through observed child exit/containment;
hash verification and the separate probe are excluded from completion throughput.

The server ledger counts bytes successfully accepted by socket sends, stores the
actual sent length of each requested range, and computes their union.
`retransmitted_bytes` means repeated HTTP payload ranges, not inferred TCP
retransmission. Headers and HEAD requests are excluded. Completion requires full
range coverage and the verified 64 MiB client output; the ledger identity must
hold. Failed partial sends remain diagnostic.

Every passed completion also needs a **separate graceful engine-process
containment probe**. The probe uses the same fixture with a 1 MiB/s per-connection
origin, waits for at least 64 KiB of actual server payload, then measures SIGTERM
to verified process-group disappearance. Its closed fields are:

```text
scope, elapsed_seconds, cpu_seconds, max_rss_bytes,
server_payload_bytes, server_unique_payload_bytes, retransmitted_bytes,
client_logical_bytes, allocated_disk_bytes, latency_seconds, forced, contained
```

The scope is exactly `separate_graceful_engine_containment`. Probe bytes and CPU
have separate summary totals and are included in the aggregate run budget, never
silently folded into completion throughput. `client_logical_bytes` is observed
file length: segmented output may be sparse, so it is not treated as received
payload. Probe ledger retransmission is separate. Missing/unobserved failure
metrics remain null; zeros are never fabricated to obtain a passing record.
For an uncertain failed probe, `contained` is false (phase containment was not
verified), `forced` is null if no signal accounting was observed, and the measured
containment latency is null. A passed probe still requires explicit boolean flags
and every strict measurement. Private process evidence preserves any independently
verified engine shutdown when the origin is the uncertain component.

`summarize_baseline_trials` requires exactly the six expected configurations and
three repetitions, validates every version-2 record, and reuses the version-1
fail-closed summarizer only after explicit conversion to version 1, removing the
version-2-only fields without mutating the input. It rejects missing/extra/failed/duplicate records and
mismatched fixture, run, source/runner hash, scope, settings, cross-configuration
version inventory, trial identity, and accounting.
A complete computed version-2 report is `measured_local_baseline`; an actual
single CLI smoke is `measured_local_smoke`. Those labels describe this narrow
local baseline, not project acceptance. Unit-generated records still are not
measured evidence. Failed runs return nonzero, retain their diagnostics, and do
not write a passing `summary.json`.

Fault-injection tests verify these diagnostic contracts without orphaning a real
engine or targeting an unknown process group. They are not live failure acceptance.
The source revision invalidates previous exact smoke evidence; a fresh committed
candidate 64 MiB completion plus separate probe smoke remains a parent verification
step, as do independent specification and quality/security review.

## Deferred worker and live acceptance thresholds

The controlled local matrix uses a deterministic **64 MiB** synthetic
payload and three balanced repetitions for each of:

1. `curl-single`
2. `aria2-single`
3. `aria2-multi`

All configurations use the same fixture and record versions, settings, hash, CPU,
RSS, server payload ledger, client accounting, actual allocated disk blocks, pause
latency, and completion hash. The fixture must separately model unrestricted and
per-connection throttling. This local matrix consumes no WAN budget. Native Swift
is explicitly excluded for the concrete reason above.

Worker cap, recovery, and live acceptance remain separate work. The baseline and
its process-containment probe make no A08, mixed-worker, recovery, full-A09, or A11
delivery claim. Approved future worker acceptance criteria include:

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
authorized direct real-file acceptance with an expected checksum remains required
for A11. YouTube/`ffprobe`, extracted quality/audio/subtitles and playlist acceptance
were retired by the user on 2026-10-03, not passed. A12 retains direct duplicate and
source-identity checks. Local synthetic media bytes do not satisfy real-source A11.
See `.hermes/handoffs/2026-10-03-download-manager-direct-only-scope.md`.
