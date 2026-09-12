# Hermes Download Manager Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Deliver a persistent local download service controlled exclusively through Hermes, covering direct files and supported video pages, with explicit-start queue semantics and predictable Downloads output.

**Architecture:** A small Python controller owns durable intent, scheduling, engine process groups, SQLite state, and events. A stdio MCP adapter and a notification-only Desktop plugin are thin clients of the same worker over a private Unix socket. Reuse aria2, yt-dlp, FFmpeg, and the official Hermes plugin SDK; do not write another downloader or a separate application UI.

**Tech Stack:** Python 3.12; stdlib asyncio/sqlite3/pathlib; official MCP SDK; pinned yt-dlp + local EJS; aria2 1.37.0 candidate; FFmpeg/ffprobe; Deno; macOS launchd; plain ESM Desktop plugin; pytest and Node's test runner.

**Canonical root:** `/Users/mohamadsmt/Documents/Download Manager`

**Approved design:** `Docs/superpowers/specs/2026-09-12-hermes-download-manager-design.md` (initial design commit `93cd18b`; subsequent user approval is recorded in that file).

**Date:** 2026-09-12 / 1405-06-21.

**Status:** Ready to execute after the user resolved G0 by selecting trusted, user-vetted links. No additional egress proxy/guard is required. The service and benchmarks are NOT implemented; all execution acceptance gates remain.

**Additional user instructions:** Clear the current legacy download queue while preserving payload files; if the existing application is changed, build the latest version and install it in Applications; commit and push completed repository changes. Queue maintenance is a separate local operation; raw state, personal queue inventory and backups must stay outside Git. No application code or installed bundle is changed by a documentation-only update.

---

## 1. Verified discovery and implementation decisions

### D1 — Selective reuse, not a Swift/Python bridge

Choose a small Python service under `headless/` in the existing repository. Preserve the Swift application and its stores unchanged.

Why this is less code than extracting the old application:

- `Package.swift` exports `DownloadManagerCore`, but actual lifecycle/pause/queue dispatch lives in AppKit/Combine `DownloadController`.
- `JSONDownloadStore` is atomic file replacement, not a transaction across queue intent, command idempotency, workers, and event delivery.
- `DownloadQueue.nextEligibleID` sorts priority/createdAt, so its `move` array order does not determine subsequent dispatch. Do not port this hidden priority/reorder mismatch.
- `FilenameResolver.sanitize` replaces separators but is not path containment: e.g. dot names and filesystem aliases need real validation. Port only the intended naming/category behavior, not a trust-boundary guarantee.
- Keeping the Swift state model would still need new persistent gates, worker identity, retry budgets, outbox, video control, process cancellation, and MCP packaging. A bridge would add another runtime interface without reusing the hard parts.
- Reuse the existing smoke test scenarios as characterization inputs: filename precedence, range fallback, coverage, FIFO in equal priority, monotonic progress. Add corrected tests for reorder/manual pause/start behavior. Keep original Swift tests as regression evidence.

Proven local packaging pattern: `/Users/mohamadsmt/Documents/google-docs-mcp/scripts/run-mcp` unsets `PYTHONPATH` and `PYTHONHOME` and executes a canonical non-editable venv entry point. Reuse the pattern; never import that project's private modules.

### D2 — Global speed budget by whole-transfer allocations

Use finite integer shares `b_i` so each running job has `b_i <= item_cap_i` and `sum(b_i) <= global_cap`. Equal-share capped allocation is enough; no adaptive bandwidth optimizer.

- Direct: one private aria2 daemon, per-GID `max-download-limit`, plus `max-overall-download-limit` equal to the total direct allocation.
- Video: yt-dlp uses aria2 for certified payload paths. The installed `Aria2cFD._make_cmd` maps `ratelimit` to aria2 `--max-overall-download-limit`; appended `-j/-x/-s` override its hardcoded 16-connection defaults. This mapping was independently exercised by the parent **without launching a child or network request**, and passed.
- A video job may invoke multiple payload contexts. Budget each simultaneous child, or serialize audio/video payload phases; never give several simultaneous children the entire job budget.
- Zero allocated budget means WAIT, not `0` passed to aria2 (where zero means unlimited).
- Reconfiguration closes admission, reduces/quiesces old transfers, confirms process-group termination when necessary, applies reductions before increases, then reopens admission. No old-cap process may survive the acknowledgement.
- yt-dlp can fall back to native or networked FFmpeg despite `--downloader aria2c`. Certified dispatch must reject unbudgeted paths before media payload transfer. VOD HTTP(S), tested DASH and tested HLS paths are included incrementally; unsupported live/byte-range/DRM cases return a bounded unsupported reason rather than disable a limit.
- Native fragment throttling is not a job-global cap. Do not implement `rate/F` and assume it covers multiple audio/video pools.
- Proposed acceptance semantics: media/file payload bytes, not total machine traffic or instantaneous wire rate; each 30-second steady measurement window after 10-second settling must be <=105% of cap. Measure short-window bursts separately. These are test thresholds, not already-proven engine guarantees. If stock engines fail, stop and revise rather than waive A08.

### D3 — Official notification-only plugin

Use the official Desktop SDK, no pane/sidebar/page/chat injection:

`worker outbox -> plugin_api.py -> ctx.socket/ctx.rest -> host.notify`

The backend only reads events and acknowledges presentation attempts over private worker IPC. The renderer subscribes at plugin registration, not in a session component. Use the SDK's shared query client for fallback reconciliation, not a hot polling loop. A bounded query every 15 seconds while enabled is sufficient; live socket wakes the same query.

- Hermes running, chat closed: app-level event notification.
- Entire Desktop exited: preserve events and show a coalesced summary on relaunch. Immediate app-exited notifications need a separately approved channel; Telegram is not silently enabled.
- Toasts are ephemeral, not a durable inbox or proof of being read. The worker's event history remains queryable through Hermes. Track `presented` separately from user acknowledgement/resolution.
- Notifications never steal focus or automatically open another session. Optional action reveals the completed file using supported `ctx.os.revealPath` after worker path validation.
- Collapse a burst of completed items into one collection event so the host's four-toast capacity does not silently drop important information.
- Backend enablement (`plugins.enabled`) and renderer enablement are separate. Default-profile installation must preserve unrelated config and plugin toggles.

Parent checked live official SDK documentation and local source: `host.notify`, `ctx.os.revealPath`, namespaced plugin REST/socket, allowlist gate, and ephemeral toast behavior are present. **Actual plugin loading and event delivery remain A13/A14 acceptance tests.**

### D4 — Version readiness is not acceptance

Parent verified aria2 1.37.0 earlier, installed yt-dlp `2026.3.17`, EJS `0.8.0`, and existence of ffmpeg/ffprobe/Deno. Child reports additional version inventory, but capture all versions/digests again in the actual acceptance environment.

Candidate Python dependencies: `mcp[cli]==1.29.1` (proven local MCP packaging precedent), `yt-dlp[default]==2026.3.17`, with exact transitive hashes from `uv.lock`. EJS must resolve to 0.8.0 for that selected release. These are reproducibility candidates, not a claim the older installed yt-dlp still works against today's YouTube. Select a newer exact release only if a live test demonstrates need; record the change and rerun all relevant acceptance from fresh output.

Use explicit executable paths, `--ignore-config`, `--no-plugin-dirs`, `--no-update`, `--no-remote-components`, local EJS, `--no-js-runtimes --js-runtimes deno:<path>`, and explicit FFmpeg location. Disable implicit cookies, netrc, environment proxies and arbitrary extra CLI arguments. Never self-update a Homebrew installation behind the user's back.

## 2. G0 — Resolved: trusted user-provided links, no extra network guard

The user explicitly stated that they vet submitted links and do not require special protection against malicious links. This supersedes the previous all-hop private-network rejection requirement. The decision is recorded in `Docs/plans/download-network-decision.md` and the approved spec has been amended consistently.

Use stock engine networking with initial bounded HTTP/HTTPS URL validation. Reject malformed URLs, control characters, embedded credentials and unsupported schemes; preserve valid signed URL bytes. Basic initial-host screening may reject literal loopback/private targets by default, with explicit local-fixture grants in tests, but it must not be presented as a guarantee about later DNS resolution, redirects or extractor-discovered destinations. Do not build/install an egress proxy, add root/VPN/system-routing changes, or block implementation on full SSRF protection.

Residual risk is explicit: a trusted source can be compromised or redirect unexpectedly; subprocess engines can resolve/connect independently. The application does not guarantee prevention of private/internal connections across that chain. Normal TLS certificate verification stays enabled. Essential filesystem containment, no-clobber publication, subprocess argument validation, secret redaction, cookie consent and no automatic execution remain mandatory.

D2 remains a smoothed **payload** speed limit with measured tolerance, not a machine-wide instantaneous network shaper. Report actual measured behavior and short-window bursts; no extra approval gate is needed for the already disclosed ordinary download-manager semantics.

## 3. Files, contracts, and limits

All paths below are relative to the canonical repository unless absolute.

```text
headless/
  pyproject.toml, uv.lock, .python-version
  src/hermes_downloads/
    __init__.py, cli.py, models.py, store.py, queue.py
    paths.py, processes.py, direct.py, video.py
    bandwidth.py, retry.py, worker.py, ipc.py, mcp_server.py
  tests/
    conftest.py
    unit/test_models.py, test_store.py, test_queue.py, test_paths.py
    unit/test_bandwidth.py, test_retry.py, test_video_policy.py
    integration/test_processes.py, test_direct.py, test_video.py
    integration/test_recovery.py, test_ipc.py, test_mcp.py
    integration/test_network_policy.py, test_space.py, test_events.py
    acceptance/test_live.py
    fixtures/http_origin.py, process_fixture.py
  scripts/run-tests, run-mcp, run-worker, verify-install.py
  scripts/benchmark.py, live-acceptance.py, install.py
integrations/hermes-downloads/
  dashboard/manifest.json, plugin_api.py
  desktop/plugin.js, plugin.test.mjs
Docs/plans/download-network-decision.md
Docs/download-manager-operations.md
Docs/benchmark-download-manager.md
```

Do not create empty production modules just to match the tree. Create each when its test first needs it. Keep adapters thin and avoid a plugin registry/factory abstraction for two fixed engines.

Runtime ownership:

- Default profile state: `~/Library/Application Support/HermesDownloadManager/default/`, owner-only, SQLite WAL plus private Unix socket, lock and redacted log. Other profiles are untouched.
- Downloads: `~/Downloads/Hermes/`; incomplete bytes: `.incomplete/<job_id>/`; no final output in state/cache/project directories.
- LaunchAgent: `~/Library/LaunchAgents/com.mohamadsmt.hermes-downloads.default.plist`, installed only after acceptance prerequisites and config backup.
- The socket filename must fit macOS Unix-socket length constraints; reject too-long configured roots rather than silently choose a different directory.
- MCP clients never open SQLite. Only the worker writes queue state. The plugin backend uses the same IPC for outbox operations.
- Worker startup obtains its exclusive lock, reconciles in-scope child identities, closes admission, then listens. Reconnect is distinct from worker restart. If old child identity cannot be safely established, fail blocked; do not kill by substring.

Model contract:

- `job_id`, `collection_id`, immutable original source, source kind, chosen formats, stable content identity, destination intent/final path, priority/order key, schedule, retry budget, generation, effective engine state, manual hold and authorization.
- Separate queue gate (`paused`, `pausing`, `running`), collection hold and per-item manual hold. Eligibility is conjunction, not a single overloaded status.
- Public states: `queued`, `resolving`, `downloading`, `pausing`, `paused`, `retry_wait`, `needs_link`, `needs_auth`, `blocked`, `finalizing`, `completed`, `cancelled`, `removed`, `failed`.
- Changes use request ID plus payload digest and optional expected revision. Same request/digest is idempotent; same request/different digest conflicts. Stale engine callbacks with a different generation cannot complete or restart a job.
- `remove` is a tombstone retaining data ownership/history. `purge` validates explicit selected paths/job IDs and confirmation, and only deletes that job's artifacts.
- Batch validation returns per-entry outcomes, no silent drops. Max 500 input links per call, 100 rows per list page, bounded metadata per item, and explicit pagination totals. These are tool batching limits, not a maximum durable queue size.
- Secrets are persisted only in private state when needed for resume; public listings redact URL query/userinfo. Original URL bytes are not normalized for signature/dedup.

Minimum callable surface (official typed schemas, no raw RPC/shell):

1. `downloads_add(items, collection?, start=false, request_id)`
2. `downloads_query(scope, ids?, cursor?, limit=100)` — status, list, detail, events, health.
3. `downloads_control(action, scope, ids?, request_id, expected_revision?)` — pause/resume/start-now/remove/retry.
4. `downloads_edit(ids, patch, request_id, expected_revision?)` — priority/order/destination/quality/schedule; active changes quiesce first.
5. `downloads_replace_source(id, url, request_id, expected_revision?)` — never unsafe append.
6. `downloads_configure(global_limit_bps?, concurrency?, connections?, request_id)`
7. `downloads_files(action, ids, confirmation?)` — reveal or explicit purge, never execution.

Names are the plan contract, not a claim these tools exist now. Mutation replies contain command status (`applied`/`pending`/`blocked`), revision and target readback. Long transitions return an operation ID and are followed to a real terminal result before Hermes says done.

## 4. Test and command convention

Use a canonical Python 3.12 environment, independent of ambient Hermes Python 3.14/PYTHONPATH. Each task's steps are small actions; integration groups can take longer than a microtask. Do not promise that an entire integration group takes 2–5 minutes.

Every code task below uses this sequence:

1. Add the listed failing test case(s).
2. Run the listed exact test command: expected **FAIL for the specified missing behavior**, not an unrelated import/dependency error.
3. Implement only the listed contract.
4. Rerun the command: expected **PASS**. Run neighboring regression tests.
5. `git add -- <explicit changed files>` and the task's stated commit message. Never `git add .` over user changes.

Commands are run from repository root; `headless/scripts/run-tests` will resolve its own directory, clear PYTHONPATH/PYTHONHOME, use temporary HOME/HERMES_HOME/state/output roots, disable live network by default, and invoke its pinned installed pytest. Do not claim the following commands executed during planning.

Canonical setup during execution:

```bash
uv sync --project headless --python 3.12 --no-editable
headless/scripts/run-tests
node --test integrations/hermes-downloads/desktop/plugin.test.mjs
swift run DownloadManagerCoreSmokeTests
```

The first lock generation is deliberate; subsequent acceptance uses `--locked --no-editable`. Runtime never uses a floating `uvx` install on connection.

## 5. Ordered implementation tasks

### T01 — Record G0 decision and freeze acceptance boundaries (completed in documentation)

**Files:** `Docs/plans/download-network-decision.md`, this plan, approved spec only for an explicitly approved amendment.

1. Record the user-vetted-link decision and explicit residual DNS/redirect risk.
2. Amend spec section 8, this G0 section and T07 so they do not contradict one another.
3. Preserve non-network protections and ordinary measured payload-rate semantics.
4. Check that no active task still demands a proxy or full SSRF acceptance.
5. Commit/push the documentation amendment; continue with T02 without re-asking the same security choice.

**Gate outcome:** G0 is resolved. No proxy research/installation or full-chain private-destination denial is required. Live tests still require their explicit source/traffic scope.

### T02 — Bootstrap isolated package and harness

**Create:** `headless/pyproject.toml`, `.python-version`, `uv.lock`, `src/hermes_downloads/__init__.py`, `tests/conftest.py`, `tests/unit/test_models.py`, `scripts/run-tests`, `scripts/run-mcp`, `scripts/run-worker`.

**Test:** `headless/scripts/run-tests tests/unit/test_models.py`.

- First packaging test must import the installed wheel from a scratch cwd and verify no source-tree import or ambient PYTHONPATH leakage.
- Python 3.12, dependency bounds above, explicit console entry points `hermes-downloads`, `hermes-downloads-mcp`, `hermes-downloads-worker`.
- Test harness creates temporary private roots and forbids real HOME/default profile writes. No runtime download starts on import.
- Launcher follows the already-read google-docs-mcp pattern, pointing to `headless/.venv`.
- Commit: `build: add isolated headless download service package`.

### T03 — Model intent independently from observation

**Create/modify:** `models.py`, `tests/unit/test_models.py`.

**Test:** `headless/scripts/run-tests tests/unit/test_models.py`.

Test candidate eligibility with every gate closed individually; scheduled time alone never authorizes transfer. Define finite enums, immutable validated command snapshots and URL source bytes; record generation/revision.

Example invariant test (actual public contract to implement):

```python
from hermes_downloads.models import Admission

def test_manual_hold_survives_queue_resume():
    admission = Admission(queue_running=True, collection_held=False,
                          authorized=True, item_held=True, due=True)
    assert admission.allowed is False
```

Commit: `feat: separate download authorization from observed state`.

### T04 — Transactional queue persistence and idempotency

**Create:** `store.py`, `tests/unit/test_store.py`.

**Test:** `headless/scripts/run-tests tests/unit/test_store.py`.

Use sqlite3 transactions for jobs/settings/commands/events, unique request IDs and payload digests, durable generation/revision. Test restart, conflicting request IDs, duplicate delivery, rollback and SQLite-full failure; no partial queue mutation may return success. Worker startup writes paused before scheduling.

Commit: `feat: persist queue commands and events atomically`.

### T05 — Safe output paths and naming

**Create:** `paths.py`, `tests/unit/test_paths.py`.

**Test:** `headless/scripts/run-tests tests/unit/test_paths.py`.

Test exact destination contract, Persian names, collection precedence, reserved `.incomplete`, dot names, traversal, absolute names, collisions and symlinks. A renamed final extension follows selected codec/container; listing cannot promise an unselected MP4. Refuse inaccessible destination without fallback. Use job-owned directories and no-clobber publication within the same filesystem.

Commit: `feat: enforce predictable Downloads paths and no-clobber naming`.

### T06 — Runnable ordering, reorder and pause gates

**Create:** `queue.py`, `tests/unit/test_queue.py`.

**Test:** `headless/scripts/run-tests tests/unit/test_queue.py`.

Priority descending, explicit order key within equal priority, FIFO default. Test manual reorder actually changes dispatch (unlike the old implementation), due schedule, collection hold, explicit start-now intent, remove tombstone and resume-all preserving manual holds. Queue pause closes admission synchronously before asynchronous cancellation.

Commit: `feat: add deterministic queue scheduling and persistent pause gates`.

### T07 — Bounded URL and credential handling for trusted sources

**Files:** `headless/src/hermes_downloads/network.py`, `headless/tests/integration/test_network_policy.py`, `headless/tests/fixtures/http_origin.py`.

**Test:** `headless/scripts/run-tests tests/integration/test_network_policy.py`.

Implement bounded HTTP/HTTPS parsing, unsupported-scheme/control-character/userinfo rejection and exact signed-query preservation. Test literal loopback/private initial-host screening and explicit local-origin fixture grants if that basic screen is enabled. Do not implement DNS pinning, a custom transport/proxy, or claim all-hop SSRF prevention. Standard engine redirects remain inside the explicitly trusted-source model.

Disable ambient netrc/cookies/proxies by default; use credentials only after explicit user consent through a scoped supported mechanism. Never forward a generic Authorization header to arbitrary redirected origins. If a specific engine/auth path cannot uphold credential scope, reject that auth mode rather than disable TLS or leak credentials. Redact signed URL queries and child diagnostics in public output. Tests cover malformed input, redaction, credential opt-in and TLS verification, not an unimplemented private-network guarantee.

Commit: `feat: validate trusted download sources and protect credentials`.

### T08 — Contained engine processes

**Create:** `processes.py`, `tests/integration/test_processes.py`, `tests/fixtures/process_fixture.py`.

**Test:** `headless/scripts/run-tests tests/integration/test_processes.py`.

Fixed argv, explicit paths, filtered env, shell disabled, new process group, bounded concurrent stdout/stderr readers and bounded per-operation deadlines. Streaming progress is incremental bounded storage, not unbounded capture. Graceful stop then TERM/KILL fallback; leader exit does not imply descendants gone. Test cancelled startup, flood both streams, failed decoder, detached descendant, normal exit with descendant, and exact tracked process cleanup. Persist enough engine identity to reconcile crash recovery without PID substring matching.

Commit: `feat: contain engine lifecycles and redact bounded diagnostics`.

### T09 — Minimal real HTTP fixture

**Create/modify:** `tests/fixtures/http_origin.py`, `tests/integration/test_direct.py`.

**Test:** `headless/scripts/run-tests tests/integration/test_direct.py -k fixture`.

Standard-library local origin with deterministic generated bytes, known hash, request/byte/connection ledger, Range/no-Range, ETag change at same size, delayed chunks, disconnect, 403, 404, 429 Retry-After and 503 sequences. Explicit test-only local-origin grant if basic initial-host screening is enabled; no privileged network policy changes. Fixture is labelled synthetic, not benchmark evidence from the internet.

Commit: `test: add deterministic download fault origin`.

### T10 — Direct aria2 adapter, no spontaneous queueing

**Create:** `direct.py`; extend `test_direct.py`.

**Test:** `headless/scripts/run-tests tests/integration/test_direct.py`.

Create a worker-owned authenticated loopback aria2 daemon with `--no-conf`, `--no-netrc`, `--file-allocation=none`, RPC local-only, explicit concurrency and no automatic session resurrection. Read secrets from private config, not model-visible argv. Controller admits only authorized jobs; aria2 must not independently start queued intent. Incomplete outputs stay job-owned.

Prove add-paused causes zero body bytes, initial pause persists, Range fallback and segmented successful hash, GID mapping, stale callback rejection and safe restart. RPC request success is followed by state readback. Do not count size alone as byte-identical validation where expected hash is supplied.

Commit: `feat: integrate aria2 direct transfers under queue control`.

### T11 — Bounded retry and replacement semantics

**Create:** `retry.py`, `tests/unit/test_retry.py`; extend `test_direct.py`.

**Test:** `headless/scripts/run-tests tests/unit/test_retry.py tests/integration/test_direct.py`.

One outer job attempt budget with bounded engine retries recorded, exponential backoff with jitter and Retry-After, offline wait distinct from host errors. Initial candidate policy: at most five ordinary failed attempts, base 5 seconds, cap 5 minutes; authentication/link-needed stops ordinary retries immediately. Resume explicit after exhaustion creates a new audited budget, not an implicit infinite loop.

Test 403 ambiguity, transient 5xx, disk-full not network-retried, pause during retry, and stale generation callbacks. New direct URL requires compatible strong validator or trusted digest; unknown identity preserves old partial and returns needs-decision. Never append merely by name/size. Hashless completion is labelled transport-verified, not checksum-verified.

Commit: `feat: add bounded retries and safe source replacement`.

### T12 — Video metadata policy before body download

**Create:** `video.py`, `tests/unit/test_video_policy.py`, `tests/integration/test_video.py`.

**Test:** `headless/scripts/run-tests tests/unit/test_video_policy.py` then `headless/scripts/run-tests tests/integration/test_video.py -k metadata`.

Use pinned yt-dlp APIs/CLI without custom extractors. Metadata response bounded, no autoplay/payload download, original page retained, cookies opt-in. Format default equivalent to `bv*[height<=1080]+ba/b[height<=1080]`, verified for actual available formats; if none fit, report unavailable. Mixed audio/video IDs and extension must be explicit.

No playlist expansion by ambiguous video URL; pure playlist bounded selection required. Distinguish unsupported, DRM, auth-needed and transient failure. Final filename is established before body transfer; provisional path has an explicit marker.

Commit: `feat: resolve video pages and quality without starting payloads`.

### T13 — Certified video payload and local merge

**Modify:** `video.py`; extend `tests/integration/test_video.py`, `test_processes.py`.

**Test:** `headless/scripts/run-tests tests/integration/test_video.py tests/integration/test_processes.py`.

Use aria2 external payload path with explicit concurrency and allocation. Test selected downloader class and real protocol fixture behavior, including native/FFmpeg fallback rejection before body bytes. Serialize audio/video payload phases or subdivide the job budget across every simultaneously spawned context.

External progress hooks are incomplete in the inspected release; report measured job bytes/phase from bounded file/engine observations. Do not use logical sparse file length as downloaded bytes. Mark speed/ETA unavailable when not reliably measurable, never invent zero/precise ETA. No private upstream `if False` RPC patch without an explicit redesign.

FFmpeg only opens verified local input paths for merging; forbid network protocols during local processing. Validate output with ffprobe and known metadata/stream presence; do not transcode by default. Strict fragment behavior cannot silently skip missing fragments and still complete. Test stop/resume before/during merge and incompatible refresh. Audio-only and available subtitles follow the same output ownership rules.

Commit: `feat: download supported videos with budgeted engines and verified merge`.

### T14 — Whole-queue bandwidth allocation and transitions

**Create:** `bandwidth.py`, `tests/unit/test_bandwidth.py`; extend direct/video integration tests.

**Test:** `headless/scripts/run-tests tests/unit/test_bandwidth.py tests/integration/test_direct.py tests/integration/test_video.py`.

Implement capped equal-share allocation; priority affects admission, not implicit bandwidth stealing. Example test contract:

```python
from hermes_downloads.bandwidth import allocate

def test_shares_never_exceed_job_or_global_caps():
    shares = allocate(global_limit_bps=300, item_caps_bps=[50, None, 500])
    assert shares == [50, 125, 125]
    assert sum(shares) <= 300

def test_zero_share_is_not_unlimited():
    shares = allocate(global_limit_bps=1, item_caps_bps=[None, None])
    assert shares == [1, 0]
```

Check shares across job arrivals/departures, tiny caps, unlimited distinction, active engine contexts, child failure and pending reconfiguration. Apply direct RPC reductions before increases; video quiesces process group before new-cap restart. A failed stop blocks reallocation acknowledgement.

Commit: `feat: enforce aggregate payload budgets across engines`.

### T15 — Disk capacity, final publication and ownership cleanup

**Modify:** `paths.py`, `worker.py` as introduced by task; create `tests/integration/test_space.py`.

**Test:** `headless/scripts/run-tests tests/integration/test_space.py tests/unit/test_paths.py`.

Track actual allocated bytes (`st_blocks` where supported), logical size separately, expected merge peak and free space. Unknown size is unknown, not zero reservation. Measure during writes; catch ENOSPC, retain partial, no deleting other files. Use fsync/close/verify and no-overwrite publication; on crash, reconcile publish-vs-DB state idempotently. Clear only job-owned intermediates after successful verified publication, preserving subtitles/requested sidecars. No auto-purge history/failed partials in v1.

Commit: `feat: finalize downloads safely and account for real disk use`.

### T16 — Worker loop and crash recovery

**Create/modify:** `worker.py`, `tests/integration/test_recovery.py`.

**Test:** `headless/scripts/run-tests tests/integration/test_recovery.py`.

One writer/worker, admission generation, pause-before-schedule on cold start, in-scope orphan containment, atomic transition/event publication. Recovery maps incomplete jobs to paused, preserves per-item holds and paths, and does not spawn old aria2 sessions. Test killing worker while direct/video/merge/retry is active, two simultaneous clients, duplicate worker start and Desktop/MCP exit while worker remains alive. Use event handshakes, not arbitrary sleeps.

Commit: `feat: run a durable single-owner download worker`.

### T17 — Private bounded IPC and typed CLI

**Create:** `ipc.py`, `cli.py`, `tests/integration/test_ipc.py`.

**Test:** `headless/scripts/run-tests tests/integration/test_ipc.py`.

Bounded NDJSON over owner-only AF_UNIX socket, request IDs, fixed command table and schema validation; no arbitrary shell/path/RPC methods. Malformed/oversized messages fail without mutation. CLI has status/diagnostic controls for acceptance, but user interface remains Hermes. Bootstrap command starts worker paused only; read/list can never launch payloads.

Commit: `feat: expose bounded local queue control protocol`.

### T18 — MCP adapter and tool contracts

**Create:** `mcp_server.py`, `tests/integration/test_mcp.py`; complete `scripts/run-mcp`.

**Test:** `headless/scripts/run-tests tests/integration/test_mcp.py`.

Official SDK stdio, sampling disabled, exact tools listed above, page-size caps, target disambiguation, redaction and post-mutation readback. Exercise fresh SDK initialize/list_tools/call_tool round trip over actual stdio and actual worker IPC. Inspect tool totals programmatically, do not claim discovery equals successful mutation. Live download processes cannot be owned by MCP lifespan.

Commit: `feat: expose download management through Hermes MCP`.

### T19 — Durable event claims and acknowledgement

**Modify:** `store.py`, `worker.py`, `ipc.py`; create `tests/integration/test_events.py`.

**Test:** `headless/scripts/run-tests tests/integration/test_events.py`.

Outbox rows keyed by job generation/event type or collection terminal generation. Coalesce completion burst; prioritize needs-action. Claim lease avoids multiple windows presenting the same event simultaneously. Presentation ack != user read/resolution. If client dies before ack, lease expires and event may be replayed with stable ID; never promise exactly-once visual delivery across crashes. Test transport failure, presentation failure, profile mismatch and pause/retry noise suppression.

Commit: `feat: persist actionable download events for Hermes delivery`.

### T20 — Notification-only Desktop plugin

**Create:** `integrations/hermes-downloads/dashboard/{manifest.json,plugin_api.py}`, `integrations/hermes-downloads/desktop/{plugin.js,plugin.test.mjs}`.

**Test:** `node --test integrations/hermes-downloads/desktop/plugin.test.mjs` and `headless/scripts/run-tests tests/integration/test_events.py`.

Backend relays only event/read/ack capability via worker socket, not worker lifecycle/download commands or secrets. Renderer uses `@hermes/plugin-sdk` imports only, app-level socket registration and shared-query fallback; no panes/routes/sidebar. Claim, notify, then ack presentation; retain underlying history. Use stable identifiers and coalesced summary for app-closed backlog. Disable toggles are respected. No implicit native OS/Telegram notification.

Test registration/disposal, replay, dedup, ack-loss, failed notify, wrong profile and no UI contribution. Actual `host.notify` receipt and toast visibility require the later live Desktop acceptance; mocked tests are not that evidence.

Commit: `feat: deliver download events with a notification-only Hermes plugin`.

### T21 — Install, rollback and default-profile scope

**Create:** `headless/scripts/install.py`, `scripts/verify-install.py`, `Docs/download-manager-operations.md`.

**Test:** `headless/scripts/run-tests tests/integration/test_ipc.py tests/integration/test_mcp.py` plus install-specific tests added under `tests/integration/test_install.py`.

Prepare LaunchAgent and canonical non-editable env; all command lines absolute and no shell. Installer has dry-run by default, explicit apply with exact default-profile target, before/after config backup, no overwrite of unrelated dirty settings or plugin IDs. Avoid symlink-based same-account hostile mutation guarantees beyond agreed owner-only boundary; state the boundary.

Copy small plugin integration files to default-profile managed paths and enable only its backend entry; add `mcp_servers.downloads`, explicit tool allowlist, sampling false, startup/call deadlines. Respect renderer disabled choice. No other profile modified. On rollback stop only tracked service, restore only owned settings/files and preserve all user downloads/queue data. Reload MCP/new-session requirement reported accurately.

Commit: `feat: add scoped installation and reversible service wiring`.

### T22 — Benchmark runner and numerical acceptance

**Create:** `headless/scripts/benchmark.py`, `Docs/benchmark-download-manager.md`.

**Test:** add `tests/unit/test_benchmark.py`, run `headless/scripts/run-tests tests/unit/test_benchmark.py` before real runner use.

Runner creates a fresh no-clobber evidence directory under ignored `.artifacts/download-manager/<run-id>/`, enforces bytes/time budgets, records exact versions/settings/source and fixture hashes, CPU/RSS, server byte ledger, client accounting, actual disk blocks, pause latency and completion hash. Summarizer fails closed on missing/failed runs; never silently drops trials.

Controlled benchmark budget: deterministic local payload 64 MiB, 3 balanced repetitions per curl/aria2-single/aria2-multi configuration; fixture models unrestricted and per-connection throttling separately. Generate real bytes and label synthetic source; no WAN consumption from this matrix. Compare native Swift only with a disposable independent harness, never launch the old app against real state. If unsafe/non-comparable, record exclusion reason.

Mixed speed fixtures: at least 30-second measurement windows with 10-second settling, global/per-job caps, joining/leaving jobs, unlimited, stall/retry and cap-reduction; count payload from server ledger and record retransmitted bytes separately. Certified steady cap <=105%; short burst magnitude/stop time separately reported, not hidden by average. Pause target <=5 seconds graceful under fixture; if force cleanup needed <=10 seconds, and no success before containment. Timing expectations are acceptance criteria, not measured claims.

Report p50/range over complete trials; calculate in code. Fresh failure requires new canonical evidence run after fix, with failed artifacts retained as diagnostic only.

Commit: `test: add reproducible downloader performance and recovery benchmark`.

### T23 — Authorized real-source acceptance

**Create:** `headless/scripts/live-acceptance.py`, `tests/acceptance/test_live.py`.

**Test:** `headless/scripts/run-tests tests/acceptance/test_live.py` must SKIP live by default with explicit reason; this skip is not A11 PASS.

After obtaining an explicit public test URL from the user or an approved small upstream test video with clear public test provenance, run opt-in acceptance with a 256 MiB total external-body budget and 15-minute wall deadline. Record original source privately, public redacted handle, version/format metadata, file hash and ffprobe output with real video/audio streams. Do not auto-download a playlist, use cookies or ask for passwords in chat. Test lower-quality selection and one real quality within 1080p, as available. If cap/source unavailable, report blocked and request a suitable URL, never synthesize the output.

Direct live file likewise requires a bounded approved source and expected digest when available. Local fixture success never substitutes for blocked YouTube access.

Commit: `test: add opt-in live download acceptance`.

### T24 — Canonical install and live Hermes QA

**Files:** acceptance evidence only plus `Docs/download-manager-operations.md`.

1. Integrate reviewed commits into canonical checkout while preserving any user changes.
2. Build a fresh non-editable acceptance environment from the locked dependencies; verify source/package byte identity using `scripts/verify-install.py` and installed module paths from scratch cwd.
3. Run complete Python suite, JS plugin tests and original Swift smoke tests. Record exits, totals and any skipped live coverage explicitly.
4. With explicit installation authorization, apply default-profile integration, read back exact config/service/plugin targets and exercise actual Hermes MCP calls (add-only, list, start, pause, reorder, speed, replace, output path, remove/purge semantics).
5. Verify a real notification when a different chat is focused; close relevant chat, finish work, observe notification; quit/relaunch full Desktop only with user approval if it would interrupt other work. Verify pending event replay. Disable/re-enable notification plugin and ensure user choice respected.
6. Verify file path in Finder from the notification action, no separate UI, and no surprise transfer on cold start. No claim of complete delivery before A01–A14 coverage is reconciled.

If implementation changes any existing Swift application behavior, run `swift build`, `swift run DownloadManagerCoreSmokeTests`, and the repository's `script/build_and_run.sh --bundle-only` after reviewing that script. Inspect the built bundle ID and executable version; safely quit the exact running old app, stage/validate the new bundle and replace `/Applications/Download Manager.app` with a recoverable previous-bundle backup. Verify installed/source build identity and launch the installed artifact against the explicitly cleared queue; verify no spontaneous transfer and preserve completed history. Do not substitute a project-local build for the requested Applications installation. If only the headless service/docs changed, report the desktop bundle as unchanged, not reinstalled.

Commit documentation/evidence summaries: `docs: record canonical Hermes download manager acceptance`.

### T25 — Independent review, fixes, commit and handoff

**Files:** `Docs/download-manager-operations.md`, `Docs/benchmark-download-manager.md`, `.hermes/handoffs/<dated-handoff>.md` if rollover needed.

- Independent spec reviewer maps exact commit to A01–A14; require explicit PASS before quality/security review.
- Independent quality/security reviewer examines same commit, real failure/recovery evidence and G0 policy; require APPROVED. Any fix invalidates affected review/acceptance and is re-reviewed on new commit.
- Parent independently reads exact evidence and reruns canonical acceptance as appropriate. Do not use child self-report alone.
- Commit all finished code/docs (exclude runtime credentials/raw signed URLs, binaries and private acceptance input). `git diff --check`, `git status --short`, `git log -1`, and exact changed-file list must be clean/within scope.
- Push the exact committed branch to the discovered origin after checking visibility and staged content. Never force-push or upload runtime queues, credentials, backup state, private signed URLs or raw personal test inputs. Verify remote branch SHA using `git ls-remote`; local commit success alone is not push success.
- Deliver short Persian usage guide, paths, measured benchmark results, plugin/worker health, installed app status when applicable, remote commit handle, and complete/blocked coverage. Do not replay all task steps.
- Preserve reusable download-control operating rules in a task-scoped skill only after live verification; record pitfalls about start gates, allocated-vs-logical disk, engine fallback rate limits and official notifications. No transient measurements as durable facts.

## 6. Coverage map

| Acceptance | Implementation tasks | Final evidence |
|---|---|---|
| A01 | T03,T04,T06,T10,T16,T18,T24 | body request ledger + cold-start/MCP read/add-only tests |
| A02 | T06,T08,T16,T24 | process and server ledger after pause ack |
| A03 | T03,T06,T11,T16 | hold/retry/new-item invariant tests |
| A04 | T04,T06,T17,T18 | actual order + request id/revision reconciliation |
| A05 | T08,T10,T16,T24 | crash/MCP-exit/worker survival evidence |
| A06 | T07,T09,T10,T11 | real origin fault matrix |
| A07 | T11,T12,T13 | direct/media identity replacement matrix |
| A08 | T10,T13,T14,T22 | mixed-engine server-observed numerical windows |
| A09 | T05,T13,T15,T22 | physical blocks and merge peak/cleanup |
| A10 | T05,T07,T15,T24 | safe path/collision/permission tests + Finder |
| A11 | T10,T13,T22,T23 | expected hash + actual YouTube output/ffprobe |
| A12 | T04,T12,T13,T23 | duplicate/quality/audio/subtitle/playlist tests |
| A13 | T19,T20,T24 | actual toast plus failure/dedup/replay receipts |
| A14 | T17,T18,T21,T24 | fresh SDK calls and actual Hermes operation |

## 7. Planning verification and remaining decisions

Completed planning evidence: approved spec read, prior Swift controllers/models/store/naming/tests inspected, existing local MCP launcher precedent read, official notification SDK/source verified, installed yt-dlp command builder tested without network. The user resolved G0 in favor of trusted sources and no extra guard. Neither the service, plugin nor benchmark harness has been created or run.

No remaining G0 choice blocks implementation. Continue at T02 with fresh execution context. Do not re-ask about malicious links or add a proxy. The actual live-source/credential and disruptive app-restart gates still apply when reached; Python, schema, file names and task order are implementation decisions.

## Sources and exact discovery references

- Approved source: `Docs/superpowers/specs/2026-09-12-hermes-download-manager-design.md`.
- Previous implementation: `Sources/DownloadManagerApp/App/DownloadController.swift`; `Sources/DownloadManagerCore/{Models/DownloadQueue.swift,Stores/JSONDownloadStore.swift,Services/FilenameResolver.swift,Services/Aria2DownloadEngine.swift}`.
- Official Desktop SDK: https://hermes-agent.nousresearch.com/docs/developer-guide/desktop-plugin-sdk ; local `~/.hermes/hermes-agent/website/docs/developer-guide/desktop-plugin-sdk.md:496–521,760–825`.
- Host toast limits/lifetime: `~/.hermes/hermes-agent/apps/desktop/src/store/notifications.ts:48–59,162–194`.
- yt-dlp pinned adapter: https://github.com/yt-dlp/yt-dlp/blob/2026.03.17/yt_dlp/downloader/external.py#L269-L349 ; parent independently read installed equivalent and asserted `_make_cmd` output.
- Fallback dispatch: https://github.com/yt-dlp/yt-dlp/blob/2026.03.17/yt_dlp/downloader/__init__.py#L87-L126 ; installed equivalent read by parent.
- Native rate behavior: https://github.com/yt-dlp/yt-dlp/blob/2026.03.17/yt_dlp/downloader/common.py#L201-L215 ; installed equivalent read by parent.
- Resolver boundary: https://github.com/yt-dlp/yt-dlp/blob/2026.03.17/yt_dlp/networking/_helper.py#L235-L260 ; installed equivalent read by parent; no private-IP policy in that connection helper.
- aria2 options/RPC: https://aria2.github.io/manual/en/html/aria2c.html ; local help confirms default preallocation and aggregate-rate options.
