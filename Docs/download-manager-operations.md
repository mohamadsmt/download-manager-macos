# Hermes direct-download operations

Hermes is the sole control interface for the headless service. Use trusted,
user-vetted direct HTTP(S) sources. Direct media files are ordinary bytes;
legacy literal video rows remain readable, read-only and inert. Extraction,
ffprobe/playability, quality, audio, subtitle and playlist gates are RETIRED,
never PASS. The retained Swift release is a separate historical application.

## Installation, verification and recovery

An existing valid, owner-owned private `~/Downloads/Hermes/` output root is a
prerequisite. There is no missing-root auto-creation or cache fallback promise.
Use an isolated Python 3.12 non-editable environment with ambient `PYTHONPATH`
and `PYTHONHOME` cleared. The installer is
[`headless/scripts/install.py`](../headless/scripts/install.py); its default
mode is a readiness plan. Applying requires explicit default-profile authority
and the exact reviewed commit via `--apply --profile default --expected-commit`.
[`verify-install.py`](../headless/scripts/verify-install.py) checks the exact
commit, private manifest, physical installed parity, owned service and seven
schemas. Keep manifests, backups and runtime state outside this public repository.
These are operator boundaries, not authorization to install or change profiles.

For separately authorized operator work, run the scripts with the physical
isolated interpreter from the canonical checkout:

```sh
env -u PYTHONPATH -u PYTHONHOME PYTHONDONTWRITEBYTECODE=1 headless/.venv/bin/python -I -B headless/scripts/install.py
env -u PYTHONPATH -u PYTHONHOME PYTHONDONTWRITEBYTECODE=1 headless/.venv/bin/python -I -B headless/scripts/verify-install.py
```

`PLAN_READY` is a plan; `INSTALLED_WIRING` is paused local wiring; neither is
live acceptance. `NOT_READY` or nonzero exit blocks progress. Source cleanliness,
ordered independent exact-commit reviews and physical source/site/commit parity
are prerequisites. Installation rejects other profiles, unknown arguments and
foreign ownership; account home comes from the account database. Existing state,
history, files and runtimes are retained. The verifier reads only: it never
repairs, launches or resumes a worker. Installation never resumes downloads.
Only the downloads MCP entry and owned plugin enablement are config-owned;
an explicit backend-disabled choice blocks apply. Renderer enablement uses the
official Desktop toggle/readback, never Chromium storage edits.

Supported rollback uses `--rollback` with the exact private manifest and
`--profile default`. Fresh backup/file/config/service authority checks must pass;
changed or uncertain ownership blocks it. Retain failure evidence; failed
installation is not automatic rollback. Unrelated configuration, runtime, state
and Downloads remain retained. Backend recycling interrupts default Desktop
turns and needs separate authorization; installer/verifier never perform it.

`VERIFIED_LOCAL` does not establish actual Hermes discovery, Desktop paint or
Finder reveal; those require separate observations. Before this documentation update, documentation source
`75e4d45b6d34c1f9b0031e21109bf4b7c221dc62` matched canonical main, remote
and GitHub GET. Exact-source runtime apply/verify, independent 16-module parity
and a fresh actual Hermes query were verified on that deployed source with
unchanged implementation S (defined below). Final-document exact reviews, canonical normal integration,
fast-forward/push and remote readback, exact deployed documentation-commit runtime
rebind/verify and fresh Hermes discovery are separate parent-owned REQUIRED RECEIPTS.
These docs do not self-certify their own postcommit deployment. Do not manually
rewrite a manifest SHA or waive rebind because this change contains prose. No
Swift change or app reinstallation is claimed here.

Recovery of owned stale wiring was `RECOVERED_OWNED_STALE_WIRING`, with separately
guarded owned-service quiescence. It is not supported rollback PASS: the earlier
supported rollback remains BLOCKED and the original failure stage is unknown.
Stop on uncertain ownership; do not signal unrelated services, rewrite runtime
history or retro-certify checkpoints. The failed integration postcondition is
retained; a later no-op fast-forward/push reconciled the already-applied change
and checked the current source/history-metadata baseline. No earlier historical
byte/metadata parity or manual chmod is claimed. A Git-created private checkout
mode change from 0644 to 0600 was observed; executable status remained false.

## Usage through Hermes

The seven tools are `downloads_add`, `downloads_query`, `downloads_control`,
`downloads_edit`, `downloads_replace_source`, `downloads_configure` and
`downloads_files`. Add, query, exact replay and cold start grant no BODY authority.
Selected start authorizes a job while preserving its paused state. A cold-paused
individual job can need explicit supported job resume under its existing authority,
followed separately by global queue resume; global resume alone does not guarantee
that such a job starts. Use fresh worker epoch, revisions and stable request IDs. Poll pending control receipts to durable outcomes;
manual, collection and retry holds are independent. Queue/collection start
captures transactional cohorts of at most 500, not a queue-size limit. Query
queue/status/detail/control for gate, job revision, effective settings and
request outcome. Edits support priority/order, explicitly zoned schedules,
destination and per-item cap; schedules persist in UTC and read back in
Asia/Tehran. Active edits/replacements quiesce first, and completed files are
not moved. Stock max-tries is one; the host ordinary-attempt budget is five.
Retry-After is a floor, exhaustion survives restart, and a fresh explicit retry
opens a new audited budget. HTTP 403 alone proves neither expiry nor login.
Caps are bytes per second; desired settings and applied readback are separate.
Null removes a cap; zero
means WAIT. The normal baseline is one job, four stock connections and null cap.

- «این لینک‌ها را فقط به صف اضافه کن.» سپس وضعیت و شناسه‌ها را بررسی کنید.
- «دانلود انتخاب‌شده را شروع کن» و در صورت توقف سراسری، جداگانه «صف را ادامه بده».
- «کل صف را متوقف کن»؛ تأیید مهار پردازش را از رسید کنترل بخوانید.
- «سقف سرعت را بردار» یعنی null؛ صفر دانلود را در انتظار نگه می‌دارد.
- «این مورد را حذف کن» فایل‌ها را نگه می‌دارد. پاک‌سازی فایل فقط برای شناسهٔ
  انتخاب‌شده با تأیید صریح `confirmed: true` انجام می‌شود.
- برای لینک تازه از جایگزینی منبع استفاده کنید؛ برای نمایش فایل کامل، عمل
  صریح نمایش در Finder را انتخاب کنید.

Saved partial identity requires a trusted original digest for a different URL,
or qualified same-resource strong-validator/length lineage. Inconclusive identity
preserves partials and offers a separate download. An isolated Python standard
library HEAD probe owns saved identity observation; stock aria2 owns BODY and
qualified If-Match. HEAD is not a stock-aria2 probe.

No overwrite, traversal/symlink escape, arbitrary argv, automatic execution,
credential/TLS bypass, full SSRF proxy or system-wide network change is permitted.
An explicit reveal click freshly certifies file authority; same-account namespace
races remain a limit. A returned path alone does not prove Finder selection.

Durable event IDs and the outbox survive delivery failure and replay. Store/SDK
receipts do not prove visible paint or reading. Existing behavior polls every
15 seconds, shows ordinary success toasts for five seconds, and keeps actionable
and reveal offers persistent. Do not inject events or impersonate the SDK.
There is no exactly-once visual/read guarantee across crashes.

## Evidence identities and limits

Use these full source identities throughout the table:

- Historical H: `01c078ba45ad4fd7d8fd449a689c6a38ef0216ad`.
- Historical C: `398c8c13f49102ea935f612b962d8dd2956f9e5a`.
- Current S: `5db8c883a6384312c1276d2aa2d505512de9c05b`.

H alone has the historical full 2405 PASS plus one explicit live SKIP. S has
105 targeted tests passed in 108.09 seconds (wrapper 108.643752/360 seconds),
with natural zero exit, original wait/reaping, ECHILD and group absence, no
signals, and unchanged 141 source/16 installed-site files. This is not a fresh
full suite. Earlier failed/incomplete executions and intermediate quality
non-approval remain historical failures, not passing evidence.

The C-to-S aggregate received ordered SPEC PASS followed by distinct
QUALITY/SECURITY APPROVED on exact S. The fix handles obsolete per-file allocation
RPC failure only after fresh exact completed-stock readback and durable owner
qualification. Byte/hash/publication/no-overwrite checks still govern; active
or global faults are not broadly hidden. Synthetic reproduction supports this
mechanism but cannot recover the unavailable original actual-default traceback.
No review of this documentation commit is asserted by its author.

## Final acceptance table

PASS below is limited to the named source and evidence class. It never inherits
all historical tests onto S. A05 current view-close survival through completion is PASS, source-qualified.
Whole-project delivery remains OPEN for A13 ordinary automatic completion paint
and final UI preference restoration.
Final-document deployment requires the separate parent-owned receipts above.

| Gate | Outcome | Source, evidence class and material limits |
| --- | --- | --- |
| A01 | PASS, scoped | H accepted cold-start/read/add-only fixture ledger; C actual-control evidence. S exact add-only replay succeeded with zero BODY causes under the historical expired authorization; later independently bounded HTTP trials are described below. |
| A02 | PASS, scoped | H accepted direct pause/containment fixtures; C actual control and numerical boundary ledger: no BODY after pause acknowledgement. S cleanup confirmed paused queue and no engines; no fresh fault matrix. |
| A03 | PASS, scoped | H accepted authorization/global gate/manual-hold fixtures; C actual-control scope retained. S settings restoration and paused queue confirmed; no new hold/retry matrix. |
| A04 | PASS, scoped | H accepted order/priority/preemption/schedule/batch/duplicate receipt fixtures; C actual-control scope retained. No new live schedule/preemption matrix on S. |
| A05 | PASS, source-qualified actual view-close/completion | H accepted crash/reconnect fixtures. C actual own-view close/immediate worker survival passed, but later cap clearing crashed the old worker. On deployed documentation source 75e4d45b6d34c1f9b0031e21109bf4b7c221dc62 with unchanged S, the user closed the originating tab while active; fresh accessibility and read-only engine observations confirmed its absence and the same live engine/worker/epoch after closure. That worker completed with one BODY cause and qualified final identity/hash. The two preceding fresh downloads did not observe origin closure or paint; those failed acceptance outcomes remain retained. |
| A06 | PASS, scoped | H accepted synthetic Range/fault-origin and bounded-retry evidence; C retained actual-control scope. Not a fresh S live origin-fault matrix. |
| A07 | PASS, scoped | H accepted direct replacement/validator/digest/incompatible-partial fixtures; C retained actual-control scope. No new S live replacement matrix or blanket expiry inference from 403. |
| A08 | PASS, qualified historical | H V7 six finite cap windows, at least 30 seconds measurement after at least 10 seconds settling. C changed-boundary numerical evidence retained; no fresh S numerical certification, complete zero/null/join/stall matrix or WAN hard-cap claim. |
| A09 | PASS, qualified historical | H V7 allocation/merge/owned-cleanup evidence with 50 ms sampling and st_blocks/APFS limits. Sampled allocation is not a certified unique physical peak. No fresh S peak-disk/collision trial. |
| A10 | PASS, scoped | H accepted destination/Unicode/collision/symlink/traversal/permissions/disk-full fixtures; C actual explicit Finder/path evidence. No new S live filesystem fault matrix. |
| A11 | PASS, source-qualified actual HTTP/hash | H actual authorized official 538364-byte download/hash; C actual 188900-byte HTTP/hash. S recovered existing 188900-byte publication, certified nofollow single-link identity and SHA-256 a41b5d356aea97a529fe27e0f7316d2f9d946d75927476cf9cf1b90637d00505; no extra BODY, two earlier BODY causes remain historical. Three subsequent independently bounded fresh official 188900-byte downloads on deployed documentation source 75e4d45b6d34c1f9b0031e21109bf4b7c221dc62 and unchanged S each completed with exactly one BODY cause, unchanged worker birth/epoch and the same certified nofollow single-link final identity/hash. These are fresh HTTP evidence, separate from publication-only recovery. |
| A12 | PASS, direct scope | H accepted direct duplicate/source-identity fixtures; C actual exact-control replay retained. S add-only exact replay succeeded without BODY. Extraction/quality/audio/subtitle/playlist gates RETIRED. |
| A13 | PARTIAL/OPEN | C actual actionable 404 remained pending with both renderers OFF; MAIN ON painted it, then PEER ON deduplicated the same event: one claim/one receipt. In the final fresh trial the user explicitly reported no ordinary automatic completion toast through completion. Store insertion and SDK presentation receipts do not prove visible paint; the screenshot/accessibility watch failed and the claimant window remains unknown. Ordinary automatic completion paint remains OPEN. Completion-event-only disabled replay NOT TESTED; preserved-outbox failure replay was proven by the actionable event and needs no additional completion-specific cycle. No exactly-once visual/read guarantee. |
| A14 | PASS, scoped; separate deployment receipts required | Actual seven MCP tools retained; C explicit reveal/Finder selection/hash passed. S recovery/query/config work passed. Final-document exact reviews, integration, push/remote readback, runtime rebind/verify and fresh discovery are separate parent-owned REQUIRED RECEIPTS. These docs do not self-certify their own postcommit deployment. |

The original 900-second authorization expired with zero BODY from add-only replay;
its unused slots remain RETIRED without extension, reset or refund. Its separate
cleanup completed naturally in 38.252662 seconds within its original 90s bound.
This historical zero-BODY result does not describe the later independent trials.
A later cold-paused start-only attempt admitted zero BODY and retired its unused
slot: preserving paused state was expected API behavior, not a code bug. The next
two independent fresh downloads completed but did not observe originating-view
closure or ordinary paint; those acceptance failures remain retained. The final
independent trial established A05 as described above, while the user reported no
ordinary completion toast. No trial clock or charged/retired slot was reset or
refunded.

The final trial's cap-clear phase made zero MCP calls because its message omitted
fulfilled prerequisite facts; the cap was not cleared and the file completed
naturally under it. Subsequent clear preparation stopped at an already-completed
guard, without a new BODY or product failure. Fresh read-only host capture on
2026-10-17 confirmed 10 jobs, six certified official finals and 11 cumulative BODY
causes, desired/applied one job/four stock connections/null cap, a paused queue,
no engines and the same worker. Old jobs, holds and payload hashes were preserved;
no earlier BODY checkpoint is retro-certified. The authorized ten-minute awake
assertion expired naturally without renewal or security change.

Final UI preference restoration remains OPEN. The empty search field was actually
observed, but Show all sessions OFF restoration and current watched Desktop plugin
ON verification remain pending because the Mac is explicitly locked. Host state
is not proof of those UI preferences. The claimant window/profile cause remains
unknown; a project label does not prove a profile. These follow-ups and the
separate parent-owned documentation receipts are required before any final
complete claim. No repeated Finder trial or extra completion-specific disabled
replay is required.

See [benchmark qualifications](benchmark-download-manager.md) and the
[final handoff](../.hermes/handoffs/2026-10-14-download-manager-final.md).
