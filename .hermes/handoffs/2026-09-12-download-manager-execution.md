# Execution handoff — Hermes Download Manager

Date: 2026-09-12 / 1405-06-21.

Status: design approved, implementation plan recorded, final network-scope decision resolved. **No headless service implementation or benchmark has started.** This handoff prevents discovery/context drift at the start of implementation.

## First next action

1. Load `subagent-driven-development`, `test-driven-development`, `ponytail-workflows`, and the relevant packaging/verification skills.
2. In this canonical checkout, run `git status --short` and `git log -3 --oneline`; read the spec, plan and network decision below.
3. Enumerate T01–T25 in the task tracker; T01 is documentation-complete. Start **T02: isolated headless package/harness**, with a failing behavior/packaging test, not stubs claimed as delivery.
4. Use bounded fresh implementation children and ordered independent reviews. Never begin another overlapping writer before the prior exact commit is verified.
5. Do not ask the user again whether to add a malicious-link guard or approve the same design. That question was answered and the plan/spec were amended.

## Canonical locations

- Root: `/Users/mohamadsmt/Documents/Download Manager`.
- Desktop project: `Hermes Download Manager`, anchored to this root; not Google Docs MCP.
- Spec: `Docs/superpowers/specs/2026-09-12-hermes-download-manager-design.md`.
- Plan: `Docs/plans/2026-09-12-hermes-download-manager.md`.
- Network decision: `Docs/plans/download-network-decision.md`.
- Repo: `https://github.com/mohamadsmt/download-manager-macos` (**public**), origin main.
- Last pushed baseline before this handoff/amendment: `02ea56a7bd45f9e65518290008aec041c5c61f15`.
- This handoff and scope amendment belong to the following documentation commit; read current HEAD for its exact SHA rather than assuming the baseline remains HEAD.
- Existing installed application: `/Applications/Download Manager.app`, bundle ID `com.mohamadsmt.DownloadManager`.

## What the user approved

A background local manager operated through Hermes conversation only. Batch direct URLs and video pages, particularly YouTube; durable queue/edit/order/priority/start/pause/resume, retry and expired-link handling, rate limits and segmented downloads, predictable folders in Downloads, no preallocation surprise. No separate UI.

Default policies agreed in design:

- No transfer merely from startup, reconnect, queue inspection or add-only. Explicit download command authorizes its target; persistent global hold cannot be bypassed.
- Cold worker restart paused; closing only the chat/MCP must not kill authorized worker downloads. Manual item holds survive resume-all.
- Default video quality up to 1080p, audio included, no unnecessary transcoding, no accidental whole-playlist download.
- `~/Downloads/Hermes/` root. Explicit collection takes precedence over type; Videos/Audio/Documents/Software/Other otherwise. `.incomplete/<job-id>` for partials; explicit final path and no silent fallback or overwrite.
- Retry finite, 403 not automatically expiry, refreshed source cannot corrupt/mix partial bytes.
- No extra malicious-link protector/proxy: the user explicitly said they vet their links and have no special sensitivity here. Full-chain private-destination protection was removed from spec. Preserve TLS, credential isolation/consent, safe paths/process arguments and no auto-execution.
- If changing existing Swift app, build latest and install/verify it in Applications. Commit **and push** finished repository changes.

## Work already completed and verified

- Read old app's controllers, models, queue store, native/aria2 adapters, filename resolver, smoke tests, AppPaths and settings.
- Documented comparison of aria2/IDM/FDM/JDownloader/XDM and added yt-dlp/FFmpeg after user confirmed video-page inputs. This was a capability comparison, **not a throughput benchmark**.
- Parent verified installed yt-dlp command construction without launching/network: `ratelimit=123456` produces aria2 `--max-overall-download-limit 123456`, explicit concurrency args follow upstream defaults, and `--file-allocation=none` is present. Do not treat this as speed or YouTube acceptance.
- Parent verified official Desktop SDK `host.notify`, `ctx.socket/ctx.rest`, `ctx.os.revealPath`, separate backend allowlist and ephemeral toast behavior. No notification plugin has been installed/tested.
- Prior plan/spec commits were pushed and remote SHA matched; repo was clean before the latest scope amendment.
- User asked to clear the old queue. This was completed separately through an atomic, backed-up local state edit with the app not running. Unfinished entries removed; completed history and all payload files preserved. Queue readback confirmed no unfinished entries. Do not re-run or clear history again.
- Private backup of old queue is outside Git under the existing app state directory. Do not publish it or print its URLs. Local maintenance helper exists in Hermes cache (`download-manager-ops/clear_legacy_queue.py`), not production code or a repository deliverable.
- No Swift source, installed bundle, Hermes config or service settings changed. No app build/reinstall, proxy install or live payload download was performed.
- Both read-only discovery subagents completed. There are no task-owned running worker/download/benchmark processes from this conversation.

## Findings to preserve

- `DownloadController.init` unconditionally resumes queue after load; add defaults start immediately. Pause invokes scheduling of another item; no persistent whole-queue gate in the examined path.
- Old `DownloadQueue.nextEligibleID` sorts by priority/createdAt, ignoring array reorder for dispatch. Do not port this mismatch.
- Old aria2 wrapper checks cancellation only after `waitUntilExit`; not safe immediate stop.
- Old native engine keeps segment files while writing a complete merged file; observed path did not clean them on success. This is code evidence, not measured attribution of past disk use.
- Old JSON store is atomic replacement, not a cross-command/intent/event transaction. Actual controller tied to AppKit/Combine, so Python headless control is chosen over a Swift bridge; reuse intended tests/rules, not broken algorithms.
- macOS file logical length is not actual allocated blocks, especially sparse files. aria2 uses `file-allocation=none`; test physical space and merge peak.
- yt-dlp native `-N` fragment readers do not share one rate budget. Prefer certified aria2 payload paths with whole-child limits; beware multiple simultaneous format contexts and silent native/networked-FFmpeg fallback. Zero share means wait, not aria2 zero/unlimited.
- Upstream yt-dlp external aria2 progress RPC branch is disabled in examined release. Do not promise continuous progress hooks without proving a usable observation path.
- Official Desktop notification-only plugin can work with chat closed while app running. Full app exit means durable event replay on relaunch, not immediate message delivery. Toast presented != user read. No state.db/transcript/DOM injection.

## Runtime / packaging facts (recheck before acceptance)

- macOS. Ambient `python3` was Python 3.14.0; use explicit isolated Python 3.12 for the headless package.
- Existing `/opt/homebrew/bin/aria2c` reports 1.37.0.
- Existing yt-dlp resolves into `/opt/homebrew/Cellar/yt-dlp/2026.3.17_1/libexec/`; parent verified package 2026.3.17 and EJS 0.8.0.
- ffmpeg, ffprobe, deno and node are on PATH; exact fresh acceptance versions/digests must be recorded.
- Candidate dependency versions and packaging are in the plan, not universal latest recommendations. If live YouTube requires an update, pin/review the new version and regenerate affected acceptance.
- Existing launcher pattern in google-docs-mcp clears `PYTHONPATH` and `PYTHONHOME` and executes installed venv entry point. Reuse pattern, not foreign project modules.
- New files go under `headless/` and `integrations/hermes-downloads/`; do not scaffold every planned module empty.

## Remaining work / gates

T02–T25 have not been implemented. Complete tests, actual fault recovery, numeric speed/disk benchmarks, actual YouTube file+ffprobe, real Desktop event/readback, scoped installation, canonical non-editable source/install identity, two-stage reviews, commit/push. Full A01–A14 map is in the plan.

G0 is **not** a remaining blocker. Actual credential access, external live test URL/budget, or disruptive app quit/restart still require their scoped approvals at the point needed. No password/cookie secrets in chat. No system/VPN changes.

Before another long phase, checkpoint exact commits/tests/processes and use a fresh session again. Do not replace missing evidence with plausible summaries or turn plan-only items into completed ones.
