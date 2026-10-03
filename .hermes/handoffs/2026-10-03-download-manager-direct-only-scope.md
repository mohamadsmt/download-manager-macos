# Direct-file-only scope amendment — 2026-10-03

Source of truth: the user explicitly retired YouTube/web-video capabilities and authorized continued direct-file delivery. This amendment supersedes the web-video requirements in the retained `2026-09-12-download-manager-execution.md` and later historical handoffs, including the old actual-YouTube/ffprobe completion gate. It does not relabel historical tests, failures or incomplete delivery as passed.

The bounded implementation starts from accepted clean BASE `16f6981b0b3e004adf4a9c04c4e9268085616a6f` in the managed direct-only worktree. The implementation contract prohibits agents, other writers/worktrees, runtime-state access, live sources, integration, push, app/profile/core installation or config changes, and self-approval. The parent owns subsequent ordered independent SPEC then QUALITY review on the frozen exact commit and ongoing canonical delivery. The private implementation report records exact commit, checks, process closures and limits; this document is a scope handoff, not either review gate.

## Active product

- Only direct HTTP(S) files are active sources. MP4/WEBM/MP3, signed and extensionless URLs remain generic bytes; no new extension, MIME or domain ban or extractor-based type inference is introduced. Videos/Audio remain ordinary destination categories.
- Remove `video.py`, its dedicated unit/integration suites, yt-dlp dependency/lock metadata and every extraction/media-helper entrypoint. No importable video stub or FFmpeg/ffprobe merge fallback remains. Shared global tools are untouched.
- Keep the accepted direct engine, worker, IPC, serving/MCP/CLI entrypoints, URL/TLS/credential rules, generic byte-range and segment merge, caps/process controls, retries, checksum, safe paths and no-clobber publication. Swift/browser/UI/core/runtime sources are unchanged; no app build/reinstall claim follows this headless amendment.

## Read-only historical compatibility

Literal persisted `source_kind='video'` decodes as `SourceKind.LEGACY_VIDEO`; it is not an active capability. No new add, conversion, retry write, target control, direct admission/dispatch/reconciliation or fresh marker/staged/final binding may authorize work or write target history. Exact historical immutable receipts/bindings may replay readback only.

Cold restart can advance the global worker epoch and paused queue gate as usual. It excludes legacy jobs from per-job pause, generation/revision, retry and event rewrites. Their source bytes, IDs, state, flags/holds/schedule, commands/receipts/events, retry history, reservations and marker/staged/final bindings remain unchanged. Existing files and directories remain untouched. Unknown/malformed kinds fail closed. There is no new schema or runtime-data migration.

Regression evidence uses only private synthetic SQLite/history/files and a local synthetic HTTP origin. It includes mixed direct/legacy cold worker restarts, bounded reads, global resume, all target controls, dispatch receipt preservation, inherited direct plans, retry/binding setters, malformed kinds, actual worker IPC, and ordinary `.mp4` bytes/hash/no-clobber publication with no yt-dlp/FFmpeg import or launch. Synthetic fixtures do not establish actual user-state or live acceptance.

## Updated acceptance mapping

| Gate | Current scope and outcome boundary |
| --- | --- |
| A01 | No body transfer on cold start, reconnect, list or add-only; direct files only. |
| A02 | Direct whole-queue pause and observed containment; next job/retry stays stopped. |
| A03 | Direct authorization, persistent global gate and manual holds. |
| A04 | Direct order/priority/preemption/schedule/batch/duplicate command readback. |
| A05 | Direct crash/recovery, worker survival and reconnect without duplicate execution. |
| A06 | Direct Range/fault origin and bounded retry. |
| A07 | Direct source replacement, expired-link/403 distinction, validators/digest and incompatible partial preservation. |
| A08 | Direct-engine global/per-job numerical cap windows and transitions. |
| A09 | Ordinary file allocation, physical blocks, segment merge peak and owned cleanup. |
| A10 | Direct destination, Unicode/collision/symlink/traversal/permissions/disk-full, no overwrite/fallback. |
| A11 | Actual authorized direct real-file and expected checksum remain required. YouTube/ffprobe/playability/quality assertions are retired by user scope, not PASS. |
| A12 | Direct duplicate/source identity as applicable remain required. Quality/audio/subtitle/playlist assertions are retired, not PASS. |
| A13 | Actual official Hermes/Desktop toast, dedup and replay remain required. |
| A14 | Actual Hermes MCP discovery/control/path/reveal remain required. |

T12/T13 are retired scope, not completed feature tasks. T14/T16/T23 and A07–A09 are direct only. Keep actual direct correctness, fault/crash/cap/disk/path, performance, installation/parity, official Desktop notification, independent review and canonical commit/push requirements. No full A01–A14 or ongoing direct-only delivery completion is asserted by this removal commit.

Active documents: `Docs/superpowers/specs/2026-09-12-hermes-download-manager-design.md`, `Docs/plans/2026-09-12-hermes-download-manager.md`, `Docs/plans/download-network-decision.md`, `Docs/benchmark-download-manager.md`, and `AGENTS.md`. All older handoff/evidence files remain historical and unchanged.
