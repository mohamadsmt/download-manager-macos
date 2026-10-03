# Deferred notification adapter (T20a)

`plugin.js` is a plain ESM, default-disabled Hermes renderer plugin. It imports
only `host` and `queryClient` from the official `@hermes/plugin-sdk`. The host
owns enablement and invokes disposal on disable; the adapter does not inspect
or change plugin decisions. It contributes no panels, routes, sidebar or chat
UI. It is active only for profile `default`, connection `null` or `local`, and
gateway `open`, independently of chat/session state.

This slice does **not** implement or install the future backend bridge or
durable notification outbox. No live notification, A13 or A14
acceptance is claimed. No worker/downloader, core/profile/configuration or
Swift application is changed.

Current delivery scope is direct-file downloads. `Videos` and `Audio` are
destination categories for direct files; this adapter adds no extraction or
video-page capability.

## Closed future bridge

All routes below are relative to this plugin's SDK REST/socket namespace.
REST calls have a 5000 ms timeout. Responses reject missing/unknown fields,
wrong types, wrong profile/source, overlimit lists and invalid identifiers.

- `GET /events?view=pending&limit=1`: exact fields `schema_version: 1`,
  `profile: "default"`, `source: "local"`, `events: [DTO]` (zero or one),
  `next_cursor: null|string`, `has_more: boolean`. A cursor is opaque,
  1–256 ASCII letters/digits or `._:-`; the adapter does not follow it.
- `POST /events/claim` with `{consumer_id, limit: 1}`: exact fields
  `schema_version`, `profile`, `source`, `claims` (zero or one),
  `server_now_ms`. Each claim has exactly `{event, claim_token,
  lease_expires_at_ms}`. Token: 64 lowercase hex characters. Times: safe
  integers, server time nonnegative, lease strictly later than server time.
- `POST /events/presented` with `{consumer_id, event_id, claim_token,
  notification_id}`: exact `{status}` with `presented`,
  `already_presented` or `stale_claim`.
- `POST /events/release` with `{consumer_id, event_id, claim_token}`:
  exact `{status}` with `released`, `already_released` or `stale_claim`.
- Socket `/events` is a wake signal only. Its contents grant no authority.

DTO fields are exactly `event_id`, `type`, `job_id`, `collection_id`,
`generation`, `created_at_ms`, `count`, `reason_code`, `reveal_path`,
`presentation_state`. Event ID is a lowercase canonical UUID followed by
`:` and a positive safe decimal sequence (no leading zero). Job/collection
IDs are null or match the existing project identifier rule:
`[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`. Generation and creation time are
nonnegative safe integers; count is an integer from 1 through 500.

Types: `completion`, `needs_link`, `needs_login`, `needs_space`,
`persistent_error`. Presentation states: `pending`, `claimed`, `presented`,
`resolved`; only pending/claimed events may be delivered. `reason_code` is
null or one of this fixed enum: `link_expired`, `login_required`,
`insufficient_space`, `retry_exhausted`, `download_failed`. Reasons are
validated but never rendered. Toast titles/messages come from fixed Persian
templates using only type and bounded count. URLs, names, engine text and
arbitrary server messages never enter notification copy.

## Lifetime and uncertainty

Each activation has a fresh consumer UUID and generation, a scoped socket
and a 15-second polling interval. Scope changes retire them; old responses
and captured reveal actions are inert, including after returning to default.
Old claims are not released through a newly routed REST context: their server
leases expire. One application-level lane serializes reads and mutations;
in-flight wakes coalesce into one pending reconciliation. Each reconciliation
can notify at most one event. `has_more` does not trigger backlog draining.

Pure reads use the shared query client with an instance/generation-scoped
key, no retry, zero stale time and 15-second cache GC. Claims and mutations
never use query caching. Exact keys are cancelled/removed on retirement and
after stale in-flight reads where those query-client methods are supported.
There are no query subscriptions or per-event caches/timers.

The stable toast ID is `hermes-downloads:<event_id>`. A nonempty bounded
notification-store receipt is required before acknowledgement. A throw or
invalid receipt instead attempts release. An uncertain ack/release retains
at most one event/token/receipt and retries that identical mutation on a later
wake without notifying again. Malformed/unknown mutation responses retain
uncertainty; `stale_claim` clears authority for a later claim. Disposal drops
only local authority/cache, never durable history. Future worker code owns
immutable completion coalescing, needs-action ordering and durable replay.

The SDK receipt proves insertion into the host's notification store, not
visible paint or user reading. There is no exactly-once visual guarantee
across process crashes. Success toasts last 5000 ms; warning/error toasts are
persistent (`durationMs: 0`). No native notification channel is used.

## Explicit reveal boundary

Only completion DTOs may carry a non-null reveal path. Paths must be absolute
and lexically canonical under `/Users/<home>/Downloads/Hermes/` or
`/home/<home>/Downloads/Hermes/`, followed by at least a collection/category
component and a file component. Safe Unicode named collections, including
Persian names, override category organization; ordinary nested folders are
allowed between collection/category and file. Without a named collection,
direct files use `Videos`, `Audio`, `Documents`, `Software` or `Other`; those
category names are not a renderer allowlist. Paths are bounded to 4096
characters and components to 255; control/NUL characters, backslashes,
unpaired surrogates, empty/whitespace components and `.`/`..` are rejected.
The optional Persian action calls only `ctx.os.revealPath` on an explicit
click while its captured activation remains valid, with no auto-open or
fallback. The SDK exposes no home directory/inode/ownership certification;
the future backend must certify the actual user's canonical owned completed
path. Renderer validation alone cannot establish those facts.

## Deterministic adapter tests

Run exactly:

```sh
node --test integrations/hermes-downloads/desktop/plugin.test.mjs
```

The test-only ESM loader substitutes the SDK module while executing the actual
adapter. Controlled atoms, promises, scoped timer ticks, sockets, query cache,
disposers and notification receipts exercise behavior without a backend,
network, configuration changes or installation. These are renderer contract
tests, not live acceptance.
