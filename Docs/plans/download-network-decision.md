# Download network trust decision

Date: 2026-09-12 / 1405-06-21

Status: accepted user scope amendment; G0 closed.

## Decision

The user stated that they vet links before submitting them and do not require special handling of malicious links. Use stock aria2/yt-dlp networking for trusted, user-provided sources. Do not add an egress proxy, custom downloader transport, root network rules, VPN changes, or a separate network service.

This supersedes the original design's universal rejection of private/internal destinations across DNS/redirect/extractor chains. Do not re-open that requirement in implementation or review without a new user request or materially changed use case.

## Retained boundaries

- Bounded initial HTTP/HTTPS URL parsing; reject malformed/control-character URLs, embedded credentials and unsupported schemes. Preserve exact valid signed query strings.
- Optional simple literal-host screening is a convenience guard only; it is not socket pinning. Test localhost fixtures may use explicit isolated grants.
- TLS certificate verification stays enabled. Do not leak Authorization/cookies across origins; unsupported credential scopes fail safely rather than weakening TLS or broadening access.
- No automatic cookie/browser-session import. Obtain explicit permission when authenticated sources actually require it.
- Fixed validated subprocess arguments, bounded redacted diagnostics, local authenticated RPC, owner-only state and no arbitrary shell exposure.
- Safe filesystem paths, no-clobber final publication, explicit delete/purge scope, no automatically executing downloaded content.
- Ordinary engine retry, pause, speed and file-integrity acceptance remains unchanged.

## Explicit residual risk

A source initially considered valid can be compromised, redirect to an unexpected address, or lead an extractor to a different host. The engines resolve and connect independently. This delivery does not guarantee that the entire chain avoids private/internal destinations or hostile content. No passing fixture or URL preflight should be described as full SSRF or malware protection.

## Implementation consequence

No additional G0 choice or guard feasibility study remains. T01 is complete once this decision and the matching spec/plan amendment are committed and pushed. T07 now covers bounded URL validation, credential scope, TLS and redaction, not a new network sandbox. Continue at T02 using TDD, fresh isolated environments and two-stage reviews.

## Related canonical documents

- `Docs/superpowers/specs/2026-09-12-hermes-download-manager-design.md`, section 8.
- `Docs/plans/2026-09-12-hermes-download-manager.md`, G0 and T01/T07.
