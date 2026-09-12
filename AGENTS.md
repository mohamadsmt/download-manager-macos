# Hermes Download Manager project

## Current work

Resume from `.hermes/handoffs/2026-09-12-download-manager-execution.md` when continuing this project. Read it before rediscovering requirements or changing code. The approved spec and implementation plan are linked there; G0 is resolved (trusted user-vetted links, no extra proxy).

## Delivery boundaries

- Hermes is the sole control interface. No new download-manager UI or custom downloader engine.
- Use the existing repository as canonical. Preserve user-owned dirty changes and old payload files/history.
- Use TDD and bounded subagent execution with specification PASS followed by quality/security APPROVED on the exact commit; verify child writes/tests independently.
- Python headless work uses an isolated Python 3.12 non-editable environment; clear ambient `PYTHONPATH`/`PYTHONHOME`.
- Runtime queues, signed URLs, credentials, raw user test inputs and state backups never go into this public repository.
- Final output belongs under `~/Downloads/Hermes/`; no implicit transfer on startup/list/add-only; pause and deletion semantics follow the spec.
- Do not reintroduce full SSRF protection, a network proxy or system-wide network changes: the user chose trusted-source scope. Keep TLS, credentials, filesystem, process and no-overwrite protections.
- If the existing Swift application is changed, build/test it and install/verify the latest bundle in `/Applications/Download Manager.app`. If only headless/docs change, do not claim the app was reinstalled.
- Commit and push completed changes; verify remote SHA. No force-push, automatic core-Hermes edits or modification of another Hermes profile.
- A plan, installed binary or local fixture is not live acceptance. Mark every A01–A14 outcome honestly; actual YouTube and Desktop notification tests are required before complete delivery.
