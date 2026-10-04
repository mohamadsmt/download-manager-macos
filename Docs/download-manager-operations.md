# Default-profile installation operations

This wiring supports trusted direct-file downloads through Python 3.12, the official MCP SDK and `/opt/homebrew/bin/aria2c`. YouTube and web-video extraction are retired; video helpers are not installation requirements. Hermes remains the control interface; no additional application is installed.

Run the scripts with the independently installed physical Python 3.12 interpreter, clearing `PYTHONPATH`/`PYTHONHOME` and disabling bytecode. From the canonical `/Users/mohamadsmt/Documents/Download Manager` checkout:

```sh
env -u PYTHONPATH -u PYTHONHOME PYTHONDONTWRITEBYTECODE=1 headless/.venv/bin/python -I -B headless/scripts/install.py
env -u PYTHONPATH -u PYTHONHOME PYTHONDONTWRITEBYTECODE=1 headless/.venv/bin/python -I -B headless/scripts/verify-install.py
```

The installer defaults to a read-only dry run. `NOT_READY` and nonzero exit mean prerequisites or ownership checks failed. The current one-tool candidate and absent integration bundle are NOT_READY even if synthetic installer tests pass. `PLAN_READY` means the local plan is ready, not live acceptance. Source must be clean and the exact commit must have independent specification PASS followed by quality/security APPROVED, including backend profile/auth behavior and the renderer SDK contract. Untracked regular, owner-owned, single-link Markdown documents directly under `.hermes/handoffs/` are retained as history and do not count as source changes. Tracked changes, all other untracked paths and linked or nested handoffs still block. Presence and schemas cannot prove runtime behavior.

After independent review and separately authorized real application, add `--apply --profile default --expected-commit <full-reviewed-HEAD>` to the installer command. Other profiles, root overrides, contradictory modes and unknown arguments are rejected. HOME comes from the current account database, not the process environment. Unit/integration callers use private fixture layouts; they do not change this CLI boundary.

Installation requires the complete reviewed three-file integration bundle and exact seven closed typed tools: `downloads_add`, `downloads_query`, `downloads_control`, `downloads_edit`, `downloads_replace_source`, `downloads_configure`, `downloads_files`. Direct-file schemas reject media options. Discovery uses official SDK stdio initialization from scratch directories and does not start a worker. Physical runtime checks enumerate every package Python module, compare source/site/commit hashes, and reject editable imports and untrusted entrypoints. The versioned runtime is under canonical `.artifacts/download-manager/runtime/<commit>`; a foreign occupied path is retained and blocks creation.

The fixed default state is `~/Library/Application Support/HermesDownloadManager/default`, endpoint `worker.sock`, output `~/Downloads/Hermes`, and LaunchAgent `~/Library/LaunchAgents/com.mohamadsmt.hermes-downloads.default.plist`. The full lexical path must fit Darwin's 103-byte socket bound. Unknown endpoints, nonprivate directories, symlinks, linked/special files and collisions block before installation. State, queues, history, incomplete files and output are retained. Certified service restarts recover paused under the existing T16f service contract; installation never resumes downloads.

Only `mcp_servers.downloads` and membership in `plugins.enabled` are owned config settings. Sampling uses `{enabled: false}`, connect timeout is 15 seconds and call timeout 30 seconds. An explicit backend disabled choice blocks application. Renderer decisions remain `NOT_OBSERVED`: use Desktop's official Capabilities → Plugins toggle and readback. Do not edit Chromium storage or infer renderer enablement from backend configuration.

Raw config backup, proposed bytes, a strict ownership manifest and step readbacks live privately under `~/.hermes/installations/hermes-downloads/<run-id>/`. Files are 0600 and managed directories 0700. Failed steps retain evidence; `INSTALL_FAILED` does not imply automatic rollback. The YAML parser rejects duplicates, anchors/aliases, tags, multiple documents and layouts whose untouched round trip is not byte preserving. Ordinary comments, quotes, ordering, flow styles and unknown values are retained in supported documents. Unsupported formatting blocks replacement rather than normalizing the user's document. Compare/recheck, nofollow reads, atomic replacements and file/directory fsync form an owner-only boundary; they do not defeat every malicious same-account filesystem race.

`INSTALLED_WIRING` records file/service wiring and paused local IPC. `VERIFIED_LOCAL` separately confirms current installation ownership, hashes, schema discovery and one redacted IPC page. Neither proves actual Hermes tool registration/calls, direct-file cap/recovery acceptance, Desktop authenticated route, toast visibility, user read or reveal. Reports mark those as `LIVE_PENDING`. A missing worker is a real failure, never an empty healthy queue. The verifier only reads and never repairs, launches, recycles or resumes.

For rollback, add `--rollback <exact-private-manifest> --profile default` to the installer command. Backup hashes and each current object are freshly checked before stopping the exact recorded launchd service. A changed PID, argv, label/plist, user-owned file or owned config setting blocks rollback. Unrelated config edits are preserved through selective removal of the installed patch; an unchanged config can be restored byte for byte. Only unchanged installer-owned integration/plist files are removed. Runtime, state and Downloads roots are retained. Partial failure reports `ROLLBACK_BLOCKED`; retain its evidence and resolve the changed ownership explicitly.

Backend route mounting requires separately authorized Desktop live acceptance. The supported backend-only operation is:

```js
await window.hermesDesktop.recycleBackend('default')
```

It tears down the primary default backend and can interrupt active Desktop turns. Installer and verifier never call it. A successful `{ok:true}` proves teardown only: verify old owned backend exit, new owned backend PID/port, authenticated plugin route and renderer profile/source scope afterward. `host.restartGateway` concerns a different gateway and is not this operation. Start a fresh Hermes session or explicitly rediscover MCP, enable/read back the renderer through its official toggle, and record actual direct-file calls, notification paint, user acknowledgement and reveal independently. No installation report completes A01–A14.
