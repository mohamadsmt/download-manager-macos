# Persistence

The current runnable implementation uses:

- `JSONDownloadStore` for queue/history metadata,
- `SidecarManifestStore` for resumable partial download state,
- per-download working directories under Application Support.

This keeps the app buildable with the currently selected Command Line Tools
toolchain. The planned SwiftData store should be added after full Xcode is
installed and selected because the SwiftData macro plugin is not available from
the current `/Library/Developer/CommandLineTools` setup.

The migration boundary is intentionally narrow: `DownloadItem` remains the
canonical Codable value model, so a SwiftData `@Model` can store the encoded
item plus indexed fields such as id, status, fileName, createdAt, and updatedAt.
