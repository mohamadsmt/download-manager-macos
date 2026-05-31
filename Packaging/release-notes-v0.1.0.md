# Download Manager 0.1.0

Initial public macOS release.

## Requirements

- macOS 14 or newer
- Apple Silicon Mac (`arm64`)

## Included

- Native SwiftUI download manager app
- Segmented HTTP/HTTPS download engine with pause, cancel, resume from partial
  segment files, and atomic merge
- Queue list, detail inspector, settings, clipboard monitoring, browser inbox
  monitoring, and menu bar status
- Chrome WebExtension/native messaging assets and Safari WebExtension notes
- Bundled `aria2c` 1.37.0 from Homebrew for optional aria2 engine mode

## aria2 provenance

The DMG bundles `aria2c` from Homebrew `homebrew/core/aria2` formula revision
`1.37.0_2`. aria2 source is available from:

<https://github.com/aria2/aria2/releases/download/release-1.37.0/aria2-1.37.0.tar.xz>

Source SHA-256:

```text
60a420ad7085eb616cb6e2bdf0a7206d68ff3d37fb5a956dc44242eb2f79b66b
```

## Signing status

This first release is ad-hoc signed only. It is not signed with an Apple
Developer ID certificate and is not notarized by Apple. macOS Gatekeeper may
warn or block opening the app after download. Review the source and release
artifact before running it.
