# Download Manager macOS

Native macOS download manager for macOS 14+.

The v0.1.0 release targets Apple Silicon Macs and is distributed as an
unsigned, unnotarized DMG. macOS Gatekeeper may warn or block the downloaded app
because this first public build is not signed with an Apple Developer ID
certificate.

## Features

- SwiftUI macOS app with sidebar, queue list, detail inspector, settings, and menu bar status.
- Custom app icon generated from `Assets/AppIcon.svg` into `Assets/AppIcon.icns`.
- One-active-download queue coordinator.
- Native HTTP/HTTPS segmented downloader with byte-range probing, partial files, pause/cancel via task cancellation, resume from partial segment files, and atomic merge.
- HTTP download support in the generated app bundle through `NSAppTransportSecurity/NSAllowsArbitraryLoads`, because many direct file mirrors still use plain HTTP.
- Batch URL entry, text-file import, clipboard monitoring, browser inbox monitoring, per-app speed limit setting, segment count setting, history persistence, and sidecar manifests.
- Chrome WebExtension/native-messaging assets and Safari WebExtension wrapper notes.
- Optional aria2 engine path. Automatic mode uses the native engine; bundled `aria2c` is used only when the user explicitly selects the aria2 engine.
- English and Persian localization resources.

## Requirements

- macOS 14 or newer
- Swift 5.9+ for local builds
- Homebrew for release DMG packaging with bundled aria2

## Build from source

```bash
swift build
swift run DownloadManagerCoreSmokeTests
./script/generate_app_icon.swift
./script/build_and_run.sh --bundle-only
./script/build_and_run.sh
```

## Create a release DMG

```bash
Packaging/create_dmg.sh
hdiutil verify dist/Download-Manager-0.1.0-macOS-arm64.dmg
```

The release script stages `aria2c` from Homebrew, copies its required non-system
dylibs into the app bundle, rewrites their load paths to relative
`@loader_path` entries, ad-hoc signs the staged binaries, and creates the DMG
under `dist/`.

## aria2 distribution

The repository does not include an `aria2c` binary. Release builds stage aria2
from Homebrew at packaging time and include the GPL/source compliance material
described in `Packaging/ARIA2_COMPLIANCE.md`.

## License

Download Manager macOS is distributed under GPL-2.0-or-later. See `LICENSE`.
