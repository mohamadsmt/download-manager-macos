# aria2 Homebrew Provenance

The v0.1.0 macOS release DMG bundles `aria2c` from the Homebrew `aria2`
formula and stages the required non-system dynamic libraries into the app
bundle. The release package does not commit these binaries to the repository.

## aria2 formula

- Formula: `homebrew/core/aria2`
- Homebrew formula revision: `1.37.0_2`
- aria2 program version: `1.37.0`
- License: GPL-2.0-or-later
- Homepage: <https://aria2.github.io/>
- Source tarball: <https://github.com/aria2/aria2/releases/download/release-1.37.0/aria2-1.37.0.tar.xz>
- Source SHA-256: `60a420ad7085eb616cb6e2bdf0a7206d68ff3d37fb5a956dc44242eb2f79b66b`
- Homebrew bottle root: <https://ghcr.io/v2/homebrew/core>
- arm64 Tahoe bottle SHA-256: `e02198308a07cc13589297bd682c0f63fe2e4ce09ff61d373696f4157eab89e5`

## Staged runtime dependencies

The release staging script copies these Homebrew-provided non-system libraries
when they are referenced by `aria2c`, then rewrites their install names to
relative `@loader_path` entries:

- `c-ares` 1.34.6
- `gettext` 1.0
- `libssh2` 1.11.1_1
- `openssl@3` 3.6.2
- `sqlite` 3.53.0

System libraries from `/usr/lib` and `/System/Library` are not copied.

## Reproducibility

Run the release script from the repository root:

```bash
Packaging/create_dmg.sh
```

The script installs `aria2` with Homebrew when needed, stages the binary under
`dist/release-vendor/aria2`, rewrites Homebrew load paths, ad-hoc signs the
staged binaries, and builds `dist/Download-Manager-0.1.0-macOS-arm64.dmg`.
