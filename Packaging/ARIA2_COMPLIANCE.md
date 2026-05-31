# aria2 Distribution Compliance

Release DMGs that include bundled `aria2c` must ship:

- the bundled `aria2c` executable,
- the GPLv2-or-later license text,
- a clear source URL or written source offer for the exact bundled aria2 build,
- any notices required by the build source.

For v0.1.0, release packaging stages `aria2c` from the Homebrew `aria2`
formula under `dist/release-vendor/aria2` and then copies that staged directory
into the app bundle at `Contents/Resources/Vendor/aria2`. The repository keeps
only metadata and compliance documents; it does not commit the `aria2c` binary
or staged dylibs.

Before publishing a DMG, verify:

```bash
Packaging/stage_aria2_homebrew.sh
dist/release-vendor/aria2/aria2c --version
otool -L dist/release-vendor/aria2/aria2c
Packaging/create_dmg.sh
hdiutil verify dist/Download-Manager-0.1.0-macOS-arm64.dmg
```

See `Packaging/ARIA2_HOMEBREW_PROVENANCE.md` and
`Packaging/THIRD_PARTY_NOTICES.md` for the release metadata shipped with the
bundle.
