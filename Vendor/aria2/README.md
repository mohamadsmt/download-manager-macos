# aria2 Bundle Slot

This directory documents the expected bundle slot for `aria2c`.

The repository does not commit the third-party `aria2c` binary. Release builds
stage it under ignored `dist/release-vendor/aria2` and copy that directory into
the app bundle at:

```text
Download Manager.app/Contents/Resources/Vendor/aria2/aria2c
```

The app's Automatic engine mode uses the bundled `aria2c` when it exists and is
executable, otherwise it falls back to the native Swift segmented engine.

aria2 is GPLv2-or-later. A distributed DMG that includes `aria2c` must include
the corresponding license notices and source-offer/source-access material.
