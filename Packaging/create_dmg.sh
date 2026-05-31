#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="${VERSION:-0.1.0}"
APP_NAME="Download Manager"
APP_BUNDLE="$ROOT_DIR/dist/Download Manager.app"
DMG_PATH="$ROOT_DIR/dist/Download-Manager-$VERSION-macOS-arm64.dmg"
RW_DMG_PATH="$ROOT_DIR/dist/Download-Manager-$VERSION-macOS-arm64-rw.dmg"
DMG_ROOT="$ROOT_DIR/dist/dmg-root"
RELEASE_VENDOR_DIR="$ROOT_DIR/dist/release-vendor"
ARIA2="$RELEASE_VENDOR_DIR/aria2/aria2c"

"$ROOT_DIR/Packaging/stage_aria2_homebrew.sh" "$RELEASE_VENDOR_DIR/aria2"

DOWNLOAD_MANAGER_VENDOR_DIR="$RELEASE_VENDOR_DIR" \
  APP_VERSION="$VERSION" \
  "$ROOT_DIR/script/build_and_run.sh" --bundle-only

if [[ ! -x "$APP_BUNDLE/Contents/Resources/Vendor/aria2/aria2c" ]]; then
  echo "Bundled aria2c is missing from the app bundle." >&2
  exit 1
fi

xattr -cr "$APP_BUNDLE"
codesign --force --deep --sign - "$APP_BUNDLE"
xattr -cr "$APP_BUNDLE"
codesign --verify --deep --verbose=2 "$APP_BUNDLE"

rm -rf "$DMG_ROOT"
mkdir -p "$DMG_ROOT"
COPYFILE_DISABLE=1 ditto --noextattr --norsrc "$APP_BUNDLE" "$DMG_ROOT/$APP_NAME.app"
xattr -cr "$DMG_ROOT"
codesign --verify --deep --strict --verbose=2 "$DMG_ROOT/$APP_NAME.app"

rm -f "$DMG_PATH" "$RW_DMG_PATH"
COPYFILE_DISABLE=1 hdiutil create \
  -volname "Download Manager $VERSION" \
  -srcfolder "$DMG_ROOT" \
  -ov \
  -format UDRW \
  -fs APFS \
  "$RW_DMG_PATH" >/dev/null

MOUNT_ROOT="$(mktemp -d /tmp/download-manager-dmg.XXXXXX)"
cleanup() {
  hdiutil detach "$MOUNT_ROOT" >/dev/null 2>&1 || true
  rm -rf "$MOUNT_ROOT"
}
trap cleanup EXIT

hdiutil attach -nobrowse -readwrite -mountpoint "$MOUNT_ROOT" "$RW_DMG_PATH" >/dev/null
xattr -cr "$MOUNT_ROOT/$APP_NAME.app"
codesign --verify --deep --strict --verbose=2 "$MOUNT_ROOT/$APP_NAME.app"
hdiutil detach "$MOUNT_ROOT" >/dev/null
trap - EXIT
rm -rf "$MOUNT_ROOT"

hdiutil convert "$RW_DMG_PATH" -format UDZO -o "$DMG_PATH" >/dev/null
rm -f "$RW_DMG_PATH"
rm -rf "$DMG_ROOT"
echo "$DMG_PATH"
