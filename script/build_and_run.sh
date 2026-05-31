#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-run}"
APP_NAME="Download Manager"
APP_BINARY_NAME="Download Manager"
APP_PRODUCT="DownloadManagerApp"
HOST_PRODUCT="DownloadManagerNativeHost"
BUNDLE_ID="com.mohamadsmt.DownloadManager"
APP_VERSION="${APP_VERSION:-0.1.0}"
MIN_SYSTEM_VERSION="14.0"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"
VENDOR_SOURCE_DIR="${DOWNLOAD_MANAGER_VENDOR_DIR:-$ROOT_DIR/Vendor}"
APP_BUNDLE="$DIST_DIR/$APP_NAME.app"
APP_CONTENTS="$APP_BUNDLE/Contents"
APP_MACOS="$APP_CONTENTS/MacOS"
APP_RESOURCES="$APP_CONTENTS/Resources"
APP_BINARY="$APP_MACOS/$APP_BINARY_NAME"
INFO_PLIST="$APP_CONTENTS/Info.plist"

build_bundle() {
  pkill -x "$APP_BINARY_NAME" >/dev/null 2>&1 || true

  swift build --product "$APP_PRODUCT"
  swift build --product "$HOST_PRODUCT"

  BUILD_DIR="$(swift build --product "$APP_PRODUCT" --show-bin-path)"
  HOST_BUILD_DIR="$(swift build --product "$HOST_PRODUCT" --show-bin-path)"

  rm -rf "$APP_BUNDLE"
  mkdir -p "$APP_MACOS" "$APP_RESOURCES/NativeMessaging"

  cp "$BUILD_DIR/$APP_PRODUCT" "$APP_BINARY"
  chmod +x "$APP_BINARY"

  if [[ -x "$HOST_BUILD_DIR/$HOST_PRODUCT" ]]; then
    cp "$HOST_BUILD_DIR/$HOST_PRODUCT" "$APP_RESOURCES/NativeMessaging/$HOST_PRODUCT"
    chmod +x "$APP_RESOURCES/NativeMessaging/$HOST_PRODUCT"
  fi

  find "$BUILD_DIR" -maxdepth 1 -name '*DownloadManagerApp*.bundle' -exec cp -R {} "$APP_RESOURCES/" \;
  cp -R "$ROOT_DIR/BrowserExtensions" "$APP_RESOURCES/BrowserExtensions"
  if [[ -d "$VENDOR_SOURCE_DIR" ]]; then
    cp -R "$VENDOR_SOURCE_DIR" "$APP_RESOURCES/Vendor"
  fi
  if [[ -f "$ROOT_DIR/Assets/AppIcon.icns" ]]; then
    cp "$ROOT_DIR/Assets/AppIcon.icns" "$APP_RESOURCES/AppIcon.icns"
  fi

  cat >"$INFO_PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleExecutable</key>
  <string>$APP_BINARY_NAME</string>
  <key>CFBundleIdentifier</key>
  <string>$BUNDLE_ID</string>
  <key>CFBundleName</key>
  <string>$APP_NAME</string>
  <key>CFBundleDisplayName</key>
  <string>$APP_NAME</string>
  <key>CFBundleIconFile</key>
  <string>AppIcon</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>$APP_VERSION</string>
  <key>CFBundleVersion</key>
  <string>1</string>
  <key>LSMinimumSystemVersion</key>
  <string>$MIN_SYSTEM_VERSION</string>
  <key>LSApplicationCategoryType</key>
  <string>public.app-category.utilities</string>
  <key>NSPrincipalClass</key>
  <string>NSApplication</string>
  <key>NSAppTransportSecurity</key>
  <dict>
    <key>NSAllowsArbitraryLoads</key>
    <true/>
  </dict>
  <key>CFBundleURLTypes</key>
  <array>
    <dict>
      <key>CFBundleURLName</key>
      <string>$BUNDLE_ID.add</string>
      <key>CFBundleURLSchemes</key>
      <array>
        <string>downloadmanager</string>
      </array>
    </dict>
  </array>
</dict>
</plist>
PLIST
}

open_app() {
  /usr/bin/open -n "$APP_BUNDLE"
}

case "$MODE" in
  run)
    build_bundle
    open_app
    ;;
  --bundle-only|bundle)
    build_bundle
    ;;
  --debug|debug)
    build_bundle
    lldb -- "$APP_BINARY"
    ;;
  --logs|logs)
    build_bundle
    open_app
    /usr/bin/log stream --info --style compact --predicate "process == \"$APP_BINARY_NAME\""
    ;;
  --telemetry|telemetry)
    build_bundle
    open_app
    /usr/bin/log stream --info --style compact --predicate "subsystem == \"$BUNDLE_ID\""
    ;;
  --verify|verify)
    build_bundle
    open_app
    sleep 1
    pgrep -x "$APP_BINARY_NAME" >/dev/null
    ;;
  *)
    echo "usage: $0 [run|--bundle-only|--debug|--logs|--telemetry|--verify]" >&2
    exit 2
    ;;
esac
