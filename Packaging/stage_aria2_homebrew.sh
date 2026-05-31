#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGE_DIR="${1:-$ROOT_DIR/dist/release-vendor/aria2}"
LIB_DIR="$STAGE_DIR/lib"

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required to stage aria2 for release packaging." >&2
  exit 1
fi

if ! brew list aria2 >/dev/null 2>&1; then
  brew install aria2
fi

ARIA2_BIN="$(brew --prefix aria2)/bin/aria2c"
if [[ ! -x "$ARIA2_BIN" ]]; then
  echo "Homebrew aria2c was not found at $ARIA2_BIN." >&2
  exit 1
fi

rm -rf "$STAGE_DIR"
mkdir -p "$LIB_DIR"

cp "$ARIA2_BIN" "$STAGE_DIR/aria2c"
chmod u+w "$STAGE_DIR/aria2c"
chmod +x "$STAGE_DIR/aria2c"

cp "$ROOT_DIR/Vendor/aria2/README.md" "$STAGE_DIR/README.md"
cp "$ROOT_DIR/Vendor/aria2/LICENSE-GPL-2.0-or-later.txt" "$STAGE_DIR/LICENSE-GPL-2.0-or-later.txt"
cp "$ROOT_DIR/Packaging/THIRD_PARTY_NOTICES.md" "$STAGE_DIR/THIRD_PARTY_NOTICES.md"
cp "$ROOT_DIR/Packaging/ARIA2_HOMEBREW_PROVENANCE.md" "$STAGE_DIR/ARIA2_HOMEBREW_PROVENANCE.md"

is_bundle_dependency() {
  case "$1" in
    /opt/homebrew/*|/usr/local/*) [[ -f "$1" ]] ;;
    *) return 1 ;;
  esac
}

dependency_paths() {
  otool -L "$1" | awk 'NR > 1 { print $1 }' | while IFS= read -r dep; do
    if is_bundle_dependency "$dep"; then
      printf '%s\n' "$dep"
    fi
  done
}

while :; do
  candidates="$(mktemp)"
  dependency_paths "$STAGE_DIR/aria2c" >>"$candidates"
  for dylib in "$LIB_DIR"/*.dylib; do
    [[ -e "$dylib" ]] || continue
    dependency_paths "$dylib" >>"$candidates"
  done

  copied_new=0
  while IFS= read -r dep; do
    [[ -n "$dep" ]] || continue
    dest="$LIB_DIR/$(basename "$dep")"
    if [[ ! -f "$dest" ]]; then
      cp -L "$dep" "$dest"
      chmod u+w "$dest"
      copied_new=1
    fi
  done < <(sort -u "$candidates")
  rm -f "$candidates"

  [[ "$copied_new" -eq 0 ]] && break
done

rewrite_load_paths() {
  local file="$1"
  local lib_prefix="$2"
  dependency_paths "$file" | while IFS= read -r dep; do
    local base
    base="$(basename "$dep")"
    if [[ "$file" == "$LIB_DIR/$base" ]]; then
      continue
    fi
    install_name_tool -change "$dep" "$lib_prefix/$base" "$file"
  done
}

rewrite_load_paths "$STAGE_DIR/aria2c" "@loader_path/lib"
for dylib in "$LIB_DIR"/*.dylib; do
  [[ -e "$dylib" ]] || continue
  install_name_tool -id "@loader_path/$(basename "$dylib")" "$dylib"
  rewrite_load_paths "$dylib" "@loader_path"
done

for mach_o in "$LIB_DIR"/*.dylib "$STAGE_DIR/aria2c"; do
  [[ -e "$mach_o" ]] || continue
  codesign --force --sign - "$mach_o"
done

if otool -L "$STAGE_DIR/aria2c" "$LIB_DIR"/*.dylib | grep -E '/opt/homebrew|/usr/local'; then
  echo "Bundled aria2 still has Homebrew load paths." >&2
  exit 1
fi

"$STAGE_DIR/aria2c" --version >/dev/null
echo "$STAGE_DIR"
