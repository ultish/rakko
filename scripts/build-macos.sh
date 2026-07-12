#!/usr/bin/env bash
# Build a native macOS rakko binary (no container needed — you're already on the
# target platform, unlike scripts/build-tui-rhel9.sh which cross-builds for Linux).
# rdkafka's cmake-build/ssl-vendored/libz-static features (see Cargo.toml) apply on
# any platform, so `cargo build --release` alone already statically vendors
# librdkafka + OpenSSL; this script just packages the result to match the RHEL9
# script's dist/ conventions.
#
# Output (repo root):
#   dist/rakko-macos-<arch>              # bare Mach-O binary
#   dist/rakko-macos-<arch>.tar.gz       # release archive
#   dist/SHA256SUMS                      # merged with any existing entries
#   dist/otool-macos-<arch>.txt          # dynamic-link audit (otool -L)
#
# Usage:
#   ./scripts/build-macos.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DIST="${DIST:-$ROOT/dist}"
mkdir -p "$DIST"

ARCH_RAW="$(uname -m)"
case "$ARCH_RAW" in
  arm64) ARCH="arm64" ;;
  x86_64) ARCH="x86_64" ;;
  *) ARCH="$ARCH_RAW" ;;
esac

BIN_NAME="rakko"
BIN_MACOS_NAME="${BIN_NAME}-macos-${ARCH}"
ARCHIVE_NAME="${BIN_MACOS_NAME}.tar.gz"
BIN_MACOS="$DIST/$BIN_MACOS_NAME"
ARCHIVE="$DIST/$ARCHIVE_NAME"

echo "==> cargo build --release (native $ARCH_RAW)"
cargo build --release

cp "target/release/$BIN_NAME" "$BIN_MACOS"
chmod +x "$BIN_MACOS"

echo
echo "==> dynamic-link audit (otool -L)"
OTOOL_OUT="$DIST/otool-macos-${ARCH}.txt"
otool -L "$BIN_MACOS" | tee "$OTOOL_OUT"
if grep -qiE 'libssl|libcrypto|librdkafka' "$OTOOL_OUT"; then
  echo "warning: binary has dynamic OpenSSL/librdkafka links (vendoring may have failed):" >&2
  grep -iE 'libssl|libcrypto|librdkafka' "$OTOOL_OUT" >&2
else
  echo "no dynamic OpenSSL/librdkafka links — vendoring looks correct"
fi

echo
echo "==> packaging"
STAGE=$(mktemp -d "${TMPDIR:-/tmp}/rakko-pack.XXXXXX")
VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
INNER="${BIN_NAME}-v${VERSION}-macos-${ARCH}"
mkdir -p "$STAGE/$INNER"
cp "$BIN_MACOS" "$STAGE/$INNER/$BIN_NAME"
cat > "$STAGE/$INNER/README.txt" <<EOF
rakko v${VERSION} (macOS ${ARCH})

librdkafka and OpenSSL are statically linked into this binary.

  chmod +x rakko
  ./rakko --help
  ./rakko --profile local

Config: ~/.config/rakko/config.toml
  (see config.example.toml in the repo)

Dev stack (for testing against a local broker):
  docker compose up -d
EOF
cp "$OTOOL_OUT" "$STAGE/$INNER/otool.txt"
tar -C "$STAGE" -czf "$ARCHIVE" "$INNER"
rm -rf "$STAGE"

# Merge into dist/SHA256SUMS rather than clobbering (build-tui-rhel9.sh may have
# already written Linux entries there).
SUMS_TMP="$(mktemp)"
if [[ -f "$DIST/SHA256SUMS" ]]; then
  grep -v -E "$BIN_MACOS_NAME|$ARCHIVE_NAME" "$DIST/SHA256SUMS" > "$SUMS_TMP" || true
fi
(cd "$DIST" && shasum -a 256 "$BIN_MACOS_NAME" "$ARCHIVE_NAME" >> "$SUMS_TMP")
mv "$SUMS_TMP" "$DIST/SHA256SUMS"

echo
echo "==> artifacts"
ls -la "$DIST"
echo
file "$BIN_MACOS" || true
echo
echo "GitHub Release attach (recommended):"
echo "  gh release upload <tag> $ARCHIVE $DIST/SHA256SUMS"
