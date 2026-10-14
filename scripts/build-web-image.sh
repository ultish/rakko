#!/usr/bin/env bash
# Build the rakko-web container image and export it for air-gapped use.
#
# Output (repo root):
#   dist/rakko-web-<version>-linux-amd64.tar.gz   docker-save archive (gzip) — load with
#                                                 `docker load -i` / `podman load -i`
#   dist/SHA256SUMS                               merged, not clobbered
#
# Usage:
#   ./scripts/build-web-image.sh
#   PLATFORM=linux/arm64 ./scripts/build-web-image.sh
#   PUSH=ghcr.io/ultish/rakko-web ./scripts/build-web-image.sh   # also tag + push
#   DOCKER=podman ./scripts/build-web-image.sh
#
# Attach the .tar.gz to a GitHub Release:
#   gh release upload v<X.Y.Z> dist/rakko-web-<X.Y.Z>-linux-amd64.tar.gz dist/SHA256SUMS
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DOCKER="${DOCKER:-docker}"
PLATFORM="${PLATFORM:-linux/amd64}"
ARCH="${PLATFORM#linux/}"
VERSION="${VERSION:-$(sed -n 's/^version = "\(.*\)"/\1/p' crates/rakko-web/Cargo.toml | head -1)}"
IMAGE="${IMAGE:-rakko-web}"
DIST="${DIST:-$ROOT/dist}"
ARCHIVE="rakko-web-${VERSION}-linux-${ARCH}.tar.gz"

command -v "$DOCKER" >/dev/null || { echo "error: $DOCKER not found" >&2; exit 1; }
mkdir -p "$DIST"

echo "==> building $IMAGE:$VERSION ($PLATFORM)"
"$DOCKER" build --platform "$PLATFORM" -f Dockerfile.web \
  --build-arg "VERSION=$VERSION" -t "$IMAGE:$VERSION" -t "$IMAGE:latest" .

echo "==> exporting $ARCHIVE"
"$DOCKER" save "$IMAGE:$VERSION" | gzip -9 > "$DIST/$ARCHIVE"

(
  cd "$DIST"
  touch SHA256SUMS
  grep -v " $ARCHIVE\$" SHA256SUMS > SHA256SUMS.tmp || true
  if command -v sha256sum >/dev/null; then sha256sum "$ARCHIVE" >> SHA256SUMS.tmp; else shasum -a 256 "$ARCHIVE" >> SHA256SUMS.tmp; fi
  mv SHA256SUMS.tmp SHA256SUMS
)
ls -lh "$DIST/$ARCHIVE"

if [[ -n "${PUSH:-}" ]]; then
  echo "==> pushing $PUSH:$VERSION"
  "$DOCKER" tag "$IMAGE:$VERSION" "$PUSH:$VERSION"
  "$DOCKER" tag "$IMAGE:$VERSION" "$PUSH:latest"
  "$DOCKER" push "$PUSH:$VERSION"
  "$DOCKER" push "$PUSH:latest"
fi
echo "done. air-gapped: copy $ARCHIVE across, then: docker load -i $ARCHIVE"
