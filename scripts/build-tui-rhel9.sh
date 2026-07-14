#!/usr/bin/env bash
# Build a RHEL 9–compatible x86_64 rakko binary via a container runtime.
#
# Supports (auto-detected, or set DOCKER=…):
#   - docker          (Docker Desktop / Rancher Desktop / colima …)
#   - container       (Apple Container app — macOS)
#   - podman
#
# Output (repo root):
#   dist/rakko-linux-amd64              # bare ELF (Unix style, no extension)
#   dist/rakko-linux-amd64.tar.gz       # release archive (has .tar.gz)
#   dist/rakko                          # short name (same binary)
#   dist/SHA256SUMS
#   dist/ldd.txt                          # dynamic-link audit from the builder
#
# Attach the .tar.gz (and SHA256SUMS) to a GitHub Release.
#
# Usage:
#   ./scripts/build-tui-rhel9.sh
#   ./scripts/build-tui-rhel9.sh --no-cache
#   DOCKER=container ./scripts/build-tui-rhel9.sh
#   DOCKER=podman ./scripts/build-tui-rhel9.sh
#
# Adapted from harness/scripts/build-tui-rhel9.sh. rakko delta: Dockerfile installs
# cmake + perl so rdkafka (cmake-build, ssl-vendored) can compile librdkafka and
# OpenSSL from source; ldd audit rejects stray dynamic OpenSSL/librdkafka links.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

IMAGE_TAG="${IMAGE_TAG:-rakko:rhel9}"
# Force x86_64 even on Apple Silicon
PLATFORM="${PLATFORM:-linux/amd64}"
CONTAINER_NAME="${CONTAINER_NAME:-rakko-rhel9-extract}"
DIST="${DIST:-$ROOT/dist}"
NO_CACHE=0

BIN_NAME="rakko"
BIN_LINUX_NAME="${BIN_NAME}-linux-amd64"
ARCHIVE_NAME="${BIN_LINUX_NAME}.tar.gz"

for arg in "$@"; do
  case "$arg" in
    --no-cache) NO_CACHE=1 ;;
    -h|--help)
      sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown arg: $arg (try --help)" >&2
      exit 2
      ;;
  esac
done

# ── detect docker | container (Apple) | podman ───────────────────────────────

_cli_ready() {
  # $1 = binary name. Return 0 if it can talk to its daemon/backend.
  local bin="$1"
  command -v "$bin" >/dev/null 2>&1 || return 1
  case "$bin" in
    docker)
      # Rancher/colima often put docker on PATH even when the daemon is down.
      docker info >/dev/null 2>&1
      ;;
    podman)
      podman info >/dev/null 2>&1
      ;;
    container)
      # Apple Container app — status field "running" when the system service is up.
      container system status 2>/dev/null | grep -qiE 'status[[:space:]]+running' \
        || container system status 2>/dev/null | grep -qi 'running'
      ;;
    *)
      return 1
      ;;
  esac
}

detect_cli() {
  if [[ -n "${DOCKER:-}" ]]; then
    if ! command -v "$DOCKER" >/dev/null 2>&1; then
      echo "error: DOCKER=$DOCKER not found on PATH" >&2
      exit 2
    fi
    if ! _cli_ready "$DOCKER"; then
      echo "error: DOCKER=$DOCKER is installed but not ready." >&2
      case "$DOCKER" in
        docker)
          echo "  Start Docker Desktop / Rancher Desktop / colima, or use:" >&2
          echo "    DOCKER=container ./scripts/build-tui-rhel9.sh" >&2
          ;;
        container)
          echo "  Start the Container app, then: container system start" >&2
          ;;
        podman)
          echo "  Start the podman machine: podman machine start" >&2
          ;;
      esac
      exit 2
    fi
    echo "$DOCKER"
    return
  fi

  # Prefer a working backend. Docker first when its daemon is up; then Apple
  # Container; then podman. A docker binary with a dead daemon is skipped by
  # _cli_ready (common with Rancher Desktop on PATH while stopped).
  local cand
  for cand in docker container podman; do
    if _cli_ready "$cand"; then
      echo "$cand"
      return
    fi
  done

  echo "error: no working container runtime found." >&2
  echo "  Tried: docker, container (Apple), podman" >&2
  echo "" >&2
  if command -v container >/dev/null 2>&1; then
    echo "  Apple Container is installed but not running. Try:" >&2
    echo "    open -a Container   # or launch the app" >&2
    echo "    container system start" >&2
    echo "    DOCKER=container ./scripts/build-tui-rhel9.sh" >&2
  elif command -v docker >/dev/null 2>&1; then
    echo "  docker is on PATH but the daemon is not reachable." >&2
    echo "  Start Docker Desktop / Rancher Desktop, or use Apple Container:" >&2
    echo "    DOCKER=container ./scripts/build-tui-rhel9.sh" >&2
  else
    echo "  Install Docker Desktop, Rancher Desktop, Apple Container, or Podman." >&2
  fi
  exit 2
}

DOCKER="$(detect_cli)"
echo "==> using runtime: $DOCKER"

if [[ ! -f Cargo.toml || ! -d src ]]; then
  echo "error: run from rakko repo root (missing Cargo.toml or src/)" >&2
  exit 2
fi
if [[ ! -f Dockerfile.rhel9 ]]; then
  echo "error: missing Dockerfile.rhel9" >&2
  exit 2
fi
if [[ ! -f Cargo.lock ]]; then
  echo "error: missing Cargo.lock (commit it so airgap builds are reproducible)" >&2
  exit 2
fi

# Context is repo root so Dockerfile can COPY Cargo.toml + src without a prefix.
BUILD_ARGS=(build --platform "$PLATFORM" -f Dockerfile.rhel9 -t "$IMAGE_TAG")
if [[ "$NO_CACHE" -eq 1 ]]; then
  BUILD_ARGS+=(--no-cache)
fi
BUILD_ARGS+=(.)

echo "==> building image $IMAGE_TAG (platform=$PLATFORM, context=.) …"
echo "    $DOCKER ${BUILD_ARGS[*]}"
echo "    note: first build compiles librdkafka + vendored OpenSSL — this can take a long time"
if ! "$DOCKER" "${BUILD_ARGS[@]}"; then
  echo "" >&2
  echo "error: image build failed." >&2
  echo "" >&2
  echo "Common on Apple Silicon building --platform linux/amd64:" >&2
  echo "" >&2
  echo "  exec /bin/sh: exec format error" >&2
  echo "  qemu: uncaught target signal 11 (Segmentation fault)" >&2
  echo "  rustc installed - (error reading rustc version)  then SIGSEGV" >&2
  echo "" >&2
  echo "  That is broken/missing x86_64 emulation under Docker (QEMU/Rosetta)," >&2
  echo "  not a problem with the Dockerfile itself." >&2
  echo "" >&2
  echo "  Fixes (pick one):" >&2
  echo "    1) Prefer Apple Container (often works when Docker QEMU segfaults):" >&2
  echo "         DOCKER=container ./scripts/build-tui-rhel9.sh" >&2
  echo "         # open Container app; container system start if needed" >&2
  echo "    2) Docker Desktop → Settings → General:" >&2
  echo "         Virtualization framework ON + Rosetta for x86_64/amd64 ON" >&2
  echo "         restart Docker, then: DOCKER=docker ./scripts/build-tui-rhel9.sh --no-cache" >&2
  echo "    3) Build on real x86_64 Linux / GitHub Actions (no emulation)." >&2
  echo "    4) If dist/ already has a good binary from an earlier successful build," >&2
  echo "       you can skip rebuild and attach that tarball." >&2
  exit 2
fi

# The container build also writes its own /out/SHA256SUMS (linux entries only),
# and the extraction below overwrites dist/SHA256SUMS with it — clobbering any
# macOS entries build-macos.sh already left there. Snapshot whatever's there
# right now (not earlier in this script — the docker build above can take
# minutes, and a concurrent build-macos.sh run could write fresh entries
# during that window; snapshotting late keeps the race window to the width of
# this one extraction step instead) so the merge step near the end of this
# script has the real pre-extraction baseline.
EXISTING_SUMS="$(mktemp)"
if [[ -f "$DIST/SHA256SUMS" ]]; then
  cp "$DIST/SHA256SUMS" "$EXISTING_SUMS"
fi

echo "==> extracting /out → $DIST …"
mkdir -p "$DIST"

# Primary extract: stream tar on stdout → host tar. No volume mounts, no
# container cp. Avoids Apple Container issues:
#   - cp from stopped container → "not running"
#   - cp -a / chmod on bind mount → "Operation not permitted"
extract_via_tar_stream() {
  echo "    (using $DOCKER run | tar stream — no volume mount)"
  # GNU tar in Rocky; BSD tar on macOS host both accept this for simple files.
  if ! "$DOCKER" run --rm --platform "$PLATFORM" "$IMAGE_TAG" \
      tar -C /out -cf - . \
    | tar -C "$DIST" -xf -; then
    return 1
  fi
  # Host-side sanity
  [[ -f "$DIST/$BIN_NAME" || -f "$DIST/$BIN_LINUX_NAME" ]]
}

extract_via_create_cp() {
  # Docker/Podman only: copy from a created-but-stopped container.
  "$DOCKER" rm -f "$CONTAINER_NAME" >/dev/null 2>&1 \
    || "$DOCKER" delete -f "$CONTAINER_NAME" >/dev/null 2>&1 \
    || true

  local create_args=(create --name "$CONTAINER_NAME" --platform "$PLATFORM" "$IMAGE_TAG")
  if ! "$DOCKER" "${create_args[@]}" >/dev/null; then
    return 1
  fi

  if ! "$DOCKER" cp "$CONTAINER_NAME:/out/." "$DIST/" 2>/dev/null; then
    if "$DOCKER" cp "$CONTAINER_NAME:/out" "$DIST-tmp" 2>/dev/null; then
      if [[ -d "$DIST-tmp/out" ]]; then
        cp -R "$DIST-tmp/out"/. "$DIST"/
      else
        cp -R "$DIST-tmp"/. "$DIST"/
      fi
      rm -rf "$DIST-tmp"
    else
      "$DOCKER" rm -f "$CONTAINER_NAME" >/dev/null 2>&1 \
        || "$DOCKER" delete -f "$CONTAINER_NAME" >/dev/null 2>&1 \
        || true
      return 1
    fi
  fi

  "$DOCKER" rm -f "$CONTAINER_NAME" >/dev/null 2>&1 \
    || "$DOCKER" delete -f "$CONTAINER_NAME" >/dev/null 2>&1 \
    || true
  [[ -f "$DIST/$BIN_NAME" || -f "$DIST/$BIN_LINUX_NAME" ]]
}

if ! extract_via_tar_stream; then
  echo "    tar stream failed; trying create+cp (docker/podman) …" >&2
  if ! extract_via_create_cp; then
    echo "error: could not extract /out from image with $DOCKER" >&2
    echo "  Manual fallback:" >&2
    echo "    $DOCKER run --rm --platform $PLATFORM $IMAGE_TAG tar -C /out -cf - . | tar -C dist -xf -" >&2
    exit 2
  fi
fi

# Normalize names (support older image that only wrote short name, or rust-triple name)
BIN_SHORT="$DIST/$BIN_NAME"
BIN_LINUX="$DIST/$BIN_LINUX_NAME"
BIN_LEGACY="$DIST/${BIN_NAME}-x86_64-unknown-linux-gnu"
if [[ ! -f "$BIN_LINUX" && -f "$BIN_LEGACY" ]]; then
  cp "$BIN_LEGACY" "$BIN_LINUX"
fi
if [[ ! -f "$BIN_LINUX" && -f "$BIN_SHORT" ]]; then
  cp "$BIN_SHORT" "$BIN_LINUX"
fi
if [[ -f "$BIN_LINUX" && ! -f "$BIN_SHORT" ]]; then
  cp "$BIN_LINUX" "$BIN_SHORT"
fi
# Drop verbose rust-triple name if present (redundant)
rm -f "$BIN_LEGACY" 2>/dev/null || true

chmod +x "$BIN_LINUX" "$BIN_SHORT" 2>/dev/null || true

if [[ ! -f "$BIN_LINUX" ]]; then
  echo "error: extract failed — $BIN_LINUX missing. Contents of $DIST:" >&2
  ls -la "$DIST" >&2 || true
  exit 2
fi

# Host-side dynamic-link audit when ldd.txt was extracted (built inside Rocky).
if [[ -f "$DIST/ldd.txt" ]]; then
  echo "==> reviewing builder ldd audit"
  cat "$DIST/ldd.txt"
  if grep -Eiq 'libssl|libcrypto|librdkafka|libz\.so' "$DIST/ldd.txt"; then
    echo "error: binary has unexpected dynamic deps — vendoring failed" >&2
    exit 2
  fi
  echo "    (no OpenSSL / librdkafka / libz dynamic links — good)"
fi

# Package version from Cargo.toml for the archive README
VERSION="$(sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -1)"
VERSION="${VERSION:-0.0.0}"

# Release archive with a real extension (what you attach on GitHub)
ARCHIVE="$DIST/$ARCHIVE_NAME"
# Pack binary under a versioned-looking dir name inside the tarball
STAGE=$(mktemp -d "${TMPDIR:-/tmp}/rakko-pack.XXXXXX")
INNER="${BIN_NAME}-v${VERSION}-linux-amd64"
mkdir -p "$STAGE/$INNER"
cp "$BIN_LINUX" "$STAGE/$INNER/$BIN_NAME"
cat > "$STAGE/$INNER/README.txt" <<EOF
rakko v${VERSION} (Linux amd64 / x86_64)

Built for RHEL 9–class glibc (Rocky 9 builder).
librdkafka and OpenSSL are statically linked into this binary.

  chmod +x rakko
  ./rakko --help
  ./rakko --profile local

Config: ~/.config/rakko/config.toml
  (see config.example.toml in the repo)

Dev stack (for testing against a local broker):
  docker compose up -d
EOF
# Also drop the ldd audit into the archive when present
if [[ -f "$DIST/ldd.txt" ]]; then
  cp "$DIST/ldd.txt" "$STAGE/$INNER/ldd.txt"
fi
tar -C "$STAGE" -czf "$ARCHIVE" "$INNER"
rm -rf "$STAGE"

# Merge into dist/SHA256SUMS rather than clobbering (build-macos.sh may have
# already written macOS entries there). Filter from the pre-extraction snapshot
# taken at the top of this script, not the live file — extraction above already
# overwrote the live file with the container's own linux-only SHA256SUMS.
SUMS_TMP="$(mktemp)"
if [[ -s "$EXISTING_SUMS" ]]; then
  grep -v -E "$BIN_NAME$|$BIN_LINUX_NAME$|$ARCHIVE_NAME$" "$EXISTING_SUMS" > "$SUMS_TMP" || true
fi
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$DIST" && sha256sum "$BIN_NAME" "$BIN_LINUX_NAME" "$ARCHIVE_NAME" >> "$SUMS_TMP")
elif command -v shasum >/dev/null 2>&1; then
  (cd "$DIST" && shasum -a 256 "$BIN_NAME" "$BIN_LINUX_NAME" "$ARCHIVE_NAME" >> "$SUMS_TMP")
fi
mv "$SUMS_TMP" "$DIST/SHA256SUMS"
rm -f "$EXISTING_SUMS"

echo
echo "==> artifacts"
ls -la "$DIST"
echo
if command -v file >/dev/null 2>&1; then
  file "$BIN_LINUX" || true
fi
echo
echo "GitHub Release attach (recommended):"
echo "  gh release upload <tag> $ARCHIVE $DIST/SHA256SUMS"
echo "  # optional bare binary:"
echo "  gh release upload <tag> $BIN_LINUX"
echo
echo "Airgap smoke (on RHEL9 x86_64):"
echo "  tar -xzf $ARCHIVE_NAME"
echo "  cd $INNER && chmod +x rakko && ./rakko --help"
