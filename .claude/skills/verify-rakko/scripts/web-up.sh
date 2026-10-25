#!/usr/bin/env bash
# Start rakko-web against the local compose stack on a private port with a scratch config.
# Usage: web-up.sh [port]    Prints the base URL; state lives in $RAKKO_VERIFY_DIR.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
PORT="${1:-18090}"
DIR="${RAKKO_VERIFY_DIR:-${TMPDIR:-/tmp}/rakko-verify-$PORT}"
mkdir -p "$DIR"
[ -x "$ROOT/target/release/rakko-web" ] || (cd "$ROOT" && cargo build --release -p rakko-web)
cat > "$DIR/config.toml" <<TOML
[[profiles]]
name = "local"
bootstrap_servers = "127.0.0.1:19092"
tls_enabled = false
schema_registry_url = "http://127.0.0.1:18081"

[profiles.auth]
type = "none"
TOML
if [ -f "$DIR/pid" ] && kill -0 "$(cat "$DIR/pid")" 2>/dev/null; then echo "already running pid $(cat "$DIR/pid")" >&2; echo "http://127.0.0.1:$PORT"; exit 0; fi
nohup "$ROOT/target/release/rakko-web" --bind "127.0.0.1:$PORT" --config "$DIR/config.toml" \
  --static-dir "$ROOT/crates/rakko-web/web/dist" >"$DIR/server.log" 2>&1 &
echo $! >"$DIR/pid"
for _ in $(seq 1 50); do curl -sf "http://127.0.0.1:$PORT/api/profiles" >/dev/null && { echo "http://127.0.0.1:$PORT"; exit 0; }; sleep 0.2; done
echo "rakko-web did not come up; see $DIR/server.log" >&2; exit 1
