#!/usr/bin/env bash
# Stop the rakko-web that web-up.sh started (by pid file, never by name). Usage: web-down.sh [port]
PORT="${1:-18090}"
DIR="${RAKKO_VERIFY_DIR:-${TMPDIR:-/tmp}/rakko-verify-$PORT}"
[ -f "$DIR/pid" ] && kill "$(cat "$DIR/pid")" 2>/dev/null
rm -rf "$DIR"
echo stopped
