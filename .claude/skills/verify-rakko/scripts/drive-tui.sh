#!/usr/bin/env bash
# Drives the TUI in a private tmux session against the local stack: connect, open a topic,
# filter, open a message. Needs rakko-web running (web-up.sh) only to create seed data.
# Writes pane captures to $YAGURA_EVIDENCE (default ./evidence-rakko). Exit 0 = all passed.
# Usage: drive-tui.sh [port]
set -uo pipefail
ROOT="$(git rev-parse --show-toplevel)"
PORT="${1:-18090}"; B="http://127.0.0.1:$PORT/api"; J='content-type: application/json'
EV="${YAGURA_EVIDENCE:-./evidence-rakko}"; mkdir -p "$EV"
CFG="$(mktemp -d)"; SESS="rakko-verify-$$"; T="verify-tui-$$"
cat >"$CFG/config.toml" <<TOML
[[profiles]]
name = "local"
bootstrap_servers = "127.0.0.1:19092"
tls_enabled = false
schema_registry_url = "http://127.0.0.1:18081"

[profiles.auth]
type = "none"
TOML
cleanup() { tmux kill-session -t "$SESS" 2>/dev/null; curl -s -XDELETE -o /dev/null "$B/p/local/topics/$T?confirm=$T"; rm -rf "$CFG"; }
trap cleanup EXIT
fail=0
cap() { tmux capture-pane -t "$SESS" -p | grep -v '^\s*$' >"$EV/tui-$1.txt"; }
check() { if grep -q -- "$2" "$EV/tui-$1.txt"; then echo "ok   $1: saw '$2'"; else echo "FAIL $1: no '$2' (see $EV/tui-$1.txt)"; fail=1; fi; }

curl -s -XPOST -H "$J" -d "{\"name\":\"$T\",\"partitions\":1,\"replication_factor\":1}" $B/p/local/topics >/dev/null
for i in 1 2 3; do
  st=new; [ $i = 2 ] && st=paid
  curl -s -XPOST -H "$J" -d "{\"key\":{\"text\":\"k$i\"},\"value\":{\"text\":\"{\\\"id\\\":$i,\\\"status\\\":\\\"$st\\\"}\"}}" $B/p/local/topics/$T/produce >/dev/null
done

tmux new-session -d -s "$SESS" -x 140 -y 40 "$ROOT/target/release/rakko --config-dir $CFG"
sleep 2; tmux send-keys -t "$SESS" Enter; sleep 1; cap picker; check picker "local"
tmux send-keys -t "$SESS" Enter; sleep 3; cap topics; check topics "1 Topics"
tmux send-keys -t "$SESS" "/" "$T" Enter; sleep 1; tmux send-keys -t "$SESS" Enter; sleep 3; cap messages; check messages 'raw   k3'
tmux send-keys -t "$SESS" "/" "paid" Enter; sleep 2; cap filtered; check filtered 'raw   k2'
if grep -q -E 'raw +k[13] ' "$EV/tui-filtered.txt"; then echo "FAIL filtered: non-matching row still listed"; fail=1; else echo "ok   filtered: non-matching rows hidden"; fi
tmux send-keys -t "$SESS" Enter; sleep 2; cap detail; check detail "offset:     1"
echo "evidence in $EV"; exit $fail
