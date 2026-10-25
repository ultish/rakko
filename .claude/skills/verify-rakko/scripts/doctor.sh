#!/usr/bin/env bash
# Read-only: is the stack worth driving? Usage: doctor.sh [port]. Exit 0 = healthy.
PORT="${1:-18090}"; ok=0
chk() { if eval "$2" >/dev/null 2>&1; then echo "ok   $1"; else echo "FAIL $1"; ok=1; fi; }
chk "kafka container running"      "[ \"\$(docker inspect -f '{{.State.Running}}' rakko-kafka)\" = true ]"
chk "broker lists topics"          "docker exec rakko-kafka kafka-topics --bootstrap-server localhost:29092 --list"
chk "schema registry answers"      "curl -sf http://127.0.0.1:18081/subjects"
chk "demo-events topic seeded"     "docker exec rakko-kafka kafka-topics --bootstrap-server localhost:29092 --describe --topic demo-events"
chk "rakko-web serves /api/profiles on $PORT" "curl -sf http://127.0.0.1:$PORT/api/profiles | grep -q '\"local\"'"
chk "rakko TUI binary built"       "[ -x \"\$(git rev-parse --show-toplevel)/target/release/rakko\" ]"
exit $ok
