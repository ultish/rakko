#!/usr/bin/env bash
# Names in the sidebar lists are real <a href> links that load directly in a fresh page.
# Usage: drive-links.sh [port]   Needs headless Chrome (CHROME=… to override). Exits non-zero on any difference.
set -uo pipefail
PORT="${1:-18090}"; U="http://127.0.0.1:$PORT"; B="$U/api"
EV="${YAGURA_EVIDENCE:-./evidence-rakko}"; mkdir -p "$EV"
CHROME="${CHROME:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
T="verify-links-$$"; G="verify-links-g-$$"; J='content-type: application/json'
fail=0
check() { if grep -qF -- "$3" <<<"$2"; then echo "ok   $1"; else echo "FAIL $1: missing $3"; fail=1; fi; }
dom() { "$CHROME" --headless=new --disable-gpu --virtual-time-budget=6000 --dump-dom "$U/$1" 2>/dev/null; }

curl -sf -XPOST -H "$J" -d "{\"name\":\"$T\",\"partitions\":1,\"replication_factor\":1}" $B/p/local/topics >/dev/null || { echo "FAIL create topic"; exit 1; }
curl -s -XPOST -H "$J" -d '{"key":{"text":"k"},"value":{"text":"v"}}' $B/p/local/topics/$T/produce >/dev/null
docker exec rakko-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic $T --group $G --from-beginning --timeout-ms 6000 >/dev/null 2>&1

a=$(dom "#/local/topics"); echo "$a" >"$EV/links-topics.html"
check "topic anchor" "$a" "href=\"#/local/topics/$T\""
b=$(dom "#/local/groups"); echo "$b" >"$EV/links-groups.html"
check "group anchor" "$b" "href=\"#/local/groups/$G\""
c=$(dom "#/local/schemas"); echo "$c" >"$EV/links-schemas.html"
check "schema anchor" "$c" "href=\"#/local/schemas/demo-orders-avro-value\""
d=$(dom "#/local/topics/$T"); echo "$d" >"$EV/links-topic-direct.html"
check "topic opens directly" "$d" "class=\"topic active\" href=\"#/local/topics/$T\""
e=$(dom "#/local/groups/$G"); echo "$e" >"$EV/links-group-direct.html"
check "group opens directly" "$e" "class=\"topic active\" href=\"#/local/groups/$G\""
f=$(dom "#/local/schemas/demo-orders-avro-value"); echo "$f" >"$EV/links-schema-direct.html"
check "schema opens directly" "$f" "class=\"topic active\" href=\"#/local/schemas/demo-orders-avro-value\""

curl -s -XDELETE -o /dev/null "$B/p/local/topics/$T?confirm=$T"; curl -s -XDELETE -o /dev/null "$B/p/local/groups/$G?confirm=$G"
exit $fail
