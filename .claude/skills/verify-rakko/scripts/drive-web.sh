#!/usr/bin/env bash
# End-to-end journey against a running rakko-web (see web-up.sh): topic create, produce,
# scan, detail, replay, export -> import, group lag + reset, delete. Asserts each step and
# writes the raw responses to $YAGURA_EVIDENCE (default: ./evidence-rakko). Exit 0 = all passed.
# Usage: drive-web.sh [port]
set -uo pipefail
PORT="${1:-18090}"
B="http://127.0.0.1:$PORT/api"
EV="${YAGURA_EVIDENCE:-./evidence-rakko}"; mkdir -p "$EV"
H='x-rakko-session: 11111111-1111-1111-1111-111111111111'; J='content-type: application/json'
T="verify-$$-$RANDOM"; T2="$T-copy"; G="$T-grp"
fail=0
check() { # name, actual, expected-substring
  if [[ "$2" == *"$3"* ]]; then echo "ok   $1"; else echo "FAIL $1: wanted '$3' in: $2"; fail=1; fi
}
rec() { printf '%s\n' "$2" >"$EV/$1.json"; }

r=$(curl -s -XPOST -H "$J" -d "{\"name\":\"$T\",\"partitions\":2,\"replication_factor\":1}" $B/p/local/topics); rec create-topic "$r"
check "create topic" "$r" "\"name\":\"$T\""
for i in 1 2 3 4; do
  st=new; [ $((i % 2)) = 0 ] && st=paid
  curl -s -XPOST -H "$J" -d "{\"key\":{\"text\":\"k$i\"},\"value\":{\"text\":\"{\\\"id\\\":$i,\\\"status\\\":\\\"$st\\\"}\"}}" $B/p/local/topics/$T/produce >/dev/null
done
sleep 1
r=$(curl -s $B/p/local/topics/$T/partitions); rec partitions "$r"
check "4 messages across 2 partitions" "$(echo "$r" | grep -o '"messages":[0-9]*' | cut -d: -f2 | paste -sd+ - | bc)" 4

s=$(curl -s -XPOST -H "$H" -H "$J" -d '{"query":"value.status = \"paid\""}' $B/p/local/topics/$T/scans); rec scan-create "$s"
ID=$(echo "$s" | sed 's/.*scan_id":"\([^"]*\)".*/\1/')
r=$(curl -s -H "$H" "$B/scans/$ID/matches?limit=10"); rec scan-matches "$r"
check "query matches exactly the 2 paid messages" "$r" '"found":2'
check "scan finished" "$r" '"done":true'
curl -s -H "$H" -XDELETE -o /dev/null $B/scans/$ID

P=$(echo "$r" | sed 's/.*"partition":\([0-9]*\),"offset":\([0-9]*\).*/\1/'); O=$(echo "$r" | sed 's/.*"partition":\([0-9]*\),"offset":\([0-9]*\).*/\2/')
r=$(curl -s "$B/p/local/topics/$T/messages/$P/$O?part=value"); rec detail "$r"
check "detail decodes JSON" "$r" '"format":"json"'
check "detail text is pretty-printed paid message" "$r" '\"status\": \"paid\"'

r=$(curl -s -XPOST $B/p/local/topics/$T/messages/$P/$O/replay); rec replay "$r"
check "replay returns a new offset" "$r" '"offset"'
curl -s "$B/p/local/topics/$T/export" -o "$EV/export.jsonl"
check "export has 5 lines (4 + replay)" "$(wc -l <"$EV/export.jsonl" | tr -d ' ')" 5

curl -s -XPOST -H "$J" -d "{\"name\":\"$T2\",\"partitions\":1,\"replication_factor\":1}" $B/p/local/topics >/dev/null
r=$(curl -s -XPOST --data-binary @"$EV/export.jsonl" $B/p/local/topics/$T2/import); rec import "$r"
check "import sent 5, failed 0" "$r" '"sent":5'
sleep 1
curl -s "$B/p/local/topics/$T2/export" -o "$EV/reexport.jsonl"
a=$(sed 's/"topic":"[^"]*","partition":[0-9]*,"offset":[0-9]*,"timestamp_millis":[0-9]*,//' "$EV/export.jsonl" | sort)
b=$(sed 's/"topic":"[^"]*","partition":[0-9]*,"offset":[0-9]*,"timestamp_millis":[0-9]*,//' "$EV/reexport.jsonl" | sort)
check "key/value/headers bytes identical after import" "$([ "$a" = "$b" ] && echo same)" same

docker exec rakko-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic $T --group $G --from-beginning --timeout-ms 6000 >/dev/null 2>&1
r=$(curl -s $B/p/local/groups/$G); rec group-after-consume "$r"
check "group fully caught up" "$r" '"total_lag":0'
r=$(curl -s -XPOST -H "$J" -d '{"target":{"kind":"earliest"},"confirm":"wrong"}' $B/p/local/groups/$G/reset); rec reset-bad-confirm "$r"
check "reset refused without confirm" "$r" 'confirm must equal the group name'
r=$(curl -s -XPOST -H "$J" -d "{\"target\":{\"kind\":\"earliest\"},\"confirm\":\"$G\"}" $B/p/local/groups/$G/reset); rec reset "$r"
check "reset touched 2 partitions" "$r" '"reset":2'
r=$(curl -s $B/p/local/groups/$G); rec group-after-reset "$r"
check "lag is 5 after reset to earliest" "$r" '"total_lag":5'

curl -s -XDELETE -o /dev/null "$B/p/local/topics/$T?confirm=$T"; curl -s -XDELETE -o /dev/null "$B/p/local/topics/$T2?confirm=$T2"
curl -s -XDELETE -o /dev/null "$B/p/local/groups/$G?confirm=$G"
r=$(curl -s $B/p/local/topics); check "scratch topics deleted" "$([[ "$r" != *"$T"* ]] && echo gone)" gone
echo "evidence in $EV"; exit $fail
