# Topics and message scan

Browse a whole topic newest-first, filtered by text or a structured query, then open one message.

## Sub-features

- Topic list with partitions, replication, compression, message count
- Create / delete topic (delete needs `confirm`)
- Scan: `text` (case-insensitive substring over key and value) and `query` (`value.status = "paid" AND value.customer.tier = "gold"`)
- Message detail, decoded (JSON pretty-printed, Avro via registry), served in ranges
- Filter autocomplete (`/complete`)

## How to get to it (user POV)

Web UI Topics tab → open a topic → filter box. API: `$B/p/<profile>/topics...`. Profile here is `local`.

## Driving it with curl

```bash
curl -s -XPOST -H "$J" -d "{\"name\":\"$T\",\"partitions\":2,\"replication_factor\":1}" $B/p/local/topics
# {"name":"verify-<pid>"}
for i in 1 2 3 4; do
  st=new; [ $((i%2)) = 0 ] && st=paid
  curl -s -XPOST -H "$J" -d "{\"key\":{\"text\":\"k$i\"},\"value\":{\"text\":\"{\\\"id\\\":$i,\\\"status\\\":\\\"$st\\\"}\"}}" $B/p/local/topics/$T/produce
done
# {"offset":0,"partition":1} ... one line per message; partition depends on the key hash
curl -s $B/p/local/topics/$T/partitions
# [{"partition":0,"low":0,"high":1,"messages":1},{"partition":1,"low":0,"high":3,"messages":3}]   (sums to 4)
S=$(curl -s -XPOST -H "$H" -H "$J" -d '{"query":"value.status = \"paid\""}' $B/p/local/topics/$T/scans)
# {"scan_id":"<uuid>","total_offsets":4}
ID=$(echo $S | sed 's/.*scan_id":"\([^"]*\)".*/\1/')
curl -s -H "$H" "$B/scans/$ID/matches?limit=10"
# {"matches":[{"partition":..,"offset":..,"key":{"format":"raw","preview":"k4","size":2},
#   "value":{"format":"json","preview":"{\"id\":4,\"status\":\"paid\"}","size":24},...}, ...],
#  "found":2,"scanned_offsets":4,"total_offsets":4,"done":true,"error":null,"skipped_undecodable":0}
curl -s -H "$H" -XDELETE -w '%{http_code}\n' $B/scans/$ID           # 200
curl -s "$B/p/local/topics/$T/messages/1/1?part=value"                # part=key|value is required
# {"partition":1,"offset":1,...,"part":"value","present":true,"format":"json","size":24,
#  "text":"{\n  \"id\": 2,\n  \"status\": \"paid\"\n}","start":0,"end":33,"total":33}
curl -s -XPOST -H "$J" -d '{"input":"val"}' $B/p/local/topics/$T/complete
# {"candidates":["value"],"prefix":""}
curl -s -XDELETE "$B/p/local/topics/$T?confirm=wrong"                 # {"error":"confirm must equal the topic name"}  (HTTP 400)
curl -s -XDELETE "$B/p/local/topics/$T?confirm=$T"                    # {"deleted":true}   topic gone from the list
```

`found` is the proof: 2 of 4 match. `done:true` means the scan reached a definite end (poll `matches` with `from=` while `done:false` on large topics). Use `{"text":"k2"}` for a plain substring scan.

## Gotchas

- Seeded `demo-*` topics may be empty (retention); use your own topic.
- A scan on a missing session header is a 4xx; any UUID works.
- Message detail without `?part=` fails with `Failed to deserialize query string: missing field \`part\``.
