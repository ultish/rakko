# Export and import

## Sub-features

- Export a topic (all, or filtered by `text` / `query`, optionally `limit`, time or offset range) as JSONL with raw base64 key/value/headers
- Import an exported JSONL file into a topic, byte for byte; bad lines are reported, not fatal

## How to get to it (user POV)

Web UI: topic → Export / Import. TUI: Messages screen `x`/`X` export, `i` import. API below.

## Driving it with curl

```bash
curl -s "$B/p/local/topics/$T/export?query=value.status%20%3D%20%22paid%22"
# one JSON object per line, newest-first, e.g.
# {"topic":"verify-1","partition":0,"offset":0,"timestamp_millis":1792890698023,"key_b64":"azQ=","value_b64":"eyJpZCI6NCwic3RhdHVzIjoicGFpZCJ9","headers":[]}
curl -s "$B/p/local/topics/$T/export" -o /tmp/exp.jsonl       # content-type application/x-ndjson, attachment filename <topic>.jsonl
curl -s -XPOST -H "$J" -d "{\"name\":\"$T-copy\",\"partitions\":1,\"replication_factor\":1}" $B/p/local/topics
curl -s -XPOST --data-binary @/tmp/exp.jsonl $B/p/local/topics/$T-copy/import
# {"errors":[],"failed":0,"sent":<lines in file>}
curl -s "$B/p/local/topics/$T-copy/export"    # same key_b64/value_b64/headers, new offsets
```

Proof: `sent` equals the line count, and the re-export matches the original after stripping `topic/partition/offset/timestamp_millis` (done by `drive-web.sh`).

## Gotchas

- Export walks the whole topic; run it on small scratch topics.
- Imported records get new offsets and timestamps; only key, value and headers are preserved.
