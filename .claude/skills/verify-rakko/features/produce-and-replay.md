# Produce and replay

## Sub-features

- Produce `text` or `base64` key/value, headers, null value
- Produce Avro: JSON encoded with the latest schema of a subject, in Confluent wire format
- Replay: re-sends a stored message's exact raw bytes to the same topic

## How to get to it (user POV)

Web UI: open a message → Replay / produce box. TUI: Messages screen, `w` produce, `y` replay. API below.

## Driving it with curl

```bash
curl -s -XPOST -H "$J" -d '{"key":{"text":"k1"},"value":{"text":"{\"id\":1}"}}' $B/p/local/topics/$T/produce
# {"offset":N,"partition":P}
curl -s -XPOST -H "$J" -d '{}' $B/p/local/topics/$T/produce
# {"error":"a message needs a key or a value"}  (HTTP 400)
curl -s -XPOST $B/p/local/topics/$T/messages/<P>/<N>/replay
# {"offset":N+k,"partition":P'}   a NEW offset; the bytes equal the original
# Avro (needs subject demo-orders-avro-value in the registry):
curl -s -XPOST -H "$J" -d '{"key":{"text":"verify-k"},"value":{"avro":{"subject":"demo-orders-avro-value","json":"{\"order_id\":\"v-1\",\"customer\":\"zed\",\"amount_cents\":5,\"currency\":\"USD\",\"status\":\"PAID\"}"}}}' $B/p/local/topics/demo-orders-avro/produce
# {"offset":N,"partition":P}
```

Read-back for Avro: scan the topic with `{"text":"verify-k"}`; the value shows `"format":"avro:<schema id>"` and a decoded JSON preview. Prove replay byte-identity with an export: the `key_b64`/`value_b64` of the original and the replayed offset match (`drive-web.sh` replays then counts 5 lines in the export).

## Gotchas

- Avro produce writes to `demo-orders-avro`, a shared topic: one message is left behind (messages cannot be removed individually; leave it, it is harmless).
- Replay always targets the same topic and the partition the key hashes to, which may differ from the source partition for keyless messages.
