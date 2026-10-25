# Live tail

New messages (from the log end, not history) as server-sent events, optionally filtered.

## How to get to it (user POV)

Web UI: topic → Live. TUI: Messages screen, `Tab`/`s` toggles seek ↔ tail. API: `GET $B/p/local/topics/$T/tail[?text=..&query=..]`.

## Driving it with curl

```bash
( curl -sN -m 6 "$B/p/local/topics/$T/tail" > /tmp/tail.out & ); sleep 2
curl -s -XPOST -H "$J" -d '{"value":{"text":"live-1"}}' $B/p/local/topics/$T/produce
sleep 5; cat /tmp/tail.out
# event: messages
# data: [{"partition":0,"offset":5,"timestamp_ms":...,"key":{"format":"—","preview":"<null>","size":null},"value":{"format":"raw","preview":"live-1","size":6},"header_count":0}]
```

Other events: `dropped` (`{count}`) when the client falls behind, `error` (`{message}`).

## Gotchas

- Messages produced before the connection are not delivered; produce after the consumer is attached (the 2 s sleep).
- The stream stays open; use `-m` so curl exits.
