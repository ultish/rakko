# Consumer groups

## Sub-features

- List groups with state and member count; detail with per-partition committed / high / lag
- Groups consuming a topic (`/topics/<t>/consumers`)
- Offset reset (`earliest`, `latest`, `offset`, `timestamp`) gated by `confirm`; works when the group has no active members
- Delete group (gated by `confirm`); lag history

## How to get to it (user POV)

Web UI: Groups tab. TUI: `2 Groups`, `Enter` detail, `z` reset. API below.

## Driving it with curl

Create a group by consuming the scratch topic (a topic with 5 messages here):

```bash
G=$T-grp
docker exec rakko-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic $T --group $G --from-beginning --timeout-ms 6000
# ends with "Processed a total of 5 messages" (a TimeoutException line above it is the normal exit)
curl -s $B/p/local/groups/$G
# {"name":"<G>","state":"Empty","total_lag":0,"members":[],"lags":[{"committed":1,"high":1,"lag":0,"low":0,"partition":0,"topic":"<T>"},...]}
curl -s -XPOST -H "$J" -d '{"target":{"kind":"earliest"},"confirm":"wrong"}' $B/p/local/groups/$G/reset
# {"error":"confirm must equal the group name"}  (HTTP 400)
curl -s -XPOST -H "$J" -d "{\"target\":{\"kind\":\"earliest\"},\"confirm\":\"$G\"}" $B/p/local/groups/$G/reset
# {"reset":2}      number of partitions reset
curl -s $B/p/local/groups/$G                 # total_lag now equals the number of messages; committed 0
curl -s $B/p/local/topics/$T/consumers       # [{"name":"<G>","state":"Empty","total_lag":5,"members":0}]
curl -s -XDELETE "$B/p/local/groups/$G?confirm=$G"
```

Targets: `{"kind":"latest"}`, `{"kind":"offset","value":N}`, `{"kind":"timestamp","value":ms}`; optional `"partitions":[{"topic":..,"partition":..}]`.

## Gotchas

- Reset on a group with live members is unreliable (the UI warns); verify with `"state":"Empty"`.
- The body field is `kind`, not a bare variant name; `{"target":{"earliest":null}}` fails with `missing field \`kind\``.
- The seeded `demo-consumer` group is shared; do not reset it.
