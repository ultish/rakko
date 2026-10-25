# Brokers and Schema Registry

## Sub-features

- Broker list with leader/replica partition counts; per-broker config; ACL list
- Registry: list subjects, versions of a subject, register/check/delete (delete gated by `confirm`), compatibility config
- Avro decode of messages through the registry (see produce-and-replay for the write side)

## How to get to it (user POV)

Web UI: Brokers tab, Schemas tab. TUI: `3 Brokers`, `Enter` for config. API below.

## Driving it with curl

```bash
curl -s $B/p/local/brokers
# {"brokers":[{"host":"127.0.0.1","id":1,"leader_partitions":N,"port":19092,"replica_partitions":N}],"offline":0,"under_replicated":0}
curl -s $B/p/local/brokers/1/configs | head -c 200
# [{"name":"advertised.listeners","value":"PLAINTEXT://kafka:29092,PLAINTEXT_HOST://127.0.0.1:19092"},...]
curl -s $B/p/local/acls                  # []  (the local stack has no authorizer)
curl -s $B/p/local/schemas               # JSON array of subjects; includes "demo-orders-avro-value"
curl -s $B/p/local/schemas/demo-orders-avro-value   # [1]   version numbers
curl -s $B/p/local/topics/demo-orders-avro/avro-subjects
# {"key":[],"value":["demo-orders-avro-value"]}
```

Avro decode read-back: produce the Avro message from produce-and-replay, scan `demo-orders-avro` with `{"text":"verify-k"}`, then `GET $B/p/local/topics/demo-orders-avro/messages/<P>/<O>?part=value` returns `"format":"avro:<id>"` and pretty `text` with `order_id`, `customer`, `amount_cents`, `currency`, `status`.

## Gotchas

- The registry list on a shared dev stack contains test subjects left by `cargo test -- --ignored`; filter by name.
- Do not DELETE `demo-orders-avro-value`; the Avro page depends on it.
