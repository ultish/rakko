#!/usr/bin/env bash
# Registers a deeply-nested (6 levels) Avro schema with arrays at multiple levels,
# then produces COUNT Confluent-wire-format Avro messages against it and exits.
# Sibling to produce-avro-test-messages.sh, but self-contained: it registers its own
# schema instead of assuming one is already in the registry, and sends a fixed batch
# instead of running until Ctrl+C - handy for quickly repopulating a topic after a
# broker/registry wipe.
#
# Schema shape (OrderEvent, namespace com.rakko.test):
#   OrderEvent                              (1)
#     .customer                             (2)  record
#       .addresses[]                        (3)  array<record>
#         .contacts[]                       (4)  array<record>
#           .metadata                       (5)  record
#             .tags[]                       (6)  array<record>  <- 6 levels deep
#     .items[]                              (2)  array<record>, sibling array
#       .modifiers[]                        (3)  array<string>
#
# kcat itself can only DEserialize Avro, so this script builds the wire format by
# hand: register the schema, Avro-encode each record with Python (fastavro), prepend
# the Confluent magic byte (0x00) + 4-byte big-endian schema ID, then hand kcat the
# raw bytes as a whole file (same trick as produce-avro-test-messages.sh).
#
# Requires: kcat (brew install kcat), python3 with fastavro (pip3 install fastavro).
#
# Usage:
#   ./scripts/produce-nested-avro-test-messages.sh
#   BROKER=host:port TOPIC=my-topic COUNT=500 \
#     ./scripts/produce-nested-avro-test-messages.sh
#
# TLS for the Kafka broker (type = "tls"/"mtls" in rakko - same as the other scripts):
#   KCAT_TLS=1 CA_CERT=/path/to/ca.pem [CLIENT_CERT=... CLIENT_KEY=...] \
#     ./scripts/produce-nested-avro-test-messages.sh
#
# TLS for the Schema Registry itself (only if it's HTTPS with a private CA):
#   SCHEMA_REGISTRY_CA=/path/to/ca.pem ./scripts/produce-nested-avro-test-messages.sh
#
# Env vars:
#   BROKER                default 192.168.50.148:9093
#   TOPIC                 default jxhui-nested-avro
#   SCHEMA_REGISTRY_URL   default http://192.168.50.148:8081
#   SUBJECT               default ${TOPIC}-value (TopicNameStrategy)
#   COUNT                 default 300 (messages to send, then exit)
#   MIN_DELAY / MAX_DELAY default 0 / 0.05 (seconds between messages)

set -euo pipefail

BROKER="${BROKER:-192.168.50.148:9093}"
TOPIC="${TOPIC:-jxhui-nested-avro}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://192.168.50.148:8081}"
SUBJECT="${SUBJECT:-${TOPIC}-value}"
COUNT="${COUNT:-300}"
MIN_DELAY="${MIN_DELAY:-0}"
MAX_DELAY="${MAX_DELAY:-0.05}"

for bin in kcat python3 curl; do
    if ! command -v "$bin" >/dev/null 2>&1; then
        echo "error: $bin not found on PATH" >&2
        exit 1
    fi
done
if ! python3 -c "import fastavro" >/dev/null 2>&1; then
    echo "error: python3 module 'fastavro' not found. Install it with: pip3 install fastavro" >&2
    exit 1
fi

CURL_ARGS=()
if [[ -n "${SCHEMA_REGISTRY_CA:-}" ]]; then
    CURL_ARGS+=(--cacert "$SCHEMA_REGISTRY_CA")
fi

SCHEMA_JSON='{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "com.rakko.test",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "created_at", "type": "long"},
    {"name": "customer", "type": {
      "type": "record",
      "name": "Customer",
      "fields": [
        {"name": "customer_id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "tier", "type": "string"},
        {"name": "addresses", "type": {"type": "array", "items": {
          "type": "record",
          "name": "Address",
          "fields": [
            {"name": "line1", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "country", "type": "string"},
            {"name": "contacts", "type": {"type": "array", "items": {
              "type": "record",
              "name": "Contact",
              "fields": [
                {"name": "type", "type": "string"},
                {"name": "value", "type": "string"},
                {"name": "metadata", "type": {
                  "type": "record",
                  "name": "ContactMetadata",
                  "fields": [
                    {"name": "verified", "type": "boolean"},
                    {"name": "source", "type": "string"},
                    {"name": "tags", "type": {"type": "array", "items": {
                      "type": "record",
                      "name": "Tag",
                      "fields": [
                        {"name": "key", "type": "string"},
                        {"name": "value", "type": "string"}
                      ]
                    }}}
                  ]
                }}
              ]
            }}}
          ]
        }}}
      ]
    }},
    {"name": "items", "type": {"type": "array", "items": {
      "type": "record",
      "name": "OrderItem",
      "fields": [
        {"name": "sku", "type": "string"},
        {"name": "qty", "type": "int"},
        {"name": "unit_price_cents", "type": "long"},
        {"name": "modifiers", "type": {"type": "array", "items": "string"}}
      ]
    }}}
  ]
}'

echo "Registering schema for subject '$SUBJECT' at $SCHEMA_REGISTRY_URL ..."
register_payload="$(python3 -c 'import json,sys; print(json.dumps({"schema": json.dumps(json.loads(sys.argv[1]))}))' "$SCHEMA_JSON")"
register_response="$(curl -sf "${CURL_ARGS[@]+"${CURL_ARGS[@]}"}" \
    -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d "$register_payload" \
    "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT}/versions")" \
    || { echo "error: failed to register schema" >&2; exit 1; }
SCHEMA_ID="$(printf '%s' "$register_response" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
echo "Registered schema id $SCHEMA_ID for subject '$SUBJECT'"

# One reusable Python helper: build a randomized nested record matching SCHEMA_JSON,
# encode it against the schema, prepend the Confluent wire-format header, write raw
# bytes to stdout. Also prints the order_id (used as the kcat key) to stderr so the
# caller doesn't need a second Python invocation per message.
ENCODER='
import sys, json, struct, random
from io import BytesIO
import fastavro

schema = json.loads(sys.argv[1])
schema_id = int(sys.argv[2])
seq = int(sys.argv[3])

CUSTOMERS = ["alice", "bob", "carol", "dave", "erin"]
TIERS = ["bronze", "silver", "gold", "platinum"]
CITIES = [("221B Baker St", "London", "UK"), ("350 5th Ave", "New York", "US"),
          ("1 Rue de Rivoli", "Paris", "FR"), ("Unter den Linden 1", "Berlin", "DE")]
CONTACT_TYPES = ["email", "phone", "sms"]
TAG_KEYS = ["source", "campaign", "priority", "locale"]
SKUS = ["sku-widget", "sku-gadget", "sku-gizmo", "sku-doohickey"]
MODIFIERS = ["gift-wrap", "express", "engraved", "recurring"]

def rand_tag():
    return {"key": random.choice(TAG_KEYS), "value": f"v{random.randint(1, 999)}"}

def rand_metadata():
    return {
        "verified": random.random() > 0.3,
        "source": random.choice(["web", "mobile", "call-center"]),
        "tags": [rand_tag() for _ in range(random.randint(1, 3))],
    }

def rand_contact():
    return {
        "type": random.choice(CONTACT_TYPES),
        "value": f"contact-{random.randint(1000, 9999)}",
        "metadata": rand_metadata(),
    }

def rand_address():
    line1, city, country = random.choice(CITIES)
    return {
        "line1": line1,
        "city": city,
        "country": country,
        "contacts": [rand_contact() for _ in range(random.randint(1, 2))],
    }

def rand_item():
    return {
        "sku": random.choice(SKUS),
        "qty": random.randint(1, 5),
        "unit_price_cents": random.randint(500, 20000),
        "modifiers": random.sample(MODIFIERS, k=random.randint(0, 2)),
    }

order_id = f"ord-{seq}-{random.randint(1000, 9999)}"
record = {
    "order_id": order_id,
    "created_at": 1700000000000 + seq * 1000,
    "customer": {
        "customer_id": f"cust-{random.randint(1, 500)}",
        "name": random.choice(CUSTOMERS),
        "tier": random.choice(TIERS),
        "addresses": [rand_address() for _ in range(random.randint(1, 2))],
    },
    "items": [rand_item() for _ in range(random.randint(1, 3))],
}

parsed = fastavro.parse_schema(schema)
buf = BytesIO()
buf.write(b"\x00")
buf.write(struct.pack(">I", schema_id))
fastavro.schemaless_writer(buf, parsed, record)
sys.stdout.buffer.write(buf.getvalue())
sys.stderr.write(order_id)
'

KCAT_ARGS=(-b "$BROKER" -t "$TOPIC" -P)
if [[ "${KCAT_TLS:-0}" == "1" ]]; then
    KCAT_ARGS+=(-X security.protocol=SSL)
    if [[ -n "${CA_CERT:-}" ]]; then
        KCAT_ARGS+=(-X ssl.ca.location="$CA_CERT")
    fi
    if [[ -n "${CLIENT_CERT:-}" && -n "${CLIENT_KEY:-}" ]]; then
        KCAT_ARGS+=(-X ssl.certificate.location="$CLIENT_CERT" -X ssl.key.location="$CLIENT_KEY")
    fi
fi

tmpfile="$(mktemp "${TMPDIR:-/tmp}/rakko-nested-avro-msg.XXXXXX")"
trap 'rm -f "$tmpfile"' EXIT

echo "Producing $COUNT nested Avro message(s) to $BROKER topic '$TOPIC' ..."
for ((i = 1; i <= COUNT; i++)); do
    # Swap-redirect idiom: `2>&1` first duplicates stderr onto the current stdout
    # target (the command-substitution capture), then `1>"$tmpfile"` repoints stdout
    # to the file - so $key captures stderr (the order_id) while the raw Avro bytes
    # land in $tmpfile, without a second Python invocation per message.
    key="$(python3 -c "$ENCODER" "$SCHEMA_JSON" "$SCHEMA_ID" "$i" 2>&1 1>"$tmpfile")"

    if kcat "${KCAT_ARGS[@]}" -k "$key" "$tmpfile"; then
        echo "[$i/$COUNT] key=$key"
    else
        echo "[$i/$COUNT] send failed - retrying after delay" >&2
    fi

    delay=$(awk -v min="$MIN_DELAY" -v max="$MAX_DELAY" -v seed="$RANDOM$$" \
        'BEGIN { srand(seed); printf "%.3f", min + rand() * (max - min) }')
    sleep "$delay"
done

echo "done: sent $COUNT message(s) to '$TOPIC'"
