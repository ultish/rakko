#!/usr/bin/env bash
# Continuously produces Confluent-wire-format Avro messages to a Kafka topic until
# killed (Ctrl+C). Sibling to produce-test-messages.sh, but for an Avro topic backed
# by a Schema Registry - exercises rakko's Avro auto-detect + decode path.
#
# kcat itself can only DEserialize Avro (`-s value=avro` errors with "only available
# in the consumer"), so this script builds the wire format by hand: fetch the
# subject's schema once at startup, Avro-encode each record with Python (fastavro),
# prepend the Confluent magic byte (0x00) + 4-byte big-endian schema ID, then hand
# kcat the raw bytes as a whole file (same "positional file arg = one message,
# untouched by embedded bytes" trick as send-large-message.sh).
#
# Requires: kcat (brew install kcat), python3 with fastavro (pip3 install fastavro).
#
# Usage:
#   ./scripts/produce-avro-test-messages.sh
#   BROKER=host:port TOPIC=my-avro-topic SCHEMA_REGISTRY_URL=http://host:8081 \
#     ./scripts/produce-avro-test-messages.sh
#
# NOTE: the record shape below (order_id/customer/amount_cents/currency/status) is
# hardcoded to match the OrderValue schema currently registered for jxhui-avro. If
# that schema changes, update build_record() below to match its fields.
#
# TLS for the Kafka broker (type = "tls"/"mtls" in rakko - same as the other scripts):
#   KCAT_TLS=1 CA_CERT=/path/to/ca.pem [CLIENT_CERT=... CLIENT_KEY=...] \
#     ./scripts/produce-avro-test-messages.sh
#
# TLS for the Schema Registry itself (only if it's HTTPS with a private CA):
#   SCHEMA_REGISTRY_CA=/path/to/ca.pem ./scripts/produce-avro-test-messages.sh
#
# Env vars:
#   BROKER                default 192.168.50.148:9093
#   TOPIC                 default jxhui-avro
#   SCHEMA_REGISTRY_URL   default http://192.168.50.148:8081
#   SUBJECT               default ${TOPIC}-value (TopicNameStrategy)
#   MIN_DELAY / MAX_DELAY default 0.5 / 3 (seconds between messages)

set -euo pipefail

BROKER="${BROKER:-192.168.50.148:9093}"
TOPIC="${TOPIC:-jxhui-avro}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://192.168.50.148:8081}"
SUBJECT="${SUBJECT:-${TOPIC}-value}"
MIN_DELAY="${MIN_DELAY:-0.5}"
MAX_DELAY="${MAX_DELAY:-3}"

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

echo "Fetching schema for subject '$SUBJECT' from $SCHEMA_REGISTRY_URL ..."
# The "${arr[@]+"${arr[@]}"}" form (not plain "${arr[@]}") is required because macOS's
# default /bin/bash is 3.2, which treats expanding an empty array under `set -u` as an
# unbound-variable error - fixed in bash 4.4+, but can't assume that's what's on PATH.
schema_response="$(curl -sf "${CURL_ARGS[@]+"${CURL_ARGS[@]}"}" "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT}/versions/latest")" \
    || { echo "error: failed to fetch schema (is the subject name right? try SUBJECT=...)" >&2; exit 1; }

SCHEMA_ID="$(printf '%s' "$schema_response" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
SCHEMA_JSON="$(printf '%s' "$schema_response" | python3 -c 'import json,sys; print(json.dumps(json.loads(json.load(sys.stdin)["schema"])))')"
echo "Using schema id $SCHEMA_ID for subject '$SUBJECT'"

# One reusable Python helper: encode a JSON record against $SCHEMA_JSON, prepend the
# Confluent wire-format header, write raw bytes to stdout. Schema is fetched once
# above (not per-message) to avoid a registry round-trip on every send.
ENCODER='
import sys, json, struct
from io import BytesIO
import fastavro

schema = json.loads(sys.argv[1])
record = json.loads(sys.argv[2])
schema_id = int(sys.argv[3])

parsed = fastavro.parse_schema(schema)
buf = BytesIO()
buf.write(b"\x00")
buf.write(struct.pack(">I", schema_id))
fastavro.schemaless_writer(buf, parsed, record)
sys.stdout.buffer.write(buf.getvalue())
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

CUSTOMERS=(alice bob carol dave erin)
CURRENCIES=(USD EUR GBP)
STATUSES=(PENDING PAID SHIPPED CANCELLED)

# Matches the OrderValue schema's fields exactly - see the NOTE at the top of this
# file if the registered schema has since changed.
build_record() {
    local order_id customer currency status amount_cents
    order_id="ord-$(date +%s)-$RANDOM"
    customer="${CUSTOMERS[$((RANDOM % ${#CUSTOMERS[@]}))]}"
    currency="${CURRENCIES[$((RANDOM % ${#CURRENCIES[@]}))]}"
    status="${STATUSES[$((RANDOM % ${#STATUSES[@]}))]}"
    amount_cents=$((RANDOM * 10 + 100))
    printf '{"order_id":"%s","customer":"%s","amount_cents":%d,"currency":"%s","status":"%s"}' \
        "$order_id" "$customer" "$amount_cents" "$currency" "$status"
}

tmpfile="$(mktemp "${TMPDIR:-/tmp}/rakko-avro-msg.XXXXXX")"
trap 'rm -f "$tmpfile"; echo; echo "stopped after $count message(s)"; exit 0' INT TERM
trap 'rm -f "$tmpfile"' EXIT

count=0
echo "Producing Avro to $BROKER topic '$TOPIC' (Ctrl+C to stop) ..."

while true; do
    count=$((count + 1))
    record_json="$(build_record)"
    key="$(printf '%s' "$record_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["order_id"])')"

    python3 -c "$ENCODER" "$SCHEMA_JSON" "$record_json" "$SCHEMA_ID" > "$tmpfile"

    if kcat "${KCAT_ARGS[@]}" -k "$key" "$tmpfile"; then
        echo "[$count] key=$key -> $record_json"
    else
        echo "[$count] send failed - retrying after delay" >&2
    fi

    delay=$(awk -v min="$MIN_DELAY" -v max="$MAX_DELAY" -v seed="$RANDOM$$" \
        'BEGIN { srand(seed); printf "%.2f", min + rand() * (max - min) }')
    sleep "$delay"
done
