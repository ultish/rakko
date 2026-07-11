#!/usr/bin/env bash
# Continuously produces test JSON messages to a Kafka topic until killed (Ctrl+C).
# Handy for exercising rakko's tail-mode message browser against a real broker.
#
# Requires kcat (brew install kcat) - a thin CLI wrapper over librdkafka, the same
# library rakko itself uses, so its -X properties map 1:1 onto rakko's
# AuthMode/TLS config (see config.example.toml).
#
# Usage:
#   ./scripts/produce-test-messages.sh
#   BROKER=host:port TOPIC=my-topic ./scripts/produce-test-messages.sh
#
# TLS (type = "tls" in rakko, i.e. custom CA / private CA, no client cert):
#   KCAT_TLS=1 CA_CERT=/path/to/ca.pem ./scripts/produce-test-messages.sh
#
# mTLS (type = "mtls" in rakko, client cert + key):
#   KCAT_TLS=1 CA_CERT=/path/to/ca.pem CLIENT_CERT=/path/to/client.pem \
#     CLIENT_KEY=/path/to/client.key ./scripts/produce-test-messages.sh
#
# Env vars:
#   BROKER      default 192.168.50.148:9093
#   TOPIC       default jxhui-test
#   MIN_DELAY   default 0.5 (seconds between messages, may be fractional)
#   MAX_DELAY   default 3

set -euo pipefail

BROKER="${BROKER:-192.168.50.148:9093}"
TOPIC="${TOPIC:-jxhui-test}"
MIN_DELAY="${MIN_DELAY:-0.5}"
MAX_DELAY="${MAX_DELAY:-3}"

if ! command -v kcat >/dev/null 2>&1; then
    echo "error: kcat not found on PATH. Install it with: brew install kcat" >&2
    exit 1
fi

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

# A small fixed key pool (rather than a unique key per message) so filtering by key
# in rakko's message browser has something meaningful to narrow down.
KEYS=(device-a device-b device-c device-d device-e)
STATUSES=(ok warn error pending)

count=0
trap 'echo; echo "stopped after $count message(s)"; exit 0' INT TERM

echo "Producing to $BROKER topic '$TOPIC' (Ctrl+C to stop) ..."

while true; do
    count=$((count + 1))
    key="${KEYS[$((RANDOM % ${#KEYS[@]}))]}"
    status="${STATUSES[$((RANDOM % ${#STATUSES[@]}))]}"
    ts="$(date +%s)"
    value=$(printf '{"seq":%d,"ts":%s,"key":"%s","status":"%s","value":%d}' \
        "$count" "$ts" "$key" "$status" "$RANDOM")

    if printf '%s' "$value" | kcat "${KCAT_ARGS[@]}" -k "$key"; then
        echo "[$count] key=$key -> $value"
    else
        echo "[$count] send failed (broker unreachable or auth misconfigured?) - retrying after delay" >&2
    fi

    # Fractional random delay between MIN_DELAY and MAX_DELAY seconds. Seeded with
    # $RANDOM+PID so consecutive awk invocations within the same wall-clock second
    # don't all draw the same value.
    delay=$(awk -v min="$MIN_DELAY" -v max="$MAX_DELAY" -v seed="$RANDOM$$" \
        'BEGIN { srand(seed); printf "%.2f", min + rand() * (max - min) }')
    sleep "$delay"
done
