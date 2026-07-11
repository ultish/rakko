#!/usr/bin/env bash
# Sends ONE large test message to a Kafka topic, then exits. Useful for exercising a
# broker's max.message.bytes limit (e.g. after raising it above Kafka's 1MB default).
#
# Requires kcat (brew install kcat) - same underlying librdkafka as rakko, so its -X
# properties map 1:1 onto rakko's AuthMode/TLS config (see config.example.toml).
#
# Usage:
#   ./scripts/send-large-message.sh                    # ~1.5 MiB message
#   MSG_SIZE_BYTES=5000000 ./scripts/send-large-message.sh
#
# TLS (type = "tls" in rakko, i.e. custom CA / private CA, no client cert):
#   KCAT_TLS=1 CA_CERT=/path/to/ca.pem ./scripts/send-large-message.sh
#
# mTLS (type = "mtls" in rakko, client cert + key):
#   KCAT_TLS=1 CA_CERT=/path/to/ca.pem CLIENT_CERT=/path/to/client.pem \
#     CLIENT_KEY=/path/to/client.key ./scripts/send-large-message.sh
#
# Env vars:
#   BROKER          default 192.168.50.148:9093
#   TOPIC           default jxhui-test
#   MSG_SIZE_BYTES  default 1572864 (1.5 MiB) - the actual on-wire message size built
#   MAX_MSG_BYTES   default 20971520 - producer-side message.max.bytes override
#                   (librdkafka caps produced messages at 1,000,000 bytes by
#                   default; this must be >= MSG_SIZE_BYTES, and should match what
#                   your broker/topic's max.message.bytes actually allows)

set -euo pipefail

BROKER="${BROKER:-192.168.50.148:9093}"
TOPIC="${TOPIC:-jxhui-test}"
MSG_SIZE_BYTES="${MSG_SIZE_BYTES:-1572864}"
MAX_MSG_BYTES="${MAX_MSG_BYTES:-20971520}"

if ! command -v kcat >/dev/null 2>&1; then
    echo "error: kcat not found on PATH. Install it with: brew install kcat" >&2
    exit 1
fi

if (( MSG_SIZE_BYTES > MAX_MSG_BYTES )); then
    echo "error: MSG_SIZE_BYTES ($MSG_SIZE_BYTES) exceeds MAX_MSG_BYTES ($MAX_MSG_BYTES)" >&2
    echo "  MAX_MSG_BYTES is the producer-side message.max.bytes override - raise it" >&2
    echo "  (but no higher than what the broker/topic actually allows) or shrink" >&2
    echo "  MSG_SIZE_BYTES." >&2
    exit 1
fi

# Build a JSON envelope padded with 'x' characters to land on an exact byte count,
# rather than a raw random blob - keeps the message readable/decodable in rakko's
# message browser (it'll auto-detect as JSON) instead of falling back to raw/hex.
now_ts="$(date +%s)"
prefix="{\"kind\":\"large-message-test\",\"sent_at\":${now_ts},\"target_bytes\":${MSG_SIZE_BYTES},\"payload\":\""
suffix="\"}"
prefix_len=${#prefix}
suffix_len=${#suffix}
padding_len=$(( MSG_SIZE_BYTES - prefix_len - suffix_len ))

if (( padding_len < 0 )); then
    echo "error: MSG_SIZE_BYTES ($MSG_SIZE_BYTES) is too small to fit the JSON envelope" \
        "(need at least $((prefix_len + suffix_len)) bytes)" >&2
    exit 1
fi

tmpfile="$(mktemp "${TMPDIR:-/tmp}/rakko-large-msg.XXXXXX")"
trap 'rm -f "$tmpfile"' EXIT

{
    printf '%s' "$prefix"
    head -c "$padding_len" /dev/zero | tr '\0' 'x'
    printf '%s' "$suffix"
} > "$tmpfile"

actual_bytes="$(wc -c < "$tmpfile" | tr -d ' ')"

KCAT_ARGS=(-b "$BROKER" -t "$TOPIC" -P -k "large-msg-test" -X message.max.bytes="$MAX_MSG_BYTES")

if [[ "${KCAT_TLS:-0}" == "1" ]]; then
    KCAT_ARGS+=(-X security.protocol=SSL)
    if [[ -n "${CA_CERT:-}" ]]; then
        KCAT_ARGS+=(-X ssl.ca.location="$CA_CERT")
    fi
    if [[ -n "${CLIENT_CERT:-}" && -n "${CLIENT_KEY:-}" ]]; then
        KCAT_ARGS+=(-X ssl.certificate.location="$CLIENT_CERT" -X ssl.key.location="$CLIENT_KEY")
    fi
fi

echo "Sending a ${actual_bytes}-byte message to $BROKER topic '$TOPIC' (message.max.bytes=$MAX_MSG_BYTES) ..."

# A positional file argument (no -l) sends the ENTIRE file as ONE message, unlike
# stdin/-l which kcat splits on newlines - important since a large payload could
# otherwise get silently chopped into many small messages.
if kcat "${KCAT_ARGS[@]}" "$tmpfile"; then
    echo "Sent OK: ${actual_bytes} bytes, key=large-msg-test"
else
    echo "Send FAILED - check that the broker/topic's max.message.bytes actually" \
        "allows ${actual_bytes} bytes (yours is reportedly 20971520)" >&2
    exit 1
fi
