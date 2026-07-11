#!/usr/bin/env bash
# Consumes from a Kafka topic as a real, committing consumer group, until killed
# (Ctrl+C). Uses a fixed group id across runs so you can:
#   1. start it, let it catch up
#   2. kill it (Ctrl+C) while scripts/produce-test-messages.sh keeps producing
#   3. watch lag build up for GROUP in rakko's consumer-group screen
#   4. start this script again - it resumes from the group's last committed
#      offset (kcat -G auto-commits periodically), lag drains back down
#
# Requires kcat (brew install kcat) - same underlying librdkafka as rakko, so its
# -X properties map 1:1 onto rakko's AuthMode/TLS config (see config.example.toml).
#
# Usage:
#   ./scripts/consume-test-messages.sh
#   BROKER=host:port TOPIC=my-topic GROUP=my-group ./scripts/consume-test-messages.sh
#
# TLS (type = "tls" in rakko, i.e. custom CA / private CA, no client cert):
#   KCAT_TLS=1 CA_CERT=/path/to/ca.pem ./scripts/consume-test-messages.sh
#
# mTLS (type = "mtls" in rakko, client cert + key):
#   KCAT_TLS=1 CA_CERT=/path/to/ca.pem CLIENT_CERT=/path/to/client.pem \
#     CLIENT_KEY=/path/to/client.key ./scripts/consume-test-messages.sh
#
# Env vars:
#   BROKER             default 192.168.50.148:9093
#   TOPIC               default jxhui-test
#   GROUP                default jxhui-test-consumer (keep this the SAME across
#                        runs - a different value starts a brand new group with
#                        no lag history instead of resuming)
#   AUTO_OFFSET_RESET     default earliest (only applies the very first time this
#                        group id is ever used - after that, resumption is always
#                        from the last committed offset, regardless of this value)

set -euo pipefail

BROKER="${BROKER:-192.168.50.148:9093}"
TOPIC="${TOPIC:-jxhui-test}"
GROUP="${GROUP:-jxhui-test-consumer}"
AUTO_OFFSET_RESET="${AUTO_OFFSET_RESET:-earliest}"

if ! command -v kcat >/dev/null 2>&1; then
    echo "error: kcat not found on PATH. Install it with: brew install kcat" >&2
    exit 1
fi

KCAT_ARGS=(-b "$BROKER" -G "$GROUP" -u -f 'partition %p @ offset %o  key=%k  %s\n')
KCAT_ARGS+=(-X auto.offset.reset="$AUTO_OFFSET_RESET")

if [[ "${KCAT_TLS:-0}" == "1" ]]; then
    KCAT_ARGS+=(-X security.protocol=SSL)
    if [[ -n "${CA_CERT:-}" ]]; then
        KCAT_ARGS+=(-X ssl.ca.location="$CA_CERT")
    fi
    if [[ -n "${CLIENT_CERT:-}" && -n "${CLIENT_KEY:-}" ]]; then
        KCAT_ARGS+=(-X ssl.certificate.location="$CLIENT_CERT" -X ssl.key.location="$CLIENT_KEY")
    fi
fi

echo "Consuming from $BROKER topic '$TOPIC' as group '$GROUP' (Ctrl+C to stop) ..."
echo "(offsets auto-commit as you go, so killing and re-running this script resumes"
echo " from where it left off - that's what makes lag drain back down on restart)"
echo

exec kcat "${KCAT_ARGS[@]}" "$TOPIC"
