#!/usr/bin/env bash
# One command for a working rakko-web demo: starts the docker-compose Kafka + Schema
# Registry, creates and seeds demo topics (only the ones that are empty, so re-running
# never piles up data), then builds and runs rakko-web on http://127.0.0.1:8080.
#
# Needs: docker, curl, cargo, npm (npm only if the UI isn't built yet). No kcat/python.
# Ctrl+C stops rakko-web; the containers keep running (`docker compose down` to stop them).
#
#   ./scripts/dev-up.sh
#   PORT=9000 ./scripts/dev-up.sh
#   RESET=1 ./scripts/dev-up.sh     # wipe the compose volumes/data first

set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${PORT:-8080}"
REGISTRY="http://127.0.0.1:18081"
KAFKA_EXEC=(docker exec -i rakko-kafka)
BS=localhost:29092

for bin in docker curl cargo; do
    command -v "$bin" >/dev/null || { echo "error: $bin not found on PATH" >&2; exit 1; }
done

if [[ "${RESET:-0}" == "1" ]]; then
    docker compose down -v
fi
docker compose up -d

echo "Waiting for Kafka and Schema Registry ..."
for _ in $(seq 1 90); do
    "${KAFKA_EXEC[@]}" kafka-topics --bootstrap-server "$BS" --list >/dev/null 2>&1 \
        && curl -sf "$REGISTRY/subjects" >/dev/null && break
    sleep 2
done
curl -sf "$REGISTRY/subjects" >/dev/null || { echo "error: stack did not come up" >&2; exit 1; }

# Number of messages currently in a topic (0 if it doesn't exist yet).
topic_count() {
    "${KAFKA_EXEC[@]}" bash -c "
        e=\$(kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server $BS --topic $1 --time -1 2>/dev/null | awk -F: '{s+=\$3} END{print s+0}')
        b=\$(kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server $BS --topic $1 --time -2 2>/dev/null | awk -F: '{s+=\$3} END{print s+0}')
        echo \$((e-b))" </dev/null
}

for t in demo-events demo-orders-avro demo-large; do
    "${KAFKA_EXEC[@]}" kafka-topics --bootstrap-server "$BS" --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 1 >/dev/null
done

if [[ "$(topic_count demo-events)" == "0" ]]; then
    echo "Seeding demo-events (600 JSON messages) ..."
    awk 'BEGIN{
        split("created paid shipped cancelled refunded", st, " ");
        split("alice bob carol dave erin", cu, " ");
        srand(7);
        for (i = 1; i <= 600; i++) {
            u = cu[int(rand()*5)+1];
            printf "%s|{\"id\":%d,\"user\":\"%s\",\"status\":\"%s\",\"amount\":%.2f,\"tags\":[\"demo\",\"t%d\"],\"customer\":{\"tier\":\"%s\"}}\n", \
                u, i, u, st[int(rand()*5)+1], rand()*500, i%7, (i%3==0 ? "gold" : "basic")
        }}' | "${KAFKA_EXEC[@]}" kafka-console-producer --bootstrap-server "$BS" --topic demo-events \
            --property parse.key=true --property key.separator='|' >/dev/null
fi

if [[ "$(topic_count demo-large)" == "0" ]]; then
    echo "Seeding demo-large (a few ~600 KiB messages) ..."
    for n in 1 2 3; do
        head -c 450000 /dev/urandom | base64 | tr -d '\n' | sed "s/^/{\"n\":$n,\"blob\":\"/;s/\$/\"}/" \
            | "${KAFKA_EXEC[@]}" kafka-console-producer --bootstrap-server "$BS" --topic demo-large \
                --producer-property max.request.size=20971520 >/dev/null
    done
fi

SCHEMA='{"type":"record","name":"Order","namespace":"demo","fields":[{"name":"order_id","type":"string"},{"name":"customer","type":"string"},{"name":"amount_cents","type":"long"},{"name":"currency","type":"string"},{"name":"status","type":{"type":"enum","name":"Status","symbols":["PENDING","PAID","SHIPPED","CANCELLED"]}}]}'
if ! curl -sf "$REGISTRY/subjects/demo-orders-avro-value/versions/latest" >/dev/null; then
    echo "Registering schema demo-orders-avro-value ..."
    python_free_body="{\"schema\":\"$(printf '%s' "$SCHEMA" | sed 's/"/\\"/g')\"}"
    curl -sf -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
        -d "$python_free_body" "$REGISTRY/subjects/demo-orders-avro-value/versions" >/dev/null
fi

# The web UI and server.
if [[ ! -f crates/rakko-web/web/dist/index.html ]]; then
    command -v npm >/dev/null || { echo "error: npm needed to build the UI" >&2; exit 1; }
    npm --prefix crates/rakko-web/web ci
    npm --prefix crates/rakko-web/web run build
fi
cargo build --release -p rakko-web

CONF_DIR="$(mktemp -d)"
cat > "$CONF_DIR/config.toml" <<TOML
[[profiles]]
name = "local"
bootstrap_servers = "127.0.0.1:19092"
tls_enabled = false
schema_registry_url = "$REGISTRY"

[profiles.auth]
type = "none"
TOML

target/release/rakko-web --bind "127.0.0.1:$PORT" --config "$CONF_DIR/config.toml" --static-dir crates/rakko-web/web/dist &
SERVER=$!
trap 'kill $SERVER 2>/dev/null; rm -rf "$CONF_DIR"' EXIT INT TERM

for _ in $(seq 1 50); do curl -sf "http://127.0.0.1:$PORT/api/profiles" >/dev/null && break; sleep 0.2; done

if [[ "$(topic_count demo-orders-avro)" == "0" ]]; then
    echo "Seeding demo-orders-avro (150 Avro messages) ..."
    customers=(alice bob carol dave erin); currencies=(USD EUR GBP); statuses=(PENDING PAID SHIPPED CANCELLED)
    for i in $(seq 1 150); do
        c=${customers[$((i % 5))]}
        body="{\"key\":{\"text\":\"$c\"},\"value\":{\"avro\":{\"subject\":\"demo-orders-avro-value\",\"json\":\"{\\\"order_id\\\":\\\"ord-$i\\\",\\\"customer\\\":\\\"$c\\\",\\\"amount_cents\\\":$((i * 137 % 90000 + 500)),\\\"currency\\\":\\\"${currencies[$((i % 3))]}\\\",\\\"status\\\":\\\"${statuses[$((i % 4))]}\\\"}\"}}}"
        curl -sf -X POST -H 'Content-Type: application/json' -H 'x-rakko-session: 00000000-0000-0000-0000-000000000000' \
            -d "$body" "http://127.0.0.1:$PORT/api/p/local/topics/demo-orders-avro/produce" >/dev/null
    done
fi

echo
echo "rakko-web is up: http://127.0.0.1:$PORT   (Ctrl+C to stop; Kafka keeps running)"
wait $SERVER
