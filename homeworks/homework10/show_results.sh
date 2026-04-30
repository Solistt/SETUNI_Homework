#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
LIMIT="${1:-10}"

if ! [[ "$LIMIT" =~ ^[0-9]+$ ]]; then
  echo "ERROR: limit must be a positive integer"
  exit 1
fi

echo "=== INPUT TOPIC (input) ==="
docker compose -f "$COMPOSE_FILE" exec -T kafka \
  kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic input \
  --from-beginning \
  --max-messages "$LIMIT" \
  --timeout-ms 10000 || true

echo ""
echo "=== PROCESSED TOPIC (processed) ==="
docker compose -f "$COMPOSE_FILE" exec -T kafka \
  kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic processed \
  --from-beginning \
  --max-messages "$LIMIT" \
  --timeout-ms 10000 || true

echo ""
echo "=== CASSANDRA TABLE (hw10.page_creations) ==="
docker compose -f "$COMPOSE_FILE" exec -T cassandra \
  cqlsh -e "SELECT user_id, domain, created_at, page_title FROM hw10.page_creations LIMIT $LIMIT;"
