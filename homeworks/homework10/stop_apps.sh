#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" stop cassandra-writer stream-processor generator >/dev/null 2>&1 || true
docker compose -f "$COMPOSE_FILE" rm -f cassandra-writer stream-processor generator >/dev/null 2>&1 || true

echo "Generator and Spark streaming apps removed."
