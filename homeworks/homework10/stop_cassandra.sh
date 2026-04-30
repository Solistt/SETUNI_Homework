#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" stop cassandra-init cassandra >/dev/null 2>&1 || true
docker compose -f "$COMPOSE_FILE" rm -f cassandra-init cassandra >/dev/null 2>&1 || true
docker volume rm hw10_cassandra_data >/dev/null 2>&1 || true

echo "Cassandra stack removed."
