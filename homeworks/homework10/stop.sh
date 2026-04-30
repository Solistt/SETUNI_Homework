#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

"$SCRIPT_DIR/stop_apps.sh"
"$SCRIPT_DIR/stop_spark.sh"
"$SCRIPT_DIR/stop_cassandra.sh"
"$SCRIPT_DIR/stop_kafka.sh"

docker compose -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true

echo "Homework 10 stack stopped."
