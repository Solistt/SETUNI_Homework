#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is not running."
  echo "Start Docker Desktop and retry."
  exit 1
fi

docker compose -f "$COMPOSE_FILE" up -d zookeeper kafka kafka-init

echo "Kafka stack started (zookeeper, kafka, kafka-init)."
echo "Topics created: input, processed"
