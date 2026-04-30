#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is not running."
  echo "Start Docker Desktop and retry."
  exit 1
fi

mkdir -p "$SCRIPT_DIR/checkpoints"

MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-300}"

echo "Starting generator and Spark streaming apps..."
echo "Generator runtime: $MAX_RUNTIME_SECONDS seconds"
MAX_RUNTIME_SECONDS="$MAX_RUNTIME_SECONDS" \
  docker compose -f "$COMPOSE_FILE" up -d --build generator stream-processor cassandra-writer

echo "Application containers started."
