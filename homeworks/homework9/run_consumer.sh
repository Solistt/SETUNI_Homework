#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
OUTPUT_DIR="$SCRIPT_DIR/output"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker CLI not found. Install Docker Desktop first."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is not running. Start Docker Desktop first."
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
GROUP_ID="${GROUP_ID:-hw9_tweet_consumer_$(date +%s)}"

echo "Starting consumer service (and required dependencies)..."
echo "Consumer group: $GROUP_ID"
GROUP_ID="$GROUP_ID" docker compose -f "$COMPOSE_FILE" up -d --build consumer

echo "Consumer started."
echo "Logs: docker compose -f $COMPOSE_FILE logs -f consumer"
