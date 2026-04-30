#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
MAX_MESSAGES="${1:-20}"

docker compose -f "$COMPOSE_FILE" exec kafka \
  kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --from-beginning \
    --max-messages "$MAX_MESSAGES"
