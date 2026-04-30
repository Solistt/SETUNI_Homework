#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
DATA_DIR="$SCRIPT_DIR/data"
OUTPUT_DIR="$SCRIPT_DIR/output"

DEFAULT_DATASET_HW7="$SCRIPT_DIR/../homework7/data/amazon_reviews.csv"
DEFAULT_DATASET_HW6="$SCRIPT_DIR/../homework6/data/amazon_reviews.csv"

ensure_docker_ready() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker CLI not found. Install Docker Desktop first."
    exit 1
  fi

  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker daemon is not running."
    echo "Start Docker Desktop and retry."
    exit 1
  fi

  if ! docker compose version >/dev/null 2>&1; then
    echo "ERROR: docker compose plugin is not available."
    exit 1
  fi
}

resolve_dataset_path() {
  if [[ $# -ge 1 && -n "$1" ]]; then
    echo "$1"
    return
  fi

  if [[ -f "$DEFAULT_DATASET_HW7" ]]; then
    echo "$DEFAULT_DATASET_HW7"
    return
  fi

  if [[ -f "$DEFAULT_DATASET_HW6" ]]; then
    echo "$DEFAULT_DATASET_HW6"
    return
  fi

  echo "$DEFAULT_DATASET_HW7"
}

ensure_docker_ready

SOURCE_DATASET="$(resolve_dataset_path "${1:-}")"
TARGET_DATASET="$DATA_DIR/amazon_reviews.csv"

if [[ ! -f "$SOURCE_DATASET" ]]; then
  echo "ERROR: Dataset not found at $SOURCE_DATASET"
  echo "Usage: $0 [path/to/amazon_reviews.csv]"
  exit 1
fi

mkdir -p "$DATA_DIR" "$OUTPUT_DIR"

if [[ "$SOURCE_DATASET" != "$TARGET_DATASET" ]]; then
  echo "Copying dataset to $TARGET_DATASET"
  cp "$SOURCE_DATASET" "$TARGET_DATASET"
fi

MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-900}"
GROUP_ID="${GROUP_ID:-hw9_tweet_consumer_$(date +%s)}"

echo "Starting Kafka + producer + consumer..."
echo "Producer runtime: $MAX_RUNTIME_SECONDS seconds"
echo "Consumer group: $GROUP_ID"
MAX_RUNTIME_SECONDS="$MAX_RUNTIME_SECONDS" GROUP_ID="$GROUP_ID" \
  docker compose -f "$COMPOSE_FILE" up -d --build zookeeper kafka producer consumer

echo "Waiting for Kafka broker healthcheck..."
until docker inspect hw9_kafka --format='{{.State.Health.Status}}' 2>/dev/null | grep -q healthy; do
  sleep 3
done

echo ""
echo "Stack is running."
echo "Use these commands for deliverables:"
echo "  docker ps"
echo "  ls -1 $OUTPUT_DIR"
echo "  $SCRIPT_DIR/show_results.sh"
echo "  docker compose -f $COMPOSE_FILE logs -f consumer"
