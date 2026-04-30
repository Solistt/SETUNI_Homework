#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
IMAGE_NAME="${IMAGE_NAME:-hw8-tweet-producer:latest}"

ensure_docker_ready() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker CLI not found. Install Docker Desktop first."
    echo "Install guide: https://docs.docker.com/desktop/install/mac-install/"
    exit 1
  fi

  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker daemon is not running."
    echo "Start Docker Desktop and wait until Engine is running, then retry."
    echo "Quick start on macOS: open -a Docker"
    exit 1
  fi

  if ! docker compose version >/dev/null 2>&1; then
    echo "ERROR: docker compose plugin is not available."
    exit 1
  fi
}

DEFAULT_DATASET_HW7="$SCRIPT_DIR/../homework7/data/amazon_reviews.csv"
DEFAULT_DATASET_HW6="$SCRIPT_DIR/../homework6/data/amazon_reviews.csv"

if [[ $# -ge 1 ]]; then
  DATASET_PATH="$1"
elif [[ -f "$DEFAULT_DATASET_HW7" ]]; then
  DATASET_PATH="$DEFAULT_DATASET_HW7"
elif [[ -f "$DEFAULT_DATASET_HW6" ]]; then
  DATASET_PATH="$DEFAULT_DATASET_HW6"
else
  DATASET_PATH="$DEFAULT_DATASET_HW7"
fi

TOPIC_NAME="${TOPIC:-tweets}"
MIN_RATE="${MIN_MSGS_PER_SEC:-10}"
MAX_RATE="${MAX_MSGS_PER_SEC:-15}"
MAX_RUNTIME="${MAX_RUNTIME_SECONDS:-300}"

ensure_docker_ready

if [[ ! -f "$DATASET_PATH" ]]; then
  echo "ERROR: Dataset not found at $DATASET_PATH"
  echo "Usage: $0 [path/to/amazon_reviews.csv]"
  exit 1
fi

echo "Starting Kafka stack..."
docker compose -f "$COMPOSE_FILE" up -d zookeeper kafka

echo "Waiting for Kafka broker healthcheck..."
until docker inspect hw8_kafka --format='{{.State.Health.Status}}' 2>/dev/null | grep -q healthy; do
  sleep 3
done

"$SCRIPT_DIR/build.sh"

PROJECT_NAME="${COMPOSE_PROJECT_NAME:-$(basename "$SCRIPT_DIR")}"
NETWORK_NAME="${PROJECT_NAME}_hw8_net"

if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
  echo "ERROR: Expected Docker network not found: $NETWORK_NAME"
  exit 1
fi

echo "Using dataset: $DATASET_PATH"
echo "Topic: $TOPIC_NAME"
echo "Rate: $MIN_RATE-$MAX_RATE msg/s"
echo "Runtime: $MAX_RUNTIME seconds"

echo "Running producer container on network: $NETWORK_NAME"
docker run --rm --name hw8_tweet_producer \
  --network "$NETWORK_NAME" \
  -v "$DATASET_PATH:/data/amazon_reviews.csv:ro" \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e INPUT_FILE=/data/amazon_reviews.csv \
  -e TOPIC="$TOPIC_NAME" \
  -e MIN_MSGS_PER_SEC="$MIN_RATE" \
  -e MAX_MSGS_PER_SEC="$MAX_RATE" \
  -e MAX_RUNTIME_SECONDS="$MAX_RUNTIME" \
  "$IMAGE_NAME"

echo ""
echo "Producer finished."
echo "Read topic sample with:"
echo "  $SCRIPT_DIR/consume.sh 20"
