#!/usr/bin/env bash
# run.sh — convenience wrapper for Homework 7
# Usage: ./run.sh /path/to/amazon_reviews.csv
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
mkdir -p "$DATA_DIR"

if [[ $# -ge 1 ]]; then
  SRC="$1"
  DEST="$DATA_DIR/amazon_reviews.csv"
  [[ "$SRC" != "$DEST" ]] && cp "$SRC" "$DEST" && echo "Dataset copied to $DEST"
fi

if [[ ! -f "$DATA_DIR/amazon_reviews.csv" ]]; then
  echo "ERROR: Dataset not found at $DATA_DIR/amazon_reviews.csv"
  echo "Usage: $0 /path/to/amazon_reviews.csv"
  exit 1
fi

echo "Starting all services..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d cassandra redis

echo "Waiting for Cassandra to be healthy..."
until docker inspect hw7_cassandra --format='{{.State.Health.Status}}' 2>/dev/null | grep -q healthy; do
  sleep 5
done

echo "Applying Cassandra schema..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up cassandra-init

echo "Starting API..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d api

echo "Running Spark ETL..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" run --rm spark-submit

echo ""
echo "✅  Done!"
echo "   API:      http://localhost:8000"
echo "   API docs: http://localhost:8000/docs"
