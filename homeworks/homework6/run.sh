#!/usr/bin/env bash
# run.sh – convenience wrapper for Homework 6
# Usage: ./run.sh [path/to/amazon_reviews.tsv]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"

# ─── Step 1: copy / link the data file ─────────────────────────────────────
if [[ $# -ge 1 ]]; then
  SRC_FILE="$1"
  DEST_FILE="$DATA_DIR/amazon_reviews.csv"
  if [[ "$SRC_FILE" != "$DEST_FILE" ]]; then
    echo "Copying dataset to $DEST_FILE …"
    cp "$SRC_FILE" "$DEST_FILE"
  fi
fi

if [[ ! -f "$DATA_DIR/amazon_reviews.csv" ]]; then
  echo "ERROR: Dataset not found at $DATA_DIR/amazon_reviews.csv"
  echo "Usage: $0 /path/to/amazon_reviews.tsv"
  exit 1
fi

# ─── Step 2: start infrastructure ───────────────────────────────────────────
echo "Starting MongoDB + Spark cluster …"
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d mongodb spark-master spark-worker

echo "Waiting for MongoDB to be healthy …"
until docker inspect hw6_mongodb --format='{{.State.Health.Status}}' 2>/dev/null | grep -q healthy; do
  sleep 3
done

# ─── Step 3: submit the Spark job ───────────────────────────────────────────
echo "Submitting PySpark ETL job …"
docker compose -f "$SCRIPT_DIR/docker-compose.yml" run --rm spark-submit

echo ""
echo "✅  ETL complete. Query MongoDB at localhost:27018"
echo "    mongosh 'mongodb://root:rootpass@localhost:27018/amazon_reviews?authSource=admin'"
