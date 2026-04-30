#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is not running."
  echo "Start Docker Desktop and retry."
  exit 1
fi

"$SCRIPT_DIR/start_kafka.sh"
"$SCRIPT_DIR/start_cassandra.sh"
"$SCRIPT_DIR/start_spark.sh"

MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-300}" \
  "$SCRIPT_DIR/start_apps.sh"

echo ""
echo "Homework 10 pipeline is running."
echo "Wait 3-5 minutes, then run:"
echo "  docker ps"
echo "  $SCRIPT_DIR/show_results.sh 10"
echo ""
echo "Useful logs:"
echo "  docker compose -f $SCRIPT_DIR/docker-compose.yml logs -f generator"
echo "  docker compose -f $SCRIPT_DIR/docker-compose.yml logs -f stream-processor"
echo "  docker compose -f $SCRIPT_DIR/docker-compose.yml logs -f cassandra-writer"
