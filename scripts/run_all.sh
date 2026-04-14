#!/usr/bin/env bash
set -euo pipefail

# Run the full stack and benchmarks sequentially from repository root.
# Usage: bash scripts/run_all.sh

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "[1/8] Ensure .env exists"
if [ ! -f .env ]; then
  cp example.env .env
  echo ".env created from example.env. Edit .env now to provide real credentials if needed." 
  read -rp "Press Enter to continue after editing .env (or Ctrl-C to abort)..."
fi

echo "[2/8] Start Docker services (build if needed)"
docker-compose up -d --build db mongodb redis api

wait_for_health() {
  container="$1"
  timeout_s=${2:-180}
  interval=5
  elapsed=0
  printf "Waiting for %s to be healthy (timeout %ss)" "$container" "$timeout_s"
  while [ $elapsed -lt $timeout_s ]; do
    status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "unknown")
    if [ "$status" = "healthy" ]; then
      echo " -> healthy"
      return 0
    fi
    printf '.'
    sleep $interval
    elapsed=$((elapsed + interval))
  done
  echo
  echo "ERROR: $container did not become healthy within ${timeout_s}s" >&2
  return 1
}

echo "[3/8] Waiting for services to become healthy"
wait_for_health adtech_mysql 120 || { echo "MySQL failed to become healthy"; exit 1; }
wait_for_health adtech_redis 60 || { echo "Redis failed to become healthy"; exit 1; }
# MongoDB can take longer on first start
if ! wait_for_health adtech_mongo 180; then
  echo "Warning: MongoDB health check failed or timeout; continuing anyway (it may still finish initializing)."
fi

echo "[4/8] Copy .env into API container (to support scripts that use load_dotenv)"
docker cp .env adtech_api:/app/.env || true

DOCKER_ENV="-e MYSQL_HOST=adtech_mysql -e MONGO_HOST=adtech_mongo -e REDIS_HOST=adtech_redis"

echo "[5/8] Insert sample data into MySQL"
docker cp src/scripts/tmp_insert.py adtech_api:/app/tmp_insert.py
docker exec $DOCKER_ENV adtech_api python3 /app/tmp_insert.py || echo "tmp_insert.py may have failed; check logs"

echo "[6/8] Run ETL (MySQL -> MongoDB)"
docker exec $DOCKER_ENV adtech_api /bin/sh -c "cd /app && python3 -u -m src.mongo_loader"

echo "[7/8] Run benchmark script (cached vs direct DB)"
docker cp tests/benchmark_api.py adtech_api:/app/benchmark_api.py || true
docker exec $DOCKER_ENV adtech_api /bin/sh -c "cd /app && python3 benchmark_api.py"

echo "[8/8] Retrieve benchmark results"
mkdir -p output
docker cp adtech_api:/output/benchmark_results.json output/benchmark_results.json || true
docker cp adtech_api:/output/benchmark_results.md output/benchmark_results.md || true
echo "Benchmark results copied to output/benchmark_results.*"

echo "Done. To stop and remove containers and volumes:"
echo "  docker-compose down -v"

exit 0
