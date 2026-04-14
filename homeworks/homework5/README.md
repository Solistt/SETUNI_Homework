# Homework 5 — REST API with Redis Caching and Benchmarking

## Objective

Build a REST API on top of the existing AdTech data pipeline (MySQL + MongoDB) that serves
analytics queries through a Redis read-through cache, then benchmark the performance gain.

---

## Architecture

```
Client ──► FastAPI ──► Redis (cache check)
                │           │ HIT → return cached JSON
                │           │ MISS ↓
                │       MySQL / MongoDB
                │           │
                │       ◄───┘ store result in Redis with TTL
                └──► return JSON response
```

**Stack:** FastAPI · MySQL 8.0 · MongoDB 6.0 · Redis 7 · Docker Compose

---

## API Endpoints

### 1. Campaign Performance
```
GET /campaign/{campaign_id}/performance?use_cache=true
```
Returns impressions, clicks, CTR, and ad spend for the campaign.
Queries **MySQL** (`impressions` + `clicks` tables).
**Cache TTL: 30 seconds.**

Example response:
```json
{
  "campaign_id": 1,
  "impressions": 959,
  "clicks": 45,
  "ctr": 0.0469,
  "ad_spend": 2134.99
}
```

### 2. Advertiser Spending
```
GET /advertiser/{advertiser_id}/spending?use_cache=true
```
Returns total ad spend across all of the advertiser's campaigns.
Queries **MySQL** (`impressions` + `campaigns` tables).
**Cache TTL: 5 minutes (300 s).**

Example response:
```json
{
  "advertiser_id": 1,
  "total_ad_spend": 18013.55
}
```

### 3. User Engagements
```
GET /user/{user_id}/engagements?use_cache=true
```
Returns every ad the user engaged with (impression or click).
Queries **MongoDB** (`users` collection, embedded sessions).
**Cache TTL: 60 seconds.**

Example response:
```json
{
  "user_id": 78770,
  "engagements": [
    {
      "campaign_id": 172,
      "campaign_name": "Campaign_172",
      "advertiser_name": "Advertiser_18",
      "timestamp": "2024-10-28T02:57:57",
      "category": "Age 27-40, Health, USA",
      "engagement_type": "impression"
    }
  ]
}
```

---

## Redis Caching Strategy

| Feature | Implementation |
|---------|---------------|
| Pattern | Read-through cache |
| Cache key format | `{entity}:{id}:{metric}` (e.g. `campaign:1:performance`) |
| Miss behavior | Query DB → serialize to JSON → `SETEX` with TTL → return |
| Hit behavior | `GET` from Redis → deserialize → return immediately |
| Bypass | Pass `?use_cache=false` to skip cache entirely |
| Expiration | Per-endpoint TTL (see table above) |

---

## Benchmarking Results

Each endpoint was called **10 times**; averages are reported.

| Endpoint | Without Cache (s) | With Cache (s) | Speedup |
|----------|-------------------|----------------|---------|
| Campaign Performance | 0.0616 | 0.0039 | 15.90x |
| Advertiser Spending  | 0.0338 | 0.0025 | 13.34x |
| User Engagements     | 0.0031 | 0.0022 | 1.39x  |

**Key findings:**
- **MySQL-backed endpoints** see a **13–16x** latency improvement with Redis.
- **MongoDB-backed endpoint** is already fast due to the embedded document model,
  so the cache adds only a modest improvement.
- Cold-cache (first request after flush) is comparable to the no-cache path
  because it still hits the database.

---

## How to Run

### Prerequisites
- Docker and Docker Compose installed.
- Clone this repository and `cd` into its root.

### 1. Create `.env` from the template
```bash
cp example.env .env
```
Edit `.env` and fill in credentials (at minimum `MYSQL_USER`, `MYSQL_PASSWORD`,
`MONGO_USER`, `MONGO_PASSWORD`). Defaults matching `docker-compose.yml`:
```
MYSQL_USER=adtech
MYSQL_PASSWORD=adtechpass
MONGO_USER=root
MONGO_PASSWORD=rootpass
```

### 2. Start all services
```bash
docker-compose up -d --build
```
Wait for all health checks to pass:
```bash
docker inspect --format='{{.State.Health.Status}}' adtech_mysql   # healthy
docker inspect --format='{{.State.Health.Status}}' adtech_redis   # healthy
docker inspect --format='{{.State.Health.Status}}' adtech_mongo   # healthy
```

### 3. Seed MySQL with sample data
```bash
docker exec adtech_api python3 /app/src/scripts/tmp_insert.py
```

### 4. Run ETL (MySQL → MongoDB)
```bash
docker exec adtech_api python3 -m src.mongo_loader
```

### 5. Test the API
```bash
curl "http://127.0.0.1:8000/campaign/1/performance"
curl "http://127.0.0.1:8000/advertiser/1/spending"
curl "http://127.0.0.1:8000/user/1000/engagements"
```

### 6. Run benchmarks
```bash
docker cp tests/benchmark_api.py adtech_api:/app/benchmark_api.py
docker exec adtech_api python3 /app/benchmark_api.py
```
Results are written to `output/benchmark_results.md` and `output/benchmark_results.json`.

### One-command run (all steps above)
```bash
bash scripts/run_all.sh
```

### Tear down
```bash
docker-compose down -v
```

---

## File Structure

```
SETUNI_Homework/
├── .env                        # Credentials (not committed)
├── .gitignore
├── docker-compose.yml          # MySQL, MongoDB, Redis, API
├── Dockerfile                  # API container image
├── example.env                 # Template for .env
├── README.md                   # Project overview
│
├── src/
│   ├── api.py                  # FastAPI app (3 endpoints + Redis cache)
│   ├── config.py               # Centralized env-var loader with validation
│   ├── connection.py           # ConnectionFactory for MySQL and MongoDB
│   ├── etl_process.py          # CSV → MySQL ETL
│   ├── mongo_loader.py         # MySQL → MongoDB streaming ETL
│   ├── mongo_queries.py        # MongoDB aggregation pipelines
│   ├── benchmark.py            # Lightweight local benchmark
│   ├── generate_reports.py     # BI report generation
│   ├── utils.py                # JSON encoder utilities
│   └── requirements.txt        # Python dependencies
│
├── tests/
│   └── benchmark_api.py        # Full benchmark script (cached vs direct)
│
├── scripts/
│   └── run_all.sh              # End-to-end automation script
│
├── output/                     # Generated reports and benchmark results
├── screenshots/Homework5/      # Demonstration screenshots
└── homeworks/homework5/        # This README
```

---

## Docker Compose Services

| Service | Image | Port | Health Check |
|---------|-------|------|-------------|
| `adtech_mysql` | mysql:8.0 | 127.0.0.1:3306 | `mysqladmin ping` |
| `adtech_mongo` | mongo:6.0 | 127.0.0.1:27017 | `mongosh --eval "db.adminCommand('ping')"` |
| `adtech_redis` | redis:7-alpine | 127.0.0.1:6379 | `redis-cli ping` |
| `adtech_api` | custom (Dockerfile) | 127.0.0.1:8000 | `curl http://localhost:8000/docs` |

All services run on an isolated Docker bridge network (`internal_net`).
Port bindings are restricted to `127.0.0.1` (localhost only).

---

## Screenshots

See `screenshots/Homework5/` for demonstrations of:
1. API endpoint responses (campaign, advertiser, user).
2. Benchmark results comparison table.
3. Docker containers running and healthy.
4. Cache hit vs cache miss behavior.
