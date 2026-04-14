# AdTech Data Pipeline & Analytics API

## Overview

Full-stack AdTech analytics platform: CSV → MySQL → MongoDB ETL pipeline,
FastAPI REST API with Redis read-through caching, and performance benchmarking.

All code, comments, logs, and documentation are **English-only**. No hardcoded
credentials — everything is driven by a root `.env` file.

**Stack:** Python 3.10 · FastAPI · MySQL 8.0 · MongoDB 6.0 · Redis 7 · Docker Compose

---

## Quick Start

```bash
cp example.env .env          # fill in credentials (see example.env comments)
docker-compose up -d --build # start MySQL, MongoDB, Redis, API
bash scripts/run_all.sh      # seed data, run ETL, run benchmarks
```

Test the API:
```bash
curl http://127.0.0.1:8000/campaign/1/performance
curl http://127.0.0.1:8000/advertiser/1/spending
curl http://127.0.0.1:8000/user/1000/engagements
```

Tear down:
```bash
docker-compose down -v
```

---

## Project Structure

```
SETUNI_Homework/
├── .env                        # Credentials (not committed)
├── .gitignore
├── docker-compose.yml          # MySQL, MongoDB, Redis, API + healthchecks
├── Dockerfile                  # API container image
├── example.env                 # Template for .env
│
├── src/
│   ├── api.py                  # FastAPI REST API (3 endpoints + Redis cache)
│   ├── config.py               # Centralized env-var loader with validation
│   ├── connection.py           # ConnectionFactory (MySQL, MongoDB)
│   ├── etl_process.py          # CSV → MySQL ETL (batch loading)
│   ├── mongo_loader.py         # MySQL → MongoDB streaming ETL
│   ├── mongo_queries.py        # MongoDB aggregation pipelines (BI reports)
│   ├── generate_reports.py     # Report generation to output/
│   ├── benchmark.py            # Lightweight local benchmark
│   ├── utils.py                # Custom JSON encoders
│   └── requirements.txt
│
├── tests/
│   └── benchmark_api.py        # Full cached vs direct-DB benchmark
│
├── scripts/
│   └── run_all.sh              # End-to-end automation
│
├── output/                     # Generated reports & benchmark results
├── screenshots/                # Demo screenshots per homework
└── homeworks/
    ├── hw2_mysql/              # SQL schema & queries
    ├── hw3_mongodb/            # MongoDB ETL & queries docs
    └── homework5/              # REST API + Redis caching docs
```

---

## Homework Assignments

| # | Topic | Key Files | Docs |
|---|-------|-----------|------|
| 2 | MySQL Schema & SQL Queries | `homeworks/hw2_mysql/`, `src/etl_process.py` | `homeworks/hw2_mysql/` |
| 3 | MongoDB ETL & Aggregations | `src/mongo_loader.py`, `src/mongo_queries.py` | `homeworks/hw3_mongodb/README.md` |
| 5 | REST API + Redis + Benchmarks | `src/api.py`, `tests/benchmark_api.py` | **`homeworks/homework5/README.md`** |

---

## NoSQL Schema Strategy

User-centric document model — each user document contains embedded `sessions`,
and each session contains embedded `impressions`:

```json
{
  "_id": 12345,
  "demographics": { "age": 28, "country": "USA" },
  "sessions": [
    {
      "session_start": "2024-01-01T12:00:00Z",
      "impressions": [
        { "impression_id": "a1b2c3", "campaign": { "name": "Campaign_1" }, "click": { "click_id": "..." } }
      ]
    }
  ]
}
```

### Embedding vs Referencing

| Approach | Pros | Cons |
|----------|------|------|
| **Embedding** (used here) | Fast single-document reads; atomic upserts; ideal for user-profile analytics | Documents grow for very active users; nested-array updates can be costly |
| **Referencing** | Scales for unbounded event growth; easier to shard by time | Requires `$lookup` or app-side joins; higher read latency |

**Production recommendation:** Embed for hot-path user profiles + recent sessions.
Reference (separate collection) for high-volume, append-only events. Use TTL or
time-bucketed collections to keep document sizes bounded.

---

## Security & Operations

- **Zero hardcoding:** All credentials, hosts, and ports come from `.env`.
- **Config validation:** `src/config.py` fails fast at startup if any required
  variable is missing or empty.
- **Docker network isolation:** Services communicate on an internal bridge network.
  Port bindings are restricted to `127.0.0.1` (localhost only).
- **Healthchecks:** MySQL, MongoDB, and Redis have Docker healthchecks; the API
  container depends on all three being healthy before starting.
- **`.gitignore`** blocks `.env`, `venv/`, `__pycache__/`, and IDE files.

---

## API & Caching (Homework 5)

Three REST endpoints with **read-through Redis cache**:

| Endpoint | DB | Cache TTL |
|----------|----|-----------|
| `GET /campaign/{id}/performance` | MySQL | 30 s |
| `GET /advertiser/{id}/spending` | MySQL | 5 min |
| `GET /user/{id}/engagements` | MongoDB | 60 s |

Pass `?use_cache=false` to bypass Redis and query the DB directly.

### Benchmark Results

| Endpoint | Without Cache | With Cache | Speedup |
|----------|--------------|------------|---------|
| Campaign Performance | 0.0616 s | 0.0039 s | **15.9x** |
| Advertiser Spending | 0.0338 s | 0.0025 s | **13.3x** |
| User Engagements | 0.0031 s | 0.0022 s | 1.4x |

Full details: [`homeworks/homework5/README.md`](homeworks/homework5/README.md)
