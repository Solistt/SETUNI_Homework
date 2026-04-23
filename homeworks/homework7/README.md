# Homework 7 – Amazon Reviews: Cassandra + Spark + FastAPI + Redis

## Overview

Extension of Homework 6. Uses the same Amazon Reviews dataset but stores data in
**Apache Cassandra** (instead of MongoDB) and exposes a **FastAPI** REST API with
**Redis** read-through caching (TTL 5 min).

## Architecture

```
TSV Dataset
    │
    ▼
PySpark ETL ──writes──► Cassandra (7 tables, no ALLOW FILTERING)
                              │
                         FastAPI API ──reads──► Response
                              │
                            Redis (cache TTL=300s)
```

## Cassandra Schema

| Table | Partition Key | Purpose |
|---|---|---|
| `reviews_by_product` | `product_id` | Endpoint 1 |
| `reviews_by_product_rating` | `(product_id, star_rating)` | Endpoint 2 |
| `reviews_by_customer` | `customer_id` | Endpoint 3 |
| `product_reviews_by_month` | `year_month` | Endpoint 4 |
| `customer_verified_by_month` | `year_month` | Endpoint 5 |
| `customer_haters_by_month` | `year_month` | Endpoint 6 |
| `customer_backers_by_month` | `year_month` | Endpoint 7 |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/reviews/product/{product_id}` | All reviews for a product |
| GET | `/reviews/product/{product_id}/rating/{star_rating}` | Reviews filtered by rating |
| GET | `/reviews/customer/{customer_id}` | All reviews by a customer |
| GET | `/analytics/top-products?start=YYYY-MM&end=YYYY-MM&n=10` | Top N reviewed products |
| GET | `/analytics/top-customers?start=YYYY-MM&end=YYYY-MM&n=10` | Top N customers (verified) |
| GET | `/analytics/top-haters?start=YYYY-MM&end=YYYY-MM&n=10` | Top N 1–2★ reviewers |
| GET | `/analytics/top-backers?start=YYYY-MM&end=YYYY-MM&n=10` | Top N 4–5★ reviewers |
| GET | `/health` | Health check |
| GET | `/docs` | Interactive Swagger UI |

## Quick Start

```bash
# 1. Place the dataset (comma-separated CSV with header)
cp /path/to/amazon_reviews.csv ./data/amazon_reviews.csv

# 2. Run everything (one command)
chmod +x run.sh
./run.sh /path/to/amazon_reviews.csv

# 3. Test
curl http://localhost:8000/reviews/product/0439784549
curl "http://localhost:8000/analytics/top-products?start=2000-01&end=2015-12&n=5"
```

## Manual Steps

```bash
# Start Cassandra + Redis
docker compose up -d cassandra redis

# Apply schema (waits until Cassandra is healthy)
docker compose up cassandra-init

# Start API
docker compose up -d api

# Run ETL (after placing CSV in ./data/)
docker compose run --rm spark-submit
```

## Environment Variables

| Variable | Default | Service |
|---|---|---|
| `CASSANDRA_HOST` | `cassandra` | ETL, API |
| `CASSANDRA_KEYSPACE` | `amazon_reviews` | ETL, API |
| `REDIS_HOST` | `redis` | API |
| `CACHE_TTL` | `300` | API |
| `CSV_PATH` | `/data/amazon_reviews.tsv` | ETL |

## Teardown

```bash
docker compose down -v
```
