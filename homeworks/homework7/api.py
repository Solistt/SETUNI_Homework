"""
Homework 7: REST API — FastAPI + Cassandra + Redis
7 endpoints, Redis cache TTL=300s, no ALLOW FILTERING
"""

import json
import os
import time
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Optional

import redis as redis_lib
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.util import Date as CassandraDate
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

# ── Config ────────────────────────────────────────────────────────────────────
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "amazon_reviews")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))  # 5 minutes

app = FastAPI(title="Amazon Reviews API – HW7")

# ── Globals (set at startup) ───────────────────────────────────────────────────
_cass_session = None
_redis_client: Optional[redis_lib.Redis] = None


def _connect_cassandra():
    global _cass_session
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        load_balancing_policy=RoundRobinPolicy(),
    )
    for attempt in range(30):
        try:
            _cass_session = cluster.connect(CASSANDRA_KEYSPACE)
            print(f"Connected to Cassandra ({CASSANDRA_HOST}:{CASSANDRA_PORT})")
            return
        except Exception as exc:
            print(f"  Cassandra attempt {attempt + 1}/30: {exc}")
            time.sleep(5)
    raise RuntimeError("Could not connect to Cassandra after 30 attempts")


def _connect_redis():
    global _redis_client
    _redis_client = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    print(f"Connected to Redis ({REDIS_HOST}:{REDIS_PORT})")


@app.on_event("startup")
def startup():
    _connect_cassandra()
    _connect_redis()


# ── Cache helpers ─────────────────────────────────────────────────────────────
def _serialize(obj: Any) -> str:
    def default(o):
        if isinstance(o, (date, datetime, CassandraDate)):
            return str(o)
        raise TypeError(f"Not serializable: {type(o)}")
    return json.dumps(obj, default=default)


def _cache_get(key: str):
    val = _redis_client.get(key)
    return json.loads(val) if val else None


def _cache_set(key: str, data: Any) -> None:
    _redis_client.setex(key, CACHE_TTL, _serialize(data))


def _row_to_dict(row) -> dict:
    result = {}
    for k, v in row._asdict().items():
        if isinstance(v, (date, datetime, CassandraDate)):
            result[k] = str(v)
        else:
            result[k] = v
    return result


# ── Period helpers ────────────────────────────────────────────────────────────
def _year_months(start: str, end: str) -> list:
    """Return ['YYYY-MM', ...] from start to end inclusive."""
    sy, sm = int(start[:4]), int(start[5:])
    ey, em = int(end[:4]), int(end[5:])
    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return months


def _top_n_from_months(table: str, id_col: str, months: list, n: int) -> list:
    """Query table for each month, aggregate counts, return top-n."""
    counts: dict = defaultdict(int)
    stmt = _cass_session.prepare(
        f"SELECT {id_col}, review_count FROM {table} WHERE year_month = ?"
    )
    for ym in months:
        for row in _cass_session.execute(stmt, (ym,)):
            counts[getattr(row, id_col)] += row.review_count
    return sorted(
        [{"id": k, "review_count": v} for k, v in counts.items()],
        key=lambda x: x["review_count"],
        reverse=True,
    )[:n]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/reviews/product/{product_id}")
def reviews_by_product(product_id: str):
    """Return all reviews for a given product_id."""
    key = f"reviews:product:{product_id}"
    if cached := _cache_get(key):
        return cached
    stmt = _cass_session.prepare(
        "SELECT * FROM reviews_by_product WHERE product_id = ?"
    )
    result = [_row_to_dict(r) for r in _cass_session.execute(stmt, (product_id,))]
    _cache_set(key, result)
    return result


@app.get("/reviews/product/{product_id}/rating/{star_rating}")
def reviews_by_product_rating(product_id: str, star_rating: int):
    """Return all reviews for a given product_id filtered by star_rating."""
    if star_rating not in range(1, 6):
        raise HTTPException(400, "star_rating must be 1–5")
    key = f"reviews:product:{product_id}:rating:{star_rating}"
    if cached := _cache_get(key):
        return cached
    stmt = _cass_session.prepare(
        "SELECT * FROM reviews_by_product_rating WHERE product_id = ? AND star_rating = ?"
    )
    result = [_row_to_dict(r) for r in _cass_session.execute(stmt, (product_id, star_rating))]
    _cache_set(key, result)
    return result


@app.get("/reviews/customer/{customer_id}")
def reviews_by_customer(customer_id: str):
    """Return all reviews written by a given customer_id."""
    key = f"reviews:customer:{customer_id}"
    if cached := _cache_get(key):
        return cached
    stmt = _cass_session.prepare(
        "SELECT * FROM reviews_by_customer WHERE customer_id = ?"
    )
    result = [_row_to_dict(r) for r in _cass_session.execute(stmt, (customer_id,))]
    _cache_set(key, result)
    return result


@app.get("/analytics/top-products")
def top_products(
    start: str = Query(..., description="Start month YYYY-MM"),
    end: str = Query(..., description="End month YYYY-MM"),
    n: int = Query(10, ge=1, le=100),
):
    """Return N most reviewed products for the given period."""
    key = f"analytics:top_products:{start}:{end}:{n}"
    if cached := _cache_get(key):
        return cached
    months = _year_months(start, end)
    result = _top_n_from_months("product_reviews_by_month", "product_id", months, n)
    _cache_set(key, result)
    return result


@app.get("/analytics/top-customers")
def top_customers(
    start: str = Query(..., description="Start month YYYY-MM"),
    end: str = Query(..., description="End month YYYY-MM"),
    n: int = Query(10, ge=1, le=100),
):
    """Return N most productive customers (verified purchases) for the given period."""
    key = f"analytics:top_customers:{start}:{end}:{n}"
    if cached := _cache_get(key):
        return cached
    months = _year_months(start, end)
    result = _top_n_from_months("customer_verified_by_month", "customer_id", months, n)
    _cache_set(key, result)
    return result


@app.get("/analytics/top-haters")
def top_haters(
    start: str = Query(..., description="Start month YYYY-MM"),
    end: str = Query(..., description="End month YYYY-MM"),
    n: int = Query(10, ge=1, le=100),
):
    """Return N customers with the most 1- or 2-star reviews for the given period."""
    key = f"analytics:top_haters:{start}:{end}:{n}"
    if cached := _cache_get(key):
        return cached
    months = _year_months(start, end)
    result = _top_n_from_months("customer_haters_by_month", "customer_id", months, n)
    _cache_set(key, result)
    return result


@app.get("/analytics/top-backers")
def top_backers(
    start: str = Query(..., description="Start month YYYY-MM"),
    end: str = Query(..., description="End month YYYY-MM"),
    n: int = Query(10, ge=1, le=100),
):
    """Return N customers with the most 4- or 5-star reviews for the given period."""
    key = f"analytics:top_backers:{start}:{end}:{n}"
    if cached := _cache_get(key):
        return cached
    months = _year_months(start, end)
    result = _top_n_from_months("customer_backers_by_month", "customer_id", months, n)
    _cache_set(key, result)
    return result


@app.get("/health")
def health():
    return {"status": "ok"}
