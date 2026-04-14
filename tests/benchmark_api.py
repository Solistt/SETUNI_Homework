"""Benchmark the API endpoints with and without Redis caching.

Run after the API is running on http://127.0.0.1:8000 and Redis is available.
Writes results to `output/benchmark_results.md`.
"""
import time
import statistics
import os
import json
from pathlib import Path

import requests
import redis
from dotenv import load_dotenv


BASE = Path(__file__).resolve().parents[1]
OUT_DIR = BASE / 'output'
OUT_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_FILE = OUT_DIR / 'benchmark_results.json'


def load_env():
    env_path = BASE / '.env'
    if env_path.exists():
        load_dotenv(env_path)


def flush_redis():
    r = redis.Redis(host=os.getenv('REDIS_HOST', '127.0.0.1'), port=int(os.getenv('REDIS_PORT', 6379)))
    r.flushdb()


def time_requests(urls, n=10):
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        resp = requests.get(urls)
        t1 = time.perf_counter()
        resp.raise_for_status()
        times.append((t1 - t0) * 1000.0)  # ms
    return times


def benchmark_one(endpoint, params_cache_enabled, params_cache_disabled, n=10):
    base = 'http://127.0.0.1:8000'

    # Without cache (direct DB)
    no_cache_url = f"{base}{endpoint}"
    if params_cache_disabled:
        no_cache_url = no_cache_url + ('&' if '?' in no_cache_url else '?') + params_cache_disabled

    # With cache (read-through)
    flush_redis()
    cache_url = f"{base}{endpoint}"
    if params_cache_enabled:
        cache_url = cache_url + ('&' if '?' in cache_url else '?') + params_cache_enabled

    # Cold cache: first request will hit DB then populate cache
    cold = time_requests(cache_url, n=1)
    # Warm cached responses
    warm = time_requests(cache_url, n=n)
    # Direct DB bypass cache
    direct = time_requests(no_cache_url, n=n)

    return {
        'cold_ms': cold[0],
        'warm_avg_ms': statistics.mean(warm),
        'direct_avg_ms': statistics.mean(direct),
        'warm_p50_ms': statistics.median(warm),
    }


def run_all():
    load_env()
    results = {}

    # Campaign performance (campaign_id=1)
    ep = '/campaign/1/performance'
    res = benchmark_one(ep, 'use_cache=true', 'use_cache=false', n=10)
    results['campaign_performance'] = res

    # Advertiser spending (advertiser_id=1)
    ep = '/advertiser/1/spending'
    res = benchmark_one(ep, 'use_cache=true', 'use_cache=false', n=10)
    results['advertiser_spending'] = res

    # User engagements (user_id=1000)
    ep = '/user/1000/engagements'
    res = benchmark_one(ep, 'use_cache=true', 'use_cache=false', n=10)
    results['user_engagements'] = res

    with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)

    # Generate Markdown report
    md_file = OUT_DIR / 'benchmark_results.md'
    with open(md_file, 'w', encoding='utf-8') as f:
        f.write("# Benchmarking Results\n\n")
        f.write("| Endpoint | Without Cache (s) | With Cache (s) | Speedup |\n")
        f.write("|----------|-------------------|----------------|---------|\n")
        
        # Mapping from internal results keys to display names for the report
        display_names = {
            'campaign_performance': 'Campaign Performance',
            'advertiser_spending': 'Advertiser Spending',
            'user_engagements': 'User Engagements',
        }

        for key, data in results.items():
            endpoint_name = display_names.get(key, key.replace('_', ' ').title())
            no_cache_s = data['direct_avg_ms'] / 1000.0
            with_cache_s = data['warm_avg_ms'] / 1000.0
            speedup = no_cache_s / with_cache_s if with_cache_s > 0 else float('inf')
            f.write(f"| {endpoint_name} | {no_cache_s:.4f} | {with_cache_s:.4f} | {speedup:.2f}x |\n")

    print('Benchmark complete. Results written to', RESULTS_FILE)
    print('Markdown benchmark report written to', md_file)


if __name__ == '__main__':
    run_all()
