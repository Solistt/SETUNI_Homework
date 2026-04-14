# AdTech Analytics API - Project Summary

## Overview
This project implements a REST API for AdTech analytics with Redis caching, built using FastAPI, MySQL, MongoDB, and Redis.

## Architecture
- **FastAPI**: REST API framework
- **MySQL**: Relational data storage (campaigns, impressions, clicks, advertisers)
- **MongoDB**: Document storage for user engagement history
- **Redis**: In-memory caching layer
- **Docker Compose**: Container orchestration

## API Endpoints

### 1. Campaign Performance
**GET /campaign/{campaign_id}/performance**
- Returns: impressions, clicks, CTR, ad spend
- Cache TTL: 30 seconds

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
**GET /advertiser/{advertiser_id}/spending**
- Returns: total ad spend across all campaigns
- Cache TTL: 5 minutes

```json
{
  "advertiser_id": 1,
  "total_ad_spend": 18013.55
}
```

### 3. User Engagements
**GET /user/{user_id}/engagements**
- Returns: ads user engaged with (impressions + clicks)
- Cache TTL: 1 minute

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
    },
    {
      "campaign_id": 9,
      "campaign_name": "Campaign_9",
      "advertiser_name": "Advertiser_2",
      "timestamp": "2024-12-08T12:13:05",
      "category": "Age 26-41, Finance, USA",
      "engagement_type": "click"
    }
  ]
}
```

## Caching Implementation
- **Read-through cache**: Check Redis first, query DB if miss
- **TTL expiration**: Different timeouts per endpoint
- **Cache keys**: Structured as `{endpoint}:{id}:{subendpoint}`

## Performance Benchmarking

### Results
| Endpoint | Without Cache (s) | With Cache (s) | Speedup |
|----------|-------------------|----------------|---------|
| Campaign Performance | 0.0635 | 0.0031 | 20.19x |
| Advertiser Spending | 0.0626 | 0.0027 | 23.41x |
| User Engagements | 0.0032 | 0.0023 | 1.43x |

### Key Findings
- **MySQL queries**: 20-23x speedup with Redis cache
- **MongoDB queries**: 1.43x speedup (MongoDB already fast)
- **Cache effectiveness**: Significant latency reduction for DB-heavy operations

## Docker Setup
All services running via Docker Compose:
- API (FastAPI) on port 8000
- MySQL on port 3306
- MongoDB on port 27017
- Redis on port 6379

## Data Pipeline
1. **ETL Process**: Load CSV → MySQL (relational data)
2. **MongoDB Loader**: Transform → MongoDB (user engagement docs)
3. **API**: Serve cached analytics queries

## Files Structure
```
src/
├── api.py              # FastAPI application with endpoints
├── benchmark.py        # Performance testing script
├── etl_process.py      # MySQL data loading
├── mongo_loader.py     # MongoDB ETL pipeline
├── requirements.txt    # Python dependencies
└── benchmark_results.md # Performance results

docker-compose.yml      # Container orchestration
Dockerfile             # API container build
```

## Screenshots Required
1. **API Endpoints**: Demonstrate each endpoint response
2. **Benchmark Results**: Performance comparison table
3. **Docker Containers**: Running services status
4. **Cache Demonstration**: Show cache hit vs miss behavior
5. **Database Queries**: Sample data in MySQL/MongoDB

## Usage
```bash
# Start services
docker-compose up -d

# Run benchmarks
cd src && python3 benchmark.py

# Test endpoints
curl "http://localhost:8000/campaign/1/performance"
curl "http://localhost:8000/advertiser/1/spending"
curl "http://localhost:8000/user/78770/engagements"
```