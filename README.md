AdTech Data Pipeline (MySQL -> MongoDB)
=====================================

Overview
--------
This repository contains an ETL pipeline and analytics tooling to move relational ad engagement data (MySQL) into a document-oriented schema in MongoDB. All code, comments, and documentation are English-only and follow enterprise security best practices: no hardcoded credentials, `.env` driven configuration, and centralized connection management.

NoSQL Schema Strategy
----------------------
This project favors a user-centric document model where each user document contains an array of `sessions`, and each session contains an array of `impressions`.

Example document shape (simplified):

{
  "_id": 12345,
  "demographics": { ... },
  "sessions": [
    {
      "session_start": "2024-01-01T12:00:00Z",
      "impressions": [ { "impression_id": "...", "campaign": {...}, "click": {...} } ]
    }
  ]
}

Embedding vs Referencing — Trade-offs for AdTech Workloads
---------------------------------------------------------
- Embedding (used here for sessions and impressions):
  - Pros: Fast reads for user-centric analytics and profile building; single-document atomicity simplifies upserts; efficient for workloads where most queries fetch a user's recent sessions or impressions.
  - Cons: Documents can grow large for very active users; updating deeply nested arrays can be more costly for extremely high write throughput.

- Referencing (storing impressions as a separate collection with user_id foreign key):
  - Pros: Better for unbounded event growth and independent scaling of read/write workloads. Easier to shard impressions by time or campaign.
  - Cons: Requires joins (application-side or via $lookup) for user-centric reads, increasing query complexity and latency.

Recommended pattern for production AdTech workloads
-------------------------------------------------
- Use embedding for denormalized, read-heavy user profiles and recent sessions (hot-path queries).
- Use referencing (separate impressions table/collection) for high-volume, append-only events that must scale horizontally.
- Use TTL, bucketing, or time-partitioned collections for older impression data to keep document sizes bounded.

Security & Operations
---------------------
- Populate a root-level `.env` from `example.env`. The repository `.gitignore` already excludes `.env`.
- Docker Compose uses environment variable interpolation; no secrets are stored in `docker-compose.yml`.
- The codebase uses a centralized `Config` and `ConnectionFactory` to validate and inject credentials at runtime.

Files changed in this refactor
-----------------------------
- `src/config.py` — validated configuration loader (fail-fast).
- `src/connection.py` — connection factory for MySQL and MongoDB.
- `src/mongo_loader.py` — streaming ETL, batch processing, sessionization, bulk upserts.
- `src/mongo_queries.py` — optimized aggregation pipelines and safe JSON encoding.
- `docker-compose.yml` — healthchecks, network isolation, and secret interpolation.
- `example.env` — template for required environment variables.

Next steps
----------
- Fill `.env` from `example.env` with secure credentials and run the stack.
- Run the ETL: `python -m src.mongo_loader` (or from inside the container after building).
- Inspect output reports in `output/hw3_reports`.
