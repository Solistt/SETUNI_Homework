# Homework 6 – Amazon Reviews with Apache Spark & MongoDB

## Overview

PySpark ETL pipeline that:
1. **Ingests** the Amazon Reviews TSV dataset into a Spark DataFrame
2. **Cleans** data (drops nulls in critical columns, parses dates, filters verified purchases)
3. **Aggregates** three views and persists them to MongoDB collections

---

## Collections

| Collection | Key fields | Purpose |
|---|---|---|
| `product_reviews_summary` | `product_id` | Total reviews + avg star rating per product |
| `customer_review_counts` | `customer_id` | Total verified reviews per customer |
| `monthly_product_reviews` | `product_id`, `year`, `month` | Monthly trend per product |

---

## Prerequisites

- Docker & Docker Compose
- The Amazon Reviews TSV file (tab-separated, with header row)

---

## Running

```bash
# From this directory
chmod +x run.sh
./run.sh /path/to/amazon_reviews.tsv
```

The script will:
1. Copy the dataset into `./data/amazon_reviews.csv`
2. Start MongoDB + Spark cluster via Docker Compose
3. Submit the PySpark job
4. Print a connection string for manual queries

### Manual step-by-step

```bash
# Copy dataset
cp /path/to/amazon_reviews.tsv ./data/amazon_reviews.csv

# Start infrastructure
docker compose up -d mongodb spark-master spark-worker

# Run ETL
docker compose run --rm spark-submit
```

---

## Querying Results

```bash
# Connect to MongoDB
mongosh 'mongodb://root:rootpass@localhost:27018/amazon_reviews?authSource=admin'
```

```js
// Top 10 products by review count
db.product_reviews_summary.find().sort({ total_reviews: -1 }).limit(10)

// Reviews for a specific product
db.product_reviews_summary.findOne({ product_id: "B00XYZ" })

// Most active customers (verified purchases)
db.customer_review_counts.find().sort({ total_verified_reviews: -1 }).limit(10)

// Monthly trend for a product
db.monthly_product_reviews.find({ product_id: "B00XYZ" }).sort({ year: 1, month: 1 })
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `CSV_PATH` | `/data/amazon_reviews.csv` | Path inside the Spark container |
| `MONGO_URI` | `mongodb://root:rootpass@mongodb:27017/amazon_reviews?authSource=admin` | MongoDB connection URI |
| `MONGO_DB` | `amazon_reviews` | Target database name |

---

## Teardown

```bash
docker compose down -v   # removes containers and the mongo_data volume
```
