"""
Homework 6: Amazon Reviews ETL with Apache Spark → MongoDB
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ---------------------------------------------------------------------------
# Configuration (override via environment variables)
# ---------------------------------------------------------------------------
CSV_PATH = os.getenv("CSV_PATH", "/data/amazon_reviews.csv")

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:rootpass@mongodb:27017/amazon_reviews?authSource=admin",
)
MONGO_DB = os.getenv("MONGO_DB", "amazon_reviews")

PRODUCT_SUMMARY_COLLECTION = "product_reviews_summary"
CUSTOMER_REVIEW_COLLECTION = "customer_review_counts"
MONTHLY_REVIEWS_COLLECTION = "monthly_product_reviews"

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("AmazonReviewsETL")
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Data Ingestion
# ---------------------------------------------------------------------------
print(">>> Loading CSV...")
raw_df = (
    spark.read.option("header", "true")
    .option("sep", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .csv(CSV_PATH)
)

print(f"    Raw row count: {raw_df.count()}")

# ---------------------------------------------------------------------------
# 2. Data Cleaning
# ---------------------------------------------------------------------------
print(">>> Cleaning data...")

# --- 2a. Report nulls per column before cleaning ---
print("    Null counts per column (raw):")
null_counts = raw_df.select(
    [F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), c)).alias(c) for c in raw_df.columns]
)
null_counts.show(vertical=True, truncate=False)

# --- 2b. Drop rows where critical columns are null or blank ---
critical_cols = ["review_id", "product_id", "star_rating", "review_date"]
clean_df = raw_df.dropna(subset=critical_cols)

# Also remove rows where critical columns are empty strings (common in TSV data)
for col_name in critical_cols:
    clean_df = clean_df.filter(F.trim(F.col(col_name)) != "")

dropped = raw_df.count() - clean_df.count()
print(f"    Dropped {dropped} rows with null/blank critical values.")

# Cast star_rating to integer
clean_df = clean_df.withColumn("star_rating", F.col("star_rating").cast(IntegerType()))

# Convert review_date string to DateType
clean_df = clean_df.withColumn("review_date", F.to_date(F.col("review_date"), "yyyy-MM-dd"))

# Filter only verified purchases
clean_df = clean_df.filter(F.col("verified_purchase").isin("1", "Y", "y", "true", "True"))

print(f"    Cleaned row count (verified purchases only): {clean_df.count()}")

# Cache to avoid re-reading for multiple aggregations
clean_df.cache()

# ---------------------------------------------------------------------------
# 3. Aggregations
# ---------------------------------------------------------------------------

# --- 3a. Total reviews + average star rating per product ---
print(">>> Aggregating: product review summary...")
product_summary_df = clean_df.groupBy("product_id").agg(
    F.count("review_id").alias("total_reviews"),
    F.round(F.avg("star_rating"), 2).alias("avg_star_rating"),
)

# --- 3b. Total verified reviews per customer ---
print(">>> Aggregating: customer review counts...")
customer_reviews_df = clean_df.groupBy("customer_id").agg(
    F.count("review_id").alias("total_verified_reviews"),
)

# --- 3c. Monthly review count per product ---
print(">>> Aggregating: monthly reviews per product...")
monthly_reviews_df = clean_df.withColumn(
    "year", F.year("review_date")
).withColumn(
    "month", F.month("review_date")
).groupBy("product_id", "year", "month").agg(
    F.count("review_id").alias("review_count"),
).orderBy("product_id", "year", "month")

# ---------------------------------------------------------------------------
# 4. Write to MongoDB
# ---------------------------------------------------------------------------

def write_to_mongo(df, collection: str, mode: str = "overwrite") -> None:
    print(f">>> Writing {df.count()} rows to collection '{collection}'...")
    (
        df.write.format("mongodb")
        .mode(mode)
        .option("connection.uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", collection)
        .save()
    )
    print(f"    Done → {collection}")


write_to_mongo(product_summary_df, PRODUCT_SUMMARY_COLLECTION)
write_to_mongo(customer_reviews_df, CUSTOMER_REVIEW_COLLECTION)
write_to_mongo(monthly_reviews_df, MONTHLY_REVIEWS_COLLECTION)

# ---------------------------------------------------------------------------
# 5. Sample output for verification
# ---------------------------------------------------------------------------
print("\n>>> Sample — product_reviews_summary:")
product_summary_df.show(5, truncate=False)

print(">>> Sample — customer_review_counts:")
customer_reviews_df.show(5, truncate=False)

print(">>> Sample — monthly_product_reviews:")
monthly_reviews_df.show(10, truncate=False)

spark.stop()
print("\nETL completed successfully.")
