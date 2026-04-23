"""
Homework 7: Amazon Reviews ETL — PySpark → Cassandra
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

CSV_PATH = os.getenv("CSV_PATH", "/data/amazon_reviews.tsv")
CSV_SEPARATOR = os.getenv("CSV_SEPARATOR", "\t")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "amazon_reviews")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("AmazonReviews_HW7_Cassandra")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
        .getOrCreate()
    )


def write_cassandra(df, table: str) -> None:
    print(f">>> Writing to Cassandra table '{table}'...")
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=table, keyspace=CASSANDRA_KEYSPACE) \
        .save()
    print(f"    Done → {table}")


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Load ──────────────────────────────────────────────────────────────
    print(">>> Loading dataset...")
    raw_df = (
        spark.read
        .option("header", "true")
        .option("sep", CSV_SEPARATOR)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(CSV_PATH)
    )
    print(f"    Raw row count: {raw_df.count()}")

    # ── 2. Clean ─────────────────────────────────────────────────────────────
    print(">>> Cleaning data...")
    critical = ["review_id", "product_id", "customer_id", "star_rating", "review_date"]
    clean_df = raw_df.dropna(subset=critical)
    for col in critical:
        clean_df = clean_df.filter(F.trim(F.col(col)) != "")

    clean_df = clean_df.withColumn("star_rating", F.col("star_rating").cast(IntegerType()))
    clean_df = clean_df.filter(F.col("star_rating").isNotNull())
    clean_df = clean_df.filter(F.col("star_rating").between(1, 5))

    clean_df = clean_df.withColumn("review_date", F.to_date(F.col("review_date"), "yyyy-MM-dd"))
    clean_df = clean_df.filter(F.col("review_date").isNotNull())

    clean_df = clean_df.withColumn(
        "verified_purchase",
        F.col("verified_purchase").isin("1", "Y", "y", "true", "True", 1),
    )
    clean_df = clean_df.withColumn("year_month", F.date_format("review_date", "yyyy-MM"))
    clean_df = clean_df.cache()
    print(f"    Cleaned row count: {clean_df.count()}")

    # ── 3. Write review tables ────────────────────────────────────────────────
    review_cols = [
        "review_date", "review_id", "customer_id", "star_rating",
        "verified_purchase", "review_headline", "review_body", "marketplace", "product_title",
    ]

    write_cassandra(
        clean_df.select(["product_id"] + review_cols),
        "reviews_by_product",
    )
    write_cassandra(
        clean_df.select(["product_id", "star_rating"] + [c for c in review_cols if c != "star_rating"]),
        "reviews_by_product_rating",
    )
    # reviews_by_customer: customer_id is partition key — must not duplicate it from review_cols
    write_cassandra(
        clean_df.select(["customer_id"] + [c for c in review_cols if c != "customer_id"] + ["product_id"]),
        "reviews_by_customer",
    )

    # ── 4. Write aggregation tables ───────────────────────────────────────────
    write_cassandra(
        clean_df.groupBy("year_month", "product_id")
        .agg(F.count("review_id").cast(IntegerType()).alias("review_count")),
        "product_reviews_by_month",
    )
    write_cassandra(
        clean_df.filter(F.col("verified_purchase"))
        .groupBy("year_month", "customer_id")
        .agg(F.count("review_id").cast(IntegerType()).alias("review_count")),
        "customer_verified_by_month",
    )
    write_cassandra(
        clean_df.filter(F.col("star_rating").isin(1, 2))
        .groupBy("year_month", "customer_id")
        .agg(F.count("review_id").cast(IntegerType()).alias("review_count")),
        "customer_haters_by_month",
    )
    write_cassandra(
        clean_df.filter(F.col("star_rating").isin(4, 5))
        .groupBy("year_month", "customer_id")
        .agg(F.count("review_id").cast(IntegerType()).alias("review_count")),
        "customer_backers_by_month",
    )

    clean_df.unpersist()
    spark.stop()
    print("\nETL completed successfully.")


if __name__ == "__main__":
    main()
