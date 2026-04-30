#!/usr/bin/env python3
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, from_json, to_timestamp
from pyspark.sql.types import StringType, StructField, StructType

PROCESSED_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("page_title", StringType(), True),
    ]
)


def write_batch(batch_df: DataFrame, batch_id: int, keyspace: str, table: str) -> None:
    del batch_id
    if batch_df.rdd.isEmpty():
        return

    (
        batch_df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(keyspace=keyspace, table=table)
        .save()
    )


def main() -> None:
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    processed_topic = os.getenv("PROCESSED_TOPIC", "processed")
    cassandra_host = os.getenv("CASSANDRA_HOST", "cassandra")
    cassandra_keyspace = os.getenv("CASSANDRA_KEYSPACE", "hw10")
    cassandra_table = os.getenv("CASSANDRA_TABLE", "page_creations")
    checkpoint_dir = os.getenv(
        "CHECKPOINT_DIR", "/opt/hw10/checkpoints/write_to_cassandra"
    )

    spark = (
        SparkSession.builder.appName("hw10-write-to-cassandra")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.cassandra.connection.host", cassandra_host)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_input_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", processed_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = kafka_input_df.select(
        from_json(col("value").cast("string"), PROCESSED_SCHEMA).alias("event")
    ).select("event.*")

    prepared_df = parsed_df.select(
        col("user_id").cast("string").alias("user_id"),
        col("domain").alias("domain"),
        coalesce(
            to_timestamp(col("created_at")),
            to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ssX"),
        ).alias("created_at"),
        col("page_title").alias("page_title"),
    ).where(
        col("user_id").isNotNull()
        & col("domain").isNotNull()
        & col("created_at").isNotNull()
        & col("page_title").isNotNull()
    )

    query = (
        prepared_df.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .foreachBatch(
            lambda batch_df, batch_id: write_batch(
                batch_df, batch_id, cassandra_keyspace, cassandra_table
            )
        )
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
