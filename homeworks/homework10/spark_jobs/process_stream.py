#!/usr/bin/env python3
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, from_json, struct, to_json
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

ALLOWED_DOMAINS = [
    "en.wikipedia.org",
    "www.wikidata.org",
    "commons.wikimedia.org",
]

INPUT_SCHEMA = StructType(
    [
        StructField(
            "meta",
            StructType(
                [
                    StructField("domain", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "performer",
            StructType(
                [
                    StructField("user_id", LongType(), True),
                    StructField("user_is_bot", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField("created_at", StringType(), True),
        StructField("dt", StringType(), True),
        StructField("page_title", StringType(), True),
    ]
)


def main() -> None:
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    input_topic = os.getenv("INPUT_TOPIC", "input")
    output_topic = os.getenv("OUTPUT_TOPIC", "processed")
    checkpoint_dir = os.getenv(
        "CHECKPOINT_DIR", "/opt/hw10/checkpoints/process_stream"
    )

    spark = (
        SparkSession.builder.appName("hw10-process-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_input_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = kafka_input_df.select(
        from_json(col("value").cast("string"), INPUT_SCHEMA).alias("event")
    ).select("event.*")

    filtered_df = parsed_df.filter(
        col("meta.domain").isin(ALLOWED_DOMAINS)
        & (col("performer.user_is_bot") == False)  # noqa: E712
    )

    projected_df = filtered_df.select(
        col("performer.user_id").cast("string").alias("user_id"),
        col("meta.domain").alias("domain"),
        coalesce(col("created_at"), col("dt")).alias("created_at"),
        col("page_title").alias("page_title"),
    ).where(
        col("user_id").isNotNull()
        & col("domain").isNotNull()
        & col("created_at").isNotNull()
        & col("page_title").isNotNull()
    )

    output_df = projected_df.select(
        to_json(
            struct(
                col("user_id"),
                col("domain"),
                col("created_at"),
                col("page_title"),
            )
        ).alias("value")
    )

    query = (
        output_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("topic", output_topic)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
