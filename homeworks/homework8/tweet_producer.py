#!/usr/bin/env python3
"""
Homework 8: stream tweet-like events to Kafka from an existing CSV file.
"""

import csv
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

INPUT_FILE = os.getenv("INPUT_FILE", "/data/amazon_reviews.csv")
TOPIC = os.getenv("TOPIC", "tweets")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

MIN_MSGS_PER_SEC = float(os.getenv("MIN_MSGS_PER_SEC", "10"))
MAX_MSGS_PER_SEC = float(os.getenv("MAX_MSGS_PER_SEC", "15"))
MAX_RUNTIME_SECONDS = int(os.getenv("MAX_RUNTIME_SECONDS", "300"))

CONNECT_RETRIES = int(os.getenv("CONNECT_RETRIES", "30"))
CONNECT_RETRY_DELAY_SECONDS = float(os.getenv("CONNECT_RETRY_DELAY_SECONDS", "2"))


def parse_bootstrap_servers(raw: str) -> List[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def iter_rows(csv_path: Path) -> Iterable[Dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def build_tweet(row: Dict[str, str], sequence: int) -> Dict[str, str]:
    now_iso = datetime.now(timezone.utc).isoformat()

    text = row.get("review_body") or row.get("review_headline") or ""
    text = " ".join(text.split())
    if len(text) > 280:
        text = f"{text[:277]}..."

    return {
        "tweet_id": row.get("review_id") or f"generated-{sequence}",
        "user_id": row.get("customer_id") or "unknown",
        "text": text,
        "created_at": now_iso,
        # Requirement: replace source timestamp with current time before send.
        "review_date": now_iso,
        "original_review_date": row.get("review_date"),
        "product_id": row.get("product_id"),
        "star_rating": row.get("star_rating"),
        "source": "amazon_reviews",
    }


def connect_producer() -> KafkaProducer:
    servers = parse_bootstrap_servers(BOOTSTRAP_SERVERS)
    if not servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS is empty.")

    last_error: Exception | None = None
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                acks="all",
                retries=5,
                linger_ms=50,
                value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
            )

            if producer.bootstrap_connected():
                print(f"Connected to Kafka ({','.join(servers)})")
                return producer

            producer.close()
            raise NoBrokersAvailable("Kafka broker is not ready yet.")
        except Exception as exc:  # pragma: no cover
            last_error = exc
            print(
                f"Kafka connection attempt {attempt}/{CONNECT_RETRIES} failed: {exc}",
                file=sys.stderr,
            )
            time.sleep(CONNECT_RETRY_DELAY_SECONDS)

    raise RuntimeError(
        f"Could not connect to Kafka after {CONNECT_RETRIES} attempts: {last_error}"
    )


def main() -> int:
    input_path = Path(INPUT_FILE)
    if not input_path.exists():
        print(f"Input file does not exist: {input_path}", file=sys.stderr)
        return 1

    min_rate = min(MIN_MSGS_PER_SEC, MAX_MSGS_PER_SEC)
    max_rate = max(MIN_MSGS_PER_SEC, MAX_MSGS_PER_SEC)
    if min_rate <= 0:
        print("Message rate must be greater than 0.", file=sys.stderr)
        return 1

    sleep_min = 1.0 / max_rate
    sleep_max = 1.0 / min_rate

    print(f"Streaming from: {input_path}")
    print(f"Topic: {TOPIC}")
    print(f"Rate: {min_rate:.1f} - {max_rate:.1f} messages/sec")
    if MAX_RUNTIME_SECONDS > 0:
        print(f"Max runtime: {MAX_RUNTIME_SECONDS} seconds")

    producer = connect_producer()

    sent = 0
    start = time.monotonic()

    try:
        for sequence, row in enumerate(iter_rows(input_path), start=1):
            if MAX_RUNTIME_SECONDS > 0 and (time.monotonic() - start) >= MAX_RUNTIME_SECONDS:
                break

            tweet = build_tweet(row, sequence)
            future = producer.send(TOPIC, value=tweet)
            future.get(timeout=15)
            sent += 1

            if sent % 100 == 0:
                elapsed = time.monotonic() - start
                avg_rate = sent / elapsed if elapsed > 0 else 0.0
                print(f"Sent {sent} messages (avg {avg_rate:.2f} msg/s)")

            time.sleep(random.uniform(sleep_min, sleep_max))
    except KafkaError as exc:
        print(f"Kafka send failed: {exc}", file=sys.stderr)
        return 1
    finally:
        producer.flush()
        producer.close()

    elapsed = time.monotonic() - start
    avg_rate = sent / elapsed if elapsed > 0 else 0.0
    print(f"Finished. Sent {sent} messages in {elapsed:.1f}s (avg {avg_rate:.2f} msg/s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
