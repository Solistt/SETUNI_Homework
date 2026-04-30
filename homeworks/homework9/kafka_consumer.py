#!/usr/bin/env python3
"""
Homework 9: read Kafka messages from topic "tweets" and write minute-based CSV files.
"""

import csv
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from kafka import KafkaConsumer
from kafka.errors import KafkaError

TOPIC = os.getenv("TOPIC", "tweets")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
GROUP_ID = os.getenv("GROUP_ID", "hw9_tweet_consumer_group")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/output"))
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")

POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))
MAX_BATCH_RECORDS = int(os.getenv("MAX_BATCH_RECORDS", "500"))
LOG_EVERY = int(os.getenv("LOG_EVERY", "100"))

CONNECT_RETRIES = int(os.getenv("CONNECT_RETRIES", "40"))
CONNECT_RETRY_DELAY_SECONDS = float(os.getenv("CONNECT_RETRY_DELAY_SECONDS", "2"))

STOP_REQUESTED = False


def parse_bootstrap_servers(raw: str) -> List[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def request_stop(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def parse_created_at(value: Any) -> datetime:
    now = datetime.now(timezone.utc)
    if value is None:
        return now

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (ValueError, OSError):
            return now

    raw = str(value).strip()
    if not raw:
        return now

    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        parsed = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            return now

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def decode_payload(raw: bytes) -> Dict[str, Any]:
    try:
        payload = json.loads(raw.decode("utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {}


def extract_row(payload: Dict[str, Any]) -> Tuple[datetime, List[str]]:
    created_dt = parse_created_at(payload.get("created_at"))

    author_id = payload.get("author_id") or payload.get("user_id") or payload.get("customer_id") or ""
    text = payload.get("text")
    if text is None:
        text = payload.get("review_body") or payload.get("review_headline") or ""

    compact_text = " ".join(str(text).split())
    row = [str(author_id), created_dt.isoformat(), compact_text]
    return created_dt, row


class CsvMinuteWriter:
    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.current_path: Optional[Path] = None
        self.current_handle = None
        self.current_writer = None

    def _target_path(self, created_dt: datetime) -> Path:
        return self.output_dir / created_dt.strftime("tweets_%d_%m_%Y_%H_%M.csv")

    def _switch_file(self, target_path: Path) -> None:
        if self.current_path == target_path and self.current_writer is not None:
            return

        self.close_current()

        exists = target_path.exists()
        self.current_handle = target_path.open("a", encoding="utf-8", newline="")
        self.current_writer = csv.writer(self.current_handle)
        if not exists:
            self.current_writer.writerow(["author_id", "created_at", "text"])

        self.current_path = target_path
        print(f"Writing to {target_path.name}")

    def write(self, created_dt: datetime, row: List[str]) -> None:
        target_path = self._target_path(created_dt)
        self._switch_file(target_path)
        self.current_writer.writerow(row)
        self.current_handle.flush()

    def close_current(self) -> None:
        if self.current_handle is not None:
            self.current_handle.close()
        self.current_handle = None
        self.current_writer = None
        self.current_path = None

    def close(self) -> None:
        self.close_current()


def connect_consumer() -> KafkaConsumer:
    servers = parse_bootstrap_servers(BOOTSTRAP_SERVERS)
    if not servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS is empty")

    last_error: Optional[Exception] = None
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=servers,
                group_id=GROUP_ID,
                enable_auto_commit=True,
                auto_offset_reset=AUTO_OFFSET_RESET,
                value_deserializer=decode_payload,
            )

            # Force metadata fetch to ensure broker connectivity.
            consumer.topics()
            print(f"Connected to Kafka ({','.join(servers)})")
            return consumer
        except Exception as exc:  # pragma: no cover
            last_error = exc
            print(
                f"Connection attempt {attempt}/{CONNECT_RETRIES} failed: {exc}",
                file=sys.stderr,
            )
            time.sleep(CONNECT_RETRY_DELAY_SECONDS)

    raise RuntimeError(f"Could not connect to Kafka after retries: {last_error}")


def main() -> int:
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    print(f"Topic: {TOPIC}")
    print(f"Consumer group: {GROUP_ID}")
    print(f"Output directory: {OUTPUT_DIR}")

    writer = CsvMinuteWriter(OUTPUT_DIR)
    consumer = connect_consumer()
    processed = 0

    try:
        while not STOP_REQUESTED:
            batches = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=MAX_BATCH_RECORDS)
            if not batches:
                continue

            for records in batches.values():
                for message in records:
                    payload = message.value
                    if not payload:
                        continue

                    created_dt, row = extract_row(payload)
                    writer.write(created_dt, row)
                    processed += 1

                    if processed % LOG_EVERY == 0:
                        print(f"Processed {processed} messages")
    except KafkaError as exc:
        print(f"Kafka consumer error: {exc}", file=sys.stderr)
        return 1
    finally:
        writer.close()
        consumer.close()

    print(f"Consumer stopped. Total messages processed: {processed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
