#!/usr/bin/env python3
import json
import logging
import os
import signal
import sys
import time
from typing import Iterator, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

SHOULD_STOP = False


def setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def parse_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logging.warning("Invalid integer for %s=%r, using default=%d", name, value, default)
        return default


def handle_shutdown(signum: int, _frame: Optional[object]) -> None:
    del signum
    global SHOULD_STOP
    SHOULD_STOP = True


def build_producer(bootstrap_servers: str, topic: str) -> KafkaProducer:
    while not SHOULD_STOP:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
                retries=5,
                linger_ms=50,
            )
            producer.partitions_for(topic)
            logging.info("Connected to Kafka at %s", bootstrap_servers)
            return producer
        except NoBrokersAvailable:
            logging.warning("Kafka is not ready yet at %s, retrying in 3s", bootstrap_servers)
            time.sleep(3)

    raise RuntimeError("Stopped while waiting for Kafka")


def iter_sse_payloads(
    session: requests.Session,
    endpoint_url: str,
    stop_deadline: Optional[float],
) -> Iterator[str]:
    with session.get(endpoint_url, stream=True, timeout=60) as response:
        response.raise_for_status()

        buffer = []
        for raw_line in response.iter_lines(decode_unicode=True):
            if SHOULD_STOP:
                return

            if stop_deadline is not None and time.time() >= stop_deadline:
                return

            if raw_line is None:
                continue

            line = raw_line.strip()

            if line == "":
                if buffer:
                    yield "\n".join(buffer)
                    buffer.clear()
                continue

            if line.startswith(":"):
                continue

            if line.startswith("data:"):
                buffer.append(line[5:].lstrip())

        if buffer:
            yield "\n".join(buffer)


def main() -> int:
    setup_logging()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    endpoint_url = os.getenv(
        "WIKIMEDIA_STREAM_URL",
        "https://stream.wikimedia.org/v2/stream/page-create",
    )
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "input")
    max_runtime_seconds = parse_env_int("MAX_RUNTIME_SECONDS", 0)
    wikimedia_user_agent = os.getenv(
        "WIKIMEDIA_USER_AGENT",
        (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
    )

    stop_deadline: Optional[float] = None
    if max_runtime_seconds > 0:
        stop_deadline = time.time() + max_runtime_seconds

    logging.info("Starting Wikimedia generator")
    logging.info("Endpoint: %s", endpoint_url)
    logging.info("Kafka topic: %s", kafka_topic)
    if stop_deadline is None:
        logging.info("Runtime: unlimited")
    else:
        logging.info("Runtime limit: %d seconds", max_runtime_seconds)

    producer = build_producer(kafka_bootstrap_servers, kafka_topic)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": wikimedia_user_agent,
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
        }
    )

    sent_messages = 0

    try:
        while not SHOULD_STOP:
            if stop_deadline is not None and time.time() >= stop_deadline:
                break

            try:
                for payload in iter_sse_payloads(session, endpoint_url, stop_deadline):
                    if SHOULD_STOP:
                        break

                    if stop_deadline is not None and time.time() >= stop_deadline:
                        break

                    try:
                        event = json.loads(payload)
                    except json.JSONDecodeError:
                        logging.debug("Skipping malformed payload: %s", payload[:200])
                        continue

                    future = producer.send(kafka_topic, value=event)
                    future.get(timeout=10)
                    sent_messages += 1

                    if sent_messages % 100 == 0:
                        logging.info("Published %d messages", sent_messages)

            except requests.RequestException as exc:
                if SHOULD_STOP:
                    break
                logging.warning("Stream disconnected (%s), reconnecting in 2s", exc)
                time.sleep(2)
            except KafkaError as exc:
                if SHOULD_STOP:
                    break
                logging.warning("Kafka error (%s), retrying in 2s", exc)
                time.sleep(2)
    finally:
        try:
            producer.flush(timeout=10)
        except Exception:
            pass
        producer.close()
        session.close()

    logging.info("Stopped generator. Total published messages: %d", sent_messages)
    return 0


if __name__ == "__main__":
    sys.exit(main())
