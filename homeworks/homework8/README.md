# Homework 8 - Kafka Producer (Tweet Stream Simulation)

## Overview

This solution uses the same dataset as Homework 6/7 (`amazon_reviews.csv`) and sends
rows to Kafka as tweet-like JSON messages.

Behavior:

1. Reads rows sequentially from the CSV file
2. Replaces the row timestamp with current UTC time
3. Publishes each row as an individual message to topic `tweets`
4. Sends at a randomized rate between 10 and 15 messages/second
5. Runs for ~5 minutes by default (`MAX_RUNTIME_SECONDS=300`)

## Files

- `tweet_producer.py` - Python Kafka producer
- `Dockerfile` - producer container image
- `docker-compose.yml` - Kafka + Zookeeper stack
- `build.sh` - builds producer image
- `run.sh` - starts Kafka and runs producer container in the same network
- `consume.sh` - reads back messages from Kafka topic

## Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- Dataset file from previous homework:
  - default path priority used by script:
    1. `../homework7/data/amazon_reviews.csv`
    2. `../homework6/data/amazon_reviews.csv`

## Run

```bash
cd homeworks/homework8
chmod +x build.sh run.sh consume.sh
./run.sh
```

Optional (custom dataset path):

```bash
./run.sh /absolute/path/to/amazon_reviews.csv
```

Optional (custom topic/rate/runtime):

```bash
TOPIC=tweets MIN_MSGS_PER_SEC=10 MAX_MSGS_PER_SEC=15 MAX_RUNTIME_SECONDS=300 ./run.sh
```

## Verify topic contents

```bash
./consume.sh 20
```

Equivalent direct command:

```bash
docker compose -f docker-compose.yml exec kafka \
  kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic tweets \
    --from-beginning \
    --max-messages 20
```

## Required screenshots

1. Kafka installation running:

```bash
docker ps
```

Expected containers include `hw8_zookeeper` and `hw8_kafka`.

2. Program container execution:

- terminal output of `./run.sh` showing producer progress (e.g., `Sent 100 messages...`)

3. Topic read result:

- terminal output of `./consume.sh 20` showing JSON messages from topic `tweets`

## Stop and cleanup

```bash
docker compose -f docker-compose.yml down -v
```
