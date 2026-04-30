# Homework 9 - Kafka Consumer to Local Files

## Overview

This homework reads messages continuously from Kafka topic `tweets`, extracts:

- `author_id`
- `created_at`
- `text`

and writes them to minute-based CSV files in local filesystem with format:

`tweets_dd_mm_yyyy_hh_mm.csv`

Example:

`tweets_03_05_2022_17_08.csv`

## Components

- `kafka_consumer.py` - consumer program (continuous reader + CSV writer)
- `Dockerfile` - container image for consumer
- `docker-compose.yml` - Kafka, Zookeeper, producer (from Homework 8), consumer
- `build.sh` - builds the consumer image
- `run.sh` - launches full stack: Kafka + producer + consumer
- `run_consumer.sh` - launches only consumer service
- `show_results.sh` - lists generated files and prints contents of 1-2 files

## Quick start

```bash
cd homeworks/homework9
chmod +x build.sh run.sh run_consumer.sh show_results.sh
./run.sh
```

Defaults:

- Producer duration: `900` seconds (15 minutes)
- Dataset source priority:
  1. `../homework7/data/amazon_reviews.csv`
  2. `../homework6/data/amazon_reviews.csv`

Custom dataset path:

```bash
./run.sh /absolute/path/to/amazon_reviews.csv
```

Custom runtime (10 minutes):

```bash
MAX_RUNTIME_SECONDS=600 ./run.sh
```

## Required deliverables and commands

1. Kafka installation and running containers (`docker ps`):

```bash
docker ps
```

2. Result of consumer execution (list of generated files after 10-15 min):

```bash
ls -1 output
```

3. Contents of 1-2 generated files:

```bash
./show_results.sh
```

Or manually:

```bash
head -n 20 output/tweets_*.csv
```

## Useful logs

```bash
docker compose -f docker-compose.yml logs -f consumer
docker compose -f docker-compose.yml logs -f producer
```

## Stop and cleanup

```bash
docker compose -f docker-compose.yml down -v
```
