# Homework 10 - Processing Event Streams

## Goal

This solution processes real-time Wikipedia page creation events from:

https://stream.wikimedia.org/v2/stream/page-create

Pipeline:

generator -> Kafka topic `input` -> Spark filter/transform -> Kafka topic `processed` -> Spark writer -> Cassandra

## Implemented Requirements

- Kafka setup with two topics: `input`, `processed`
- Single-node Cassandra with table `hw10.page_creations`
- Containerized generator program with Dockerfile in `generator/`
- Spark installation in detached mode with two containers: `spark-master`, `spark-worker`
- Spark Streaming job #1 reads `input`, filters by required domains and non-bot users, writes to `processed`
- Spark Streaming job #2 reads `processed`, writes selected fields to Cassandra

## Project Structure

- `docker-compose.yml` - full infrastructure and app stack
- `generator/wikimedia_generator.py` - Wikimedia stream -> Kafka `input`
- `spark_jobs/process_stream.py` - Kafka `input` -> Kafka `processed`
- `spark_jobs/write_to_cassandra.py` - Kafka `processed` -> Cassandra
- `cassandra/init.cql` - keyspace/table initialization
- `run.sh` - start full pipeline
- `stop.sh` - stop full pipeline
- `show_results.sh` - print Kafka topic samples + Cassandra rows
- `start_kafka.sh`, `stop_kafka.sh` - Kafka installation lifecycle
- `start_cassandra.sh`, `stop_cassandra.sh` - Cassandra installation lifecycle
- `start_spark.sh`, `stop_spark.sh` - Spark installation lifecycle

## Data Processing Logic

Events are kept only when:

- `domain` is one of:
  - `en.wikipedia.org`
  - `www.wikidata.org`
  - `commons.wikimedia.org`
- `user_is_bot` is `false`

`processed` topic payload fields:

- `user_id`
- `domain`
- `created_at`
- `page_title`

Cassandra table stores exactly these fields.

## Quick Start

```bash
cd homeworks/homework10
chmod +x *.sh
./run.sh
```

Default generator runtime is 300 seconds (5 minutes).

Custom runtime example (3 minutes):

```bash
MAX_RUNTIME_SECONDS=180 ./run.sh
```

## Start/Remove Installations Separately

Kafka:

```bash
./start_kafka.sh
./stop_kafka.sh
```

Cassandra:

```bash
./start_cassandra.sh
./stop_cassandra.sh
```

Spark:

```bash
./start_spark.sh
./stop_spark.sh
```

## Demonstration Commands (for screenshots)

1. Running containers:

```bash
docker ps
```

2. Kafka topics contents (input + processed):

```bash
./show_results.sh 10
```

3. Cassandra query results:

```bash
docker compose -f docker-compose.yml exec -T cassandra \
  cqlsh -e "SELECT user_id, domain, created_at, page_title FROM hw10.page_creations LIMIT 20;"
```

## Stop Everything

```bash
./stop.sh
```
