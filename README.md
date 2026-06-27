# FlightDB — OpenSky Pipeline

Real-time flight pipeline: **OpenSky → Kafka → Spark Structured Streaming → Postgres**.

This branch (`write_bronze`) covers the bronze ingestion lane: the Spark consumer reads the
`opensky-states` Kafka topic and appends raw state vectors to `bronze.states` in Postgres.

```
Producer (OpenSky API)  →  Kafka (opensky-states)  →  bronze_consumer.py  →  Postgres bronze.states
```

## Prerequisites

- Python venv at `.venv` (project root)
- Docker (for Kafka)
- Postgres running on `localhost:5433`, database `OpenSky`, with `bronze.states` created
- `Creds/PGCreds.json` — Postgres connection (gitignored)
- Java 17 (`/usr/local/opt/openjdk@17`) for Spark

## Setup

Activate the virtual environment first (every new terminal):

```bash
cd /Users/nelsonswasono/PycharmProjects/OpenSky
source .venv/bin/activate
```

> The venv dir is `.venv` (leading dot). `source .venv/bin/activate` — not `venv/`.

## Running the stack

Start each piece in its own terminal (each needs the venv activated).

### 1. Kafka

```bash
docker compose up -d
```

### 2. Bronze consumer (start before the producer)

`startingOffsets=latest`, so start the consumer first — it bookmarks the current end of the
topic and only reads messages produced after it starts.

```bash
JAVA_HOME=/usr/local/opt/openjdk@17 \
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,org.postgresql:postgresql:42.7.3 \
  Consumer/bronze_consumer.py
```

First run downloads the Spark/Postgres JARs (~1 min); later runs use the ivy cache.
Watch for `bronze batch N: writing M rows`.

### 3. Producer

```bash
python Producer/producer.py
```

Polls OpenSky every 300s and publishes one Kafka message per aircraft. Run only **one**
producer at a time (each instance burns OpenSky API credits).

## One-time database setup

The consumer connects as `spark_user`, which needs privileges on the target schema. Run
`SQL/grants.sql` as the schema owner / a superuser (e.g. via DBeaver or psql):

```bash
psql -h localhost -p 5433 -U postgres -d OpenSky -f SQL/grants.sql
```

## Verify

```bash
psql -h localhost -p 5433 -U spark_user -d OpenSky \
  -c "SELECT COUNT(*), MIN(ingested_at), MAX(ingested_at) FROM bronze.states;"
```

## Troubleshooting

- **`permission denied for schema bronze`** — `spark_user` is missing grants. Run
  `SQL/grants.sql` (see above).
- **`NullPointerException` in `KafkaMicroBatchStream.metrics` after a write** — usually a
  stale checkpoint replaying an uncommitted batch. Stop the consumer, wipe its checkpoint,
  and restart:
  ```bash
  rm -rf /tmp/flightdb-checkpoint/bronze
  ```
  Note: with `startingOffsets=latest`, a wiped checkpoint skips anything already in the topic
  and resumes from the next new message.
- **Consumer logs `empty, skipping`** — no new messages landed in that 5-min trigger window.
  Confirm the producer is running and has published since the consumer started.
