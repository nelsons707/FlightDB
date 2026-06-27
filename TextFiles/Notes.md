# OpenSky API Side Project

# Goals 
1. Learn streaming technologies
2. Create an analytical dataset that we can use real time to answer some metrics
3. Metric Ideas: 
    1. Identify the airport that has the most flights leaving from it within the past 24 hours
    2. Identify the airplane that has the most miles on it within the last 30 days
    3. Number of flights in air at this moment
    4. Number of flights in air by region
    5. Identify the airport that is the "worse" to have a connection in


# Architecture

0. **Message schema** - define a flat record per aircraft per poll, e.g.:
    - `icao24`, `callsign`, `origin_country`
    - `longitude`, `latitude`, `altitude`, `velocity`, `heading`, `vertical_rate`
    - `on_ground`, `time_position`, `last_contact`

1. **Producer (Python)** - authenticates via OAuth2 (registered account), polls OpenSky
   `/states/all` every 300s (5 min) for the continental US bounding box
   (lamin=24.4, lomin=-125.0, lamax=49.4, lomax=-66.9), flattens the response into one
   message per aircraft, and publishes each to Kafka.
    - Credit cost: continental US is >400 sq deg = 4 credits/call. At 300s intervals,
      288 calls/day x 4 = 1,152 credits/day (~29% of the 4,000/day standard budget).

2. **Kafka** - single topic (e.g. `opensky-states`), keyed by `icao24` so all updates for
   a given aircraft land on the same partition (useful for ordering / windowed state).
    - Running locally via Docker: `docker run -d --name kafkalocal -p 9092:9092 apache/kafka:4.3.0`
    - Topic retention set to 12h (default is 7 days):
      `docker exec kafkalocal /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --topic opensky-states --add-config retention.ms=43200000`
      (this is a per-container config and needs to be reapplied if the container is recreated)

3. **Spark Structured Streaming (speed lane)** - consumes from the Kafka topic, parses/cleans
   the JSON, and writes:
    - **Bronze** table - raw/cleaned events, append-only
    - **"Live" gold tables** - continuously upserted "right now" metrics (e.g. flights
      currently in air, flights by region, current position per aircraft)

4. **Airflow + dbt (batch lane)** - Airflow runs on a schedule (e.g. every 15-30 min) and
   triggers dbt models that transform bronze -> silver -> "rolling window" gold tables for
   metrics that don't need to be instantaneous, e.g.:
    - most flights departed per airport in the last 24h
    - most miles flown per aircraft in the last 30 days
   dbt also handles data quality tests/dedup on the bronze -> silver step.

5. **Postgres** - serves as the warehouse for all three layers (bronze/silver/gold) and is
   shared by both lanes above.

6. **Dashboard (React)** - final step. A React app queries Postgres (via a small API layer,
   e.g. FastAPI/Express). "Live" gold tables (speed lane) update second-by-second; "rolling
   window" gold tables (batch lane) show "as of last dbt run".



# Target Metrics

## Real-time (Spark → gold_live)
Computed from the current state vector snapshot each poll. Updated every 5 minutes.

| Metric | gold_live table | Notes |
|--------|----------------|-------|
| Flights in air right now | `summary` | Count of `on_ground = false` |
| Flights by US census region | `flights_by_region` | 4 regions: Northeast, South, Midwest, West (lat/lon bounding box) |
| Flight phase breakdown | `flight_phases` | `vertical_rate` > +2 m/s = climbing, < -2 = descending, else cruising |
| Fastest aircraft right now | `fastest_aircraft` | Top N by `velocity`; flag > 290 m/s as likely military |
| Emergency squawk monitor | `emergency_squawks` | Squawk 7700 = general emergency, 7600 = radio failure, 7500 = hijack |
| Fleet breakdown by airline | `fleet_by_airline` | Callsign prefix → carrier (AAL=American, DAL=Delta, UAL=United, SWA=Southwest, N-prefix=GA) |

## Batch (Airflow + dbt → gold_batch)
Computed from accumulated bronze history on a rolling window. Runs hourly.

| Metric | Window | Notes |
|--------|--------|-------|
| Aircraft with most miles flown | 30 days | Distance from lat/lon deltas |
| Busiest aircraft by frequency | 24h | Most unique polls seen (proxy for high-utilization) |
| Traffic by hour of day | rolling | Flight counts bucketed by hour — shows morning/evening peaks |
| Airport with most departures | 24h | ⚠️ Needs `on_ground` transition detection or `/flights/departure` API — stubbed |
| Worst connecting airport | rolling | ⚠️ Needs departure/arrival event detection — stubbed |

---

# Running the Stack

## Start Kafka
```bash
docker compose up -d
```

## Run the Producer
```bash
python Producer/producer.py
```

## Run the Spark Consumers
Bronze and gold_live run as two separate streaming jobs (own checkpoint each), so start
each in its own terminal:
```bash
# Bronze: Kafka → bronze.states (append)
JAVA_HOME=/usr/local/opt/openjdk@17 \
.venv/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,org.postgresql:postgresql:42.7.3 \
  Consumer/bronze_consumer.py

# Gold live: Kafka → gold_live.* (snapshot replace)
JAVA_HOME=/usr/local/opt/openjdk@17 \
.venv/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,org.postgresql:postgresql:42.7.3 \
  Consumer/gold_live_consumer.py
```
First run downloads JARs to ~/.ivy2/ (takes ~1 min). Subsequent runs use the cache.

**Prerequisites:**
- Kafka running (`docker compose up -d`)
- Postgres running on localhost:5433, database `OpenSky`, schema DDL applied (`SQL/bronze/states.sql` + all `SQL/gold_live/*.sql`)
- Producer running (or messages already in `opensky-states` topic)

## Apply Schema DDL
Connect to Postgres and run in order:
```
SQL/bronze/states.sql
SQL/gold_live/summary.sql
SQL/gold_live/flights_by_region.sql
SQL/gold_live/flight_phases.sql
SQL/gold_live/fastest_aircraft.sql
SQL/gold_live/emergency_squawks.sql
SQL/gold_live/fleet_by_airline.sql
```

## Verify Consumer is Working
```bash
# Check bronze is getting rows
psql -h localhost -p 5433 -U spark_user -d OpenSky -c "SELECT COUNT(*), MAX(ingested_at) FROM bronze.states;"

# Check flight_phases is updating
psql -h localhost -p 5433 -U spark_user -d OpenSky -c "SELECT * FROM gold_live.flight_phases;"
```

---

# Architecture Notes

1. Python Script is the producer of the data, authenticates via OAuth2, and polls OpenSky
   every 300s (5 min) for the continental US bounding box (~1,152 credits/day of 4,000 budget).
2. Kafka is the message queue that sits after the producer.
3. Spark Streaming reads from the Kafka topic, writes raw events to a bronze table, and
   continuously upserts "live" gold tables (real-time metrics) to Postgres.
4. Postgres holds bronze/silver/gold tables and is shared by both the streaming and batch lanes.
5. Airflow orchestrates scheduled dbt runs that transform bronze -> silver -> "rolling window"
   gold tables (e.g. 24h/30-day metrics) - these don't need to be real-time.
6. React dashboard queries Postgres gold tables - some live (from Spark), some periodically
   refreshed (from dbt).

---

# Backlog / To-Dos

- [ ] **CI/CD pipeline** — automate deploys. Two targets:
    - *Database*: apply schema DDL (`SQL/bronze/*`, `SQL/gold_live/*`) on merge — idempotent
      migrations so re-runs are safe. Versioned migrations (Flyway/Alembic/sqitch) over
      raw `psql` so we get ordering + rollback.
    - *Front-end* (later): build + deploy the React dashboard once it exists.
    - Likely GitHub Actions (repo already on GitHub).

- [ ] **"Flights near me" widget** — dashboard input takes an address, returns flights
      currently in air within a 50-mile radius. Needs a new `gold_live` table + a data
      model decision. Open questions to figure out later:
    - *Address → lat/lon*: geocode on the API/front-end side (the DB should store the
      already-resolved point, not the address string).
    - *Radius query shape*: do we precompute a table at all, or just query the live
      current-position table with a distance filter at request time? 50mi over a snapshot
      of US flights is small — may not need its own gold table.
    - *Distance math*: haversine vs. PostGIS `geography` + `ST_DWithin`. PostGIS gives a
      GiST spatial index (fast radius lookups) but adds an extension dependency.
    - *Granularity*: one current-position row per `icao24` (latest poll), so the widget
      needs "most recent state per aircraft" — overlaps with a generic `current_positions`
      gold table we don't have yet (doesn't exist — needs to be designed).

- [ ] **Bronze retention policy** — cap how long data lives in `bronze.states`: keep
      45 days, drop older (30d batch window + buffer). The term is *data retention / TTL*.
      Options:
    - *Scheduled DELETE*: `DELETE FROM bronze.states WHERE poll_date < now() - interval '30 days'`
      on a cron/Airflow job. Simple, but DELETEs bloat the table (needs VACUUM).
    - *Partition + drop* (preferred): if `bronze.states` is partitioned by `poll_date`
      (we already write that column), dropping a whole day's partition is instant and
      bloat-free vs. row-by-row DELETE. Worth checking whether the table is actually
      partitioned or just has the column.
    - Sanity-check 30d against disk: ~?? rows/poll × 288 polls/day × 30d.