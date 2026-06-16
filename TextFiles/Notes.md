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