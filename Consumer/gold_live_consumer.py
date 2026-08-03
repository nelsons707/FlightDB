import contextlib

import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import build_spark, load_pg_creds, log, read_states_stream

# ── Config ─────────────────────────────────────────────────────────────────────

CHECKPOINT_DIR = "/tmp/flightdb-checkpoint/gold_live"
TRIGGER_INTERVAL = "5 minutes"

# ── Postgres connection (driver-side, for snapshot replace) ──────────────────────

def pg_conn(creds: dict):
    return psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["user"],
        password=creds["password"],
    )

# ── Snapshot helper ────────────────────────────────────────────────────────────

def latest_snapshot(batch_df: DataFrame) -> DataFrame:
    """Reduce a batch to the single most recent poll.

    The producer polls every 300s and this job triggers every 300s, but they run on
    independent clocks and drift, so a batch can contain two polls (or zero). Every
    gold_live table answers "right now", so aggregating a two-poll batch would count
    each aircraft twice. poll_time is stamped once per OpenSky response, identical
    across every record in that poll, which makes it the natural snapshot key.

    Tradeoff: if one poll ever spans two batches we process a partial snapshot rather
    than a doubled one. Undercounting one interval beats double-counting it.
    """
    latest = batch_df.agg(F.max("poll_time")).collect()[0][0]
    return batch_df.filter(F.col("poll_time") == latest)

# ── Gold live: flight phases ───────────────────────────────────────────────────

def write_flight_phases(df: DataFrame, conn) -> None:
    rows = (
        df.filter(F.col("on_ground") == False)
          .withColumn(
              "phase",
              F.when(F.col("vertical_rate") > 2,    "climbing")
               .when(F.col("vertical_rate") < -2,   "descending")
               .when(F.col("vertical_rate").isNotNull(), "cruising")
               .otherwise("unknown")
          )
          .groupBy("phase")
          .agg(F.count("*").alias("flight_count"))
          .collect()
    )

    with conn.cursor() as cur:
        cur.execute("TRUNCATE gold_live.flight_phases")
        cur.executemany(
            """
            INSERT INTO gold_live.flight_phases (phase, flight_count, updated_at)
            VALUES (%s, %s, NOW())
            """,
            [(row["phase"], row["flight_count"]) for row in rows],
        )

# ── foreachBatch handler ───────────────────────────────────────────────────────

def make_batch_processor(creds: dict):
    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            log.info("gold_live batch %d: empty, skipping", batch_id)
            return

        snapshot = latest_snapshot(batch_df)
        snapshot.cache()
        try:
            log.info("gold_live batch %d: processing %d rows", batch_id, snapshot.count())
            # One connection, one transaction, all sinks — so the dashboard never reads a
            # half-updated set of gold tables, and we don't leak a connection per sink.
            with contextlib.closing(pg_conn(creds)) as conn:
                with conn:
                    write_flight_phases(snapshot, conn)
                    # TODO: finish up live gold tables:
                        # summary (flights_in_air)
                        # flights_by_region
                        # fastest_aircraft
                        # emergency_squawks  (UPSERT on icao24 — do NOT truncate)
                        # fleet_by_airline
        finally:
            snapshot.unpersist()

    return process_batch

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark("FlightDB-GoldLiveConsumer")
    creds = load_pg_creds()
    parsed = read_states_stream(spark)

    query = (
        parsed.writeStream
            .foreachBatch(make_batch_processor(creds))
            .trigger(processingTime=TRIGGER_INTERVAL)
            .option("checkpointLocation", CHECKPOINT_DIR)
            .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
