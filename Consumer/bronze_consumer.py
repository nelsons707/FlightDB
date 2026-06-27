from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import (
    build_spark, load_pg_creds, log, pg_jdbc_url, read_states_stream,
)

# ── Config ─────────────────────────────────────────────────────────────────────

CHECKPOINT_DIR = "/tmp/flightdb-checkpoint/bronze"
TRIGGER_INTERVAL = "5 minutes"

# ── Bronze sink ────────────────────────────────────────────────────────────────

def write_bronze(df: DataFrame, creds: dict) -> None:
    (
        df.withColumn("poll_date", F.to_date(F.from_unixtime("poll_time")))
          .withColumn("ingested_at", F.current_timestamp())
          .write
          .format("jdbc")
          .option("url", pg_jdbc_url(creds))
          .option("dbtable", "bronze.states")
          .option("driver", "org.postgresql.Driver")
          .option("user", creds["user"])
          .option("password", creds["password"])
          .mode("append")
          .save()
    )

# ── foreachBatch handler ───────────────────────────────────────────────────────

def make_batch_processor(creds: dict):
    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            log.info("bronze batch %d: empty, skipping", batch_id)
            return
        log.info("bronze batch %d: writing %d rows", batch_id, batch_df.count())
        write_bronze(batch_df, creds)

    return process_batch

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark("FlightDB-BronzeConsumer")
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
