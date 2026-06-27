import json
import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType,
    LongType, StringType, StructField, StructType,
)

# TODO: pre-download spark-sql-kafka and postgresql JARs into jars/ and switch
#       to --jars for air-gapped environments (e.g. Docker, offline CI)

# ── Config ─────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "opensky-states"
CREDS_PATH = Path(__file__).resolve().parent.parent / "Creds" / "PGCreds.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Schema ─────────────────────────────────────────────────────────────────────

STATE_SCHEMA = StructType([
    StructField("icao24",          StringType()),
    StructField("callsign",        StringType()),
    StructField("origin_country",  StringType()),
    StructField("time_position",   LongType()),
    StructField("last_contact",    LongType()),
    StructField("longitude",       DoubleType()),
    StructField("latitude",        DoubleType()),
    StructField("baro_altitude",   DoubleType()),
    StructField("on_ground",       BooleanType()),
    StructField("velocity",        DoubleType()),
    StructField("true_track",      DoubleType()),
    StructField("vertical_rate",   DoubleType()),
    StructField("sensors",         ArrayType(IntegerType())),
    StructField("geo_altitude",    DoubleType()),
    StructField("squawk",          StringType()),
    StructField("spi",             BooleanType()),
    StructField("position_source", IntegerType()),
    StructField("poll_time",       LongType()),
])

# ── Postgres helpers ───────────────────────────────────────────────────────────

def load_pg_creds() -> dict:
    return json.loads(CREDS_PATH.read_text())

def pg_jdbc_url(creds: dict) -> str:
    return f"jdbc:postgresql://{creds['host']}:{creds['port']}/{creds['database']}"

# ── Spark + Kafka source ─────────────────────────────────────────────────────────

def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_states_stream(spark: SparkSession) -> DataFrame:
    """Subscribe to the OpenSky topic and parse the JSON value into state columns.

    Each streaming query manages its own Kafka offsets via its checkpoint dir, so
    two consumers reading this topic stay fully independent.
    """
    raw = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), STATE_SCHEMA).alias("d"))
           .select("d.*")
    )
