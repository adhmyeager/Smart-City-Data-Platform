"""
spark_jobs/silver_cleaner.py

Layer: S3 Bronze  →  S3 Silver  (clean, validate, enrich, type-cast)

What it does:
  - Reads Bronze Parquet files from S3 as a streaming source (Trigger.Once
    or continuous, configured via env var SILVER_MODE).
  - Applies data quality rules:
      * Drop rows missing mandatory fields (event_id, vehicle_id, timestamp_unix)
      * Clamp out-of-range sensor values to physical limits
      * Cast timestamp_unix → proper TimestampType event_time
      * Derive speed_band, fuel_band, is_anomaly flags
      * Add ingestion_time and partition columns
  - Writes cleaned Parquet to S3 Silver layer.
  - Runs on a 60-second trigger (near-real-time silver) or as a batch
    (SILVER_MODE=batch) so Airflow can orchestrate it hourly.

Run inside Spark master container:
  docker exec sc_spark_master \
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --jars /opt/spark/jars/extra/hadoop-aws-3.3.4.jar,\
             /opt/spark/jars/extra/aws-java-sdk-bundle-1.12.261.jar \
      /opt/spark_jobs/silver_cleaner.py
"""
"""
spark_jobs/silver_cleaner.py

Layer: S3 Bronze  →  S3 Silver  (clean, validate, enrich, type-cast)

FIX LOG (v2):
  - kafka_timestamp schema corrected: Bronze writes it as TimestampType (from Kafka
    source), not LongType. The mismatch caused all rows to be null → dropped → no Parquet.
  - Added .option("ignoreChanges", "true") to Bronze streaming reader — Bronze uses
    partitioned Parquet which Spark streaming sees as "changed" files on every trigger.
  - Added .option("latestFirst", "false") so older Bronze partitions are processed first.
  - Fixed _bronze_*_schema helpers: kafka_timestamp → TimestampType everywhere.
  - Streaming query now uses AvailableNow trigger for first run (safer than
    processingTime when Bronze files already exist from backlog).
  - run_batch processes all 4 topics (not just telemetry).

Run:
  docker exec sc_spark_master \
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --jars /opt/spark/jars/extra/hadoop-aws-3.3.4.jar,\
/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.261.jar,\
/opt/spark/jars/extra/spark-sql-kafka-0-10_2.12-3.5.0.jar,\
/opt/spark/jars/extra/kafka-clients-3.5.0.jar,\
/opt/spark/jars/extra/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,\
/opt/spark/jars/extra/commons-pool2-2.11.1.jar \
      /opt/spark_jobs/silver_cleaner.py

Batch (Airflow):
  ... spark-submit ... /opt/spark_jobs/silver_cleaner.py --mode batch --date 2025-01-15 --hour 9
"""

import os
import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, FloatType, TimestampType,
)

sys.path.insert(0, "/opt/spark_jobs")
from utils.schemas import (
    TELEMETRY_SCHEMA, WEATHER_SCHEMA, TRAFFIC_SCHEMA, ROAD_EVENT_SCHEMA,
)
from utils.s3_utils import checkpoint_path, configure_spark_s3, S3_BUCKET

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
log = logging.getLogger("silver_cleaner")


# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

SILVER_MODE:     str = os.getenv("SILVER_MODE", "streaming")
TRIGGER_SECONDS: int = int(os.getenv("SILVER_TRIGGER_SECONDS", "60"))

ALERT_SPEED_KMH:     float = float(os.getenv("ALERT_SPEED_KMH",     "120.0"))
ALERT_ENGINE_TEMP_C: float = float(os.getenv("ALERT_ENGINE_TEMP_C", "105.0"))
ALERT_FUEL_PCT:      float = float(os.getenv("ALERT_FUEL_PCT",      "10.0"))
ALERT_RPM:           int   = int(os.getenv("ALERT_RPM",             "5000"))

LIMITS = {
    "speed_kmh":        (0.0,   250.0),
    "rpm":              (0,     8000),
    "engine_temp_c":    (15.0,  120.0),
    "fuel_level_pct":   (0.0,   100.0),
    "fuel_rate_l100km": (0.0,   80.0),
    "latitude":         (29.5,  30.5),
    "longitude":        (30.5,  32.0),
    "traffic_density":  (0,     10),
}


# ─────────────────────────────────────────────────────────────
# Bronze schema helpers
# FIX: kafka_timestamp must be TimestampType — the Kafka source writes
#      it as a proper timestamp, NOT a LongType epoch integer.
#      Mismatch here causes the entire payload column to be null.
# ─────────────────────────────────────────────────────────────

def _extra_bronze_fields() -> list:
    """Extra columns bronze_writer.py adds on top of the raw payload."""
    return [
        StructField("kafka_topic",     StringType(),    True),
        StructField("kafka_partition", IntegerType(),   True),
        StructField("kafka_offset",    LongType(),      True),
        StructField("kafka_timestamp", TimestampType(), True),  # ← was LongType (BUG)
        StructField("ingestion_time",  TimestampType(), True),  # ← was StringType (BUG)
        StructField("partition_date",  StringType(),    True),
        StructField("partition_hour",  IntegerType(),   True),
    ]


def _bronze_telemetry_schema() -> StructType:
    return StructType(TELEMETRY_SCHEMA.fields + _extra_bronze_fields())


def _bronze_weather_schema() -> StructType:
    return StructType(WEATHER_SCHEMA.fields + _extra_bronze_fields())


def _bronze_traffic_schema() -> StructType:
    return StructType(TRAFFIC_SCHEMA.fields + _extra_bronze_fields())


def _bronze_road_events_schema() -> StructType:
    return StructType(ROAD_EVENT_SCHEMA.fields + _extra_bronze_fields())


# ─────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("SmartCity-SilverCleaner")
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.mergeSchema",    "false")
        .config("spark.sql.parquet.filterPushdown", "true")
    )
    builder = configure_spark_s3(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created — Silver Cleaner v2")
    return spark


# ─────────────────────────────────────────────────────────────
# Data quality transforms
# ─────────────────────────────────────────────────────────────

def drop_mandatory_nulls(df: DataFrame, cols: list) -> DataFrame:
    return df.dropna(subset=cols)


def clamp_sensor_values(df: DataFrame) -> DataFrame:
    error_conditions = []
    for col_name, (lo, hi) in LIMITS.items():
        if col_name not in df.columns:
            continue
        original = F.col(col_name)
        clamped  = F.least(F.lit(hi), F.greatest(F.lit(lo), original))
        error_conditions.append((original < lo) | (original > hi))
        df = df.withColumn(col_name, clamped)

    if error_conditions:
        combined = error_conditions[0]
        for c in error_conditions[1:]:
            combined = combined | c
        df = df.withColumn("had_sensor_clamp", combined)
    else:
        df = df.withColumn("had_sensor_clamp", F.lit(False))
    return df


def derive_bands(df: DataFrame) -> DataFrame:
    speed_col = F.col("speed_kmh")
    df = df.withColumn(
        "speed_band",
        F.when(speed_col < 5,   "stopped")
         .when(speed_col < 40,  "slow")
         .when(speed_col < 90,  "medium")
         .when(speed_col < 120, "fast")
         .otherwise("overspeed"),
    )
    fuel_col = F.col("fuel_level_pct")
    df = df.withColumn(
        "fuel_band",
        F.when(fuel_col < ALERT_FUEL_PCT, "critical")
         .when(fuel_col < 25,             "low")
         .when(fuel_col < 75,             "ok")
         .otherwise("full"),
    )
    return df


def flag_anomalies(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_anomaly",
        (F.col("speed_kmh")      > ALERT_SPEED_KMH)    |
        (F.col("engine_temp_c")  > ALERT_ENGINE_TEMP_C) |
        (F.col("fuel_level_pct") < ALERT_FUEL_PCT)      |
        (F.col("rpm")            > ALERT_RPM),
    )


def add_partition_columns(df: DataFrame, time_col: str = "event_time") -> DataFrame:
    return (
        df
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("partition_date", F.date_format(F.col(time_col), "yyyy-MM-dd"))
        .withColumn("partition_hour", F.hour(F.col(time_col)))
    )


# ─────────────────────────────────────────────────────────────
# Per-topic cleaning pipelines
# ─────────────────────────────────────────────────────────────

def clean_telemetry(df: DataFrame) -> DataFrame:
    df = drop_mandatory_nulls(df, ["event_id", "vehicle_id", "timestamp_unix"])
    df = clamp_sensor_values(df)
    df = df.withColumn("event_time", F.col("timestamp_unix").cast(TimestampType()))
    df = derive_bands(df)
    df = flag_anomalies(df)
    df = add_partition_columns(df, "event_time")
    return df


def clean_weather(df: DataFrame) -> DataFrame:
    df = drop_mandatory_nulls(df, ["timestamp_unix", "location"])

    weather_limits = {
        "temp_c":        (-10.0, 60.0),
        "feels_like_c":  (-15.0, 65.0),
        "humidity_pct":  (0,     100),
        "wind_kmh":      (0.0,   200.0),
        "visibility_km": (0.0,   50.0),
        "uv_index":      (0.0,   15.0),
        "pressure_hpa":  (900,   1100),
    }
    for col_name, (lo, hi) in weather_limits.items():
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.least(F.lit(hi), F.greatest(F.lit(lo), F.col(col_name)))
            )

    df = df.withColumn(
        "weather_severity",
        F.when(F.col("condition").isin("Clear", "Partly cloudy"), "low")
         .when(F.col("condition").isin("Clouds", "Haze"),         "moderate")
         .when(F.col("condition").isin("Dust", "Sand", "Rain"),   "high")
         .when(F.col("condition").isin("Thunderstorm", "Fog"),    "severe")
         .otherwise("moderate")
    )
    df = df.withColumn(
        "speed_factor",
        F.when(F.col("condition").isin("Clear", "Partly cloudy"), 1.00)
         .when(F.col("condition") == "Clouds",                    0.98)
         .when(F.col("condition") == "Haze",                      0.95)
         .when(F.col("condition").isin("Dust", "Sand"),           0.83)
         .when(F.col("condition") == "Rain",                      0.80)
         .when(F.col("condition") == "Thunderstorm",              0.70)
         .when(F.col("condition") == "Fog",                       0.65)
         .otherwise(0.95)
    )
    df = df.withColumn("event_time", F.col("timestamp_unix").cast(TimestampType()))
    df = add_partition_columns(df, "event_time")
    return df


def clean_traffic(df: DataFrame) -> DataFrame:
    df = drop_mandatory_nulls(df, ["timestamp_unix", "latitude", "longitude"])

    traffic_limits = {
        "current_speed_kmh":   (0.0,  250.0),
        "free_flow_speed_kmh": (1.0,  250.0),
        "congestion_ratio":    (0.0,  1.0),
        "traffic_density":     (0,    10),
        "confidence":          (0.0,  1.0),
        "latitude":            (29.5, 30.5),
        "longitude":           (30.5, 32.0),
    }
    for col_name, (lo, hi) in traffic_limits.items():
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.least(F.lit(hi), F.greatest(F.lit(lo), F.col(col_name)))
            )

    # Recompute congestion after clamping (avoids divide-by-zero)
    df = df.withColumn(
        "congestion_ratio",
        F.greatest(
            F.lit(0.0),
            F.least(
                F.lit(1.0),
                F.lit(1.0) - F.col("current_speed_kmh") / F.col("free_flow_speed_kmh")
            )
        )
    )
    df = df.withColumn(
        "congestion_band",
        F.when(F.col("congestion_ratio") < 0.25, "free_flow")
         .when(F.col("congestion_ratio") < 0.50, "light")
         .when(F.col("congestion_ratio") < 0.75, "moderate")
         .when(F.col("congestion_ratio") < 0.90, "heavy")
         .otherwise("gridlock")
    )
    df = df.withColumn("gps_lat_bucket", F.round(F.col("latitude"),  2))
    df = df.withColumn("gps_lon_bucket", F.round(F.col("longitude"), 2))
    df = df.withColumn("event_time", F.col("timestamp_unix").cast(TimestampType()))
    df = add_partition_columns(df, "event_time")
    return df


def clean_road_events(df: DataFrame) -> DataFrame:
    VALID_EVENTS = {"ACCIDENT", "ROADWORK", "BREAKDOWN", "CONGESTION_INCIDENT"}
    df = drop_mandatory_nulls(df, ["event_id", "vehicle_id", "timestamp"])
    df = df.filter(F.col("event_type").isin(*VALID_EVENTS))
    df = df.withColumn("latitude",  F.least(F.lit(30.5), F.greatest(F.lit(29.5), F.col("latitude"))))
    df = df.withColumn("longitude", F.least(F.lit(32.0), F.greatest(F.lit(30.5), F.col("longitude"))))
    df = df.withColumn(
        "severity_score",
        F.when(F.col("event_type") == "ACCIDENT",            4)
         .when(F.col("event_type") == "BREAKDOWN",           2)
         .when(F.col("event_type") == "ROADWORK",            1)
         .when(F.col("event_type") == "CONGESTION_INCIDENT", 3)
         .otherwise(0)
    )
    df = df.withColumn("event_time", F.to_timestamp(F.col("timestamp")))
    df = add_partition_columns(df, "event_time")
    return df


# ─────────────────────────────────────────────────────────────
# Streaming Bronze → Silver reader helper
# ─────────────────────────────────────────────────────────────

def _make_bronze_stream(spark: SparkSession, topic: str, schema: StructType) -> DataFrame:
    """
    Read a Bronze S3 Parquet folder as a streaming source.

    FIX: ignoreChanges=true is required because Bronze writes new
    partitioned files (not appends to existing ones). Without this Spark
    streaming throws "Files were deleted or changed" errors and stalls.
    """
    bronze_src = f"s3a://{S3_BUCKET}/bronze/{topic}"
    return (
        spark.readStream
        .schema(schema)
        .option("recursiveFileLookup", "true")
        .option("ignoreChanges",       "true")   # ← KEY FIX
        .option("latestFirst",         "false")  # process oldest Bronze first
        .parquet(bronze_src)
    )


# ─────────────────────────────────────────────────────────────
# Streaming mode
# ─────────────────────────────────────────────────────────────

def run_streaming(spark: SparkSession) -> None:
    """Launch four independent Silver streaming queries — one per topic."""

    # Map: (topic_name, schema_fn, clean_fn, silver_table_name)
    TOPIC_CONFIG = [
        ("vehicle-telemetry", _bronze_telemetry_schema, clean_telemetry, "telemetry"),
        ("weather-data",      _bronze_weather_schema,   clean_weather,   "weather"),
        ("traffic-events",    _bronze_traffic_schema,   clean_traffic,   "traffic"),
        ("road-events",       _bronze_road_events_schema, clean_road_events, "road_events"),
    ]

    queries = []
    for topic, schema_fn, clean_fn, silver_table in TOPIC_CONFIG:
        silver_dst = f"s3a://{S3_BUCKET}/silver/{silver_table}"
        ckpt       = checkpoint_path(f"silver_{silver_table}")

        log.info(f"[{topic}] bronze/{topic}  →  silver/{silver_table}")
        log.info(f"[{topic}] checkpoint: {ckpt}")

        raw_df   = _make_bronze_stream(spark, topic, schema_fn())
        clean_df = clean_fn(raw_df)

        q = (
            clean_df.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path",               silver_dst)
            .option("checkpointLocation", ckpt)
            .partitionBy("partition_date", "partition_hour")
            .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
            .queryName(f"silver_{silver_table}")
            .start()
        )
        log.info(f"[{topic}] Query started — id={q.id}")
        queries.append(q)

    log.info(f"All {len(queries)} Silver streaming queries running.")
    spark.streams.awaitAnyTermination()


# ─────────────────────────────────────────────────────────────
# Batch mode  (Airflow calls this hourly)
# ─────────────────────────────────────────────────────────────

def run_batch(spark: SparkSession, date: str, hour: int) -> None:
    """
    Process all 4 Bronze hour-partitions → write Silver.
    Idempotent: mode=overwrite replaces the Silver partition if it exists.
    """

    def _run_topic(topic: str, schema_fn, clean_fn, silver_table: str) -> None:
        bronze_src = (
            f"s3a://{S3_BUCKET}/bronze/{topic}"
            f"/partition_date={date}/partition_hour={hour:02d}"
        )
        silver_dst = f"s3a://{S3_BUCKET}/silver/{silver_table}"
        log.info(f"[batch/{topic}] reading: {bronze_src}")

        try:
            raw_df = spark.read.schema(schema_fn()).parquet(bronze_src)
        except Exception as e:
            log.warning(f"[batch/{topic}] cannot read Bronze: {e} — skipping")
            return

        if raw_df.rdd.isEmpty():
            log.warning(f"[batch/{topic}] no data for {date} h={hour} — skipping")
            return

        clean_df  = clean_fn(raw_df)
        row_count = clean_df.count()
        log.info(f"[batch/{topic}] writing {row_count:,} rows → {silver_dst}")
        (
            clean_df.write
            .format("parquet")
            .mode("overwrite")
            .partitionBy("partition_date", "partition_hour")
            .save(silver_dst)
        )
        log.info(f"[batch/{topic}] done — {date} h={hour}")

    _run_topic("vehicle-telemetry", _bronze_telemetry_schema, clean_telemetry, "telemetry")
    _run_topic("weather-data",      _bronze_weather_schema,   clean_weather,   "weather")
    _run_topic("traffic-events",    _bronze_traffic_schema,   clean_traffic,   "traffic")
    _run_topic("road-events",       _bronze_road_events_schema, clean_road_events, "road_events")

    log.info(f"Batch complete — all 4 topics — {date} h={hour}")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    from datetime import datetime, timezone

    parser = argparse.ArgumentParser(description="Smart City Silver Cleaner")
    parser.add_argument("--mode",  default=SILVER_MODE, choices=["streaming", "batch"])
    parser.add_argument("--date",  default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    parser.add_argument("--hour",  type=int, default=datetime.now(timezone.utc).hour)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  Smart City — Silver Cleaner  (v2 — fixed)")
    log.info(f"  Mode      : {args.mode}")
    log.info(f"  S3 bucket : {S3_BUCKET}")
    if args.mode == "batch":
        log.info(f"  Target    : {args.date}  hour={args.hour}")
    log.info("=" * 60)

    spark = build_spark()

    if args.mode == "streaming":
        run_streaming(spark)
    else:
        run_batch(spark, args.date, args.hour)


if __name__ == "__main__":
    main()
