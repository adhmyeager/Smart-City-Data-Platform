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

import os
import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

sys.path.insert(0, "/opt/spark_jobs")
from utils.s3_utils import (
    silver_path, checkpoint_path, configure_spark_s3, S3_BUCKET,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
log = logging.getLogger("silver_cleaner")


# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

SILVER_MODE: str     = os.getenv("SILVER_MODE", "streaming")   # "streaming" | "batch"
TRIGGER_SECONDS: int = int(os.getenv("SILVER_TRIGGER_SECONDS", "60"))

# Alert thresholds — must match config.py in the simulator exactly
ALERT_SPEED_KMH:     float = float(os.getenv("ALERT_SPEED_KMH",     "120.0"))
ALERT_ENGINE_TEMP_C: float = float(os.getenv("ALERT_ENGINE_TEMP_C", "105.0"))
ALERT_FUEL_PCT:      float = float(os.getenv("ALERT_FUEL_PCT",      "10.0"))
ALERT_RPM:           int   = int(os.getenv("ALERT_RPM",             "5000"))

# Physical sensor limits — values outside these are sensor errors, not anomalies
LIMITS = {
    "speed_kmh":      (0.0,   250.0),
    "rpm":            (0,     8000),
    "engine_temp_c":  (15.0,  120.0),
    "fuel_level_pct": (0.0,   100.0),
    "fuel_rate_l100km": (0.0, 80.0),
    "latitude":       (29.5,  30.5),    # Cairo bounding box
    "longitude":      (30.5,  32.0),
    "traffic_density": (0,    10),
}


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
        # Schema merging on Parquet reads (handles Bronze schema evolution)
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
    )
    builder = configure_spark_s3(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created — Silver Cleaner")
    return spark


# ─────────────────────────────────────────────────────────────
# Data quality transforms  (pure DataFrame → DataFrame functions)
# ─────────────────────────────────────────────────────────────

def drop_mandatory_nulls(df: DataFrame) -> DataFrame:
    """
    Drop rows missing any field we cannot recover.
    We keep rows with bad sensor readings (clamped below) but not
    rows with no identity or timestamp.
    """
    mandatory = ["event_id", "vehicle_id", "timestamp_unix"]
    before = df
    df = df.dropna(subset=mandatory)
    log.info("Mandatory null filter applied")
    return df


def clamp_sensor_values(df: DataFrame) -> DataFrame:
    """
    Clamp numeric columns to their physical limits.
    Values outside limits are sensor errors — clamp, don't drop.
    A separate 'sensor_error' flag is set when clamping occurs.
    """
    error_conditions = []

    for col_name, (lo, hi) in LIMITS.items():
        if col_name not in df.columns:
            continue

        original = F.col(col_name)
        clamped  = F.least(F.lit(hi), F.greatest(F.lit(lo), original))

        # Flag if value was outside limits
        error_conditions.append(
            (original < lo) | (original > hi)
        )
        df = df.withColumn(col_name, clamped)

    # Combine all error flags
    if error_conditions:
        combined_error = error_conditions[0]
        for cond in error_conditions[1:]:
            combined_error = combined_error | cond
        df = df.withColumn("had_sensor_clamp", combined_error)
    else:
        df = df.withColumn("had_sensor_clamp", F.lit(False))

    return df


def cast_timestamps(df: DataFrame) -> DataFrame:
    """
    Convert epoch-second timestamp_unix → proper TimestampType event_time.
    Keeps timestamp_unix for downstream compatibility.
    """
    df = df.withColumn(
        "event_time",
        F.col("timestamp_unix").cast(TimestampType()),
    )
    return df


def derive_bands(df: DataFrame) -> DataFrame:
    """
    Categorise continuous metrics into labelled bands.
    These are used directly as Grafana filter dimensions.

    speed_band:
      "stopped"  → < 5 km/h
      "slow"     → 5–40 km/h   (urban crawl)
      "medium"   → 40–90 km/h  (arterial)
      "fast"     → 90–120 km/h (highway)
      "overspeed"→ > 120 km/h

    fuel_band:
      "critical" → < 10 %  (alert threshold)
      "low"      → 10–25 %
      "ok"       → 25–75 %
      "full"     → > 75 %
    """
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
    """
    Replicate the VehicleTelemetry.is_anomaly() logic in Spark.
    Must stay in sync with config.py thresholds.
    """
    df = df.withColumn(
        "is_anomaly",
        (F.col("speed_kmh")      > ALERT_SPEED_KMH)     |
        (F.col("engine_temp_c")  > ALERT_ENGINE_TEMP_C)  |
        (F.col("fuel_level_pct") < ALERT_FUEL_PCT)       |
        (F.col("rpm")            > ALERT_RPM),
    )
    return df


def add_partition_columns(df: DataFrame) -> DataFrame:
    """
    Add partition_date and partition_hour from event_time.
    Using event_time (not ingestion_time) keeps data in the correct
    hourly partition even if micro-batch runs a few minutes late.
    """
    df = (
        df
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("partition_date", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
        .withColumn("partition_hour", F.hour(F.col("event_time")))
    )
    return df


def clean_telemetry(df: DataFrame) -> DataFrame:
    """Apply the full Silver cleaning pipeline to a telemetry DataFrame."""
    df = drop_mandatory_nulls(df)
    df = clamp_sensor_values(df)
    df = cast_timestamps(df)
    df = derive_bands(df)
    df = flag_anomalies(df)
    df = add_partition_columns(df)
    return df
def clean_weather(df: DataFrame) -> DataFrame:
    """
    Silver cleaning for weather-data topic.
    Weather updates every 5 minutes — very few rows, simple cleaning.
    """
    # Drop rows with no identity or timestamp
    df = df.dropna(subset=["timestamp_unix", "location"])
 
    # Clamp sensor values to physical limits for Cairo climate
    weather_limits = {
        "temp_c":         (-10.0, 60.0),   # Cairo range: 0–45°C, be generous
        "feels_like_c":   (-15.0, 65.0),
        "humidity_pct":   (0,     100),
        "wind_kmh":       (0.0,   200.0),
        "visibility_km":  (0.0,   50.0),
        "uv_index":       (0.0,   15.0),
        "pressure_hpa":   (900,   1100),
    }
    for col_name, (lo, hi) in weather_limits.items():
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.least(F.lit(hi), F.greatest(F.lit(lo), F.col(col_name)))
            )
 
    # Derive weather_severity — used as a Grafana filter dimension
    df = df.withColumn(
        "weather_severity",
        F.when(F.col("condition").isin("Clear", "Partly cloudy"), "low")
         .when(F.col("condition").isin("Clouds", "Haze"),         "moderate")
         .when(F.col("condition").isin("Dust", "Sand", "Rain"),   "high")
         .when(F.col("condition").isin("Thunderstorm", "Fog"),    "severe")
         .otherwise("moderate")
    )
 
    # Derive speed_factor from condition (mirrors simulator's CONDITION_SPEED_FACTOR)
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
 
    # Timestamps and partitions
    df = df.withColumn("event_time",     F.col("timestamp_unix").cast(TimestampType()))
    df = df.withColumn("ingestion_time", F.current_timestamp())
    df = df.withColumn("partition_date", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
    df = df.withColumn("partition_hour", F.hour(F.col("event_time")))
    return df
 
 
def clean_traffic(df: DataFrame) -> DataFrame:
    """
    Silver cleaning for traffic-events topic.
    Traffic updates every 60s per vehicle position — moderate row count.
    """
    df = df.dropna(subset=["timestamp_unix", "latitude", "longitude"])
 
    traffic_limits = {
        "current_speed_kmh":   (0.0,  250.0),
        "free_flow_speed_kmh": (1.0,  250.0),   # must be > 0 (used as divisor)
        "congestion_ratio":    (0.0,  1.0),
        "traffic_density":     (0,    10),
        "confidence":          (0.0,  1.0),
        "latitude":            (29.5, 30.5),     # Cairo bounding box
        "longitude":           (30.5, 32.0),
    }
    for col_name, (lo, hi) in traffic_limits.items():
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.least(F.lit(hi), F.greatest(F.lit(lo), F.col(col_name)))
            )
 
    # Recompute congestion_ratio from actual speeds after clamping
    # avoids divide-by-zero because free_flow_speed_kmh clamped to ≥1
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
 
    # Derive congestion_band — used by Grafana worldmap panel
    df = df.withColumn(
        "congestion_band",
        F.when(F.col("congestion_ratio") < 0.25, "free_flow")
         .when(F.col("congestion_ratio") < 0.50, "light")
         .when(F.col("congestion_ratio") < 0.75, "moderate")
         .when(F.col("congestion_ratio") < 0.90, "heavy")
         .otherwise("gridlock")
    )
 
    # GPS grid bucket (~1.1 km cell) — used as JOIN key in congestion_hotspots Gold mart
    df = df.withColumn("gps_lat_bucket", F.round(F.col("latitude"),  2))
    df = df.withColumn("gps_lon_bucket", F.round(F.col("longitude"), 2))
 
    df = df.withColumn("event_time",     F.col("timestamp_unix").cast(TimestampType()))
    df = df.withColumn("ingestion_time", F.current_timestamp())
    df = df.withColumn("partition_date", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
    df = df.withColumn("partition_hour", F.hour(F.col("event_time")))
    return df
 
 
def clean_road_events(df: DataFrame) -> DataFrame:
    """
    Silver cleaning for road-events topic.
    Low-volume — only fires when a vehicle triggers an incident event.
    """
    VALID_EVENTS = {"ACCIDENT", "ROADWORK", "BREAKDOWN", "CONGESTION_INCIDENT"}
 
    df = df.dropna(subset=["event_id", "vehicle_id", "timestamp"])
 
    # Filter to only recognised event types (drop NONE — shouldn't arrive but may)
    df = df.filter(F.col("event_type").isin(*VALID_EVENTS))
 
    # Clamp GPS to Cairo bounding box
    df = df.withColumn("latitude",  F.least(F.lit(30.5),  F.greatest(F.lit(29.5),  F.col("latitude"))))
    df = df.withColumn("longitude", F.least(F.lit(32.0),  F.greatest(F.lit(30.5),  F.col("longitude"))))
 
    # Derive severity_score per event type — used by Gold road_incidents mart
    df = df.withColumn(
        "severity_score",
        F.when(F.col("event_type") == "ACCIDENT",            4)
         .when(F.col("event_type") == "BREAKDOWN",           2)
         .when(F.col("event_type") == "ROADWORK",            1)
         .when(F.col("event_type") == "CONGESTION_INCIDENT", 3)
         .otherwise(0)
    )
 
    # Parse the timestamp string → TimestampType (road-events use timestamp_iso, not unix)
    df = df.withColumn("event_time",     F.to_timestamp(F.col("timestamp")))
    df = df.withColumn("ingestion_time", F.current_timestamp())
    df = df.withColumn("partition_date", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
    df = df.withColumn("partition_hour", F.hour(F.col("event_time")))
    return df


# ─────────────────────────────────────────────────────────────
# Streaming mode
# ─────────────────────────────────────────────────────────────

def run_streaming(spark: SparkSession):
    """
    Launch four parallel streaming queries — one per Bronze topic.
    All checkpoints are independent so each query can restart separately.
    """
 
    def start_query(topic: str, clean_fn, bronze_schema, silver_table: str):
        """Generic helper: bronze topic → clean fn → silver table."""
        bronze_src = f"s3a://{S3_BUCKET}/bronze/{topic}"
        silver_dst = f"s3a://{S3_BUCKET}/silver/{silver_table}"
        ckpt       = checkpoint_path(f"silver_{silver_table}")
 
        log.info(f"[{topic}] {bronze_src}  →  {silver_dst}")
 
        raw = (
            spark.readStream
            .schema(bronze_schema(spark))
            .option("recursiveFileLookup", "true")
            .parquet(bronze_src)
        )
        clean = clean_fn(raw)
 
        return (
            clean.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", silver_dst)
            .option("checkpointLocation", ckpt)
            .partitionBy("partition_date", "partition_hour")
            .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
            .queryName(f"silver_{silver_table}")
            .start()
        )
 
    # ── 1. Telemetry (existing, unchanged) ──
    q1 = start_query(
        "vehicle-telemetry",
        clean_telemetry,
        _bronze_telemetry_schema,
        "telemetry",
    )
 
    # ── 2. Weather ──
    q2 = start_query(
        "weather-data",
        clean_weather,
        _bronze_weather_schema,    # add helper below
        "weather",
    )
 
    # ── 3. Traffic ──
    q3 = start_query(
        "traffic-events",
        clean_traffic,
        _bronze_traffic_schema,    # add helper below
        "traffic",
    )
 
    # ── 4. Road events ──
    q4 = start_query(
        "road-events",
        clean_road_events,
        _bronze_road_events_schema,  # add helper below
        "road_events",
    )
 
    log.info("All 4 Silver streaming queries running.")
    spark.streams.awaitAnyTermination()



# ─────────────────────────────────────────────────────────────
# Batch mode  (called by Airflow DAG hourly)
# ─────────────────────────────────────────────────────────────

def run_batch(spark: SparkSession, date: str, hour: int):
    """
    Process a single Bronze hour-partition → write Silver.
    Idempotent: overwrite the Silver partition if it already exists.

    Args:
        date: "yyyy-MM-dd"
        hour: 0–23
    """
    bronze_src = (
        f"s3a://{S3_BUCKET}/bronze/vehicle-telemetry"
        f"/partition_date={date}/partition_hour={hour:02d}"
    )
    silver_dst = f"s3a://{S3_BUCKET}/silver/telemetry"

    log.info(f"[batch] Reading  : {bronze_src}")

    raw_df = spark.read.parquet(bronze_src)

    if raw_df.rdd.isEmpty():
        log.warning(f"[batch] No data for {date} hour={hour} — skipping")
        return

    clean_df = clean_telemetry(raw_df)

    row_count = clean_df.count()
    log.info(f"[batch] Writing {row_count:,} rows → Silver")

    (
        clean_df.write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("partition_date", "partition_hour")
        .save(silver_dst)
    )

    log.info(f"[batch] Done — {date} hour={hour}")


def _bronze_telemetry_schema(spark: SparkSession):
    """
    Infer schema from an existing Bronze file, or fall back to explicit schema.
    Using the explicit schema avoids a full scan on startup.
    """
    from utils.schemas import TELEMETRY_SCHEMA
    from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

    # Bronze adds these extra columns on top of the raw payload schema
    extra_fields = [
        StructField("kafka_topic",      StringType(),  True),
        StructField("kafka_partition",  IntegerType(), True),
        StructField("kafka_offset",     LongType(),    True),
        StructField("kafka_timestamp",  LongType(),    True),
        StructField("ingestion_time",   StringType(),  True),
        StructField("partition_date",   StringType(),  True),
        StructField("partition_hour",   IntegerType(), True),
    ]
    return StructType(TELEMETRY_SCHEMA.fields + extra_fields)
def _bronze_weather_schema(spark):
    from utils.schemas import WEATHER_SCHEMA
    from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
    extra = [
        StructField("kafka_topic",     StringType(),  True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset",    LongType(),    True),
        StructField("kafka_timestamp", LongType(),    True),
        StructField("ingestion_time",  StringType(),  True),
        StructField("partition_date",  StringType(),  True),
        StructField("partition_hour",  IntegerType(), True),
    ]
    return StructType(WEATHER_SCHEMA.fields + extra)
 
 
def _bronze_traffic_schema(spark):
    from utils.schemas import TRAFFIC_SCHEMA
    from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
    extra = [
        StructField("kafka_topic",     StringType(),  True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset",    LongType(),    True),
        StructField("kafka_timestamp", LongType(),    True),
        StructField("ingestion_time",  StringType(),  True),
        StructField("partition_date",  StringType(),  True),
        StructField("partition_hour",  IntegerType(), True),
    ]
    return StructType(TRAFFIC_SCHEMA.fields + extra)
 
 
def _bronze_road_events_schema(spark):
    from utils.schemas import ROAD_EVENT_SCHEMA
    from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
    extra = [
        StructField("kafka_topic",     StringType(),  True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset",    LongType(),    True),
        StructField("kafka_timestamp", LongType(),    True),
        StructField("ingestion_time",  StringType(),  True),
        StructField("partition_date",  StringType(),  True),
        StructField("partition_hour",  IntegerType(), True),
    ]
    return StructType(ROAD_EVENT_SCHEMA.fields + extra)
 

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    import argparse
    from datetime import datetime, timezone

    parser = argparse.ArgumentParser(description="Smart City Silver Cleaner")
    parser.add_argument("--mode",  default=SILVER_MODE,
                        choices=["streaming", "batch"],
                        help="streaming=continuous, batch=single run (use with --date --hour)")
    parser.add_argument("--date",  default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        help="Partition date for batch mode (yyyy-MM-dd)")
    parser.add_argument("--hour",  type=int,
                        default=datetime.now(timezone.utc).hour,
                        help="Partition hour for batch mode (0-23)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  Smart City — Silver Cleaner")
    log.info(f"  Mode           : {args.mode}")
    log.info(f"  S3 bucket      : {S3_BUCKET}")
    if args.mode == "batch":
        log.info(f"  Processing     : {args.date}  hour={args.hour}")
    log.info("=" * 60)

    spark = build_spark()

    if args.mode == "streaming":
        run_streaming(spark)
    else:
        run_batch(spark, args.date, args.hour)


if __name__ == "__main__":
    main()
