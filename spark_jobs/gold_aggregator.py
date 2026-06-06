"""
spark_jobs/gold_aggregator.py

Layer: S3 Silver  →  S3 Gold  (aggregated KPIs, window functions)

Aggregations produced (all written as Parquet to S3 Gold):

  FROM silver/telemetry:
  1. vehicle_5min        — 5-min window per vehicle (speed, fuel, engine, GPS)
  2. route_hourly        — 1-hour window per route (congestion, throughput, incidents)
  3. fuel_daily          — 1-day window per vehicle_type (fuel + CO2 estimate)
  4. road_event_summary  — 15-min window per event type from telemetry road_event column

  FROM silver/weather:
  5. weather_hourly      — 1-hour window per condition (temp, humidity, speed_factor)

  FROM silver/traffic:
  6. traffic_30min       — 30-min window per GPS grid bucket (congestion, speed vs free-flow)

  FROM silver/road_events:
  7. incident_summary    — 1-hour window per event_type (richer than road_event_summary:
                           has severity_score, exact GPS, dedicated vehicle reporting)

Why 7 aggregations from 4 Silver tables?
  - telemetry is the richest source → 4 different angles on it
  - weather, traffic, road_events each add a dimension telemetry alone cannot answer
  - Grafana joins them in mart layer via dbt for cross-topic insights

Run (batch, via Airflow):
  .\spark_jobs\submit_jobs.ps1 -Job gold_batch -Date 2026-06-03 -Hour 14

Run (streaming, continuous):
  .\spark_jobs\submit_jobs.ps1 -Job gold
"""

import os
import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    FloatType, DoubleType, BooleanType,
    TimestampType,
)

sys.path.insert(0, "/opt/spark_jobs")
from utils.s3_utils import (
    gold_path, checkpoint_path, configure_spark_s3, S3_BUCKET,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
log = logging.getLogger("gold_aggregator")

GOLD_MODE: str       = os.getenv("GOLD_MODE", "streaming")
TRIGGER_SECONDS: int = int(os.getenv("GOLD_TRIGGER_SECONDS", "300"))   # 5 min


# ─────────────────────────────────────────────────────────────
# Silver schemas — defined inline to avoid circular imports
# Matches exactly what silver_cleaner.py writes to S3
# ─────────────────────────────────────────────────────────────

# silver/weather columns (WEATHER_SCHEMA fields + clean_weather additions)
SILVER_WEATHER_SCHEMA = StructType([
    StructField("location",           StringType(),  True),
    StructField("latitude",           DoubleType(),  True),
    StructField("longitude",          DoubleType(),  True),
    StructField("temp_c",             FloatType(),   True),
    StructField("feels_like_c",       FloatType(),   True),
    StructField("humidity_pct",       IntegerType(), True),
    StructField("wind_kmh",           FloatType(),   True),
    StructField("wind_direction_deg", IntegerType(), True),
    StructField("condition",          StringType(),  True),
    StructField("description",        StringType(),  True),
    StructField("visibility_km",      FloatType(),   True),
    StructField("uv_index",           FloatType(),   True),
    StructField("pressure_hpa",       IntegerType(), True),
    StructField("timestamp_unix",     LongType(),    True),
    StructField("source",             StringType(),  True),
    # Added by clean_weather():
    StructField("weather_severity",   StringType(),  True),
    StructField("speed_factor",       FloatType(),   True),
    StructField("event_time",         TimestampType(),True),
    StructField("ingestion_time",     TimestampType(),True),
    StructField("partition_date",     StringType(),  True),
    StructField("partition_hour",     IntegerType(), True),
])

# silver/traffic columns (TRAFFIC_SCHEMA fields + clean_traffic additions)
SILVER_TRAFFIC_SCHEMA = StructType([
    StructField("latitude",              DoubleType(),  True),
    StructField("longitude",             DoubleType(),  True),
    StructField("current_speed_kmh",     FloatType(),   True),
    StructField("free_flow_speed_kmh",   FloatType(),   True),
    StructField("congestion_ratio",      FloatType(),   True),
    StructField("traffic_density",       IntegerType(), True),
    StructField("confidence",            FloatType(),   True),
    StructField("road_closure",          BooleanType(), True),
    StructField("timestamp_unix",        LongType(),    True),
    StructField("source",                StringType(),  True),
    # Added by clean_traffic():
    StructField("congestion_band",       StringType(),  True),
    StructField("gps_lat_bucket",        DoubleType(),  True),
    StructField("gps_lon_bucket",        DoubleType(),  True),
    StructField("event_time",            TimestampType(),True),
    StructField("ingestion_time",        TimestampType(),True),
    StructField("partition_date",        StringType(),  True),
    StructField("partition_hour",        IntegerType(), True),
])

# silver/road_events columns (ROAD_EVENT_SCHEMA fields + clean_road_events additions)
SILVER_ROAD_EVENTS_SCHEMA = StructType([
    StructField("event_id",      StringType(),  False),
    StructField("vehicle_id",    StringType(),  False),
    StructField("event_type",    StringType(),  True),
    StructField("latitude",      DoubleType(),  True),
    StructField("longitude",     DoubleType(),  True),
    StructField("road_type",     StringType(),  True),
    StructField("timestamp",     StringType(),  True),
    # Added by clean_road_events():
    StructField("severity_score",IntegerType(), True),
    StructField("event_time",    TimestampType(),True),
    StructField("ingestion_time",TimestampType(),True),
    StructField("partition_date",StringType(),  True),
    StructField("partition_hour",IntegerType(), True),
])


# ─────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("SmartCity-GoldAggregator")
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.streaming.statefulOperator.stateExpiry.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
    )
    builder = configure_spark_s3(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created — Gold Aggregator (v2 — all 4 Silver sources)")
    return spark


# ─────────────────────────────────────────────────────────────
# Silver readers — streaming
# ─────────────────────────────────────────────────────────────

def read_silver_telemetry_stream(spark: SparkSession) -> DataFrame:
    from utils.schemas import SILVER_TELEMETRY_SCHEMA
    src = f"s3a://{S3_BUCKET}/silver/telemetry"
    log.info(f"Silver telemetry stream: {src}")
    return (
        spark.readStream
        .schema(SILVER_TELEMETRY_SCHEMA)
        .option("recursiveFileLookup", "true")
        .parquet(src)
        .withWatermark("event_time", "10 minutes")
    )


def read_silver_weather_stream(spark: SparkSession) -> DataFrame:
    src = f"s3a://{S3_BUCKET}/silver/weather"
    log.info(f"Silver weather stream: {src}")
    return (
        spark.readStream
        .schema(SILVER_WEATHER_SCHEMA)
        .option("recursiveFileLookup", "true")
        .parquet(src)
        .withWatermark("event_time", "15 minutes")
    )


def read_silver_traffic_stream(spark: SparkSession) -> DataFrame:
    src = f"s3a://{S3_BUCKET}/silver/traffic"
    log.info(f"Silver traffic stream: {src}")
    return (
        spark.readStream
        .schema(SILVER_TRAFFIC_SCHEMA)
        .option("recursiveFileLookup", "true")
        .parquet(src)
        .withWatermark("event_time", "15 minutes")
    )


def read_silver_road_events_stream(spark: SparkSession) -> DataFrame:
    src = f"s3a://{S3_BUCKET}/silver/road_events"
    log.info(f"Silver road_events stream: {src}")
    return (
        spark.readStream
        .schema(SILVER_ROAD_EVENTS_SCHEMA)
        .option("recursiveFileLookup", "true")
        .parquet(src)
        .withWatermark("event_time", "30 minutes")
    )


# ─────────────────────────────────────────────────────────────
# Silver readers — batch (used by Airflow hourly job)
# ─────────────────────────────────────────────────────────────

def _read_batch(spark: SparkSession, table: str, schema, date: str, hour: int) -> DataFrame:
    """
    Read one hour-partition from a Silver table.
    Returns empty DataFrame (not None) if partition doesn't exist,
    so callers can check .rdd.isEmpty() safely.
    """
    src = (
        f"s3a://{S3_BUCKET}/silver/{table}"
        f"/partition_date={date}/partition_hour={hour:02d}"
    )
    log.info(f"Batch read: {src}")
    try:
        df = spark.read.schema(schema).parquet(src)
        # Ensure event_time exists (fallback for older partitions)
        if "event_time" not in df.columns and "timestamp_unix" in df.columns:
            df = df.withColumn("event_time", F.col("timestamp_unix").cast(TimestampType()))
        elif "event_time" not in df.columns and "timestamp" in df.columns:
            df = df.withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        return df
    except Exception as e:
        log.warning(f"Cannot read {src}: {e} — returning empty DataFrame")
        return spark.createDataFrame([], schema)


def read_silver_telemetry_batch(spark, date, hour):
    from utils.schemas import SILVER_TELEMETRY_SCHEMA
    return _read_batch(spark, "telemetry", SILVER_TELEMETRY_SCHEMA, date, hour)

def read_silver_weather_batch(spark, date, hour):
    return _read_batch(spark, "weather", SILVER_WEATHER_SCHEMA, date, hour)

def read_silver_traffic_batch(spark, date, hour):
    return _read_batch(spark, "traffic", SILVER_TRAFFIC_SCHEMA, date, hour)

def read_silver_road_events_batch(spark, date, hour):
    return _read_batch(spark, "road_events", SILVER_ROAD_EVENTS_SCHEMA, date, hour)


# ─────────────────────────────────────────────────────────────
# Aggregation 1: vehicle_5min  (from silver/telemetry)
# ─────────────────────────────────────────────────────────────

def agg_vehicle_5min(df: DataFrame) -> DataFrame:
    """
    5-minute tumbling window per vehicle.
    Columns: speed stats, RPM, engine temp, fuel, traffic density,
             trip distance, event counts, anomaly count.
    Grafana: per-vehicle timeline, fuel gauge, engine health panel.
    """
    return (
        df
        .groupBy(
            F.window("event_time", "5 minutes"),
            "vehicle_id",
            "vehicle_type",
            "route_name",
        )
        .agg(
            F.avg("speed_kmh")          .alias("avg_speed_kmh"),
            F.max("speed_kmh")          .alias("max_speed_kmh"),
            F.min("speed_kmh")          .alias("min_speed_kmh"),
            F.avg("rpm")                .alias("avg_rpm"),
            F.avg("engine_temp_c")      .alias("avg_engine_temp_c"),
            F.max("engine_temp_c")      .alias("max_engine_temp_c"),
            F.avg("fuel_level_pct")     .alias("avg_fuel_level_pct"),
            F.avg("fuel_rate_l100km")   .alias("avg_fuel_rate_l100km"),
            F.sum("fuel_consumed_l")    .alias("total_fuel_consumed_l"),
            F.avg("traffic_density")    .alias("avg_traffic_density"),
            F.max("trip_distance_km")   .alias("trip_distance_km"),
            F.avg("latitude")           .alias("avg_latitude"),
            F.avg("longitude")          .alias("avg_longitude"),
            F.count("*")                .alias("event_count"),
            F.sum(F.col("is_anomaly").cast("int"))        .alias("anomaly_count"),
            F.sum(F.col("had_sensor_clamp").cast("int"))  .alias("clamped_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        .withColumn("partition_date", F.date_format("window_start", "yyyy-MM-dd"))
    )


# ─────────────────────────────────────────────────────────────
# Aggregation 2: route_hourly  (from silver/telemetry)
# ─────────────────────────────────────────────────────────────

def agg_route_hourly(df: DataFrame) -> DataFrame:
    """
    1-hour tumbling window per route + road_type.
    Columns: speed stats, traffic density, fuel rate, anomaly count,
             road event count, unique vehicles, speed band distribution.
    Grafana: route congestion heatmap, throughput, anomaly rate.
    """
    return (
        df
        .groupBy(
            F.window("event_time", "1 hour"),
            "route_name",
            "road_type",
        )
        .agg(
            F.avg("speed_kmh")              .alias("avg_speed_kmh"),
            F.max("speed_kmh")              .alias("max_speed_kmh"),
            F.avg("traffic_density")        .alias("avg_traffic_density"),
            F.avg("fuel_rate_l100km")       .alias("avg_fuel_rate_l100km"),
            F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
            F.count(
                F.when(F.col("road_event") != "NONE", 1)
            )                               .alias("road_event_count"),
            F.countDistinct("vehicle_id")   .alias("unique_vehicles"),
            F.count("*")                    .alias("total_events"),
            # Speed band distribution — enables stacked bar chart in Grafana
            F.sum(F.when(F.col("speed_band") == "stopped",   1).otherwise(0)).alias("count_stopped"),
            F.sum(F.when(F.col("speed_band") == "slow",      1).otherwise(0)).alias("count_slow"),
            F.sum(F.when(F.col("speed_band") == "medium",    1).otherwise(0)).alias("count_medium"),
            F.sum(F.when(F.col("speed_band") == "fast",      1).otherwise(0)).alias("count_fast"),
            F.sum(F.when(F.col("speed_band") == "overspeed", 1).otherwise(0)).alias("count_overspeed"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        .withColumn("partition_date", F.date_format("window_start", "yyyy-MM-dd"))
    )


# ─────────────────────────────────────────────────────────────
# Aggregation 3: fuel_daily  (from silver/telemetry)
# ─────────────────────────────────────────────────────────────

def agg_fuel_daily(df: DataFrame) -> DataFrame:
    """
    1-day tumbling window per vehicle_type.
    CO2 assumption: 1 litre Egypt 95-octane petrol ≈ 2.31 kg CO2.
    Grafana: fleet fuel cost, CO2 estimate, efficiency trend by vehicle class.
    """
    CO2_KG_PER_LITRE = 2.31

    return (
        df
        .groupBy(
            F.window("event_time", "1 day"),
            "vehicle_type",
        )
        .agg(
            F.sum("fuel_consumed_l")        .alias("total_fuel_consumed_l"),
            F.avg("fuel_rate_l100km")       .alias("avg_fuel_rate_l100km"),
            F.avg("fuel_level_pct")         .alias("avg_fuel_level_pct"),
            F.countDistinct("vehicle_id")   .alias("unique_vehicles"),
            F.count("*")                    .alias("total_events"),
        )
        .withColumn(
            "estimated_co2_kg",
            F.round(F.col("total_fuel_consumed_l") * CO2_KG_PER_LITRE, 2),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        .withColumn("partition_date", F.date_format("window_start", "yyyy-MM-dd"))
    )


# ─────────────────────────────────────────────────────────────
# Aggregation 4: road_event_summary  (from silver/telemetry)
# ─────────────────────────────────────────────────────────────

def agg_road_event_summary(df: DataFrame) -> DataFrame:
    """
    15-minute window — road events extracted from telemetry road_event column.
    NOTE: This captures events as seen from vehicle telemetry perspective.
          agg_incident_summary() (below) captures the same events from the
          dedicated road-events topic — richer, has severity_score and exact GPS.
    Grafana: quick incident count overlay on route/time charts.
    """
    events_only = df.filter(F.col("road_event") != "NONE")
    return (
        events_only
        .groupBy(
            F.window("event_time", "15 minutes"),
            "road_event",
            "road_type",
            "route_name",
        )
        .agg(
            F.count("*")                  .alias("event_count"),
            F.countDistinct("vehicle_id") .alias("vehicles_involved"),
            F.avg("speed_kmh")            .alias("avg_speed_at_event_kmh"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        .withColumn("partition_date", F.date_format("window_start", "yyyy-MM-dd"))
    )


# ─────────────────────────────────────────────────────────────
# Aggregation 5: weather_hourly  (from silver/weather)
# ─────────────────────────────────────────────────────────────

def agg_weather_hourly(df: DataFrame) -> DataFrame:
    """
    1-hour tumbling window per weather condition + severity.
    Captures how Cairo weather evolves hour by hour.

    Key insight for dashboards:
      - speed_factor < 1.0 means weather is slowing vehicles
      - Join with route_hourly on window_start to answer:
        "Was this hour's congestion caused by weather or traffic?"
    Grafana: weather context panel, speed_factor trend, condition timeline.
    """
    return (
        df
        .groupBy(
            F.window("event_time", "1 hour"),
            "condition",
            "weather_severity",
            "location",
        )
        .agg(
            F.avg("temp_c")          .alias("avg_temp_c"),
            F.max("temp_c")          .alias("max_temp_c"),
            F.avg("feels_like_c")    .alias("avg_feels_like_c"),
            F.avg("humidity_pct")    .alias("avg_humidity_pct"),
            F.avg("wind_kmh")        .alias("avg_wind_kmh"),
            F.avg("visibility_km")   .alias("avg_visibility_km"),
            F.avg("pressure_hpa")    .alias("avg_pressure_hpa"),
            # speed_factor: how much this weather slows vehicles (1.0 = no effect)
            F.avg("speed_factor")    .alias("avg_speed_factor"),
            F.min("speed_factor")    .alias("min_speed_factor"),
            F.count("*")             .alias("observation_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        .withColumn("partition_date", F.date_format("window_start", "yyyy-MM-dd"))
    )


# ─────────────────────────────────────────────────────────────
# Aggregation 6: traffic_30min  (from silver/traffic)
# ─────────────────────────────────────────────────────────────

def agg_traffic_30min(df: DataFrame) -> DataFrame:
    """
    30-minute tumbling window per GPS grid bucket (~1.1 km grid).
    gps_lat_bucket and gps_lon_bucket are rounded to 2 decimal places
    (added by silver_cleaner.clean_traffic).

    Key insight for dashboards:
      - congestion_ratio = 1 - (current_speed / free_flow_speed)
      - A ratio > 0.75 = heavy congestion on that road segment
      - Join with vehicle_5min on time window + approximate GPS to answer:
        "Were our vehicles actually in the same congested area?"
    Grafana: congestion heatmap by GPS grid, speed deficit map.
    """
    return (
        df
        .groupBy(
            F.window("event_time", "30 minutes"),
            "gps_lat_bucket",
            "gps_lon_bucket",
            "congestion_band",
        )
        .agg(
            F.avg("congestion_ratio")       .alias("avg_congestion_ratio"),
            F.max("congestion_ratio")       .alias("max_congestion_ratio"),
            F.avg("current_speed_kmh")      .alias("avg_current_speed_kmh"),
            F.avg("free_flow_speed_kmh")    .alias("avg_free_flow_speed_kmh"),
            # Speed deficit: how much slower than free-flow (positive = slower)
            F.avg(
                F.col("free_flow_speed_kmh") - F.col("current_speed_kmh")
            )                               .alias("avg_speed_deficit_kmh"),
            F.avg("traffic_density")        .alias("avg_traffic_density"),
            F.sum(
                F.when(F.col("road_closure") == True, 1).otherwise(0)
            )                               .alias("road_closure_count"),
            F.count("*")                    .alias("observation_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        .withColumn("partition_date", F.date_format("window_start", "yyyy-MM-dd"))
    )


# ─────────────────────────────────────────────────────────────
# Aggregation 7: incident_summary  (from silver/road_events)
# ─────────────────────────────────────────────────────────────

def agg_incident_summary(df: DataFrame) -> DataFrame:
    """
    1-hour tumbling window per event_type + road_type.
    Reads from silver/road_events — the dedicated road events topic.
    Richer than road_event_summary because:
      - Has severity_score (1=ROADWORK, 2=BREAKDOWN, 3=CONGESTION, 4=ACCIDENT)
      - Has exact GPS coordinates of the incident
      - Has vehicle_id that reported it
    Grafana: incident severity heatmap, mean severity by route,
             accident rate vs road_type (highway vs urban vs arterial).
    """
    return (
        df
        .groupBy(
            F.window("event_time", "1 hour"),
            "event_type",
            "road_type",
        )
        .agg(
            F.count("*")                    .alias("incident_count"),
            F.countDistinct("vehicle_id")   .alias("vehicles_involved"),
            F.avg("severity_score")         .alias("avg_severity_score"),
            F.max("severity_score")         .alias("max_severity_score"),
            F.avg("latitude")               .alias("avg_latitude"),
            F.avg("longitude")              .alias("avg_longitude"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        .withColumn("partition_date", F.date_format("window_start", "yyyy-MM-dd"))
    )


# ─────────────────────────────────────────────────────────────
# Write helpers
# ─────────────────────────────────────────────────────────────

def write_gold_streaming(agg_df: DataFrame, agg_name: str) -> object:
    gold_dst = f"s3a://{S3_BUCKET}/gold/{agg_name}"
    ckpt     = checkpoint_path(f"gold_{agg_name}")
    query = (
        agg_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", gold_dst)
        .option("checkpointLocation", ckpt)
        .partitionBy("partition_date")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .queryName(f"gold_{agg_name}")
        .start()
    )
    log.info(f"[{agg_name}] Gold streaming query started → {gold_dst}")
    return query


def write_gold_batch(agg_df: DataFrame, agg_name: str) -> None:
    gold_dst = f"s3a://{S3_BUCKET}/gold/{agg_name}"
    row_count = agg_df.count()
    if row_count == 0:
        log.warning(f"[{agg_name}] 0 rows — skipping write")
        return
    (
        agg_df.write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("partition_date")
        .save(gold_dst)
    )
    log.info(f"[{agg_name}] Gold batch write complete: {row_count:,} rows → {gold_dst}")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    import argparse
    from datetime import datetime, timezone

    parser = argparse.ArgumentParser(description="Smart City Gold Aggregator v2")
    parser.add_argument("--mode",  default=GOLD_MODE, choices=["streaming", "batch"])
    parser.add_argument("--date",  default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    parser.add_argument("--hour",  type=int, default=datetime.now(timezone.utc).hour)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  Smart City — Gold Aggregator  v2")
    log.info(f"  Mode      : {args.mode}")
    log.info(f"  S3 bucket : {S3_BUCKET}")
    log.info(f"  Sources   : telemetry, weather, traffic, road_events")
    log.info(f"  Outputs   : 7 Gold tables")
    if args.mode == "batch":
        log.info(f"  Target    : {args.date}  hour={args.hour}")
    log.info("=" * 60)

    spark = build_spark()

    if args.mode == "streaming":
        # Each Silver source gets its own independent streaming read
        tel_df     = read_silver_telemetry_stream(spark)
        weather_df = read_silver_weather_stream(spark)
        traffic_df = read_silver_traffic_stream(spark)
        events_df  = read_silver_road_events_stream(spark)

        queries = [
            # From telemetry
            write_gold_streaming(agg_vehicle_5min(tel_df),       "vehicle_5min"),
            write_gold_streaming(agg_route_hourly(tel_df),       "route_hourly"),
            write_gold_streaming(agg_fuel_daily(tel_df),         "fuel_daily"),
            write_gold_streaming(agg_road_event_summary(tel_df), "road_event_summary"),
            # From weather
            write_gold_streaming(agg_weather_hourly(weather_df), "weather_hourly"),
            # From traffic
            write_gold_streaming(agg_traffic_30min(traffic_df),  "traffic_30min"),
            # From road_events
            write_gold_streaming(agg_incident_summary(events_df),"incident_summary"),
        ]
        log.info(f"All {len(queries)} Gold streaming queries running.")
        spark.streams.awaitAnyTermination()

    else:
        # ── Batch mode — process each Silver source independently ──
        # Telemetry (main source — 4 aggregations)
        tel_df = read_silver_telemetry_batch(spark, args.date, args.hour)
        if not tel_df.rdd.isEmpty():
            write_gold_batch(agg_vehicle_5min(tel_df),       "vehicle_5min")
            write_gold_batch(agg_route_hourly(tel_df),       "route_hourly")
            write_gold_batch(agg_fuel_daily(tel_df),         "fuel_daily")
            write_gold_batch(agg_road_event_summary(tel_df), "road_event_summary")
        else:
            log.warning(f"No Silver telemetry for {args.date} h={args.hour} — skipping 4 aggs")

        # Weather (1 aggregation)
        weather_df = read_silver_weather_batch(spark, args.date, args.hour)
        if not weather_df.rdd.isEmpty():
            write_gold_batch(agg_weather_hourly(weather_df), "weather_hourly")
        else:
            log.warning(f"No Silver weather for {args.date} h={args.hour} — skipping weather_hourly")

        # Traffic (1 aggregation)
        traffic_df = read_silver_traffic_batch(spark, args.date, args.hour)
        if not traffic_df.rdd.isEmpty():
            write_gold_batch(agg_traffic_30min(traffic_df),  "traffic_30min")
        else:
            log.warning(f"No Silver traffic for {args.date} h={args.hour} — skipping traffic_30min")

        # Road events (1 aggregation)
        events_df = read_silver_road_events_batch(spark, args.date, args.hour)
        if not events_df.rdd.isEmpty():
            write_gold_batch(agg_incident_summary(events_df),"incident_summary")
        else:
            log.warning(f"No Silver road_events for {args.date} h={args.hour} — skipping incident_summary")

        log.info(f"Gold batch complete — {args.date} h={args.hour}")


if __name__ == "__main__":
    main()
