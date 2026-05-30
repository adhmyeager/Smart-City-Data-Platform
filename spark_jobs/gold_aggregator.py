"""
spark_jobs/gold_aggregator.py

Layer: S3 Silver  →  S3 Gold  (aggregated KPIs, window functions)

Aggregations produced (all written as Parquet to S3 Gold):

  1. vehicle_5min
     ─ 5-minute tumbling window per vehicle_id
     ─ avg/max speed, avg fuel rate, avg engine temp, total distance, event count
     ─ Used by: Grafana real-time vehicle dashboard

  2. route_hourly
     ─ Hourly aggregation per route_name
     ─ avg congestion, avg speed, anomaly count, unique vehicles
     ─ Used by: Grafana traffic analytics dashboard

  3. fuel_daily
     ─ Daily fuel summary per vehicle_type
     ─ total fuel consumed, avg fuel rate, avg efficiency
     ─ Used by: Grafana fuel & environmental dashboard

  4. road_event_summary
     ─ 15-minute window count of road events by type + road_type
     ─ Used by: Grafana KPI overview (incident rate)

Run (streaming, continuous Gold updates):
  docker exec sc_spark_master \
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --jars /opt/spark/jars/extra/hadoop-aws-3.3.4.jar,\
             /opt/spark/jars/extra/aws-java-sdk-bundle-1.12.261.jar \
      /opt/spark_jobs/gold_aggregator.py

Run (batch, hourly via Airflow):
  /opt/spark_jobs/gold_aggregator.py --mode batch --date 2025-01-15 --hour 9
"""

import os
import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

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
# SparkSession
# ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("SmartCity-GoldAggregator")
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Watermark state expiry — drop state older than 30 min
        .config("spark.sql.streaming.statefulOperator.stateExpiry.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
    )
    builder = configure_spark_s3(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created — Gold Aggregator")
    return spark


# ─────────────────────────────────────────────────────────────
# Read Silver telemetry
# ─────────────────────────────────────────────────────────────

def read_silver_streaming(spark: SparkSession) -> DataFrame:
    """Stream new Silver Parquet files as they arrive."""
    silver_src = f"s3a://{S3_BUCKET}/silver/telemetry"

    from utils.schemas import SILVER_TELEMETRY_SCHEMA

    df = (
        spark.readStream
        .schema(SILVER_TELEMETRY_SCHEMA)
        .option("recursiveFileLookup", "true")
        .parquet(silver_src)
        # event_time is a proper TimestampType in Silver
        .withWatermark("event_time", "10 minutes")   # tolerate 10-min late arrivals
    )
    log.info(f"Silver streaming source: {silver_src}")
    return df


def read_silver_batch(spark: SparkSession, date: str, hour: int) -> DataFrame:
    """Read a single Silver hour-partition for batch aggregation."""
    silver_src = (
        f"s3a://{S3_BUCKET}/silver/telemetry"
        f"/partition_date={date}/partition_hour={hour:02d}"
    )
    log.info(f"Silver batch source: {silver_src}")
    return spark.read.parquet(silver_src)


# ─────────────────────────────────────────────────────────────
# Aggregation 1: vehicle_5min
# ─────────────────────────────────────────────────────────────

def agg_vehicle_5min(df: DataFrame) -> DataFrame:
    """
    5-minute tumbling window per vehicle.
    Grafana uses this for: speed timeline, fuel gauge, engine temp, distance.
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
            # Speed
            F.avg("speed_kmh")         .alias("avg_speed_kmh"),
            F.max("speed_kmh")         .alias("max_speed_kmh"),
            F.min("speed_kmh")         .alias("min_speed_kmh"),

            # Powertrain
            F.avg("rpm")               .alias("avg_rpm"),
            F.avg("engine_temp_c")     .alias("avg_engine_temp_c"),
            F.max("engine_temp_c")     .alias("max_engine_temp_c"),

            # Fuel
            F.avg("fuel_level_pct")    .alias("avg_fuel_level_pct"),
            F.avg("fuel_rate_l100km")  .alias("avg_fuel_rate_l100km"),
            F.sum("fuel_consumed_l")   .alias("total_fuel_consumed_l"),

            # Traffic
            F.avg("traffic_density")   .alias("avg_traffic_density"),

            # Trip progress
            F.max("trip_distance_km")  .alias("trip_distance_km"),

            # Quality
            F.count("*")               .alias("event_count"),
            F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
            F.sum(F.col("had_sensor_clamp").cast("int")).alias("clamped_count"),
        )
        # Unpack window struct → window_start, window_end
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        # Partition by date for efficient downstream reads
        .withColumn("partition_date", F.date_format("window_start", "yyyy-MM-dd"))
    )


# ─────────────────────────────────────────────────────────────
# Aggregation 2: route_hourly
# ─────────────────────────────────────────────────────────────

def agg_route_hourly(df: DataFrame) -> DataFrame:
    """
    1-hour tumbling window per route.
    Grafana uses this for: route congestion heatmap, throughput, anomaly rate.
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
            F.avg("traffic_density")        .alias("avg_traffic_density"),
            F.avg("fuel_rate_l100km")       .alias("avg_fuel_rate_l100km"),

            # Incident rate
            F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
            F.count(
                F.when(F.col("road_event") != "NONE", 1)
            )                               .alias("road_event_count"),

            # Throughput
            F.countDistinct("vehicle_id")   .alias("unique_vehicles"),
            F.count("*")                    .alias("total_events"),

            # Speed band distribution (for stacked bar charts)
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
# Aggregation 3: fuel_daily
# ─────────────────────────────────────────────────────────────

def agg_fuel_daily(df: DataFrame) -> DataFrame:
    """
    1-day tumbling window per vehicle_type.
    Grafana uses this for: fleet fuel cost, CO2 estimate, efficiency trend.
    Assumption: 1 litre petrol ≈ 2.31 kg CO2 (Egypt 95 octane)
    """
    CO2_KG_PER_LITRE = 2.31

    return (
        df
        .groupBy(
            F.window("event_time", "1 day"),
            "vehicle_type",
        )
        .agg(
            F.sum("fuel_consumed_l")       .alias("total_fuel_consumed_l"),
            F.avg("fuel_rate_l100km")      .alias("avg_fuel_rate_l100km"),
            F.avg("fuel_level_pct")        .alias("avg_fuel_level_pct"),
            F.countDistinct("vehicle_id")  .alias("unique_vehicles"),
            F.count("*")                   .alias("total_events"),
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
# Aggregation 4: road_event_summary
# ─────────────────────────────────────────────────────────────

def agg_road_event_summary(df: DataFrame) -> DataFrame:
    """
    15-minute window, count of road events by type and road_type.
    Grafana KPI: incidents per hour, event type breakdown.
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
# Aggregation 5: traffic_hourly (from silver/traffic)
# ─────────────────────────────────────────────────────────────
 
def agg_traffic_hourly(spark: SparkSession, date: str, hour: int) -> DataFrame:
    """
    Hourly aggregation of real traffic API data (TomTom / simulation).
    Separate from route_hourly which is derived from vehicle telemetry.
    This uses the raw TomTom congestion_ratio and free-flow speed.
 
    Grafana: congestion heatmap, actual vs free-flow speed comparison.
    """
    traffic_src = (
        f"s3a://{S3_BUCKET}/silver/traffic"
        f"/partition_date={date}/partition_hour={hour:02d}"
    )
    df = spark.read.parquet(traffic_src)
    if df.rdd.isEmpty():
        log.warning(f"[traffic_hourly] No silver/traffic data for {date} hour={hour}")
        return None
 
    return (
        df
        .groupBy(
            F.window("event_time", "1 hour"),
            "gps_lat_bucket",
            "gps_lon_bucket",
            "congestion_band",
        )
        .agg(
            F.avg("congestion_ratio")       .alias("avg_congestion_ratio"),
            F.max("congestion_ratio")       .alias("peak_congestion_ratio"),
            F.avg("current_speed_kmh")      .alias("avg_current_speed_kmh"),
            F.avg("free_flow_speed_kmh")    .alias("avg_free_flow_speed_kmh"),
            F.avg("traffic_density")        .alias("avg_traffic_density"),
            F.sum(F.col("road_closure").cast("int")).alias("road_closure_count"),
            F.count("*")                    .alias("reading_count"),
        )
        .withColumn("speed_loss_pct",
            F.round(
                (F.lit(1.0) - F.col("avg_current_speed_kmh") / F.col("avg_free_flow_speed_kmh"))
                * 100, 1
            )
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        .withColumn("partition_date", F.date_format("window_start", "yyyy-MM-dd"))
    )
 
 
# ─────────────────────────────────────────────────────────────
# Aggregation 6: weather_impact (JOIN telemetry + weather)
# ─────────────────────────────────────────────────────────────
 
def agg_weather_impact(spark: SparkSession, date: str, hour: int) -> DataFrame:
    """
    Joins silver/telemetry with silver/weather on hour to measure
    how weather conditions affect vehicle speed and fuel consumption.
 
    KEY DESIGN DECISION: Batch-only with broadcast join.
    Weather has at most 12 rows per hour (one per 5-min cache window),
    so we broadcast it — no shuffle needed.
 
    Join strategy:
      Both tables → truncate event_time to hour → LEFT JOIN on join_hour.
      This matches every telemetry event to the weather reading for that hour.
 
    Grafana: scatter plot of speed vs weather condition, fuel cost by weather.
    """
    tel_src = (
        f"s3a://{S3_BUCKET}/silver/telemetry"
        f"/partition_date={date}/partition_hour={hour:02d}"
    )
    wx_src = (
        f"s3a://{S3_BUCKET}/silver/weather"
        f"/partition_date={date}/partition_hour={hour:02d}"
    )
 
    try:
        telemetry = spark.read.parquet(tel_src)
        weather   = spark.read.parquet(wx_src)
    except Exception as e:
        log.warning(f"[weather_impact] Could not read silver data: {e}")
        return None
 
    if telemetry.rdd.isEmpty() or weather.rdd.isEmpty():
        log.warning(f"[weather_impact] Empty silver data for {date} hour={hour}")
        return None
 
    # Round both to the nearest hour as the join key
    telemetry = telemetry.withColumn(
        "join_hour", F.date_trunc("hour", F.col("event_time"))
    )
    weather = weather.withColumn(
        "join_hour", F.date_trunc("hour", F.col("event_time"))
    ).select(
        "join_hour", "condition", "weather_severity", "speed_factor",
        "temp_c", "wind_kmh", "visibility_km", "humidity_pct"
    )
 
    # Broadcast join: weather is tiny (~12 rows/hour)
    joined = telemetry.join(F.broadcast(weather), on="join_hour", how="left")
 
    return (
        joined
        .groupBy("join_hour", "condition", "weather_severity", "route_name", "road_type")
        .agg(
            F.avg("speed_kmh")              .alias("avg_speed_kmh"),
            F.avg("fuel_rate_l100km")       .alias("avg_fuel_rate_l100km"),
            F.avg("engine_temp_c")          .alias("avg_engine_temp_c"),
            F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
            F.count("*")                    .alias("event_count"),
 
            # Weather context (same for all rows with same join_hour)
            F.first("temp_c")               .alias("temp_c"),
            F.first("wind_kmh")             .alias("wind_kmh"),
            F.first("visibility_km")        .alias("visibility_km"),
            F.first("speed_factor")         .alias("weather_speed_factor"),
        )
        # Speed loss vs clear-weather baseline (speed_factor=1.0 → 100% speed)
        .withColumn(
            "speed_loss_vs_clear_pct",
            F.round((F.lit(1.0) - F.col("weather_speed_factor")) * 100, 1)
        )
        .withColumn("partition_date", F.date_format("join_hour", "yyyy-MM-dd"))
    )
 
 
# ─────────────────────────────────────────────────────────────
# Aggregation 7: congestion_hotspots (JOIN telemetry + traffic)
# ─────────────────────────────────────────────────────────────
 
def agg_congestion_hotspots(spark: SparkSession, date: str, hour: int) -> DataFrame:
    """
    Joins silver/telemetry with silver/traffic on GPS grid bucket + hour
    to produce a GPS-indexed congestion heatmap.
 
    GPS bucket: 0.01 degree ≈ 1.1 km grid cell (same bucket used in silver/traffic).
    This lets Grafana Worldmap show a heatmap of Cairo congestion.
 
    Grafana: worldmap panel — coloured by congestion_ratio, sized by vehicle count.
    """
    tel_src = (
        f"s3a://{S3_BUCKET}/silver/telemetry"
        f"/partition_date={date}/partition_hour={hour:02d}"
    )
    trx_src = (
        f"s3a://{S3_BUCKET}/silver/traffic"
        f"/partition_date={date}/partition_hour={hour:02d}"
    )
 
    try:
        telemetry = spark.read.parquet(tel_src)
        traffic   = spark.read.parquet(trx_src)
    except Exception as e:
        log.warning(f"[congestion_hotspots] Could not read silver data: {e}")
        return None
 
    if telemetry.rdd.isEmpty() or traffic.rdd.isEmpty():
        log.warning(f"[congestion_hotspots] Empty silver data for {date} hour={hour}")
        return None
 
    # Add GPS bucket to telemetry (mirrors what silver/traffic already has)
    telemetry = (
        telemetry
        .withColumn("gps_lat_bucket", F.round(F.col("latitude"),  2))
        .withColumn("gps_lon_bucket", F.round(F.col("longitude"), 2))
        .withColumn("join_hour",      F.date_trunc("hour", F.col("event_time")))
    )
 
    # Aggregate traffic per GPS bucket + hour (small table after aggregation)
    traffic_agg = (
        traffic
        .withColumn("join_hour", F.date_trunc("hour", F.col("event_time")))
        .groupBy("gps_lat_bucket", "gps_lon_bucket", "join_hour")
        .agg(
            F.avg("congestion_ratio")  .alias("avg_congestion_ratio"),
            F.avg("traffic_density")   .alias("avg_traffic_density"),
            F.avg("current_speed_kmh") .alias("traffic_speed_kmh"),
            F.first("congestion_band") .alias("congestion_band"),
        )
    )
 
    # Broadcast the aggregated traffic (now small) into telemetry join
    joined = telemetry.join(
        F.broadcast(traffic_agg),
        on=["gps_lat_bucket", "gps_lon_bucket", "join_hour"],
        how="left"
    )
 
    return (
        joined
        .groupBy(
            "join_hour",
            "gps_lat_bucket",
            "gps_lon_bucket",
            "congestion_band",
            "route_name",
        )
        .agg(
            # Traffic sensor data
            F.avg("avg_congestion_ratio") .alias("congestion_ratio"),
            F.avg("avg_traffic_density")  .alias("traffic_density"),
            F.avg("traffic_speed_kmh")    .alias("traffic_speed_kmh"),
 
            # Vehicle telemetry at this location
            F.avg("speed_kmh")            .alias("vehicle_avg_speed_kmh"),
            F.countDistinct("vehicle_id") .alias("vehicle_count"),
            F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
        )
        # Speed discrepancy: vehicle speed vs what TomTom reports for that road
        .withColumn(
            "speed_discrepancy_kmh",
            F.round(F.col("vehicle_avg_speed_kmh") - F.col("traffic_speed_kmh"), 1)
        )
        .withColumn("partition_date", F.date_format("join_hour", "yyyy-MM-dd"))
    )

# ─────────────────────────────────────────────────────────────
# Write helpers
# ─────────────────────────────────────────────────────────────

def write_gold_streaming(agg_df: DataFrame, agg_name: str) -> object:
    """Write aggregation result as a streaming Gold query."""
    gold_dst = f"s3a://{S3_BUCKET}/gold/{agg_name}"
    ckpt     = checkpoint_path(f"gold_{agg_name}")

    query = (
        agg_df.writeStream
        .format("parquet")
        .outputMode("append")       # append-mode with watermark
        .option("path", gold_dst)
        .option("checkpointLocation", ckpt)
        .partitionBy("partition_date")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .queryName(f"gold_{agg_name}")
        .start()
    )
    log.info(f"[{agg_name}] Gold query started → {gold_dst}")
    return query


def write_gold_batch(agg_df: DataFrame, agg_name: str):
    """Write aggregation result as a batch Gold output (overwrite partition)."""
    gold_dst = f"s3a://{S3_BUCKET}/gold/{agg_name}"

    (
        agg_df.write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("partition_date")
        .save(gold_dst)
    )
    log.info(f"[{agg_name}] Gold batch write complete → {gold_dst}")


# ─────────────────────────────────────────────────────────────
# Full batch run — moved to module level (fix)
# ─────────────────────────────────────────────────────────────
def run_batch_complete(spark, date, hour):
    """
    Full batch run — all 7 Gold aggregations for one hour partition.
    Called by Airflow daily_pipeline DAG for each hour.
    Idempotent: re-running the same date+hour overwrites that partition.
    """
    df = read_silver_batch(spark, date, hour)
    if df.rdd.isEmpty():
        log.warning("No Silver telemetry data — skipping Gold aggregation")
        return

    if "event_time" not in df.columns:
        df = df.withColumn("event_time", F.col("timestamp_unix").cast(TimestampType()))

    # ── Telemetry-only Gold marts (existing 4, streaming-safe) ──
    write_gold_batch(agg_vehicle_5min(df),       "vehicle_5min")
    write_gold_batch(agg_route_hourly(df),       "route_hourly")
    write_gold_batch(agg_fuel_daily(df),         "fuel_daily")
    write_gold_batch(agg_road_event_summary(df), "road_event_summary")

    # ── New Gold marts (need additional Silver tables) ──
    traffic_agg = agg_traffic_hourly(spark, date, hour)
    if traffic_agg is not None:
        write_gold_batch(traffic_agg, "traffic_hourly")

    weather_agg = agg_weather_impact(spark, date, hour)
    if weather_agg is not None:
        write_gold_batch(weather_agg, "weather_impact")

    hotspot_agg = agg_congestion_hotspots(spark, date, hour)
    if hotspot_agg is not None:
        write_gold_batch(hotspot_agg, "congestion_hotspots")

    log.info(f"Gold batch complete — {date} hour={hour} — all 7 marts written.")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    import argparse
    from datetime import datetime, timezone

    parser = argparse.ArgumentParser(description="Smart City Gold Aggregator")
    parser.add_argument("--mode",  default=GOLD_MODE, choices=["streaming", "batch"])
    parser.add_argument("--date",  default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    parser.add_argument("--hour",  type=int, default=datetime.now(timezone.utc).hour)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  Smart City — Gold Aggregator")
    log.info(f"  Mode      : {args.mode}")
    log.info(f"  S3 bucket : {S3_BUCKET}")
    log.info(f"  Trigger   : {TRIGGER_SECONDS}s")
    log.info("=" * 60)

    spark = build_spark()

    if args.mode == "streaming":
        df = read_silver_streaming(spark)
        queries = [
            write_gold_streaming(agg_vehicle_5min(df),       "vehicle_5min"),
            write_gold_streaming(agg_route_hourly(df),       "route_hourly"),
            write_gold_streaming(agg_fuel_daily(df),         "fuel_daily"),
            write_gold_streaming(agg_road_event_summary(df), "road_event_summary"),
        ]
        log.info(f"All {len(queries)} Gold queries running. Awaiting termination …")
        spark.streams.awaitAnyTermination()

    else:
        run_batch_complete(spark, args.date, args.hour)


if __name__ == "__main__":
    main()