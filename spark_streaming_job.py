"""
spark_streaming_job.py
======================
CairoFlow Smart City — Spark Structured Streaming Pipeline

Architecture:
  Kafka → Bronze (raw)
        → Silver (cleaned + enriched)   reads from Bronze Parquet
        → Gold   (30s window agg)       reads from Silver Parquet
        → Alerts (threshold breaches)   reads from Silver Parquet

Layered design guarantees:
  Bronze count >= Silver count (Silver filters invalid records)
  Silver count >= Gold count   (Gold aggregates into windows)

Topics consumed:
  • vehicle-telemetry   (every 1s per vehicle)
  • weather-data        (every 5 min)
  • traffic-events      (every 60s)

Run:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    spark_streaming_job.py
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, BooleanType, LongType, DoubleType,
    TimestampType,
)

# ─── Config ────────────────────────────────────────────────────────────────────

KAFKA_BROKER    = "sc_kafka:9092"
CHECKPOINT_BASE = "/tmp/spark_checkpoints"
OUTPUT_BASE     = "/tmp/spark_output"

TOPIC_TELEMETRY = "vehicle-telemetry"
TOPIC_WEATHER   = "weather-data"
TOPIC_TRAFFIC   = "traffic-events"

# Alert thresholds
ALERT_SPEED_KMH     = 120.0
ALERT_ENGINE_TEMP_C = 105.0
ALERT_FUEL_PCT      = 10.0
ALERT_RPM           = 5000


# ─── Schemas ───────────────────────────────────────────────────────────────────

TELEMETRY_SCHEMA = StructType([
    StructField("event_id",          StringType(),  True),
    StructField("vehicle_id",        StringType(),  True),
    StructField("vehicle_type",      StringType(),  True),
    StructField("route_name",        StringType(),  True),
    StructField("timestamp_iso",     StringType(),  True),
    StructField("timestamp_unix",    LongType(),    True),
    StructField("latitude",          DoubleType(),  True),
    StructField("longitude",         DoubleType(),  True),
    StructField("altitude_m",        FloatType(),   True),
    StructField("heading_deg",       FloatType(),   True),
    StructField("gps_accuracy_m",    FloatType(),   True),
    StructField("speed_kmh",         FloatType(),   True),
    StructField("acceleration_ms2",  FloatType(),   True),
    StructField("rpm",               IntegerType(), True),
    StructField("gear",              IntegerType(), True),
    StructField("engine_temp_c",     FloatType(),   True),
    StructField("engine_on",         BooleanType(), True),
    StructField("fuel_level_pct",    FloatType(),   True),
    StructField("fuel_consumed_l",   FloatType(),   True),
    StructField("fuel_rate_l100km",  FloatType(),   True),
    StructField("road_type",         StringType(),  True),
    StructField("road_event",        StringType(),  True),
    StructField("traffic_density",   IntegerType(), True),
    StructField("trip_id",           StringType(),  True),
    StructField("odometer_km",       FloatType(),   True),
    StructField("trip_distance_km",  FloatType(),   True),
    StructField("engine_runtime_s",  IntegerType(), True),
])

WEATHER_SCHEMA = StructType([
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
])

TRAFFIC_SCHEMA = StructType([
    StructField("latitude",            DoubleType(),  True),
    StructField("longitude",           DoubleType(),  True),
    StructField("current_speed_kmh",   FloatType(),   True),
    StructField("free_flow_speed_kmh", FloatType(),   True),
    StructField("congestion_ratio",    FloatType(),   True),
    StructField("traffic_density",     IntegerType(), True),
    StructField("confidence",          FloatType(),   True),
    StructField("road_closure",        BooleanType(), True),
    StructField("timestamp_unix",      LongType(),    True),
    StructField("source",              StringType(),  True),
])

# Silver schema = Bronze telemetry fields + ingested_at + derived columns
SILVER_TELEMETRY_SCHEMA = StructType(
    TELEMETRY_SCHEMA.fields + [
        StructField("ingested_at", TimestampType(), True),
        StructField("event_time",  TimestampType(), True),
        StructField("is_moving",   BooleanType(),   True),
        StructField("speed_band",  StringType(),    True),
        StructField("fuel_band",   StringType(),    True),
    ]
)


# ─── Spark Session ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CairoFlow-SmartCity-Streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ─── Kafka Reader ──────────────────────────────────────────────────────────────

def read_kafka_topic(spark: SparkSession, topic: str, schema: StructType) -> DataFrame:
    """Read a Kafka topic and parse the JSON payload into typed columns."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(F.from_json(F.col("value"), schema).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", F.current_timestamp())
    )


# ─── Stream Writer ─────────────────────────────────────────────────────────────

def stream_writer(df: DataFrame, checkpoint_folder: str, output_path: str,
                  trigger: str = "10 seconds", partition_by: str = None):
    """Write a streaming DataFrame to Parquet."""
    writer = (
        df.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint_folder)
        .option("path", output_path)
        .outputMode("append")
        .trigger(processingTime=trigger)
    )
    if partition_by:
        writer = writer.partitionBy(partition_by)
    return writer.start()


# ═══════════════════════════════════════════════════════════════════════════════
# BRONZE — Raw ingest from Kafka, no transformations
# ═══════════════════════════════════════════════════════════════════════════════

def start_bronze(spark: SparkSession):
    """
    Read all three Kafka topics and write raw records to Parquet.
    Nothing is filtered or transformed here — exactly what arrived from Kafka.
    """
    telemetry_df = read_kafka_topic(spark, TOPIC_TELEMETRY, TELEMETRY_SCHEMA)
    weather_df   = read_kafka_topic(spark, TOPIC_WEATHER,   WEATHER_SCHEMA)
    traffic_df   = read_kafka_topic(spark, TOPIC_TRAFFIC,   TRAFFIC_SCHEMA)

    q_telemetry = stream_writer(
        telemetry_df,
        checkpoint_folder=f"{CHECKPOINT_BASE}/bronze/telemetry",
        output_path=f"{OUTPUT_BASE}/bronze/telemetry",
        trigger="10 seconds",
    )
    q_weather = stream_writer(
        weather_df,
        checkpoint_folder=f"{CHECKPOINT_BASE}/bronze/weather",
        output_path=f"{OUTPUT_BASE}/bronze/weather",
        trigger="10 seconds",
    )
    q_traffic = stream_writer(
        traffic_df,
        checkpoint_folder=f"{CHECKPOINT_BASE}/bronze/traffic",
        output_path=f"{OUTPUT_BASE}/bronze/traffic",
        trigger="10 seconds",
    )

    return q_telemetry, q_weather, q_traffic


# ═══════════════════════════════════════════════════════════════════════════════
# SILVER — Read from Bronze Parquet, clean + enrich
# ═══════════════════════════════════════════════════════════════════════════════

def build_silver_telemetry(spark: SparkSession) -> DataFrame:
    """
    Read Bronze telemetry Parquet and apply:
      • Parse timestamp_iso → event_time (TimestampType)
      • Filter: Cairo bounding box + physical value ranges
      • Enrich: is_moving, speed_band, fuel_band
    """
    bronze_df = (
        spark.readStream
        .schema(
            StructType(TELEMETRY_SCHEMA.fields + [
                StructField("ingested_at", TimestampType(), True)
            ])
        )
        .format("parquet")
        .option("path", f"{OUTPUT_BASE}/bronze/telemetry")
        .load()
    )

    return (
        bronze_df
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp_iso"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
        )
        # Cairo bounding box
        .filter(
            F.col("latitude").between(29.5, 30.5) &
            F.col("longitude").between(30.8, 31.9)
        )
        # Physical sanity checks
        .filter(
            (F.col("speed_kmh").between(0, 200))    &
            (F.col("fuel_level_pct").between(0, 100)) &
            (F.col("engine_temp_c").between(20, 120))
        )
        .withColumn("is_moving", F.col("speed_kmh") > 5.0)
        .withColumn(
            "speed_band",
            F.when(F.col("speed_kmh") < 20,  "crawling")
             .when(F.col("speed_kmh") < 50,  "slow")
             .when(F.col("speed_kmh") < 90,  "normal")
             .when(F.col("speed_kmh") < 120, "fast")
             .otherwise("speeding")
        )
        .withColumn(
            "fuel_band",
            F.when(F.col("fuel_level_pct") < 10,  "critical")
             .when(F.col("fuel_level_pct") < 25,  "low")
             .when(F.col("fuel_level_pct") < 60,  "medium")
             .otherwise("full")
        )
        .drop("timestamp_iso")
    )


def start_silver(spark: SparkSession):
    """Write cleaned Silver telemetry to Parquet, partitioned by vehicle."""
    silver_df = build_silver_telemetry(spark)
    q_silver = stream_writer(
        silver_df,
        checkpoint_folder=f"{CHECKPOINT_BASE}/silver/telemetry",
        output_path=f"{OUTPUT_BASE}/silver/telemetry",
        trigger="15 seconds",
        partition_by="vehicle_id",
    )
    return q_silver


# ═══════════════════════════════════════════════════════════════════════════════
# GOLD — Read from Silver Parquet, 30s tumbling window aggregations
# ═══════════════════════════════════════════════════════════════════════════════

def build_gold_aggregations(spark: SparkSession) -> DataFrame:
    """
    Read Silver telemetry and compute 30-second tumbling window KPIs per vehicle.
    Input is already clean — no re-filtering needed.
    """
    silver_df = (
        spark.readStream
        .schema(SILVER_TELEMETRY_SCHEMA)
        .format("parquet")
        .option("path", f"{OUTPUT_BASE}/silver/telemetry")
        .load()
    )

    return (
        silver_df
        .withWatermark("event_time", "1 minute")
        .groupBy(
            F.window("event_time", "30 seconds"),
            F.col("vehicle_id"),
            F.col("vehicle_type"),
            F.col("route_name"),
        )
        .agg(
            F.avg("speed_kmh")        .alias("avg_speed_kmh"),
            F.max("speed_kmh")        .alias("max_speed_kmh"),
            F.min("speed_kmh")        .alias("min_speed_kmh"),
            F.avg("rpm")              .alias("avg_rpm"),
            F.avg("engine_temp_c")    .alias("avg_engine_temp_c"),
            F.max("engine_temp_c")    .alias("max_engine_temp_c"),
            F.avg("fuel_level_pct")   .alias("avg_fuel_pct"),
            F.min("fuel_level_pct")   .alias("min_fuel_pct"),
            F.sum("fuel_consumed_l")  .alias("total_fuel_consumed_l"),
            F.avg("traffic_density")  .alias("avg_traffic_density"),
            F.count("*")              .alias("reading_count"),
            F.count(
                F.when(F.col("road_event") != "NONE", 1)
            ).alias("road_events_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
        .withColumn("avg_speed_kmh",         F.round("avg_speed_kmh", 1))
        .withColumn("avg_rpm",               F.round("avg_rpm", 0).cast(IntegerType()))
        .withColumn("avg_engine_temp_c",     F.round("avg_engine_temp_c", 1))
        .withColumn("avg_fuel_pct",          F.round("avg_fuel_pct", 1))
        .withColumn("total_fuel_consumed_l", F.round("total_fuel_consumed_l", 4))
        .withColumn("avg_traffic_density",   F.round("avg_traffic_density", 1))
    )


def start_gold(spark: SparkSession):
    """Write Gold KPIs to Parquet and print to console."""
    gold_df = build_gold_aggregations(spark)

    q_gold_parquet = stream_writer(
        gold_df,
        checkpoint_folder=f"{CHECKPOINT_BASE}/gold/vehicle_kpis",
        output_path=f"{OUTPUT_BASE}/gold/vehicle_kpis",
        trigger="30 seconds",
    )
    q_gold_console = (
        gold_df.writeStream
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )
    return q_gold_parquet, q_gold_console


# ═══════════════════════════════════════════════════════════════════════════════
# ALERTS — Read from Silver Parquet, fire on threshold breach
# ═══════════════════════════════════════════════════════════════════════════════

def build_alerts(spark: SparkSession) -> DataFrame:
    """
    Detect anomalies from Silver stream.
    Uses a separate Silver read so Alerts and Gold don't interfere.
    """
    silver_df = (
        spark.readStream
        .schema(SILVER_TELEMETRY_SCHEMA)
        .format("parquet")
        .option("path", f"{OUTPUT_BASE}/silver/telemetry")
        .load()
    )

    return (
        silver_df
        .withColumn("alert_speed",       F.col("speed_kmh")      > ALERT_SPEED_KMH)
        .withColumn("alert_engine_temp", F.col("engine_temp_c")   > ALERT_ENGINE_TEMP_C)
        .withColumn("alert_low_fuel",    F.col("fuel_level_pct")  < ALERT_FUEL_PCT)
        .withColumn("alert_high_rpm",    F.col("rpm")             > ALERT_RPM)
        .filter(
            F.col("alert_speed")       |
            F.col("alert_engine_temp") |
            F.col("alert_low_fuel")    |
            F.col("alert_high_rpm")
        )
        .withColumn(
            "alert_reasons",
            F.concat_ws(", ",
                F.when(F.col("alert_speed"),       F.lit(f"SPEED>{ALERT_SPEED_KMH}kmh")),
                F.when(F.col("alert_engine_temp"), F.lit(f"TEMP>{ALERT_ENGINE_TEMP_C}C")),
                F.when(F.col("alert_low_fuel"),    F.lit(f"FUEL<{ALERT_FUEL_PCT}%")),
                F.when(F.col("alert_high_rpm"),    F.lit(f"RPM>{ALERT_RPM}")),
            )
        )
        .select(
            "event_id", "vehicle_id", "vehicle_type", "route_name", "event_time",
            "speed_kmh", "engine_temp_c", "fuel_level_pct", "rpm",
            "latitude", "longitude",
            "alert_speed", "alert_engine_temp", "alert_low_fuel", "alert_high_rpm",
            "alert_reasons",
        )
    )


def start_alerts(spark: SparkSession):
    """Write Alerts to Parquet and print to console."""
    alerts_df = build_alerts(spark)

    q_alerts_parquet = stream_writer(
        alerts_df,
        checkpoint_folder=f"{CHECKPOINT_BASE}/alerts",
        output_path=f"{OUTPUT_BASE}/alerts",
        trigger="5 seconds",
    )
    q_alerts_console = (
        alerts_df.writeStream
        .format("console")
        .option("truncate", False)
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )
    return q_alerts_parquet, q_alerts_console


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("""
╔══════════════════════════════════════════════════════════════════╗
║   CairoFlow — Spark Structured Streaming Pipeline              ║
║                                                                ║
║   Kafka ──▶ Bronze ──▶ Silver ──▶ Gold                         ║
║                             └───▶ Alerts                       ║
║                                                                ║
║   Bronze >= Silver >= Gold    (counts guaranteed)              ║
╚══════════════════════════════════════════════════════════════════╝
    """)

    # Layer 1 — Bronze: Kafka → raw Parquet
    q_b_tel, q_b_wea, q_b_tra = start_bronze(spark)
    print(f"[Bronze] telemetry → {OUTPUT_BASE}/bronze/telemetry")
    print(f"[Bronze] weather   → {OUTPUT_BASE}/bronze/weather")
    print(f"[Bronze] traffic   → {OUTPUT_BASE}/bronze/traffic\n")

    # Layer 2 — Silver: Bronze Parquet → cleaned Parquet
    q_silver = start_silver(spark)
    print(f"[Silver] telemetry → {OUTPUT_BASE}/silver/telemetry\n")

    # Layer 3 — Gold: Silver Parquet → aggregated Parquet + console
    q_gold_pq, q_gold_con = start_gold(spark)
    print(f"[Gold]   kpis      → {OUTPUT_BASE}/gold/vehicle_kpis\n")

    # Layer 4 — Alerts: Silver Parquet → alert Parquet + console
    q_alert_pq, q_alert_con = start_alerts(spark)
    print(f"[Alerts] anomalies → {OUTPUT_BASE}/alerts\n")

    print("All queries active. Ctrl+C to stop.\n")

    all_queries = [
        q_b_tel, q_b_wea, q_b_tra,
        q_silver,
        q_gold_pq, q_gold_con,
        q_alert_pq, q_alert_con,
    ]

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        for q in all_queries:
            try:
                q.stop()
            except Exception:
                pass
        spark.stop()
        print("Done.")


if __name__ == "__main__":
    main()
