"""
spark_jobs/bronze_writer.py

STREAMING JOB — Bronze Layer
Reads raw data from ALL Kafka topics and writes to local storage as JSON files.
This is the raw layer — no transformations, just store everything as-is.

Run with:
docker exec sc_spark_master spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 `
  /opt/spark_jobs/bronze_writer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, LongType
)

# ── Spark Session ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SmartCity-Bronze-Writer") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/spark_jobs/checkpoints/bronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KAFKA_BROKER = "kafka:29092"   # internal Docker address

# ══════════════════════════════════════════════════════════════
# 1. VEHICLE TELEMETRY SCHEMA
# ══════════════════════════════════════════════════════════════
vehicle_schema = StructType([
    StructField("event_id",         StringType()),
    StructField("vehicle_id",       StringType()),
    StructField("vehicle_type",     StringType()),
    StructField("route_name",       StringType()),
    StructField("timestamp_iso",    StringType()),
    StructField("timestamp_unix",   LongType()),
    StructField("latitude",         DoubleType()),
    StructField("longitude",        DoubleType()),
    StructField("altitude_m",       DoubleType()),
    StructField("heading_deg",      IntegerType()),
    StructField("speed_kmh",        DoubleType()),
    StructField("acceleration_ms2", DoubleType()),
    StructField("rpm",              IntegerType()),
    StructField("gear",             IntegerType()),
    StructField("engine_temp_c",    DoubleType()),
    StructField("engine_on",        BooleanType()),
    StructField("fuel_level_pct",   DoubleType()),
    StructField("fuel_consumed_l",  DoubleType()),
    StructField("road_type",        StringType()),
    StructField("road_event",       StringType()),
    StructField("traffic_density",  IntegerType()),
    StructField("trip_id",          StringType()),
    StructField("odometer_km",      DoubleType()),
])

# ══════════════════════════════════════════════════════════════
# 2. WEATHER SCHEMA
# ══════════════════════════════════════════════════════════════
weather_schema = StructType([
    StructField("location",           StringType()),
    StructField("latitude",           DoubleType()),
    StructField("longitude",          DoubleType()),
    StructField("temp_c",             DoubleType()),
    StructField("feels_like_c",       DoubleType()),
    StructField("humidity_pct",       IntegerType()),
    StructField("wind_kmh",           DoubleType()),
    StructField("wind_direction_deg", IntegerType()),
    StructField("condition",          StringType()),
    StructField("description",        StringType()),
    StructField("visibility_km",      DoubleType()),
    StructField("pressure_hpa",       IntegerType()),
    StructField("timestamp_unix",     LongType()),
    StructField("source",             StringType()),
])

# ══════════════════════════════════════════════════════════════
# 3. TRAFFIC SCHEMA
# ══════════════════════════════════════════════════════════════
traffic_schema = StructType([
    StructField("latitude",              DoubleType()),
    StructField("longitude",             DoubleType()),
    StructField("current_speed_kmh",     DoubleType()),
    StructField("free_flow_speed_kmh",   DoubleType()),
    StructField("congestion_ratio",      DoubleType()),
    StructField("traffic_density",       IntegerType()),
    StructField("confidence",            DoubleType()),
    StructField("road_closure",          BooleanType()),
    StructField("timestamp_unix",        LongType()),
    StructField("source",                StringType()),
])


# ── Helper: read from Kafka topic ─────────────────────────────
def read_kafka_topic(topic: str):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()


# ── Helper: write to local JSON files ─────────────────────────
def write_bronze(df, topic_name: str):
    return df.writeStream \
        .format("json") \
        .option("path", f"/opt/spark_jobs/data/bronze/{topic_name}") \
        .option("checkpointLocation", f"/opt/spark_jobs/checkpoints/bronze/{topic_name}") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()


# ══════════════════════════════════════════════════════════════
# START ALL 3 STREAMING QUERIES
# ══════════════════════════════════════════════════════════════

# 1. Vehicle telemetry
vehicle_raw = read_kafka_topic("vehicle-telemetry")
vehicle_df = vehicle_raw.select(
    from_json(col("value").cast("string"), vehicle_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp") \
 .withColumn("ingested_at", current_timestamp())

q1 = write_bronze(vehicle_df, "vehicle-telemetry")
print("✅ Vehicle telemetry stream started")

# 2. Weather data
weather_raw = read_kafka_topic("weather-data")
weather_df = weather_raw.select(
    from_json(col("value").cast("string"), weather_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp") \
 .withColumn("ingested_at", current_timestamp())

q2 = write_bronze(weather_df, "weather-data")
print("✅ Weather stream started")

# 3. Traffic events
traffic_raw = read_kafka_topic("traffic-events")
traffic_df = traffic_raw.select(
    from_json(col("value").cast("string"), traffic_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp") \
 .withColumn("ingested_at", current_timestamp())

q3 = write_bronze(traffic_df, "traffic-events")
print("✅ Traffic stream started")

print("\n🚀 Bronze writer running — writes every 30 seconds")
print("📁 Output: /opt/spark_jobs/data/bronze/")
print("Press Ctrl+C to stop\n")

# Keep all streams running
spark.streams.awaitAnyTermination()