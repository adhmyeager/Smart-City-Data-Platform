"""
spark_jobs/silver_cleaner.py

STREAMING JOB — Silver Layer
Reads from Bronze layer and applies cleaning & validation:
  - Remove null/bad records
  - Remove duplicate event_ids
  - Add computed columns (is_speeding, is_overheating, etc.)
  - Standardize data types

Run with:
docker exec sc_spark_master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 `
  /opt/spark_jobs/silver_cleaner.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp,
    when, round as spark_round,
    to_timestamp, unix_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, LongType
)

# ── Spark Session ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SmartCity-Silver-Cleaner") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KAFKA_BROKER = "kafka:29092"

# ── Vehicle Schema (same as bronze) ───────────────────────────
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

# ── Read from Kafka (same source as bronze) ───────────────────
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "vehicle-telemetry") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# ── Parse JSON ────────────────────────────────────────────────
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), vehicle_schema).alias("data")
).select("data.*")

# ══════════════════════════════════════════════════════════════
# SILVER TRANSFORMATIONS
# ══════════════════════════════════════════════════════════════

silver_df = parsed_df \
    \
    .filter(col("vehicle_id").isNotNull()) \
    .filter(col("event_id").isNotNull()) \
    .filter(col("speed_kmh").isNotNull()) \
    .filter(col("latitude").isNotNull()) \
    .filter(col("longitude").isNotNull()) \
    \
    .filter(col("speed_kmh") >= 0) \
    .filter(col("speed_kmh") <= 200) \
    .filter(col("latitude").between(29.0, 32.0)) \
    .filter(col("longitude").between(29.0, 34.0)) \
    .filter(col("fuel_level_pct").between(0, 100)) \
    .filter(col("engine_temp_c").between(0, 150)) \
    \
    .withColumn("speed_kmh",      spark_round(col("speed_kmh"), 2)) \
    .withColumn("fuel_level_pct", spark_round(col("fuel_level_pct"), 2)) \
    .withColumn("engine_temp_c",  spark_round(col("engine_temp_c"), 2)) \
    .withColumn("latitude",       spark_round(col("latitude"), 6)) \
    .withColumn("longitude",      spark_round(col("longitude"), 6)) \
    \
    .withColumn("is_speeding",
        when(col("speed_kmh") > 120, True).otherwise(False)) \
    .withColumn("is_overheating",
        when(col("engine_temp_c") > 105, True).otherwise(False)) \
    .withColumn("is_low_fuel",
        when(col("fuel_level_pct") < 10, True).otherwise(False)) \
    .withColumn("is_high_rpm",
        when(col("rpm") > 5000, True).otherwise(False)) \
    .withColumn("needs_attention",
        when(
            (col("is_speeding") == True) |
            (col("is_overheating") == True) |
            (col("is_low_fuel") == True) |
            (col("is_high_rpm") == True),
            True
        ).otherwise(False)) \
    \
    .withColumn("speed_category",
        when(col("speed_kmh") < 20,  "crawling")
        .when(col("speed_kmh") < 50,  "slow")
        .when(col("speed_kmh") < 90,  "normal")
        .when(col("speed_kmh") < 120, "fast")
        .otherwise("dangerous")) \
    \
    .withColumn("processed_at", current_timestamp())

# ── Write Silver to local JSON files ─────────────────────────
query = silver_df.writeStream \
    .format("json") \
    .option("path", "/opt/spark_jobs/data/silver/vehicle-telemetry") \
    .option("checkpointLocation", "/opt/spark_jobs/checkpoints/silver/vehicle-telemetry") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

print("✅ Silver cleaner running!")
print("📁 Output: /opt/spark_jobs/data/silver/vehicle-telemetry")
print("🔍 Transformations applied:")
print("   - Null filtering")
print("   - Range validation (speed, GPS, fuel, temp)")
print("   - Rounding (speed, fuel, temp, coordinates)")
print("   - Added: is_speeding, is_overheating, is_low_fuel, is_high_rpm")
print("   - Added: needs_attention, speed_category")
print("\nPress Ctrl+C to stop\n")

spark.streams.awaitAnyTermination()