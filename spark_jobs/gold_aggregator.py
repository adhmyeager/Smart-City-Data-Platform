"""
spark_jobs/gold_aggregator.py

BATCH JOB — Gold Layer
Reads ALL silver data and produces aggregated KPIs:
  - Average speed per route
  - Fuel consumption per vehicle type
  - Congestion score per road type
  - Alert summary (speeding, overheating, low fuel)
  - Vehicle performance summary

Run manually or via Airflow scheduler:
docker exec sc_spark_master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.cores.max=2 `
  /opt/spark_jobs/gold_aggregator.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, count, sum as spark_sum,
    max as spark_max, min as spark_min,
    round as spark_round, col,
    current_timestamp, lit,
    when
)
import datetime

# ── Spark Session ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SmartCity-Gold-Aggregator") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "/opt/spark_jobs/data/silver/vehicle-telemetry"
GOLD_PATH   = "/opt/spark_jobs/data/gold"
RUN_TIME    = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

print("\n" + "═"*60)
print("  SmartCity Gold Aggregator — Batch Job")
print(f"  Run time: {RUN_TIME}")
print("═"*60 + "\n")

# ── Read ALL silver data ──────────────────────────────────────
print("📖 Reading silver data...")
df = spark.read.json(SILVER_PATH)
total_records = df.count()
print(f"   Total records found: {total_records:,}")

if total_records == 0:
    print("❌ No silver data found! Run silver_cleaner.py first.")
    spark.stop()
    exit(1)

# ══════════════════════════════════════════════════════════════
# KPI 1 — Average speed per route
# ══════════════════════════════════════════════════════════════
print("\n📊 Calculating KPI 1: Speed per route...")
speed_per_route = df.groupBy("route_name").agg(
    spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
    spark_round(spark_max("speed_kmh"), 2).alias("max_speed_kmh"),
    spark_round(spark_min("speed_kmh"), 2).alias("min_speed_kmh"),
    count("*").alias("total_readings"),
    spark_round(
        (count(when(col("is_speeding") == True, 1)) / count("*") * 100), 2
    ).alias("speeding_pct")
).withColumn("batch_time", lit(RUN_TIME))

speed_per_route.show(truncate=False)
speed_per_route.write.mode("overwrite").json(
    f"{GOLD_PATH}/speed_per_route"
)
print("✅ Speed per route saved!")

# ══════════════════════════════════════════════════════════════
# KPI 2 — Fuel consumption per vehicle type
# ══════════════════════════════════════════════════════════════
print("\n📊 Calculating KPI 2: Fuel consumption per vehicle type...")
fuel_per_type = df.groupBy("vehicle_type").agg(
    spark_round(avg("fuel_level_pct"), 2).alias("avg_fuel_pct"),
    spark_round(avg("fuel_consumed_l"), 6).alias("avg_fuel_consumed_l"),
    spark_round(spark_sum("fuel_consumed_l"), 4).alias("total_fuel_consumed_l"),
    count(when(col("is_low_fuel") == True, 1)).alias("low_fuel_alerts"),
    count("*").alias("total_readings")
).withColumn("batch_time", lit(RUN_TIME))

fuel_per_type.show(truncate=False)
fuel_per_type.write.mode("overwrite").json(
    f"{GOLD_PATH}/fuel_per_vehicle_type"
)
print("✅ Fuel consumption saved!")

# ══════════════════════════════════════════════════════════════
# KPI 3 — Traffic congestion per road type
# ══════════════════════════════════════════════════════════════
print("\n📊 Calculating KPI 3: Congestion per road type...")
congestion_per_road = df.groupBy("road_type").agg(
    spark_round(avg("traffic_density"), 2).alias("avg_traffic_density"),
    spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
    count("*").alias("total_readings"),
    spark_round(
        avg(when(col("road_event") != "NONE", 1).otherwise(0)) * 100, 2
    ).alias("event_rate_pct")
).withColumn("batch_time", lit(RUN_TIME))

congestion_per_road.show(truncate=False)
congestion_per_road.write.mode("overwrite").json(
    f"{GOLD_PATH}/congestion_per_road_type"
)
print("✅ Congestion data saved!")

# ══════════════════════════════════════════════════════════════
# KPI 4 — Vehicle health summary
# ══════════════════════════════════════════════════════════════
print("\n📊 Calculating KPI 4: Vehicle health summary...")
vehicle_health = df.groupBy("vehicle_id", "vehicle_type").agg(
    spark_round(avg("engine_temp_c"), 2).alias("avg_engine_temp_c"),
    spark_round(spark_max("engine_temp_c"), 2).alias("max_engine_temp_c"),
    spark_round(avg("rpm"), 0).alias("avg_rpm"),
    spark_round(spark_max("speed_kmh"), 2).alias("max_speed_kmh"),
    spark_round(avg("fuel_level_pct"), 2).alias("avg_fuel_pct"),
    count(when(col("needs_attention") == True, 1)).alias("attention_count"),
    count("*").alias("total_readings")
).withColumn("health_score",
    spark_round(
        100 -
        (col("attention_count") / col("total_readings") * 100),
        2
    )
).withColumn("batch_time", lit(RUN_TIME))

vehicle_health.show(truncate=False)
vehicle_health.write.mode("overwrite").json(
    f"{GOLD_PATH}/vehicle_health"
)
print("✅ Vehicle health saved!")

# ══════════════════════════════════════════════════════════════
# KPI 5 — Overall summary
# ══════════════════════════════════════════════════════════════
print("\n📊 Calculating KPI 5: Overall summary...")
summary = df.agg(
    count("*").alias("total_records"),
    spark_round(avg("speed_kmh"), 2).alias("overall_avg_speed_kmh"),
    spark_round(avg("engine_temp_c"), 2).alias("overall_avg_engine_temp"),
    spark_round(avg("fuel_level_pct"), 2).alias("overall_avg_fuel_pct"),
    count(when(col("is_speeding") == True, 1)).alias("total_speeding_events"),
    count(when(col("is_overheating") == True, 1)).alias("total_overheating_events"),
    count(when(col("is_low_fuel") == True, 1)).alias("total_low_fuel_events"),
    count(when(col("needs_attention") == True, 1)).alias("total_attention_needed"),
).withColumn("batch_time", lit(RUN_TIME))

summary.show(truncate=False)
summary.write.mode("overwrite").json(
    f"{GOLD_PATH}/overall_summary"
)
print("✅ Overall summary saved!")

# ── Done ──────────────────────────────────────────────────────
print("\n" + "═"*60)
print("  ✅ Gold Aggregator Complete!")
print(f"  📁 Output: {GOLD_PATH}/")
print("     ├── speed_per_route/")
print("     ├── fuel_per_vehicle_type/")
print("     ├── congestion_per_road_type/")
print("     ├── vehicle_health/")
print("     └── overall_summary/")
print("═"*60 + "\n")

spark.stop()