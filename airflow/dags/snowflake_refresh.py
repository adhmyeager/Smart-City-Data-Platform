from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# ── 1. CONFIGURATION ──────────────────────────────────────────────────────────
SNOWFLAKE_CONN  = "snowflake_smart_city"   # اسم الاتصال في Airflow UI
S3_BUCKET       = "smart-city-datalake"     # اسم الـ S3 Bucket بتاعك
GOLD_PREFIX     = "gold/"                   # الفولدر المستهدف

# ── 2. STATIC REFRESH SQL COMMANDS ────────────────────────────────────────────
REFRESH_SQL_COMMANDS = """
    USE DATABASE SMART_CITY_DB;
    USE WAREHOUSE SMART_CITY_WH;
    
    ALTER EXTERNAL TABLE raw.vehicle_5min REFRESH;
    ALTER EXTERNAL TABLE raw.route_hourly REFRESH;
    ALTER EXTERNAL TABLE raw.fuel_daily REFRESH;
    ALTER EXTERNAL TABLE raw.road_event_summary REFRESH;
    ALTER EXTERNAL TABLE raw.weather_hourly REFRESH;
    ALTER EXTERNAL TABLE raw.traffic_30min REFRESH;
    ALTER EXTERNAL TABLE raw.incident_summary REFRESH;
"""

# ── 3. DAG DEFINITION ─────────────────────────────────────────────────────────
default_args = {
    "owner":            "omar_marwan",
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="smart_city_snowflake_refresh",
    default_args=default_args,
    description="Optimized Event-Driven DAG to refresh Snowflake External Tables",
    schedule_interval="0 * * * *",   # يشتغل أوتوماتيك رأس كل ساعة
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["smart-city", "snowflake", "data-engineering"],
) as dag:

    # ── Task 1: Wait for new files in S3 ──────────────────────────────────────
    wait_for_s3_data = S3KeySensor(
        task_id        = "wait_for_s3_data",
        bucket_key     = f"s3://{S3_BUCKET}/{GOLD_PREFIX}*/*.parquet",
        wildcard_match = True,
        aws_conn_id    = "aws_smart_city",  # اسم اتصال AWS في Airflow UI
        timeout        = 3600,              # أقصى مدة انتظار ساعة كاملة
        poke_interval  = 120,               # يشيك على الـ S3 كل دقيقتين
        mode           = "reschedule"       # يترك الـ RAM والـ Worker أثناء فترة النوم لتوفير الموارد
    )

    # ── Task 2: Refresh all external tables ───────────────────────────────────
    refresh_external_tables = SnowflakeOperator(
        task_id           = "refresh_external_tables",
        snowflake_conn_id = SNOWFLAKE_CONN,
        sql               = REFRESH_SQL_COMMANDS,
        autocommit        = True,
    )

    # ── Task 3: Verify row counts after refresh ───────────────────────────────
    verify_refresh = SnowflakeOperator(
        task_id           = "verify_refresh",
        snowflake_conn_id = SNOWFLAKE_CONN,
        sql               = """
            SELECT 'vehicle_5min'      AS tbl, COUNT(*) AS rows FROM raw.vehicle_5min
            UNION ALL
            SELECT 'route_hourly',              COUNT(*) FROM raw.route_hourly
            UNION ALL
            SELECT 'fuel_daily',                COUNT(*) FROM raw.fuel_daily
            UNION ALL
            SELECT 'road_event_summary',        COUNT(*) FROM raw.road_event_summary
            UNION ALL
            SELECT 'weather_hourly',            COUNT(*) FROM raw.weather_hourly
            UNION ALL
            SELECT 'traffic_30min',             COUNT(*) FROM raw.traffic_30min
            UNION ALL
            SELECT 'incident_summary',          COUNT(*) FROM raw.incident_summary
            ORDER BY tbl;
        """,
    )

    # ── 4. PIPELINE LINEAGE ───────────────────────────────────────────────────
    wait_for_s3_data >> refresh_external_tables >> verify_refresh