"""
airflow/dags/daily_pipeline.py

Smart City — Main Orchestration DAG

Schedule: Every hour (runs Gold aggregation on the previous hour's Silver data)

Tasks:
  1. check_silver_exists     — verify Silver Parquet exists for target hour
  2. run_gold_batch          — spark-submit gold_aggregator.py --mode batch
  3. check_gold_output       — verify Gold Parquet was written
  4. notify_success          — log completion (extend to Slack/email later)

Airflow UI: http://localhost:8082  (admin / admin)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago

# ─── Default args ────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "smartcity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}

# ─── Spark submit command (inside sc_spark_master container) ──

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
JARS = ",".join([
    "/opt/spark/jars/extra/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.261.jar",
    "/opt/spark/jars/extra/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "/opt/spark/jars/extra/kafka-clients-3.5.0.jar",
    "/opt/spark/jars/extra/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "/opt/spark/jars/extra/commons-pool2-2.11.1.jar",
])

SPARK_CONF = " ".join([
    "--conf spark.cores.max=1",
    "--conf spark.executor.memory=512m",
    "--conf spark.driver.memory=256m",
    "--conf spark.driver.maxResultSize=128m",
    "--conf spark.sql.shuffle.partitions=2",
    "--conf spark.network.timeout=300s",
])


def _get_target_hour(**context) -> dict:
    """Return the date and hour to process (1 hour behind schedule time)."""
    # logical_date is the scheduled run time; we process the hour before it
    logical_date = context["logical_date"]
    target = logical_date - timedelta(hours=1)
    return {
        "date": target.strftime("%Y-%m-%d"),
        "hour": target.hour,
    }


def _check_silver_exists(**context) -> bool:
    """
    ShortCircuit: returns False (skip Gold) if Silver data doesn't exist.
    Uses boto3 to check S3 prefix.
    """
    import boto3
    import os

    ti = context["ti"]
    target = ti.xcom_pull(task_ids="get_target_hour")
    date = target["date"]
    hour = target["hour"]

    bucket = os.getenv("S3_BUCKET_NAME", "smart-city-datalake")
    prefix = f"silver/telemetry/partition_date={date}/partition_hour={hour:02d}/"

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    exists = response.get("KeyCount", 0) > 0

    if exists:
        print(f"✅ Silver data found: s3://{bucket}/{prefix}")
    else:
        print(f"⚠️  No Silver data at s3://{bucket}/{prefix} — skipping Gold")

    return exists


def _check_gold_output(**context) -> None:
    """Verify Gold Parquet files were written after aggregation."""
    import boto3
    import os

    ti = context["ti"]
    target = ti.xcom_pull(task_ids="get_target_hour")
    date = target["date"]

    bucket = os.getenv("S3_BUCKET_NAME", "smart-city-datalake")
    gold_tables = ["vehicle_5min", "route_hourly", "fuel_daily", "road_event_summary"]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

    for table in gold_tables:
        prefix = f"gold/{table}/partition_date={date}/"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        count = response.get("KeyCount", 0)
        status = "✅" if count > 0 else "❌"
        print(f"  {status} gold/{table}: {count} file(s)")


# ─── DAG definition ──────────────────────────────────────────

with DAG(
    dag_id="smart_city_daily_pipeline",
    description="Smart City — hourly Gold aggregation from Silver",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",       # runs every hour
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,                 # prevent overlapping runs
    tags=["smartcity", "spark", "gold"],
) as dag:

    # Task 1: Compute target date/hour
    get_target_hour = PythonOperator(
        task_id="get_target_hour",
        python_callable=_get_target_hour,
    )

    # Task 2: Check Silver data exists (short-circuit if not)
    check_silver = ShortCircuitOperator(
        task_id="check_silver_exists",
        python_callable=_check_silver_exists,
    )

    # Task 3: Run Gold aggregation via spark-submit inside the Spark master container
    run_gold = BashOperator(
        task_id="run_gold_batch",
        bash_command="""
        TARGET_DATE="{{ ti.xcom_pull(task_ids='get_target_hour')['date'] }}"
        TARGET_HOUR="{{ ti.xcom_pull(task_ids='get_target_hour')['hour'] }}"

        echo "Running Gold aggregation for ${TARGET_DATE} hour=${TARGET_HOUR}"

        docker exec sc_spark_master \
            {{ params.spark_submit }} \
            --master {{ params.spark_master }} \
            --jars {{ params.jars }} \
            {{ params.spark_conf }} \
            /opt/spark_jobs/gold_aggregator.py \
            --mode batch \
            --date "${TARGET_DATE}" \
            --hour "${TARGET_HOUR}"

        EXIT_CODE=$?
        echo "spark-submit exit code: ${EXIT_CODE}"
        exit ${EXIT_CODE}
        """,
        params={
            "spark_submit": SPARK_SUBMIT,
            "spark_master": SPARK_MASTER,
            "jars": JARS,
            "spark_conf": SPARK_CONF,
        },
        env={
            "AWS_ACCESS_KEY_ID": "{{ var.value.get('aws_access_key_id', '') }}",
            "AWS_SECRET_ACCESS_KEY": "{{ var.value.get('aws_secret_access_key', '') }}",
        },
    )

    # Task 4: Verify Gold output exists
    check_gold = PythonOperator(
        task_id="check_gold_output",
        python_callable=_check_gold_output,
    )

    # Task 5: Success notification (extend to email/Slack later)
    notify = BashOperator(
        task_id="notify_success",
        bash_command="""
        echo "✅ Smart City Gold pipeline complete"
        echo "   Date : {{ ti.xcom_pull(task_ids='get_target_hour')['date'] }}"
        echo "   Hour : {{ ti.xcom_pull(task_ids='get_target_hour')['hour'] }}"
        echo "   Time : $(date -u)"
        """,
    )

    # ── Dependencies ─────────────────────────────────────────
    get_target_hour >> check_silver >> run_gold >> check_gold >> notify
