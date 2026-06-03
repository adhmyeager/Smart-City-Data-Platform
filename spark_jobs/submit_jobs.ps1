# ============================================================
#  Smart City — Spark Job Submit Script
#  submit_jobs.ps1
#
#  Usage:
#    .\submit_jobs.ps1 -Job bronze       # Kafka → S3 Bronze (streaming)
#    .\submit_jobs.ps1 -Job silver       # S3 Bronze → Silver (streaming)
#    .\submit_jobs.ps1 -Job alerts       # Kafka → Kafka alerts (streaming)
#    .\submit_jobs.ps1 -Job gold         # Silver → Gold (streaming)
#    .\submit_jobs.ps1 -Job test         # Quick Kafka connectivity test
#    .\submit_jobs.ps1 -Job s3test       # S3 read/write test
#    .\submit_jobs.ps1 -Job silver_batch -Date 2025-01-15 -Hour 9
#    .\submit_jobs.ps1 -Job gold_batch   -Date 2025-01-15 -Hour 9
#
#  All streaming jobs run until you press Ctrl+C.
#  Check Spark UI: http://localhost:8081
# ============================================================

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("bronze","silver","alerts","gold","test","s3test","silver_batch","gold_batch")]
    [string]$Job,

    [string]$Date = (Get-Date -Format "yyyy-MM-dd"),
    [int]$Hour    = (Get-Date).Hour
)

# ─── JAR list (all 6 required — verified working) ───────────
$JARS = @(
    "/opt/spark/jars/extra/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.261.jar",
    "/opt/spark/jars/extra/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "/opt/spark/jars/extra/kafka-clients-3.5.0.jar",
    "/opt/spark/jars/extra/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "/opt/spark/jars/extra/commons-pool2-2.11.1.jar"
) -join ","

$CONTAINER  = "sc_spark_master"
$SPARK_BIN  = "/opt/spark/bin/spark-submit"
$MASTER     = "spark://spark-master:7077"
$JOBS_DIR   = "/opt/spark_jobs"

# ─── Base spark-submit command ───────────────────────────────
function Invoke-SparkSubmit {
    param(
        [string]$Script,
        [string]$ExtraArgs = "",
        [int]$MaxCores = 2,
        [string]$DriverMemory = "512m",
        [string]$ExecutorMemory = "512m"
    )
    
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  Submitting : $Script" -ForegroundColor Cyan
    Write-Host "  Max Cores  : $MaxCores" -ForegroundColor Cyan
    Write-Host "  Spark UI   : http://localhost:8081" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
    
    $ExtraArgsList = $ExtraArgs.Split(" ", [System.StringSplitOptions]::RemoveEmptyEntries)
    
    docker exec -it $CONTAINER $SPARK_BIN `
        --master $MASTER `
        --jars $JARS `
        --conf "spark.cores.max=$MaxCores" `
        --conf "spark.executor.memory=$ExecutorMemory" `
        --conf "spark.driver.memory=$DriverMemory" `
        "$JOBS_DIR/$Script" `
        @ExtraArgsList
}

# ─── Job dispatch ─────────────────────────────────────────────
switch ($Job) {
    "bronze" {
        Write-Host "Starting Bronze Writer (2 cores)..." -ForegroundColor Green
        Invoke-SparkSubmit "bronze_writer.py" -MaxCores 2
    }
    "silver" {
        Write-Host "Starting Silver Cleaner (1 core)..." -ForegroundColor Green
        Write-Host "  IMPORTANT: Start AFTER Bronze has written at least one batch" -ForegroundColor Yellow
        Invoke-SparkSubmit "silver_cleaner.py" -MaxCores 1
    }
    "silver_batch" {
        Invoke-SparkSubmit "silver_cleaner.py" "--mode batch --date $Date --hour $Hour" -MaxCores 1
    }
    "alerts" {
        Write-Host "Starting Alert Detector (1 core)..." -ForegroundColor Green
        Invoke-SparkSubmit "alert_detector.py" -MaxCores 1
    }
    "gold" {
        Write-Host "Starting Gold Aggregator (streaming, 1 core)..." -ForegroundColor Green
        Write-Host "  IMPORTANT: Start AFTER Silver has real Parquet files" -ForegroundColor Yellow
        Invoke-SparkSubmit "gold_aggregator.py" "--mode streaming" -MaxCores 1
    }
    "gold_batch" {
        Invoke-SparkSubmit "gold_aggregator.py" "--mode batch --date $Date --hour $Hour" -MaxCores 1
    }
    "test" {
        Invoke-SparkSubmit "test_spark.py" -MaxCores 1
    }
    "s3test" {
        docker exec -it $CONTAINER $SPARK_BIN `
            --master "local[2]" --jars $JARS `
            "$JOBS_DIR/test_s3_connection.py"
    }
}
