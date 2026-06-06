<#
.SYNOPSIS
    Helper to spark-submit any Smart City job into the running Docker container.

.USAGE
    # Start all 3 jobs (bronze + silver streaming + alert detector)
    .\spark_jobs\submit_jobs.ps1 -Job all

    # Start individual jobs
    .\spark_jobs\submit_jobs.ps1 -Job bronze
    .\spark_jobs\submit_jobs.ps1 -Job silver
    .\spark_jobs\submit_jobs.ps1 -Job gold
    .\spark_jobs\submit_jobs.ps1 -Job alerts

    # Run silver in batch mode for a specific hour
    .\spark_jobs\submit_jobs.ps1 -Job silver -Mode batch -Date "2025-01-15" -Hour 9
#>

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("bronze","silver","gold","alerts","all")]
    [string]$Job,

    [string]$Mode  = "streaming",
    [string]$Date  = (Get-Date -Format "yyyy-MM-dd"),
    [int]   $Hour  = (Get-Date).Hour
)

$CONTAINER  = "sc_spark_master"
$SPARK_HOME = "/opt/spark"
$JOBS_DIR   = "/opt/spark_jobs"
$JARS_DIR   = "/opt/spark/jars/extra"
$MASTER     = "spark://spark-master:7077"

# JARs needed per job type
$KAFKA_JAR  = "$JARS_DIR/spark-sql-kafka-0-10_2.12-3.5.0.jar"
$KAFKA_TOKEN= "$JARS_DIR/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
$S3_JARS    = "$JARS_DIR/hadoop-aws-3.3.4.jar,$JARS_DIR/aws-java-sdk-bundle-1.12.261.jar"
$POOL_JAR   = "$JARS_DIR/commons-pool2-2.11.1.jar"
$ALL_JARS   = "$KAFKA_JAR,$KAFKA_TOKEN,$S3_JARS,$POOL_JAR"

function Submit-SparkJob {
    param([string]$AppName, [string]$ScriptPath, [string]$ExtraArgs = "", [string]$Jars = $ALL_JARS)

    $cmd = @(
        "$SPARK_HOME/bin/spark-submit",
        "--master", $MASTER,
        "--name", $AppName,
        "--jars", $Jars,
        "--conf", "spark.sql.shuffle.partitions=4",
        "--conf", "spark.driver.memory=512m",
        "--conf", "spark.executor.memory=1g",
        $ScriptPath
    )
    if ($ExtraArgs) { $cmd += $ExtraArgs.Split(" ") }

    $fullCmd = $cmd -join " "
    Write-Host ""
    Write-Host "Submitting: $AppName"
    Write-Host "Command   : $fullCmd"
    Write-Host ""

    docker exec -d $CONTAINER bash -c $fullCmd
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] $AppName submitted (running in background)"
        Write-Host "     Check: docker logs $CONTAINER -f"
        Write-Host "     UI   : http://localhost:8081"
    } else {
        Write-Host "[ERROR] Failed to submit $AppName" -ForegroundColor Red
    }
}

# Check container is running
$running = docker ps --filter "name=$CONTAINER" --filter "status=running" -q
if (-not $running) {
    Write-Host "[ERROR] Container $CONTAINER is not running." -ForegroundColor Red
    Write-Host "        Run: docker compose up -d"
    exit 1
}

switch ($Job) {
    "bronze" {
        Submit-SparkJob "BronzeWriter" "$JOBS_DIR/bronze_writer.py" -Jars $ALL_JARS
    }
    "silver" {
        $args = "--mode $Mode"
        if ($Mode -eq "batch") { $args += " --date $Date --hour $Hour" }
        # Silver only needs S3 JARs (reads Parquet, no Kafka)
        Submit-SparkJob "SilverCleaner" "$JOBS_DIR/silver_cleaner.py" $args -Jars "$S3_JARS"
    }
    "gold" {
        $args = "--mode $Mode"
        if ($Mode -eq "batch") { $args += " --date $Date --hour $Hour" }
        Submit-SparkJob "GoldAggregator" "$JOBS_DIR/gold_aggregator.py" $args -Jars "$S3_JARS"
    }
    "alerts" {
        Submit-SparkJob "AlertDetector" "$JOBS_DIR/alert_detector.py" -Jars $ALL_JARS
    }
    "all" {
        Write-Host "Starting all streaming jobs …"
        Submit-SparkJob "BronzeWriter"   "$JOBS_DIR/bronze_writer.py"   -Jars $ALL_JARS
        Start-Sleep -Seconds 5
        Submit-SparkJob "AlertDetector"  "$JOBS_DIR/alert_detector.py"  -Jars $ALL_JARS
        Write-Host ""
        Write-Host "NOTE: SilverCleaner and GoldAggregator wait for Bronze data."
        Write-Host "      Start them after first Bronze files appear in S3 (~30s):"
        Write-Host "      .\spark_jobs\submit_jobs.ps1 -Job silver"
        Write-Host "      .\spark_jobs\submit_jobs.ps1 -Job gold"
    }
}

Write-Host ""
Write-Host "Active Spark jobs: http://localhost:8081"
