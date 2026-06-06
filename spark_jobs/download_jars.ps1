<#
.SYNOPSIS
    Download required Spark JARs for Kafka + S3 integration.
    Run this ONCE before starting the stack.

.USAGE
    cd D:\ITI campaign\ITI content\final_project\data_sources\test
    .\spark_jobs\download_jars.ps1

    JARs are saved to: .\spark\jars\
    They are mounted into Spark containers at: /opt/spark/jars/extra/
#>

$JarDir = "$PSScriptRoot\..\spark\jars"

# Create directory if it doesn't exist
if (-not (Test-Path $JarDir)) {
    New-Item -ItemType Directory -Path $JarDir | Out-Null
    Write-Host "[+] Created $JarDir"
}

$MAVEN = "https://repo1.maven.org/maven2"

$Jars = @(
    @{
        Name = "spark-sql-kafka-0-10_2.12-3.5.0.jar"
        Url  = "$MAVEN/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar"
    },
    @{
        Name = "kafka-clients-3.5.0.jar"
        Url  = "$MAVEN/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar"
    },
    @{
        Name = "hadoop-aws-3.3.4.jar"
        Url  = "$MAVEN/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    },
    @{
        Name = "aws-java-sdk-bundle-1.12.261.jar"
        Url  = "$MAVEN/com/amazonaws/aws-java-sdk-bundle/1.12.261/aws-java-sdk-bundle-1.12.261.jar"
    },
    @{
        # Required transitive dep of spark-sql-kafka
        Name = "spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
        Url  = "$MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
    },
    @{
        # Commons pool for Kafka connections
        Name = "commons-pool2-2.11.1.jar"
        Url  = "$MAVEN/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
    }
)

Write-Host ""
Write-Host "Downloading Spark JARs to: $JarDir"
Write-Host "=" * 60

$Total   = $Jars.Count
$Success = 0
$Skipped = 0

foreach ($jar in $Jars) {
    $dest = Join-Path $JarDir $jar.Name

    if (Test-Path $dest) {
        $size = (Get-Item $dest).Length / 1MB
        Write-Host "  [SKIP] $($jar.Name) (already exists, $([math]::Round($size,1)) MB)"
        $Skipped++
        continue
    }

    Write-Host "  [DL]   $($jar.Name) ..."
    try {
        $ProgressPreference = 'SilentlyContinue'   # hide progress bar (faster)
        Invoke-WebRequest -Uri $jar.Url -OutFile $dest -UseBasicParsing
        $size = (Get-Item $dest).Length / 1MB
        Write-Host "         -> $([math]::Round($size,1)) MB  OK"
        $Success++
    } catch {
        Write-Host "         -> FAILED: $($_.Exception.Message)" -ForegroundColor Red
        if (Test-Path $dest) { Remove-Item $dest }
    }
}

Write-Host ""
Write-Host "=" * 60
Write-Host "Done: $Success downloaded, $Skipped skipped, $($Total - $Success - $Skipped) failed"
Write-Host ""
Write-Host "JARs are mounted into Spark at: /opt/spark/jars/extra/"
Write-Host "No container rebuild needed."
