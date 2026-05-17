# CairoFlow - Project Scaffold Script
# Run this in PowerShell as Administrator
# Usage: .\setup_project.ps1

$PROJECT = "cairoflow"
$BASE = "$PWD\$PROJECT"

Write-Host "`n CairoFlow project scaffold starting..." -ForegroundColor Cyan

# --- Folder structure ---
$folders = @(
    "$BASE\.github\workflows",
    "$BASE\docs",
    "$BASE\simulator\tests",
    "$BASE\simulator\routes",
    "$BASE\spark_jobs\utils",
    "$BASE\spark_jobs\tests",
    "$BASE\dbt_project\models\staging",
    "$BASE\dbt_project\models\intermediate",
    "$BASE\dbt_project\models\marts",
    "$BASE\dbt_project\tests",
    "$BASE\dbt_project\macros",
    "$BASE\airflow\dags",
    "$BASE\airflow\plugins",
    "$BASE\grafana\dashboards",
    "$BASE\grafana\provisioning\datasources",
    "$BASE\grafana\provisioning\dashboards",
    "$BASE\prometheus",
    "$BASE\infrastructure",
    "$BASE\notebooks",
    "$BASE\config"
)

foreach ($folder in $folders) {
    New-Item -ItemType Directory -Path $folder -Force | Out-Null
}

Write-Host " Folders created" -ForegroundColor Green

# --- .gitignore ---
@"
# Python
__pycache__/
*.py[cod]
*.egg-info/
.venv/
venv/
dist/
build/
*.egg

# Environment
.env
*.env
!.env.example

# Docker
.docker/

# Spark
spark-warehouse/
derby.log
metastore_db/

# dbt
dbt_project/target/
dbt_project/dbt_packages/
dbt_project/logs/
dbt_project/.user.yml

# Notebooks
.ipynb_checkpoints/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
desktop.ini

# Logs
*.log
logs/

# AWS credentials (NEVER commit these)
.aws/
credentials
"@ | Set-Content "$BASE\.gitignore"

# --- .env.example ---
@"
# =============================================
# CairoFlow - Environment Variables Template
# Copy this file to .env and fill in values
# NEVER commit .env to git
# =============================================

# AWS S3
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=us-east-1
S3_BUCKET_NAME=cairoflow-datalake

# Snowflake
SNOWFLAKE_ACCOUNT=your_account_here
SNOWFLAKE_USER=your_user_here
SNOWFLAKE_PASSWORD=your_password_here
SNOWFLAKE_DATABASE=CAIROFLOW_DB
SNOWFLAKE_WAREHOUSE=CAIROFLOW_WH
SNOWFLAKE_SCHEMA=RAW

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_TELEMETRY=vehicle-telemetry
KAFKA_TOPIC_WEATHER=weather-data
KAFKA_TOPIC_TRAFFIC=traffic-events
KAFKA_TOPIC_ROAD_EVENTS=road-events
KAFKA_TOPIC_ALERTS=alerts

# OpenWeatherMap
OPENWEATHER_API_KEY=your_api_key_here
OPENWEATHER_CITY=Cairo
OPENWEATHER_COUNTRY=EG

# TomTom (optional)
TOMTOM_API_KEY=your_api_key_here

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__FERNET_KEY=your_fernet_key_here
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW_CONN_AWS_DEFAULT=aws://your_key:your_secret@

# Grafana
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=cairoflow123

# Simulation settings
SIM_VEHICLE_COUNT=5
SIM_EMIT_INTERVAL_SECONDS=1
SIM_ROUTE=route_cairo_to_new_capital
"@ | Set-Content "$BASE\.env.example"

# --- docker-compose.yml ---
@"
version: '3.8'

services:

  # ─── Kafka stack ───────────────────────────────────────
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: cairoflow_zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - '2181:2181'
    networks: [cairoflow_net]

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: cairoflow_kafka
    depends_on: [zookeeper]
    ports:
      - '9092:9092'
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_LOG_RETENTION_HOURS: 24
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
    networks: [cairoflow_net]

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: cairoflow_kafka_ui
    depends_on: [kafka]
    ports:
      - '8080:8080'
    environment:
      KAFKA_CLUSTERS_0_NAME: cairoflow
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    networks: [cairoflow_net]

  # ─── Spark cluster ─────────────────────────────────────
  spark-master:
    image: bitnami/spark:3.5
    container_name: cairoflow_spark_master
    environment:
      SPARK_MODE: master
      SPARK_RPC_AUTHENTICATION_ENABLED: 'no'
      SPARK_UI_PORT: 8081
    ports:
      - '7077:7077'
      - '8081:8081'
    networks: [cairoflow_net]

  spark-worker-1:
    image: bitnami/spark:3.5
    container_name: cairoflow_spark_worker_1
    depends_on: [spark-master]
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 1G
      SPARK_WORKER_CORES: 2
    networks: [cairoflow_net]

  spark-worker-2:
    image: bitnami/spark:3.5
    container_name: cairoflow_spark_worker_2
    depends_on: [spark-master]
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 1G
      SPARK_WORKER_CORES: 2
    networks: [cairoflow_net]

  # ─── Airflow ───────────────────────────────────────────
  postgres:
    image: postgres:15
    container_name: cairoflow_postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks: [cairoflow_net]

  airflow:
    image: apache/airflow:2.8.1
    container_name: cairoflow_airflow
    depends_on: [postgres]
    ports:
      - '8082:8080'
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      AIRFLOW__CORE__FERNET_KEY: \${AIRFLOW__CORE__FERNET_KEY}
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/plugins:/opt/airflow/plugins
      - airflow_logs:/opt/airflow/logs
    command: >
      bash -c "airflow db init &&
               airflow users create --username admin --password admin
               --firstname Cairo --lastname Flow --role Admin --email admin@cairoflow.com &&
               airflow webserver"
    networks: [cairoflow_net]

  # ─── Monitoring ────────────────────────────────────────
  prometheus:
    image: prom/prometheus:latest
    container_name: cairoflow_prometheus
    ports:
      - '9090:9090'
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
    networks: [cairoflow_net]

  grafana:
    image: grafana/grafana:10.2.0
    container_name: cairoflow_grafana
    depends_on: [prometheus]
    ports:
      - '3000:3000'
    environment:
      GF_SECURITY_ADMIN_USER: \${GRAFANA_ADMIN_USER:-admin}
      GF_SECURITY_ADMIN_PASSWORD: \${GRAFANA_ADMIN_PASSWORD:-cairoflow123}
      GF_INSTALL_PLUGINS: grafana-clock-panel,grafana-worldmap-panel
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning
      - ./grafana/dashboards:/var/lib/grafana/dashboards
    networks: [cairoflow_net]

  # ─── Vehicle simulator ─────────────────────────────────
  simulator:
    build:
      context: ./simulator
      dockerfile: Dockerfile
    container_name: cairoflow_simulator
    depends_on: [kafka]
    env_file: .env
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    networks: [cairoflow_net]
    restart: on-failure

networks:
  cairoflow_net:
    driver: bridge

volumes:
  postgres_data:
  airflow_logs:
  grafana_data:
"@ | Set-Content "$BASE\docker-compose.yml"

# --- prometheus config ---
@"
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka:9308']

  - job_name: 'spark'
    static_configs:
      - targets: ['spark-master:8081']
"@ | Set-Content "$BASE\prometheus\prometheus.yml"

# --- GitHub Actions CI ---
@"
name: CI Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install -r simulator/requirements.txt
          pip install pytest flake8 black

      - name: Lint with flake8
        run: flake8 simulator/ spark_jobs/ --max-line-length=100

      - name: Check formatting with black
        run: black --check simulator/ spark_jobs/

      - name: Run unit tests
        run: pytest simulator/tests/ spark_jobs/tests/ -v

  dbt-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dbt
        run: pip install dbt-snowflake
      - name: dbt deps
        run: cd dbt_project && dbt deps
        env:
          SNOWFLAKE_ACCOUNT: \${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: \${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: \${{ secrets.SNOWFLAKE_PASSWORD }}
"@ | Set-Content "$BASE\.github\workflows\ci.yml"

# --- README ---
@"
# CairoFlow — Smart City Vehicle Intelligence Platform

![CI](https://github.com/YOUR_USERNAME/cairoflow/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Kafka](https://img.shields.io/badge/kafka-3.5-black)
![Spark](https://img.shields.io/badge/spark-3.5-orange)
![dbt](https://img.shields.io/badge/dbt-1.7-red)
![Snowflake](https://img.shields.io/badge/snowflake-DWH-29B5E8)
![Grafana](https://img.shields.io/badge/grafana-10-F46800)

> A production-grade Big Data Engineering platform simulating real-time vehicle
> telemetry and IoT data for Smart City monitoring in Cairo & the New Administrative Capital, Egypt.

---

## Architecture

![Architecture Diagram](docs/architecture.png)

**Data Flow:**
`Python Simulator → Kafka → Spark Streaming → S3 (Bronze/Silver/Gold) → dbt → Snowflake → Grafana`

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/cairoflow.git
cd cairoflow

# 2. Set up environment variables
cp .env.example .env
# Edit .env with your API keys

# 3. Start the full stack
docker compose up -d

# 4. Access services
# Kafka UI   → http://localhost:8080
# Spark UI   → http://localhost:8081
# Airflow    → http://localhost:8082
# Grafana    → http://localhost:3000
# Prometheus → http://localhost:9090
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Simulation | Python 3.11 |
| Message Broker | Apache Kafka 3.5 |
| Stream Processing | Apache Spark 3.5 (Structured Streaming) |
| Data Lake | AWS S3 (Bronze / Silver / Gold) |
| Orchestration | Apache Airflow 2.8 |
| Transformation | dbt (core) |
| Data Warehouse | Snowflake |
| Dashboards | Grafana 10 |
| Monitoring | Prometheus + Grafana |
| Containerization | Docker + Docker Compose |
| CI/CD | GitHub Actions |

---

## Data Sources

- **Vehicle Telemetry** — Physics-based Python simulator (speed, RPM, fuel, GPS, engine temp)
- **Weather** — OpenWeatherMap API (Cairo / New Capital)
- **Road Network** — OpenStreetMap / Overpass API (real Cairo GPS routes)
- **Traffic** — TomTom Traffic API (optional enrichment)

---

## Project Structure

\`\`\`
cairoflow/
├── simulator/          Vehicle telemetry simulator + Kafka producers
├── spark_jobs/         Streaming + batch Spark jobs
├── dbt_project/        dbt models, tests, and documentation
├── airflow/            Orchestration DAGs
├── grafana/            Dashboard definitions
├── prometheus/         Monitoring config
├── infrastructure/     S3 + Snowflake setup scripts
└── docs/               Architecture and data dictionary
\`\`\`

---

## Team

| Member | Role |
|--------|------|
| Member 1 | Infrastructure & DevOps |
| Member 2 | Streaming Engineer |
| Member 3 | Batch & Orchestration |
| Member 4 | Analytics & Dashboards |
| Member 5 | Documentation & Presentation |

---

## Dashboard Preview

> _Add Grafana dashboard screenshots here_

---

## License

MIT
"@ | Set-Content "$BASE\README.md"

# --- simulator requirements.txt ---
@"
kafka-python==2.0.2
requests==2.31.0
faker==22.0.0
python-dotenv==1.0.0
shapely==2.0.3
numpy==1.26.3
"@ | Set-Content "$BASE\simulator\requirements.txt"

# --- simulator Dockerfile ---
@"
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "kafka_producer.py"]
"@ | Set-Content "$BASE\simulator\Dockerfile"

# --- dbt project yml ---
@"
name: cairoflow
version: '1.0.0'
config-version: 2

profile: cairoflow_snowflake

model-paths: ['models']
test-paths: ['tests']
macro-paths: ['macros']
target-path: 'target'
clean-targets: ['target', 'dbt_packages']

models:
  cairoflow:
    staging:
      +schema: staging
      +materialized: view
    intermediate:
      +schema: intermediate
      +materialized: view
    marts:
      +schema: marts
      +materialized: table
"@ | Set-Content "$BASE\dbt_project\dbt_project.yml"

# --- dbt profiles example ---
@"
cairoflow_snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: SYSADMIN
      database: CAIROFLOW_DB
      warehouse: CAIROFLOW_WH
      schema: RAW
      threads: 4
      client_session_keep_alive: False
"@ | Set-Content "$BASE\dbt_project\profiles.yml.example"

# --- Placeholder Python files ---
"# Vehicle Simulator - See docs/setup.md for usage" | Set-Content "$BASE\simulator\vehicle_simulator.py"
"# Kafka Producer - Sends telemetry to Kafka topics" | Set-Content "$BASE\simulator\kafka_producer.py"
"# Weather Fetcher - Polls OpenWeatherMap API" | Set-Content "$BASE\simulator\weather_fetcher.py"
"# Cairo GPS Routes - Real OSM waypoints" | Set-Content "$BASE\simulator\routes\cairo_routes.py"
"import pytest" | Set-Content "$BASE\simulator\tests\test_simulator.py"

"# Spark Job: Kafka -> S3 Bronze (raw write)" | Set-Content "$BASE\spark_jobs\bronze_writer.py"
"# Spark Job: Bronze -> Silver (clean, validate, type)" | Set-Content "$BASE\spark_jobs\silver_cleaner.py"
"# Spark Job: Silver -> Gold (aggregate, enrich)" | Set-Content "$BASE\spark_jobs\gold_aggregator.py"
"# Spark Job: Real-time alert detection" | Set-Content "$BASE\spark_jobs\alert_detector.py"
"# Shared schemas for all Spark jobs" | Set-Content "$BASE\spark_jobs\utils\schemas.py"
"# S3 utility functions" | Set-Content "$BASE\spark_jobs\utils\s3_utils.py"
"import pytest" | Set-Content "$BASE\spark_jobs\tests\test_jobs.py"

"# Daily pipeline DAG" | Set-Content "$BASE\airflow\dags\daily_pipeline.py"
"# dbt runner DAG" | Set-Content "$BASE\airflow\dags\dbt_runner.py"
"# Data quality DAG" | Set-Content "$BASE\airflow\dags\data_quality.py"

"# S3 bucket setup + lifecycle rules" | Set-Content "$BASE\infrastructure\s3_setup.py"
"-- Snowflake DWH setup script" | Set-Content "$BASE\infrastructure\snowflake_setup.sql"

"# Architecture and data flow documentation" | Set-Content "$BASE\docs\architecture.md"
"# Step-by-step local setup guide" | Set-Content "$BASE\docs\setup.md"
"# Field definitions for all datasets" | Set-Content "$BASE\docs\data-dictionary.md"

Write-Host "`n All files and folders created successfully!" -ForegroundColor Green
Write-Host "`n Next steps:" -ForegroundColor Yellow
Write-Host "  1. cd $PROJECT" -ForegroundColor White
Write-Host "  2. git init" -ForegroundColor White
Write-Host "  3. git add ." -ForegroundColor White
Write-Host '  4. git commit -m "chore: initial project scaffold"' -ForegroundColor White
Write-Host "  5. Create repo on GitHub and push" -ForegroundColor White
Write-Host "`n Open http://localhost:8080 after 'docker compose up -d'" -ForegroundColor Cyan
