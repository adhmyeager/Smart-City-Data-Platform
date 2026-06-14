# Smart City Real-Time Data Engineering Platform

A production-style real-time Smart City Data Engineering Platform designed to simulate intelligent transportation analytics for Cairo, Egypt using modern streaming, lakehouse, and cloud data engineering technologies.

The platform ingests live vehicle telemetry, weather, traffic, and road-event streams through Apache Kafka, processes them using Apache Spark Structured Streaming, stores them in an AWS S3 Medallion Lakehouse Architecture (Bronze / Silver / Gold), orchestrates workflows with Apache Airflow, loads analytical datasets into Snowflake, transforms warehouse models using dbt, and visualizes insights through Power BI and Grafana dashboards.

---

# Architecture Overview
<img width="6529" height="4451" alt="smartcity_diagram" src="https://github.com/user-attachments/assets/072ac907-7951-48e5-bd84-cf7212e26492" />


## Batch + Real-Time Hybrid Architecture (Lambda-Style)

```text
Vehicle Simulator
        ↓
Apache Kafka
        ↓
────────────────────────────────────────
        REAL-TIME PATH
────────────────────────────────────────

Python Alert Consumer
        ↓
PostgreSQL (Operational Serving Layer)
        ↓
Grafana Dashboards

────────────────────────────────────────
        BATCH ANALYTICS PATH
────────────────────────────────────────

Spark Bronze Writer
        ↓
AWS S3 Bronze Layer

Spark Silver Cleaner
        ↓
AWS S3 Silver Layer

Spark Gold Aggregator
        ↓
AWS S3 Gold Layer

Airflow Orchestration
        ↓
Snowflake Data Warehouse
        ↓
dbt Transformations
        ↓
Power BI Dashboards
```

---

# Project Objectives

- Simulate real-time smart transportation systems
- Process streaming telemetry data at scale
- Build a Medallion Lakehouse Architecture
- Implement distributed streaming ETL pipelines
- Design dimensional warehouse models
- Create operational monitoring dashboards
- Deliver business intelligence analytics
- Demonstrate modern enterprise data engineering architecture

---

# Technology Stack

| Layer | Technology |
|---|---|
| Programming | Python 3.11 |
| Streaming Platform | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Data Lake | AWS S3 |
| Storage Format | Apache Parquet |
| Workflow Orchestration | Apache Airflow |
| Data Warehouse | Snowflake |
| Transformations | dbt Core |
| Operational Monitoring | Grafana |
| Metrics Collection | Prometheus |
| BI Analytics | Power BI |
| Containerization | Docker |
| Database | PostgreSQL |

---

# Data Sources

The project simulates and ingests multiple intelligent transportation data streams.

| Topic | Partitions | Retention | Rate | Purpose |
|---|---|---|---|---|
| vehicle-telemetry | 3 | 24h | 5 msg/sec | Main telemetry from 5 vehicles |
| weather-data | 1 | 24h | 1 msg/5min | Cairo weather snapshots |
| traffic-events | 2 | 24h | 5 msg/min | TomTom congestion data |
| road-events | 1 | 48h | ~1% of ticks | ACCIDENT / ROADWORK / BREAKDOWN |
| alerts | 1 | 7d | On anomaly | Real-time threshold violations |

---

# Real-Time Alert Engine

The platform continuously monitors streaming telemetry and generates alerts when operational thresholds are violated.

## Alert Thresholds

| Severity | Condition |
|---|---|
| 🔴 CRITICAL | Engine temperature > 105°C |
| 🟠 HIGH | Speed > 120 km/h |
| 🟠 HIGH | RPM > 5000 |
| 🟠 HIGH | Hard braking < -4.0 m/s² |
| 🟠 HIGH | Idle + temperature > 98°C |
| 🟡 MEDIUM | Fuel level < 10% |

---

# Medallion Lakehouse Architecture

## Bronze Layer
Raw immutable streaming ingestion from Kafka stored in Parquet format.

### Characteristics
- Raw event preservation
- Append-only
- Minimal transformation
- 7-day retention

---

## Silver Layer
Validated, cleaned, standardized streaming data.

### Transformations
- Schema enforcement
- Deduplication
- Null handling
- Data quality validation
- Type standardization

---

## Gold Layer
Business-level aggregated analytical datasets.

### Generated KPIs
- Fleet performance metrics
- Fuel consumption analytics
- Route efficiency
- Safety incident analytics
- Weather impact analysis
- Vehicle operational KPIs

---

# Why Apache Parquet?

The project uses Apache Parquet as the primary analytical storage format because it provides:

- Columnar storage optimization
- Compression efficiency
- Faster Spark query performance
- Reduced storage cost
- Predicate pushdown optimization
- Industry-standard lakehouse compatibility

Streaming events are transmitted in JSON format through Kafka and stored analytically as Parquet inside AWS S3.

---

# Data Warehouse & dbt

Snowflake is used as the cloud analytical warehouse layer.

dbt is responsible for:
- Dimensional modeling
- Star schema design
- Data marts
- Reusable analytical SQL transformations
- BI semantic modeling

## Warehouse Models

### Dimensions
- DIM_DATE
- DIM_VEHICLE
- DIM_ROUTE
- DIM_WEATHER_CONDITION
- DIM_ROAD_EVENT_TYPE

### Fact Tables
- MART_VEHICLE_PERFORMANCE
- MART_ROUTE_ANALYTICS
- MART_FUEL_ENVIRONMENT
- MART_INCIDENTS_SAFETY

---

# Airflow Orchestration

Apache Airflow orchestrates:
- Spark Gold jobs
- Snowflake loading
- dbt execution workflows
- Pipeline dependency management
- Batch scheduling

---

# Dashboards

## Power BI
Business Intelligence dashboards for:
- Fleet analytics
- Fuel efficiency
- Route performance
- Safety analysis
- Environmental insights

## Grafana
Operational real-time monitoring dashboards for:
- Live alerts
- Spark cluster health
- Streaming metrics
- System observability

---

# Repository Structure

```text
Smart-City-Data-Platform/
│
├── airflow/
├── architecture/
├── dashboards/
│   ├── grafana/
│   └── powerbi/
├── dbt/
├── spark_jobs/
├── simulator/
├── docs/
├── screenshots/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# Key Engineering Concepts Demonstrated

- Distributed stream processing
- Real-time event-driven architecture
- Lambda-style hybrid architecture
- Medallion Lakehouse design
- Kafka partition strategy
- Spark Structured Streaming
- Cloud-native data engineering
- Dimensional warehouse modeling
- Infrastructure orchestration
- Operational observability
- Analytical BI modeling

---

# Screenshots

## Architecture Diagram
  <img width="6529" height="4451" alt="smartcity_diagram" src="https://github.com/user-attachments/assets/1dbaaa3f-0f75-4ac2-a875-d39d7d6f0f79" />
## Power BI Dashboard
<img width="1918" height="1037" alt="image" src="https://github.com/user-attachments/assets/da6e8747-a105-428d-861e-4631149491df" />
## Grafana Monitoring
<img width="1917" height="1025" alt="Screenshot 2026-06-11 231231" src="https://github.com/user-attachments/assets/80b68378-ca1b-4a01-a89e-30ad5e6dd8c8" />
## Airflow DAG
<img width="1920" height="1019" alt="Screenshot (309)" src="https://github.com/user-attachments/assets/1efa42b1-dc97-4d63-8cec-fdc619bcbc04" />

---

# License

MIT License

---

# Author

Smart City Data Engineering Team

Built as a production-style end-to-end real-time lakehouse and analytics engineering platform.
