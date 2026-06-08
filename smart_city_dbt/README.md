# Smart City dbt Project

## Overview

This dbt project transforms raw Gold-layer Parquet data (from S3 via Snowflake
External Tables) into a fully modeled dimensional warehouse ready for Power BI.

## Architecture

```
S3 Gold (Parquet)
    ↓
Snowflake RAW schema (External Tables — zero data movement)
    ↓
dbt STAGING (views — type casting, light derivations)
    ↓
dbt DIMENSIONS (tables — vehicle, route, date, weather, event_type)
    ↓
dbt MART (tables — 4 fact tables joined with dimensions)
    ↓
Power BI (batch analytics dashboards)
```

## Models

### Staging (7 views)
Read from RAW external tables. Cast types, derive categorical columns.
No business logic, no joins.

| Model | Source | Key additions |
|---|---|---|
| stg_vehicle_5min | RAW.vehicle_5min | speed_band, fuel_band, engine_health |
| stg_route_hourly | RAW.route_hourly | congestion_level, pct_slow_or_stopped |
| stg_fuel_daily | RAW.fuel_daily | efficiency_rating, cost_egp |
| stg_road_event_summary | RAW.road_event_summary | severity_score, severity_label |
| stg_weather_hourly | RAW.weather_hourly | heat_category, speed_reduction_pct |
| stg_traffic_30min | RAW.traffic_30min | pct_of_free_flow, congestion_score |
| stg_incident_summary | RAW.incident_summary | severity_label |

### Dimensions (5 tables)
Provide business context. Static or SCD Type 1.

| Model | Type | Rows | Key columns |
|---|---|---|---|
| dim_vehicle | SCD Type 1 | 5 | tank_size_l, fuel_base_l100km, mass_kg |
| dim_route | Static | 4 | GPS coords, distance_km, via_description |
| dim_date | Date spine | 214 | is_weekend_cairo, egypt_season |
| dim_weather_condition | Static | 10 | speed_factor, affects_visibility |
| dim_road_event_type | Static | 4 | severity_score, requires_emergency_response |

### Mart (4 tables — Power BI reads here)

| Model | Grain | Dashboard |
|---|---|---|
| mart_vehicle_performance | vehicle × 5min | Vehicle Tracking |
| mart_route_analytics | route × hour | Route Analytics |
| mart_fuel_environment | vehicle_type × day | Fuel & Environment |
| mart_incidents_safety | event_type × road_type × hour | Incidents & Safety |

## Running

```bash
# Activate dbt venv
cd D:\ITI campaign\ITI content\final_project\data_sources\test
.\dbt_venv\Scripts\Activate
cd smart_city_dbt

# Run all models
dbt run

# Run only mart models (staging already built)
dbt run --select mart

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Testing

3 custom singular tests + schema-level column tests:
- `assert_no_negative_fuel` — fuel consumption never < 0
- `assert_valid_gps_bounds` — GPS within Cairo bounding box
- `assert_speed_within_limits` — speed within 0-250 km/h

## Snowflake schemas produced

- `SMART_CITY_DB.STAGING` — staging views + dimension tables
- `SMART_CITY_DB.MART` — final fact tables (Power BI connects here)
