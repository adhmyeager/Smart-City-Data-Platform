/*
staging/stg_vehicle_5min.sql

Purpose : Type-cast and light enrichment of vehicle_5min Gold data.
Source  : RAW.vehicle_5min (Snowflake External Table → S3 Gold Parquet)
Output  : STAGING view — no storage cost

Rules applied here:
  - Cast all VARIANT columns to proper SQL types
  - Round numeric values to meaningful precision
  - Derive speed_band, fuel_band, had_anomaly
  - No joins, no business aggregations — those belong in mart layer
*/

SELECT
    -- Time window
    window_start::TIMESTAMP_NTZ                       AS window_start,
    window_end::TIMESTAMP_NTZ                         AS window_end,

    -- Vehicle identifiers
    vehicle_id::VARCHAR                               AS vehicle_id,
    vehicle_type::VARCHAR                             AS vehicle_type,
    route_name::VARCHAR                               AS route_name,

    -- Speed metrics
    ROUND(avg_speed_kmh::DOUBLE, 2)                   AS avg_speed_kmh,
    ROUND(max_speed_kmh::DOUBLE, 2)                   AS max_speed_kmh,
    ROUND(min_speed_kmh::DOUBLE, 2)                   AS min_speed_kmh,

    -- Powertrain
    ROUND(avg_rpm::DOUBLE, 0)::INT                    AS avg_rpm,
    ROUND(avg_engine_temp_c::DOUBLE, 1)               AS avg_engine_temp_c,
    ROUND(max_engine_temp_c::DOUBLE, 1)               AS max_engine_temp_c,

    -- Fuel
    ROUND(avg_fuel_level_pct::DOUBLE, 2)              AS avg_fuel_level_pct,
    ROUND(avg_fuel_rate_l100km::DOUBLE, 2)            AS avg_fuel_rate_l100km,
    ROUND(total_fuel_consumed_l::DOUBLE, 5)           AS total_fuel_consumed_l,

    -- Traffic
    ROUND(avg_traffic_density::DOUBLE, 1)             AS avg_traffic_density,

    -- GPS
    ROUND(trip_distance_km::DOUBLE, 3)                AS trip_distance_km,
    ROUND(avg_latitude::DOUBLE, 6)                    AS avg_latitude,
    ROUND(avg_longitude::DOUBLE, 6)                   AS avg_longitude,

    -- Counts
    event_count::BIGINT                               AS event_count,
    anomaly_count::BIGINT                             AS anomaly_count,
    clamped_count::BIGINT                             AS clamped_count,

    -- Partition
    partition_date::DATE                              AS partition_date,

    -- ── Derived categorical columns ───────────────────────────────────────
    -- speed_band: matches silver_cleaner.py derive_bands() exactly
    CASE
        WHEN avg_speed_kmh::DOUBLE < 5   THEN 'stopped'
        WHEN avg_speed_kmh::DOUBLE < 40  THEN 'slow'
        WHEN avg_speed_kmh::DOUBLE < 90  THEN 'medium'
        WHEN avg_speed_kmh::DOUBLE < 120 THEN 'fast'
        ELSE 'overspeed'
    END                                               AS speed_band,

    -- fuel_band: matches silver_cleaner.py derive_bands() exactly
    CASE
        WHEN avg_fuel_level_pct::DOUBLE < 10 THEN 'critical'
        WHEN avg_fuel_level_pct::DOUBLE < 25 THEN 'low'
        WHEN avg_fuel_level_pct::DOUBLE < 75 THEN 'ok'
        ELSE 'full'
    END                                               AS fuel_band,

    -- engine health indicator
    CASE
        WHEN max_engine_temp_c::DOUBLE > 105 THEN 'overheating'
        WHEN max_engine_temp_c::DOUBLE > 98  THEN 'warm'
        WHEN max_engine_temp_c::DOUBLE > 85  THEN 'normal'
        ELSE 'cold'
    END                                               AS engine_health,

    -- anomaly flag
    CASE WHEN anomaly_count::BIGINT > 0
         THEN TRUE ELSE FALSE
    END                                               AS had_anomaly

FROM {{ source('raw', 'vehicle_5min') }}
WHERE window_start IS NOT NULL
  AND vehicle_id    IS NOT NULL
