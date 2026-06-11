/*
mart/mart_vehicle_performance.sql

Purpose : Central fact table for vehicle-level KPIs.
          Power BI Dashboard 1: Vehicle Performance & Tracking

Grain   : One row per vehicle_id per 5-minute window

Joins:
  stg_vehicle_5min   (fact source — Gold telemetry aggregations)
  dim_vehicle        (vehicle attributes: type, tank size, fuel profile)
  dim_route          (route attributes: distance, start/end location)
  dim_date           (calendar: is_weekend, is_rush_hour)
  stg_weather_hourly (weather context at time of window)

Why join weather here:
  Speed, fuel rate, and engine temp are all affected by weather.
  A vehicle showing low speed COULD be traffic or COULD be fog.
  The weather join lets analysts distinguish between these causes.

Materialized as TABLE so Power BI queries are fast.
Refreshed every time Airflow triggers dbt run (hourly).
*/

WITH vehicle_fact AS (
    SELECT * FROM {{ ref('stg_vehicle_5min') }}
),

vehicle_dim AS (
    SELECT * FROM {{ ref('dim_vehicle') }}
),

route_dim AS (
    SELECT * FROM {{ ref('dim_route') }}
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
),

weather AS (
    SELECT * FROM {{ ref('stg_weather_hourly') }}
)

SELECT
    -- ── Time ──────────────────────────────────────────────────────────────
    f.window_start,
    f.window_end,
    f.partition_date,
    d.day_name,
    d.is_weekend_cairo,
    d.is_workday_cairo,
    d.egypt_season,

    -- ── Vehicle identity ──────────────────────────────────────────────────
    f.vehicle_id,
    f.vehicle_type,
    v.vehicle_label,
    v.tank_size_l,
    v.fuel_base_l100km              AS manufacturer_fuel_rate_l100km,
    v.mass_kg,

    -- ── Route identity ────────────────────────────────────────────────────
    f.route_name,
    r.start_location,
    r.end_location,
    r.distance_km                   AS route_total_km,
    r.via_description,

    -- ── Speed metrics ─────────────────────────────────────────────────────
    f.avg_speed_kmh,
    f.max_speed_kmh,
    f.min_speed_kmh,
    f.speed_band,

    -- ── Powertrain ────────────────────────────────────────────────────────
    f.avg_rpm,
    f.avg_engine_temp_c,
    f.max_engine_temp_c,
    f.engine_health,

    -- ── Fuel ──────────────────────────────────────────────────────────────
    f.avg_fuel_level_pct,
    f.avg_fuel_rate_l100km,
    f.total_fuel_consumed_l,
    f.fuel_band,

    -- Excess fuel rate vs manufacturer baseline
    ROUND(
        f.avg_fuel_rate_l100km - v.fuel_base_l100km, 2
    )                               AS excess_fuel_rate_l100km,

    -- Fuel cost for this 5-min window (Egypt 11 EGP/litre)
    ROUND(f.total_fuel_consumed_l * 11.0, 3) AS fuel_cost_egp,

    -- ── Traffic ───────────────────────────────────────────────────────────
    f.avg_traffic_density,

    -- ── GPS ───────────────────────────────────────────────────────────────
    f.trip_distance_km,
    f.avg_latitude,
    f.avg_longitude,

    -- ── Quality flags ─────────────────────────────────────────────────────
    f.event_count,
    f.anomaly_count,
    f.had_anomaly,
    f.clamped_count,

    -- ── Weather context (joined on matching hour) ─────────────────────────
    w.condition                     AS weather_condition,
    w.avg_temp_c                    AS weather_temp_c,
    w.weather_severity,
    w.heat_category,
    w.avg_speed_factor              AS weather_speed_factor,
    w.speed_reduction_pct           AS weather_speed_reduction_pct,
    w.ac_load_extra_l100km,
    w.visibility_risk,

    -- ── Derived cross-dimension metrics ───────────────────────────────────
    -- Was vehicle significantly below expected speed for road type?
    CASE
        WHEN f.avg_speed_kmh < (f.avg_speed_kmh / NULLIF(w.avg_speed_factor, 0)) * 0.7
        THEN TRUE ELSE FALSE
    END                             AS was_significantly_slowed,

    -- CO2 estimate for this window (2.31 kg per litre)
    ROUND(f.total_fuel_consumed_l * 2.31, 5) AS co2_kg

FROM vehicle_fact f
LEFT JOIN vehicle_dim v
    ON f.vehicle_id = v.vehicle_id
LEFT JOIN route_dim r
    ON f.route_name = r.route_name
LEFT JOIN date_dim d
    ON f.partition_date = d.date_actual
LEFT JOIN weather w
    ON DATE_TRUNC('hour', f.window_start) = DATE_TRUNC('hour', w.window_start)
