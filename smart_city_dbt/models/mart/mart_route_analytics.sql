/*
mart/mart_route_analytics.sql

Purpose : Route-level congestion and throughput analysis.
          Power BI Dashboard 2: Route Analytics & Congestion

Grain   : One row per route_name + road_type per hour

Joins:
  stg_route_hourly   (fact source — hourly route aggregations)
  dim_route          (route metadata: distance, locations)
  dim_date           (calendar: weekend, rush hour)
  stg_weather_hourly (weather at time of congestion)
  stg_traffic_30min  (TomTom/simulated traffic congestion ratios)

Key insight this table enables:
  "Was the Tahrir→New Capital route congested because of weather
   (dust reducing speed_factor) or because of traffic volume?"
  Answer: join weather speed_factor + traffic congestion_ratio
  If both are high → compounding effect
  If only traffic is high → pure demand problem
  If only weather is high → conditions problem
*/

WITH route_fact AS (
    SELECT * FROM {{ ref('stg_route_hourly') }}
),

route_dim AS (
    SELECT * FROM {{ ref('dim_route') }}
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
),

weather AS (
    SELECT * FROM {{ ref('stg_weather_hourly') }}
),

-- Aggregate traffic to route level by matching GPS bounding boxes
-- Each route has known GPS ranges from cairo_routes.py
traffic AS (
    SELECT
        DATE_TRUNC('hour', window_start)   AS traffic_hour,
        AVG(avg_congestion_ratio)          AS avg_congestion_ratio,
        AVG(avg_speed_deficit_kmh)         AS avg_speed_deficit_kmh,
        AVG(pct_of_free_flow)              AS avg_pct_of_free_flow,
        MAX(road_closure_count)            AS road_closure_count,
        -- Most common congestion band in this hour
        MODE(congestion_band)              AS dominant_congestion_band
    FROM {{ ref('stg_traffic_30min') }}
    -- Cairo bounding box covers all 4 routes
    WHERE gps_lat_bucket BETWEEN 29.9 AND 30.2
      AND gps_lon_bucket BETWEEN 30.9 AND 31.8
    GROUP BY 1
)

SELECT
    -- Surrogate key: unique row identifier for Power BI relationships
    MD5(
        COALESCE(f.route_name, '') || '|' ||
        COALESCE(f.road_type, '')  || '|' ||
        COALESCE(CAST(f.window_start AS VARCHAR), '')
    )                               AS route_analytics_key,

    -- ── Time ──────────────────────────────────────────────────────────────
    f.window_start,
    f.window_end,
    f.partition_date,
    d.day_name,
    d.is_weekend_cairo,
    d.is_workday_cairo,
    d.egypt_season,

    -- ── Route identity ────────────────────────────────────────────────────
    f.route_name,
    f.road_type,
    r.start_location,
    r.end_location,
    r.distance_km,
    r.via_description,
    r.primary_road_type,

    -- ── Speed & congestion ────────────────────────────────────────────────
    f.avg_speed_kmh,
    f.max_speed_kmh,
    f.avg_traffic_density,
    f.avg_fuel_rate_l100km,
    f.congestion_level,
    f.pct_slow_or_stopped,

    -- ── Speed band distribution (for stacked bar charts in Power BI) ──────
    f.count_stopped,
    f.count_slow,
    f.count_medium,
    f.count_fast,
    f.count_overspeed,
    f.total_events,

    -- Percentage breakdown for 100% stacked bar
    ROUND(f.count_stopped  / NULLIF(f.total_events, 0) * 100, 1) AS pct_stopped,
    ROUND(f.count_slow     / NULLIF(f.total_events, 0) * 100, 1) AS pct_slow,
    ROUND(f.count_medium   / NULLIF(f.total_events, 0) * 100, 1) AS pct_medium,
    ROUND(f.count_fast     / NULLIF(f.total_events, 0) * 100, 1) AS pct_fast,
    ROUND(f.count_overspeed/ NULLIF(f.total_events, 0) * 100, 1) AS pct_overspeed,

    -- ── Throughput ────────────────────────────────────────────────────────
    f.unique_vehicles,
    f.anomaly_count,
    f.road_event_count,

    -- ── Weather context ───────────────────────────────────────────────────
    w.condition                    AS weather_condition,
    w.weather_severity,
    w.avg_temp_c                   AS weather_temp_c,
    w.avg_speed_factor             AS weather_speed_factor,
    w.speed_reduction_pct          AS weather_speed_reduction_pct,
    w.avg_visibility_km,
    w.visibility_risk,

    -- ── Traffic sensor context ────────────────────────────────────────────
    t.avg_congestion_ratio,
    t.avg_speed_deficit_kmh,
    t.avg_pct_of_free_flow,
    t.dominant_congestion_band,
    t.road_closure_count,

    -- ── Congestion cause analysis ─────────────────────────────────────────
    -- Combines weather + traffic to attribute congestion root cause
    CASE
        WHEN t.avg_congestion_ratio  > 0.7
         AND w.avg_speed_factor      < 0.9
        THEN 'weather_and_traffic'
        WHEN t.avg_congestion_ratio  > 0.7
        THEN 'traffic_volume'
        WHEN w.avg_speed_factor      < 0.9
        THEN 'weather_conditions'
        WHEN f.anomaly_count         > 0
        THEN 'incidents'
        ELSE 'normal'
    END                            AS congestion_cause

FROM route_fact f
LEFT JOIN route_dim r
    ON f.route_name = r.route_name
LEFT JOIN date_dim d
    ON f.partition_date = d.date_actual
LEFT JOIN weather w
    ON DATE_TRUNC('hour', f.window_start) = DATE_TRUNC('hour', w.window_start)
LEFT JOIN traffic t
    ON DATE_TRUNC('hour', f.window_start) = t.traffic_hour