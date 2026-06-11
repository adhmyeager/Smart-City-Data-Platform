/*
mart/mart_route_analytics.sql  (v2 — fixed GPS columns)

Purpose : Route-level congestion and throughput analysis.
          Power BI Dashboard 2: Route Analytics & Congestion

Grain   : One row per route_name + road_type per hour

Changes v2:
  - Added route_mid_lat / route_mid_lon from dim_route for map visual
  - avg_congestion_ratio, avg_speed_deficit_kmh now come from
    route_hourly staging (historical generator writes these columns)
    and fall back to traffic_30min grid aggregate
  - Added surrogate key route_analytics_key
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

traffic_grid AS (
    SELECT
        DATE_TRUNC('hour', window_start)       AS traffic_hour,
        AVG(avg_congestion_ratio)              AS grid_avg_congestion_ratio,
        AVG(avg_speed_deficit_kmh)             AS grid_avg_speed_deficit_kmh,
        AVG(pct_of_free_flow)                  AS grid_avg_pct_of_free_flow,
        MAX(road_closure_count)                AS road_closure_count,
        MODE(congestion_band)                  AS grid_dominant_congestion_band,
        AVG(gps_lat_bucket)                    AS grid_avg_lat,
        AVG(gps_lon_bucket)                    AS grid_avg_lon
    FROM {{ ref('stg_traffic_30min') }}
    WHERE gps_lat_bucket BETWEEN 29.9 AND 30.2
      AND gps_lon_bucket BETWEEN 30.9 AND 31.8
    GROUP BY 1
)

SELECT
    f.window_start,
    f.window_end,
    f.partition_date,
    d.day_name,
    d.is_weekend_cairo,
    d.is_workday_cairo,
    d.egypt_season,

    f.route_name,
    f.road_type,
    r.start_location,
    r.end_location,
    r.distance_km,
    r.via_description,
    r.primary_road_type,

    -- Route GPS midpoint for map visual
    ROUND((r.start_latitude  + r.end_latitude)  / 2, 4) AS route_mid_lat,
    ROUND((r.start_longitude + r.end_longitude) / 2, 4) AS route_mid_lon,

    f.avg_speed_kmh,
    f.max_speed_kmh,
    f.avg_traffic_density,
    f.avg_fuel_rate_l100km,
    f.congestion_level,
    f.pct_slow_or_stopped,

    f.count_stopped,
    f.count_slow,
    f.count_medium,
    f.count_fast,
    f.count_overspeed,
    f.total_events,
    f.unique_vehicles,
    f.anomaly_count,
    f.road_event_count,

    ROUND(f.count_stopped   / NULLIF(f.total_events, 0) * 100, 1) AS pct_stopped,
    ROUND(f.count_slow      / NULLIF(f.total_events, 0) * 100, 1) AS pct_slow,
    ROUND(f.count_medium    / NULLIF(f.total_events, 0) * 100, 1) AS pct_medium,
    ROUND(f.count_fast      / NULLIF(f.total_events, 0) * 100, 1) AS pct_fast,
    ROUND(f.count_overspeed / NULLIF(f.total_events, 0) * 100, 1) AS pct_overspeed,

    w.condition                             AS weather_condition,
    w.weather_severity,
    w.avg_temp_c                            AS weather_temp_c,
    w.avg_speed_factor                      AS weather_speed_factor,
    w.speed_reduction_pct                   AS weather_speed_reduction_pct,
    w.avg_visibility_km,
    w.visibility_risk,

    -- Congestion from route_hourly (richer for historical data)
    -- Falls back to GPS grid if column missing
    COALESCE(
        TRY_CAST(f.avg_congestion_ratio AS DOUBLE),
        tg.grid_avg_congestion_ratio
    )                                       AS avg_congestion_ratio,

    COALESCE(
        TRY_CAST(f.avg_speed_deficit_kmh AS DOUBLE),
        tg.grid_avg_speed_deficit_kmh
    )                                       AS avg_speed_deficit_kmh,

    COALESCE(
        TRY_CAST(f.avg_pct_of_free_flow AS DOUBLE),
        tg.grid_avg_pct_of_free_flow
    )                                       AS avg_pct_of_free_flow,

    COALESCE(
        f.dominant_congestion_band,
        tg.grid_dominant_congestion_band
    )                                       AS dominant_congestion_band,

    COALESCE(f.road_closure_count, tg.road_closure_count, 0)
                                            AS road_closure_count,

    tg.grid_avg_lat,
    tg.grid_avg_lon,

    CASE
        WHEN COALESCE(
                 TRY_CAST(f.avg_congestion_ratio AS DOUBLE),
                 tg.grid_avg_congestion_ratio
             ) > 0.7
         AND w.avg_speed_factor < 0.9
        THEN 'weather_and_traffic'
        WHEN COALESCE(
                 TRY_CAST(f.avg_congestion_ratio AS DOUBLE),
                 tg.grid_avg_congestion_ratio
             ) > 0.7
        THEN 'traffic_volume'
        WHEN w.avg_speed_factor < 0.9
        THEN 'weather_conditions'
        WHEN f.anomaly_count > 0
        THEN 'incidents'
        ELSE 'normal'
    END                                     AS congestion_cause,

    MD5(
        COALESCE(f.route_name, '') || '|' ||
        COALESCE(f.road_type, '')  || '|' ||
        COALESCE(CAST(f.window_start AS VARCHAR), '')
    )                                       AS route_analytics_key

FROM route_fact f
LEFT JOIN route_dim r
    ON f.route_name = r.route_name
LEFT JOIN date_dim d
    ON f.partition_date = d.date_actual
LEFT JOIN weather w
    ON DATE_TRUNC('hour', f.window_start) = DATE_TRUNC('hour', w.window_start)
LEFT JOIN traffic_grid tg
    ON DATE_TRUNC('hour', f.window_start) = tg.traffic_hour
