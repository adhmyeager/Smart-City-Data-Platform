/*
staging/stg_route_hourly.sql  (v2)

Changes v2:
  - Added gps_lat_bucket, gps_lon_bucket (written by historical generator
    and by gold_aggregator.py into route_hourly Parquet)
  - Added avg_congestion_ratio, avg_speed_deficit_kmh, avg_pct_of_free_flow,
    dominant_congestion_band, road_closure_count
    (mart_route_analytics uses these via COALESCE with traffic_30min)
  - All new columns use TRY_CAST so real streaming data (which may not
    have these columns) does not fail — returns NULL instead
*/

SELECT
    window_start::TIMESTAMP_NTZ                       AS window_start,
    window_end::TIMESTAMP_NTZ                         AS window_end,
    route_name::VARCHAR                               AS route_name,
    road_type::VARCHAR                                AS road_type,

    ROUND(avg_speed_kmh::DOUBLE, 2)                   AS avg_speed_kmh,
    ROUND(max_speed_kmh::DOUBLE, 2)                   AS max_speed_kmh,
    ROUND(avg_traffic_density::DOUBLE, 1)             AS avg_traffic_density,
    ROUND(avg_fuel_rate_l100km::DOUBLE, 2)            AS avg_fuel_rate_l100km,

    anomaly_count::BIGINT                             AS anomaly_count,
    road_event_count::BIGINT                          AS road_event_count,
    unique_vehicles::BIGINT                           AS unique_vehicles,
    total_events::BIGINT                              AS total_events,
    count_stopped::BIGINT                             AS count_stopped,
    count_slow::BIGINT                                AS count_slow,
    count_medium::BIGINT                              AS count_medium,
    count_fast::BIGINT                                AS count_fast,
    count_overspeed::BIGINT                           AS count_overspeed,

    partition_date::DATE                              AS partition_date,

    -- ── New columns written by historical generator and gold_aggregator v2 ─
    -- Use TRY_CAST so old real-streaming Parquet files return NULL safely
    TRY_CAST(value:gps_lat_bucket::DOUBLE AS DOUBLE)        AS gps_lat_bucket,
    TRY_CAST(value:gps_lon_bucket::DOUBLE AS DOUBLE)        AS gps_lon_bucket,
    TRY_CAST(value:avg_congestion_ratio::DOUBLE AS DOUBLE)  AS avg_congestion_ratio,
    TRY_CAST(value:avg_speed_deficit_kmh::DOUBLE AS DOUBLE) AS avg_speed_deficit_kmh,
    TRY_CAST(value:avg_pct_of_free_flow::DOUBLE AS DOUBLE)  AS avg_pct_of_free_flow,
    TRY_CAST(value:dominant_congestion_band::VARCHAR AS VARCHAR) AS dominant_congestion_band,
    TRY_CAST(value:road_closure_count::BIGINT AS BIGINT)    AS road_closure_count,

    -- ── Derived ───────────────────────────────────────────────────────────
    CASE
        WHEN road_type = 'highway'
             AND avg_speed_kmh::DOUBLE < 60  THEN 'heavy'
        WHEN road_type = 'highway'
             AND avg_speed_kmh::DOUBLE < 90  THEN 'moderate'
        WHEN road_type = 'arterial'
             AND avg_speed_kmh::DOUBLE < 30  THEN 'heavy'
        WHEN road_type = 'arterial'
             AND avg_speed_kmh::DOUBLE < 50  THEN 'moderate'
        WHEN road_type = 'urban'
             AND avg_speed_kmh::DOUBLE < 15  THEN 'heavy'
        WHEN road_type = 'urban'
             AND avg_speed_kmh::DOUBLE < 30  THEN 'moderate'
        ELSE 'light'
    END                                               AS congestion_level,

    ROUND(
        (count_stopped::DOUBLE + count_slow::DOUBLE)
        / NULLIF(total_events::DOUBLE, 0) * 100, 1
    )                                                 AS pct_slow_or_stopped

FROM {{ source('raw', 'route_hourly') }}
WHERE window_start IS NOT NULL
