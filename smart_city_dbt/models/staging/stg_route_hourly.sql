/*
staging/stg_route_hourly.sql

Purpose : Type-cast route_hourly Gold data, derive congestion_level.
Source  : RAW.route_hourly
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

    -- Congestion level based on speed vs road type speed limits
    -- highway limit=120, arterial limit=80, urban limit=50
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

    -- Percentage of vehicles stopped or slow (traffic quality indicator)
    ROUND(
        (count_stopped::DOUBLE + count_slow::DOUBLE)
        / NULLIF(total_events::DOUBLE, 0) * 100, 1
    )                                                 AS pct_slow_or_stopped

FROM {{ source('raw', 'route_hourly') }}
WHERE window_start IS NOT NULL
