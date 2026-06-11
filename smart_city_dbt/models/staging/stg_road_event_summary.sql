/*
staging/stg_road_event_summary.sql

Purpose : Type-cast road_event_summary, add severity scores.
Source  : RAW.road_event_summary (from telemetry road_event column)
*/

SELECT
    window_start::TIMESTAMP_NTZ                         AS window_start,
    window_end::TIMESTAMP_NTZ                           AS window_end,
    road_event::VARCHAR                                 AS road_event,
    road_type::VARCHAR                                  AS road_type,
    route_name::VARCHAR                                 AS route_name,

    event_count::BIGINT                                 AS event_count,
    vehicles_involved::BIGINT                           AS vehicles_involved,
    ROUND(avg_speed_at_event_kmh::DOUBLE, 2)            AS avg_speed_at_event_kmh,

    partition_date::DATE                                AS partition_date,

    -- Severity score (matches dim_road_event_type)
    CASE road_event::VARCHAR
        WHEN 'ACCIDENT'            THEN 4
        WHEN 'CONGESTION_INCIDENT' THEN 3
        WHEN 'BREAKDOWN'           THEN 2
        WHEN 'ROADWORK'            THEN 1
        ELSE 0
    END                                                 AS severity_score,

    CASE road_event::VARCHAR
        WHEN 'ACCIDENT'            THEN 'critical'
        WHEN 'CONGESTION_INCIDENT' THEN 'high'
        WHEN 'BREAKDOWN'           THEN 'medium'
        WHEN 'ROADWORK'            THEN 'low'
        ELSE 'unknown'
    END                                                 AS severity_label

FROM {{ source('raw', 'road_event_summary') }}
WHERE window_start IS NOT NULL
  AND road_event IS NOT NULL
