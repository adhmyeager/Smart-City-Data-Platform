/*
staging/stg_incident_summary.sql

Purpose : Type-cast incident_summary from dedicated road-events topic.
Source  : RAW.incident_summary (from silver/road_events via gold_aggregator)
Note    : Richer than road_event_summary — has GPS coords + severity_score
*/

SELECT
    window_start::TIMESTAMP_NTZ                          AS window_start,
    window_end::TIMESTAMP_NTZ                            AS window_end,
    event_type::VARCHAR                                  AS event_type,
    road_type::VARCHAR                                   AS road_type,

    incident_count::BIGINT                               AS incident_count,
    vehicles_involved::BIGINT                            AS vehicles_involved,
    ROUND(avg_severity_score::DOUBLE, 2)                 AS avg_severity_score,
    max_severity_score::BIGINT                           AS max_severity_score,
    ROUND(avg_latitude::DOUBLE, 4)                       AS avg_latitude,
    ROUND(avg_longitude::DOUBLE, 4)                      AS avg_longitude,

    partition_date::DATE                                 AS partition_date,

    CASE event_type::VARCHAR
        WHEN 'ACCIDENT'            THEN 'critical'
        WHEN 'CONGESTION_INCIDENT' THEN 'high'
        WHEN 'BREAKDOWN'           THEN 'medium'
        WHEN 'ROADWORK'            THEN 'low'
        ELSE 'unknown'
    END                                                  AS severity_label

FROM {{ source('raw', 'incident_summary') }}
WHERE window_start IS NOT NULL
  AND event_type IS NOT NULL
