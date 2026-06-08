/*
staging/stg_traffic_30min.sql

Purpose : Type-cast traffic_30min, derive pct_of_free_flow.
Source  : RAW.traffic_30min (from silver/traffic via gold_aggregator)
*/

SELECT
    window_start::TIMESTAMP_NTZ                              AS window_start,
    window_end::TIMESTAMP_NTZ                                AS window_end,
    ROUND(gps_lat_bucket::DOUBLE, 2)                         AS gps_lat_bucket,
    ROUND(gps_lon_bucket::DOUBLE, 2)                         AS gps_lon_bucket,
    congestion_band::VARCHAR                                 AS congestion_band,

    ROUND(avg_congestion_ratio::DOUBLE, 3)                   AS avg_congestion_ratio,
    ROUND(max_congestion_ratio::DOUBLE, 3)                   AS max_congestion_ratio,
    ROUND(avg_current_speed_kmh::DOUBLE, 1)                  AS avg_current_speed_kmh,
    ROUND(avg_free_flow_speed_kmh::DOUBLE, 1)                AS avg_free_flow_speed_kmh,
    ROUND(avg_speed_deficit_kmh::DOUBLE, 1)                  AS avg_speed_deficit_kmh,
    ROUND(avg_traffic_density::DOUBLE, 1)                    AS avg_traffic_density,
    road_closure_count::BIGINT                               AS road_closure_count,
    observation_count::BIGINT                                AS observation_count,
    partition_date::DATE                                     AS partition_date,

    -- Percentage of free-flow speed achieved (100% = no congestion)
    ROUND(
        avg_current_speed_kmh::DOUBLE
        / NULLIF(avg_free_flow_speed_kmh::DOUBLE, 0) * 100, 1
    )                                                        AS pct_of_free_flow,

    -- Congestion severity score 0-10 for heatmap
    ROUND(avg_congestion_ratio::DOUBLE * 10, 1)              AS congestion_score

FROM {{ source('raw', 'traffic_30min') }}
WHERE window_start IS NOT NULL
