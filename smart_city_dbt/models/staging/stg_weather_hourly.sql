/*
staging/stg_weather_hourly.sql

Purpose : Type-cast weather_hourly, add heat_category and speed impact.
Source  : RAW.weather_hourly (from silver/weather via gold_aggregator)
*/

SELECT
    window_start::TIMESTAMP_NTZ                             AS window_start,
    window_end::TIMESTAMP_NTZ                               AS window_end,
    condition::VARCHAR                                      AS condition,
    weather_severity::VARCHAR                               AS weather_severity,
    location::VARCHAR                                       AS location,

    ROUND(avg_temp_c::DOUBLE, 1)                            AS avg_temp_c,
    ROUND(max_temp_c::DOUBLE, 1)                            AS max_temp_c,
    ROUND(avg_feels_like_c::DOUBLE, 1)                      AS avg_feels_like_c,
    ROUND(avg_humidity_pct::DOUBLE, 1)                      AS avg_humidity_pct,
    ROUND(avg_wind_kmh::DOUBLE, 1)                          AS avg_wind_kmh,
    ROUND(avg_visibility_km::DOUBLE, 1)                     AS avg_visibility_km,
    ROUND(avg_pressure_hpa::DOUBLE, 0)::INT                 AS avg_pressure_hpa,
    ROUND(avg_speed_factor::DOUBLE, 3)                      AS avg_speed_factor,
    ROUND(min_speed_factor::DOUBLE, 3)                      AS min_speed_factor,
    observation_count::BIGINT                               AS observation_count,
    partition_date::DATE                                    AS partition_date,

    -- How much this weather reduces vehicle speed (percentage)
    ROUND((1.0 - avg_speed_factor::DOUBLE) * 100, 1)        AS speed_reduction_pct,

    -- Heat category for Cairo context
    CASE
        WHEN avg_temp_c::DOUBLE >= 40 THEN 'extreme_heat'
        WHEN avg_temp_c::DOUBLE >= 35 THEN 'very_hot'
        WHEN avg_temp_c::DOUBLE >= 28 THEN 'hot'
        WHEN avg_temp_c::DOUBLE >= 20 THEN 'warm'
        ELSE 'mild'
    END                                                     AS heat_category,

    -- A/C load factor: high temps force A/C on → more fuel
    CASE
        WHEN avg_temp_c::DOUBLE >= 30 THEN 1.5
        WHEN avg_temp_c::DOUBLE >= 22 THEN 1.0
        ELSE 0.0
    END                                                     AS ac_load_extra_l100km,

    -- Visibility risk for accident analysis
    CASE
        WHEN avg_visibility_km::DOUBLE < 2  THEN 'very_high_risk'
        WHEN avg_visibility_km::DOUBLE < 5  THEN 'high_risk'
        WHEN avg_visibility_km::DOUBLE < 8  THEN 'moderate_risk'
        ELSE 'normal_risk'
    END                                                     AS visibility_risk

FROM {{ source('raw', 'weather_hourly') }}
WHERE window_start IS NOT NULL
