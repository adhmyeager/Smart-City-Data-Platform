/*
staging/stg_fuel_daily.sql

Purpose : Type-cast fuel_daily Gold data, add cost estimates.
Source  : RAW.fuel_daily
Note    : Egypt average petrol price ~11 EGP/litre (2026)
          CO2 factor: 2.31 kg per litre (Egypt 95-octane)
*/

SELECT
    window_start::TIMESTAMP_NTZ                             AS window_start,
    window_end::TIMESTAMP_NTZ                               AS window_end,
    vehicle_type::VARCHAR                                   AS vehicle_type,

    ROUND(total_fuel_consumed_l::DOUBLE, 3)                 AS total_fuel_consumed_l,
    ROUND(avg_fuel_rate_l100km::DOUBLE, 2)                  AS avg_fuel_rate_l100km,
    ROUND(avg_fuel_level_pct::DOUBLE, 2)                    AS avg_fuel_level_pct,
    unique_vehicles::BIGINT                                 AS unique_vehicles,
    total_events::BIGINT                                    AS total_events,
    ROUND(estimated_co2_kg::DOUBLE, 2)                      AS estimated_co2_kg,

    partition_date::DATE                                    AS partition_date,

    -- Business derived metrics
    -- Egypt petrol price estimate
    ROUND(total_fuel_consumed_l::DOUBLE * 11.0, 2)          AS estimated_cost_egp,

    -- CO2 per vehicle in this day
    ROUND(
        estimated_co2_kg::DOUBLE
        / NULLIF(unique_vehicles::DOUBLE, 0), 2
    )                                                       AS co2_per_vehicle_kg,

    -- Cost per vehicle
    ROUND(
        (total_fuel_consumed_l::DOUBLE * 11.0)
        / NULLIF(unique_vehicles::DOUBLE, 0), 2
    )                                                       AS cost_per_vehicle_egp,

    -- Efficiency rating based on industry benchmarks for Cairo conditions
    CASE
        WHEN avg_fuel_rate_l100km::DOUBLE < 8  THEN 'excellent'
        WHEN avg_fuel_rate_l100km::DOUBLE < 12 THEN 'good'
        WHEN avg_fuel_rate_l100km::DOUBLE < 16 THEN 'average'
        ELSE 'poor'
    END                                                     AS efficiency_rating

FROM {{ source('raw', 'fuel_daily') }}
WHERE window_start IS NOT NULL
