/*
mart/mart_fuel_environment.sql

Purpose : Fleet fuel efficiency and environmental impact analysis.
          Power BI Dashboard 3: Fuel & Environment

Grain   : One row per vehicle_type per day

Joins:
  stg_fuel_daily     (fact source — daily fuel aggregations by vehicle type)
  dim_date           (calendar attributes)
  stg_weather_hourly (temperature context — A/C load drives fuel consumption)

Key business questions this table answers:
  1. Which vehicle type consumes the most fuel? (sedan vs suv vs microbus)
  2. How much does Cairo summer heat increase fuel consumption via A/C?
  3. What is the fleet's daily CO2 footprint?
  4. How much does the fleet cost per day in fuel (EGP)?
  5. Does efficiency improve on weekends (less traffic, less idling)?
*/

WITH fuel_fact AS (
    SELECT * FROM {{ ref('stg_fuel_daily') }}
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
),

-- Daily average weather (fuel runs daily, weather is hourly)
weather_daily AS (
    SELECT
        partition_date,
        AVG(avg_temp_c)        AS avg_daily_temp_c,
        AVG(ac_load_extra_l100km) AS avg_ac_load,
        MODE(heat_category)    AS dominant_heat_category,
        MODE(condition)        AS dominant_condition
    FROM {{ ref('stg_weather_hourly') }}
    GROUP BY partition_date
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

    -- ── Vehicle type ──────────────────────────────────────────────────────
    f.vehicle_type,
    f.unique_vehicles,

    -- ── Fuel consumption ──────────────────────────────────────────────────
    f.total_fuel_consumed_l,
    f.avg_fuel_rate_l100km,
    f.avg_fuel_level_pct,
    f.efficiency_rating,

    -- Per-vehicle metrics
    f.co2_per_vehicle_kg,
    f.cost_per_vehicle_egp,

    -- ── Environmental impact ──────────────────────────────────────────────
    f.estimated_co2_kg,

    -- EGP cost breakdown
    f.estimated_cost_egp,

    -- Annualized projections (if current rate continued)
    ROUND(f.estimated_co2_kg    * 365, 1) AS projected_annual_co2_kg,
    ROUND(f.estimated_cost_egp  * 365, 0) AS projected_annual_cost_egp,

    -- ── Weather impact on fuel ────────────────────────────────────────────
    wd.avg_daily_temp_c,
    wd.dominant_heat_category,
    wd.dominant_condition        AS dominant_weather,
    wd.avg_ac_load,

    -- Estimated A/C overhead fuel (how much extra fuel from heat)
    ROUND(
        f.total_fuel_consumed_l * (wd.avg_ac_load / NULLIF(f.avg_fuel_rate_l100km, 0)),
        3
    )                            AS estimated_ac_fuel_consumed_l,

    -- ── Fleet totals (all vehicle types combined, calculated per row) ──────
    -- These allow Power BI to show fleet total without extra aggregation
    SUM(f.total_fuel_consumed_l) OVER (
        PARTITION BY f.partition_date
    )                            AS fleet_total_fuel_l,

    SUM(f.estimated_co2_kg) OVER (
        PARTITION BY f.partition_date
    )                            AS fleet_total_co2_kg,

    SUM(f.estimated_cost_egp) OVER (
        PARTITION BY f.partition_date
    )                            AS fleet_total_cost_egp,

    -- This vehicle type's share of fleet fuel
    ROUND(
        f.total_fuel_consumed_l / NULLIF(
            SUM(f.total_fuel_consumed_l) OVER (PARTITION BY f.partition_date), 0
        ) * 100, 1
    )                            AS pct_of_fleet_fuel

FROM fuel_fact f
LEFT JOIN date_dim d
    ON f.partition_date = d.date_actual
LEFT JOIN weather_daily wd
    ON f.partition_date = wd.partition_date
