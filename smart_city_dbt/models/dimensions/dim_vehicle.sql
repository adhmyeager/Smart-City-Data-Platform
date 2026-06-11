/*
dimensions/dim_vehicle.sql

Type    : SCD Type 1 (overwrite — vehicle attributes don't change)
Source  : stg_vehicle_5min (DISTINCT vehicles seen in telemetry)
Purpose : Provides business context for each vehicle ID.
          Power BI uses this to filter/group dashboards by vehicle type.

Grain   : One row per vehicle_id (5 vehicles in this simulation)

Columns explained:
  vehicle_type     — sedan/suv/microbus, determines fuel profile
  route_name       — primary route this vehicle operates on
  tank_size_l      — from vehicle_simulator.py VEHICLE_PROFILES
  fuel_base_l100km — baseline fuel consumption (before traffic/weather)
  max_rpm          — redline RPM for this vehicle class
*/

WITH vehicles_seen AS (

    SELECT
        vehicle_id,

        MAX(vehicle_type) AS vehicle_type,
        MAX(route_name)   AS route_name

    FROM {{ ref('stg_vehicle_5min') }}

    GROUP BY vehicle_id
),
vehicle_specs AS (
    SELECT
        vehicle_type,
        CASE vehicle_type
            WHEN 'sedan'    THEN 55.0
            WHEN 'suv'      THEN 70.0
            WHEN 'microbus' THEN 70.0
            WHEN 'bus'      THEN 200.0
            ELSE 55.0
        END AS tank_size_l,
        CASE vehicle_type
            WHEN 'sedan'    THEN 8.0
            WHEN 'suv'      THEN 11.0
            WHEN 'microbus' THEN 12.0
            WHEN 'bus'      THEN 22.0
            ELSE 8.0
        END AS fuel_base_l100km,
        CASE vehicle_type
            WHEN 'sedan'    THEN 6500
            WHEN 'suv'      THEN 6000
            WHEN 'microbus' THEN 5000
            WHEN 'bus'      THEN 2800
            ELSE 6500
        END AS max_rpm,
        CASE vehicle_type
            WHEN 'sedan'    THEN 1300
            WHEN 'suv'      THEN 1900
            WHEN 'microbus' THEN 2500
            WHEN 'bus'      THEN 12000
            ELSE 1300
        END AS mass_kg
    FROM (
        SELECT DISTINCT vehicle_type
        FROM {{ ref('stg_vehicle_5min') }}
    )
)

SELECT
    v.vehicle_id,
    v.vehicle_type,
    v.route_name,
    s.tank_size_l,
    s.fuel_base_l100km,
    s.max_rpm,
    s.mass_kg,
    -- Human-readable label for Power BI reports
    v.vehicle_id || ' (' || v.vehicle_type || ')' AS vehicle_label,
    CURRENT_TIMESTAMP()                            AS dbt_updated_at

FROM vehicles_seen v
LEFT JOIN vehicle_specs s
    ON v.vehicle_type = s.vehicle_type
