/*
tests/assert_no_negative_fuel.sql

Custom singular test: fuel consumption should never be negative.
A negative value would indicate a data pipeline error.

dbt runs this as: SELECT * → if any rows returned, test FAILS.
*/

SELECT
    vehicle_id,
    window_start,
    total_fuel_consumed_l,
    avg_fuel_rate_l100km
FROM {{ ref('mart_vehicle_performance') }}
WHERE total_fuel_consumed_l < 0
   OR avg_fuel_rate_l100km  < 0
