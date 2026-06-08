/*
tests/assert_speed_within_limits.sql

Custom singular test: average speed must be within physical limits.
Silver cleaner clamps speed to 0-250 km/h.
If avg_speed_kmh > 250 appears in mart, Silver clamping failed.
*/

SELECT
    vehicle_id,
    window_start,
    avg_speed_kmh,
    max_speed_kmh
FROM {{ ref('mart_vehicle_performance') }}
WHERE avg_speed_kmh < 0
   OR avg_speed_kmh > 250
   OR max_speed_kmh > 260    -- small tolerance for 5-min window max
