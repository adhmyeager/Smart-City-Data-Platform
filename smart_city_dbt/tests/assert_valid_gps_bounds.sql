/*
tests/assert_valid_gps_bounds.sql

Custom singular test: all GPS coordinates must be within the
Cairo + New Administrative Capital bounding box.

Cairo bounding box (from silver_cleaner.py LIMITS):
  latitude:  29.5 → 30.5
  longitude: 30.5 → 32.0

Any coordinates outside this box indicate a data quality issue
in the Silver cleaner's clamping logic.
*/

SELECT
    vehicle_id,
    window_start,
    avg_latitude,
    avg_longitude
FROM {{ ref('mart_vehicle_performance') }}
WHERE avg_latitude  NOT BETWEEN 29.5 AND 30.5
   OR avg_longitude NOT BETWEEN 30.5 AND 32.0
