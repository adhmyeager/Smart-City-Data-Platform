/*
dimensions/dim_route.sql

Type    : Static (routes never change — hardcoded from cairo_routes.py)
Source  : Hardcoded — 4 real Cairo OSM routes
Purpose : Provides geographic context for each route_name.
          Power BI uses this for route-level filtering and map context.

Grain   : One row per route_name (4 rows total)

Data source: cairo_routes.py — real OpenStreetMap GPS coordinates
*/

WITH route_data AS (

    SELECT 'tahrir_to_new_capital'   AS route_name UNION ALL
    SELECT 'maadi_to_nasr_city'                    UNION ALL
    SELECT 'giza_to_heliopolis'                    UNION ALL
    SELECT 'sixth_october_to_maadi'

),

route_details AS (

    SELECT
        route_name,
        CASE route_name
            WHEN 'tahrir_to_new_capital'   THEN 'Tahrir Square'
            WHEN 'maadi_to_nasr_city'      THEN 'Maadi Metro Station'
            WHEN 'giza_to_heliopolis'      THEN 'Giza Square'
            WHEN 'sixth_october_to_maadi'  THEN '6th October City Center'
        END AS start_location,

        CASE route_name
            WHEN 'tahrir_to_new_capital'   THEN 'New Capital Downtown'
            WHEN 'maadi_to_nasr_city'      THEN 'Nasr City Center'
            WHEN 'giza_to_heliopolis'      THEN 'Heliopolis'
            WHEN 'sixth_october_to_maadi'  THEN 'Maadi'
        END AS end_location,

        CASE route_name
            WHEN 'tahrir_to_new_capital'   THEN 60.0
            WHEN 'maadi_to_nasr_city'      THEN 22.0
            WHEN 'giza_to_heliopolis'      THEN 30.0
            WHEN 'sixth_october_to_maadi'  THEN 38.0
        END AS distance_km,

        CASE route_name
            WHEN 'tahrir_to_new_capital'   THEN 14
            WHEN 'maadi_to_nasr_city'      THEN 9
            WHEN 'giza_to_heliopolis'      THEN 8
            WHEN 'sixth_october_to_maadi'  THEN 9
        END AS waypoint_count,

        -- Primary road type for this route
        CASE route_name
            WHEN 'tahrir_to_new_capital'   THEN 'highway'
            WHEN 'maadi_to_nasr_city'      THEN 'highway'
            WHEN 'giza_to_heliopolis'      THEN 'arterial'
            WHEN 'sixth_october_to_maadi'  THEN 'highway'
        END AS primary_road_type,

        -- Start GPS (from cairo_routes.py)
        CASE route_name
            WHEN 'tahrir_to_new_capital'   THEN 30.0444
            WHEN 'maadi_to_nasr_city'      THEN 29.9602
            WHEN 'giza_to_heliopolis'      THEN 29.9870
            WHEN 'sixth_october_to_maadi'  THEN 29.9343
        END AS start_latitude,

        CASE route_name
            WHEN 'tahrir_to_new_capital'   THEN 31.2357
            WHEN 'maadi_to_nasr_city'      THEN 31.2569
            WHEN 'giza_to_heliopolis'      THEN 31.2118
            WHEN 'sixth_october_to_maadi'  THEN 30.9274
        END AS start_longitude,

        -- End GPS (from cairo_routes.py)
        CASE route_name
            WHEN 'tahrir_to_new_capital'   THEN 30.0150
            WHEN 'maadi_to_nasr_city'      THEN 30.0626
            WHEN 'giza_to_heliopolis'      THEN 30.0868
            WHEN 'sixth_october_to_maadi'  THEN 29.9602
        END AS end_latitude,

        CASE route_name
            WHEN 'tahrir_to_new_capital'   THEN 31.7400
            WHEN 'maadi_to_nasr_city'      THEN 31.3417
            WHEN 'giza_to_heliopolis'      THEN 31.3275
            WHEN 'sixth_october_to_maadi'  THEN 31.2569
        END AS end_longitude,

        -- Via description
        CASE route_name
            WHEN 'tahrir_to_new_capital'
                THEN 'Ring Road East → Cairo-Suez Desert Road'
            WHEN 'maadi_to_nasr_city'
                THEN 'Ring Road → Autostrad'
            WHEN 'giza_to_heliopolis'
                THEN '6th October Bridge → Downtown → Ramses → Airport Road'
            WHEN 'sixth_october_to_maadi'
                THEN 'Desert Road → Giza → Ring Road'
        END AS via_description,

        'Cairo, Egypt' AS city

    FROM route_data

)

SELECT
    route_name,
    start_location,
    end_location,
    distance_km,
    waypoint_count,
    primary_road_type,
    start_latitude,
    start_longitude,
    end_latitude,
    end_longitude,
    via_description,
    city,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM route_details
