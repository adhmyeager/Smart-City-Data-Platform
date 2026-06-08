/*
dimensions/dim_weather_condition.sql

Type    : Static (finite set of Cairo weather conditions)
Source  : Hardcoded — matches weather_fetcher.py CONDITION_SPEED_FACTOR dict
Purpose : Provides business labels and speed impact for each weather condition.

Grain   : One row per weather condition (8 conditions)

Data source: weather_fetcher.py
  CONDITION_SPEED_FACTOR = {
      "Clear": 1.00, "Partly cloudy": 1.00, "Clouds": 0.98,
      "Haze": 0.95, "Dust": 0.85, "Sand": 0.80,
      "Rain": 0.80, "Thunderstorm": 0.70, "Fog": 0.65, "Unknown": 0.95
  }
*/

SELECT
    condition,
    severity,
    speed_factor,
    -- How much this condition reduces fleet speed in km/h
    -- (assuming 80 km/h average free-flow speed on Cairo arterials)
    ROUND((1.0 - speed_factor) * 80, 1)    AS speed_reduction_kmh,
    description,
    affects_visibility,
    CURRENT_TIMESTAMP()                     AS dbt_updated_at

FROM (
    VALUES
        ('Clear',        'low',      1.00, 'Clear sky, no weather impact',             FALSE),
        ('Partly cloudy','low',      1.00, 'Partly cloudy, negligible impact',         FALSE),
        ('Clouds',       'moderate', 0.98, 'Overcast, minor speed reduction',          FALSE),
        ('Haze',         'moderate', 0.95, 'Haze reduces visibility slightly',         TRUE),
        ('Dust',         'high',     0.85, 'Dust storm, significant speed reduction',  TRUE),
        ('Sand',         'high',     0.80, 'Sandstorm, dangerous driving conditions',  TRUE),
        ('Rain',         'high',     0.80, 'Rain, slippery roads, reduced visibility', TRUE),
        ('Thunderstorm', 'severe',   0.70, 'Thunderstorm, hazardous conditions',       TRUE),
        ('Fog',          'severe',   0.65, 'Fog, very low visibility, slow speeds',    TRUE),
        ('Unknown',      'moderate', 0.95, 'Unknown condition, cautious estimate',     FALSE)
) AS t (condition, severity, speed_factor, description, affects_visibility)
