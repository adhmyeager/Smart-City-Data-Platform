/*
dimensions/dim_road_event_type.sql

Type    : Static (finite set defined in vehicle_simulator.py)
Source  : Hardcoded — matches ROAD_EVENTS list in vehicle_simulator.py
Purpose : Provides severity scoring and response context for incident analysis.

Grain   : One row per event_type (4 event types + 1 for NONE baseline)

Data source: vehicle_simulator.py
  ROAD_EVENTS = ["NONE","NONE","NONE","NONE","NONE",
                 "ACCIDENT","ROADWORK","BREAKDOWN","CONGESTION_INCIDENT"]

Probability from simulator (approximate):
  NONE:                 ~85% of ticks
  ACCIDENT:             ~3.75% of events
  ROADWORK:             ~3.75% of events
  BREAKDOWN:            ~3.75% of events
  CONGESTION_INCIDENT:  ~3.75% of events
*/

SELECT
    event_type,
    severity_score,
    severity_label,
    requires_emergency_response,
    typical_duration_minutes,
    impact_radius_km,
    description,
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM (
    VALUES
        (
            'ACCIDENT', 4, 'critical', TRUE, 60, 2.0,
            'Vehicle collision — emergency services required, lane closure likely'
        ),
        (
            'CONGESTION_INCIDENT', 3, 'high', FALSE, 30, 1.0,
            'Severe traffic buildup — secondary to another event or bottleneck'
        ),
        (
            'BREAKDOWN', 2, 'medium', FALSE, 15, 0.5,
            'Vehicle breakdown — tow truck needed, partial lane blockage'
        ),
        (
            'ROADWORK', 1, 'low', FALSE, 480, 0.3,
            'Planned road maintenance — reduced lanes, expect delays'
        )
) AS t (
    event_type,
    severity_score,
    severity_label,
    requires_emergency_response,
    typical_duration_minutes,
    impact_radius_km,
    description
)
