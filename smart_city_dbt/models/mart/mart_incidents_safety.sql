/*
mart/mart_incidents_safety.sql

Purpose : Road incident and safety KPI analysis.
          Power BI Dashboard 4: Incidents & Safety

Grain   : One row per event_type + road_type per hour

Joins:
  stg_incident_summary   (fact source — from dedicated road-events Kafka topic)
  stg_road_event_summary (corroborating counts from telemetry road_event column)
  dim_road_event_type    (severity metadata, response requirements)
  dim_date               (calendar attributes)
  stg_weather_hourly     (weather context — does bad weather cause more incidents?)

Key business questions:
  1. Which road type (highway/arterial/urban) has the most accidents?
  2. Does fog or dust increase accident frequency?
  3. What is the mean severity score of incidents per hour?
  4. How many vehicles are affected by each incident type?
  5. Where (GPS) do most incidents occur?
*/

WITH incident_fact AS (
    SELECT * FROM {{ ref('stg_incident_summary') }}
),

event_telemetry AS (
    -- Corroborating signal from telemetry road_event column
    SELECT
        DATE_TRUNC('hour', window_start)  AS event_hour,
        road_event,
        road_type,
        SUM(event_count)                  AS telemetry_event_count,
        AVG(avg_speed_at_event_kmh)       AS avg_speed_at_event,
        MAX(severity_score)               AS max_telemetry_severity
    FROM {{ ref('stg_road_event_summary') }}
    GROUP BY 1, 2, 3
),

event_dim AS (
    SELECT * FROM {{ ref('dim_road_event_type') }}
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
),

weather AS (
    SELECT * FROM {{ ref('stg_weather_hourly') }}
)

SELECT
    -- ── Time ──────────────────────────────────────────────────────────────
    i.window_start,
    i.window_end,
    i.partition_date,
    d.day_name,
    d.is_weekend_cairo,
    d.egypt_season,

    -- ── Incident type ─────────────────────────────────────────────────────
    i.event_type,
    i.road_type,
    e.severity_label,
    e.severity_score,
    e.requires_emergency_response,
    e.typical_duration_minutes,
    e.impact_radius_km,
    e.description                  AS event_description,

    -- ── Incident metrics ──────────────────────────────────────────────────
    i.incident_count,
    i.vehicles_involved,
    i.avg_severity_score,
    i.max_severity_score,

    -- GPS location of incidents (for Power BI map visual)
    i.avg_latitude,
    i.avg_longitude,

    -- ── Corroborating telemetry signal ────────────────────────────────────
    et.telemetry_event_count,
    et.avg_speed_at_event,
    et.max_telemetry_severity,

    -- Agreement between dedicated topic and telemetry signal
    CASE
        WHEN i.incident_count > 0
         AND et.telemetry_event_count > 0
        THEN 'confirmed_by_both'
        WHEN i.incident_count > 0
        THEN 'road_events_topic_only'
        ELSE 'telemetry_only'
    END                            AS signal_agreement,

    -- ── Weather at time of incident ───────────────────────────────────────
    w.condition                    AS weather_condition,
    w.weather_severity,
    w.avg_temp_c                   AS weather_temp_c,
    w.avg_visibility_km,
    w.visibility_risk,

    -- ── Risk analysis ─────────────────────────────────────────────────────
    -- Combined risk score: incident severity × visibility risk × weather severity
    ROUND(
        i.avg_severity_score *
        CASE w.visibility_risk
            WHEN 'very_high_risk' THEN 2.0
            WHEN 'high_risk'      THEN 1.5
            WHEN 'moderate_risk'  THEN 1.2
            ELSE 1.0
        END *
        CASE w.weather_severity
            WHEN 'severe'   THEN 1.5
            WHEN 'high'     THEN 1.3
            WHEN 'moderate' THEN 1.1
            ELSE 1.0
        END,
    2)                             AS composite_risk_score,

    -- Was this a high-risk incident given conditions?
    CASE
        WHEN i.avg_severity_score >= 3
         AND w.visibility_risk IN ('very_high_risk', 'high_risk')
        THEN TRUE
        ELSE FALSE
    END                            AS is_high_risk_incident

FROM incident_fact i
LEFT JOIN event_telemetry et
    ON DATE_TRUNC('hour', i.window_start) = et.event_hour
    AND i.event_type = et.road_event
    AND i.road_type  = et.road_type
LEFT JOIN event_dim e
    ON i.event_type = e.event_type
LEFT JOIN date_dim d
    ON i.partition_date = d.date_actual
LEFT JOIN weather w
    ON DATE_TRUNC('hour', i.window_start) = DATE_TRUNC('hour', w.window_start)
