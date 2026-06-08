/*
dimensions/dim_date.sql

Type    : Date spine (generated — covers all dates in simulation data)
Source  : Generated using Snowflake GENERATOR function
Purpose : Provides rich time context for all fact tables.
          Critical for Power BI time intelligence functions.

Cairo calendar notes:
  - Weekend = Friday + Saturday (NOT Saturday + Sunday)
  - Rush hours = 8:00-10:30 AM and 4:00-7:30 PM on weekdays
  - Ramadan: traffic patterns change significantly (not modeled here)

Grain   : One row per calendar date
Range   : 2026-06-01 to 2026-12-31 (covers project duration)
*/

WITH date_spine AS (
    SELECT
        DATEADD(
            DAY,
            SEQ4(),
            '2026-06-01'::DATE
        ) AS date_actual
    FROM TABLE(GENERATOR(ROWCOUNT => 214))  -- 214 days: Jun 2026 to Dec 2026
),

date_attributes AS (
    SELECT
        date_actual,
        DATE_PART('year',    date_actual)::INT  AS year,
        DATE_PART('quarter', date_actual)::INT  AS quarter,
        DATE_PART('month',   date_actual)::INT  AS month_number,
        MONTHNAME(date_actual)                   AS month_name,
        DATE_PART('week',    date_actual)::INT  AS week_of_year,
        DATE_PART('day',     date_actual)::INT  AS day_of_month,
        DAYOFWEEK(date_actual)::INT              AS day_of_week,     -- 0=Sun, 6=Sat
        DAYNAME(date_actual)                     AS day_name,

        -- Cairo weekend: Friday (day_of_week=6 in Snowflake) + Saturday (day_of_week=0)
        -- Snowflake: 0=Sunday, 1=Monday, ..., 5=Friday, 6=Saturday
        CASE
            WHEN DAYOFWEEK(date_actual) IN (5, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend_cairo,

        -- Cairo rush-hour days (Sun-Thu are workdays)
        CASE
            WHEN DAYOFWEEK(date_actual) NOT IN (5, 6) THEN TRUE
            ELSE FALSE
        END AS is_workday_cairo,

        -- Month quarter label
        'Q' || DATE_PART('quarter', date_actual)::VARCHAR AS quarter_label,

        -- Year-Month label for Power BI axis
        TO_VARCHAR(date_actual, 'YYYY-MM')        AS year_month,

        -- Is this a month start/end?
        CASE WHEN DATE_PART('day', date_actual) = 1
             THEN TRUE ELSE FALSE END              AS is_month_start,
        CASE WHEN date_actual = LAST_DAY(date_actual)
             THEN TRUE ELSE FALSE END              AS is_month_end,

        -- Season in Egypt
        CASE DATE_PART('month', date_actual)
            WHEN 12 THEN 'Winter' WHEN 1 THEN 'Winter' WHEN 2 THEN 'Winter'
            WHEN 3  THEN 'Spring' WHEN 4 THEN 'Spring' WHEN 5 THEN 'Spring'
            WHEN 6  THEN 'Summer' WHEN 7 THEN 'Summer' WHEN 8 THEN 'Summer'
            ELSE 'Autumn'
        END AS egypt_season,

        -- Expected weather category by month (Cairo averages)
        CASE DATE_PART('month', date_actual)
            WHEN 6  THEN 'hot'
            WHEN 7  THEN 'hot'
            WHEN 8  THEN 'hot'
            WHEN 9  THEN 'warm'
            WHEN 10 THEN 'warm'
            WHEN 11 THEN 'mild'
            WHEN 12 THEN 'mild'
            ELSE 'warm'
        END AS typical_heat_category

    FROM date_spine
)

SELECT
    date_actual,
    year,
    quarter,
    quarter_label,
    month_number,
    month_name,
    year_month,
    week_of_year,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend_cairo,
    is_workday_cairo,
    is_month_start,
    is_month_end,
    egypt_season,
    typical_heat_category,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM date_attributes
ORDER BY date_actual
