-- ============================================================
--  SMART CITY GALAXY SCHEMA — S3 → SNOWFLAKE FULL LOAD SCRIPT  v2
--  Covers: Storage Integration, Stage, File Format,
--          6 Dimension Tables, 5 Fact Tables, COPY INTO commands
-- ============================================================

-- ── 0. ONE-TIME SETUP (run as ACCOUNTADMIN) ─────────────────

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS SMART_CITY_DW;
CREATE SCHEMA  IF NOT EXISTS SMART_CITY_DW.RAW;   -- dimension & fact tables live here
CREATE SCHEMA  IF NOT EXISTS SMART_CITY_DW.STAGING; -- external stage lives here

-- Warehouse (skip if you already have one)
CREATE WAREHOUSE IF NOT EXISTS SMART_CITY_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND   = 120
  AUTO_RESUME    = TRUE;

USE WAREHOUSE SMART_CITY_WH;
USE DATABASE   SMART_CITY_DW;
USE SCHEMA     SMART_CITY_DW.STAGING;


-- ── 1. S3 STORAGE INTEGRATION ────────────────────────────────
-- Replace <YOUR_AWS_ACCOUNT_ID> and bucket/prefix values.

CREATE STORAGE INTEGRATION IF NOT EXISTS s3_smart_city_integration
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = 'S3'
  ENABLED                   = TRUE
  STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/<YOUR_IAM_ROLE>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://smart-city-datalake/gold/');

-- After running, execute the command below and add the output values
-- (STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID) to your
-- AWS IAM role Trust Policy so Snowflake can assume it.
DESC INTEGRATION s3_smart_city_integration;


-- ── 2. FILE FORMAT ────────────────────────────────────────────
-- Assumes Parquet files from your Gold layer.
-- Swap TYPE = CSV if your raw files are CSV (and add field_delimiter etc.)

CREATE OR REPLACE FILE FORMAT SMART_CITY_DW.STAGING.parquet_format
  TYPE             = PARQUET
  SNAPPY_COMPRESSION = TRUE;

-- If CSV, use this instead:
-- CREATE OR REPLACE FILE FORMAT SMART_CITY_DW.STAGING.csv_format
--   TYPE             = CSV
--   FIELD_DELIMITER  = ','
--   RECORD_DELIMITER = '\n'
--   SKIP_HEADER      = 1
--   NULL_IF          = ('NULL','null','')
--   EMPTY_FIELD_AS_NULL = TRUE
--   COMPRESSION      = AUTO;


-- ── 3. EXTERNAL STAGES ────────────────────────────────────────
-- One stage per logical S3 prefix.  Adjust paths to match your bucket layout.

CREATE OR REPLACE STAGE SMART_CITY_DW.STAGING.stage_dim_vehicle
  URL               = 's3://smart-city-datalake/gold/dim_vehicle/'
  STORAGE_INTEGRATION = s3_smart_city_integration
  FILE_FORMAT       = SMART_CITY_DW.STAGING.parquet_format;

CREATE OR REPLACE STAGE SMART_CITY_DW.STAGING.stage_dim_route
  URL               = 's3://smart-city-datalake/gold/dim_route/'
  STORAGE_INTEGRATION = s3_smart_city_integration
  FILE_FORMAT       = SMART_CITY_DW.STAGING.parquet_format;

CREATE OR REPLACE STAGE SMART_CITY_DW.STAGING.stage_dim_location
  URL               = 's3://smart-city-datalake/gold/dim_location/'
  STORAGE_INTEGRATION = s3_smart_city_integration
  FILE_FORMAT       = SMART_CITY_DW.STAGING.parquet_format;

CREATE OR REPLACE STAGE SMART_CITY_DW.STAGING.stage_dim_weather
  URL               = 's3://smart-city-datalake/gold/dim_weather/'
  STORAGE_INTEGRATION = s3_smart_city_integration
  FILE_FORMAT       = SMART_CITY_DW.STAGING.parquet_format;

CREATE OR REPLACE STAGE SMART_CITY_DW.STAGING.stage_dim_road_event_type
  URL               = 's3://smart-city-datalake/gold/dim_road_event_type/'
  STORAGE_INTEGRATION = s3_smart_city_integration
  FILE_FORMAT       = SMART_CITY_DW.STAGING.parquet_format;

CREATE OR REPLACE STAGE SMART_CITY_DW.STAGING.stage_dim_alert
  URL               = 's3://smart-city-datalake/gold/dim_alert/'
  STORAGE_INTEGRATION = s3_smart_city_integration
  FILE_FORMAT       = SMART_CITY_DW.STAGING.parquet_format;

CREATE OR REPLACE STAGE SMART_CITY_DW.STAGING.stage_fact_vehicle_telemetry
  URL               = 's3://smart-city-datalake/gold/fact_vehicle_telemetry/'
  STORAGE_INTEGRATION = s3_smart_city_integration
  FILE_FORMAT       = SMART_CITY_DW.STAGING.parquet_format;

CREATE OR REPLACE STAGE SMART_CITY_DW.STAGING.stage_fact_route_traffic
  URL               = 's3://smart-city-datalake/gold/fact_route_traffic/'
  STORAGE_INTEGRATION = s3_smart_city_integration
  FILE_FORMAT       = SMART_CITY_DW.STAGING.parquet_format;

CREATE OR REPLACE STAGE SMART_CITY_DW.STAGING.stage_fact_road_events
  URL               = 's3://smart-city-datalake/gold/fact_road_events/'
  STORAGE_INTEGRATION = s3_smart_city_integration
  FILE_FORMAT       = SMART_CITY_DW.STAGING.parquet_format;

CREATE OR REPLACE STAGE SMART_CITY_DW.STAGING.stage_fact_fuel_emissions
  URL               = 's3://smart-city-datalake/gold/fact_fuel_emissions/'
  STORAGE_INTEGRATION = s3_smart_city_integration
  FILE_FORMAT       = SMART_CITY_DW.STAGING.parquet_format;

CREATE OR REPLACE STAGE SMART_CITY_DW.STAGING.stage_fact_weather_impact
  URL               = 's3://smart-city-datalake/gold/fact_weather_impact/'
  STORAGE_INTEGRATION = s3_smart_city_integration
  FILE_FORMAT       = SMART_CITY_DW.STAGING.parquet_format;

-- Quick check: list files in a stage
-- LIST @SMART_CITY_DW.STAGING.stage_dim_date;


-- ── 4. TARGET TABLE DDL ───────────────────────────────────────

USE SCHEMA SMART_CITY_DW.RAW;

-- ── DIMENSION TABLES ─────────────────────────────────────────

CREATE OR REPLACE TABLE dim_date (
  date_sk         NUMBER        NOT NULL PRIMARY KEY,
  full_date       DATE          NOT NULL,
  year            NUMBER(4)     NOT NULL,
  month           NUMBER(2)     NOT NULL,
  day_of_week     VARCHAR(10)   NOT NULL,
  hour_of_day     NUMBER(2)     NOT NULL,
  is_weekend      BOOLEAN       NOT NULL,
  is_peak_hour    BOOLEAN       NOT NULL
);

CREATE OR REPLACE TABLE dim_vehicle (
  vehicle_sk      NUMBER        NOT NULL PRIMARY KEY,
  vehicle_bk      VARCHAR(50)   NOT NULL,   -- natural key (vehicle_id)
  vehicle_type    VARCHAR(50),
  route_name      VARCHAR(100),
  trip_id         VARCHAR(50),
  engine_on       BOOLEAN,
  gear            VARCHAR(10),
  odometer_km     FLOAT
);

CREATE OR REPLACE TABLE dim_route (
  route_sk        NUMBER        NOT NULL PRIMARY KEY,
  route_name      VARCHAR(100)  NOT NULL,   -- business key
  road_type       VARCHAR(50),
  start_area      VARCHAR(100),
  end_area        VARCHAR(100),
  length_km       FLOAT,
  speed_limit_kmh NUMBER(3)
);

CREATE OR REPLACE TABLE dim_location (
  location_sk     NUMBER        NOT NULL PRIMARY KEY,
  lat_bucket      FLOAT,
  lon_bucket      FLOAT,
  latitude        FLOAT,
  longitude       FLOAT,
  district_name   VARCHAR(100),
  road_type       VARCHAR(50),
  altitude_m      FLOAT
);

CREATE OR REPLACE TABLE dim_weather (
  weather_sk          NUMBER        NOT NULL PRIMARY KEY,
  condition           VARCHAR(50),
  weather_severity    VARCHAR(20),
  speed_factor        FLOAT,
  temp_c              FLOAT,
  humidity_pct        FLOAT,
  wind_kmh            FLOAT,
  visibility_km       FLOAT,
  location            VARCHAR(100)
);

CREATE OR REPLACE TABLE dim_road_event_type (
  road_event_sk       NUMBER        NOT NULL PRIMARY KEY,
  event_type          VARCHAR(50),
  severity_score      FLOAT,
  severity_label      VARCHAR(20),
  affects_traffic     BOOLEAN,
  response_required   BOOLEAN,
  description         VARCHAR(500)
);

CREATE OR REPLACE TABLE dim_alert (
  alert_sk            NUMBER        NOT NULL PRIMARY KEY,
  alert_type          VARCHAR(50),
  severity            VARCHAR(20),
  rule_name           VARCHAR(100),
  threshold_value     FLOAT,
  triggered_sensor    VARCHAR(100),
  description         VARCHAR(500)
);


-- ── FACT TABLES ───────────────────────────────────────────────

-- Grain: vehicle × 5-minute window
CREATE OR REPLACE TABLE fact_vehicle_telemetry (
  fact_vehicle_sk     NUMBER        NOT NULL PRIMARY KEY,
  -- FK references
  date_sk             NUMBER        NOT NULL REFERENCES dim_date(date_sk),
  vehicle_sk          NUMBER        NOT NULL REFERENCES dim_vehicle(vehicle_sk),
  route_sk            NUMBER        NOT NULL REFERENCES dim_route(route_sk),
  location_sk         NUMBER        NOT NULL REFERENCES dim_location(location_sk),
  weather_sk          NUMBER        REFERENCES dim_weather(weather_sk),
  alert_sk            NUMBER        REFERENCES dim_alert(alert_sk),
  -- measures
  speed_kmh           FLOAT,
  acceleration_ms2    FLOAT,
  rpm                 FLOAT,
  engine_temp_c       FLOAT,
  fuel_level_pct      FLOAT,
  fuel_consumed_l     FLOAT,
  fuel_rate_l100km    FLOAT,
  traffic_density     FLOAT,
  is_anomaly          BOOLEAN,
  speed_band          VARCHAR(20),
  fuel_band           VARCHAR(20)
);

-- Grain: route × 1-hour window
CREATE OR REPLACE TABLE fact_route_traffic (
  route_traffic_sk    NUMBER        NOT NULL PRIMARY KEY,
  -- FK references
  date_sk             NUMBER        NOT NULL REFERENCES dim_date(date_sk),
  route_sk            NUMBER        NOT NULL REFERENCES dim_route(route_sk),
  location_sk         NUMBER        NOT NULL REFERENCES dim_location(location_sk),
  weather_sk          NUMBER        REFERENCES dim_weather(weather_sk),
  -- measures
  avg_speed_kmh       FLOAT,
  max_speed_kmh       FLOAT,
  avg_traffic_density FLOAT,
  unique_vehicles     NUMBER,
  anomaly_count       NUMBER,
  road_event_count    NUMBER,
  count_stopped       NUMBER,
  count_slow          NUMBER,
  count_medium        NUMBER,
  count_fast          NUMBER,
  count_overspeed     NUMBER
);

-- Grain: incident × 1-hour window
CREATE OR REPLACE TABLE fact_road_events (
  fact_road_events_sk NUMBER        NOT NULL PRIMARY KEY,
  -- FK references
  date_sk             NUMBER        NOT NULL REFERENCES dim_date(date_sk),
  vehicle_sk          NUMBER        NOT NULL REFERENCES dim_vehicle(vehicle_sk),
  location_sk         NUMBER        NOT NULL REFERENCES dim_location(location_sk),
  road_event_sk       NUMBER        NOT NULL REFERENCES dim_road_event_type(road_event_sk),
  -- degenerate dimension
  event_id            VARCHAR(50),
  -- measures
  severity_score      FLOAT,
  incident_count      NUMBER,
  vehicles_involved   NUMBER,
  avg_severity_score  FLOAT,
  avg_latitude        FLOAT,
  avg_longitude       FLOAT
);

-- Grain: vehicle_type × 1-day window  (NEW)
CREATE OR REPLACE TABLE fact_fuel_emissions (
  fact_fuel_sk            NUMBER        NOT NULL PRIMARY KEY,
  -- FK references
  date_sk                 NUMBER        NOT NULL REFERENCES dim_date(date_sk),
  vehicle_sk              NUMBER        NOT NULL REFERENCES dim_vehicle(vehicle_sk),
  -- measures
  total_fuel_consumed_l   FLOAT,
  avg_fuel_rate_l100km    FLOAT,
  avg_fuel_level_pct      FLOAT,
  estimated_co2_kg        FLOAT,
  unique_vehicles         NUMBER,
  total_events            NUMBER
);

-- Grain: condition × 1-hour window  (NEW)
CREATE OR REPLACE TABLE fact_weather_impact (
  fact_weather_sk         NUMBER        NOT NULL PRIMARY KEY,
  -- FK references
  date_sk                 NUMBER        NOT NULL REFERENCES dim_date(date_sk),
  weather_sk              NUMBER        NOT NULL REFERENCES dim_weather(weather_sk),
  -- measures
  avg_temp_c              FLOAT,
  max_temp_c              FLOAT,
  avg_humidity_pct        FLOAT,
  avg_wind_kmh            FLOAT,
  avg_visibility_km       FLOAT,
  avg_speed_factor        FLOAT,
  min_speed_factor        FLOAT,
  observation_count       NUMBER
);


-- ── 5. COPY INTO — LOAD DIMENSIONS FIRST ─────────────────────
-- Always load dimensions before facts (FK integrity).

COPY INTO SMART_CITY_DW.RAW.dim_date
FROM (
  SELECT
    $1:date_sk::NUMBER,
    $1:full_date::DATE,
    $1:year::NUMBER,
    $1:month::NUMBER,
    $1:day_of_week::VARCHAR,
    $1:hour_of_day::NUMBER,
    $1:is_weekend::BOOLEAN,
    $1:is_peak_hour::BOOLEAN
  FROM @SMART_CITY_DW.STAGING.stage_dim_date
)
FILE_FORMAT = (FORMAT_NAME = 'SMART_CITY_DW.STAGING.parquet_format')
ON_ERROR    = 'CONTINUE';

COPY INTO SMART_CITY_DW.RAW.dim_vehicle
FROM (
  SELECT
    $1:vehicle_sk::NUMBER,
    $1:vehicle_bk::VARCHAR,
    $1:vehicle_type::VARCHAR,
    $1:route_name::VARCHAR,
    $1:trip_id::VARCHAR,
    $1:engine_on::BOOLEAN,
    $1:gear::VARCHAR,
    $1:odometer_km::FLOAT
  FROM @SMART_CITY_DW.STAGING.stage_dim_vehicle
)
FILE_FORMAT = (FORMAT_NAME = 'SMART_CITY_DW.STAGING.parquet_format')
ON_ERROR    = 'CONTINUE';

COPY INTO SMART_CITY_DW.RAW.dim_route
FROM (
  SELECT
    $1:route_sk::NUMBER,
    $1:route_name::VARCHAR,
    $1:road_type::VARCHAR,
    $1:start_area::VARCHAR,
    $1:end_area::VARCHAR,
    $1:length_km::FLOAT,
    $1:speed_limit_kmh::NUMBER
  FROM @SMART_CITY_DW.STAGING.stage_dim_route
)
FILE_FORMAT = (FORMAT_NAME = 'SMART_CITY_DW.STAGING.parquet_format')
ON_ERROR    = 'CONTINUE';

COPY INTO SMART_CITY_DW.RAW.dim_location
FROM (
  SELECT
    $1:location_sk::NUMBER,
    $1:lat_bucket::FLOAT,
    $1:lon_bucket::FLOAT,
    $1:latitude::FLOAT,
    $1:longitude::FLOAT,
    $1:district_name::VARCHAR,
    $1:road_type::VARCHAR,
    $1:altitude_m::FLOAT
  FROM @SMART_CITY_DW.STAGING.stage_dim_location
)
FILE_FORMAT = (FORMAT_NAME = 'SMART_CITY_DW.STAGING.parquet_format')
ON_ERROR    = 'CONTINUE';

COPY INTO SMART_CITY_DW.RAW.dim_weather
FROM (
  SELECT
    $1:weather_sk::NUMBER,
    $1:condition::VARCHAR,
    $1:weather_severity::VARCHAR,
    $1:speed_factor::FLOAT,
    $1:temp_c::FLOAT,
    $1:humidity_pct::FLOAT,
    $1:wind_kmh::FLOAT,
    $1:visibility_km::FLOAT,
    $1:location::VARCHAR
  FROM @SMART_CITY_DW.STAGING.stage_dim_weather
)
FILE_FORMAT = (FORMAT_NAME = 'SMART_CITY_DW.STAGING.parquet_format')
ON_ERROR    = 'CONTINUE';

COPY INTO SMART_CITY_DW.RAW.dim_road_event_type
FROM (
  SELECT
    $1:road_event_sk::NUMBER,
    $1:event_type::VARCHAR,
    $1:severity_score::FLOAT,
    $1:severity_label::VARCHAR,
    $1:affects_traffic::BOOLEAN,
    $1:response_required::BOOLEAN,
    $1:description::VARCHAR
  FROM @SMART_CITY_DW.STAGING.stage_dim_road_event_type
)
FILE_FORMAT = (FORMAT_NAME = 'SMART_CITY_DW.STAGING.parquet_format')
ON_ERROR    = 'CONTINUE';

COPY INTO SMART_CITY_DW.RAW.dim_alert
FROM (
  SELECT
    $1:alert_sk::NUMBER,
    $1:alert_type::VARCHAR,
    $1:severity::VARCHAR,
    $1:rule_name::VARCHAR,
    $1:threshold_value::FLOAT,
    $1:triggered_sensor::VARCHAR,
    $1:description::VARCHAR
  FROM @SMART_CITY_DW.STAGING.stage_dim_alert
)
FILE_FORMAT = (FORMAT_NAME = 'SMART_CITY_DW.STAGING.parquet_format')
ON_ERROR    = 'CONTINUE';


-- ── 6. COPY INTO — LOAD FACTS AFTER ALL DIMENSIONS ───────────

COPY INTO SMART_CITY_DW.RAW.fact_vehicle_telemetry
FROM (
  SELECT
    $1:fact_vehicle_sk::NUMBER,
    $1:date_sk::NUMBER,
    $1:vehicle_sk::NUMBER,
    $1:route_sk::NUMBER,
    $1:location_sk::NUMBER,
    $1:weather_sk::NUMBER,
    $1:alert_sk::NUMBER,
    $1:speed_kmh::FLOAT,
    $1:acceleration_ms2::FLOAT,
    $1:rpm::FLOAT,
    $1:engine_temp_c::FLOAT,
    $1:fuel_level_pct::FLOAT,
    $1:fuel_consumed_l::FLOAT,
    $1:fuel_rate_l100km::FLOAT,
    $1:traffic_density::FLOAT,
    $1:is_anomaly::BOOLEAN,
    $1:speed_band::VARCHAR,
    $1:fuel_band::VARCHAR
  FROM @SMART_CITY_DW.STAGING.stage_fact_vehicle_telemetry
)
FILE_FORMAT = (FORMAT_NAME = 'SMART_CITY_DW.STAGING.parquet_format')
ON_ERROR    = 'CONTINUE';

COPY INTO SMART_CITY_DW.RAW.fact_route_traffic
FROM (
  SELECT
    $1:route_traffic_sk::NUMBER,
    $1:date_sk::NUMBER,
    $1:route_sk::NUMBER,
    $1:location_sk::NUMBER,
    $1:weather_sk::NUMBER,
    $1:avg_speed_kmh::FLOAT,
    $1:max_speed_kmh::FLOAT,
    $1:avg_traffic_density::FLOAT,
    $1:unique_vehicles::NUMBER,
    $1:anomaly_count::NUMBER,
    $1:road_event_count::NUMBER,
    $1:count_stopped::NUMBER,
    $1:count_slow::NUMBER,
    $1:count_medium::NUMBER,
    $1:count_fast::NUMBER,
    $1:count_overspeed::NUMBER
  FROM @SMART_CITY_DW.STAGING.stage_fact_route_traffic
)
FILE_FORMAT = (FORMAT_NAME = 'SMART_CITY_DW.STAGING.parquet_format')
ON_ERROR    = 'CONTINUE';

COPY INTO SMART_CITY_DW.RAW.fact_road_events
FROM (
  SELECT
    $1:fact_road_events_sk::NUMBER,
    $1:date_sk::NUMBER,
    $1:vehicle_sk::NUMBER,
    $1:location_sk::NUMBER,
    $1:road_event_sk::NUMBER,
    $1:event_id::VARCHAR,
    $1:severity_score::FLOAT,
    $1:incident_count::NUMBER,
    $1:vehicles_involved::NUMBER,
    $1:avg_severity_score::FLOAT,
    $1:avg_latitude::FLOAT,
    $1:avg_longitude::FLOAT
  FROM @SMART_CITY_DW.STAGING.stage_fact_road_events
)
FILE_FORMAT = (FORMAT_NAME = 'SMART_CITY_DW.STAGING.parquet_format')
ON_ERROR    = 'CONTINUE';

COPY INTO SMART_CITY_DW.RAW.fact_fuel_emissions
FROM (
  SELECT
    $1:fact_fuel_sk::NUMBER,
    $1:date_sk::NUMBER,
    $1:vehicle_sk::NUMBER,
    $1:total_fuel_consumed_l::FLOAT,
    $1:avg_fuel_rate_l100km::FLOAT,
    $1:avg_fuel_level_pct::FLOAT,
    $1:estimated_co2_kg::FLOAT,
    $1:unique_vehicles::NUMBER,
    $1:total_events::NUMBER
  FROM @SMART_CITY_DW.STAGING.stage_fact_fuel_emissions
)
FILE_FORMAT = (FORMAT_NAME = 'SMART_CITY_DW.STAGING.parquet_format')
ON_ERROR    = 'CONTINUE';

COPY INTO SMART_CITY_DW.RAW.fact_weather_impact
FROM (
  SELECT
    $1:fact_weather_sk::NUMBER,
    $1:date_sk::NUMBER,
    $1:weather_sk::NUMBER,
    $1:avg_temp_c::FLOAT,
    $1:max_temp_c::FLOAT,
    $1:avg_humidity_pct::FLOAT,
    $1:avg_wind_kmh::FLOAT,
    $1:avg_visibility_km::FLOAT,
    $1:avg_speed_factor::FLOAT,
    $1:min_speed_factor::FLOAT,
    $1:observation_count::NUMBER
  FROM @SMART_CITY_DW.STAGING.stage_fact_weather_impact
)
FILE_FORMAT = (FORMAT_NAME = 'SMART_CITY_DW.STAGING.parquet_format')
ON_ERROR    = 'CONTINUE';


-- ── 7. VERIFY LOADS ───────────────────────────────────────────

SELECT 'dim_date'              AS tbl, COUNT(*) AS rows FROM SMART_CITY_DW.RAW.dim_date              UNION ALL
SELECT 'dim_vehicle'           AS tbl, COUNT(*) AS rows FROM SMART_CITY_DW.RAW.dim_vehicle           UNION ALL
SELECT 'dim_route'             AS tbl, COUNT(*) AS rows FROM SMART_CITY_DW.RAW.dim_route             UNION ALL
SELECT 'dim_location'          AS tbl, COUNT(*) AS rows FROM SMART_CITY_DW.RAW.dim_location          UNION ALL
SELECT 'dim_weather'           AS tbl, COUNT(*) AS rows FROM SMART_CITY_DW.RAW.dim_weather           UNION ALL
SELECT 'dim_road_event_type'   AS tbl, COUNT(*) AS rows FROM SMART_CITY_DW.RAW.dim_road_event_type   UNION ALL
SELECT 'dim_alert'             AS tbl, COUNT(*) AS rows FROM SMART_CITY_DW.RAW.dim_alert             UNION ALL
SELECT 'fact_vehicle_telemetry'AS tbl, COUNT(*) AS rows FROM SMART_CITY_DW.RAW.fact_vehicle_telemetry UNION ALL
SELECT 'fact_route_traffic'    AS tbl, COUNT(*) AS rows FROM SMART_CITY_DW.RAW.fact_route_traffic    UNION ALL
SELECT 'fact_road_events'      AS tbl, COUNT(*) AS rows FROM SMART_CITY_DW.RAW.fact_road_events      UNION ALL
SELECT 'fact_fuel_emissions'   AS tbl, COUNT(*) AS rows FROM SMART_CITY_DW.RAW.fact_fuel_emissions   UNION ALL
SELECT 'fact_weather_impact'   AS tbl, COUNT(*) AS rows FROM SMART_CITY_DW.RAW.fact_weather_impact
ORDER BY tbl;

-- Check COPY history for any errors
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME   => 'SMART_CITY_DW.RAW.fact_vehicle_telemetry',
  START_TIME   => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
));
