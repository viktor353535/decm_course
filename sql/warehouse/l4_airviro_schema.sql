-- Lecture 4 advanced Airviro ETL warehouse objects.
-- Safe to rerun for the current Lecture 4 schema shape.
-- If this schema definition changes during lecture preparation,
-- reset l4_raw and l4_mart, then run bootstrap again.

CREATE SCHEMA IF NOT EXISTS __RAW_SCHEMA__;
CREATE SCHEMA IF NOT EXISTS __MART_SCHEMA__;

CREATE TABLE IF NOT EXISTS __RAW_SCHEMA__.airviro_measurement (
  source_type text NOT NULL,
  station_id integer NOT NULL,
  observed_at timestamp without time zone NOT NULL,
  local_hour_occurrence integer NOT NULL DEFAULT 1,
  indicator_code text NOT NULL,
  indicator_name text NOT NULL,
  value_numeric double precision,
  source_row_hash text NOT NULL,
  extracted_at timestamp with time zone NOT NULL DEFAULT now()
);

ALTER TABLE __RAW_SCHEMA__.airviro_measurement
  ADD COLUMN IF NOT EXISTS local_hour_occurrence integer;

UPDATE __RAW_SCHEMA__.airviro_measurement
SET local_hour_occurrence = 1
WHERE local_hour_occurrence IS NULL;

ALTER TABLE __RAW_SCHEMA__.airviro_measurement
  ALTER COLUMN local_hour_occurrence SET DEFAULT 1,
  ALTER COLUMN local_hour_occurrence SET NOT NULL;

ALTER TABLE __RAW_SCHEMA__.airviro_measurement
  DROP CONSTRAINT IF EXISTS airviro_measurement_pk;

ALTER TABLE __RAW_SCHEMA__.airviro_measurement
  ADD CONSTRAINT airviro_measurement_pk
    PRIMARY KEY (source_type, station_id, observed_at, indicator_code, local_hour_occurrence);

CREATE INDEX IF NOT EXISTS idx_airviro_measurement_observed_at
  ON __RAW_SCHEMA__.airviro_measurement (observed_at);

CREATE INDEX IF NOT EXISTS idx_airviro_measurement_source_indicator
  ON __RAW_SCHEMA__.airviro_measurement (source_type, indicator_code);

CREATE TABLE IF NOT EXISTS __RAW_SCHEMA__.airviro_ingestion_audit (
  ingestion_audit_id bigserial PRIMARY KEY,
  batch_id text NOT NULL,
  source_key text,
  source_type text NOT NULL,
  station_id integer,
  window_start timestamp with time zone NOT NULL,
  window_end timestamp with time zone NOT NULL,
  rows_read integer NOT NULL,
  records_upserted integer NOT NULL,
  duplicate_records integer NOT NULL,
  split_events integer NOT NULL,
  status text NOT NULL,
  message text,
  created_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS __RAW_SCHEMA__.pipeline_watermark (
  pipeline_name text PRIMARY KEY,
  watermark_date date NOT NULL,
  updated_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS __MART_SCHEMA__.dim_indicator (
  source_type text NOT NULL,
  indicator_code text NOT NULL,
  indicator_name text NOT NULL,
  PRIMARY KEY (source_type, indicator_code)
);

CREATE TABLE IF NOT EXISTS __MART_SCHEMA__.dim_datetime_hour (
  observed_at timestamp without time zone PRIMARY KEY,
  date_value date NOT NULL,
  year_number integer NOT NULL,
  quarter_number integer NOT NULL,
  month_number integer NOT NULL,
  month_name text NOT NULL,
  month_short text NOT NULL,
  day_number integer NOT NULL,
  hour_number integer NOT NULL,
  iso_week_number integer NOT NULL,
  day_of_week_number integer NOT NULL,
  day_name text NOT NULL,
  day_short text NOT NULL
);

CREATE TABLE IF NOT EXISTS __MART_SCHEMA__.dim_wind_direction (
  sector_id integer PRIMARY KEY,
  sector_code text NOT NULL UNIQUE,
  sector_name text NOT NULL,
  min_degree double precision NOT NULL,
  max_degree double precision NOT NULL,
  wraps_around boolean NOT NULL
);

DELETE FROM __MART_SCHEMA__.dim_wind_direction;

INSERT INTO __MART_SCHEMA__.dim_wind_direction (
  sector_id, sector_code, sector_name, min_degree, max_degree, wraps_around
)
VALUES
  (1, 'N',  'North',      337.50, 22.50,  true),
  (2, 'NE', 'Northeast',   22.50, 67.50,  false),
  (3, 'E',  'East',        67.50, 112.50, false),
  (4, 'SE', 'Southeast',  112.50, 157.50, false),
  (5, 'S',  'South',      157.50, 202.50, false),
  (6, 'SW', 'Southwest',  202.50, 247.50, false),
  (7, 'W',  'West',       247.50, 292.50, false),
  (8, 'NW', 'Northwest',  292.50, 337.50, false);

CREATE OR REPLACE VIEW __MART_SCHEMA__.v_airviro_measurements_long AS
SELECT
  m.source_type,
  m.station_id,
  m.observed_at,
  m.indicator_code,
  m.indicator_name,
  m.value_numeric,
  m.extracted_at
FROM __RAW_SCHEMA__.airviro_measurement AS m;

CREATE OR REPLACE VIEW __MART_SCHEMA__.v_air_quality_hourly_station_8 AS
WITH air_quality AS (
  SELECT
    station_id,
    observed_at,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'so2') AS so2,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'no2') AS no2,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'co') AS co,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'o3') AS o3,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'pm10') AS pm10,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'pm2_5') AS pm2_5,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'temp') AS temp,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'wd10') AS wd10,
    MAX(value_numeric) FILTER (WHERE indicator_code = 'ws10') AS ws10
  FROM __RAW_SCHEMA__.airviro_measurement
  WHERE source_type = 'air_quality'
    AND station_id = 8
  GROUP BY station_id, observed_at
)
SELECT
  'lecture4_station_8_only'::text AS dataset_scope,
  aq.station_id,
  aq.observed_at,
  dt.date_value,
  dt.year_number,
  dt.month_number,
  TRIM(dt.month_name) AS month_name,
  dt.day_number,
  dt.day_name,
  dt.day_of_week_number,
  dt.hour_number,
  aq.so2,
  aq.no2,
  aq.co,
  aq.o3,
  aq.pm10,
  aq.pm2_5,
  aq.temp,
  aq.wd10,
  aq.ws10,
  wd.sector_code AS wind_sector,
  wd.sector_name AS wind_sector_name,
  dt.month_short,
  dt.day_short
FROM air_quality AS aq
LEFT JOIN __MART_SCHEMA__.dim_datetime_hour AS dt
  ON dt.observed_at = aq.observed_at
LEFT JOIN __MART_SCHEMA__.dim_wind_direction AS wd
  ON (
    (wd.wraps_around IS TRUE AND (aq.wd10 >= wd.min_degree OR aq.wd10 < wd.max_degree))
    OR
    (wd.wraps_around IS FALSE AND aq.wd10 >= wd.min_degree AND aq.wd10 < wd.max_degree)
  );

CREATE OR REPLACE VIEW __MART_SCHEMA__.v_pollen_daily_station_25 AS
SELECT
  'lecture4_station_25_only'::text AS dataset_scope,
  m.station_id,
  m.observed_at,
  m.observed_at::date AS observed_date,
  m.indicator_code,
  m.indicator_name,
  m.value_numeric
FROM __RAW_SCHEMA__.airviro_measurement AS m
WHERE m.source_type = 'pollen'
  AND m.station_id = 25;
