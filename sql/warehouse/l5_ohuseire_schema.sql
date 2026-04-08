-- Lecture 5 Ohuseire raw-layer bootstrap objects.
-- Safe to rerun. dbt owns the star-schema objects in l5_mart.

CREATE SCHEMA IF NOT EXISTS __RAW_SCHEMA__;
CREATE SCHEMA IF NOT EXISTS __MART_SCHEMA__;

CREATE TABLE IF NOT EXISTS __RAW_SCHEMA__.ohuseire_measurement (
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

ALTER TABLE __RAW_SCHEMA__.ohuseire_measurement
  ADD COLUMN IF NOT EXISTS local_hour_occurrence integer;

UPDATE __RAW_SCHEMA__.ohuseire_measurement
SET local_hour_occurrence = 1
WHERE local_hour_occurrence IS NULL;

ALTER TABLE __RAW_SCHEMA__.ohuseire_measurement
  ALTER COLUMN local_hour_occurrence SET DEFAULT 1,
  ALTER COLUMN local_hour_occurrence SET NOT NULL;

ALTER TABLE __RAW_SCHEMA__.ohuseire_measurement
  DROP CONSTRAINT IF EXISTS ohuseire_measurement_pk;

ALTER TABLE __RAW_SCHEMA__.ohuseire_measurement
  ADD CONSTRAINT ohuseire_measurement_pk
    PRIMARY KEY (source_type, station_id, observed_at, indicator_code, local_hour_occurrence);

CREATE INDEX IF NOT EXISTS idx_ohuseire_measurement_observed_at
  ON __RAW_SCHEMA__.ohuseire_measurement (observed_at);

CREATE INDEX IF NOT EXISTS idx_ohuseire_measurement_source_indicator
  ON __RAW_SCHEMA__.ohuseire_measurement (source_type, indicator_code);

CREATE TABLE IF NOT EXISTS __RAW_SCHEMA__.ohuseire_ingestion_audit (
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
