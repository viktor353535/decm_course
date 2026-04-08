{{ config(materialized='table') }}

select
  station_key,
  indicator_key,
  date_key,
  source_type,
  station_id,
  observed_date,
  observed_at,
  indicator_code,
  indicator_name,
  value_numeric,
  measurements_in_day,
  extracted_at
from {{ ref('int_pollen_daily') }}
