{{ config(materialized='view') }}

select
  station_key,
  indicator_key,
  source_type,
  station_id,
  observed_date,
  date_key,
  indicator_code,
  indicator_name,
  max(observed_at) as observed_at,
  max(value_numeric) as value_numeric,
  count(*) as measurements_in_day,
  max(extracted_at) as extracted_at
from {{ ref('stg_ohuseire_measurement') }}
where source_type = 'pollen'
group by
  station_key,
  indicator_key,
  source_type,
  station_id,
  observed_date,
  date_key,
  indicator_code,
  indicator_name
