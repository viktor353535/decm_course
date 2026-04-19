{{ config(materialized='table') }}

select
  hour_key,
  hour_key as hour_number,
  lpad(hour_key::text, 2, '0') || ':00' as hour_label
from generate_series(0, 23) as hour_key
