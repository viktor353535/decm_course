{{ config(materialized='view') }}

select
  fact.station_key,
  station.station_id,
  station.station_name,
  station.city_name,
  fact.observed_date,
  fact.date_key,
  date_dim.year_number,
  date_dim.quarter_number,
  date_dim.month_number,
  date_dim.month_name,
  date_dim.month_short,
  date_dim.day_number,
  date_dim.iso_week_number,
  date_dim.day_of_week_number,
  date_dim.day_name,
  date_dim.day_short,
  fact.indicator_key,
  indicator.indicator_code,
  indicator.indicator_name,
  fact.observed_at,
  fact.value_numeric,
  fact.measurements_in_day
from {{ ref('fct_pollen_daily') }} as fact
left join {{ ref('dim_station') }} as station
  on station.station_key = fact.station_key
left join {{ ref('dim_indicator') }} as indicator
  on indicator.indicator_key = fact.indicator_key
left join {{ ref('dim_date') }} as date_dim
  on date_dim.date_key = fact.date_key
