{{ config(materialized='table') }}

with dates as (
  select distinct observed_date as date_value
  from {{ ref('stg_ohuseire_measurement') }}
)
select
  cast(to_char(date_value, 'YYYYMMDD') as integer) as date_key,
  date_value,
  extract(year from date_value)::integer as year_number,
  extract(quarter from date_value)::integer as quarter_number,
  extract(month from date_value)::integer as month_number,
  trim(to_char(date_value, 'Month')) as month_name,
  trim(to_char(date_value, 'Mon')) as month_short,
  extract(day from date_value)::integer as day_number,
  extract(week from date_value)::integer as iso_week_number,
  extract(isodow from date_value)::integer as day_of_week_number,
  trim(to_char(date_value, 'Day')) as day_name,
  trim(to_char(date_value, 'Dy')) as day_short
from dates
