{{ config(materialized='table') }}

select distinct
  observed_at,
  observed_at::date as date_value,
  extract(year from observed_at)::integer as year_number,
  extract(quarter from observed_at)::integer as quarter_number,
  extract(month from observed_at)::integer as month_number,
  trim(to_char(observed_at, 'Month')) as month_name,
  case extract(month from observed_at)::integer
    when 1 then '           Jan'
    when 2 then '          Feb'
    when 3 then '         Mar'
    when 4 then '        Apr'
    when 5 then '       May'
    when 6 then '      Jun'
    when 7 then '     Jul'
    when 8 then '    Aug'
    when 9 then '   Sep'
    when 10 then '  Oct'
    when 11 then ' Nov'
    else 'Dec'
  end as month_short,
  extract(day from observed_at)::integer as day_number,
  extract(hour from observed_at)::integer as hour_number,
  extract(week from observed_at)::integer as iso_week_number,
  extract(isodow from observed_at)::integer as day_of_week_number,
  trim(to_char(observed_at, 'Dy')) as day_name,
  case extract(isodow from observed_at)::integer
    when 1 then '      Mon'
    when 2 then '     Tue'
    when 3 then '    Wed'
    when 4 then '   Thu'
    when 5 then '  Fri'
    when 6 then ' Sat'
    else 'Sun'
  end as day_short
from {{ ref('stg_airviro_measurement') }}
