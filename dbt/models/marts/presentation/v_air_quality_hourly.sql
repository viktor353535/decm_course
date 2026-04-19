{{ config(materialized='view') }}

select
  wide.station_key,
  station.station_id,
  station.station_name,
  station.city_name,
  wide.observed_date,
  wide.date_key,
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
  wide.hour_key,
  wide.hour_occurrence_in_day,
  wide.slot_sequence_in_day,
  time_dim.hour_number,
  time_dim.hour_label,
  wide.slot_first_observed_at as observed_at,
  wide.slot_last_observed_at,
  wide.indicators_present_in_slot,
  wide.all_loaded_indicators_have_complete_day_series,
  wide.any_indicator_has_clock_anomaly,
  wide.any_indicator_has_unexpected_clock_pattern,
  wide.is_expected_dst_transition_day,
  wide.so2,
  wide.no2,
  wide.co,
  wide.o3,
  wide.pm10,
  wide.pm2_5,
  wide.temp,
  wide.wd10,
  wide.ws10,
  wind.sector_code as wind_sector,
  wind.sector_name as wind_sector_name,
  wide.hum,
  wide.rain,
  wide.press,
  wide.rad
from {{ ref('int_air_quality_hourly_wide') }} as wide
left join {{ ref('dim_station') }} as station
  on station.station_key = wide.station_key
left join {{ ref('dim_date') }} as date_dim
  on date_dim.date_key = wide.date_key
left join {{ ref('dim_time_hour') }} as time_dim
  on time_dim.hour_key = wide.hour_key
left join {{ ref('dim_wind_direction') }} as wind
  on (
    (wind.wraps_around and (wide.wd10 >= wind.min_degree or wide.wd10 < wind.max_degree))
    or
    ((not wind.wraps_around) and wide.wd10 >= wind.min_degree and wide.wd10 < wind.max_degree)
  )
