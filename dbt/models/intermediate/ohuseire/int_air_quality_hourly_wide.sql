{{ config(materialized='view') }}

with grouped as (
  select
    station_key,
    station_id,
    observed_date,
    date_key,
    hour_key,
    hour_occurrence_in_day,
    min(observed_at) as slot_first_observed_at,
    max(observed_at) as slot_last_observed_at,
    bool_and(is_complete_day_series) as all_loaded_indicators_have_complete_day_series,
    bool_or(has_repeated_or_skipped_clock_hour) as any_indicator_has_clock_anomaly,
    bool_or(has_unexpected_clock_pattern) as any_indicator_has_unexpected_clock_pattern,
    bool_or(is_expected_dst_transition_day) as is_expected_dst_transition_day,
    count(*) as indicators_present_in_slot,
    max(value_numeric) filter (where indicator_code = 'so2') as so2,
    max(value_numeric) filter (where indicator_code = 'no2') as no2,
    max(value_numeric) filter (where indicator_code = 'co') as co,
    max(value_numeric) filter (where indicator_code = 'o3') as o3,
    max(value_numeric) filter (where indicator_code = 'pm10') as pm10,
    max(value_numeric) filter (where indicator_code = 'pm2_5') as pm2_5,
    max(value_numeric) filter (where indicator_code = 'temp') as temp,
    max(value_numeric) filter (where indicator_code = 'hum') as hum,
    max(value_numeric) filter (where indicator_code = 'rain') as rain,
    max(value_numeric) filter (where indicator_code = 'press') as press,
    max(value_numeric) filter (where indicator_code = 'rad') as rad,
    max(value_numeric) filter (where indicator_code = 'wd10') as wd10,
    max(value_numeric) filter (where indicator_code = 'ws10') as ws10
  from {{ ref('int_air_quality_measurement') }}
  group by station_key, station_id, observed_date, date_key, hour_key, hour_occurrence_in_day
)
select
  grouped.*,
  row_number() over (
    partition by grouped.station_key, grouped.observed_date
    order by grouped.slot_first_observed_at, grouped.hour_key, grouped.hour_occurrence_in_day
  ) - 1 as slot_sequence_in_day
from grouped
