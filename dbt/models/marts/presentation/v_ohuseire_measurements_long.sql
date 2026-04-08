{{ config(materialized='view') }}

select
  measurement.source_type,
  measurement.station_key,
  station.station_id,
  station.station_name,
  station.city_name,
  measurement.indicator_key,
  indicator.indicator_code,
  indicator.indicator_name,
  measurement.observed_at,
  measurement.observed_date,
  measurement.date_key,
  air_fact.hour_key,
  air_fact.hour_occurrence_in_day,
  air_fact.slot_sequence_in_day,
  measurement.observed_clock_hour,
  measurement.source_timezone_name,
  air_fact.is_expected_dst_transition_day,
  air_fact.is_complete_day_series,
  air_fact.has_repeated_or_skipped_clock_hour,
  air_fact.has_unexpected_clock_pattern,
  measurement.value_numeric,
  measurement.source_row_hash,
  measurement.extracted_at
from {{ ref('stg_ohuseire_measurement') }} as measurement
left join {{ ref('dim_station') }} as station
  on station.station_key = measurement.station_key
left join {{ ref('dim_indicator') }} as indicator
  on indicator.indicator_key = measurement.indicator_key
left join {{ ref('fct_air_quality_hourly') }} as air_fact
  on air_fact.source_row_hash = measurement.source_row_hash
