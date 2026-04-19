{{ config(materialized='view') }}

with source as (
  select
    station_key,
    indicator_key,
    source_type,
    station_id,
    observed_at,
    observed_at_local,
    observed_date,
    date_key,
    observed_clock_hour,
    local_hour_occurrence,
    source_timezone_name,
    indicator_code,
    indicator_name,
    value_numeric,
    source_row_hash,
    extracted_at
  from {{ ref('stg_ohuseire_measurement') }}
  where source_type = 'air_quality'
),
dst_calendar as (
  select distinct
    observed_date,
    (
      (
        make_date(extract(year from observed_date)::integer, 4, 1) - interval '1 day'
      )::date
      - (
        extract(
          isodow from (
            make_date(extract(year from observed_date)::integer, 4, 1) - interval '1 day'
          )::date
        )::integer % 7
      )
    )::date as spring_forward_date,
    (
      (
        make_date(extract(year from observed_date)::integer, 11, 1) - interval '1 day'
      )::date
      - (
        extract(
          isodow from (
            make_date(extract(year from observed_date)::integer, 11, 1) - interval '1 day'
          )::date
        )::integer % 7
      )
    )::date as autumn_fallback_date
  from source
),
daily_stats as (
  select
    source.station_key,
    source.indicator_key,
    source.observed_date,
    dst_calendar.spring_forward_date,
    dst_calendar.autumn_fallback_date,
    source.observed_date = dst_calendar.spring_forward_date as is_spring_forward_day,
    source.observed_date = dst_calendar.autumn_fallback_date as is_autumn_fallback_day,
    count(*) as measurements_in_day,
    count(distinct source.observed_clock_hour) as distinct_clock_hours_in_day,
    case
      when source.observed_date = dst_calendar.spring_forward_date then 23
      when source.observed_date = dst_calendar.autumn_fallback_date then 25
      else 24
    end as expected_measurements_in_day,
    case
      when source.observed_date = dst_calendar.spring_forward_date then 23
      else 24
    end as expected_distinct_clock_hours_in_day
  from source
  inner join dst_calendar
    on source.observed_date = dst_calendar.observed_date
  group by
    source.station_key,
    source.indicator_key,
    source.observed_date,
    dst_calendar.spring_forward_date,
    dst_calendar.autumn_fallback_date
),
profiled as (
  select
    source.station_key,
    source.indicator_key,
    source.source_type,
    source.station_id,
    source.observed_at,
    source.observed_at_local,
    source.observed_date,
    source.date_key,
    source.observed_clock_hour as hour_key,
    source.observed_clock_hour,
    source.local_hour_occurrence as hour_occurrence_in_day,
    source.source_timezone_name,
    source.indicator_code,
    source.indicator_name,
    source.value_numeric,
    source.source_row_hash,
    source.extracted_at,
    row_number() over (
      partition by source.station_key, source.indicator_key, source.observed_date
      order by source.observed_at, source.local_hour_occurrence, source.source_row_hash
    ) - 1 as slot_sequence_in_day
  from source
)
select
  profiled.station_key,
  profiled.indicator_key,
  profiled.source_type,
  profiled.station_id,
  profiled.observed_at,
  profiled.observed_at_local,
  profiled.observed_date,
  profiled.date_key,
  profiled.hour_key,
  profiled.hour_occurrence_in_day,
  profiled.slot_sequence_in_day,
  profiled.observed_clock_hour,
  profiled.source_timezone_name,
  profiled.indicator_code,
  profiled.indicator_name,
  profiled.value_numeric,
  profiled.source_row_hash,
  profiled.extracted_at,
  daily_stats.is_spring_forward_day,
  daily_stats.is_autumn_fallback_day,
  daily_stats.is_spring_forward_day
    or daily_stats.is_autumn_fallback_day as is_expected_dst_transition_day,
  daily_stats.measurements_in_day,
  daily_stats.distinct_clock_hours_in_day,
  daily_stats.expected_measurements_in_day,
  daily_stats.expected_distinct_clock_hours_in_day,
  daily_stats.measurements_in_day = daily_stats.expected_measurements_in_day
    and daily_stats.distinct_clock_hours_in_day = daily_stats.expected_distinct_clock_hours_in_day
    as is_complete_day_series,
  daily_stats.measurements_in_day > daily_stats.distinct_clock_hours_in_day as has_repeated_clock_hour,
  daily_stats.distinct_clock_hours_in_day < 24 as has_missing_clock_hour,
  daily_stats.measurements_in_day > daily_stats.distinct_clock_hours_in_day
    or daily_stats.distinct_clock_hours_in_day < 24 as has_repeated_or_skipped_clock_hour,
  not (
    daily_stats.measurements_in_day = daily_stats.expected_measurements_in_day
    and daily_stats.distinct_clock_hours_in_day = daily_stats.expected_distinct_clock_hours_in_day
  ) as has_unexpected_clock_pattern
from profiled
inner join daily_stats
  on profiled.station_key = daily_stats.station_key
 and profiled.indicator_key = daily_stats.indicator_key
 and profiled.observed_date = daily_stats.observed_date
