{{ config(materialized='table') }}

with indicator_names as (
  select distinct
    indicator_key,
    source_type,
    indicator_code,
    indicator_name
  from {{ ref('stg_ohuseire_measurement') }}
),
indicator_name_stats as (
  select
    indicator_key,
    count(*) as source_name_variant_count
  from indicator_names
  group by indicator_key
),
ranked_indicator_names as (
  select
    indicator_names.indicator_key,
    indicator_names.source_type,
    indicator_names.indicator_code,
    indicator_names.indicator_name,
    row_number() over (
      partition by indicator_names.indicator_key
      order by length(indicator_names.indicator_name) desc, indicator_names.indicator_name desc
    ) as indicator_name_rank
  from indicator_names
)
select
  ranked_indicator_names.indicator_key,
  ranked_indicator_names.source_type,
  ranked_indicator_names.indicator_code,
  ranked_indicator_names.indicator_name,
  case
    when ranked_indicator_names.source_type = 'pollen' then 'daily'
    else 'hourly'
  end as expected_grain,
  indicator_name_stats.source_name_variant_count
from ranked_indicator_names
inner join indicator_name_stats
  on ranked_indicator_names.indicator_key = indicator_name_stats.indicator_key
where ranked_indicator_names.indicator_name_rank = 1
