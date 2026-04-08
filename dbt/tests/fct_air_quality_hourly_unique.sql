-- Fail when the hourly fact has duplicate dimensional keys.

select
  station_key,
  indicator_key,
  date_key,
  hour_key,
  hour_occurrence_in_day
from {{ ref('fct_air_quality_hourly') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
