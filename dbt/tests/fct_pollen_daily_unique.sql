-- Fail when the pollen fact has duplicate daily keys.

select
  station_key,
  indicator_key,
  date_key
from {{ ref('fct_pollen_daily') }}
group by 1, 2, 3
having count(*) > 1
