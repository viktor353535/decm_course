{{ config(materialized='table') }}

select
  cast(station_key as text) as station_key,
  cast(source_type as text) as source_type,
  cast(station_id as integer) as station_id,
  cast(station_name as text) as station_name,
  cast(city_name as text) as city_name,
  cast(station_type as text) as station_type,
  cast(airviro_code as text) as airviro_code,
  cast(is_default_course_station as boolean) as is_default_course_station
from {{ ref('dim_station_seed') }}
