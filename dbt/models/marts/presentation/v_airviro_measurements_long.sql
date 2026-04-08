{{ config(materialized='view') }}

-- Compatibility view for older notes and saved datasets.
select *
from {{ ref('v_ohuseire_measurements_long') }}
