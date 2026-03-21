{{ config(materialized="view", schema="staging", tags=["staging", "pitstops"]) }}

select
    driver_id,
    lap,
    stop,
    time       as time_of_day,
    duration
from {{ read_silver_parquet('pitstops') }}
