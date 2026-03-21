{{ config(materialized="view", schema="staging", tags=["staging", "pitstops"]) }}

select
    assumeNotNull(driver_id) as driver_id,
    assumeNotNull(lap) as lap,
    assumeNotNull(stop) as stop,
    time       as time_of_day,
    duration
from {{ read_silver_parquet('pitstops') }}
