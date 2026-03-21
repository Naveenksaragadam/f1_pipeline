{{ config(materialized="view", schema="staging", tags=["staging", "laps"]) }}

-- Laps are exploded: LapSchema.timings list is flattened with timings_ prefix
select
    assumeNotNull(number)                as lap_number,
    assumeNotNull(timings_driver_id)     as driver_id,
    timings_position      as position,
    timings_time          as lap_time
from {{ read_silver_parquet('laps') }}
