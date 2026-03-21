{{ config(materialized="view", schema="staging", tags=["staging", "laps"]) }}

select
    -- Race context (from file path)
    assumeNotNull(CAST(extract(_path, 'season=([0-9]+)'), 'UInt16')) as season,
    assumeNotNull(CAST(extract(_path, 'round=([0-9]+)'), 'UInt8'))   as round,

    assumeNotNull(timings_driver_id) as driver_id,
    number as lap_number,
    timings_position as position,
    timings_time as time

from {{ read_silver_parquet('laps', pattern='season=*/round=*/**/*.parquet') }}
