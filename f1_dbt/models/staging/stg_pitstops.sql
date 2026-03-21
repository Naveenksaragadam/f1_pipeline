{{ config(materialized="view", schema="staging", tags=["staging", "pitstops"]) }}

select
    -- Race context (from file path)
    assumeNotNull(CAST(extract(_path, 'season=([0-9]+)'), 'UInt16')) as season,
    assumeNotNull(CAST(extract(_path, 'round=([0-9]+)'), 'UInt8'))   as round,

    assumeNotNull(driver_id) as driver_id,
    assumeNotNull(lap) as lap,
    assumeNotNull(stop) as stop,
    time       as time_of_day,
    duration

from {{ read_silver_parquet('pitstops', pattern='season=*/round=*/**/*.parquet') }}
