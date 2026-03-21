{{ config(materialized="view", schema="staging", tags=["staging", "sprint"]) }}

select
    -- Race context (from file path)
    assumeNotNull(CAST(extract(_path, 'season=([0-9]+)'), 'UInt16')) as season,
    assumeNotNull(CAST(extract(_path, 'round=([0-9]+)'), 'UInt8'))   as round,

    number,
    position,
    points,
    grid,
    laps,
    status,

    -- Driver
    assumeNotNull(driver_driver_id)          as driver_id,
    driver_code               as driver_code,
    driver_given_name         as driver_given_name,
    driver_family_name        as driver_family_name,

    -- Constructor
    assumeNotNull(constructor_constructor_id) as constructor_id,
    constructor_name           as constructor_name,

    -- Timing
    time_time                 as sprint_time,
    time_millis               as sprint_time_millis

from {{ read_silver_parquet('sprint', pattern='season=*/round=*/**/*.parquet') }}
