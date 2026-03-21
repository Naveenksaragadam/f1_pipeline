{{ config(materialized="view", schema="staging", tags=["staging", "qualifying"]) }}

select
    -- Race context (from file path)
    assumeNotNull(CAST(extract(_path, 'season=([0-9]+)'), 'UInt16')) as season,
    assumeNotNull(CAST(extract(_path, 'round=([0-9]+)'), 'UInt8'))   as round,

    -- Driver
    assumeNotNull(driver_driver_id)          as driver_id,
    driver_code               as driver_code,
    driver_given_name         as driver_given_name,
    driver_family_name        as driver_family_name,

    -- Constructor
    assumeNotNull(constructor_constructor_id) as constructor_id,
    constructor_name           as constructor_name,

    -- Qualifying performance
    number,
    position,
    q1,
    q2,
    q3

from {{ read_silver_parquet('qualifying', pattern='season=*/round=*/**/*.parquet') }}
