{{ config(materialized="view", schema="staging", tags=["staging", "driver_standings"]) }}

select
    -- Race context (from file path)
    assumeNotNull(CAST(extract(_path, 'season=([0-9]+)'), 'UInt16')) as season,
    assumeNotNull(CAST(extract(_path, 'round=([0-9]+)'), 'UInt8'))   as round,

    -- Standing details
    position,
    points,
    wins,

    -- Driver
    assumeNotNull(driver_driver_id) as driver_id,

    -- Constructor (Standings can have multiple constructors, usually taken from the first)
    assumeNotNull(constructors_constructor_id) as constructor_id

from {{ read_silver_parquet('driverstandings', pattern='season=*/round=*/**/*.parquet') }}
