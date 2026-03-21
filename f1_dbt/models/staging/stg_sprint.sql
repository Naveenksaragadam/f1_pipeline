{{ config(materialized="view", schema="staging", tags=["staging", "sprint"]) }}

select
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

from {{ read_silver_parquet('sprint') }}
