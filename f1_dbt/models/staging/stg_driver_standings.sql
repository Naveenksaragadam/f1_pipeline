{{ config(materialized="view", schema="staging", tags=["staging", "standings"]) }}

-- DriverStandingSchema: constructors list is exploded, then flattened
select
    assumeNotNull(position) as position,
    points,
    wins,

    -- Driver
    assumeNotNull(driver_driver_id)          as driver_id,
    driver_code               as driver_code,
    driver_given_name         as driver_given_name,
    driver_family_name        as driver_family_name,
    driver_nationality        as driver_nationality,

    -- Constructor (exploded from list)
    constructors_constructor_id  as constructor_id,
    constructors_name            as constructor_name

from {{ read_silver_parquet('driverstandings') }}
