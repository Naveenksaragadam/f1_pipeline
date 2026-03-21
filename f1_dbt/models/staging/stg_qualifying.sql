{{ config(materialized="view", schema="staging", tags=["staging", "qualifying"]) }}

select
    number,
    position,

    -- Driver
    driver_driver_id          as driver_id,
    driver_code               as driver_code,
    driver_given_name         as driver_given_name,
    driver_family_name        as driver_family_name,

    -- Constructor
    constructor_constructor_id as constructor_id,
    constructor_name           as constructor_name,

    -- Qualifying times
    q1,
    q2,
    q3

from {{ read_silver_parquet('qualifying') }}
