{{ config(materialized="view", schema="staging", tags=["staging", "results"]) }}

select
    -- Race context (from file path / parent record structure)
    number,
    position,
    points,
    grid,
    laps,
    status,

    -- Driver
    assumeNotNull(driver_driver_id)          as driver_id,
    driver_permanent_number   as driver_number,
    driver_code               as driver_code,
    driver_given_name         as driver_given_name,
    driver_family_name        as driver_family_name,
    driver_date_of_birth      as driver_dob,
    driver_nationality        as driver_nationality,

    -- Constructor
    assumeNotNull(constructor_constructor_id) as constructor_id,
    constructor_name           as constructor_name,
    constructor_nationality    as constructor_nationality,

    -- Timing
    time_time                 as race_time,
    time_millis               as race_time_millis,

    -- Fastest lap
    fastest_lap_rank                      as fastest_lap_rank,
    fastest_lap_lap                       as fastest_lap_number,
    fastest_lap_time_time                 as fastest_lap_time,
    fastest_lap_time_millis               as fastest_lap_millis,
    fastest_lap_average_speed_speed       as fastest_lap_speed,
    fastest_lap_average_speed_units       as fastest_lap_speed_units

from {{ read_silver_parquet('results') }}
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference=1
