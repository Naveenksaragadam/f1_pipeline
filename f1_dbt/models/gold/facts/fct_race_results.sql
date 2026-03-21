{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "fact"],
        order_by="(driver_id, constructor_id)"
    )
}}

{#
    fct_race_results: Core fact table — one row per driver per race.
    Grain: (driver_id, constructor_id, number -- per race file).
    The season/round context is embedded in the Parquet file path by the Silver layer.
    Since the Silver results Parquet files are partitioned by season/round,
    each file represents a single race, so all rows within share the same context.
#}

select
    -- Foreign keys
    driver_id,
    constructor_id,

    -- Race performance measures
    number              as car_number,
    position            as finish_position,
    grid                as grid_position,
    laps                as laps_completed,
    points,
    status,

    -- Timing
    race_time,
    race_time_millis,

    -- Fastest lap details
    fastest_lap_rank,
    fastest_lap_number,
    fastest_lap_time,
    fastest_lap_millis,
    fastest_lap_speed,
    fastest_lap_speed_units,

    -- Derived measures
    position - grid     as positions_gained,

    -- Metadata
    now() as _loaded_at

from {{ ref('stg_results') }}
where driver_id is not null and driver_id != ''
