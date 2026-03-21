{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "fact"],
        order_by="(driver_id, constructor_id)"
    )
}}

{#
    fct_sprint_results: Sprint race results per driver.
    Grain: (driver_id, constructor_id) per sprint race.
#}

select
    -- Foreign keys
    driver_id,
    constructor_id,

    -- Performance
    number              as car_number,
    position            as finish_position,
    grid                as grid_position,
    laps                as laps_completed,
    points,
    status,

    -- Timing
    sprint_time,
    sprint_time_millis,

    -- Derived
    position - grid     as positions_gained,

    -- Metadata
    now() as _loaded_at

from {{ ref('stg_sprint') }}
where driver_id is not null and driver_id != ''
