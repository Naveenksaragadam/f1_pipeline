{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "fact"],
        order_by="(driver_id, lap, stop)"
    )
}}

{#
    fct_pit_stops: Pit stop events per driver per race.
    Grain: (driver_id, lap, stop) per race.
#}

select
    -- Foreign keys
    driver_id,

    -- Pit stop details
    lap,
    stop                as stop_number,
    time_of_day,
    duration            as duration_raw,

    -- Metadata
    now() as _loaded_at

from {{ ref('stg_pitstops') }}
where driver_id is not null and driver_id != ''
