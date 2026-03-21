{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "fact"],
        order_by=["season", "round", "driver_id", "stop_number"],
        settings={"allow_nullable_key": 1}
    )
}}

{#
    fct_pit_stops: Pit stop events per driver per race.
    Grain: (season, round, driver_id, stop) per race.
#}

select
    -- Foreign keys
    season,
    round,
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
