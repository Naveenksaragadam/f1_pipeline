{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "fact"],
        order_by="(driver_id, lap_number)"
    )
}}

{#
    fct_lap_times: Individual lap timing data — the largest fact table.
    Grain: (driver_id, lap_number) per race.
    Source: exploded from LapSchema.timings list in the Silver layer.
#}

select
    -- Foreign keys
    driver_id,

    -- Lap details
    lap_number,
    position            as track_position,
    lap_time,

    -- Metadata
    now() as _loaded_at

from {{ ref('stg_laps') }}
where driver_id is not null and driver_id != ''
