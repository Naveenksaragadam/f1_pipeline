{{ config(
    materialized="table",
    schema="gold",
    tags=["gold", "fact"],
    order_by=["season", "round", "lap_number", "track_position"],
    settings={"allow_nullable_key": 1}
) }}

{# fct_lap_times: Individual lap timing data — the largest fact table. #}

select
    season,
    round,
    driver_id,
    lap_number,
    position as track_position,
    time     as lap_time,
    now()    as _loaded_at
from {{ ref("stg_laps") }}
where driver_id is not null and driver_id != ''
