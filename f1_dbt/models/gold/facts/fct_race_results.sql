{{ config(
    materialized="table",
    schema="gold",
    tags=["gold", "fact"],
    order_by=["season", "round", "finish_position"],
    settings={"allow_nullable_key": 1}
) }}

{# fct_race_results: Race results per driver — core analytics fact table. #}

select
    season,
    round,
    driver_id,
    constructor_id,
    number           as car_number,
    position         as finish_position,
    grid             as grid_position,
    laps             as laps_completed,
    points,
    status,
    race_time,
    race_time_millis,
    fastest_lap_rank,
    fastest_lap_number,
    fastest_lap_time,
    fastest_lap_speed,
    fastest_lap_speed_units,
    position - grid  as positions_gained,
    now()            as _loaded_at
from {{ ref("stg_results") }}
where driver_id is not null and driver_id != ''
