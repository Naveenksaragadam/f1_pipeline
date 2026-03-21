{{ config(
    materialized="table",
    schema="gold",
    tags=["gold", "fact"],
    order_by=["season", "round", "position"],
    settings={"allow_nullable_key": 1}
) }}

{# fct_driver_standings: Driver championship standings per round. #}

select
    season,
    round,
    driver_id,
    constructor_id,
    position,
    points,
    wins,
    now() as _loaded_at
from {{ ref("stg_driver_standings") }}
