{{ config(
    materialized="table",
    schema="gold",
    tags=["gold", "fact"],
    order_by=["season", "round", "position"],
    settings={"allow_nullable_key": 1}
) }}

{# fct_constructor_standings: Constructor championship standings per round. #}

select
    season,
    round,
    constructor_id,
    position,
    points,
    wins,
    now() as _loaded_at
from {{ ref("stg_constructor_standings") }}
