{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "fact"],
        order_by="(constructor_id, championship_position)"
    )
}}

{#
    fct_constructor_standings: Constructor championship standings per round.
    Grain: (constructor_id) per standings snapshot.
#}

select
    -- Foreign keys
    constructor_id,

    -- Standing details
    position            as championship_position,
    points              as championship_points,
    wins                as season_wins,

    -- Metadata
    now() as _loaded_at

from {{ ref('stg_constructor_standings') }}
where constructor_id is not null and constructor_id != ''
