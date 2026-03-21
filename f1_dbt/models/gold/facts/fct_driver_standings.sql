{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "fact"],
        order_by="(driver_id, position)"
    )
}}

{#
    fct_driver_standings: Driver championship standings per round.
    Grain: (driver_id, constructor_id) per standings snapshot.
    Note: constructors list is exploded in staging, so a driver who
    raced for multiple teams in a season will have multiple rows.
#}

select
    -- Foreign keys
    driver_id,
    constructor_id,

    -- Standing details
    position            as championship_position,
    points              as championship_points,
    wins                as season_wins,

    -- Metadata
    now() as _loaded_at

from {{ ref('stg_driver_standings') }}
where driver_id is not null and driver_id != ''
