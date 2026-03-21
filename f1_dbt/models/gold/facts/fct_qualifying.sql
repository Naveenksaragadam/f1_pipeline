{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "fact"],
        order_by="(driver_id, constructor_id)"
    )
}}

{#
    fct_qualifying: Qualifying session results per driver per race.
    Grain: (driver_id, constructor_id) per qualifying session.
#}

select
    -- Foreign keys
    driver_id,
    constructor_id,

    -- Performance
    number              as car_number,
    position            as qualifying_position,

    -- Session times
    q1,
    q2,
    q3,

    -- Best qualifying time (coalesce Q3 > Q2 > Q1)
    coalesce(q3, q2, q1) as best_qualifying_time,

    -- Metadata
    now() as _loaded_at

from {{ ref('stg_qualifying') }}
where driver_id is not null and driver_id != ''
