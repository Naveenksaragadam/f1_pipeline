{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "dimension"],
        order_by="constructor_id"
    )
}}

{#
    dim_constructors: One row per unique constructor/team.
    Deduplicates by constructor_id.
#}

with ranked_constructors as (
    select
        constructor_id,
        name,
        nationality,
        url,
        row_number() over (
            partition by constructor_id
            order by name
        ) as rn
    from {{ ref('stg_constructors') }}
    where constructor_id is not null and constructor_id != ''
)

select
    constructor_id,
    name,
    nationality,
    url,
    now() as _loaded_at
from ranked_constructors
where rn = 1
