{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "dimension"],
        order_by="constructor_id"
    )
}}

{# dim_constructors: One row per unique constructor/team. #}

with ranked_constructors as (
    select
        constructor_id,
        name,
        nationality,
        url,
        row_number() over (
            partition by constructor_id
            order by dbt_updated_at desc
        ) as rn
    from {{ ref('snp_constructors') }}
    where dbt_valid_to is null
)

select
    constructor_id,
    name,
    nationality,
    url,
    now() as _loaded_at
from ranked_constructors
where rn = 1
