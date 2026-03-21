{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "dimension"],
        order_by="circuit_id"
    )
}}

{#
    dim_circuits: One row per unique circuit with geographic data.
    Sources from staged circuits data (which has flattened location).
#}

with ranked_circuits as (
    select
        circuit_id,
        circuit_name,
        url,
        lat,
        lng,
        locality,
        country,
        row_number() over (
            partition by circuit_id
            order by circuit_name asc
        ) as rn
    from {{ ref('stg_circuits') }}
    where circuit_id is not null and circuit_id != ''
)

select
    circuit_id,
    circuit_name,
    url,
    lat,
    lng,
    locality,
    country,
    now() as _loaded_at
from ranked_circuits
where rn = 1
