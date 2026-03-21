{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "dimension"],
        order_by="constructor_id"
    )
}}

{# dim_constructors: One row per unique constructor/team. #}

select
    constructor_id,
    name,
    nationality,
    url,
    dbt_updated_at as _loaded_at,
    dbt_valid_from,
    dbt_scd_id
from {{ ref('snp_constructors') }}
where dbt_valid_to is null
