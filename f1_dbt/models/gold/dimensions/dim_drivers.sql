{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "dimension"],
        order_by="driver_id"
    )
}}

{# dim_drivers: One row per unique driver across all seasons. #}

select
    driver_id,
    permanent_number,
    code,
    given_name,
    family_name,
    date_of_birth,
    nationality,
    url,
    concat(given_name, ' ', family_name) as full_name,
    dbt_updated_at as _loaded_at,
    dbt_valid_from,
    dbt_scd_id
from {{ ref('snp_drivers') }}
where dbt_valid_to is null
