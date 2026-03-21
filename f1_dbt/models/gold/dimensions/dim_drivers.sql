{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "dimension"],
        order_by="driver_id"
    )
}}

{# dim_drivers: One row per unique driver across all seasons. #}

with ranked_drivers as (
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
        row_number() over (
            partition by driver_id
            order by dbt_updated_at desc
        ) as rn
    from {{ ref('snp_drivers') }}
    where dbt_valid_to is null
)

select
    driver_id,
    permanent_number,
    code,
    given_name,
    family_name,
    full_name,
    date_of_birth,
    nationality,
    url,
    now() as _loaded_at
from ranked_drivers
where rn = 1
