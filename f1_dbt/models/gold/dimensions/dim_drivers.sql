{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "dimension"],
        order_by="driver_id"
    )
}}

{#
    dim_drivers: One row per unique driver across all seasons.
    Deduplicates by driver_id, taking the latest known attributes.
#}

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
        row_number() over (
            partition by driver_id
            order by
                permanent_number desc nulls last,
                code desc nulls last
        ) as rn
    from {{ ref('stg_drivers') }}
    where driver_id is not null and driver_id != ''
)

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
    now() as _loaded_at
from ranked_drivers
where rn = 1
