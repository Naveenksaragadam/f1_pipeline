{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "dimension"],
        order_by="(season, round)"
    )
}}

{#
    dim_races: One row per race event (season + round composite key).
    Enriched with circuit foreign key for star schema joins.
#}

select
    season,
    round,
    race_name,
    circuit_id,
    circuit_name,
    circuit_locality,
    circuit_country,
    date               as race_date,
    time               as race_start_time,
    url,
    now() as _loaded_at
from {{ ref('stg_races') }}
where season is not null and round is not null
