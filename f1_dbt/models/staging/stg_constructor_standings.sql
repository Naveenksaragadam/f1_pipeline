{{ config(materialized="view", schema="staging", tags=["staging", "standings"]) }}

select
    position,
    points,
    wins,

    -- Constructor
    constructor_constructor_id  as constructor_id,
    constructor_name            as constructor_name,
    constructor_nationality     as constructor_nationality

from {{ read_silver_parquet('constructorstandings') }}
