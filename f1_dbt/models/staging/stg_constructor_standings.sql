{{ config(materialized="view", schema="staging", tags=["staging", "standings"]) }}

select
    assumeNotNull(position) as position,
    points,
    wins,

    -- Constructor
    assumeNotNull(constructor_constructor_id)  as constructor_id,
    constructor_name            as constructor_name,
    constructor_nationality     as constructor_nationality

from {{ read_silver_parquet('constructorstandings') }}
