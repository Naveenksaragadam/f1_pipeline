{{ config(materialized="view", schema="staging", tags=["staging", "constructors"]) }}

select distinct
    assumeNotNull(constructor_id) as constructor_id,
    name,
    nationality,
    url
from {{ read_silver_parquet('constructors') }}
