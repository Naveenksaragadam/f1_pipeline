{{ config(materialized="view", schema="staging", tags=["staging", "constructors"]) }}

select
    assumeNotNull(constructor_id) as constructor_id,
    name,
    nationality,
    url
from {{ read_silver_parquet('constructors') }}
