{{ config(materialized="view", schema="staging", tags=["staging", "constructors"]) }}

select
    constructor_id,
    name,
    nationality,
    url
from {{ read_silver_parquet('constructors') }}
