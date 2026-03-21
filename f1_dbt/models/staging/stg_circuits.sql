{{ config(materialized="view", schema="staging", tags=["staging", "circuits"]) }}

select
    circuit_id,
    name,
    url,
    location_lat     as lat,
    location_long    as lng,
    location_locality as locality,
    location_country  as country
from {{ read_silver_parquet('circuits') }}
