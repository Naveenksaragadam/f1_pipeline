{{ config(materialized="view", schema="staging", tags=["staging", "circuits"]) }}

select distinct
    assumeNotNull(circuit_id) as circuit_id,
    name as circuit_name,
    location_lat       as lat,
    location_long      as lng,
    location_locality  as locality,
    location_country   as country,
    url
from {{ read_silver_parquet('circuits') }}
