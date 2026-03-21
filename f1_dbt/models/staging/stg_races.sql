{{ config(materialized="view", schema="staging", tags=["staging", "races"]) }}

select
    season,
    round,
    url,
    race_name,
    date,
    time,
    circuit_circuit_id  as circuit_id,
    circuit_name        as circuit_name,
    circuit_url         as circuit_url,
    circuit_location_lat       as circuit_lat,
    circuit_location_long      as circuit_lng,
    circuit_location_locality  as circuit_locality,
    circuit_location_country   as circuit_country
from {{ read_silver_parquet('races') }}
