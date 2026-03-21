{{ config(materialized="view", schema="staging", tags=["staging", "seasons"]) }}

select
    season,
    url
from {{ read_silver_parquet('seasons') }}
