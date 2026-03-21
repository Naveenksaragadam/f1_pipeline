{{ config(materialized="view", schema="staging", tags=["staging", "status"]) }}

select
    status_id,
    status,
    count
from {{ read_silver_parquet('status') }}
