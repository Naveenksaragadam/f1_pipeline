{{ config(materialized="view", schema="staging", tags=["staging", "status"]) }}

select
    assumeNotNull(status_id) as status_id,
    status,
    count
from {{ read_silver_parquet('status') }}
