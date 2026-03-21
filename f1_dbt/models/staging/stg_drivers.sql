{{ config(materialized="view", schema="staging", tags=["staging", "drivers"]) }}

select
    assumeNotNull(driver_id) as driver_id,
    permanent_number,
    code,
    given_name,
    family_name,
    date_of_birth,
    nationality,
    url
from {{ read_silver_parquet('drivers') }}
