{{ config(materialized="view", schema="staging", tags=["staging", "constructor_standings"]) }}

select
    -- Race context (from file path)
    assumeNotNull(CAST(extract(_path, 'season=([0-9]+)'), 'UInt16')) as season,
    assumeNotNull(CAST(extract(_path, 'round=([0-9]+)'), 'UInt8'))   as round,

    -- Standing details
    position,
    points,
    wins,

    -- Constructor
    assumeNotNull(constructor_constructor_id) as constructor_id,
    constructor_name            as constructor_name,
    constructor_nationality     as constructor_nationality

from {{ read_silver_parquet('constructorstandings', pattern='season=*/round=*/**/*.parquet') }}
