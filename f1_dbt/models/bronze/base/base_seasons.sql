{{ config(materialized="view", schema="staging", tags=["base", "seasons"]) }}

with
    source_files as (
        select
            _path as file_path,
            json_content as raw_json
        from
            s3(
                '{{ var("minio_endpoint") }}/{{ var("bronze_bucket") }}/ergast/endpoint=seasons/**/*.json',
                '{{ var("minio_access_key") }}',
                '{{ var("minio_secret_key") }}',
                'JSONAsString',
                'json_content String', -- Required: Maps file content to this column name
                'gzip'                 -- Required: Handles the compressed binary data
            )
    ),

    parsed_metadata as (
        select
            file_path,
            raw_json,
            JSONExtractString(raw_json, 'metadata', 'batch_id') as batch_id,
            JSONExtractString(raw_json, 'metadata', 'endpoint') as endpoint,
            parseDateTimeBestEffort(
                JSONExtractString(raw_json, 'metadata', 'ingestion_timestamp')
            ) as ingestion_timestamp,
            JSONExtractRaw(raw_json, 'data', 'MRData') as mr_data
        from source_files
    ),

    unnested_seasons as (
        select
            batch_id,
            endpoint,
            ingestion_timestamp,
            file_path,
            JSONExtractString(season_json, 'season') as season_year,
            JSONExtractString(season_json, 'url') as season_url
        from parsed_metadata
        array join JSONExtractArrayRaw(mr_data, 'SeasonTable', 'Seasons') as season_json
    )

select 
    season_year, 
    season_url, 
    batch_id, 
    ingestion_timestamp, 
    file_path
from unnested_seasons
where season_year is not null and season_year != ''