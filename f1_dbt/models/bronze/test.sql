{{ config(materialized="view", schema="staging", tags=["base", "seasons"]) }}

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
            ) as season
;
