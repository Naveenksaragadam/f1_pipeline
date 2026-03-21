{# Reusable macro to read Silver Parquet files from MinIO via ClickHouse s3() #}
{% macro read_silver_parquet(endpoint) %}
    s3(
        '{{ var("minio_endpoint") }}/silver/ergast/endpoint={{ endpoint }}/**/*.parquet',
        '{{ var("minio_access_key") }}',
        '{{ var("minio_secret_key") }}',
        'Parquet'
    )
{% endmacro %}
