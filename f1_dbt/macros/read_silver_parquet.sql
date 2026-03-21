{# 
    Reusable macro to read Silver Parquet files from MinIO via ClickHouse s3().
    
    Arguments:
    - endpoint: The Ergast endpoint name (e.g., 'results', 'drivers').
    - pattern: Optional glob pattern for partition extraction (e.g., 'season={season:UInt16}/*.parquet').
               Defaults to '**/*.parquet' if not provided.
#}
{% macro read_silver_parquet(endpoint, pattern=none) %}
    s3(
        '{{ var("minio_endpoint") }}/silver/ergast/endpoint={{ endpoint }}/{{ pattern if pattern else "**/*.parquet" }}',
        '{{ var("minio_access_key") }}',
        '{{ var("minio_secret_key") }}',
        'Parquet'
    )
{% endmacro %}
