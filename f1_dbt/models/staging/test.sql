select *
from
    s3(
        'http://minio:9000/bronze/ergast/endpoint=seasons/**/*.json',
        'minioadmin',
        'password',
        'JSONAsString',
        'json String',
        'gzip'
    )
limit 500
