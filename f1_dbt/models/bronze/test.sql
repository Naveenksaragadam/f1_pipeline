select
    jsonextractstring(season, 'season') as season_year,
    jsonextractstring(season, 'url') as season_url
from
    s3(
        'http://minio:9000/bronze/ergast/endpoint=seasons/**/*.json',
        'minioadmin',
        'password',
        'JSONAsString',
        'json String',
        'gzip'
    ) as season
;
