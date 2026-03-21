{% snapshot snp_drivers %}

{{
    config(
      target_schema='snapshots',
      unique_key='driver_id',
      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True,
    )
}}

select
    driver_id,
    permanent_number,
    code,
    given_name,
    family_name,
    date_of_birth,
    nationality,
    url
from {{ ref('stg_drivers') }}
where driver_id is not null

{% endsnapshot %}
