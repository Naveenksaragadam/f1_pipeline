{% snapshot snp_constructors %}

{{
    config(
      target_schema='snapshots',
      unique_key='constructor_id',
      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True,
    )
}}

select
    constructor_id,
    name,
    nationality,
    url
from {{ ref('stg_constructors') }}
where constructor_id is not null

{% endsnapshot %}
