{{
    config(
        materialized="table",
        schema="gold",
        tags=["gold", "dimension"],
        order_by="status_id"
    )
}}

{#
    dim_status: One row per race finishing status code.
    Relatively static reference data.
#}

select
    status_id,
    status              as status_text,
    count               as occurrence_count,
    status = 'Finished' as is_classified,
    now() as _loaded_at
from {{ ref('stg_status') }}
where status_id is not null
