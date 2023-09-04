{{ config(
    engine='MergeTree()',
    order_by=['datetime']
) }}

select distinct on (track_id, method, success) *
from {{ source('src_actions', 'src_actions') }}
where event == 'apply_promo'
