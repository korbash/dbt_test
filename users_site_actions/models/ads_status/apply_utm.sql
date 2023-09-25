{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['datetime']
) }}

select distinct on (track_id, type, method, merge_id, success) *
from {{ source('src_actions', 'src_actions') }}
where event == 'apply_utm'
