{{ config(
    engine='MergeTree()',
    order_by=['event', 'success', 'type', 'method', 'datetime']
) }}

select *
from {{ source('src_actions', 'src_actions') }}
where event not in ['apply_ref', 'apply_promo', 'apply_utm']

union all

select *
from {{ ref("apply_ref") }}

union all

select *
from {{ ref("apply_promo") }}

union all

select *
from {{ ref("apply_utm") }}
