{{ config(
    engine='MergeTree()',
    order_by=['event', 'success', 'type', 'method', 'datetime']
) }}

select * from {{ ref("distinct_actions") }}
-- WHERE event IN ['apply_ref', 'apply_promo']
limit 10000000
