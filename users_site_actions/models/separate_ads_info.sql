{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['event', 'success', 'type', 'method', 'datetime']
) }}

SELECT *,
    if(event == 'apply_promo', method, Null) as promo0,
    if(event == 'apply_promo', datetime, Null) as promo_time0,
    if(event == 'apply_ref', method, Null) as ref0,
    if(event == 'apply_ref', datetime, Null) as ref_time0,
    if(event == 'apply_utm', type, Null) as utm_source0,
    if(event == 'apply_utm', method, Null) as utm_medium0,
    if(event == 'apply_utm', merge_id, Null) as utm_campain0,
    if(event == 'apply_utm', datetime, Null) as utm_time0
FROM {{ ref("distinct_actions") }}