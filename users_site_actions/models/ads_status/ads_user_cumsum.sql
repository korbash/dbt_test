{{ config(
    materialized='view',
    engine='MergeTree()',
    order_by=['event', 'success', 'type', 'method', 'datetime']
) }}
SELECT id, user_id, event, time_start,
    groupArray(100)(promo0) OVER w AS promocodes,
    groupArray(100)(promo_time0) OVER w AS promo_times,
    groupArray(100)(ref0) OVER w AS refs,
    groupArray(100)(ref_time0) OVER w AS ref_times,
    groupArray(100)(utm_source0) OVER w AS utm_source,
    groupArray(100)(utm_medium0) OVER w AS utm_medium,
    groupArray(100)(utm_campain0) OVER w AS utm_campain,
    groupArray(100)(utm_time0) OVER w AS utm_times

FROM {{ref('ads_add_all_users')}}
WINDOW w AS (PARTITION BY user_id ORDER BY time_start)
