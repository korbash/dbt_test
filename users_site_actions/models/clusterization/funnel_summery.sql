{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['event', 'type', 'success']
)}}

SELECT track_id, event, type, success,
    min(time_sort) AS first_act,
    max(time_sort) AS last_act,
    count() AS transactions,
    assumeNotNull(abs(sumIf(spend * exchange_rate, spend != 0))) AS sum_usd
FROM {{ ref('users_actions') }}
GROUP BY event, success, type, track_id
ORDER BY event, success, type