{{ config(
    tags=['ready'],
    materialized='table',
    engine='MergeTree()',
    order_by=['source', 'medium', 'campaign']
)}}

SELECT track_id,
    any(time_sort) AS time_sort,
    any(type) AS source,
    any(method) AS medium,
    any(merge_id) AS campaign,
    any(tb2.user_id) AS user_id
FROM {{ ref('apply_utm') }} LEFT JOIN {{ ref('track_login_status') }} AS tb2 USING(track_id)
GROUP BY track_id



