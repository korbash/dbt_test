{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by= 'source',
) }}
SELECT DISTINCT ON(type, method, merge_id)
    type AS source,
    method AS medium,
    merge_id AS campaign
FROM {{ source('src_actions', 'src_actions') }}
WHERE event == 'apply_utm'
ORDER BY source, medium, campaign
