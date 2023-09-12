{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['event', 'success', 'type', 'method', 'datetime']
) }}

SELECT * FROM {{ ref("actions_with_ads") }} LEFT JOIN {{ ref("extract_users") }} USING(user_id)