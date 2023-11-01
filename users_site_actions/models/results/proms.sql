{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by= 'promocode'
) }}
SELECT DISTINCT method AS promocode FROM {{ source('src_actions', 'src_actions') }}
WHERE event == 'apply_promo'
ORDER BY promocode

UNION ALL

SELECT 'all_users' AS promocode

UNION ALL

SELECT 'use_any' AS promocode

UNION ALL

SELECT 'use_nothing' AS promocode

