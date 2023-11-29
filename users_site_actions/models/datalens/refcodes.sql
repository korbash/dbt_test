{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by= 'refcode'
) }}
SELECT DISTINCT method AS refcode FROM {{ source('src_actions', 'src_actions') }}
WHERE event == 'apply_ref'
ORDER BY refcode
