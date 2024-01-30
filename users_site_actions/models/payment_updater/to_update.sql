{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['time_sort']
)}} 
SELECT * EXCEPT (success), true AS success FROM (
SELECT * FROM {{ ref('users_actions') }}
WHERE event == 'payment'
    AND type == 'withdraw'
    AND NOT success
    AND merge_id IN (
        SELECT merge_id FROM {{ source('src_back', 'withdraws_updates') }}
        WHERE success
        )
)
