{{config(
    materialized='table',
    engine='MergeTree()',
    order_by=['on_sale', 'in_sept', 'on_withdrawal']
    )}}
SELECT user_id, good_id, on_sale, on_withdrawal, count() AS n, (datetime >= '2023-09-01') AS in_sept 
FROM {{ ref('user_items_preprocess') }}
GROUP BY user_id, good_id, on_sale, on_withdrawal, in_sept