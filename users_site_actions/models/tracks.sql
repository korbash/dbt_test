{{ config(
    materialized='incremental',
    incremental_strategy='legacy',
    engine='MergeTree()',
    order_by='track_id',
    unique_key='track_id'
)}}

SELECT track_id, 
    any(browser) AS browser, 
    any(device) AS device,
    any(domain) AS domain
FROM {{ source('src_actions', 'src_actions') }}
WHERE event == 'session_start'
GROUP BY track_id