{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='MergeTree()',
    order_by=['event', 'success', 'type', 'method', 'datetime']
)}} 
SELECT * FROM {{ ref('actions_add_status_info') }}
