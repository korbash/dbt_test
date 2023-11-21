{{ config(
    tags=['ready'],
    materialized='incremental',
    incremental_strategy='append',
    engine='MergeTree()',
    order_by=['event', 'success', 'type', 'method', 'time_sort']
)}} 
SELECT * FROM {{ ref('join_with_user_info') }}
