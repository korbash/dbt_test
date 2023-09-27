{{ config(
    materialized='incremental',
    incremental_strategy='legacy',
    engine='MergeTree()',
    order_by='time_end',
    unique_key='user_id'
)}}

SELECT * FROM {{ ref('cash_add_time_end') }}