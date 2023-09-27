{{ config(
    materialized='incremental',
    incremental_strategy='legacy',
    engine='MergeTree()',
    order_by='time_end',
    unique_key='id'
)}}

SELECT * FROM {{ ref('login_add_time_end') }}