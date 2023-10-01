{{ config(
    materialized='incremental',
    incremental_strategy='legacy',
    engine='MergeTree()',
    order_by='time_end',
    unique_key='id'
)}}

SELECT * FROM {{ ref('cash_status_final_sample') }}