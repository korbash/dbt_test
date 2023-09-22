{{ config(
    materialized='incremental',
    engine='MergeTree()',
    order_by='time_end',
    uniq_id='id'
)}}

SELECT * FROM {{ ref("add_time_end") }}