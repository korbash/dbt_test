{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['user_id'],
) }}


SELECT DISTINCT user_id FROM {{ ref('ads_info_join_user') }}