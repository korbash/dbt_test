{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['time_sort']
) }}

SELECT
    join_with_ads_status.*,
    country,
    currency,
    lang
FROM {{ ref('join_with_ads_status') }}
LEFT JOIN {{ ref('track_geo_status') }} AS geo_status USING(track_id)
WHERE (time_sort >= geo_status.time_start AND time_sort < geo_status.time_end) OR geo_status.time_start = '1970-01-01'
SETTINGS join_algorithm = 'partial_merge'