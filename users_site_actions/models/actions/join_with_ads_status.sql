{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['time_sort']
) }}

SELECT
    {{ ref('join_with_track_info') }}.*,
    promocodes,
    promo_times,
    refs,
    ref_times,
    utm_source,
    utm_medium,
    utm_campain,
    utm_times
FROM {{ ref('join_with_track_info') }}
LEFT JOIN {{ ref('track_ads_status') }} AS ads_status USING(track_id)
WHERE (time_sort >= ads_status.time_start AND time_sort < ads_status.time_end) OR ads_status.time_start = '1970-01-01'

