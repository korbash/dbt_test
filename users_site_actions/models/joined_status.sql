{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='track_id'
)}} 

SELECT
    arrayMax(arrayFilter(x -> x != '1970-01-01', array(time_start1, time_start2, time_start3))) AS time_start,
    arrayMin(arrayFilter(x -> x != '1970-01-01', array(time_end1, time_end2, time_end3))) AS time_end,
    tracks.track_id AS track_id,
    tb1.user_id AS user_id,
    tracks.device AS device,
    tracks.browser AS browser,
    tracks.domain AS domain,
    tb2.promocodes AS promocodes,
    tb2.promo_times AS promo_times,
    tb2.refs AS refs,
    tb2.ref_times AS ref_times,
    tb2.utm_source AS utm_source,
    tb2.utm_medium AS utm_medium,
    tb2.utm_campain AS utm_campain,
    tb2.utm_times AS utm_times,
    tb3.country AS country,
    tb3.currency AS currency,
    tb3.lang AS lang,
    tb1.time_start AS time_start1,
    tb1.time_end AS time_end1,
    tb2.time_start AS time_start2,
    tb2.time_end AS time_end2,
    tb3.time_start AS time_start3,
    tb3.time_end AS time_end3
FROM {{ ref('tracks') }} AS tracks
    LEFT JOIN {{ ref('track_login_status') }} AS tb1 ON tracks.track_id == tb1.track_id
    LEFT JOIN {{ ref('track_ads_status') }} AS tb2 ON tracks.track_id == tb2.track_id
    LEFT JOIN {{ ref('track_geo_status') }} AS tb3 ON tracks.track_id == tb3.track_id
WHERE   time_start <= time_end
{# 
    AND ((time_sort >= tb1.time_start AND time_sort < tb1.time_end) OR tb1.time_start == '1970-01-01')
    AND ((time_sort >= tb2.time_start AND time_sort < tb2.time_end) OR tb2.time_start == '1970-01-01')
    AND ((time_sort >= tb3.time_start AND time_sort < tb3.time_end) OR tb3.time_start == '1970-01-01') #}
LIMIT 1000
SETTINGS join_algorithm = 'partial_merge'