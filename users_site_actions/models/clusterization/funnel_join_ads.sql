{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['event', 'type', 'success']
) }}

SELECT
    tb1.*,
    ads.promocodes AS promocodes,
    ads.promo_times AS promo_times,
    ads.refs AS refs,
    ads.ref_times AS ref_times,
    ads.utm_source AS utm_source,
    ads.utm_medium AS utm_medium,
    ads.utm_campain AS utm_campain,
    ads.utm_times AS utm_times
FROM
    {{ ref("funnel_join_tracks") }} AS tb1
    LEFT JOIN {{ ref('track_ads_status') }} AS ads
        ON tb1.track_id = ads.track_id
            AND ads.time_end = '2050-01-01'   