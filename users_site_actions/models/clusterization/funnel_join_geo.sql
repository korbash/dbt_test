{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['event', 'type', 'success']
) }}

SELECT
    tb1.*,
    geo.country AS country,
    geo.currency AS currency,
    geo.lang AS lang
FROM
    {{ ref("funnel_join_ads") }} AS tb1
    LEFT JOIN {{ ref('track_geo_status') }} AS geo
        ON tb1.track_id = geo.track_id
            AND geo.time_end = '2050-01-01'   
