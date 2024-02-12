{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['event', 'type', 'success']
) }}

SELECT
    tb1.*,
    tb3.domain AS domain,
    tb3.device AS device,
    tb3.browser AS browser
FROM
    {{ ref("funnel_join_clusters") }} AS tb1
    LEFT JOIN {{ ref("tracks") }} AS tb3 USING(track_id)