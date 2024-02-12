{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['event', 'type', 'success']
) }}

SELECT
    tb1.track_id AS track_id,
    tb1.cluster_id AS cluster_id,
    tb1.degree AS degree,
    tb1.time_created AS time_created,
    tb1.is_track AS is_track,
    tb2.event AS event,
    tb2.type AS type,
    tb2.success AS success,
    tb2.first_act AS first_act,
    tb2.last_act AS last_act,
    tb2.transactions AS transactions,
    tb2.sum_usd AS sum_usd
FROM
    {{ source(
        'clusters',
        'clients'
    ) }} AS tb1
    INNER JOIN {{ ref("funnel_summery") }} AS tb2 USING(track_id)
    
