{{ config(
    materialized = 'table',
    engine = 'MergeTree()',
    order_by = ['event', 'type', 'success', 'time_created']
) }}
{# EXPLAIN PIPELINE #}
SELECT cluster_id, event, type, success,
    min(time_created) AS time_created,
    min(first_act) AS first_act,
    max(last_act) AS last_act,
    sum(transactions) AS transactions,
    sum(sum_usd) AS sum_usd,
    groupUniqArray(domain) AS domain,
    groupUniqArray(device) AS device,
    groupUniqArray(browser) AS browser,
    arrayDistinct(arrayFlatten(groupArray(promocodes))) AS promocodes,
    arrayUniq(groupUniqArray(promo_times)) AS promo_times,
    arrayUniq(groupUniqArray(refs)) AS refs,
    arrayUniq(groupUniqArray(ref_times)) AS ref_times,
    arrayUniq(groupUniqArray(utm_source)) AS utm_source,
    arrayUniq(groupUniqArray(utm_campain)) AS utm_campain,
    arrayUniq(groupUniqArray(utm_medium)) AS utm_medium,
    arrayUniq(groupUniqArray(utm_times)) AS utm_times,
    groupUniqArray(country) AS country,
    groupUniqArray(currency) AS currency,
    groupUniqArray(lang) AS lang
FROM {{ ref('funnel_join_geo') }}
GROUP BY cluster_id, event, type, success
ORDER BY event, type, success, time_created
