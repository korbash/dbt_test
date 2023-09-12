{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['event', 'success', 'type', 'method', 'datetime']
) }}

SELECT {{
        dbt_utils.star(ref("separate_ads_info"),
        except=['promo0', 'promo_time0', 'ref0', 'ref_time0',
        'utm_source0', 'utm_medium0', 'utm_campain0', 'utm_time0'])
    }},
    groupArray(50)(promo0) OVER w AS promocodes,
    groupArray(50)(promo_time0) OVER w AS promo_times,
    groupArray(50)(ref0) OVER w AS refs,
    groupArray(50)(ref_time0) OVER w AS ref_times,
    groupArray(50)(utm_source0) OVER w AS utm_source,
    groupArray(50)(utm_medium0) OVER w AS utm_medium,
    groupArray(50)(utm_campain0) OVER w AS utm_campain,
    groupArray(50)(utm_time0) OVER w AS utm_times

FROM {{ref("separate_ads_info")}}
-- WHERE modulo(sipHash64(track_id),100) == 0
WINDOW w AS (PARTITION BY track_id ORDER BY datetime)
