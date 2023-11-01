{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['time_sort'],
) }}
{%- set max_date = get_max_date2('time_sort', 'users_actions')  -%}
-- Model 1: actions_get_sample_with_where
SELECT 
    * EXCEPT (user_id, device, browser, domain, promocodes,
    promo_times, refs, ref_times, utm_source, utm_medium,
    utm_campain, utm_times, country, currency, lang)
FROM {{ ref('distinct_actions') }}
WHERE  time_sort BETWEEN
    toDate('{{ max_date }}') + toIntervalDay(1) 
    AND toDate('{{ max_date }}') + toIntervalDay(2 + 1)
