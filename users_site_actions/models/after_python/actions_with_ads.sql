{%- set n = 500 -%}
{%- set f_list = [] -%}
{%- for i in range(n) -%}
    {%- set f_list = f_list.append('{}'.format(i)) -%}
{%- endfor -%}

{{ config(
    materialized='table_by_parts',
    filter_list=f_list,
    engine='MergeTree()',
    order_by=['event', 'success', 'type', 'method', 'datetime']
) }}

SELECT {{
        dbt_utils.star(ref("separate_ads_info"),
        except=['promo0', 'promo_time0', 'ref0', 'ref_time0',
        'utm_source0', 'utm_medium0', 'utm_campain0', 'utm_time0',
        'user_id', 'device', 'browser', 'country', 'lang', 'currency'])
    }},
    anyLast(user_id) OVER w AS user_id,
    anyLast(device) OVER w AS device,
    anyLast(browser) OVER w AS browser,
    anyLast(country) OVER w AS country,
    anyLast(lang) OVER w AS lang,
    anyLast(currency) OVER w AS currency,
    groupArray(100)(promo0) OVER w AS promocodes,
    groupArray(100)(promo_time0) OVER w AS promo_times,
    groupArray(100)(ref0) OVER w AS refs,
    groupArray(100)(ref_time0) OVER w AS ref_times,
    groupArray(100)(utm_source0) OVER w AS utm_source,
    groupArray(100)(utm_medium0) OVER w AS utm_medium,
    groupArray(100)(utm_campain0) OVER w AS utm_campain,
    groupArray(100)(utm_time0) OVER w AS utm_times

FROM {{ref("separate_ads_info")}}
WHERE modulo(sipHash64(track_id), 500) == '__filter__'
WINDOW w AS (PARTITION BY track_id ORDER BY datetime)
