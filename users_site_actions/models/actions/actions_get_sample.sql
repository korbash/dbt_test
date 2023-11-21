{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['time_sort'],
) }}
{%- set max_date = get_max_date2('time_sort', 'users_actions') -%}
-- Model 1: actions_get_sample_with_where
select
    * except (
        user_id,
        device,
        browser,
        domain,
        promocodes,
        promo_times,
        refs,
        ref_times,
        utm_source,
        utm_medium,
        utm_campain,
        utm_times,
        country,
        currency,
        lang
    )
from {{ ref('distinct_actions') }}
where
    toDate(time_sort)
    between todate('{{ max_date }}')
    + tointervalday(1) and todate('{{ max_date }}')
    + tointervalday(2 + 1)
    