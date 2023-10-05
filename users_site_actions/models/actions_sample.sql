{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='MergeTree()',
    order_by=['event', 'success', 'type', 'method', 'datetime']
)}} 

SELECT 
    {{ dbt_utils.star(
        source('src_actions', 'src_actions'),
        relation_alias='tb0',
        prefix=' ',
        quote_identifiers=False,
        except=['user_id', 'device', 'browser', 'domain', 'promocodes',
        'promo_times', 'refs', 'ref_times', 'utm_source', 'utm_medium',
        'utm_campain', 'utm_times', 'country', 'currency', 'lang']
        ) }},
    {# tb0.user_id AS user_id_ald, #}
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
    tb2.time_start AS time_start2,
    tb3.time_start AS time_start3
FROM {{ source('src_actions', 'src_actions') }} AS tb0
    LEFT JOIN {{ ref('tracks') }} AS tracks ON tb0.track_id == tracks.track_id
    LEFT JOIN {{ ref('track_login_status') }} AS tb1 ON tb0.track_id == tb1.track_id
    LEFT JOIN {{ ref('track_ads_status') }} AS tb2 ON tb0.track_id == tb2.track_id
    LEFT JOIN {{ ref('track_geo_status') }} AS tb3 ON tb0.track_id == tb3.track_id
WHERE   true
    {# AND ((time_sort >= tb1.time_start AND time_sort < tb1.time_end) OR tb1.time_start == '1970-01-01')
    AND ((time_sort >= tb2.time_start AND time_sort < tb2.time_end) OR tb2.time_start == '1970-01-01')
    AND ((time_sort >= tb3.time_start AND time_sort < tb3.time_end) OR tb3.time_start == '1970-01-01') #}
    {% if is_incremental() %}
        {{ log('time_sort: {}, incr'.format(time_sort), info=True) }}
        AND time_sort > (SELECT max(time_sort) FROM {{ this }})
        AND time_sort <= (SELECT max(time_sort) + INTERVAL 24 HOUR FROM {{ this }})
    {% else %}
        {{ log('time_sort: {}, not incr'.format(time_sort), info=True) }}
        AND time_sort <= (SELECT min(time_sort) + INTERVAL 24 HOUR FROM {{ source('src_actions', 'src_actions') }})
    {% endif %}