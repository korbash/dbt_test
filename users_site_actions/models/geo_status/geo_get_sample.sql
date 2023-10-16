{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='time_start'
)}}
{%- set max_date = get_max_date('track_geo_status') -%}
SELECT id, track_id, event, type, lower(country) AS country, currency, lang, time_sort AS time_start
FROM {{ source('src_actions', 'src_actions') }}
WHERE event IN ['session_start', 'change', 'trade', 'payment']
    AND toDate(time_start) BETWEEN
    toDate('{{ max_date }}') + toIntervalDay(1)
    AND toDate('{{ max_date }}') + toIntervalDay(5 + 1)

