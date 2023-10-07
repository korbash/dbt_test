{%- set max_date = get_max_date('track_geo_status') -%}
SELECT id, track_id, event, type, lower(country) AS country, currency, lang, time_sort
FROM {{ source('src_actions', 'src_actions') }}
WHERE event IN ['session_start', 'change', 'trade', 'payment']
    AND toDate(time_sort) BETWEEN
    toDate('{{ max_date }}') + toIntervalDay(1)
    AND toDate('{{ max_date }}') + toIntervalDay(10 + 1)

