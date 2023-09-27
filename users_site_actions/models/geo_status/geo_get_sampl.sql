{% set get_max_date %}
SELECT max(toDate(time_start)) AS last_date FROM track_geo_status
{% endset %}
{% if execute %}
    {% if run_query('EXISTS TABLE track_geo_status').columns[0].values()[0] %}
        {% set max_date = run_query(get_max_date).columns[0].values()[0] %}
    {% else %} {% set max_date = '2022-09-01' %}
    {% endif %}
{% endif %}

SELECT id, track_id, event, type, country, currency, lang, datetime, time_sort
FROM {{ source('src_actions', 'src_actions') }}
WHERE event IN ['session_start', 'change', 'trade', 'payment']
    AND toDate(time_sort) BETWEEN
    toDate('{{ max_date }}') + toIntervalDay(1)
    AND toDate('{{ max_date }}') + toIntervalDay(10)

