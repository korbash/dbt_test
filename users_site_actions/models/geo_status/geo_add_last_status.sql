{% if execute %}
    {% if run_query('EXISTS TABLE track_geo_status').columns[0].values()[0] %}
SELECT id, track_id, event, type, country, currency, lang, datetime, time_start AS time_sort
FROM track_geo_status
WHERE toDate(time_end) == '2050-01-01'
    AND track_id IN (
        SELECT track_id
        FROM {{ ref('geo_get_sampl') }}
    )
UNION ALL
    {% endif %}
{% endif %}

SELECT *
FROM {{ ref('geo_get_sampl') }}