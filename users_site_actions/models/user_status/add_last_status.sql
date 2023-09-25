{% if execute %}
    {% if run_query('EXISTS TABLE track_user_status').columns[0].values()[0] %}
SELECT id, track_id, user_id, event, type, method, datetime, time_start AS time_sort
FROM track_user_status
WHERE toDate(time_end) == '2050-01-01'
    AND track_id IN (
        SELECT track_id
        FROM {{ ref("clean_dubles_ids") }}
    )
UNION ALL
    {% endif %}
{% endif %}

SELECT *
FROM {{ ref("clean_dubles_ids") }}

