{%- set max_date = get_max_date('track_login_status') -%}
SELECT id, track_id, Null AS user_id, 'init' AS event, 'pass' AS type, 'pass' AS method, time_start
FROM ({{ get_uniq_track_sample(max_date, 15) }})
{% if max_date != '2022-09-01'%}
    WHERE track_id NOT IN (
        SELECT track_id FROM ({{ get_actual_info('track_login_status', ref('login_get_sample')) }})
    )
    UNION ALL (
    {{ get_actual_info('track_login_status', ref('login_get_sample')) }}
    )
{% endif %}
UNION ALL
SELECT * FROM {{ ref('login_get_sample') }}
