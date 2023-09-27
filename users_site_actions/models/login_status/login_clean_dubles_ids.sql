SELECT * EXCEPT rang FROM (
    SELECT id, track_id, user_id, event, type, method, datetime, time_sort,
        row_number() OVER (PARTITION BY id ORDER BY time_sort) AS rang
    FROM {{ ref('login_get_sample') }}
)
WHERE rang == 1