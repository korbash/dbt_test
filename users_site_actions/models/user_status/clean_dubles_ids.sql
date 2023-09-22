SELECT * EXCEPT rang FROM (
    SELECT id, track_id, user_id, datetime, time_sort, type,
        row_number() OVER (PARTITION BY id ORDER BY time_sort) AS rang
    FROM {{ ref("auth_sample") }}
)
WHERE rang == 1