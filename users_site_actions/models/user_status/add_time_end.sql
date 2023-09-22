

SELECT id, track_id, user_id, datetime, time_start,
    if(time_end == '1970-01-01', date('2050-01-01'), time_end) AS time_end
FROM (
    SELECT id, track_id, user_id, datetime, time_sort AS time_start,
        any(time_sort) OVER (PARTITION BY track_id ORDER BY time_sort ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS time_end,
        if(time_end == '1970-01-01', date('2050-01-01'), time_end)
    FROM {{ ref("remove_consist_auth") }}
)