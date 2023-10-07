SELECT * EXCEPT time_end,
    if(time_end == '1970-01-01', date('2050-01-01'), time_end) AS time_end
FROM (
    SELECT *,
        any(time_start) OVER (PARTITION BY track_id ORDER BY time_start ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS time_end
    FROM {{ ref('login_remove_consist_auth') }}
)
