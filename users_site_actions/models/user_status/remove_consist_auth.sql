SELECT * FROM (
    SELECT * EXCEPT last_user_id,
        any(user_id) OVER (PARTITION BY track_id ORDER BY time_sort ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS last_user_id
    FROM {{ ref("add_last_status") }}
    )
WHERE user_id != last_user_id OR isNull(last_user_id)