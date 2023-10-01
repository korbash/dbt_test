SELECT * EXCEPT last_user_id FROM (
    SELECT *,
        any(user_id) OVER (PARTITION BY track_id ORDER BY time_start ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS last_user_id
    FROM {{ ref('login_clean_dubles_ids') }}
    )
WHERE user_id != last_user_id OR isNull(last_user_id)
    