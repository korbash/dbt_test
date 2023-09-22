SELECT * FROM (
    SELECT *,
        any(user_id) OVER (PARTITION BY track_id ORDER BY datetime ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS last_user_id
    FROM {{ ref("clean_dubles_ids") }}
    )
WHERE user_id != last_user_id