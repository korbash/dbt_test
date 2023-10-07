SELECT * EXCEPT (last_user_id, last_event) FROM (
    SELECT *,
        any(user_id) OVER w AS last_user_id,
        any(event) OVER w AS last_event
    FROM {{ ref('login_clean_dubles_ids') }}
    WINDOW w AS (PARTITION BY track_id ORDER BY time_start ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
    )
WHERE (user_id != last_user_id)
    OR (last_event == '')
    OR (isNull(last_user_id) AND isNotNull(user_id))
    