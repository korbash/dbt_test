SELECT id, track_id,
    Null AS user_id, 'init' AS event,
    'pass' AS type, 'pass' AS method,
    time_start
FROM {{ ref('login_uniq_track_sample') }} LEFT ANTI JOIN {{ ref('login_actual_info_sample') }} USING track_id