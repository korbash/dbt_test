SELECT id, track_id,
    {{ source('src_actions', 'src_actions') }}.user_id,
    tb2.user_id AS uid2,
    event, time_sort, time_start, time_end
FROM {{ source('src_actions', 'src_actions') }} JOIN {{ ref('track_login_status') }} AS tb2 USING track_id
WHERE time_sort BETWEEN toDate('2023-08-01') AND toDate('2023-08-01') + INTERVAL 1 MINUTE
    AND time_sort BETWEEN time_start AND time_end