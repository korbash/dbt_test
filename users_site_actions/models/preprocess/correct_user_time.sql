

SELECT track_id, event, time_upload, time_user,
    max(time_user) OVER w AS last_action,
    isNotNull(time_user) ? toDateTime64(
        (age('millisecond', toDateTime(0), time_user)
        + date_diff('millisecond', last_action, time_upload)
        )/1000, 3
    ) : time_upload as datetime
FROM (SELECT * FROM {{ ref("extract_meta_params")}} WHERE time_upload BETWEEN toDate('2023-01-01') AND toDate('2023-01-01'))
WINDOW w AS (PARTITION BY time_upload, track_id)
