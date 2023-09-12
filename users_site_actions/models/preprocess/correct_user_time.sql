
{% set d1 = dt.date(2023, 7, 1) %}
{% set d2 = dt.date(2023, 7, 3) %}
{% for d in np.arange(d1, d2, dt.timedelta(days=1), dtype=dt.datetime) %}
SELECT track_id, event, time_upload, time_user,
    max(time_user) OVER w AS last_action,
    isNotNull(time_user) ? toDateTime64(
        (age('millisecond', toDateTime(0), time_user)
        + date_diff('millisecond', last_action, time_upload)
        )/1000, 3
    ) : time_upload as datetime
FROM (SELECT * FROM {{ ref("extract_meta_params")}} WHERE time_upload BETWEEN toDate({{d.date()}}) AND toDate({{d.date()}}))
WINDOW w AS (PARTITION BY time_upload, track_id)
{% endfor %}
