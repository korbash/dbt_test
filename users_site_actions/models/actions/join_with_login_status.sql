{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['time_sort']
) }}

-- Model 3: actions_with_login_status
SELECT {{ ref('actions_get_sample') }}.*,
    user_id
FROM {{ ref('actions_get_sample') }}
LEFT JOIN {{ ref('track_login_status') }} AS login_status USING(track_id)
WHERE (time_sort >= login_status.time_start AND time_sort < login_status.time_end) OR login_status.time_start = '1970-01-01'
SETTINGS join_algorithm = 'partial_merge'
