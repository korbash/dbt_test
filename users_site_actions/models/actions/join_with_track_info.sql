{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['time_sort']
) }}

-- Model 2: actions_with_tracks
SELECT {{ ref('join_with_login_status') }}.*,
    device,
    browser,
    domain
FROM {{ ref('join_with_login_status') }}
LEFT JOIN {{ ref('tracks') }} AS tracks USING(track_id)