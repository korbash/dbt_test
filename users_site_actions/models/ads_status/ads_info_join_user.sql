{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['user_id', 'time_start'],
) }}

SELECT * EXCEPT rank FROM (
    SELECT * EXCEPT (user_id, track_id, time_end),
        assumeNotNull(user_id) AS user_id,
        rank() OVER (PARTITION BY id ORDER BY time_end) AS rank
    FROM (
        SELECT tb1.*, tb2.user_id, tb2.time_end FROM {{ ref('ads_exstract_info') }} AS tb1 LEFT JOIN {{ ref('track_login_status') }} AS tb2 USING(track_id)
        WHERE tb2.user_id IS NOT NULL
            AND tb2.time_end >= tb1.time_start
    )
)
WHERE rank == 1

