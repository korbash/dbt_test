{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='time_end'
)}}

SELECT * EXCEPT (time_sort, time_end),
    if(time_end == '1970-01-01', date('2050-01-01'), time_end) AS time_end
FROM (
    SELECT *, time_sort AS time_start,
        any(time_sort) OVER (PARTITION BY track_id ORDER BY time_sort ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS time_end
    FROM {{ ref('ads_cumsum') }}
)

