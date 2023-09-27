{{
    config(
        materialized='incremental',
        uniq_key='id',
        order_by=['time_start','id','track_id'],
        incremental_strategy='delete+insert'
        )
}}

SELECT id, type, track_id, user_id, time_start,
    if(time_end == '1970-01-01', date('2050-01-01'), time_end) AS time_end
FROM (
        SELECT id, type, track_id, user_id, datetime AS time_start,
                any(datetime) OVER (PARTITION BY track_id ORDER BY datetime ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS time_end
        FROM {{ ref('auth') }}
        WHERE success and datetime BETWEEN toDate('2023-01-03') and toDate('2023-03-03')
)
