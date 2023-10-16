{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['time_end', 'time_start']
)}}
select
    * except time_end,
    if(time_end == '1970-01-01', date('2050-01-01'), time_end) as time_end
from
    (
        select
            *, any (time_start) over (
                partition by track_id
                order by time_start ASC
                rows between 1 FOLLOWING and 1 FOLLOWING
            ) as time_end
        from {{ ref('geo_remove_consist') }}
    )
