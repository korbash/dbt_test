SELECT * EXCEPT time_end
      ,if(time_end == '1970-01-01', date('2050-01-01'), time_end) AS time_end
from (

    select * except time_sort
        ,time_sort as time_start
        ,any(time_sort) OVER w AS time_end
    from {{ ref('pet_sum_map') }}
    window w as (PARTITION BY user_id ORDER BY time_sort ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
    )