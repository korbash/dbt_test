

SELECT * EXCEPT time_end,
    if(time_end == '1970-01-01', date('2050-01-01'), time_end) AS time_end
from (
    select user_id 
          ,round(rub,2) as rub
          ,round(eur,2) as eur
          ,round(usd,2) as usd
          ,time_sort as time_start
          ,any(time_sort) OVER w AS time_end
    from {{ ref('cash_add_spend_sum') }}
    window w as (PARTITION BY user_id ORDER BY time_sort ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
)
order by user_id, time_start