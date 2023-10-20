
with tab as (

select * except _time_end
      ,if(_time_end == '1970-01-01', date('2050-01-01'), _time_end) AS time_end
      ,row_number() over(partition by id order by time_start) as id_rnk
from (

    select * except time_end, any(time_start) over w as _time_end
    from {{ ref('pet_union_prev') }}
    window w as (PARTITION BY user_id ORDER BY time_start ASC
                 ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
)
)

select * except id_rnk
from tab
where id_rnk == 1 
order by time_end