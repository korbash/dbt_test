{{
    config(
        materialized='incremental',
        uniq_key='id',
        order_by=['time_sort','id','track_id','success'],
        incremental_strategy='delete+insert'
        )
}}


select * except(id_cnt)
from (

select *, count(id) over(partition by id) as id_cnt  
from {{ source('src_actions', 'src_actions') }}
where (event == 'auth') 
  -- FOR INCREMENT
  and (time_sort BETWEEN ( (select toDate(max(time_sort)) from auth) + toIntervalDay(1) )
                     and ( (select toDate(max(time_sort)) from auth) + toIntervalMonth(1) ) )
  
  -- FOR MODEL START
  -- and (time_sort BETWEEN toDate( '{{ var("auth_start_date") }}') 
  --                    and toDate( '{{ var("auth_start_date") }}') + toIntervalMonth(1)
      )

where (id_cnt == 1) 
   or ( (id_cnt = 2) and (type == 'reg') )
