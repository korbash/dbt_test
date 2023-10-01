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
  and (time_sort BETWEEN ( (select toDate(max(time_sort)) from auth) + toIntervalDay(1))
                     and ( (select toDate(max(time_sort)) from auth) + toIntervalMonth(1)) )
  -- and (time_sort BETWEEN toDate('2022-09-01') and toDate('2022-10-01') )
)
where (id_cnt == 1) 
   or ( (id_cnt = 2) and (type == 'reg') )





-- select *
-- from {{ source('src_actions', 'src_actions') }}
-- where (event == 'auth')
  -- and (
  --      ( (success == true) and (user_id is not null) ) 
  --   or (success <> true)
  --     )

