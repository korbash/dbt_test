
with pet_prev_status as (

select    toString(generateUUIDv4()) as id, user_id 
          ,map() as sum_pet_map 
          ,toDateTime('1980-01-01 00:00:00') as _time_start, time_start as time_end
from (

select *, row_number() over(partition by user_id order by time_start) as action_rank
from {{ ref('pet_add_time_end') }}
where user_id not in (select distinct user_id 
                      from user_pet_status  
                      where time_start == toDateTime('1980-01-01 00:00:00')) 
)
where action_rank == 1

union all 

select * EXCEPT(track_id, success)
from {{ ref('pet_add_time_end') }}

)



select distinct *
from (
    select * except (_time_start, time_end), _time_start as time_start, time_end
    from pet_prev_status
)