select distinct *
from (

    select *
    from {{ ref('pet_add_time_end') }}

    union all 

    select *
    from user_pet_status
    where time_end == date('2050-01-01')
)
order by time_start, time_end