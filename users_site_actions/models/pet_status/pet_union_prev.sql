
select *
from {{ ref('pet_add_prev_status') }}

union all 

select *
from user_pet_status
where time_end == date('2050-01-01')
