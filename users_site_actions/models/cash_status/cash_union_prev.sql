
select *
from {{ ref('cash_add_prev_status') }}

union all 

select *
from user_cash_status
where time_end == date('2050-01-01')
