select *
from {{ ref('cash_status_final_sample') }}
where time_end == '1980-01-01'
