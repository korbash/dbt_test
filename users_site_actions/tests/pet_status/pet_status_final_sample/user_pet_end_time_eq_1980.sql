select id
from {{ ref('pet_status_final_sample') }}
where time_end == '1980-01-01'
