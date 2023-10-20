select id
from {{ ref('user_pet_status') }}
where time_start == toDateTime('1980-01-01 00:00:00') and length(sum_pet_map) > 0