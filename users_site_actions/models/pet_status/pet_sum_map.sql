
select * except(pet_map), sumMap(pet_map) over w as sum_pet_map
from {{ ref('pet_make_id_uniq') }}
WINDOW w as (partition by user_id 
             rows BETWEEN UNBOUNDED PRECEDING and current row)
