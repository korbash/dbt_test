select distinct on(id)
       * except(pet, pet_coef, pet_map), sumMap(pet_map) over w as pet_map
from {{ ref('pet_filter') }}
window w as (partition by id)