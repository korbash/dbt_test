{# select user_id, sum(pet_coef) as pet_balance
from {{ ref('pet_filter') }}
group by user_id
having pet_balance < 0 #}