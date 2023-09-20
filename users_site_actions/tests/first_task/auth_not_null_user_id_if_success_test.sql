select user_id 
from {{ ref ('auth') }}
where (user_id is null) and (success == true) 