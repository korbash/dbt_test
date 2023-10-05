select pet 
from {{ ref ('pet2game') }}
where (pet is null) and (success == true) 