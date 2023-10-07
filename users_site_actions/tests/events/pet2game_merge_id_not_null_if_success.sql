
select merge_id 
from {{ ref ('pet2game') }}
where (merge_id is null) and (success == true) 