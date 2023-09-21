
select merge_id
from {{ ref ('payment') }}
group by merge_id
having count(id) > 1