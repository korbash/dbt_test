
select id
from {{ ref ('chat') }}
group by id
having count(id) > 1