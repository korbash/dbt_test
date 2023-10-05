
select id
from {{ ref ('payment') }}
group by id
having count(id) > 1