
select id
from {{ ref ('site') }}
group by id
having count(id) > 1