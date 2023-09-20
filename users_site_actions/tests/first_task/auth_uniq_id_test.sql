
select id
from {{ ref ('auth') }}
group by id
having count(id) > 1
