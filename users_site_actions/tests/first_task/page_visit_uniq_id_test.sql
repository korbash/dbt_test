
select id
from {{ ref ('page_visit') }}
group by id
having count(id) > 1