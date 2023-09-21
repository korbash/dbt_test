
select count(id) as id_count
from {{ ref ('session_start') }}
group by id
having id_count > 1