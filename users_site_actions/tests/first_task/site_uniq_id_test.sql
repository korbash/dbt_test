
select dateTrunc('minute', datetime) as minute,
       uniq(id) as uniq_id_cnt,
       count(id) as id_cnt 
from {{ ref ('site') }}  
group by minute
having not uniq_id_cnt / id_cnt BETWEEN 0.99 and 1.01
order by minute
