select user_id, count() as n
from {{ ref('user_ads_status') }}
where time_end == toDate('2050-01-01')
group by user_id
having n > 1
