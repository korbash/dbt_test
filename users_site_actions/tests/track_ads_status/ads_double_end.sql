select track_id, count() as n
from {{ ref('track_ads_status') }}
where time_end == toDate('2050-01-01')
group by track_id
having n > 1
