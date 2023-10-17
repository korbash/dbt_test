select *
from {{ ref('track_ads_status') }}
where time_end == '1980-01-01'
