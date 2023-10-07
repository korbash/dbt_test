select *
from {{ ref('track_geo_status') }}
where time_end == '1980-01-01'
