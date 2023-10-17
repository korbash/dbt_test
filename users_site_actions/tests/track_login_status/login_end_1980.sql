select *
from {{ ref('track_login_status') }}
where time_end == '1980-01-01'
