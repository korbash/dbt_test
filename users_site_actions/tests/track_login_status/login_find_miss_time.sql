select *
from {{ ref('track_login_status') }}
where time_start == toDate('1970-01-01') or time_end == toDate('1970-01-01')
