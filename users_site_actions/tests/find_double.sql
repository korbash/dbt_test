select *
from {{ ref('track_login_status') }}
where time_start == '1980-01-01' and time_end == '1980-01-01'
