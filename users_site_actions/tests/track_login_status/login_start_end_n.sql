select
    countIf(time_end == '2050-01-01') as start_n,
    countIf(time_start == '1980-01-01') as end_n
from {{ ref('track_login_status') }}
having start_n != end_n