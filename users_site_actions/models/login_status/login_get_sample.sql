{%- set max_date = get_max_date('track_login_status') -%}

select id, track_id, user_id, event, type, method, time_sort as time_start
from {{ source('src_actions', 'src_actions') }}
where
    event in ['auth', 'trade', 'pet2game', 'payment']
    and success
    and isNotNull(user_id)
    and toDate(time_sort)
    between toDate('{{ max_date }}')
    + toIntervalDay(1) and toDate('{{ max_date }}')
    + toIntervalDay(30 + 1)
