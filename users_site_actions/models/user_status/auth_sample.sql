{% set get_max_date %}
SELECT max(toDate(time_start)) AS last_date FROM track_user_status
{% endset %}



select * 
from {{ source('src_actions', 'src_actions') }}
where event == 'auth'
  AND success
  AND isNotNull(user_id)
  AND toDate(time_sort) BETWEEN {% do get_max_date %} + toIntervalDay(1) 
                            AND {% do get_max_date %} + toIntervalMonth(1)

