{% set get_max_date %}
SELECT max(toDate(time_start)) AS last_date FROM track_user_status
{% endset %}



select * 
from {{ source('src_actions', 'src_actions') }}
where event == 'auth'
  AND success
  AND isNotNull(user_id)
  AND toDate(time_sort) BETWEEN '2000-07-01' AND '2022-12-01'

