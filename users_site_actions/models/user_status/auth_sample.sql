{% set get_max_date %}
SELECT max(toDate(time_start)) AS last_date FROM track_user_status
{% endset %}
{% if execute %}
    {% if run_query('EXISTS TABLE track_user_status').columns[0].values()[0] %}
        {% set max_date = run_query(get_max_date).columns[0].values()[0] %}
    {% else %}
        {% set max_date = '2022-09-01' %}
    {% endif %}
{% endif %}



select * 
from {{ source('src_actions', 'src_actions') }}
where event == 'auth'
  AND success
  AND isNotNull(user_id)
  AND toDate(time_sort) BETWEEN toDate('{{ max_date }}') + toIntervalDay(1) AND toDate('{{ max_date }}') + toIntervalMonth(3)

