{% set get_max_date %}
SELECT max(toDate(time_start)) AS last_date FROM track_login_status
{% endset %}
{% if execute %}
    {% if run_query('EXISTS TABLE track_login_status').columns[0].values()[0] %}
        {% set max_date = run_query(get_max_date).columns[0].values()[0] %}
    {% else %} {% set max_date = '2022-09-01' %}
    {% endif %}
{% endif %}


select *
from {{ source('src_actions', 'src_actions') }}
where
    event in ['auth', 'trade', 'pet2game', 'payment']
    and success
    and isNotNull(user_id)
    and toDate(time_sort)
    between toDate('{{ max_date }}')
    + toIntervalDay(1) and toDate('{{ max_date }}')
    + toIntervalMonth(1)
