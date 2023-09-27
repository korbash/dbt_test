
{% set get_max_date %}
SELECT max(toDate(time_start)) AS last_date FROM user_cash_status
{% endset %}
{% if execute %}
    {% if run_query('EXISTS TABLE user_cash_status').columns[0].values()[0] %}
        {% set max_date = run_query(get_max_date).columns[0].values()[0] %}
    {% else %} {% set max_date = '2022-09-01' %}
    {% endif %}
{% endif %}


SELECT user_id, currency, spend, time_sort
FROM {{ source('src_actions', 'src_actions') }}
WHERE (event IN ['payment', 'trade'])
  and (time_sort between toDate('{{ max_date }}') + toIntervalDay(1) and
                         toDate('{{ max_date }}') + toIntervalMonth(1)
      )
  and success = true
  and spend <> 0