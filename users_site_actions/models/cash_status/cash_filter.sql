
{% set get_max_date %}
SELECT max(toDate(time_start)) AS last_date FROM user_cash_status
{% endset %}
{% if execute %}
    {% if run_query('SELECT COUNT() FROM user_cash_status').columns[0].values()[0] > 0 %}
        {% set max_date = run_query(get_max_date).columns[0].values()[0] %}
    {% else %} {% set max_date = '2022-09-01' %}
    {% endif %}
{% endif %}

{# {% set max_date = '2022-09-01' %} #}



select * EXCEPT id_rank
from (

    SELECT id, track_id, user_id, event, type, pet, currency, time_sort,spend
          ,sum(spend) over (partition by id) as sum_spend_per_id
          ,row_number() over(partition by id order by time_sort) as id_rank
    FROM {{ source('src_actions', 'src_actions') }}
    WHERE (event IN ['payment', 'trade', 'pet2game'])
    and (time_sort between toDate('{{max_date}}') + toIntervalDay(1) and
                            toDate('{{max_date}}') + toIntervalMonth(1)
        )
    and (success == true)
)

where id_rank == 1