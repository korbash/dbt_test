{% set get_max_date %}
SELECT max(toDate(time_start)) AS last_date FROM user_pet_status
{% endset %}
{% if execute %}
    {% if run_query('SELECT COUNT() FROM user_pet_status').columns[0].values()[0] > 0 %}
        {% set max_date = run_query(get_max_date).columns[0].values()[0] %}
    {% else %} {% set max_date = '2022-09-01' %}
    {% endif %}
{% endif %}

{# {% set max_date = '2022-09-01' %} #}

select * 
from (

    SELECT id, track_id, user_id, success, pet, time_sort
          ,if(type == 'topup', 1, 
           if(type == 'withdraw', -1,
           if(type == 'pass', 0, null))) as pet_coef
          ,CAST(([pet], [pet_coef]), 'Map(int, int)') as pet_map
    FROM {{ source('src_actions', 'src_actions') }}
    WHERE (event == 'pet2game')
    and (time_sort between toDate('{{max_date}}') + toIntervalDay(1) and
                            toDate('{{max_date}}') + toIntervalWeek(2)
        )
    {# and (success == true) #}
    and isNotNull(pet)
)

