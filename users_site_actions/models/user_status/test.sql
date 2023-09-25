SELECT
{% if execute %}
    {% if run_query('EXISTS TABLE track_user_status').columns[0].values()[0] %}
        'qqq'
    {% else %}
        'www'
    {% endif %}
{% endif %}