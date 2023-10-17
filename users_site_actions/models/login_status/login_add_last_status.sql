{%- set cols = ['id', 'track_id', 'user_id', 'event', 'type', 'method', 'time_start'] -%}

SELECT {{ ', '.join(cols) }} FROM {{ ref('login_new_uniq_track') }}

UNION ALL 

SELECT {{ ', '.join(cols) }} FROM {{ ref('login_actual_info_sample') }}

UNION ALL

SELECT {{ ', '.join(cols) }} FROM {{ ref('login_get_sample') }}
