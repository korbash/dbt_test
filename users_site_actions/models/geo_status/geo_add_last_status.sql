{%- set cols = ['id', 'track_id', 'event', 'type', 'country', 'currency', 'lang', 'time_start'] -%}

SELECT {{ ', '.join(cols) }} FROM {{ ref('geo_new_uniq_track') }}

UNION ALL 

SELECT {{ ', '.join(cols) }} FROM {{ ref('geo_actual_info_sample') }}

UNION ALL

SELECT {{ ', '.join(cols) }} FROM {{ ref('geo_get_sample') }}
