{%- set max_date = get_max_date('track_geo_status') -%}
{{ get_uniq_track_sample(max_date, 15) }}