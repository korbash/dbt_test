{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='time_start'
)}}
{%- set max_date = get_max_date('track_login_status') -%}
{{ get_uniq_track_sample(max_date, 15) }}