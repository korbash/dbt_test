{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='time_start'
)}}
{{ get_actual_info('track_geo_status', ref('geo_get_sample')) }}