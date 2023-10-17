{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='time_start'
)}}

{{ get_actual_info('track_login_status', ref('login_get_sample')) }}