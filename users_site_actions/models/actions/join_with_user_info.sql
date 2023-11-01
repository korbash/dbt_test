{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['time_sort']
) }}

SELECT
    join_with_geo_status.*,
    email,
    public_username,
    reg_ref,
    reg_country,
    reg_currency,
    reg_type,
    reg_time,
    request_time
FROM {{ ref('join_with_geo_status') }}
LEFT JOIN {{ ref('extract_users') }} AS users USING(user_id)