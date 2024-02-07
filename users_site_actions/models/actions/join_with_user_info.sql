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
    CAST(NULL, 'String') AS reg_type,
    reg_time,
    CAST(NULL, 'String') AS request_time
FROM {{ ref('join_with_geo_status') }}
LEFT JOIN {{ source('src_back', 'src_users') }} AS users USING(user_id)

SETTINGS cast_keep_nullable = 1