{{config(
    materialized='incremental',
    engine='MergeTree()',
    order_by=['time_end']
    )}}

SELECT * FROM {{ ref("auth") }}
LIMIT 100

