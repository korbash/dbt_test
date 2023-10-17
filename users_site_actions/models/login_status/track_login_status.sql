{{ config(
    materialized='incremental',
    incremental_strategy='legacy',
    engine='MergeTree()',
    order_by='time_end',
    unique_key='id'
)}}

-- config(
--    materialized='status_incremental',
--    engine='MergeTree()',
--    order_by='time_end',
--    partition_by="time_end == '2050-01-01'"
--)

SELECT * FROM {{ ref('login_add_time_end') }}