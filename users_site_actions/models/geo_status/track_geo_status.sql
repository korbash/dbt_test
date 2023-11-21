{{ config(
    tags=['status','ready'],
    materialized='status_incremental',
    engine='MergeTree()',
    order_by='time_end',
    partition_by="time_end == '2050-01-01'"
)}}

SELECT * FROM {{ ref('geo_add_time_end') }}