{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['event', 'success', 'type', 'method', 'datetime']
)}}
{%- set max_date = get_max_date2('time_sort', 'user_actions2')  -%}

SELECT * FROM {{ source('src_actions', 'src_actions') }} 
WHERE  time_sort BETWEEN
    toDate('{{ max_date }}') + toIntervalDay(1) 
    AND toDate('{{ max_date }}') + toIntervalDay(2 + 1)