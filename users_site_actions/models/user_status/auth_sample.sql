{{ config(
    materialized='view',
    order_by=['datetime']
) }}

select * 
from {{ source('src_actions', 'src_actions') }}
where event == 'auth'
  AND success
  AND isNotNull(user_id)
  AND toDate(time_sort) BETWEEN '2023-07-01' AND '2023-09-01'

