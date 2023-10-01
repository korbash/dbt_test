{{ config(
    materialized='view',
    order_by=['datetime']
) }}

select *
from {{ source('src_actions', 'src_actions') }}
where (event == 'trade') 
  -- and (spend > 0)
  -- and (pet is not null)
  -- and (currency is not null)
  -- and (merge_id is not null)
  -- and (user_id is not null)