
{{ config(
    materialized='view',
    order_by=['datetime']
) }}

select * 
from {{ source('src_actions', 'src_actions') }}
where (event == 'pet2game')
  -- and (
  --      ( (success == true) and (pet is not null) ) 
  --   or (success <> true)
  --     )
  -- and (
  --      ( (success == true) and (merge_id is not null) ) 
  --   or (success <> true)
  --     )
