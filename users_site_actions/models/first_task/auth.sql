{{ config(
    materialized='view',
    order_by=['datetime']
) }}

select distinct on (id, track_id, datetime) * 
from {{ source('src_actions', 'src_actions') }}
where (event == 'auth')
  and (
       ( (success == true) and (user_id is not null) ) 
    or (success <> true)
      )

