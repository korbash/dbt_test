{{ config(
    materialized='view',
    order_by=['datetime']
) }}

select distinct on (id, track_id, datetime) * 
from {{ source('src_actions', 'src_actions') }}
where (event == 'payment')
  and (merge_id is not null)
  and (user_id is not null) 

